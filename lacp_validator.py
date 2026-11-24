"""LACP 聚合组状态校验器（模拟/验证用途，不构造真实 LACPDU）。

按时间片推进简化的 LACP 成员状态机，计算：
- 每个成员的聚合状态（是否可承载流量）
- 聚合组（bundle）可用性与 min-links 判定
- 基于哈希键的流映射与迁移
- 重新哈希时的在途（in-flight）去重保护

不变量：
1. 未同步（未进入 COLLECTING_DISTRIBUTING）的成员不得承载流量。
2. 成员恢复后必须经过稳定窗口（stable_window 个连续同步时间片）才重新进入哈希。
3. 成员数跌破 min-links 时，bundle 整体失效，任何成员都不得承载流量。
4. 重新哈希迁移某条流时，若其在旧成员上仍有在途数据，则迁移被推迟，
   同一连接不得在两个成员上同时存在在途数据。
"""

import zlib
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional


class MemberState(Enum):
    DETACHED = "detached"                    # 链路 down 或伙伴超时
    WAIT_SYNC = "wait_sync"                  # 收到 LACPDU 但伙伴未同步
    STABILIZING = "stabilizing"              # 已同步，处于恢复稳定窗口内
    COLLECTING_DISTRIBUTING = "collecting_distributing"  # 可承载流量


@dataclass
class TimerConfig:
    """快/慢定时器：连续 missed 多少个时间片判定伙伴超时。"""
    fast_timeout_slices: int = 3    # fast(1s) 模式，约 3 个周期超时
    slow_timeout_slices: int = 9    # slow(30s) 模式，折算后的时间片数


@dataclass
class MemberInput:
    """单个时间片内某成员的观测输入。"""
    member_id: str
    link_up: bool
    lacpdu_received: bool
    actor_key: int
    partner_key: int
    partner_sync: bool          # 伙伴 LACPDU 中的 SYN 位
    timer_mode: str = "slow"    # "fast" 或 "slow"


@dataclass
class FlowInput:
    """在途流：当前时间片在其承载成员上的在途报文数。"""
    flow_id: str
    hash_key: int
    in_flight: int = 0


@dataclass
class SliceInput:
    members: List[MemberInput]
    flows: List[FlowInput] = field(default_factory=list)


@dataclass
class Migration:
    flow_id: str
    from_member: Optional[str]
    to_member: Optional[str]
    slice_index: int
    deferred: bool = False      # 因旧成员仍有在途数据而推迟
    reason: str = ""


@dataclass
class SliceResult:
    index: int
    bundle_available: bool
    reasons: List[str]
    member_states: Dict[str, MemberState]
    flow_map: Dict[str, Optional[str]]   # flow_id -> 承载成员（None 表示被挂起）
    migrations: List[Migration]
    stalled_flows: Dict[str, int] = field(default_factory=dict)  # 失效成员上的残留


@dataclass
class SimulationResult:
    slices: List[SliceResult]

    @property
    def bundle_available(self):
        return [s.bundle_available for s in self.slices]

    @property
    def all_migrations(self):
        return [m for s in self.slices for m in s.migrations]


class _MemberRuntime:
    __slots__ = ("state", "missed", "stability")

    def __init__(self):
        self.state = MemberState.DETACHED
        self.missed = 0
        self.stability = 0


class LacpBundleValidator:
    """LACP 聚合组状态校验器。

    stable_window:   成员恢复后需连续保持同步的时间片数，期间不进入哈希。
    min_links:       可用成员数下限，跌破则 bundle 整体失效。
    drain_per_slice: 成员失效后，其在途数据每个时间片自然消化的报文数。
    """

    def __init__(self, bundle_key, min_links=1, stable_window=3,
                 drain_per_slice=1, timers=None):
        if min_links < 1:
            raise ValueError("min_links must be >= 1")
        self.bundle_key = bundle_key
        self.min_links = min_links
        self.stable_window = stable_window
        self.drain_per_slice = drain_per_slice
        self.timers = timers or TimerConfig()
        self._members = {}          # member_id -> _MemberRuntime
        self._flow_member = {}      # flow_id -> 当前承载成员
        self._flow_stalled = {}     # flow_id -> 失效成员上剩余在途数

    # ---- 成员状态机 ----

    def _timeout_for(self, mode):
        if mode == "fast":
            return self.timers.fast_timeout_slices
        return self.timers.slow_timeout_slices

    def _step_member(self, mi):
        rt = self._members.setdefault(mi.member_id, _MemberRuntime())
        prev = rt.state

        if not mi.link_up:
            rt.state, rt.missed, rt.stability = MemberState.DETACHED, 0, 0
            return rt.state

        if mi.lacpdu_received:
            rt.missed = 0
        else:
            rt.missed += 1
            if rt.missed >= self._timeout_for(mi.timer_mode):
                # 伙伴超时：立即退出哈希
                rt.state, rt.stability = MemberState.DETACHED, 0
                return rt.state

        synced = (mi.partner_sync
                  and mi.actor_key == self.bundle_key
                  and mi.partner_key == self.bundle_key)
        if not synced:
            rt.state = MemberState.WAIT_SYNC
            rt.stability = 0
            return rt.state

        # 已同步：恢复必须熬过稳定窗口；窗口内抖动会清零计数
        if prev in (MemberState.DETACHED, MemberState.WAIT_SYNC,
                    MemberState.STABILIZING):
            rt.stability += 1
            if rt.stability >= self.stable_window:
                rt.state = MemberState.COLLECTING_DISTRIBUTING
            else:
                rt.state = MemberState.STABILIZING
        else:
            rt.state = MemberState.COLLECTING_DISTRIBUTING
        return rt.state

    # ---- 哈希与流迁移 ----

    @staticmethod
    def _hash_member(hash_key, eligible):
        return eligible[zlib.crc32(str(hash_key).encode()) % len(eligible)]

    def _step_flows(self, flows, eligible, slice_index, reasons):
        flow_map = {}
        migrations = []
        eligible_set = set(eligible)

        for fl in flows:
            desired = (self._hash_member(fl.hash_key, eligible)
                       if eligible else None)
            current = self._flow_member.get(fl.flow_id)
            stalled_left = self._flow_stalled.get(fl.flow_id, 0)

            # 失效成员上的残留：按消化速率递减，期间不得在新成员发送
            if stalled_left > 0:
                stalled_left = max(0, stalled_left - self.drain_per_slice)
                self._flow_stalled[fl.flow_id] = stalled_left

            if current is not None and current not in eligible_set:
                # 旧成员已退出哈希：残留计入 stalled，立即断开映射
                residual = max(stalled_left, fl.in_flight)
                self._flow_stalled[fl.flow_id] = residual
                stalled_left = residual
                self._flow_member[fl.flow_id] = None
                current = None

            if desired is None:
                if current is not None:
                    self._flow_member[fl.flow_id] = None
                    migrations.append(Migration(
                        fl.flow_id, current, None, slice_index,
                        reason="bundle unavailable"))
                flow_map[fl.flow_id] = None
                continue

            if current is None:
                if stalled_left > 0:
                    # 旧成员在途未消化完：挂起，避免重复在途
                    flow_map[fl.flow_id] = None
                    reasons.append(
                        "flow %s stalled: %d in-flight on dead member"
                        % (fl.flow_id, stalled_left))
                    continue
                self._flow_member[fl.flow_id] = desired
                flow_map[fl.flow_id] = desired
                migrations.append(Migration(
                    fl.flow_id, None, desired, slice_index,
                    reason="initial/reattach"))
                continue

            if current == desired:
                flow_map[fl.flow_id] = current
                continue

            if fl.in_flight > 0:
                # 旧成员仍在哈希内且有在途：推迟迁移，保持钉在旧成员
                flow_map[fl.flow_id] = current
                migrations.append(Migration(
                    fl.flow_id, current, desired, slice_index,
                    deferred=True,
                    reason="deferred: %d in-flight on %s"
                           % (fl.in_flight, current)))
                continue

            self._flow_member[fl.flow_id] = desired
            flow_map[fl.flow_id] = desired
            migrations.append(Migration(
                fl.flow_id, current, desired, slice_index,
                reason="rehash"))

        return flow_map, migrations

    # ---- 主循环 ----

    def run(self, slices):
        results = []
        for idx, si in enumerate(slices):
            states = {}
            for mi in si.members:
                states[mi.member_id] = self._step_member(mi)

            synced_count = len([s for s in states.values()
                                if s is MemberState.COLLECTING_DISTRIBUTING])
            eligible = sorted(mid for mid, st in states.items()
                              if st is MemberState.COLLECTING_DISTRIBUTING)
            reasons = []

            if len(eligible) < self.min_links:
                # 跌破 min-links：bundle 整体失效，任何成员不得承载流量
                bundle_available = False
                eligible = []
                reasons.append(
                    "min-links violated: %d eligible < %d required"
                    % (synced_count, self.min_links))
            else:
                bundle_available = True

            for mid, st in states.items():
                if st is not MemberState.COLLECTING_DISTRIBUTING:
                    reasons.append("member %s not synced (%s)"
                                   % (mid, st.value))

            flow_map, migrations = self._step_flows(
                si.flows, eligible, idx, reasons)

            results.append(SliceResult(
                index=idx,
                bundle_available=bundle_available,
                reasons=reasons,
                member_states=states,
                flow_map=flow_map,
                migrations=migrations,
                stalled_flows={fid: n for fid, n in self._flow_stalled.items()
                               if n > 0},
            ))
        return SimulationResult(results)


def check_no_duplicate_in_flight(result):
    """校验不变量：一条流在旧成员上仍有未消化在途时，不得映射到任何成员。

    返回违规描述列表，空列表表示通过。
    """
    violations = []
    for sr in result.slices:
        for flow_id, mapped in sr.flow_map.items():
            left = sr.stalled_flows.get(flow_id, 0)
            if mapped is not None and left > 0:
                violations.append(
                    "slice %d: flow %s mapped to %s while %d still "
                    "in-flight on old member"
                    % (sr.index, flow_id, mapped, left))
    return violations


def check_unsynced_members_idle(result):
    """校验不变量：未同步成员与不可用 bundle 上不得承载任何流量。"""
    violations = []
    for sr in result.slices:
        for flow_id, member in sr.flow_map.items():
            if member is None:
                continue
            if not sr.bundle_available:
                violations.append(
                    "slice %d: flow %s on %s while bundle unavailable"
                    % (sr.index, flow_id, member))
            elif sr.member_states.get(member) is not \
                    MemberState.COLLECTING_DISTRIBUTING:
                violations.append(
                    "slice %d: flow %s on unsynced member %s"
                    % (sr.index, flow_id, member))
    return violations
