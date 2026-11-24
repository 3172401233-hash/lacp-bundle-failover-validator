"""LACP 状态校验器单元测试（Python 3.8.2 兼容，python -m unittest 运行）。"""

import unittest

from lacp_validator import (
    FlowInput,
    LacpBundleValidator,
    MemberInput,
    MemberState,
    SliceInput,
    TimerConfig,
    check_no_duplicate_in_flight,
    check_unsynced_members_idle,
)

KEY = 100


def member(mid, link_up=True, lacpdu=True, sync=True, mode="slow"):
    return MemberInput(
        member_id=mid,
        link_up=link_up,
        lacpdu_received=lacpdu,
        actor_key=KEY,
        partner_key=KEY,
        partner_sync=sync,
        timer_mode=mode,
    )


def flow(fid, key, in_flight=0):
    return FlowInput(flow_id=fid, hash_key=key, in_flight=in_flight)


class TestSingleMemberFlap(unittest.TestCase):
    """单成员反复抖动：未同步不得承载流量，恢复必须熬过稳定窗口。"""

    def test_flap_resets_stable_window(self):
        v = LacpBundleValidator(bundle_key=KEY, min_links=1, stable_window=3)
        # m1 稳定在线；m2 每同步 2 片就掉一次，永远熬不满窗口 3
        slices = []
        pattern = [True, True, False]  # m2 link_up 模式：起来 2 片就掉
        for t in range(12):
            slices.append(SliceInput(
                members=[member("m1"), member("m2", link_up=pattern[t % 3])],
                flows=[flow("f1", 7), flow("f2", 8), flow("f3", 9)],
            ))
        res = v.run(slices)

        # m2 从未进入 COLLECTING_DISTRIBUTING
        for sr in res.slices:
            self.assertIsNot(sr.member_states["m2"],
                             MemberState.COLLECTING_DISTRIBUTING)
        # 流绝不允许落到抖动成员 m2 上
        for sr in res.slices:
            for fid, m in sr.flow_map.items():
                self.assertNotEqual(m, "m2",
                                    "slice %d flow %s" % (sr.index, fid))
        # m1 熬满自身稳定窗口（t=2）后，所有流只落在 m1 上
        for sr in res.slices[2:]:
            for fid, m in sr.flow_map.items():
                self.assertEqual(m, "m1", "slice %d flow %s" % (sr.index, fid))
        # 不变量成立
        self.assertEqual(check_unsynced_members_idle(res), [])
        self.assertEqual(check_no_duplicate_in_flight(res), [])

    def test_unsynced_member_carries_nothing(self):
        v = LacpBundleValidator(bundle_key=KEY, min_links=1, stable_window=2)
        slices = [
            # t0: m1 伙伴未同步
            SliceInput(members=[member("m1", sync=False)],
                       flows=[flow("f1", 1)]),
            # t1: 同步第 1 片，仍在稳定窗口内
            SliceInput(members=[member("m1")], flows=[flow("f1", 1)]),
            # t2: 稳定窗口熬满，进入哈希
            SliceInput(members=[member("m1")], flows=[flow("f1", 1)]),
        ]
        res = v.run(slices)
        self.assertIsNone(res.slices[0].flow_map["f1"])
        self.assertIsNone(res.slices[1].flow_map["f1"])
        self.assertEqual(res.slices[2].flow_map["f1"], "m1")
        self.assertEqual(res.slices[0].member_states["m1"],
                         MemberState.WAIT_SYNC)
        self.assertEqual(res.slices[1].member_states["m1"],
                         MemberState.STABILIZING)
        self.assertEqual(check_unsynced_members_idle(res), [])


class TestMinLinks(unittest.TestCase):
    """跌破 min-links 时 bundle 必须整体失效。"""

    def test_bundle_fails_below_min_links(self):
        v = LacpBundleValidator(bundle_key=KEY, min_links=2, stable_window=1)
        slices = [
            # t0: 两个成员都同步，bundle 可用
            SliceInput(members=[member("m1"), member("m2")],
                       flows=[flow("f1", 7), flow("f2", 8)]),
            # t1: m2 掉线，只剩 1 个可用成员 < min_links=2
            SliceInput(members=[member("m1"), member("m2", link_up=False)],
                       flows=[flow("f1", 7), flow("f2", 8)]),
            # t2: m2 恢复并熬满窗口（stable_window=1）
            SliceInput(members=[member("m1"), member("m2")],
                       flows=[flow("f1", 7), flow("f2", 8)]),
        ]
        res = v.run(slices)

        self.assertTrue(res.slices[0].bundle_available)
        # t1: bundle 整体失效，m1 虽然同步也不得承载流量
        self.assertFalse(res.slices[1].bundle_available)
        self.assertIs(res.slices[1].member_states["m1"],
                      MemberState.COLLECTING_DISTRIBUTING)
        self.assertTrue(all(m is None
                            for m in res.slices[1].flow_map.values()))
        self.assertTrue(any("min-links" in r
                            for r in res.slices[1].reasons))
        # t2: 恢复
        self.assertTrue(res.slices[2].bundle_available)
        self.assertEqual(check_unsynced_members_idle(res), [])

    def test_reason_reported(self):
        v = LacpBundleValidator(bundle_key=KEY, min_links=3, stable_window=1)
        res = v.run([SliceInput(members=[member("m1"), member("m2")])])
        self.assertFalse(res.slices[0].bundle_available)
        self.assertIn("min-links violated: 2 eligible < 3 required",
                      res.slices[0].reasons)


class TestFastRecovery(unittest.TestCase):
    """快速恢复：稳定窗口后重新入哈希；快定时器更快判定伙伴超时。"""

    def test_recovery_after_stable_window(self):
        v = LacpBundleValidator(bundle_key=KEY, min_links=1, stable_window=2)
        slices = [
            SliceInput(members=[member("m1")], flows=[flow("f1", 7)]),
            SliceInput(members=[member("m1")], flows=[flow("f1", 7)]),
            SliceInput(members=[member("m1", link_up=False)],
                       flows=[flow("f1", 7)]),
            SliceInput(members=[member("m1")], flows=[flow("f1", 7)]),
            SliceInput(members=[member("m1")], flows=[flow("f1", 7)]),
        ]
        res = v.run(slices)
        self.assertEqual(res.slices[1].flow_map["f1"], "m1")
        self.assertIsNone(res.slices[2].flow_map["f1"])
        self.assertIsNone(res.slices[3].flow_map["f1"])  # 窗口内不得承载
        self.assertEqual(res.slices[4].flow_map["f1"], "m1")
        # 掉线期间原因被记录
        self.assertTrue(any("m1" in r for r in res.slices[2].reasons))

    def test_fast_timer_expires_sooner(self):
        timers = TimerConfig(fast_timeout_slices=2, slow_timeout_slices=6)

        def run_with(mode):
            v = LacpBundleValidator(bundle_key=KEY, min_links=1,
                                    stable_window=1, timers=timers)
            slices = [SliceInput(members=[member("m1", mode=mode)])]
            # 连续 4 片收不到 LACPDU
            for _ in range(4):
                slices.append(SliceInput(
                    members=[member("m1", lacpdu=False, mode=mode)]))
            return v.run(slices)

        fast = run_with("fast")
        slow = run_with("slow")
        # fast: 第 2 片未收到即超时退出哈希
        self.assertIs(fast.slices[2].member_states["m1"], MemberState.DETACHED)
        # slow: 第 2 片仍保持，直到第 6 片才超时
        self.assertIs(slow.slices[2].member_states["m1"],
                      MemberState.COLLECTING_DISTRIBUTING)


class TestHashChange(unittest.TestCase):
    """哈希变化：成员增减触发重新哈希，迁移不得造成重复在途。"""

    def test_rehash_on_membership_change(self):
        v = LacpBundleValidator(bundle_key=KEY, min_links=1, stable_window=1)
        flows = [flow("f%d" % i, i) for i in range(8)]
        slices = [
            SliceInput(members=[member("m1")], flows=flows),
            SliceInput(members=[member("m1"), member("m2")], flows=flows),
            SliceInput(members=[member("m1"), member("m2")], flows=flows),
        ]
        res = v.run(slices)
        # t0 全部落 m1
        self.assertTrue(all(m == "m1" for m in res.slices[0].flow_map.values()))
        # t1 起哈希到两个成员
        targets = set(res.slices[2].flow_map.values())
        self.assertEqual(targets, {"m1", "m2"})
        # 发生了 rehash 迁移
        rehashed = [mg for mg in res.all_migrations if mg.reason == "rehash"]
        self.assertTrue(rehashed)
        self.assertEqual(check_unsynced_members_idle(res), [])
        self.assertEqual(check_no_duplicate_in_flight(res), [])

    def test_no_duplicate_in_flight_on_rehash(self):
        # 先探出一条在 m1+m2 扩容时会迁移的流
        probe = LacpBundleValidator(bundle_key=KEY, min_links=1,
                                    stable_window=1)
        probe_res = probe.run([
            SliceInput(members=[member("m1")],
                       flows=[flow("f%d" % i, i) for i in range(20)]),
            SliceInput(members=[member("m1"), member("m2")],
                       flows=[flow("f%d" % i, i) for i in range(20)]),
        ])
        moved = [mg.flow_id for mg in probe_res.all_migrations
                 if mg.reason == "rehash"]
        self.assertTrue(moved, "需要至少一条流在扩容时迁移")
        fid = moved[0]
        key = int(fid[1:])

        # 正式场景：迁移发生时该流在旧成员上仍有在途数据
        v2 = LacpBundleValidator(bundle_key=KEY, min_links=1, stable_window=1)
        slices = [
            SliceInput(members=[member("m1")],
                       flows=[flow(fid, key, in_flight=5)]),
            SliceInput(members=[member("m1"), member("m2")],
                       flows=[flow(fid, key, in_flight=5)]),   # 触发 rehash
            SliceInput(members=[member("m1"), member("m2")],
                       flows=[flow(fid, key, in_flight=0)]),    # 在途清空
            SliceInput(members=[member("m1"), member("m2")],
                       flows=[flow(fid, key, in_flight=0)]),
        ]
        res = v2.run(slices)
        # t1: 目标变为 m2，但在途未清空 -> 推迟，仍钉在 m1
        deferred = [mg for mg in res.slices[1].migrations if mg.deferred]
        self.assertEqual(len(deferred), 1)
        self.assertEqual(res.slices[1].flow_map[fid], "m1")
        # t2: 在途清空后完成迁移
        self.assertEqual(res.slices[2].flow_map[fid], "m2")
        self.assertEqual(check_no_duplicate_in_flight(res), [])

    def test_member_death_stalls_until_drained(self):
        # 先探出 f1 落在哪个成员上
        probe = LacpBundleValidator(bundle_key=KEY, min_links=1,
                                    stable_window=1)
        probe_res = probe.run([
            SliceInput(members=[member("m1"), member("m2")],
                       flows=[flow("f1", 7)]),
        ])
        fid_member = probe_res.slices[0].flow_map["f1"]
        dead, alive = ("m1", "m2") if fid_member == "m1" else ("m2", "m1")

        v2 = LacpBundleValidator(bundle_key=KEY, min_links=1, stable_window=1,
                                 drain_per_slice=2)
        slices = [
            SliceInput(members=[member("m1"), member("m2")],
                       flows=[flow("f1", 7)]),
            SliceInput(members=[member("m1"), member("m2")],
                       flows=[flow("f1", 7)]),
            # 承载成员掉线，残留 5 个在途
            SliceInput(members=[member(dead, link_up=False), member(alive)],
                       flows=[flow("f1", 7, in_flight=5)]),
            SliceInput(members=[member(dead, link_up=False), member(alive)],
                       flows=[flow("f1", 7)]),
            SliceInput(members=[member(dead, link_up=False), member(alive)],
                       flows=[flow("f1", 7)]),
            SliceInput(members=[member(dead, link_up=False), member(alive)],
                       flows=[flow("f1", 7)]),
        ]
        res2 = v2.run(slices)
        # t2: 旧成员残留 5，挂起不得映射到 alive
        self.assertIsNone(res2.slices[2].flow_map["f1"])
        self.assertEqual(res2.slices[2].stalled_flows["f1"], 5)
        # t3: 消化 2 个剩 3，仍挂起
        self.assertIsNone(res2.slices[3].flow_map["f1"])
        # 消化完毕（5 -> 3 -> 1 -> 0）后迁移到 alive
        self.assertEqual(res2.slices[5].flow_map["f1"], alive)
        self.assertEqual(check_no_duplicate_in_flight(res2), [])
        self.assertEqual(check_unsynced_members_idle(res2), [])


if __name__ == "__main__":
    unittest.main()
