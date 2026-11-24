# lacp-bundle-failover-validator
验证 LACP 成员状态、min-links 和流量迁移

## 概述

`lacp_validator.py` 实现一个按时间片推进的 LACP 聚合组状态校验器。
输入每个时间片的成员状态、LACPDU 观测（actor/partner key、SYN 位）、
快/慢定时器模式、min-links、哈希键与在途流，输出每个时间片的：

- 成员状态机：`detached / wait_sync / stabilizing / collecting_distributing`
- bundle 可用性与不可用原因（min-links 违反、成员未同步等）
- 流 → 成员的哈希映射与迁移记录（含推迟原因）

## 保证的不变量

1. 未同步成员不得承载流量（`check_unsynced_members_idle` 校验）。
2. 成员恢复后必须连续同步 `stable_window` 个时间片才重新进入哈希，
   窗口内抖动会清零计数。
3. 可用成员数跌破 `min_links` 时 bundle 整体失效，任何成员都不得承载流量。
4. 重新哈希时，若流在旧成员上仍有在途数据，迁移被推迟（钉在旧成员）；
   旧成员已失效时流被挂起，直到残留按 `drain_per_slice` 消化完毕，
   同一连接不会在两个成员上产生重复在途数据
   （`check_no_duplicate_in_flight` 校验）。

## 说明

本校验器**不构造真实 LACPDU**：LACPDU 的解析结果（是否收到、actor/partner
key、SYN 位、快/慢定时器模式）以结构化输入（`MemberInput`）的形式给出，
只模拟状态机与流量工程语义，不涉及 802.3ad 帧格式、以太网收发或
慢协议组播地址。哈希使用 `crc32(hash_key) % 成员数` 的确定性取模，
仅用于验证映射与迁移逻辑，不代表任何厂商的真实哈希算法。

## 运行测试

```sh
python -m unittest
```

兼容 Python 3.8.2+（仅使用标准库）。测试覆盖：单成员反复抖动、
min-links 整体失效、快速恢复与快/慢定时器超时差异、哈希变化与
在途去重（推迟迁移 / 挂起消化）。
