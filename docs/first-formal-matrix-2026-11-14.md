# 第一份正式 matrix：未封存及工具时序纠错

记录时间：2026-11-15T00:01:03+08:00（Asia/Shanghai，TZ=Asia/Shanghai date --iso-8601=seconds实测）。文件名及原run目录的20261114沿用历史运行日期；run_id中的20261114T155220Z为UTC时间（2026-11-14 23:52:20 Asia/Shanghai），不随本地日历进入11月15日重命名。

来源：主agent正式运行/独立验收派遣消息；文档子agent只读解析现有 report、manifest、candidate ledger 和异常格 raw，未重跑 ROS、campaign 或 verify。主agent报告独立 `verify` 返回 rc1；本页不将其写为文档子agent执行结果。报告及 raw 保持原样。

## 身份与结果

- 报告：[report.json](../results/s1/late-endpoint-r0-20261114T155220Z-f03e5741/report.json)。文件字节 SHA-256：`844d064b88b826022d0c5982fea80aefa59910848c0027cc5be4e6653110c272`。报告内 `report_sha256=e685f4ba21d497883976e74c818e0792d9f73427920645c2e1d9dffac88148be` 是其自身合同摘要，不能与完整文件字节 hash 混用。
- run_id：`late-endpoint-r0-20261114T155220Z-f03e5741`；target：`discovery.late_endpoint`；revision：0；domain：180。
- manifest source HEAD：`21f0e2679703e366463c35e6d01c2be30fcec919`；`source_frozen=true`，`dirty_behavior_paths=[]`。这是本次运行保存的身份，不代表后续补丁工作树仍冻结。
- manifest behavior SHA-256：`9def58d5ff964722f8b78368237fe667e3c5b508eec5a1c2ea587d4e9efaf4b1`。
- RMW：`rmw_cyclonedds_cpp`、`rmw_fastrtps_cpp`；全部24行 `actual_run=true`、`runtime_status=completed`。运行完成不等于所有执行均 judged。

| 验收项 | 本次结果 | 含义 |
| --- | --- | --- |
| required | 17 pass + 1 unknown / 18 | unknown预算为0，未满足 |
| health | 6/6 | 满足本次报告控制门槛 |
| mutation | 6/6 | 满足本次报告控制门槛 |
| fault | 6/6，六条 primary 均按预期 fail | 独立 harness negative 控制，不计自然候选 |
| transparency | 5/6 | 未满足 |
| per-target seal | false | 不得写为第一 target 已成熟 |
| candidate ledger | candidates=[]，confirmed=0 | 不代表无缺陷或完整 S1 完成 |
| 成熟分母 | 第一target 18；完整S1 72 | `maturity_denominator_changed=false` |

## 唯一 unknown 的 raw chronology

异常格：`rmw_fastrtps_cpp.transparent.1`。runtime 完成，但 adapter 返回 `match_not_observed`，执行阶段为 `aborted`；primary oracle 为 `unknown`，reason 为 `unknown_evidence:'match'`，归因 `harness`。seal 同时列出 cell_not_actually_judged、required_unknown_gate_failed、transparency_control_failed:5/6。

原始文件：[raw.jsonl](../results/s1/late-endpoint-r0-20261114T155220Z-f03e5741/cells/rmw_fastrtps_cpp.transparent.1/runtime/raw.jsonl)，字节 SHA-256：`f14d07d40d9f3c977ea5d257c8281cc51a2f7b53144bcbd034300ed1e94e0199`，与 report 引用一致。

| 原始事件/锚点 | monotonic_ns | 解释 |
| --- | ---: | --- |
| event_index=21，writer match=1 | 296490116622450 | reader 创建调用返回前已观察到匹配 |
| event_index=23，reader `created_monotonic_ns` | 296490117369511 | create_subscription 返回后的创建锚点 |
| event_index=32，reader match=1 | 296490158852365 | reader 侧随后观察到匹配 |

writer match 比 reader 返回锚点早 **747,061 ns = 0.747061 ms**。raw 的 delivery_contract 明确期望 `[2,3,4,5,6,7]`，六条 post-match receive 的 send_sequence 也完整为 `[2,3,4,5,6,7]`，包括 sentinel=7；不能把这一工具匹配时序误判归为 SUT 丢交付。该证据解释主agent的 D013 纠错，不用于将原报告 unknown 改成 pass。

## 后续处置

D013 要求 reader 创建返回之后重新采集新鲜的双向 match snapshots，并由 raw-backed observation receipts 证明其时序、端点身份与同一次运行。不得倒填旧 match 时间、用交付结果补造 match receipt，或放宽 unknown 门槛。

runtime/campaign 补丁由相应agent完成；文档子agent本轮不暂存、不提交。主agent第二次 GO 后才可按新派遣提交；新 source identity 下重新运行所需矩阵，原 report/raw/ledger 保持 immutable，不能覆盖为新结果。不改 SUT rule、target revision 或 18/72 分母；旧报告仍为未封存历史。

ACK修复进展（2026-11-15T00:14:58+08:00，Asia/Shanghai）：reader创建返回后发出本轮snapshot请求；两端各有唯一、命令关联的新鲜match_snapshot及command_done ACK，且matched_count为整数1，才可打开post_match_delivery窗口。此ACK是snapshot命令完成回执，不是DDS交付ACK。raw须满足request→snapshot→ACK→window_open的事件顺序，以及reader创建/请求≤采样≤snapshot≤ACK≤窗口开启的单调时间关系；核对角色、API、command_id、GID/generation及worker身份。snapshot和ACK分两次stdout读取也必须等齐；缺失、重复、提前ACK或不匹配不得放行，不得退回旧match事件。 主agent独立144 tests / 37.032s / OK、diff --check和ACK逻辑审查通过，已授权本次第二commit。实际新HEAD见git log及source_manifest；本文原始run身份、unknown、seal=false及证据hash保持不变，不能由修复后的离线pass追认原run通过。
