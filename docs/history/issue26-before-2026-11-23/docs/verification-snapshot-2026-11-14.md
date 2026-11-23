# 主agent验证快照与证据边界

日期：2026-11-14。来源：主agent独立验证/派遣消息；文档子agent只读核对现有产物，不重新执行测试、网络控制、ROS 或 campaign。以下是开发阶段快照，不表示完整 S1 或 per-target seal 已通过。

## First / second在11ec76c版本上的正式通过

2026-11-15T00:32:53+08:00（Asia/Shanghai）：[正式证据快照](formal-target-snapshots-2026-11-15.md)完整保留四次attempt。first r1/domain180、second r2/domain80各18 required judged pass、6 negative judged fail、四control6/6、unknown0，独立verify sealed=true/errors=[]/rc0；source为11ec76c，target_revision均0。两次通过run共48份raw hash均按各report核对。

旧first false和second domain181 preflight blocked不改。second r2另24份capsule的replay_attempts/execution_history/non_replayable亦全0，不因矩阵成功而补出F07 replay。主agent已解冻third开发：后续source改变后以上seal只能支持原版本，最终source须rerun。first/second target manifest保持不变，精确legacy兼容不豁免source校验；第三/第四初版schema声明与合同修正未证明new rule完成。

## 前一时点的first rerun验收账更新

2026-11-15T00:27:18+08:00（Asia/Shanghai）：见 [完整S1逐项验收账](s1-acceptance-ledger.md)。主agent确认且文档子agent只读核对first rerun report/verify记录：当前HEAD `11ec76c249cb75605fd70c5d7efb5610e70514b1`、revision0，18 required judged pass、6 negative judged fail、四control6/6、unknown0、独立sealed=true/errors=[]/rc0。文档子agent未运行verify或ROS；24份raw hash逐一匹配report。首次失败历史以下保留，不覆盖。

第二target domain181尝试已有独立report及run/verify rc1，24行runtime端口preflight blocked/real_execution=false，不能计完成。F06零候选审计路径由主agent认可已执行，自然admission/triage仍未获真实异常；两次第一target48份capsule的replay_attempts、execution_history、non_replayable均0，已只读复核。F07健康及受控工具失败真实重放仍缺，不能把首跑失败保留当成replay failure。

## 首次正式运行历史进展

本次记录时间：2026-11-15T00:01:03+08:00（Asia/Shanghai，TZ=Asia/Shanghai date --iso-8601=seconds实测）。

第一份正式 report 已产生，详见 [首次正式矩阵记录](first-formal-matrix-2026-11-14.md)：domain180、target revision0、source HEAD `21f0e2679703e366463c35e6d01c2be30fcec919`；24条真实运行完成，required 17 pass+1 unknown，六条fault按预期fail，controls为health6/mutation6/fault6/transparency5，seal=false。主agent独立verify rc1。原报告immutable；D013工具时序修复待新GO提交/冻结并重跑，不改原结果。下文离线测试与开发smoke保留各自历史身份，不能混入本次正式计数。

## 离线合同测试

主agent报告执行：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -m unittest discover -s tests -q
```

历史快照：工作目录 `/home/wuyuqa/ros2-dds-fuzz-v2`；结果 **112 tests / 4.232s / OK**。文档子agent未复跑，也未获得该次测试的独立日志路径或测试时源码 digest；不编造。此数字仅为该次离线合同总数，不是 112 次真实 ROS 执行，不证明 18-cell、四类各六 control 或完整 S1 达成。代码继续开发后，其他测试总数或耗时应另记新快照。

首次冻结前历史快照：主agent独立验收报告 **131 tests / 13.485s / OK**，并明确 negative+identity 集成已通过。文档子agent未复跑；该次独立日志路径与测试时 source digest 未随消息提供，不补造。上面的命令保留为已知离线测试入口，131 项结果按本次派遣消息记录。此结果仅证明报告范围内的离线合同/集成检查，不计真实 S1 required/control matrix 或 seal 完成。历史 112 项结果保留，不能把两次数量相加。

第二次冻结前最新快照（2026-11-15T00:14:58+08:00，Asia/Shanghai）：主agent独立全仓 **144 tests / 37.032s / OK**，diff --check通过、ACK逻辑审查通过，已下达本次第二commit最终GO。runtime已报告ACK补丁freeze-ready；文档子agent只读核对 `wait_for_fresh_match()`、worker snapshot响应及adapter的request/snapshot/ACK/window绑定，没有复跑测试。独立测试日志路径与测试时完整source digest未随消息提供，不补造。144项仅为离线合同/回归验证，不能改写首份formal的unknown/seal=false，不能替代修复后新HEAD的真实矩阵。

## 真实 non-DDS 网络载体控制

主agent原结果：`/tmp/r2f-discovery-network-control.json`。文档子agent读取时 SHA-256：`87e3acecd4c4ece2168fb815fd45f6d3f5d74db1144240e25430720e3628e8ca`。

| 项目 | 文件内结果 |
| --- | --- |
| 总状态/范围 | `ok=true`；`scope=carrier_control_non_dds` |
| 健康阶段 | sent=70，received=70 |
| drop 阶段 | nft counter=33 packets；blocked=33，received=37/70 |
| restore 后恢复 | received=70/70；recovery_verified=true |
| 证据边界 | `ros2_execution=false`；`s1_execution=false`；`host_network_unchanged=true` |

这是实际隔离网络内 UDP carrier control，证明本次 non-DDS 控制路径的丢弃/恢复可观察。它没有证明 DDS discovery 报文命中、真实 ROS endpoint-attached carrier、第三 target 的匹配/恢复或 semantic oracle。`recovery_confirmation` 的命名不能覆盖文件中明确的 non-DDS 范围。

归档更新：已只读核对 `/home/wuyuqa/ros2-dds-fuzz-v2/results/network-control/2026-11-14-carrier-control/raw.json` 与 `/tmp/r2f-discovery-network-control.json` 逐字节一致，SHA-256 同为 `87e3acecd4c4ece2168fb815fd45f6d3f5d74db1144240e25430720e3628e8ca`。文档子agent未写 results。归档不改变 non-DDS 证据边界；results 保持 untracked，首次冻结显式 staging 排除。

## 双 RMW runtime smoke

主agent报告两 RMW 健康与 negative smoke 已正常。下表是文档子agent当次实际找到并核对的 contract-smoke；`runtime_status=completed`、`real_execution=true`、两个 worker returncode 均为 0。每行仅是开发 smoke，未按 required repeat 0/1/2 补齐，不计作已通过完整矩阵。

| run_id / results/runtime-smoke 子目录 | RMW | arm | raw SHA-256 |
| --- | --- | --- | --- |
| `cyclone-baseline-contract-v1` | `rmw_cyclonedds_cpp` | `baseline` | `4509823bcbbdfb714b185ae3108be15155697649be44c849fa331a2c62070e98` |
| `cyclone-mutated-contract-v1` | `rmw_cyclonedds_cpp` | `mutated` | `7794e85a88a5aadda76e57511dfbc00cd1cdad8cea04060617123151de6eeacd` |
| `cyclone-negative-contract-v1` | `rmw_cyclonedds_cpp` | `negative` | `7b45a93785c877c9eb4c1f534490893fea45aede0760c2b55b706093ceaf98b5` |
| `cyclone-transparent-contract-v1` | `rmw_cyclonedds_cpp` | `transparent` | `6d6736ef1a0a7b68702baaf98760afb61a8536d688fb2ae8fb0de45e0990b179` |
| `fastdds-mutated-contract-v1` | `rmw_fastrtps_cpp` | `mutated` | `1ad2442be09418a8ec74bc46b0f05837c006c56cf46146f28fd2a056c0338f01` |
| `fastdds-negative-contract-v1` | `rmw_fastrtps_cpp` | `negative` | `68d7627b1b8a1f4131cec8e10c9ac29adcd1f3c7cc18253b559872515f085798` |

原始产物路径模式：`/home/wuyuqa/ros2-dds-fuzz-v2/results/runtime-smoke/<run_id>/runtime_result.json`、`execution.json`、`environment.json`、`raw.jsonl`。这里只检查已有文件和 raw hash，未重跑、未重算完整 oracle/chronology/seal。上述两个 negative 产物均记录 harness omit 中间 seq=5、sentinel_received=true，归因仍是 harness control，不是自然 SUT delivery loss。

## MessageInfo 身份观察限制

主agent runtime 已核实本机 Jazzy `MessageInfo` 没有 `publisher_gid`。上述 smoke 的文件字段一致为：

```json
{"identity_basis":"endpoint_correlated","publisher_gid_status":"unavailable_api","publisher_gid_consistent":null,"endpoint_identity_correlated":true}
```

关联依据是 bounded endpoint graph、endpoint lifecycle、独立进程、run payload 与序列等实际观察；它不是 callback 对每个 sample 直接返回的 DDS writer GID。direct per-sample GID 尚未达成，须在 facts、report、candidate provenance 中保留此限制，不填成 true、不丢弃限制，也不把缺字段写成 SUT mismatch。

只有确实拿到可比较的 direct GID 且与预期不符时，才可以讨论 direct mismatch；`unavailable_api` 与 `mismatch` 是不同观察结果。endpoint_correlated 是否足以满足某条 identity/generation 规则，应由该 target/rule 的合同和证据强度明确裁定，不凭 smoke 自动提升 evidence tier 或完整成熟。

## 首次冻结前的工程更新

`source_frozen` 和当前完整 source identity 比较已有实现。主agent最终 GO 已确认所有代码agent freeze、131 项测试通过，并派遣文档子agent执行首次显式 staging/commit。实际提交状态与 source HEAD 见 `git log` 及运行 manifest；本页在提交前编写，不提前宣称提交或 source_frozen 检查成功。现有 smoke 来自提交前开发快照，保存为历史，不能仅靠首次 commit 为其补出冻结来源身份或直接用于新 HEAD seal。首次提交采用 D011 指定的单次自动化身份；首次正式 matrix 期间固定 HEAD，文档不追加 commit。

## 旧开发 instrumentation：有限字段 pass，扩展字段 unknown

本次记录时间：2026-11-15T00:01:03+08:00（Asia/Shanghai，TZ=Asia/Shanghai date --iso-8601=seconds实测）。

来源为真实已有开发run与其追加分析，文档子agent只读核对文件内容/hash，未重跑pair：

- 原run：`vendor-dev-20261114T153207-7cc731c0`，`rmw_cyclonedds_cpp`，domain174。[原report](../results/instrumentation-smoke/vendor-dev-20261114T153207-7cc731c0/report.json) SHA-256：`68bb6e9bd34d3d285333b7b5bd69668a7680e626961c7cfb1ff7d3349000f6d7`。
- [追加分析report](../results/instrumentation-smoke/analysis-20261114T153447-8816dc33/report.json) SHA-256：`a9810955d65c06458a09ce9560233b6e80c424f94368c28981448a421f4bfc77`；`reexecuted_pair=false`，`development_only=true`，`internal_event_witness=true`，vendor_event_count=18。
- `limited_projection.verdict=pass`：GUID/role及reliability、durability限定字段对账；`extended_projection.verdict=unknown`：两端graph的deadline/liveliness缺失，共四项unknown，mismatches为空。不能把limited pass写成完整QoS对账通过。
- 原collector把atime变化误判为文件变化，漏收writer；原report保留collection_errors，追加分析只重读保留raw，不覆盖原结果。工具采集失败不归因SUT。
- `control_task.status=not_run`，candidate_admission=not_performed，sealed=false，target_maturity=not_evaluated。blackbox清理检查不等于blackbox pair已运行。

该开发证据不计第四target正式执行或seal。Cyclone effective trace中的history/depth不证明wire PID presence；D014首次字段适用性定义不追改旧extended unknown为pass。计划及16个本地source hash的只读核对见D014。

## 被动SEDP attempt2开发探针与主agent补充核查

记录时间：2026-11-15T00:38:53+08:00（Asia/Shanghai）。主agent报告、文档子agent只读核对既有JSON与pcap hash；未重新运行probe/parser、ROS或测试。

| RMW / domain | 实际证据 | 结果与字节hash |
| --- | --- | --- |
| Cyclone / 184 | [netns report](../results/instrumentation-feasibility/sedp-probe-20261115/attempt2/184-rmw_cyclonedds_cpp/netns-report.json)、[wire observation](../results/instrumentation-feasibility/sedp-probe-20261115/attempt2/184-rmw_cyclonedds_cpp/wire-observation.json)、[pcap](../results/instrumentation-feasibility/sedp-probe-20261115/attempt2/184-rmw_cyclonedds_cpp/normal-rtps.pcap) | status=ok；56 parsed events、errors=[]；wire JSON SHA-256 `2a2741a982f9a9f4d604cd778fdbe3801a8d79dc30cd6847b4b396d314756212`；pcap `03dc53bb45b1a05462e35a9310cdada8a45f839acbc8019a1cde805495e2cbb0` |
| FastDDS / 185 | [netns report](../results/instrumentation-feasibility/sedp-probe-20261115/attempt2/185-rmw_fastrtps_cpp/netns-report.json)、[wire observation](../results/instrumentation-feasibility/sedp-probe-20261115/attempt2/185-rmw_fastrtps_cpp/wire-observation.json)、[pcap](../results/instrumentation-feasibility/sedp-probe-20261115/attempt2/185-rmw_fastrtps_cpp/normal-rtps.pcap) | status=ok；48 parsed events、errors=[]；wire JSON SHA-256 `60a722403ade1332d8024c71f2c49ed5b69e7ae78a13b3453719cf0e1d2469c8`；pcap `3bb6c0d7f5a53f63d13a01d5dbacfce0bfe078a42269bab2ed46cb7275dbc275` |

[总probe report](../results/instrumentation-feasibility/sedp-probe-20261115/attempt2/probe-report.json) SHA-256 `f711951e0b6e9984581a49d164b72d0f29275d803c4a5b738bccf22abc72b0e8`；development_only=true、sealed=false、candidate_admission=not_performed；两run退出码均0。default policy仍待主agent后续裁定，不能用配置补缺失字段。56/48是该解析产物event数量，不当作去重后的endpoint数量、24/72 required格或mutation hit计数。

该结果证明本次正常被动SEDP捕获/解析可行；wire公告不等于对端vendor已接收安装，缺省省略、完整projection/controls、replay及正式seal仍须独立证明。不计四target成熟或SUT candidate。原attempt1的JSON序列化失败与owner preflight阻断继续保留，不以attempt2覆盖。

主agent另报告只读逐字段Python比较：开发开始后的当前S1Module first/second target.to_dict()与两份11ec76c正式manifest完全一致（true）；这是主agent的核查结果，文档子agent未重跑该比较。source已dirty，target相同不豁免source身份变化后的最终rerun，也不允许沿用旧seal作当前成熟声明。

main定位domains.scan_processes原来将旧repo字符串当substring，误判共享路径前缀的新v2绝对脚本路径；runtime正在修真实路径边界。本轮只读已见参数路径边界匹配及cwd父子关系判断代码，但未复跑其回归，不宣称修复已完成。旧repo真实子路径、cwd/owner/locks保护不放宽；也不把该路径误判直接当成domain181端口阻断的已证根因。

## 本轮文档 freeze-ready 收束

记录时间：2026-11-15T01:09:04+08:00（Asia/Shanghai）。来源：main最新工程进展及独立测试报告，由本轮消息提供；文档子agent未复跑测试或运行ROS，未取得本条消息对应的新增产物路径，不补造路径/hash。

- third v4 两RMW runtime+ingest 已报告 judged/pass，但 `capsule_execution=null`，输入为 `reconstructed_input`；仅为development证据，不能写成真实capsule驱动或capsule执行链已贯通，更不能替代正式18+6/control/seal。下一步仍需真实capsule-driven neutral/fault执行及其独立raw、输入/执行身份、receipt和归因证据。
- main独立新增20项replay测试、31项third runtime/campaign测试通过；这些是离线测试结果，尚未取得真实replay证据。F07真实健康重放与可控工具失败重放仍待验收，原始run保持不变。
- Phase2 bridge正在新增；接口或合同实现不能替代真实trace、lifecycle及blackbox对照，Phase2→Phase4闭环仍有缺口。
- 当前用户2–3个真实SUT候选目标仍未达成：实测自然候选0，不以control、工具错误或开发pass充数；72格成熟分母不变，旧source seal仍只支持旧source。
- 本轮文档已整理为freeze-ready，含义仅为可交main审阅；不代表源码已冻结或S1已成熟。本次不暂存/commit。交付后停止文档轮询和自动追加，等待main下一里程碑明确派遣。
