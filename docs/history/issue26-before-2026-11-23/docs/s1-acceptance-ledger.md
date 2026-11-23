# 完整 S1 逐项验收账

当前逐项验收入口（2026-11-15）：见[c884437完整验收表](s1-final-acceptance-2026-11-15.md)。HEAD `c884437be86c2082d449bafade8185c132138955` / behavior `ceaee506bc189a6de9764bbcdd64c6c6753ed782a92c550e40d86b98b6e39205`：统一provider/carrier/fact/oracle已真实接线；新formal四seal、72required pass+24negative预期fail、unknown0/192worker rc0；新F07健康与受控失败、Phase2→4真实zero分支已执行。QoS72原辅助rc1/71of72保留，main已按独立旁证接受72既有观察链。seconds-edge三配置另有分层72格/144不同worker实证：首24原rc1保留并获独立合同旁证，后48新执行及独审通过，不称原首attempt整体通过。自然/instrumentation/confirmed仍0，非空生命周期及2–3自然候选目标未达，不标完整S1/goalcomplete，不进入S2。147及更早source均为历史，seal不迁移；主agent公共API执行边界不是原任务书原话。

main接续历史说明（以下保留main原文；403与“最终逐项验收尚未完成”均为其当时记录，以本页当前入口更新后续状态）：

> 当前接续状态（2026-11-15）：见[主agent接续审查与执行故障记录](main-supervision-2026-11-15.md)。147af87正式四target各seal=true（72 required pass、24预期negative fail、unknown0）；F07及Phase2→4真实zero-candidate分支已有证据，旧文中的待验收描述保留为历史。自然/confirmed候选仍0，最终逐项验收尚未完成；指定6.1-sol子agent服务返回403余额不足，具体接续工作与需用户后续处理事项已记录。

历史首轮核对时间：2026-11-15T00:24:53+08:00（Asia/Shanghai）。来源：原任务书、主agent工程裁定/验收派遣消息、文档子agent只读核对的本地文件。仅更新文档工作树，不暂存/commit，不启动ROS/campaign；当前无需要用户确认的事项。

目标口径纠正（2026-11-15T00:45:07+08:00，Asia/Shanghai；来源：本轮用户明确指令及主agent审阅意见）：当前用户目标为“S1模块完全成熟且有2–3个真实SUT候选”。已记录的真实自然候选为0，因此候选数量目标尚未达成。自然数量无法预先保证，只能由真实运行产生；负控、工具错误和合成案例不得凑数。原taskbook允许zero-candidate推进Phase，不代表用户完整目标完成；可以交中间报告，但不能据此将整个goal标记complete。

## F01–F08当前映射（c884437）

当前逐项证据见[完整验收表](s1-final-acceptance-2026-11-15.md#framework)，旧11ec/E01–E10与147按历史source解释。

| 能力 | 当前结论 / 具体剩余 |
|---|---|
| F01 | [Registry/Module](s1-final-acceptance-2026-11-15.md#interfaces)注册targets与ExecutionServices，四target真实执行 |
| F02 | [SemanticTarget及prepare](s1-final-acceptance-2026-11-15.md#semantic)生成确定性层与最终输入，scenario/config/capsule实际绑定 |
| F03 | [每state](s1-final-acceptance-2026-11-15.md#states)requires/before-after/raw chronology已证明，失败前置未实触仅合同 |
| F04 | [prepare→accepts→apply](s1-final-acceptance-2026-11-15.md#interfaces)已实际供给campaign/replay；24mutated hit/effect，原147缺口关闭 |
| F05 | [FactProducer→Oracle](s1-final-acceptance-2026-11-15.md#framework)统一服务实际进入ingest/audit，96raw/facts/primary；普通异常持久化，非SUT |
| F06 | [真实zero-ledger](s1-final-acceptance-2026-11-15.md#candidate-replay)及control排除，非空自然admission/triage/确认无对象 |
| F07 | [97健康/98受控失败](s1-final-acceptance-2026-11-15.md#candidate-replay)真实attempt，实际3worker/原340保留；自然candidate replay未实测 |
| F08 | [当前report/独审/清理链](s1-final-acceptance-2026-11-15.md#evidence)已证明，非完整用户goal |

## 四target当前固定分母与控制账（c884437）

revision0/domain96，两RMW×baseline/transparent/mutated×repeat0/1/2=每target18required，另6独立negative；control引用不重复计执行/分母。

| target | 本source确证 | required / negative | 四control / unknown / seal |
|---|---|---|---|
| discovery.late_endpoint | [report](../results/final-validation/batch-20261115T082508Z-430eccee/01-discovery.late_endpoint/campaign/report.json) / [verify](../results/final-validation/batch-20261115T082508Z-430eccee/01-discovery.late_endpoint/verify.result.json) / [cleanup](../results/final-validation/batch-20261115T082508Z-430eccee/01-discovery.late_endpoint/cleanup.json) | 18pass / 6预期fail | 各6/6 / 0 / true，仅c884437身份 |
| discovery.late_join_delivery | [report](../results/final-validation/batch-20261115T082508Z-430eccee/02-discovery.late_join_delivery/campaign/report.json) / [verify](../results/final-validation/batch-20261115T082508Z-430eccee/02-discovery.late_join_delivery/verify.result.json) / [cleanup](../results/final-validation/batch-20261115T082508Z-430eccee/02-discovery.late_join_delivery/cleanup.json) | 18pass / 6预期fail | 各6/6 / 0 / true，仅c884437身份 |
| discovery.interruption_recovery | [report](../results/final-validation/batch-20261115T082508Z-430eccee/03-discovery.interruption_recovery/campaign/report.json) / [verify](../results/final-validation/batch-20261115T082508Z-430eccee/03-discovery.interruption_recovery/verify.result.json) / [cleanup](../results/final-validation/batch-20261115T082508Z-430eccee/03-discovery.interruption_recovery/cleanup.json) | 18pass / 6预期fail | 各6/6 / 0 / true，仅c884437身份 |
| builtin.discovery_projection_reconciliation | [report](../results/final-validation/batch-20261115T082508Z-430eccee/04-builtin.discovery_projection_reconciliation/campaign/report.json) / [verify](../results/final-validation/batch-20261115T082508Z-430eccee/04-builtin.discovery_projection_reconciliation/verify.result.json) / [cleanup](../results/final-validation/batch-20261115T082508Z-430eccee/04-builtin.discovery_projection_reconciliation/cleanup.json) | 18pass / 6预期fail | 各6/6 / 0 / true，仅c884437身份 |

[Ptolemy独审](../results/main-review/formal-c884437-audit-20261115T084136Z-eca41e07/summary.json)核72pass+24negativefail/96raw-capsule/192raw-backed worker rc0；first2正常36格blackbox_observed/errors=[]，third24drain/6mutated hit，fourth24 readiness/fresh graph/prefix/cutoff。1848文件前后hash一致，main已接收release；旧失败/unknown/seal不改。

## 当前Phase、跨项边界与主决策

| 项目 | 本source实际结论 / 未完成能力 |
|---|---|
| Phase0/1 | [统一接口链](s1-final-acceptance-2026-11-15.md#interfaces)已落地，原架构缺口关闭；输入/状态合同与实际S1运行分别有据 |
| Phase2/4 | [26pair/52rc0](s1-final-acceptance-2026-11-15.md#modes)，内部事件+同输入clean+bounded24/三verify0/全JSON0diff；真实zero分支通过，原非空admission/triage/candidate六格validation仍无对象 |
| Phase3/5A/5B/6/7 | [四target](s1-final-acceptance-2026-11-15.md#targets)固定顺序/四seal；不因zero推完整生命周期，不增72分母 |
| F07 | 两规定attempt已执行、原340不变；实际3worker（失败reader未创建），不是所有worker全0或自然candidate确认 |
| QoS补参数 | [原执行与旁证](s1-final-acceptance-2026-11-15.md#next)三官方seal/144rc0；原aux71of72/controller1保留，main按独立72of72旁证接受既有链，不改旧summary |
| source/锁/ports/清理 | [阶段release](s1-final-acceptance-2026-11-15.md#evidence)绑定结束状态；存在锁未持有/无活owner，未用路径不存在，本次不probe |
| Phase8/goal | 原目标内非空生命周期/候选级validation与用户2–3目标未达，候选0，S2不进；[seconds-edge](s1-final-acceptance-2026-11-15.md#seconds-edge)已分层完成，后续仅读覆盖、无新计划/ROS授权 |

下列均为历史时点原文，不作c884437当前未完成清单；主agent公共API边界及新参数准备是工程裁定，不冒充原规格原话。main监督文件未改。

## 历史文档 freeze-ready 收束（当时状态）

记录时间：2026-11-15T01:09:04+08:00（Asia/Shanghai）。来源：main最新工程进展及独立测试报告，由本轮消息提供；文档子agent未复跑测试或运行ROS，未取得本条消息对应的新增产物路径，不补造路径/hash。

- third v4 两RMW真实development runtime+ingest均为 `judged/pass`：domain173的FastDDS/Cyclone分别观察到614/162个drop packet，restore后seq 2–7均发送并接收；但输入是 `reconstructed_input`、`capsule_execution=null`，因此只记开发证据。正式D015与18+6/control/seal仍未验收；后续仍需capsule-driven neutral/fault及完整正式矩阵。
- fourth已实际产生24行run：17 required pass、FastDDS `transparent.0` 1 unknown、6 fault行fail，transparency control 5/6，seal=false。该unknown及失败保持原样，不升级为成熟证据；D014和正式18+6/control/seal仍未验收。
- main独立新增20项replay测试、31项third runtime/campaign测试通过；这些是离线测试结果，尚未取得真实replay证据。F07真实健康重放与可控工具失败重放仍待验收，原始run保持不变。
- Phase2 bridge正在新增；接口或合同实现不能替代真实trace、lifecycle及blackbox对照，Phase2→Phase4闭环仍有缺口。
- 当前用户2–3个真实SUT候选目标仍未达成：实测自然候选0，不以control、工具错误或开发pass充数；72格成熟分母不变，旧source seal仍只支持旧source。
- 本轮文档已整理为freeze-ready，含义仅为可交main审阅；不代表源码已冻结或S1已成熟。本次不暂存/commit。交付后停止文档轮询和自动追加，等待main下一里程碑明确派遣。

## 历史已验收 source 身份与当时开发边界

历史已验收基准HEAD：`11ec76c249cb75605fd70c5d7efb5610e70514b1`；behavior SHA-256：`c1daa3a066e69229f73c76a3d52123a8090173bca7abd4b56d951eb85539f72e`。相应运行manifest记录source_frozen=true/dirty=[]；主agent现已解冻third开发，四方在独立写集推进，不能把运行时冻结状态当成此后工作树始终冻结。文档不commit，HEAD未变也不免除行为内容校验。

更新（2026-11-15T00:31:41+08:00，Asia/Shanghai）：first r1/domain180、second r2/domain80均在上述source上满足per-target seal：各18 required judged pass、6 negative judged fail、四control6/6、unknown0，独立verify rc0、sealed=true/errors=[]。旧first失败及second domain181预检阻断均保留。**这些通过只属于11ec76c版本，后续source变化须在最终source冻结后重跑；不作为新source的成熟声明，也不等于Phase2/4或完整S1完成。** 详细身份、hash及精确legacy合同见[正式证据快照](formal-target-snapshots-2026-11-15.md)。

`late-endpoint-r1-20261114T161459Z-7094038e`中的r1是attempt名称，manifest的target_revision仍为0；第二target同理。UTC目录原名保留，不因本地已进入2026-11-15重命名。

证据分层：正式证据仅支持其记录source与合同范围内的seal；旧source正式结果仅支持历史结论；真实开发smoke/可行性探针只支持当次有限观察；离线合同测试不代替真实执行。下列“已执行/已通过”均限定具体路径，不把没有异常的结果写成自然candidate已验证。

最终只读源码快照（2026-11-15T00:34:02+08:00，Asia/Shanghai）：HEAD仍为11ec76c，但source_frozen=false，dirty_behavior_paths已有10项，行为摘要为 `0503ce9ed8789db4e292cbba761e6f10d943a7fbc4b22479f59f4ea2aa246bd6`。这与主agent已解冻第三target开发一致，不是本轮文档修改导致；工作树仍在变化，不把此摘要当未来冻结身份。first/second旧source通过仍保留为历史证据，最终source rerun要求已经生效。

## 历史证据索引（E01–E10，仅支持当时source）

| ID | 实际路径与核对结果 | 能证明的范围 / 限制 |
| --- | --- | --- |
| E01 first已冻结版本正式run | [report](../results/s1/late-endpoint-r1-20261114T161459Z-7094038e/report.json)、[manifest](../results/s1/late-endpoint-r1-20261114T161459Z-7094038e/manifest.json)、[candidate ledger](../results/s1/late-endpoint-r1-20261114T161459Z-7094038e/candidate_ledger.json)、[独立verify执行记录](../results/s1/late-endpoint-r1-20261114T161459Z-7094038e.verify.execution.json)、[verify输出](../results/s1/late-endpoint-r1-20261114T161459Z-7094038e.verify.stdout.log) | 24行真实judged；18 pass+6受控fail；四control6/6；source=11ec76c记录版本；verify rc0。文档子agent读取记录，没有重跑verify |
| E02 当前run纵向样例 | [mutated.0 capsule](../results/s1/late-endpoint-r1-20261114T161459Z-7094038e/cells/rmw_cyclonedds_cpp.mutated.0/capsule.json)、[runtime result](../results/s1/late-endpoint-r1-20261114T161459Z-7094038e/cells/rmw_cyclonedds_cpp.mutated.0/runtime/runtime_result.json)、[execution](../results/s1/late-endpoint-r1-20261114T161459Z-7094038e/cells/rmw_cyclonedds_cpp.mutated.0/runtime/execution.json)、[raw](../results/s1/late-endpoint-r1-20261114T161459Z-7094038e/cells/rmw_cyclonedds_cpp.mutated.0/runtime/raw.jsonl) | 真实writer-only前置、mutation七项receipt、后置观察、输入/执行分离；全run24份raw hash均匹配report。样例不代表其他target已实现 |
| E03 首次正式失败历史 | [report](../results/s1/late-endpoint-r0-20261114T155220Z-f03e5741/report.json)、[ledger](../results/s1/late-endpoint-r0-20261114T155220Z-f03e5741/candidate_ledger.json)、[verify rc1记录](../results/s1/late-endpoint-r0-20261114T155220Z-f03e5741.verify.execution.json)、[时序分析](first-formal-matrix-2026-11-14.md) | 旧HEAD `21f0e267…`；17 pass+1 unknown、transparency5/6、seal=false保持immutable。不能因E01通过而改为pass，不能当replay failure |
| E04 second domain181阻断历史 | [report](../results/s1/late-join-delivery-r1-20261114T162102Z-067b2a2d/report.json)、[manifest](../results/s1/late-join-delivery-r1-20261114T162102Z-067b2a2d/manifest.json)、[执行记录](../results/s1/late-join-delivery-r1-20261114T162102Z-067b2a2d.execution.json)、[verify记录](../results/s1/late-join-delivery-r1-20261114T162102Z-067b2a2d.verify.execution.json)、[runtime样例](../results/s1/late-join-delivery-r1-20261114T162102Z-067b2a2d/cells/rmw_cyclonedds_cpp.baseline.0/runtime/runtime_result.json) | domain181、revision0、同当前HEAD；24行runtime blocked/real_execution=false；primary全unknown、四control0/6、seal=false、run及verify rc1。runtime原因是domain_conflict:rtps_port_block；adapter另报state step count差异，二者均保留 |
| E05 真实内部观察开发证据 | [原开发report](../results/instrumentation-smoke/vendor-dev-20261114T153207-7cc731c0/report.json)、[追加分析](../results/instrumentation-smoke/analysis-20261114T153447-8816dc33/report.json)、[独立graph raw](../results/instrumentation-smoke/vendor-dev-20261114T153207-7cc731c0/graph-snapshot.json)、[writer trace](../results/instrumentation-smoke/vendor-dev-20261114T153207-7cc731c0/writer/cyclonedds.log)、[reader trace](../results/instrumentation-smoke/vendor-dev-20261114T153207-7cc731c0/reader/cyclonedds.log) | Cyclone/domain174，18内部SEDP事件，limited pass/extended unknown；reexecuted_pair=false、control_task.not_run、candidate_admission.not_performed、sealed=false；不是当前冻结required证据 |
| E06 网络与第四target可行性 | [non-DDS控制raw](../results/network-control/2026-11-14-carrier-control/raw.json)、[归档manifest](../results/network-control/2026-11-14-carrier-control/manifest.json)、[第四target计划](../results/instrumentation-feasibility/fourth-target-plan.md)、[SEDP探针report](../results/instrumentation-feasibility/sedp-probe-20261115/probe-report.json) | 33 non-DDS packets drop及70/70恢复不计第三target hit。SEDP探针socket_created_bound=true，但JSON序列化失败/raw_saved=false，两个RMW尝试又被owner preflight阻断；不证明真实SEDP已捕获。计划16个本地source hash曾全部核对一致；计划不是实现 |
| E07 离线合同与实现 | [core测试](../tests/test_core_contracts.py)、[audit测试](../tests/test_contract_audit.py)、[campaign合同](../tests/test_campaign_contracts.py)、[evidence测试](../tests/test_campaign_evidence.py)、[snapshot回归](../tests/test_runtime_match_snapshot.py)、[S1合同](../tests/test_s1_contracts.py)、[instrumentation合同](../tests/test_instrumentation.py)、[验证快照](verification-snapshot-2026-11-14.md) | 主agent独立144 tests/37.032s/OK；历史131/112分别保留。文档子agent未复跑，未获该次单独日志/source digest，不编造。测试文件存在及总数通过不替代逐项真实证据 |
| E08 second已冻结版本通过 | [report](../results/s1/late-join-delivery-r2-20261114T162346Z-2642f6ef/report.json)、[manifest](../results/s1/late-join-delivery-r2-20261114T162346Z-2642f6ef/manifest.json)、[ledger](../results/s1/late-join-delivery-r2-20261114T162346Z-2642f6ef/candidate_ledger.json)、[独立verify记录](../results/s1/late-join-delivery-r2-20261114T162346Z-2642f6ef.verify.execution.json) | domain80、revision0、source11ec76c；18 judged pass+6 negative judged fail，四control6/6、unknown0、seal=true/verify rc0；24份raw hash核对一致。原domain181阻断不覆盖 |
| E09 被动SEDP attempt2开发成功 | [总report](../results/instrumentation-feasibility/sedp-probe-20261115/attempt2/probe-report.json)、[Cyclone netns](../results/instrumentation-feasibility/sedp-probe-20261115/attempt2/184-rmw_cyclonedds_cpp/netns-report.json)/[wire](../results/instrumentation-feasibility/sedp-probe-20261115/attempt2/184-rmw_cyclonedds_cpp/wire-observation.json)、[FastDDS netns](../results/instrumentation-feasibility/sedp-probe-20261115/attempt2/185-rmw_fastrtps_cpp/netns-report.json)/[wire](../results/instrumentation-feasibility/sedp-probe-20261115/attempt2/185-rmw_fastrtps_cpp/wire-observation.json) | netns status=ok；parsed events56/48、errors=[]；pcap hash已核对。development_only、default policy pending、sealed=false/candidate_admission=not_performed；不计四target成熟或候选，E06前次失败不覆盖 |
| E10 third/fourth历史run证据 | [third v4 summary](../results/runtime-smoke/third-development-v4/summary.json)、[FastDDS ingest](../results/runtime-smoke/third-development-v4/fastdds-ingest-development.json)、[Cyclone ingest](../results/runtime-smoke/third-development-v4/cyclone-ingest-development.json)；[fourth report](../results/s1/builtin-projection-r0-20261115T050653+0800/report.json)、[manifest](../results/s1/builtin-projection-r0-20261115T050653+0800/manifest.json) | third两RMW真实development运行及restore交付观察存在，但ingest为`reconstructed_input`且`capsule_execution=null`，不能证明正式D015验收。fourth 24行中17 required pass、1 unknown（FastDDS `transparent.0`，原始记录保留；main重算为缺少graph快照对应writer GUID）、6 fault fail；transparency 5/6、seal=false。两者均是not mature状态，不改变72 required分母或first/second旧source边界 |

本次状态同步（2026-11-15，来源：用户最新指令及本地只读文件核对）：third summary字节SHA-256=`52d7ab0a8c87ae279c2c26d533a9d7a7df6c2b815d3d04651b3f7ba793800f77`；fourth report字节SHA-256=`73487d00d4ca9393712089544cc197443e41a21d5c0ede4e3628a77f26fe86b2`。main重算unknown为`no_live_wire_endpoint_at_graph_snapshot:010f7f01b56a3f980000000000000603`；不覆盖原report/raw/pcap，不把观察缺口作为自然candidate。

E01文件字节SHA-256：report=`3d1556ff3a78285b54b3c88cb185a5c3c7fba36f1d786fb15b180d0da5deaf4c`；manifest=`b55e127f9aba78d1974df4ec418a076c4fcea4bac33314b2ff30a56548481f2b`；ledger=`c712dec3857183a6d04fc39f3f1ca9a72439c01341a04c3dc6e7f1e15e094322`。E03 report字节hash为`844d064b88b826022d0c5982fea80aefa59910848c0027cc5be4e6653110c272`。E04 report字节hash为`dcffe78ebf7074a531682b7b0cef397123e1468609b83c08d3ae8bb9cf704200`。文件字节hash与report内部规范化摘要分开。

## 历史F01–F08映射（11ec及当时开发状态）

全部八项都须离线合同；F01–F06/F08还须真实pair集成。F07须覆盖离线input、真实execution和replay failure保留。以下证据映射不把测试数量当验收结论。

| 项目 | 当前实际证据与实现入口 | 当前判断 | 缺口 / 下一份必要证据 |
| --- | --- | --- | --- |
| F01 module/target注册 | E01 manifest.catalog/target；[runner](../src/r2f/campaign/runner.py)与[registry](../src/r2f/core/registry.py)；E07 test_registry_is_generic_and_rejects_duplicate | E01/E08在11ec76c上证明first/second统一注册路径贯通 | 四target注册不代表第三/第四target真实carrier可执行；后续各自run不能绕开registry |
| F02 SemanticTarget→scenario | E01 manifest.target/rule；E02 capsule.input、report中state_steps；[scenario](../src/r2f/core/scenario.py)；E07 target/scenario与参数拒绝合同 | E01/E08已证明first/second编译结果在11ec76c上真实使用 | 第三/第四target仍须各自状态/slot/expected effect/receipt映射；E04不用于判定模型不兼容，第二target已由E08补充真实证据；最终source须重跑 |
| F03 requires/state receipts | E02 precondition_receipts和observation_receipts/raw；E01逐行stage_history；E07 steps/forged_reach/ACK回归 | E01/E08 first/second真实writer前置及后置观察已执行于11ec76c | 其他target各自时序仍待验证；第三targetmatched-before-drop不可套用late_joiner writer-only前置 |
| F04 carrier/mutation/effect | E01六mutated行及各raw；E02七项嵌套mutation receipt全true；E07 receipt/hash/neutral合同；[S1模块](../src/r2f/modules/s1/module.py) | late_joiner实际提交/命中/effect及neutral不伪hit已由当前正式run覆盖 | third v4两RMW development runtime/ingest观察到drop及restore后delivery，但输入重建且capsule execution为空，正式D015仍待验收；fourth endpoint_qos_variation已有24行、17 pass+1 unknown、6 fault fail且seal=false；两者均不替代正式target验收 |
| F05 raw→typed facts→primary oracle | E01 rows.typed_facts/oracle及24份raw/hash；[facts](../src/r2f/modules/s1/facts.py)、[oracles](../src/r2f/modules/s1/oracles.py)；E07 residual/unknown/闭合合同 | E01/E08各18 pass及6已知harness fail检出；原tool unknown保留E03 | 第三/四target独立primary/闭合/适用性仍待；first/second不得用旧source结果支持最终新source。control fail不证明自然SUT缺陷 |
| F06 candidate lifecycle与审计 | E01/E08真实zero-ledger、各六control candidate_eligible=false；E03原始失败保留；[lifecycle](../src/r2f/core/candidate.py)、[ledger](../src/r2f/reports/candidate_report.py)；E07 admission/triage/排除合同 | 按主agent工程裁定，零自然候选分支的真实审计路径已执行；不凑候选 | 自然candidate admission/triage仍未获真实异常证据；若未来出现须完整记录转换、raw/provenance、责任与处置，不能拿合成测试或control充数 |
| F07 capsule与真实replay | E01/E03共48份正式capsule；当前run24份input/execution齐备；[replay](../src/r2f/core/replay.py)；E07 fail保留、异常签名比较、fresh domain合同 | input/execution持久化已证明；真实replay验收未完成 | 48份capsule合计replay_attempts=0、execution_history=0、non_replayable=0。下一phase需真实健康重放＋可控工具失败真实重放，独立产物与attempt/failure记录，不改原capsule/raw。E03首跑失败≠replay failure |
| F08 完整report及独立审计 | E01/E08 report/manifest/ledger/capsule/raw与独立verify rc0；E03/E04失败报告；[report审计](../src/r2f/reports/run_report.py)；E07 report/ledger篡改合同 | first/second在11ec76c上的完整报告链与seal路径已证明；失败报告亦保留 | 第三/第四target正式报告/seal、完整S1处置与Phase2/4关联仍缺；registry disposition=expressible不取代seal字段，也不能扩写第一target seal |

## 历史Phase2、Phase4及当时顺序

| 要求 | 实际证据 | 当前状态与缺口 |
| --- | --- | --- |
| Phase0/1 初始化、合同框架 | 当前git HEAD、E07、E01纵向真实pair | 已有实现/合同及第一target贯通；不能覆盖未执行路径 |
| Phase2 至少一个RMW源码/内部观察 | E05真实vendor trace、进程provenance/graph与18内部事件 | 内部观察开发证据真实存在；未声称独立源码build插装已完成。需保留插装配置、loaded binary/source/process identity及其适用范围；旧开发run不能直接支持当前source seal |
| Phase2 candidate ledger及black-box任务 | E05 control_task.status=not_run、candidate_admission=not_performed；E01 ledger.instrumentation_status=not_run、instrumentation_candidates=null、linkage=not_linked_to_this_blackbox_run | 尚未形成明确连接的Phase2→Phase4闭环；null不能写成已核实的instrumentation_candidates=0。自然候选不可制造 |
| Phase3 第一18+6及四control | E01，E03历史失败保留 | 当前第一target满足per-target seal；同revision0，新attempt不覆盖旧report |
| Phase4 有instrumentation candidate | 当前未有该类候选；E05仅真实内部观察 | 若发现候选，需同输入两个RMW×3repeat black-box validation，原candidate不因replay失败删除，补责任/原因/disposition |
| Phase4 无instrumentation candidate的bounded blackbox | E01是真实公开API required矩阵，ledger明确未链接instrumentation；E05对照任务未运行 | **不能自动把E01当Phase4已完成。** 下一步需具名bounded campaign的输入/范围/次数或时间上限、隔离blackbox环境、与Phase2关联、明确instrumentation_candidates=0及完整zero-candidate/blackbox disposition |
| Phase5A 第二target | E08 domain80已正式18+6/四control通过，独立seal=true；E04 domain181阻断历史保留 | 通过仅属于11ec76c；不推断181已无冲突，最终source改变后须重跑；不强抢域/旧owner |
| Phase5B 第三target | E06 non-DDS控制、E10 third v4真实development runtime+ingest、D015工程裁定 | endpoint-attached drop/restore已有开发运行证据；但capsule execution为空且输入重建，正式D015及18+6/control/seal仍待验收 |
| Phase6/7 第四target与各target seal | D014字段定义、E09被动SEDP开发证据、E10第四target 24行report | 已执行run但17 required pass+1 unknown、6 fault fail、seal=false；保留unknown/失败，需完成D014正式18+6/control/seal。各target仍按72分母，不删格 |
| Phase8 S2入口 | 无完整S1全部稳定证据 | 不以第一target成功提前宣称S1成熟或进入S2 |

## 历史四target控制账（不作147结论）

每target required=两个RMW×baseline/transparent/mutated×repeat0/1/2=18；独立negative fault=6。health/mutation/transparency可各引用六条required；fault必须独立，不能重复增加执行或成熟计数。四target合计72 required＋24独立fault＝至少96条唯一执行行；历史attempt、replay和开发probe不加进72分母。

| target（均revision0） | 当前证据路径 | required / 独立fault | health / mutation / fault / transparency | seal与缺口 |
| --- | --- | --- | --- | --- |
| discovery.late_endpoint | E01 report，domain180；E03保留历史失败 | 18/18 judged pass；6/6 negative judged fail | 6/6 / 6/6 / 6/6 / 6/6 | 11ec76c版本per-target seal=true，unknown0；E03仍false |
| discovery.late_join_delivery | E08 report/domain80；E04 report/domain181保留 | 18/18 judged pass；6/6 negative judged fail | 6/6 / 6/6 / 6/6 / 6/6 | seal=true仅对11ec76c版本；E04仍false，原始preflight及后续step-count错误保留 |
| discovery.interruption_recovery | E10 third v4 [summary](../results/runtime-smoke/third-development-v4/summary.json)与双RMW ingest；D015 | 两RMW development runtime/ingest均judged/pass；输入为`reconstructed_input`、`capsule_execution=null`，非正式18+6 | 正式四control均待验收 | development evidence / not mature；正式D015、18+6/control/seal待验收，不删18格，72分母不变 |
| builtin.discovery_projection_reconciliation | E09被动SEDP及E10 [24行report](../results/s1/builtin-projection-r0-20261115T050653+0800/report.json) | 17 required pass、1 unknown；6 fault fail | 开发run：6/6 / 6/6 / 6/6 / 5/6，非正式验收通过 | 当前run `seal=false`；FastDDS `transparent.0` unknown及原始证据保留，正式D014与18+6/control/seal未验收，不删18格，72分母不变 |

11ec76c版本已有first/second各18格的per-target seal支持，共36个required格，不是72/72。主agent已解冻后续开发；最终source一旦变化，这36格不能自动转作新版本通过，须最终冻结后rerun。不得通过历史attempt累加、blocked排除或control复用伪造完整成熟。

## 历史跨项验收及当时约束

| 项目 | 当前实际证据 | 剩余验收条件 |
| --- | --- | --- |
| source/revision/hash | E01/E08 manifest均记录11ec76c版本；report/ledger/24raw hash已核对；E03不同HEAD；E04同HEAD | source/HEAD改变后的旧通过只作历史；runtime中身份变化须abort。r1不是revision1，D014/D015不能暗改已封存模型 |
| input identity | E01 capsule.input和effective hash；E07 deterministic receipts/实际payload、domain及campaign_kind排除合同 | 跨domain真实replay的一致性仍须F07实测；execution receipt时间不能污染input hash |
| chronology与ACK | E01 postreader请求→两端snapshot→command_done ACK→post_match_delivery window；E03旧竞态raw保留 | ACK为命令完成回执，非DDS交付ACK；后续target不能只检查receipt存在。第三target须matched-before-drop |
| attribution | E01六negative标harness且不eligible；E03 tool unknown；E04环境阻断 | 自然SUT归因尚无真实candidate；必须区分环境/进程、工具、mutation ineffective和SUT residual，allowed不能吞residual |
| provenance/identity | E01 environment、独立worker身份、raw、graph与payload关联；E05 trace/loaded provenance | MessageInfo无publisher_gid：endpoint_correlated不冒称direct per-sample GID；缺字段非SUT mismatch。第四target需D014独立capture+生命周期关联 |
| locks/隔离与清理 | E01环境/locks receipts；E04域端口preflight阻断；E06探针owner阻断记录 | 不能强抢域或清除旧repo owner。D015父进程须持锁直到整个netns子树清理；该分支尚未验证 |
| replay/失败保留 | E01/E03共48份capsule及E08另24份的replay_attempts、execution_history、non_replayable均0（分别只读复核） | F07健康真实重放及受控工具失败重放均缺；保留独立输入/执行/attempt/failure/provenance。原fail后新pass不确认原异常 |
| candidate/2–3真实候选 | E01/E08自然candidate=0、confirmed=0；E03也为0；E05未admission | **当前用户目标为2–3个真实SUT候选；实测0，该项目未达成。** 自然数量不能预先保证，只能真实运行产生；negative、tool failure、开发projection或合成合同case不得凑数。zero-candidate可支持原taskbook的Phase推进和中间报告，不能等价完整目标完成或据此标记整个goal complete |
| D014字段适用性 | 决策与第四target计划、E09、E10第四target report | 必需GUID/role/topic/type/reliability/durability/deadline/liveliness及lease；history/depth缺失仅supporting `not_applicable_by_endpoint_schema`，禁止配置补值。实际24行run有17 required pass、1 unknown、6 fault fail，seal=false；unknown与失败保留，正式验收未完成 |
| D015第三target恢复合同 | 主agent工程裁定、E10 third v4 runtime/ingest | 两RMW development运行观察到真实drop与restore后交付，但输入重建且无capsule execution，正式D015及18+6/control/seal未验收。原合同不变：wire_proxy＋显式implementation；仅mutated真drop，hit需本pair真实discovery counter>0；删规则不等于恢复，恢复需身份关联discovery＋fresh双向count/graph＋postrestore delivery；独立fault中间omit，不伪drop/applied；72分母不变 |

## 历史下一份证据需求（当前需求见顶部入口）

1. F07健康真实重放和可控工具失败真实重放，独立产物不改原run；当前已有48份capsule不能代替两条真实replay路径。
2. 明确Phase2→Phase4 linkage；无自然instrumentation候选时，真实bounded blackbox及具名zero-candidate disposition，不能用null默认为0。
3. 保留第二target domain181 blocked及domain80通过两份报告；third v4与fourth 24行run作为当前开发/执行证据保留，但third仍缺真实capsule execution及正式D015验收，fourth保留1 unknown和6 fault fail且seal=false；两者仍需按原18+6/control合同完成。first/second已seal revision0 manifest只适用于11ec76c source，最终source仍须重跑；72分母不变。
4. 后续每次source更新都重新评估哪些证据可用于新seal，不回填旧receipt/hash，不减少72分母，不保证候选数量。

以上由主agent日常工程协调，无新增需用户确认事项。此账不是运行调度授权，文档子agent不启动任何campaign。

最新兼容约束：first/second的target manifest不变不等于全局source身份不变；不得因shared helper增加字段而漂移既有target hash。第三/第四首次seal前初版合同修正不等于new rule已完成，不能提前标绿。精确target hash和legacy合同见正式证据快照。

补充核查（2026-11-15T00:38:53+08:00，Asia/Shanghai）：main逐字段比较当前S1Module first/second target.to_dict()与两份11ec76c正式manifest均true；文档只记录来源，不声称复跑。source dirty仍使旧seal不能用于当前成熟声明。main发现scan_processes旧repo substring把v2共享前缀路径误判为旧repo，runtime修真实路径边界；旧repo真实路径/进程/锁保护不放宽，回归验证仍待代码owner。E09已证明正常被动SEDP开发捕获/解析，但默认值策略及完整projection/control/seal尚未完成。


## 本轮冻结前证据账补充（2026-11-15T08:42:44+08:00，Asia/Shanghai；非最终验收）

来源：主agent本轮派遣与工程报告；本执行员仅追加文档状态，未复跑所报业务测试、未启动 ROS、未运行 campaign/replay。此前各节作为各自时间点的历史快照保留，本节不把旧证据改绑到新 source。

| 账项 | 当前记录 | 不能据此宣称 / 后续证据 |
| --- | --- | --- |
| scope / capability / maturity | scope 从 run 动态状态转为静态 capability，maturity 由 per-run report 和独立 per-target audit 给出 | 已注册/实现不等于已执行或成熟；四 target 顺序、每 target 18 required + 6 独立 negative、完整 required 分母 72 均不变 |
| capture 修复 | 主agent报告非标准端口 potential discovery RTPS 现保留 raw；普通 user-only DATA 已安全分类 | 普通 user DATA 不是 discovery hit；此前 metadata-only unknown 保留。保存 raw 不等于已补齐未知字段、实际 drop/recovery 或 projection 合同已通过 |
| observation / execution_input | execution_input 绑定与真实 Cyclone/FastDDS blackbox 兼容修复进行中 | maps 多 segment 和污染 trace 回归尚待 Ptolemy 关闭；不预记 pass、完成或最终 freeze |
| 本轮局部离线验证 | 主agent报告 capture/projection/recovery 89 tests / 5.557s / OK | 仅为该组检查报告，不是最终完整源码、正式 matrix 或 SUT 成熟证据；本执行员未复跑，报告对应的确切命令/日志路径本条未提供 |
| 更早完整离线验证 | 主agent此前报告 334 tests / 48.376s / OK，另报当时 syntax/json parse、diff-check 通过 | 来自更早工作树，不等于最终修复树通过；不沿用为最终 freeze 的证明 |
| source identity | 当前观察 HEAD `e5a1a73921b176e60bba14cb41b9ed7f18c0a538`，行为文件 dirty，未 freeze | 唯一新冻结 HEAD/behavior SHA-256 尚未给定；待 Ptolemy 最终 freeze 及主agent独立审查后追加确切结果 |

当前新 source 验收账：正式四 target **未验收**；F07 健康与可控工具失败重放 **未验收**；Phase2 同 input instrumentation/clean-blackbox pair 与 Phase4 bounded campaign **未验收**。Zeno 已获派准备 26-pair 路径仅记准备状态，不记实际 cell、raw、candidate 或运行 GO。正式四 target driver 已准备也不产生执行证据；无本轮真实 run_id/domain/RMW/raw/replay 可登记。

历史 first/second per-target seal、第四 target unknown 及其他失败均保持原 source 身份与原判；旧 source 通过不进入新 source seal，旧失败不得被新 pass 覆盖。自然 SUT candidate 仍为 **0**，候选数量目标未达成；negative、tool failure、unknown 和合成检查不计自然候选。完整 required 成熟分母维持 **72**，不因 blocked/未运行/未知缩小，也不因 control 引用重复增计。本次不 staging、不 commit；代码 owner 最终 freeze 后再更新本补充的确切验证结果，正式运行仍等主agent明确 GO。


### 最终离线证据补记（不计新 source 正式成熟）（2026-11-15T08:49:44+08:00，Asia/Shanghai）

来源：主agent最终独立审查报告及 Ptolemy 最终 freeze ACK 的转述；本执行员只读核对审计文件，不冒称自己复跑业务测试。[review.json](../results/main-review/freeze-audit-20261115T084636-3cacc8cd/review.json) 记录命令 `python3 -m unittest discover -s tests -q`、returncode=0、diff_check_rc=0，source_before/source_after 及 files_before/files_after 分别一致；[unittest.stderr.log](../results/main-review/freeze-audit-20261115T084636-3cacc8cd/unittest.stderr.log) 实载 **349 tests / 52.658s / OK**（review 的外层命令耗时约 52.898s，与测试器耗时分开）。本执行员追加前核对审计所列非文档 changed 文件 hash 仍一致。syntax/json 解析 OK、首两 target manifest 逐字段精确不变为主agent报告，不冒称 review.json 另有这些字段或本执行员重跑了对应检查。

审查树 behavior SHA-256 为 `75855d72ac9dbc8f36c4cfbab4fd718ce616b012373da09815a24be730227af0`；审计记录的 HEAD 仍为 `e5a1a73921b176e60bba14cb41b9ed7f18c0a538`，`source_frozen=false`，行为文件尚未提交。代码 owner 的 freeze ACK 表示停止源码修改，不等于 Git/runner 的正式 source freeze；唯一新提交 HEAD 及其 `source_frozen=true` 仍须主agent后续明确 staging/commit 指令及实际身份核验，不能预填。此次文档追加会改变三份文档 hash，不回写原审计 files_before/files_after，也不影响其所记录的测试时间点。

最新主agent报告：Ptolemy 的 execution_input / 真实 Cyclone/FastDDS blackbox 兼容、多 segment maps 与污染 trace 回归修复已落盘，最终 freeze ACK 已收到并关闭。其短命 loader probe rc=0：source Jazzy 后仅通过 ctypes 加载 `librmw_fastrtps_cpp`，**无 participant**；rmw/FastDDS/FastCDR 各 5 段 maps，identity_verified、逐段 path/inode/device 均 true，required_groups_verified=true。该 probe 输出仅在 Ptolemy 会话，未另落 results；本执行员未直接复跑或取得独立 probe 文件/hash，不编造路径。这是库装载/映射身份检查，不是 ROS endpoint pair、clean-blackbox 真实运行或 Phase4 通过证据。

本更新关闭前述“待 Ptolemy 最终 freeze ACK / maps 与污染 trace 离线回归关闭”的等待状态；89 项局部检查、334 项更早完整检查保留为历史，最新完整离线结果以上述 349 项审计为准。正式四 target、F07、Phase2 同 input instrumentation/clean-blackbox 与 Phase4 bounded campaign **仍未在新 source 验收**；旧 source seal、unknown 和失败保持原判及原始证据。自然 SUT candidate 仍为 **0**，完整 required 分母仍为 **72**；不据离线验证、loader probe 或 freeze ACK 声称整体目标完成。本执行员未启动 ROS、participant、campaign 或 replay，未 staging、未提交；正式执行及提交均待各自明确 GO。


### f034 正式尝试中止与接线修复待验（2026-11-15）

本次仅执行 `discovery.late_endpoint` revision0（两 RMW、domain80，run_id=`batch-20261115T005229Z-1884a215-01`），source HEAD=`f034662322017e21dac1fdc6d7d3e36da0ce9f46`、behavior=`75855d72ac9dbc8f36c4cfbab4fd718ce616b012373da09815a24be730227af0`；执行及 cleanup 时冻结身份不变。[主agent双 RMW 独立重算](../results/main-review/freeze-audit-20261115T084636-3cacc8cd/live-runner-wiring-gap.json)确认 runner 的 runtime_config 漏传 `input_layer_receipts`，capsule 保有该字段；baseline0 主 oracle pass，但 observation 为 `not_evaluated`，错误为 `observation execution_input differs:input_layer_receipts`。这是工具接线错误，非 SUT 候选，f034 正式成熟未建立。

| 中止与保留证据 | 实际结果 |
|---|---|
| [逐格审计与 raw hashes](../results/final-validation/batch-20261115T005229Z-1884a215/executor-stop-audit.json) | 24 格已计划，22 份 runtime/raw：21 completed（18 normal required + 3 Cyclone negative）及 1 interrupted（FastDDS negative.0，产物记 blocked、real_execution=true）；FastDDS negative.1/.2 未启动；21 份 capsule 有 execution。 |
| 停止与验证 | 核验 PID/start identity 后仅向 driver2062820 发 SIGINT；session44245/driver rc1，runner/verify rc=-2，无 campaign/report.json，独立 verify 未完成。 |
| [cleanup receipt](../results/final-validation/batch-20261115T005229Z-1884a215/01-discovery.late_endpoint/cleanup.json) | ok=true，无残留本 run 进程，44 workers rc0，26 runtime 锁及本地 driver 锁释放；未进入后续 target，旧 batch 不改写。 |

Ptolemy 的 runner/replay receipt 透传修复已落盘；[主agent独立修复树审计](../results/main-review/receipt-wiring-audit-20261115T090035-30551b05/review.json)及测试日志记录 **352 tests / 95.387s OK**（含 96 plan / 96 replay receipt 检查），source 与 4 文件 hash 前后不变、diff-check=0；behavior=`0fd868f4bb14466efe0bf636e22cee6f2069a9cd46ed5920b27a868228524771`。该审计 source_frozen=false，提交、新 freeze 与真实重跑仍待各自 GO。正式四 target、F07、Phase2–4 在新 source 仍待验收，candidate **0**、required 分母 **72** 不变。本轮不提交、不启动 ROS。

<a id="current-capture-snapshot"></a>

### c884437 当前逐项验收快照（2026-11-15）

当前逐项细目见[完整验收表](s1-final-acceptance-2026-11-15.md)。HEAD=`c884437be86c2082d449bafade8185c132138955`、behavior=`ceaee506bc189a6de9764bbcdd64c6c6753ed782a92c550e40d86b98b6e39205`，既有执行各自sourceguard冻结/dirty=[]且前后一致；本轮docs未提交、不改变行为身份。

| 入口 | 本source实际结论与限制 |
|---|---|
| [formal batch](../results/final-validation/batch-20261115T082508Z-430eccee/summary.json)、[Ptolemy独审](../results/main-review/formal-c884437-audit-20261115T084136Z-eca41e07/summary.json)、[final freeze](../results/main-review/formal-c884437-audit-20261115T084136Z-eca41e07/final-freeze.json) | 72required pass+24negative预期fail，四target各四control6/6、unknown0/sealtrue；96runtime/raw/capsule、192raw-backed worker rc0。third24drain/6mutated hit，fourth24 exact readiness→fresh graph→prefix/cutoff；1848文件前后hash不变，main审核退出清理。 |
| [接口实际链](s1-final-acceptance-2026-11-15.md#interfaces)、[唯一full handoff](../results/main-review/provider-integration-20261115T075412Z-9099c293/final-handoff-freeze.json) | 原147 provider缺口已修：S1Module/Registry供给ExecutionServices，campaign prepare→accepts→apply，replay复用原capsule，ingest/audit调用FactProducer/Oracle。428/428 skip0仅作合同支持；原network/exec checker误报按main裁定保留，不说原checker全绿。 |
| [F07 audit](../results/final-validation/provider-f07-launch-20261115T085101Z-7ddda0ab/executor-final-audit.json)、[freeze](../results/final-validation/provider-f07-launch-20261115T085101Z-7ddda0ab/executor-freeze.json)、[main release](../results/final-validation/provider-f07-launch-20261115T085101Z-7ddda0ab/main-window-release.json) | domain97健康pass、98受控fail_environment/runtime_tool_failure/aborted，真实attempt/raw保留；实际3worker（失败reader未创建），原340不变、完整退出清理。非自然candidate确认，正式分母增量0。 |
| [Phase24 summary](../results/final-validation/phase24-refresh-20261115T044110Z-7eee6e2a/execution-provider-20261115t084537z-8cc383f5/summary.json)、[freeze](../results/final-validation/provider-phase24-launch-20261115T085739Z-32708c25/executor-freeze.json) | 26pair/52worker rc0/26raw-capsule，内部事件1+同输入clean1+bounded24，三verify0/完整JSON0diff；真实completed_zero_candidate_branch。原目标内非空admission/triage/逐候选两RMW×3repeat validation未实测；未改源码重编译插装。 |
| [QoS原summary](../results/final-validation/qos-policy-boundaries-20261115T072932Z-656b9c5e/execution-20261115T091455Z-ef0243cb/supplementary-summary.json)、[独立旁证freeze](../results/main-review/qos72-cutoff-adjudication-20261115T093129Z-ebc642cf/freeze.json) | 72cell/144worker rc0、三官方18pass+6negativefail/seal；原aux71/72、controller rc1/incomplete保留。Popper按既有cutoff合同独审72/72链、144worker/完整prepareinput/三official audit与8正反例；main接受既有观察链，原checker额外close_unread0误报不改写，不冒称controller0/重跑后绿。 |
| [seconds-edge分层实证](s1-final-acceptance-2026-11-15.md#seconds-edge)、[首24旁证freeze](../results/main-review/seconds-edge-graph-adjudication-20261115T101357Z-41d95a6a/freeze.json)、[后48独审freeze](../results/main-review/seconds-edge-remaining48-audit-20261115T103246Z-88308ae8/freeze.json) | c884437/ceaee506：原72prepare已绑定；首配置domain102实际24格/48worker，official18pass+6negativefail/unknown0/seal，原aux23/24、controller/inner/outer/finalaudit rc1、final75of81/incomplete保留；main接受既有requested→sample合同24/24链及26正反例。只执行原未运行后两配置的新工具在103/104完成48格/96不同worker rc0、两seal/aux48、六项returncodes0/final153of153。独审将已接受首24旁证与新48分层合并，支持72实际覆盖/144不同worker、三zero-ledger候选0，不称原首attempt整体通过；清理/退出/release已收，无72分母增量。 |

四control引用、replay/Phase24/QoS均不增加72成熟分母；自然/instrumentation/confirmed0，非空candidate生命周期及用户2–3目标未达，S1完整/goal不标complete，S2不进。各自closure/release记录子树/锁/ports/source结束状态，本次不重做live probe；存在锁未持有且无活owner、未用路径不存在，不称所有预期锁曾获取释放。main公共API边界与seconds-edge补参数是工程裁定，不是原规格豁免；seconds-edge已有上述分层实证，后续QoS覆盖研究仅读、未获新计划/ROS授权，见[主决策](s1-final-acceptance-2026-11-15.md#next)。

**147历史单列**：[147 Hooke正式审计](../results/main-review/formal-147-audit-20261115T040939Z-0e90b6fa/summary.json)四seal/72pass+24negativefail、[旧F07](../results/final-validation/f07-ros-execution-20261115T042435Z-7d683e20/summary.json)、旧Phase24及96格supplement仅支持147/28146d；当时统一接口缺口成立。新source已独立重跑，不迁移这些seal，不覆盖旧unknown/失败。下列更早记录均按原时点解释。

#### 历史 source 与修复依据（保留当时结论）

历史正式矩阵 source HEAD=`f57656c4a11546ea88935d3eece176b6e80d1260`、behavior=`0fd868f4bb14466efe0bf636e22cee6f2069a9cd46ed5920b27a868228524771`，执行至 cleanup 身份保持冻结，domain80、两 RMW；[独立中止审计与逐格 raw/capture hashes](../results/final-validation/stop-review-20261115T011711Z-556e7e3a/executor-stop-audit.json)保留全部计数。

| Target / receipt | 本 source 实际结果 |
|---|---|
| [late_endpoint summary](../results/final-validation/batch-20261115T010857Z-c33c6ff1/01-discovery.late_endpoint/summary.json) / [late_join_delivery summary](../results/final-validation/batch-20261115T010857Z-c33c6ff1/02-discovery.late_join_delivery/summary.json) | 各24格：18正常 pass + 6预期 negative fail；runner/独立 verify rc0、seal=true、artifact audit/cleanup=true；正常18格均 blackbox_observed、errors=[]。仅支持本 source。 |
| [interruption_recovery cleanup](../results/final-validation/batch-20261115T010857Z-c33c6ff1/03-discovery.interruption_recovery/cleanup.json) | 7份 runtime/raw：Cyclone baseline/transparent 各3格真实执行失败（observer_final_health_incomplete）；mutated.0 中断 blocked、real_execution=false；其余17格未启动。driver/runner rc1/-2，verify rc1且中止，未形成 report/有效 seal；cleanup=true、12 workers rc0、26 runtime 锁与 driver 锁释放，无残留；第四 target 未启动。 |

上述6格 capture health 均 degraded/unsupported_or_invalid_framing，filtered target-related=0、kernel drops=0；合法非 secure ParticipantMessage/TypeLookup builtin IDs 被误判 unknown_entity，后续另确认 FastDDS statistics footer `0x80` 兼容缺口，均属工具错误。Epicurus [owner freeze ACK](../results/capture-compatibility/fastdds-statistics-20261115T013420Z/freeze.json)保留 builtin 修复，并按[官方 Fast-DDS v2.14.7 header](../results/main-review/fastdds-0x80-source/Fast-DDS-2.14.7/src/cpp/statistics/rtps/messages/RTPSStatisticsMessages.hpp)及[本机 installed headers 编译 ABI](../results/capture-compatibility/fastdds-statistics-20261115T013420Z/abi.json)确认56字节布局；footer 仅接受 vendor `010f`、flags `0/1`、显式长度56、位于末尾且字段/padding合法，其他未知/畸形仍保守，不凭这些兼容消息增加 SPDP/SEDP activity/hit 或 SUT candidate。

[离线重分析](../results/capture-compatibility/fastdds-statistics-20261115T013420Z/report.json)覆盖30 pcap、28,510 frames：known statistics gaps 15,097→0，留存样本剩余 unknown/malformed=0，28,922 typed activities 不变；[官方 decoder 逐字段对比](../results/capture-compatibility/fastdds-statistics-20261115T013420Z/official-decoder-comparison.json)15,097 footer、mismatch=0。[主agent独立历史审计](../results/main-review/statistics-history-audit-20261115T095247-6362cbe6/review.json)确认96旧 artifact hash 不变并核验官方 archive；旧 health/verdict/seal 不改。重分析仅覆盖留存数据，不能补回历史丢失 payload；无真实大端 footer 样本，大端仅有离线测试覆盖。

[owner 测试](../results/capture-compatibility/fastdds-statistics-20261115T013420Z/validation.json)为 capture **44 / 1.545s**、projection/recovery **59 / 5.583s**、full **366 / 71.328s**，均 OK；[主agent独立最终审计](../results/main-review/statistics-final-audit-20261115T095221-e021ac1d/review.json)及[测试日志](../results/main-review/statistics-final-audit-20261115T095221-e021ac1d/unittest.stderr.log)记录 **366 tests / 69.843s OK**、source/两文件 hash 前后不变、diff-check=0、89 Python AST通过，四 target manifest hash 不变。上述 capture 修复随后提交为 HEAD=`b891355705a7e43b420c712bfc336ceb8afb83be`、behavior=`8c87773b0e3c14c91e021e701d63fed7c9dd07df87681e202c581873d138e011`；以下 smoke 执行及 cleanup 时 source_frozen=true、dirty=[]，通过只支持 b891 历史身份。

[b891 历史 development smoke summary](../results/final-validation/capture-compat-smoke-20261115T012416Z-fd137f60/runs/smoke-20261115T015930Z-90a39028/summary.json)与[main独立结果审计](../results/main-review/smoke-outcome-audit-20261115T100430-85f8e97a/review.json)确认 domain80、4 attempts = 3真实 judged/pass + 1启动前 blocked，driver rc1；development=true、not_formal=true、seal=false，不伪造control支持。结果如下：

| b891 smoke cell | 实际证据 |
|---|---|
| third Cyclone / mutated.0 | 真实 drop hit 195；restore身份关联、fresh match/graph、capture health与oracle均通过。 |
| third FastDDS / mutated.0 | 真实 drop hit 420；restore身份关联、fresh match/graph、capture health与oracle均通过；保留2,952个statistics footer及36个other_builtin submessage，未算作额外SPDP/SEDP hit。 |
| fourth Cyclone / transparent.0 | 真实独立wire/graph两端点、18项比较一致，capture health=ok、oracle pass。 |
| fourth FastDDS / transparent.0 | preflight因 `domain_conflict:old_campaign_or_process_owner` 阻断，process_scan为conflicts=[]、unreadable_same_uid_pids=[2191331]；real_execution=false，无ROS workers/pcap/capsule execution，aborted/unknown是工具门结果，非SUT候选。该 b891 smoke 尚无FastDDS真实projection证据。 |

[executor逐格证据与清理](../results/final-validation/smoke-launch-20261115T015930Z-0bcaee9e/executor-final-audit.json)保存raw/pcap hashes、typed facts/oracle及receipt：四格config/capsule确定性input receipts一致，前三格capsule execution已绑定；6个实际workers均rc0，全部cleanup=true、无intervention或残留，每格26项预期runtime锁路径检查通过，已存在锁均未持有且无活owner，未使用路径不存在、ports27400–27649空闲、driver退出、串行窗口已释放，44个未选cell保持计划。[owner closeout](../results/final-validation/smoke-closeout-20261115T020428Z-c125a5b3/closeout.json) SHA256=`7747c1b79d07984633de90b39dabe6e5743a3290c3d6904b380656dca413a2bd`；未重试或改写旧batch。PID2191331归属及本次精确根因无法确认，不能认定属于其他agent。

该轮Ptolemy仅修改 `src/r2f/runtime/domains.py` / `tests/test_runtime_contract.py`，已给出[两文件owner freeze与验证](../results/process-scan-exit-race/20261115T020712Z-5b8d63c5/after.json)（SHA256=`1168ddb908100a86b36787407f0e138d95f646a857d91cf47e536f778bd786bd`）：targeted **32 tests / 0.193s OK**、full **376 tests / 67.170s OK**，测试前后source/两文件hash不变。PermissionError后有界二次核验仅排除已确认退出或同身份Z；存活、不可验证、身份冲突及PID复用继续block。[受控真实/proc交错探针](../results/process-scan-exit-race/20261115T020712Z-5b8d63c5/proc-probe.json)6个child均实际触发PermissionError并正常退出：旧HEAD对persistent live、Z、消失均blocked，新树仅确认Z/消失时排除，persistent live仍blocked。该探针非自然竞态采样，**不证明历史PID2191331归属或本次精确根因**，旧smoke阻断证据不改。

[main独立最终全仓审计](../results/main-review/process-scan-final-audit-20261115T101622-1eb1e9fe/review.json)及[测试日志](../results/main-review/process-scan-final-audit-20261115T101622-1eb1e9fe/unittest.stderr.log)确认 **376 tests / 67.721s OK**、source/4代码测试文件hash前后不变、diff-check=0、89 Python AST通过、四target manifest hash不变。[main在全新目录独立复跑受控/proc探针](../results/main-review/process-scan-probe-audit-20261115T101744-f0fed0eb/review.json)同样6/6真实PermissionError、全部child rc0、无forced cleanup，修复只放行确认Z/gone，live继续blocked；该结果仍不证明历史PID根因。该轮修复及三docs随后提交为 HEAD=`cf507e8210b7326a4f8f4838045028a398770778`、behavior=`b256dd4d2ff8f2dcd9552862db0a538573ce777801bafc99b153bdf6a2044ec1`。[cf507独立smoke审计](../results/main-review/smoke-pass-audit-20261115T022508360Z-014b796a/review.json)确认四格actual/judged/pass、8 workers rc0、cleanup通过；该development证据不计正式成熟。

cf507正式batch为 [`batch-20261115T022650Z-3e2cd9c7`](../results/final-validation/batch-20261115T022650Z-3e2cd9c7/summary.json)，revision0、domain80、两RMW；执行至cleanup时source_frozen=true、dirty=[]。[main独立结果审计](../results/main-review/formal-outcome-audit-20261115T024414000Z-e44427b7/review.json)确认：

| cf507 target | 正式实际结果 |
|---|---|
| [discovery.late_endpoint](../results/final-validation/batch-20261115T022650Z-3e2cd9c7/01-discovery.late_endpoint/summary.json) | 18 required pass + 6预期negative fail；seal=true，runner/verify rc0、audit/cleanup通过，四control各6/6、unknown0；正常18格均blackbox_observed、errors=[]。 |
| [discovery.late_join_delivery](../results/final-validation/batch-20261115T022650Z-3e2cd9c7/02-discovery.late_join_delivery/summary.json) | 18 required pass + 6预期negative fail；seal=true，runner/verify rc0、audit/cleanup通过，四control各6/6、unknown0；正常18格均blackbox_observed、errors=[]。 |
| [discovery.interruption_recovery](../results/final-validation/batch-20261115T022650Z-3e2cd9c7/03-discovery.interruption_recovery/summary.json) | 18 required pass + 6预期negative fail；seal=true，runner/verify rc0、audit/cleanup通过，四control各6/6、unknown0。 |
| [builtin.discovery_projection_reconciliation](../results/final-validation/batch-20261115T022650Z-3e2cd9c7/04-builtin.discovery_projection_reconciliation/summary.json) | 17 required pass + 1 unknown；5预期negative fail + 1 aborted/unknown；seal=false，runner/verify rc1；health/transparency/fault各5/6、mutation6/6，24格capture health均ok，cleanup通过。 |

第四target的FastDDS `baseline.2`因 `no_live_wire_endpoint_at_graph_snapshot:010f7f01fc949e600100000000000604` 判judged/unknown；`negative.1`因 `projection fault original source comparison not healthy` 为aborted/unknown，未满足预期fault control。[完整pcap逐端点只读审计](../results/readonly-review/projection-wire-timing-20261115T024241Z-ad78d35f/report.json)确认：前者缺reader完整GUID `010f7f01fc949e600100000000000604`，后者缺writer完整GUID `010f7f0140a782750000000000000603`；各自整份留存pcap（含16字节GUID直接搜索）均未见对应GUID，不能说公告出现在graph之后，也不能断言未读队列含该缺失公告。prefix、role对应builtin writer活动不能替代实际完整endpoint证据；上述为观察闭环缺口，非SUT候选。

[executor最终审计与逐target清理](../results/final-validation/formal-launch-20261115T022650Z-7b0c97d6/executor-final-audit.json)保留96份真实runtime/raw及capsule execution、95 judged + 1 aborted、192 workers全部rc0，driver rc1且已退出；四target cleanup均true、无intervention/残留，逐target26项预期runtime锁路径检查通过，已存在锁均未持有且无活owner，未使用路径不存在，ports27400–27649空闲、owner锁释放、串行窗口已交还。未重试，原batch的raw/hash、health、verdict及seal均保持不变；前三target seal仅支持cf507身份，整份formal batch未通过。

本次整合审计时，HEAD=`cf507e8210b7326a4f8f4838045028a398770778`、behavior=`28146d394ffdea9bd44b665d130fa80a78a05ca12029c0acfbeff7c165bbef6e`，九文件projection/capture修复尚未提交、source_frozen=false。修复要求实际完整endpoint GUID/role/live wire readiness成立后再取全新graph；读取hash绑定、允许尾部追加的pcap prefix，并将prefix与最终capture证据分开；stop执行有界drain，只有覆盖窗口末尾的 `complete_through` cutoff才允许形成完整观察，超时或覆盖不足继续unknown。[capture prefix/drain owner freeze](../results/observer-tools/prefix-drain-20261115T025326Z/freeze.json)及[Ptolemy四文件最终freeze](../results/projection-endpoint-readiness/20261115T031100Z-final-api-d3fc07ac/freeze.json)保留实现与定向证据，后者 **59 tests / 2.930s OK**（freeze SHA256=`456d23521cfd1afa1728f35dd0e460e04a43219bb58a364f8d42321b3cbbe90d`）。

[最终整合离线审计](../results/main-review/prefix-readiness-integration-20261115T032013Z-78e48c96/summary.json)由main派遣Hooke执行、main读盘复核通过，overall_pass=true：唯一全仓 **407 tests / 90.333s OK、rc0**，90 Python AST通过、diff-check=0；九文件SHA与owner freeze一致，九文件hash、全source inventory/behavior及四target manifest前后均一致。代码owner freeze与最终主审核均通过，没有发现需要改业务代码的问题；该审计未启动ROS或replay，不形成新source的正式seal。提交后behavior应保持上述 `28146d39…` 身份。

以上为147af87提交前的历史修复快照：当时尚待commit与新ROS GO，cf507及更早source的通过不迁移。后续147af87正式四target结果见本节顶部当前快照；旧失败与unknown保留，自然candidate **0**、完整required分母 **72** 不变。


## 2026-11-18 接续离线证据账（HEAD42fcec65 dirty；非新seal）

本节只追加，不覆盖原c884437/e448feb等历史执行、unknown/失败及原验收结论；旧source seal不迁移。详细日志/核证时点/SHA见[2026-11-18接续核证](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md)，main工程裁定见D019。当前HEAD `42fcec65fe66d1dff8dd761bf03ee96a427277c7`、source_frozen=false，代码仍并发修改。

| 证据类别 | 可入账事实 | 不可推出/剩余 |
|---|---|---|
| main修复前快照 | [offline JUnit](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/offline.junit.xml)433项=422pass+11skip，stdout46.56s、exit0；before/after的208行为文件映射相同（非两JSON整体等同） | 不是修复后全仓回归，skip为未启用native/kernel测试，不计pass/真实执行 |
| 既有netem局部 | [netem JUnit](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/existing-netem.junit.xml)87pass/0skip，stdout0.72s、XML0.735s、exit0；已有delay/reorder/duplicate基础可复用 | 与422独立计账，不相加成成熟，也不证明当前DDS绑定/hit/effect |
| F01/F02注册与输入 | [registry probe](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/topology-registration-probe.json)当时无topology target/capability被拒；[机制清单](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/current-input-mechanism.json)14surface/33slot/24operators、26authored/5pending/2unsupported/1out_of_scope，均offline/input_only；输入ID局部93pass及独立45项unittest/OK | 24算子未证明统一runner消费；10个oracle引用为bindings_only/unresolved_no_resolver，不是已解析规则；新输入层统一接线是主缺口，不靠长跑补齐 |
| F03/F04状态与工具 | topology初始257pass/3 strict xfail及3个原fail保留，新局部360pass/0xfail、最小复现6例pass、summary/freeze ACK及两源码/summary SHA已核，TM-01/TM-02已修；native71pass/2skip及hash核证、native freeze ACK | 只记离线模型/工具回归，不记reached、实际mutation、DDS drop、跨writer重叠或N图成熟 |
| F05事实/oracle | D019分开预测与observed，canonical pair八状态保持；native cleanup uncertainty不冒充SUT责任 | N图逐pair/generation raw→facts→oracle→projection仍待闭环；没有本轮新真实oracle pass |
| F06候选 | main报告自然候选仍0、本轮新增0；gap探针、native工具错误、模型缺陷和负控排除 | 非空自然admission/triage/confirmation及用户2–3目标未获新证，不预保数量 |
| F07路由/replay | Bohr errored关闭后转Feynman；routing before/first-fix19pass仅中间离线证据，gap目录有mock replay | 没有本轮真实DDS replay，不确认原异常；最终隔离/注入冲突、source/raw/cleanup审计待main |
| F08与整合 | main-review/gap-probes留存原缺口；Cicero恢复两文件逐字HEAD，组合白名单/endpoint-chains待最终证据；新Halley identity等价批读优化未完成 | 局部pass和final文件名不等于本轮完整report、全仓回归、新seal或source gate性能/等价性通过 |

固定账口：原四required target顺序和revision0合同不变，每target18、合计72；独立negative/control引用、离线测试、模型最小复现和mock replay不加成熟分子/分母。本轮required矩阵未跑，不报72/72或新增seal。RMW/domain/run_id/真实execution_stage/endpoint receipt/raw/replay执行均无新增实测（不适用或未运行），不能按目录名补填。无live/commit GO、无campaign abort，source未最终冻结，整体目标未complete。

本writer仅读文档/源码/日志、解析JSON/XML、核SHA与HEAD字节等值；未运行pytest/unittest/ROS/DDS/campaign，未staging/commit。原taskbook/acceptance不在写集、不修改；四份既有dirty文档只在尾部追加。

最终补记：Darwin恢复后的boundary_audit新写集只核artifact一致性、实施中，不是kernel再认证或真实执行证明；native两文件仍冻结。Feynman路由须保留isolated资格unverified/fail-closed，不以中间pass宣称正式隔离replay成熟。Kant局部模型owner已freeze ACK，完整source冻结与main整合仍未完成。


## 2026-11-18 D020增量验收账（固定cutoff；不是当前source完整验证）

cutoff=`2026-11-18T09:49:17.467623+08:00`；逐证据/原始失败见[本卡固定核证截面](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md)§10，[被动核验结果](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/docs-topology-owner/audit-result.json)仅为JSON/XML/hash核证，不是本writer重跑测试。main已接受core/routing离线milestone，不能上升为完整S1或正式隔离成熟；历史账保留其source/时点。

| 验收层 | 本截面接受到的证据 | 仍未获证 |
|---|---|---|
| F01/F02 输入及注册 | D020 opt-in合同；main core365pass、owner934pass及5文件局部稳定，旧required/hash与默认接口兼容账相同 | 新topology全部算子运行资格、完整provider/runtime逐guard链 |
| F07 input capsule | main Registry→真实provider→ReplayCapsule构造成功，payload_layout为r2f.replay-action-program-payload/v1；core/replay probe前后5edce1…稳定 | 独立replay、真实execution metadata、失败重放/当前source整合；静态P0不计实际失败次数 |
| F03/F04 fixture接线 | registered carrier→canonical run_pair(OfflineTopologyBackend)完成61raw，application=fixture_application_observed，七项canonical mutation receipt全false | real/hit/effect与正式requires/begin/applied/effect；fixture不是DDS执行 |
| F05/F08 ingest | 原raw/returned event0 receipt事后添raw_event_ref有独立diff，继而primary_rule缺失掩盖原ValueError；源选集当次稳定 | 统一ingest尚未成功；修复后facts/oracle/report完整链须另验，错误不算SUT candidate |
| 路由与回归 | main75pass/8文件stable、routing331pass/580.38s仅离线；原4fail修复后main4pass/选定5文件stable | routing外部树漂移保留，631pass仍有并发drift；无最终稳定完整离线或真实隔离资格 |
| 原成熟与扩展 | 四required revision0、72分母不变，扩展另列成熟义务 | HEAD42fcec65…dirty/source_frozen=false；本轮无新required/seal/live/commit，旧seal不迁移，真实自然candidate0、用户目标未达 |


## 2026-11-18 follow-up离线增量账（§10历史截面保留）

cutoff=`2026-11-18T10:25:44.648924+08:00`，证据见[接续核证§11](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md)、[12件SHA清单](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/docs-topology-followup-owner/snapshot.sha256)。本writer只核指定JSON的字节hash和内部关联，**未重读JUnit/raw/源码重验**；main/owner所记测试与raw结果按来源列入，不伪装成本writer业务测试。

| 层 | 该次离线记录 | 验收边界／尚缺 |
|---|---|---|
| F05 facts/oracle | main独立56pass/108.93s，before/after/ACK的5文件SHA映射一致、exit0；Pasteur freeze关闭 | 仅该次文件集/离线合同；不相加owner165/56为成熟，不代表最终共享树验收 |
| F03/F04、F05/F08交界 | 两层1W1R/2W2R raw79/135严格等于returned且hash稳定；fixture前置[false,false]、七项receipt全false，missing environment正确abort；ingest可非抛异常返回拒绝 | main原assert无ingestion_error的两个失败保留。persisted audit仅既有产物复核，非新执行/重放；缺topology_state_boundary仍使producer未闭合 |
| state/projection集成 | Mencius新28项=22pass/6fail（IP-01三项，IP-02/03/04各一项）；与原53项合计81项75pass/6fail，单测试已freeze | 无skip/xfail，不伪造raw补绿。边界完整性、独立pair/交付窗口/中间sentinel、neutral network与生命周期证据仍缺，不授fixture novelty/candidate/seal |
| registry/profile/replay | Cicero529pass/238.02s伴4外部漂移；两个旧断言须修原处置与1641pass/4fail/1skip保留；当前两测试ACK仅自报23pass停写 | 不替代main稳定全链；此前61raw/两层工具失败保留其时点，新增79/135不能抹旧。真实replay未证 |
| 原required与扩展 | 四required target/revision0、72分母无改动，topology扩展仍须独立成熟核证 | HEAD42fcec65…dirty/source_frozen=false，无本轮live/commit、真实RMW/domain/run_id未运行；自然candidate0、旧seal不迁移、完整用户目标未达 |


## 2026-11-18 13:11 恢复卡分层验收账（§12勘误；不是新source seal）

base cutoff=`2026-11-18T13:19:50.494563+08:00`，明确main补充另记 `2026-11-18T13:24:34.359693+08:00`。来源：[固定原字节/SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-owner/cutoff.json)、[被动artifact核证](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-owner/evidence-review.json)、[接续核证 §13](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-resume-fixed-cutoff-1311)及[六人dispatch](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-owner/main-message-supplement-1/dispatch-current-six.json)。本writer只读JSON/XML/已安装源码AST与文档，不重跑业务测试；原§12与旧账／失败／source身份均保留。

| 验收层 | 本截面可记进展 | 不可据此声称／剩余证据 |
|---|---|---|
| F01/F02 语义与目录 | D021明确required/exploratory同core.SemanticTarget；前者固定conformance，后者扩展fuzz并复用适用规则／状态；Hume统一目录／家族解析实施中 | 不是两套语义类型，也不是目录／CLI／provider/replay兼容已完成；旧身份仅为仍使用的验收/replay保留，不改旧manifest/revision0/72 |
| 离线整合 | 历史tests/日志3189pass3skip、JUnit3192/0error/0fail、243行为文件记录稳定，默认root收集40error/exit2保留 | 新netem/Mill前旧快照，不等当前源码全仓；308pass仅后续拓扑子集且本卡未获其独立日志；旧ACK版本不混用，不累加为真实执行 |
| F03 状态与投影 | 1W1R87／2W2R147 fixture raw有before/after boundary，evidence_error=null；工具环境缺失正确abort | before observation incomplete、两window_complete=false、projection incomplete/unknown、real_execution=false；Mencius新交付窗producer待验，不计状态闭环／novelty／candidate |
| F04 carrier／算子 | non-DDS netem有安装／计数／恢复记录；旧Mill独立auditor有条件反射局部记录 | 无ROS/S1 hit，reorder未逐包证乱序；Descartes route接入待验，配置反射≠timing/lease lifecycle、prediction≠observed match。旧9/21能力计数不是当前source成熟率 |
| F04/F05 CFT输入面 | node.py1583–1641的create_subscription(content_filter_options=...)及subscription.py138/143/154的is/set/get静态存在，纠正“本机无API”误判 | Hooke仅初始reader配置／实际读回**实施中**；未证两RMW支持、动态filter、样本过滤效果或match规则，不能从API存在写成filter.expr已hit |
| F05/F08 观察与报告 | saved-field统计本身可复算：124cycle/273row/246 actual_run=true，四旧fuzz_v1 target分布139/52/12/43 | 未复审这246行的raw，不证明当前真实执行／合同完整或零residual原因；0候选不能推出筛选器没有漏失 |
| F07 身份／replay | netem-multi-1保存两行target为discovery.interruption_recovery.fuzz_v1/rev0、Cyclone/domain205、旧run_id有据 | 非fuzz_windows_v1，不证明F2-windows或当前隔离replay成熟；本卡没有新attempt/raw/execution metadata，legacy身份兼容仍待Hume/main实际对账 |
| F06／整体目标 | 本轮自然candidate仍0，控制／工具错／carrier probe排除，goal恢复active | 非空真实admission/triage/replay确认及2–3真实候选目标未达，不凑数、非complete；需当前source真实运行与独审，不预保数量 |

当前名册为Descartes、Galileo、Hume、Helmholtz、Mencius新handle、Hooke六名，均main显式`gpt-6.1-sol`；旧Mill等不可访问handle不计活动，不把旧模型字段或局部测试挪给新owner。外部PID2301092仅更新**last_seen_absent、exit unknown**，不使用其测试结果，不作为环境clearance证据。

本卡 source HEAD=`42fcec65fe66d1dff8dd761bf03ee96a427277c7`／dirty／source_frozen=false；本卡target/revision执行、RMW/domain/run_id、mutation receipt、raw/replay **未运行／无新增**。四required revision0／旧manifest／72分母不变，成熟分子增量0、无新seal、无live/commit GO。旧source seal不迁移；main当前源码稳定回归、明确GO／source freeze及唯一live owner另行安排。文档验证与owner freeze ACK均不等业务或完整S1成熟证明。


## 2026-11-18 小型follow-up离线增量验收账（不折算成熟率）

cutoff=`2026-11-18T13:44:10.558816+08:00`；来源main明确补充及[固定15件产物](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/cutoff.json)，被动核证见[evidence-review](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/evidence-review.json)、边界见[接续核证§14](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-resume-docs-followup-1344)。原失败、旧freeze和历史source账不改；本writer不复跑测试。

| 层 | 本卡可记进展 | 未获资格／剩余门 |
|---|---|---|
| F01/F02目录 | Hume124pass（39新增）／freeze/main关闭；main85pass、CLIexit0、11identity/6family、21条组视图manifest全部相同 | 仅离线目录整合；不是语义／规则合同合并删除、DDS覆盖或新source成熟；required/exploratory同SemanticTarget，旧ID兼容／独立opt-in不变 |
| F04配置审计 | owner138pass；旧三9/9完整audit等价、netem原字节保留；main独立125pass、七依赖测试窗口无漂移 | 新三relation.pair/timing.shift/lease.value只configuration audit，无match/timing/lease lifecycle effect或hit；旧三param.enum/queue.depth/param.boundary原合同不变，未证真实public API反射 |
| 能力分层 | 输入24/24、至多11/24审计挂接（新三仅配置）；旧12surface10路由、8有历史代表raw | v2真实DDS0/24；旧代表raw不作当前source重跑，F2 fuzz_windows_v1无新v2实跑，不借旧fuzz_v1；这些计数不可加总或折算成熟率 |
| F03/F05/F07及最终门 | Mencius与Hooke仍实做pair／worker(CFT)和指定测试，尚未验收；本卡未产生execution/raw/replay | 当前producer/consumer稳定收口→源码冻结→精确新路径短真实DDS闭环；未消费helper不算功能完成，source稳定子集不替代最终freeze/live |

本卡HEAD42fcec65…dirty/source_frozen=false、四required revision0/旧manifest/72分母不变。**current-source seal证据0/72不是代码完成度0**，亦不删除历史72/72。自然candidate0、goal active非complete；无新增live/commit/scope变化或成熟分子，本卡RMW/domain/run_id未运行。Hume124与main85、owner138与main125不相加为执行或验收次数；85/125不是全仓回归。旧失败保留、旧seal不迁移，2–3真实候选目标仍未达。


**明确main消息独立补截面（`2026-11-18T13:48:47.092310+08:00`）**：Hooke初始CFT卡已有[delivery](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/cft-main-message-supplement/delivery.json)／[freeze ACK](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/cft-main-message-supplement/freeze-ack.json)／[final test result](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/cft-main-message-supplement/test-result-final.json)，owner65pass（21新增）、11个直接依赖在该测试窗before/after一致；main已关闭Hooke。仅核收初始reader配置／实际读回的**离线交付**，真实RMW尚未测，无dynamic filter／sample-effect／lease主张；不等于source_frozen或S1成熟。本补充不回写13:44旧截面。main当前明确Mencius是唯一代码writer，窗口producer仍在收口、未验收；`/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/main-integrated-regression/run.py`仅**已准备、未启动**，等Mencius freeze才做全仓。本docs卡未读取／执行该脚本，不借准备状态预填回归结果；无新增live/commit或scope。


**后续main消息独立补截面（`2026-11-18T13:55:10.821258+08:00`）**：Mencius已有[summary](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/mencius-main-message-supplement/summary.json)／[freeze ACK](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/mencius-main-message-supplement/freeze-ack.json)／[最终记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/mencius-main-message-supplement/producer-expanded-followup.json)，owner最终288pass/39.91s、8依赖在该测试窗稳定；31聚焦与288重叠不累计。写集仅`/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/runtime/topology_pair.py`及`/home/wuyuqa/ros2-dds-fuzz-v2/tests/test_topology_delivery_windows.py`、`/home/wuyuqa/ros2-dds-fuzz-v2/tests/test_topology_runtime_route.py`三文件，pair SHA=`25de6fa16ed37eae311513fd1e9c544a0ca3e410e2cd450ea7055511dfb2450b`。main已关闭Mencius，当前所有代码owner均停写；这只是owner离线交付／写集freeze，不等于全source冻结、真实DDS窗口effect或S1验收。main已启动`/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/main-integrated-regression/run.py`的实际全仓`tests/`，**本消息时仍运行、结果未知**；`R2F_BOUNDARY_KERNEL_TESTS=1`是non-DDS kernel对照，不是ROS/DDS/campaign或成熟证明。本docs卡不运行／等待／借用该测试，不回写13:44与13:48历史或已freeze证据；仍需main整合核证→源码冻结→精确新路径短真实DDS闭环，无新增live/commit GO、候选或72分母变更。


## 2026-11-18 D022卡验收账增量（固定截面，不回写“仍运行”）

cutoff=`2026-11-18T14:18:26.873740+08:00`；来源[本卡4件必要证据/SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-intervention-owner/cutoff.json)与main工程裁定[D022](/home/wuyuqa/ros2-dds-fuzz-v2/docs/decisions.md#s1-configuration-intervention-d022)。

| 层／事项 | 本卡核证／状态 | 不授予的资格 |
|---|---|---|
| 先前main全仓回归 | [main result](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-intervention-owner/snapshot/main-integrated-regression/result.json)／[JUnit](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-intervention-owner/snapshot/main-integrated-regression/junit.xml)：3406pass、3skip、0fail/error；wrapper411.87s／JUnit410.821s；result记录253source/test在该测试窗稳定，HEAD不变 | 非当前后续source通过、source freeze、DDS执行或seal；本docs未重跑或全量重hash |
| 三skip与kernel对照 | [skips](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-intervention-owner/snapshot/main-integrated-regression/skips.json)与JUnit一致：2真实native opt-in未开＋1需retained real provider report；non-DDS kernel boundary对照启用 | 未执行native真实DDS两项／真实provider报告核验，不把kernel对照算DDS成熟 |
| 新三配置审计历史 | relation.pair/timing.shift/lease.value旧局部freeze仍为无hit；旧红日志／旧三parity不改 | 不追认旧run hit，不永久混同实际配置干预与语义effect |
| D022 configuration intervention | **实施中**；仅真实API映射后的sealed差异、排除rounding alias、完整实际after raw readback、正确身份／代际／时序、无后层覆盖且formal real qualification满足才可认hit | fixture／booleans／回声不授hit；hit不推effect、expiry／renewal／match、candidate或seal；无独立effect证据保持false，由typed facts/oracle独立判 |
| 新解冻两卡／既有filter.expr | Banach／Sartre未交付、未真实执行；已有filter.expr按同门消费初始CFT raw | 不新增算子／target／rule／revision；不称动态filter或样本效果；旧3406不迁移；新增scope成熟义务不能由72代替 |

HEAD `42fcec65fe66d1dff8dd761bf03ee96a427277c7` dirty／`source_frozen=false`，candidate0、goal **active非complete**；原四required revision0／旧manifest／72分母不变，无新增live/commit GO。本卡真实RMW/domain/run_id、mutation_receipt、真实raw/replay均无新增；candidate delta0、成熟计数不增加，不把3406个测试换算成成熟格。goal仍未complete。后续由main稳定整合→源码冻结→精确新路径短真实DDS；docs freeze后停写，不追踪新owner代码。


### 2026-11-18 filter输入schema修正增量账（未交付／未验收）

cutoff=`2026-11-18T14:24:02.984960+08:00`；[本卡单件证据/SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-filter-increment-owner/cutoff.json)。main已发现当前valid filter `seq >= %0` 与worker使用的`std_msgs/msg/String`仅有`data:string`不符；这是**工程缺口／实施中**，不是原规格新增要求，不能据“valid”标签宣称可执行。Rawls `01a1ed4e-08f6-77b1-b709-dbb21cd25064`，main显式请求`gpt-6.1-sol`，不推actual backend；来源[main派遣来源记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-filter-increment-owner/task-contract.json)及[implementation-contract固定副本](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-filter-increment-owner/snapshot/implementation-contract.json)。其写集仅`/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/modules/s1/fuzz_mutators_v2.py`、新`/home/wuyuqa/ros2-dds-fuzz-v2/tests/test_topology_filter_generation.py`；`/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/modules/s1/input_model.py`仅必要的局部filter元数据／字段描述，非一般模型重写授权。

要求依选中endpoint声明的message schema生成合法、非no-op的`data`字符串SQL，literal／参数类型正确；未知type必须具名MutationRefused或显式unknown，不能冒称executable valid。显式empty类和invalid负类保留，invalid期望guard reject。任务卡计划核profile/corpus I2 provider→prepare/MutationPlan→最终topology_program并与既有filter_backend.validate_filter对照，含两层组合、参数类型、unknown与确定性hash；**这是待做验证点，不是已通过结果**。本docs卡不读／追踪该卡代码、不跑测试。

只涉及输入生成／离线合同，不授filtering／match／sample-effect／hit或RMW支持，也不升级[D022](/home/wuyuqa/ros2-dds-fuzz-v2/docs/decisions.md#s1-configuration-intervention-d022)的formal real qualification。旧hash／执行证据和红日志保留，不借先前3406回归证明后续source通过；不改target/rule/revision/算子／required72。其他三owner Banach、Sartre及Helmholtz沿用现任务，无新结果推断。HEAD42fcec65…dirty／source_frozen=false、candidate0、goal active非complete；无live/commit GO、无新增真实RMW/domain/run_id／raw/replay。 本条只登记输入合法性待核义务，新增成熟计数0，不能用required72替代扩展scope的独立成熟义务。


## 2026-11-18 D023固定验收增量（非执行／成熟证明）

cutoff=`2026-11-18T15:39:18.528072+08:00`；[main pre-dispatch固定记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/snapshot/pre-dispatch-evidence.json)指定SHA已匹配，[7件原字节/SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/cutoff.json)；完整状态见[接续核证§16](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-scheduler-baseline-cutoff)。

| 层 | 本卡实际核证／状态 | 不能推出 |
|---|---|---|
| Rawls旧filter离线交付 | [Rawls JUnit1](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/snapshot/Rawls-filter-run-1.junit.xml)／[JUnit2](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/snapshot/Rawls-filter-run-2.junit.xml)各281pass、0fail/error/skip；[Rawls旧filter ACK](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/snapshot/Rawls-filter-freeze-ack.json) hash与main内嵌ACK一致；ACK记录两窗20依赖稳定 | 不累计为562个独立通过；不证明当前fair策略／当前源码稳定、RMW或filter effect |
| Sartre旧干预离线交付 | [Sartre JUnit](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/snapshot/Sartre-affected-173.junit.xml)173pass、0fail/error/skip；[Sartre旧干预ACK](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/snapshot/Sartre-affected-freeze-ack.json) hash一致、offline/execution_evidence=false/candidate_eligible=false | 不授真实configuration hit/effect或新runner接线验收；[owner精确source/test hash索引](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/owner-source-identities.json)只封存ACK身份，未读当前代码 |
| main全仓／子集观察 | exec观察3538pass/12skip/451.13s、209pass/67.66s、418pass/4.74s；无saved main JUnit或稳定行为hash，12skip未知 | 非稳定source／整合seal；不累计子集，不猜skip；root rc2/169 collection errors原失败保留 |
| Banach旧probe | [旧失败probe-assessment](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/snapshot/Banach-failed-probe-assessment.json) outer loopback EPERM、rc1、nested未创建，proc inventory未跑 | 不证明nested隔离、完整N观察或通用环境不可解；cap-loss仅待诊断假设 |
| [D023](/home/wuyuqa/ros2-dds-fuzz-v2/docs/decisions.md#s1-fair-base-input-scheduler-d023)及三新代码卡 | **main工程裁定／实施中**：固定corpus/seed/history下fair选择／sampling／变异RNG不受运行反馈；旧省略feedback路径和历史log/replay字节兼容保留；ledger/retention准入仍在 | 不是全campaign无反馈、BFS/DFS状态planner、真实新状态发现、用户已批准默认更换；未有新fair／纠错probe交付 |

HEAD `42fcec65fe66d1dff8dd761bf03ee96a427277c7` dirty、source_frozen=false；source当前解冻，candidate0、goal **active非complete**；原四required target/revision0／旧manifest／72分母不变，无live/commit GO。 无新增target/rule/revision/算子／72分母、真实执行、candidate或seal；新增成熟计数0，扩展scope独立义务不能用72代替。RMW/domain/run_id／receipt／真实raw/replay无新增；本docs未跑测试／probe／ROS或commit。全部历史原文、旧freeze与红日志不改。


## 2026-11-18 D023固定cut接受账（离线milestone与真实non-DDS分层）

固定cutoff=`2026-11-18T16:03:44.254141+08:00`；[12件必要原字节/SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/cutoff.json)；[main milestone acceptance](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/milestone-acceptance.json)原字节SHA=`1eef8020a1a2341da84794d163887230d8116b2f33279b7a3cecf06db231503a`精确匹配。 [接续核证§17](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-scheduler-milestone-followup)；main工程接受不冒充用户默认变更授权。

| 层 | 本cut核证与核收 | 仍不授予 |
|---|---|---|
| 整合合同子集 | [main integration result](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/integration-result.json)／[JUnit](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/integration-junit.xml)337pass/9skip，9项全为opt-in non-DDS kernel；262-file测试窗稳定、HEAD不变 | 不称全仓／正式source freeze，不借337证明随后Banach源码 |
| 实际CLI planning | [CLI corrected assessment](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/CLI-corrected-assessment.json)；原2＋resume新2具体输入与whole4一致，完整selection一致；显式feedback mismatch拒绝 | planned/actual0，不是runtime replay；checker原suffix2对whole4的AssertionError保留，改评估非产品修复，无CLI重跑 |
| legacy兼容层 | 默认legacy不变；原公式分支按main记录另测；[36 scheduler bytes记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/legacy-36-byte-comparison.json)36 scheduler bytes离线一致 | **24-input legacy全路径parity仍未验证**；不同对照不合并授资格 |
| [D023](/home/wuyuqa/ros2-dds-fuzz-v2/docs/decisions.md#s1-fair-base-input-scheduler-d023) | main接受本cut离线scheduler/runner/CLI milestone；固定corpus/seed/history的fair策略 | ledger/corpus admission反馈保留，非全campaign无反馈／BFS/DFS／自动状态发现／用户已批准默认切换 |
| non-DDS probe | [main probe artifact review](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/probe-artifact-review.json)／[bounded probe summary](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/probe-summary.json)；[32项只读hash复核](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/probe-hash-check.json)32/32，outer UID1000 cap0、inner root+net/PID/mount/proc；严格inner inventory、detached退出、6lease释放 | host scanner不可读仍host_clearance=false；非真实ROS backend inventory／canonical 1W1R／candidate／成熟；旧EPERM版本不回写 |
| 当前producer bridge | Rawls/Sartre freeze并main关闭；Banach仅获指定producer写集解冻做canonical nested topology bridge | **尚未实现交付**，无DDS/live/commit；不得将probe或337迁移新source |

HEAD `42fcec65fe66d1dff8dd761bf03ee96a427277c7` dirty／source_frozen=false、candidate0、goal **active非complete**；原四required target/revision0／旧manifest／72分母不变，无新增target/rule/revision/算子或成熟计数；扩展scope独立义务不能用72代替。 wrapper179.42461072304286s／JUnit178.651s分列；262-file稳定仅按result声明，本docs不读／rehash代码。无新增真实RMW/domain/run_id／mutation receipt／DDS raw/replay，新增成熟计数0。所有历史red／ACK不变，下一步仍由main核接线／冻结与精确真实资格。


## 2026-11-18 frontier repair离线证据增量（不迁移成熟结论）

cutoff=`2026-11-18T19:19:38.520960+08:00`，[cutoff／12件必要artifact SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/docs-owner/cutoff.json)；详细出处／消息与直接artifact区别见[本卡接续核证](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-frontier-repair-fixed-cutoff)。

- **被动核验**：12件JSON/XML/审查文档保存SHA；独立entry JUnit7pass、review exit0／两文件稳定；baseline红控JSON exit1且expected failure。owner305pass、overlay2fail5pass为main消息报告，不扩大到全仓或DDS。
- **局部复现非修复验收**：identity同final配置异hash、M01/M03 no-change漏准入有离线报告；topology已有拒绝门，不能推广全路线错误。新frontier／identity／noop仍编码；API约定非完成，真实hit／effect／replay未运行。
- **历史证据不升级**：159cycle／543initial_seed／246 saved actualtrue／29全层equal可按保存metadata复算；仅owner4raw抽查，非全raw认证；26跨5HEAD／2RMW一组出现、scoped重复超额19。

HEAD=`979da1d4ba606e903f78fb44af8c34954e809b24`、behavior dirty／`source_frozen=false`；四required revision0／原manifest／72不变，candidate0、goal **active非complete**，无新live/commit GO。source状态来源为本卡main消息与artifact，不是docs重扫行为树。 新required执行／seal计数增加0；RMW/domain/run_id无本卡新执行值，raw/replay未运行，candidate新增0，成熟分母影响0。准备errored是工具交付失败而非SUT异常或环境不可行；无本卡abort、无新增scope／GO。


## 2026-11-18 frontier follow-up：固定增量，不覆盖历史

cutoff=`2026-11-18T20:33:52.706497+08:00`；[15件固定artifact SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/docs-followup-owner/cutoff.json)；完整分层、原始出处及main工程裁定来源见[本卡接续核证](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-frontier-followup-fixed-cutoff)。

- identity/noop/BFS提交152a292eaeeb7175e506cfcebaeeeb2906e284a5，35路径精准排docs/results；提交后behavior496bbb49f329b6ac0f5b53345f0f9fe31344e8ee189cce5c493f195eca41bf7b／source_frozen=true。launch/postrun同152a且finish source unchanged=true；Pascal获窄修复写权是owner解冻，不据此推actual dirty，后续manifest须按实际业务字节与HEAD比较，本卡不追代码。
- 原4204pass/32fail/12skip稳窗保留；23文件492pass/6fixturefail后仅CLI整文件27pass，不拼为498pass／全仓pass。F1 dirty-source12/5/12 proof首script mismatch保留，read-only correction先核24 envelope digest后通过；12generated11unique1duplicate、4 nonroot-parent选择，plan-only actual0，不计live/candidate。perf同负载7输入wall120.542→54.357、目录1367→87、逐route1362不变，不是全覆盖。
- 单次正常1W1R domain183/Cyclone、replay-e9f3225f6bb146fd85a5cbcc36121f29：main raw审查178events/send4/recv4/oracle pass、inner scope+2bindings及authenticated cleanup.confirmed=true；report仍fail_artifact/ingestion_rejected、formal real_ros_execution=false。generic worker_exit PGID0与accepted token56/71冲突是producer证据链缺陷，非SUT候选，不升级旧run。Pascal窄修producer实施中、不弱consumer、无新live/commit；Cicero单次live已结束。
- 设计audit21topology operators/11slot/31pair、3kind冲突属main报告；global24≠已覆盖24，operator/slot/class BFS未实施。四required revision0/72不变，candidate0、goal active非complete；当前无用户需裁决问题，新增成熟0。
- 本轮唯一docs请求gpt-6.1-sol low，仅追加这四文档，产物仅本卡目录，不继承旧owner权限。只被动核artifact与文档，不tests/ROS/DDS/netns/campaign/replay/stage/commit/派下级/消息其他owner；交summary/freeze-ack后停写。low/medium为main请求参数，不推实际后端。


## 2026-11-18 producer闭环增量（固定cut，不覆盖旧失败）

[2026-11-18T21:41:41.348360+08:00 cutoff/SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/docs-producer-closure-owner/cutoff.json)；详见[本卡分层核证](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-producer-closure-fixed-cutoff)。main工程来源非用户原话：独立208pass（JUnit70.319s、summary wrapper71.140s）／owner584pass297.69s重叠不加总；提交5978d067e62bb1c9d4e1bfdf3a181333e3ace25d后main冻结核验true、behavior966ba86b…，测试前dirty稳窗不倒填冻结。attempt2仍fail_artifact（cleanup_complete／child最终runtime_result缺口），attempt1/2 immutable。attempt3新普通publicAPI 1W1R／Cyclone／domain183正式pass、real_ros_execution=true、qualification[]、main独立audit valid，183events send4 receive4／child exact／raw cleanup true／source稳定；本docs核固定review与report SHA，非逐raw或kernel再认证。一次正常路径不是BFS／高压／全S1成熟。

F1六任务4root-task／2derived-parent selections仅plan-only、execution null／actual0、旧source，不计live。shadow首切218pass、12operators/15pairs/26classes；本cut canonical followup已有owner232pass及freeze ACK，但real_tree_applied=false、待main独立review，非真实source交付，与218不累计。完整21operators/11slot/31pair、多算子链、真实BFS／全覆盖／候选仍未完成，未支持不删账。candidate0、goal active非complete，四required revision0/72/catalog SHA94775740…不变，不新增成熟格。

本卡唯一docs别名docs-producer-closure-owner、requested gpt-6.1-sol low，不编造ID／实际后端；只append指定四文件、产物仅本卡目录，历史GO不继承。main派遣当前Cicero low唯一live owner完成attempt3无下一liveGO；Helmholtz medium shadow CLI（cut已交shadow ACK，未推main关闭）；Epicurus medium准备脚本；Carver已main关闭。本卡仅被动artifact/SHA/JUnit及append核验，无tests/ROS/netns/campaign/敏感进程/stage/commit/派下级/消息；summary及freeze ACK后停写。


## 2026-11-18 F1六次真实BFS固定增量（main工程来源）

[2026-11-18T22:08:58.582464+08:00 cutoff／五件SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/docs-f1-bfs-proof-owner/cutoff.json)；[详细核证与原件关联](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-f1-bfs-proof-fixed-cutoff)。main独立证据：旧frozen5978d067…／behavior966ba86b…上119.068525s、max_inputs stop6，6actual／6hit／6oraclepass，6canonical逐行资格true／errors[]、cleanup过门；任务ID与plan六任务相同、4root-task／2derived-parent selections及parent final_config继承。仅F1 P01/W02，不是topology21或全S1。main误将cycle.json送audit_exploratory_report的失败原样保留，artifact-kind correction说明cycle非正式report，不能当SUT／逐行失败，也不能称全报告audit valid；正式F1 report未接线，由Fermat shadow修复中。旧v1 outerprelaunch abort0runtime／spent GO永久保留。

main对既有D018的工程作用域解释非新豁免：4host不可读unknown保留、host_clearance=false，无kill／白名单／scanner放宽；v2仅canonical隔离，inner kernel／leases／cleanup门不变。main独立30shadowpass／124.64s仅首切离线接受；后续Socrates获main明确APPLY_GO应用10路径，固定22:06:43 continuation的current HEAD仍5978，但behavior14d39c92…／source_frozen=false／8behavior dirty，131项相关测试仍运行未知。前正常＋6F1不能借作dirtycurrent成熟证明。完整21/31另一shadow实施中、多算子链／全覆盖／正式report／候选等未完成。

main当前代码owners Helmholtz medium／Fermat medium／Socrates low（ID见固定continuation），加本docs low，均requested gpt-6.1-sol，不推实际后端；Cicero／Epicurus／Carver已关闭。无live／commit GO，candidate0／goal active、required72／四revision0／catalog不变。本卡仅原四文档append＋新docs-f1-bfs-proof-owner产物，五件被动JSON/SHA核验；不tests/ROS/netns/proc/scans/stage/commit/消息、不改decisions与旧cut/freeze，summary／freeze ACK后停写。


<a id="s1-integrated-bfs-fixed-cutoff"></a>
## 2026-11-18 integrated BFS 固定截面（main工程派遣，非原规格原话）

固定cutoff=`2026-11-18T23:16:20.447401+08:00`；[原字节及SHA索引](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/docs-integrated-bfs-owner/cutoff.json)、[17项具名JUnit](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/docs-integrated-bfs-owner/junit-named-checks.json)、[47项具名JUnit](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/docs-integrated-bfs-owner/chain-named-checks.json)。本卡仅核指定artifact，不追随后续pytest结果；历史cut／失败／freeze不回写。所有下述main裁定与owner状态来自本卡main派遣及固定产物，不冒充用户原规格要求。

- **真实tree离线接线已进树，不再沿用“plan-only硬拒仍存在”作为现状。** `main/topology-operators-real-tree-acceptance.json`独立接受精确三路径及65pass（非live）；`main/f1-topology-runtime-apply-acceptance.json`仅精确八路径compile接受；`main/chains-checkpoint-real-tree-acceptance.json`仅另一组精确八路径静态compile接受，不把路径数相加成测试覆盖。未核业务代码，以上是main固定接受记录，不是本docs重新编译或runtime验证。
- **F1 report分层**：main独立JUnit17/0failure/0error/0skip，log17pass19deselected／20.84s（JUnit20.850s），具名覆盖重hash后binding/final/plan/source等重开、移除marker仍不得绕过、旧actual缺canonical布局及malformed证据。owner原193pass6fail／802.33s因shadow root relocation而在checkpoint前拒绝，失败保留；显式repo六项通过与36focused是独立窗口，不合并成199pass。owner记载原217文件不变；derived report containment仍拒，历史报告audit未闭合，不升级为fresh正式报告。
- **checkpoint安全scope优化**：main独立10pass3s，仅接受限定完整值/type/alias key、完整模型验证后私有handoff、detached cache及新进程完整重建不变；同workload CPU16.439266987→15.802284527s，仅约3.87%，不是数量级提速。32mixed仅4generated／2unique，持续32distinct valid growth和64 worst-case frontier仍未达，不能用小样本或墙钟争用冒充性能验收。shadow接受后当前进树仅静态整合，不补造最终整合通过。
- **组合链输入合同**：新`chains-v1`／独立frontier，长度2..4；21operators／31pairs／64classes是生成口径，不等于21hit或全覆盖。guard、净no-op、overridden不计有效hit；输入effective／diagnostic prefix不等于runtime effect。固定summary/freeze与JUnit核出47具名新test通过；原pytest314收集后余267未完成，不是deselected或pass。main授权一次有界SIGINT，实际pytest与driver exit均2，130仅信号惯例，不写实际exit130。canonical-probe为15次标准CLI／71checks、actual_execution0，非live／完整64-frontier性能证明。
- **未来live preparation**：main独立76mock pass／0.581s，仅准备脚本；不授GO、不确认source binding。owner早期mock stderr被覆盖未保留，明确不可重构或冒充已核失败raw。未来domain185/186本卡未扫描，不能称可用；不新增live或commit GO。
- **整合回归仅启动，结果未知**：固定`main/integrated-regression/{before,command,process}.json`记录main在23:14:31启动真实tree统一`tests/`、PID2877515，`kernel_opt_in=false`／`new_live_go=false`。本卡不检查其后tests.xml/log、不等待、不预填pass，也不把启动当完成。所有代码owner按main消息已完成关闭；本docs唯一文档writer，main仅写results并跑该离线回归，代码停写至回归结束。

**身份与剩余门**：HEAD=`5978d067e62bb1c9d4e1bfdf3a181333e3ace25d`，behavior=`fe156af497f07b45410e8150d748089d175bb04d794c450f37ee0787dd457638`（来源最后静态整合acceptance），dirty／`source_frozen=false`。旧5978／966ba86b…的六次F1真实逐行证据仅历史，RMW=`rmw_cyclonedds_cpp`／domain184／run_id=`f1-bfs-domain184-seed42-5978d067-single2`、target=`discovery.late_endpoint.fuzz_v1` revision0；无本卡新run/raw/replay或新mutation receipt。当前source尚无fresh正式F1 report与topology BFS live，也无真实runtime新路径验证。接续应先取main当前tree完整回归终态及稳窗核验，再按明确GO做source冻结与fresh正式report／精确topology runtime闭环；fullgrowth32/64与完整组合覆盖继续保留缺口，不删除未完成267。

四required revision0／catalog SHA=`947757406e7a23662b576b5cf5ab615773f36a83ccdff9a1e19579fe569e4eb6`／72分母不变，新增required成熟格0、自然candidate0、goal active非complete。无新abort裁定／scope升级／用户待裁决项；工具或输入生成不得补候选。

**本卡owner与写集**：别名`docs-integrated-bfs-owner`，requested `gpt-6.1-sol low`仅main工程派遣参数，不证明实际后端／推理配置、不编造ID。唯一获准只append本仓库AGENTS.md、docs/s1-continuation-2026-11-18.md、docs/s1-acceptance-ledger.md、docs/open-items.md；产物仅本卡新结果子目录。不继承旧全docs权限，不改decisions/taskbook/acceptance/README/code/tests/config/他人results。不pytest/ROS/netns/campaign/scan/stage/commit/派下级/消息其他owner；本卡未读业务代码。被动JSON/SHA/JUnit检查与append前缀核验后交summary/freeze ACK即停写，不追当前回归结果。


<a id="s1-e48-live-fixed-cutoff-20261119"></a>
## 2026-11-19 e48 fresh live 固定增量（main工程来源，历史日期文件不改名）

本卡cutoff=`2026-11-19T00:14:12.264108+08:00`；[15件必要产物原字节／SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/docs-e48-live-owner/cutoff.json)、[两组具名JUnit](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/docs-e48-live-owner/named-junit-verification.json)。只核固定材料，不遍扫旧历史、不追后续shadow修复；旧docs-integrated-bfs-owner freeze与“当时回归未知”等旧cut原文保持不动。下述工程解释与owner状态来自main本卡派遣／独立review，不冒充用户原规格原话或实际后端证明。

### 终态回归、窄修与源码冻结：分别记账

- main当前已提供原统一回归终态：4528项，**4515pass／12skip／1fail**，1694.8688069200143s（约1694.9s）、returncode1、source与tests稳窗；唯一fail为`tests/test_fuzz_corpus.py::FuzzCorpusContracts::test_profiles_explicit_exploratory_disabled_and_not_required_catalog`，旧精确profile清单未纳入四新disabled topology BFS profile。原失败产物保留，不改为全绿。12skip独立保留，不以此次non-DDS子集消除全仓skip账。
- profile-catalog owner仅修一测试文件的精确预期与fail-closed合同，其他业务SHA／配置不变；初次10pass1fail（错误将combinations预算固定为30，实际120）及修正历史保留。main独立JUnit **11pass／0fail／0skip，9.585s（main口径9.58s）**，不与旧4515相加、不宣称重新完整全仓通过。main另一组kernel JUnit **9pass，13.222s（main口径13.21s）**，仅non-DDS namespace生命周期对照，不是DDS/SUT evidence或本卡kernel再认证。
- 单次精确21paths commit与main冻结核验有据：HEAD=`e48d3b0cdde2e502b7df16552ff696cbb0449f1e`／behavior=`fe156af497f07b45410e8150d748089d175bb04d794c450f37ee0787dd457638`，`source_frozen=true`、dirty_behavior_paths=[]、content_hashes_match=true。docs／AGENTS／results未stage；本docs没有执行commit/stage或重新读行为代码。冻结依据是指定main artifact，不借旧5978的dirty稳窗倒填早期source_frozen。

### F1：fresh 标准正式报告缺口已闭合，但不是required seal

main `f1-e48d3b0c-final-independent-review.json`接受新source上的`f1-bfs-formal-domain185-e48d3b0c`，Cyclone／`rmw_cyclonedds_cpp`／domain185，78.26372152299155s，6generated／6attempted／**6actual／6hit／6pass**，六行canonical `real_ros_execution=true`／qualification_errors=[]、stage judged；独立正式report audit evidence_valid=true／errors=[]／rows_verified6。4root-task／2derived-parent selections、parent final_config精确继承、同旧六任务预声明taskIDs及顺序有据。**不再把F1标准正式report写成“仍未接线／尚缺fresh闭合”。** report sealed=false、seal=not_applicable_exploratory，不能冒充72 required seal／全S1成熟。report SHA=`c9fa9876a2cc698f05620e9501e2b86a1cd920779b2fbfe3be6f9a4a78d837c9`，候选ledger0。

host_inventory_complete=false／host_clearance=false保留，不把隔离资格当host clearance。六行／frontier旧`evidence_origin=injected_runtime`是标签误标，独立canonical artifact资格仍true；标签与资格分别记，不抹去标签问题、不用标签给予资格。origin-label owner新48pass／34.427s、freeze仅shadow（42new＋6compatibility同窗），qualification/candidate gates未改；**未APPLY、未产生新source成熟**，也不改写六行原报告。其早期测试修正属于工具历史，不作SUT候选。

### topology B：一次真实attempt不等于一次qualified actual

main `topology-e48d3b0c-failure-independent-review.json`记录domain186／run_id=`topology-bfs-domain186-e48d3b0c`、冻结同source上的首次B实跑：**1input reject＋1 real runtime attempt**；canonical隔离callable确实被调用，但`real_ros_execution=false`／qualified=false、runtime_status=`completed_with_unknown`、boundary qualification invalid／`boundary_runtime_not_completed`。随后qualification early-return漏记attempt-only反馈，checkpoint触发ValueError；controller returncode1，无最终cycle/report（均不存在）。因此不能凭缺失最终cycle budget填actual0，也不能记qualified1或全成功。解除离线挡板／实际进树不等于topology真实闭环完成，首次运行暴露的是工程证据／记账缺口。

main消息确认六个合法worker alias被role binding误拒；原review六条worker_role_mismatch保留。聚合match count不可替代per-peer证据，before/after delivery／network／recovery finite-window unknown不判SUT违约、不升级candidate、不放宽consumer或checkpoint合同。review中boundary artifact audit valid=true／errors=[]与cleanup产物一致性，仅artifact核验，不等于runtime qualified或kernel重新认证。raw引用仅沿main review保留：`/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/next-live-prepare-owner/topology-e48d3b0c/live-output/cases/topology-bfs-domain186-e48d3b0c.0001.S1.topology.program/runtime/inner-runtime/raw.jsonl`，SHA=`3256510b931aeb1cf20f48a6a7157fed0c10e19de39c6b36316dee180f899fb3`；本docs未逐raw重审，未新replay，unknown自然candidate=false。

### owner、GO 与剩余门

- 初Lovelace路由errored、无执行产物且已close，main将未消费同一GO改派Socrates，属于同卡首次运行，不是额外GO，不升级失败agent为成功交付。两卡live结束后，main固定dispatch current_live_owner=null／all_live_GO_consumed=true，**无新live／commit GO**。
- main后续四owner均shadow offline only：Laplace（origin label，requested low）、Galileo（attempt-only feedback，medium）、Planck（合法worker alias role binding，medium）、Nietzsche（multipeer producer，medium），均requested gpt-6.1-sol；不猜后端／实际推理。origin48pass仅局部shadow交付，其他卡按dispatch实施状态，不追随后续fix或冒称全体完成。全部shadow未获本卡新APPLY／live／commit GO，真实source仍e48／fe156 frozen。
- 剩余：main审查shadow后显式决定APPLY与新source身份／相关回归；修复attempt反馈与alias误拒、补独立per-peer observation producer且不弱化规则；新GO下重跑精确topology路径并形成完整cycle/report／canonical资格。F1 origin标签整合后也需按新身份重资格，不能把shadow48pass当新live成熟。此前fullgrowth32/64与完整组合覆盖未获本卡新证明，继续保留缺口。

四required revision0／catalog SHA=`947757406e7a23662b576b5cf5ab615773f36a83ccdff9a1e19579fe569e4eb6`／72分母不变，new_required_seal=false、新增required成熟格0、自然candidate0、goal **active非complete**。旧5978／966ba…六真实记录仍历史，当前fresh F1独立记账；topology unknown不得凑候选，无新增用户范围裁决／scope扩张。

本卡唯一docs别名`docs-e48-live-owner`，requested `gpt-6.1-sol low`仅main工程派遣来源，不编造ID／actual backend。只append原四文件AGENTS.md、docs/s1-continuation-2026-11-18.md、docs/s1-acceptance-ledger.md、docs/open-items.md（跨日不rename历史），证据仅本卡新子目录；不继承旧docs权限。不改其他docs/code/tests/config/他人results／旧cut，不运行tests/ROS/netns/scan/campaign/replay/commit/stage，不派下级／消息他人。被动JSON/SHA／具名JUnit与旧字节前缀核验完成后交summary/freeze ACK并停写，等待main新派遣。


## 2026-11-19 topology 43031903 固定增量（cut 2026-11-19T01:16:37.255542+08:00）

来源为main工程核证与本卡明确派遣，非原规格新增要求：[source-freeze-and-topology-go-independent-43031903.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/source-freeze-and-topology-go-independent-43031903.json)；[topology-43031903-final-independent-review.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/topology-43031903-final-independent-review.json)；[topology-43031903-independent-audit.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/topology-43031903-independent-audit.json)；[post-topology-dispatch-20261119.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/post-topology-dispatch-20261119.json)。本卡[固定字节/SHA索引](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/docs-43031903-owner/cutoff.json)，不追随后续修复／F1结果。

14paths已commit；HEAD=`43031903c8b81044919ca5f27157e26cd3ad984b`、behavior SHA=`fa30e09c0cb7db37ddf895d789046fa09c82f8029395fc47ba5d9101bd572a01`，main核证`source_frozen=true`／dirty_behavior_paths=[]、精确14paths及内容hash一致，CLI全窗source不变。317pass是实际树相关窗口；main344／127／17／53／42等是独立窗口（本卡用户消息来源），不能相加或改写成一次全仓绿；旧4515pass／12skip／1fail及失败历史原样保留。

新真实topology：`rmw_cyclonedds_cpp`、domain186、run_id=`topology-bfs-domain186-43031903`；4selected=2生成拒绝＋2actual，CLIexit0，cycle/report存在。两actual均独立canonical real_ros_execution=true／qualification_errors=[]、stage=judged；合计72native samples complete、32window complete、state projection observed。两行hit=true，但effect_observed分别true／false，不把hit等同全部语义effect。两份raw仅核SHA匹配，未逐事件复审／replay。

主oracle两行仍unknown（finite_tail_or_delivery_contract_unknown）；正式report audit仍`evidence_valid=false`／4errors／rows_verified=0：两拒绝行先被要求execution资格，两执行行错套generic F1 binding。audit汇总unknown_count=0不能替代主oracle原行的两个unknown。main定位facts重建覆盖已验证no_obligation为open/missing，以及report拒绝分支顺序／topology submitted-normalized绑定合同接线；Hegel／Archimedes requested medium shadow修复实施中，未完成、未APPLY，不回改原unknown或auditfalse。canonical执行资格与正式report失败分别记账；隔离资格不是host clearance（false）。

验收边界：真实执行资格已获独立正证据；主oracle与正式report尚未通过，不能seal。F1新卡尚无最终结果；shadow修复不计成熟，不覆盖旧失败。

自然candidate0、goal active非complete；原四required revision0、catalog SHA=`947757406e7a23662b576b5cf5ab615773f36a83ccdff9a1e19579fe569e4eb6`、72分母不变，new seal=无，新增required成熟格0。


## 2026-11-19 a06c42aa 固定增量（cut 2026-11-19T02:03:40.802680+08:00）

来源：本卡main明确派遣及具名独立review，属于工程核证，不冒充原规格或新增用户范围。八件artifact原字节与SHA固定见[cutoff索引](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/docs-a06c42aa-owner/cutoff.json)，不追随后续结果。

- **新source exploratory正证据**：[topology review](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/topology-a06c42aa-domain187-final-independent-review.json)／[audit](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/topology-a06c42aa-domain187-independent-audit.json)：a06c42aa/e465、main核证frozen=true；domain187/seed41/Cyclone、run=`topology-bfs-domain187-a06c42aa`。4selected/2reject/2qualified actual/2oracle pass，report4/4、valid=true/errors[]/unknown0，72native与32windows完整，mutation hit2/effect1。cleanup与3host3inner释放均确认，但hostclearance=false；candidate0。四文件1058insert13delete、108pass与real4pass仅main消息独立窗，不是一次全仓验收。
- **历史分离**：[F1 review](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/f1-43031903-final-independent-review.json)／[parent review](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/f1-43031903-parent-inheritance-independent.json)仅430 source：6actual6pass正式审计有效、4root/2derived parent，不能迁移新source；旧430 topology unknown及audit fail原样保留。
- **未达门槛**：新正常64samples未删；32/300 profile仅离线22new/5reject/5guard-terminal、0duplicate，600被拒，未live；main消息14请求算子与固定summary13键存在差异，均不能作真实effect。guard_terminal接线shadow修复实施中；性能shadow14mainpass/29.6s仅离线，32snapshot40.48%与8every-step29.58%不混算，非every-step32成熟且未APPLY。详证链接与差异见本日continuation同cut小节。

原四required revision0、72分母、catalog SHA=`947757406e7a23662b576b5cf5ab615773f36a83ccdff9a1e19579fe569e4eb6`不变；new_required_seal=false、新增required成熟格0、candidate0、goal **active非complete**。 live GO已消费、当前无live owner；本卡无执行／raw重放／新增candidate admission验证，不声称全S1成熟。


## 2026-11-19 a938a688 小型固定截面（cut 2026-11-19T03:08:52.067428+08:00）

本轮唯一docs writer；前docs owner均freeze停写。来源为本卡main明确派遣及七件既有JSON，属于main工程核证，不冒充原规格或新增用户裁决。[七件原字节/SHA及旧文档prefix索引](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/docs-a938a688-owner/cutoff.json)固定本cut，不追随后续结果。requested `gpt-6.1-sol low`，实际backend／本writer ID未观察，不编造。

- **精确冻结**：[main/frozen-source-a938a688.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/frozen-source-a938a688.json)记HEAD=`a938a68835ced88022e5b7ff6cea29243bf416b9`、behavior SHA=`f2c25f9ec88ec997057c1e2a0549fd59212ecb5caec62c0ae4f695996600174e`、`source_frozen=true`、dirty_behavior_paths=[]、exact8 commit blobs核实、index空。仅被动核main manifest，不读取source/tests或自行重新认证。[main/zero-send-selector-apply-and-single-commit-go-20261119.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/zero-send-selector-apply-and-single-commit-go-20261119.json)是当时精确8path APPLY及条件单次commit授权，非本writer授权、非LIVE_GO；旧dirty保持，不继承历史GO。
- **修复验证分窗**：zero-send主独验12pass及4pass为两个shadow窗口；selector主独验32pass为另一窗口（[main/topology-zero-send-independent/summary.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/topology-zero-send-independent/summary.json)；[main/topology-selector-identity-independent/summary.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/topology-selector-identity-independent/summary.json)）。联合22pass有APPLY记录，实际树4项既有F1 anti-downgrade通过由本卡main消息载明（七JSON中该4项只记检查计划，未独立捕获结果）。这些窗口不相加、不称全仓绿，不等于真实DDS效果。zero-send仅delivery applicability豁免，finite tail仍unknown、闭合middle gap仍fail、graph residual与严格boundary资格不放宽；旧缺receipt行不补造。
- **旧188失败保留**：[main/topology-8b194c8a-domain188-final-independent-review.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/topology-8b194c8a-domain188-final-independent-review.json)中`topology-bfs-domain188-8b194c8a`／`rmw_cyclonedds_cpp`首1selected、1attempted actual、0qualified、1oracle unknown；report audit=false，错误`boundary_runtime_not_completed`，cycle因资格失败abort。raw SHA=`7281643dcc9035e8dc5116ed9bad36eea2ca41d2dcd72f63454dd56f7eb3d92d`仅转录review，未另读raw／重放；cleanup及leases释放、CLI退出、source未变有据，host_clearance=false。旧publish_w1被删致零发送的unknown/auditfalse不回写，未做同输入重放。
- **身份与算子勘误**：新content身份为`topology-v3`，不迁移旧hash；本卡main消息明确新首input删除`create_r2`，不是旧188删除`publish_w1`，因此不能称原异常重放。[main/normal-profile-operator-count-correction.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/normal-profile-operator-count-correction.json)明确首32请求有**13而非14** distinct operators／32requests，22new＋5reject＋5guard_terminal仅输入结果，不是runtime/effect覆盖。
- **链纯输入局限**：[chains-input-preparation-owner/summary.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/chains-input-preparation-owner/summary.json)保留13组合声明，三个length视图合计6完整effective input chain＋1guard_terminal＋6generation_rejected；actual_runs=0、execution_evidence=false、semantic_effects_proven=0，不是runtime。artifact在`8b194c8a`＋selector shadow上验证；随后source变a938，`current_source_qualification=false`且要求再验，未当前source再验，不迁移其资格。
- **当前卡之后待main核证**：本卡main消息已指定domain189、Mencius唯一live owner并发出单次GO；是否开跑／最终结果本writer不追踪、不预填，统一记当前卡之后结果待main核证。Ptolemy medium候选identity/signature离线通过仅据本卡main消息，管线shadow仍实施中、未合；不把离线通过当candidate admission或本轮真实执行。

自然candidate=0，goal **active非complete**；四required target/revision0、原catalog及72分母不变，新增required成熟格0，不新增seal。本卡只核被动JSON/SHA与旧文档prefix；无本writer live/commit/tests GO，不运行pytest/unittest/ROS/DDS/scanner/netns/campaign/replay，不读source/tests，不staging/commit，不派下级、不消息其他owner。后续仅待main核证当前卡结果、当前source链资格及未合管线；本卡不新增规则／用户范围裁决。summary/freeze交付后停写。


## 2026-11-19 domain189 follow-up 固定增量（cut 2026-11-19T09:04:24.398423+08:00）

本卡main重新派遣唯一docs writer／requested low；原docs-a938卡及其freeze保持，不回写旧截面。来源仅本卡main消息与七件指定JSON，属main工程核证，非新增规则或原规格字面。[本卡原字节/SHA与文档prefix索引](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/docs-domain189-followup-owner/cutoff.json)。实际backend／本writer ID未观察，不编造。

- **a938真实运行及失败保留**：[main/topology-a938a688-domain189-final-independent-review.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/topology-a938a688-domain189-final-independent-review.json)记source HEAD=`a938a68835ced88022e5b7ff6cea29243bf416b9`、behavior SHA=`f2c25f9ec88ec997057c1e2a0549fd59212ecb5caec62c0ae4f695996600174e`、source_frozen=true／dirty_behavior_paths=[]；`rmw_cyclonedds_cpp`、domain189、run_id=`topology-bfs-domain189-a938a688`。7selected＝6actual＋1生成拒绝，5qualified，actual oracle为5pass／1unknown，hit及effect各5。末nohit引发`execution_boundary_qualification_failed`／`real_judged_execution_missing`，正式audit **evidence_valid=false、6/7 verified**。虽CLI exit0、elapsed336.98s／预算300s，不能写成预算到时正常退出或audit pass；保留原row/raw/unknown/abort，不是旧domain188同输入重放。
- **清理资格分离**：main已独立核6份cleanup manifest hash，各cleanup_confirmed=true、descendants=[]，每份host3＋inner3 lease acquired，supervisor reported released且未quarantine；逐锁条目仅acquired、无逐锁released字段。CLI gone、source/profile/controller unchanged有据，但 **host_clearance=false**。本writer只读review，不进入真实live/raw目录、不核进程、不自行重做cleanup证明。
- **四shadow分窗，不加总**：absent-reader主独验24pass（[main/topology-absent-reader-independent/summary.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/topology-absent-reader-independent/summary.json)）；nohit-terminal32pass（[main/topology-nohit-terminal-independent/summary.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/topology-nohit-terminal-independent/summary.json)）；deadline application31pass（[main/topology-deadline-application-independent/summary.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/topology-deadline-application-independent/summary.json)）；namespace fixture11pass（[main/topology-namespace-fixture-independent/summary.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/topology-namespace-fixture-independent/summary.json)）。均未APPLY、不作全仓通过。absent-reader按send-time raw活reader inventory判适用，活reader的唯一pair／同generation及原ACK/window等门不放宽。nohit旧工件saved-context probe仅诊断重算，48原工件未改，**不是新execution/replay**；strict real_ros_execution仍false，不授qualified success/candidate/coverage。deadline exact `env.delay`＋`qos.deadline` slot配置干预与netem分开，配置hit不等独立deadline语义effect，后者未证。namespace旧合成fixture按真实canonical logical_id／worker-role alias纠错，原validator未改、无validator放宽，fixture仍offline disqualified。
- **联合准备而非部署**：据本卡main消息，20paths联合准备尚未APPLY；上述局部通过不证明新source实际BFS continuation、配置receipt或全仓回归。新source冻结／下一次live及GO统一待main核证，历史GO不复用；本卡不预填完成。
- **64sample纯输入扩面**：[chains-input-preparation-owner/breadth-partitions-a938/summary.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/chains-input-preparation-owner/breadth-partitions-a938/summary.json)在a938上被动记录16×4完整64sample保留／顺序可重建、21requested operators，47new＋10reject＋7guard，runtime_invoked=false、candidate0、required_maturity_added0。全部64 task IDs及RNG seeds相对master改变，**不称same-input replay**；54generated content distinct与8组same-final-config观察不能混同。根据本卡main消息，新8组同finalconfig的routeidentity诊断仍进行中，尚不能先判bug，不追踪后续诊断结果。
- **性能实验不合入**：[main/audit-performance-experiment-decision.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/audit-performance-experiment-decision.json)决定`do_not_apply_experimental_parse_cache`，相同boundary subtree输出相同但A wall1.250s／CPU0.644s，B wall1.562s／CPU1.070s，未见收益，单样本不作统计泛化。早期fullrow A/B均以同一oraclebinderror结束，不能伪装成功路径或完整audit成功；actual_product_changes=false，保留原错误。

自然candidate=0，goal **active非complete**；原四required target/revision0、catalog与72分母不变，新增required成熟格0／无新required seal。旧失败raw及上一卡freeze不改；无本writer live/tests/commit GO。仅被动核七JSON/SHA与四文档prefix，不读source/tests/真实live目录/进程，不运行pytest/unittest/ROS/scanner/campaign/replay，不stage/commit，不派下级、不消息他人。summary/freeze交付后停写；新source／livenext pending main。


## 2026-11-20 PM固定截面：G01真实三方向与剩余工程义务

本轮main重新派遣唯一docs writer，截面`2026-11-20T15:51:28.493865+08:00`；详见[增量进展及逐项证据](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-progress-2026-11-20.md)和[原字节/SHA索引](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-pm-docs-20261120/cutoff.json)。HEAD72c1c71d02dfed7fb1d556e5bef1639b4b292ba6 / behavior24d65f30b7277b4492e1315ce6ea14a88110936d8cec9c4821a036f7f80d48c0，工件source_frozen=true。main唯一live owner；最新用户要求main自主团队派遣验收、少重复测试。

G01rev1探索三原执行＋三strict matching real replay pass、六独审valid，Fast DDS同构92→93、Cyclone→Fast DDS94→95、Fast DDS→Cyclone96→97（原→replay domain），cleanup按main/工件确认，不冒称docs重做clearance。原工具失败及partial graph误判最终边界保留；72c1纠错不改SUT规则和foreign/ACK/identity/QoS门。92相关tests pass仅引用既有结果，diagnostic-only重算非新执行。广度/chains按main仅已启动、完成未知；64+13输入纯审计同历史identity（47new+9reject+1duplicate+7guard；5new+7reject+1guard），不能记真实执行完成。

八项L01–L08仍为工程缺口：真实session episode/checkpoint、真实并发/HB、每writer排序、动态CFT独立样本effect、writer lease连续peer inventory、native participant lease、分布式一致快照、持续高负载。配置/contrast不等effect，source较旧的执行卡不伪称当前源码重审。四requiredrevision0/72不变，current-source seal0/72、自然candidate0、goal active非complete；不写S1即将成熟。无本writer live/tests/commit/staging或新GO。


## 2026-11-20 并发接续固定截面：冻结源码、离线验收与host owner预检阻塞

本卡唯一docs writer接管已关闭Parfit后的指定三文件append写权；来源为main本卡明确派遣及补充消息，不冒充原规格字面要求。cutoff=`2026-11-20T16:39:47.790948+08:00`；[15件指定工件原字节/SHA索引](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-docs-20261120/cutoff.json)。仅核固定截面，不追随后续运行；未重扫source/tests或进程。

- **源码身份**：[source-frozen.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-final-input-20261120/source-frozen.json)记录HEAD `6fa34b221a93455cce8afa355055f50a844a0009`、behavior `658904c1168298fce8159342a139924de1371950a44a59872ccb96413dd4d06d`、`source_frozen=true`、dirty_behavior_paths=[]。[main-commit-GO.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-final-input-20261120/main-commit-GO.json)为main新精确路径提交GO；排序／并发／同次审计去重已提交。文档dirty不等于行为源码未冻结；本卡只引用工件身份，不运行source测量。旧72c1／a2f7e78真实历史及失败原件保留，不支持当前source成熟。
- **离线结果分账**：[acceptance.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-final-input-20261120/acceptance.json)为旧默认输入／五profile身份不变、90pass；canonical CLI正式opt-in并发profile仅`execution_stage=planned`、actual_run=false，真实未验。[source-chronology-acceptance.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-final-input-20261120/source-chronology-acceptance.json)记录51pass来源时序检查；初命令因不存在test_topology_canonical_receipts.py而exit4/no tests ran，纠正路径后51pass，原失败不抹除。[repair-joint.log](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-final-input-20261120/repair-joint.log)最终89pass；90／51／89存在重叠，不累计为独立测试总数。均引用main既有结果，本writer没有运行测试，不称全仓通过。排序仅supporting observation，无新rule或candidate资格；实际并发／HB及真实调度仍未验。
- **三项P2修复与历史失败**：[REVIEW.md](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-final-review-20261120/REVIEW.md)原“不通过”及R1跨writer ready-all raw event_index、R2全局provenance／fixture资格、R3 source phase原始时序缺口保留。main／agent已修三门，最终联合结果见上。[repair/summary.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-final-review-20261120/repair/summary.json)原68pass/1fail仍保存（32pass＋36pass/1fail）；旧无全局provenance的单元正例随后改为正确拒绝，不以fixture授真实并发，也不把旧agent联合失败写成当时通过。source phase测试测量显式stub，不证明真实冻结或DDS执行。
- **预检终止而非执行／replay失败**：[main-live-GO.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-live-20261120/main-live-GO.json)仅批准当前冻结source下2W1R原执行domain137及strict unchanged-input replay domain138；不是已执行证明。[original-preflight.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-live-20261120/original-preflight.json)为ok=false、`domain_conflict:old_campaign_or_process_owner`；conflicts=[]但同UID unreadable PID114642、114645、114714、115181不能认证。[wrapper.log](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-live-20261120/wrapper.log)保留`RuntimeError: environment preflight failed:domain_conflict:old_campaign_or_process_owner`。阻塞是host owner不可认证，不是source不冻结，不能声称发现已知旧campaign冲突，也不能把conflicts为空当clearance。
- **环境观察与待办**：[environment-blocker.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-live-20261120/environment-blocker.json)记录main只读ps/status：114642为chrome-sandbox，114645/114714/115181为code-insiders；exe/cwd/namespace读取PermissionError。main未读取敏感argv/env、未杀／修改进程、未绕gate；`blocked_before_runtime_submission`、actual_DDS_execution=false、replay_started=false。main已请求人类关闭VSCodeInsiders，若WorkBuddy打开一并关闭；下一步须main独立重做正常预检，进程消失不等clearance。重试wrapper仅准备、未启动（main补充消息），本卡不执行或检查它。无真实DDS raw／交付receipt／oracle／replay结果；RMW与run_id无本次执行证据，不推定。
- **可选纯输入补充已纳入同截面**：[64-input-identity-audit.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frozen-breadth-ready-20261120/64-input-identity-audit.json)／[13-chain-input-identity-audit.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frozen-breadth-ready-20261120/13-chain-input-identity-audit.json)及[64-input.log](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frozen-breadth-ready-20261120/64-input.log)／[13-chain.log](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frozen-breadth-ready-20261120/13-chain.log)记录当前6fa34b2冻结source下task/RNG/content/finalConfig/plan及生成处置均等历史准备；两脚本exit0来源为main补充消息，本卡不重跑。64为47new/9rejected/1duplicate/7guard；13为5new/7rejected/1guard。全部runtime_invoked=false；77个输入请求不是77次执行、更非77格成熟。

**记账与状态**：自然candidate=0，required新增成熟=0，无当前source新required seal；原四required target/revision0及72分母不变，S1 goal **active非complete**。以上离线接线与修复不关闭真实并发/HB/排序effect等独立成熟义务。abort／升级为环境owner认证待解决，不改合同／gate／rule／revision／统计分母。本writer只做静态append前缀、JSON与SHA验证；未运行source/tests、pytest/ROS/DDS/netns/campaign/replay/scanner、staging/commit，未派下级，未改其他文件或旧dirty。freeze ACK后停写。


## 2026-11-20 三步修复与L02单输入短闭环（终态固定cut；append-only）

本增量来源为本卡用户明确派遣及下列main终态工件；不是原taskbook新增条款，也不覆盖前文历史截面。仅记录截至该批终态工件的事实，不追踪其后live。实际模型继承本工具配置，不声明指定型号。唯一文档writer只向指定三文档追加，旧prefix及既有dirty保留。

### 三步修复：原失败各自保留，不倒填

1. **类型字段绑定修复，e5仍为unknown**：[progress-summary.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-type-binding-fix-20261120/progress-summary.json)记录source HEAD `e5b5418693c40e3d64a085f18b48fe25b6f6cc49`、behavior `c0e7b182adfb992f08625cf1971683da44df7689eaaf0aaaa6ad7eb222dd843a`、source_frozen=true。真实publish API重叠19对，match barrier及source provenance成立，但w0仅47/65、w1仅39/65，缺received final sentinel；`completed_with_unknown`、oracle=unknown、audit_evidence_valid=false、cleanup_confirmed=true、replay_started=false。类型字段修复不是交付闭合证明，原始unknown保留，不追认pass，不当自然candidate。
2. **交付barrier修复，d02仍为reached/nohit**：[main-commit-GO.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-delivery-barrier-fix-20261120/main-commit-GO.json)说明在两writer sentinel后增加既有identity-bound delivery barrier，修复异步交付未完成就结束输入的问题，不改runtime oracle。[source-frozen.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-delivery-barrier-fix-20261120/source-frozen.json)为HEAD `d02b4385ca868c4b642ad6b0d039f58bbba4a5da`、behavior `52fb6ebb9ae19b43efbc2f8f8cb1b3ad6c50dd4faa84702baa89c1c0139a0422`、source_frozen=true。[live/original-acceptance.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-delivery-barrier-fix-20261120/live/original-acceptance.json)及后续GO保留d02每writer65/65、API重叠21对的事实，但旧完全串行application trace门拒绝；canonical `execution_stage=reached`、mutation_receipt.hit=false、oracle=not_applicable，exploratory audit evidence_valid=true不等于oracle pass或qualified real_judged_execution。该run `s1-concurrent-d02b438-d137-20261120`仍按原判保留，不以后续修复追认。
3. **application trace审计修复，仅新执行获pass**：[main-commit-GO.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-application-trace-fix-20261120/main-commit-GO.json)解释旧完全串行门与已独立审计publish group冲突；改为完整有序begin/done列表及分块，普通action块保持互斥，并发块包围独立审计的group worker protocol，保留原action proof、oracle与source gate。[independent-review.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-application-trace-fix-20261120/independent-review.json)为只读静态审查、no_code_level_freeze_blocker，reviewer没有运行测试或live；helper本身不授overlap、交付、mutation或candidate。该独立代码审查与下述执行audit分账，不能互替。

### 新source下的一次真实原执行＋strict unchanged-input replay

[source-frozen.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-application-trace-fix-20261120/source-frozen.json)与[main-acceptance.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-application-trace-fix-20261120/main-acceptance.json)一致：HEAD `5b2ab31e38ae40e21a4dfbaa6f6cf5642b1850ca`，behavior `33def3c53e1d9d1744775b69884827e85c23ee21e05483443db83c3a04ef8c44`，source_frozen=true、dirty_behavior_paths=[]。这是工件冻结身份，不是writer测量当前源码的声明。

- **原执行**：2W1R，RMW=`rmw_cyclonedds_cpp`，domain=137，run=`s1-concurrent-5b2ab31-d137-20261120`；publish API重叠25对，w0/w1各65/65，窗口完整，cleanup_confirmed=true。[live/original-acceptance.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-application-trace-fix-20261120/live/original-acceptance.json)记录judged/oracle pass，main摘要记录独立audit evidence_valid=true。原raw路径及SHA载于main摘要；SHA=`139546929ddbf6fd64d28c21b34336b560724b1df63bb1f93bb70c81901f4580`。
- **严格重放**：domain=138，同一RMW、unchanged-input strict replay；publish API重叠20对，w0/w1各65/65，窗口完整，cleanup_confirmed=true。[live/delivery.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-application-trace-fix-20261120/live/delivery.json)、[live/replay-acceptance.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-application-trace-fix-20261120/live/replay-acceptance.json)及main摘要共同记录strict replay pass、audit valid、real_ros_execution=true、comparison_matches_original=true。replay raw SHA=`2f42d641bb8b2a0710552ca87a3f80cfc533498325a9f5a7304e10fc13f3413d`。此处20/25重叠对数不同不被改写成相等；comparison一致按原审计合同记账。replay产物目录为该组`live/replay/`，不另造未知replay run_id。
- **测试只引用、不重跑、不求和**：delivery GO记录86 passed in 3.86s；application GO记录129 passed in 20.33s及最终窄负例8 passed in 2.82s。上述suite有重叠，不合计成223项独立测试。e5摘要另记88pass，仅为该历史修复记录，也不累计。writer未运行这些测试，不声明全仓通过。

### 验收边界与仍开放缺口

这只关闭**L02这一2W1R输入、这一Cyclone实现的短闭环**，不关闭全部L02、跨实现并发、完整HB/调度覆盖、高吞吐成熟或排序rule。排序仍为supporting observation，`oracle_coverage=not_declared_by_target_rule`，不能借in_order observation新授排序成熟或candidate。修复不新增rule/target revision；既有四required target/revision0与72分母不变，新增required成熟=0，没有本卡新required seal。旧source失败、unknown与旧seal不可追认成5b2ab31成熟证据。

CFT八路径开发已离线准备，main `git apply --check`通过；**截至本cutoff未合入、未实跑**，此项来源为本卡用户给定main状态，writer没有运行apply检查。L01、L03–L08完整缺口及L02其余义务保持开放，不把离线准备、计划或短闭环扩大为完成。自然candidate=0，2–3个真实SUT候选目标未达到；不拿工具失败、negative/control或修复后的pass凑数。S1 goal **active，非complete**；本卡未改变目标状态、规则、scope或成熟分母，也无新增scope escalation/本卡执行abort。

**本writer静态交付（verified，仅限所列检查）**：[static-evidence-check.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-closure-docs-20261120/static-evidence-check.json)记录11份终态JSON解析、50个去重引用文件SHA-256匹配、source及关键判定字段静态断言通过；[append-prefix-check.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-concurrency-closure-docs-20261120/append-prefix-check.json)记录三文档追加前字节数/hash及追加后完整prefix相同。命令为该交付目录下`python3 static_check.py`与`python3 append_docs.py`（在主仓库执行、脚本完整路径见目录）。初次`python`命令不可用exit127，改python3成功；最初按根目录查找replay-acceptance.json不存在，纠正为实际`live/replay-acceptance.json`后核验通过，未运行replay。未运行ROS/DDS/campaign/replay/process scanner/pytest/source检查，无stage/commit，无下级agent；未写代码、测试、配置、README或AGENTS。本文及两份台账追加后停写，不擅自跟踪后续live。

## 2026-11-20 CFT能力固定cut：b9daf7f（不扩展成熟声明）

- **已整合，不再是前卡“未合入”状态**：main已整合提交8路径CFT input／可选MessageInfo采集，见[apply GO](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-main-integration-20261120/main-apply-GO.json)、[commit GO](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-main-integration-20261120/main-commit-GO.json)、[source冻结](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-main-integration-20261120/source-frozen.json)：HEAD `b9daf7f1e0c9f7c1d1e52aa7481d51548750887d`，behavior `8164045a76479dd3d22bc430767ca20adfdb218aabe3f68dcf225822216fa7fc`，source_frozen=true、dirty_behavior_paths=[]。局部33pass/6.97s仅引用main结果，writer未运行。5b2并发pass在此新source后仅为历史，待最终稳定source统一重跑；旧失败／旧seal不追认。
- **两条真实canonical exploratory行，非成功闭环**：[Cyclone原profile/domain140验收](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-main-integration-20261120/live/main-acceptance.json)与[FastDDS显式profile变体/domain142验收](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-main-integration-20261120/fastdds-capability/main-acceptance.json)，run分别为`s1-cft-b9daf7f-d140-20261120`／`s1-cft-b9daf7f-d142-20261120`。后者不是same-input replay；两行actual_run=true、runtime_status=completed_with_unknown、canonical execution_stage=aborted、oracle=unknown、audit evidence_valid=false、candidate=0、cleanup=true，raw路径/hash在各验收工件中保留。
- **只采用[v2能力摘要](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-main-integration-20261120/capability-summary-v2.json)的有效诊断**：Cyclone set/get明确unimplemented。FastDDS初empty set失败；reject set/get matched；restore set成功但get失败，不授恢复。r0实际GID后缀`0604→0704→0804`，logical/process generation仍1，旧token snapshot门阻后续4个publish（repeat_rejected/reject_sentinel/repeat_restored/restore_sentinel）。v2 canonical exact error=`delivery_snapshot_fields_incomplete`：这是工具当前对CFT身份切换的合同缺口，不是SUT候选。[初版摘要](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-main-integration-20261120/capability-summary.json)中缺`after_index`的TypeError是诊断命令错误，已保留，不作有效runtime诊断；v2修正调用，原run/raw/report未改。FastDDS publication_sequence_number observed，Cyclone unavailable_api；两者source_timestamp/received_timestamp observed，不称MessageInfo publisher_gid直接身份。
- **保持开放**：sentinel barrier另有agent三路径隔离实现，截至本cutoff尚未main应用（来源：本卡用户给定状态），不写已部署或live通过。L04及其他完整缺口保持，含动态endpoint身份切换、FastDDS多endpoint直接match、独立repeat occurrence映射、sentinel/phase闭合及filter effect/rule/replay资格。72分母不变，新增required成熟0，自然candidate0，无新seal，S1 goal active非complete；不把API成功、可选字段采集或unknown算成熟。
- **本卡静态验证**：[static-check.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-capability-docs-20261120/static-check.json)记录8份JSON（本批6件＋初版诊断历史＋上卡main验收）解析及48个去重引用文件SHA-256匹配；[independent-verification.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-capability-docs-20261120/independent-verification.json)为追加后独立重读hash/prefix检查。仅三docs append-only及自有交付目录写入，保留旧prefix/dirty；未改代码/README/AGENTS，未运行ROS/进程扫描/tests/replay/campaign、stage/commit或下级。实际模型继承工具配置；本批完成后停写，不追后续源码/live。

## 2026-11-20 sentinel固定cut（仅样本观察）

依据[main四工件](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-sentinel-barrier-20261120/)：source `4fc149c75427080a7b85373eacd541949d439d0b`已冻结。delivery_sentinel接入统一模型/runtime与CFT四phase；main修malformed keys/诊断理由；跨worker stdout顺序非DDS因果，采用同clock publish_begin<=receive。两旧depends断言改为更强observed-before-snapshot；446pass2fail及诊断red保留，最终36pass/4.93s仅引用，writer未跑。

Cyclone/domain144/run=`s1-cft-4fc149c-d144-20261120`：四barrier全true，每phase 1writer/2reader，共8个raw witness/hash已由main独立重算；仅闭合指定样本观察，非滤效/直接GID身份。三set仍unimplemented；整row completed_with_unknown/oracle unknown/audit false（stage aborted），candidate0、cleanup true，未strict replay，原raw/report保留。

b9 FastDDS reader GID切换、pubseq/occurrence、Npeer匹配及独立滤效rule仍缺；不得弱化身份门或改初始empty场景补洞。5b2并发原+重放仅历史，待最终source统一rerun。L04及其他L项缺口保持；72固定、新增required成熟0、无新seal、自然candidate0、goal active非complete。仅append及静态prefix/顶层JSON hash校验后停写，不追后续。


## 2026-11-20 CFT动态reader GID回执固定cut（局部合同，不是整体成熟）

- **来源与冻结**：本卡依据main固定cut及[main commit GO](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-transition-20261120/main-commit-GO.json)、[source冻结](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-transition-20261120/source-frozen.json)；HEAD `42937c8e5484f29d27343fdbe036fc7755743703`，behavior `448d84532864ff0b61b3300a3756e7bbd373221dabb2ae5ce5c11057c19cbfe9`，source_frozen=true、dirty_behavior_paths=[]。writer只读git show确认11个业务/测试路径提交，不作新的冻结授权。
- **局部修补与测试归属**：据main，独审4个P1（alias、严格ACK、跨update/API时钟、新窗口closure旧GID）均由main红测复现并修正；局部合同可冻结，不代表S1成熟。main 637pass/59.18s是最后closure修补前历史整合；最后修补后[final-window-regression](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-transition-20261120/final-window-regression.txt)为195pass/11.19s；worker85pass独立集，不相加。以上是main证据引用，writer未运行业务tests。
- **真实运行切片**：[main验收](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-transition-20261120/live/main-acceptance.json)与[main从持久raw独立重算的identity验收](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-transition-20261120/live/independent-identity-acceptance.json)：domain146、RMW `rmw_fastrtps_cpp`、run_id `s1-cft-42937c8-d146-20261120`。3workers、3updates/2GID transitions，r0唯一creation且logical/process generation不变；6send覆盖全部动作、11receive、guards=[]，83条r0 snapshot链通过，4sentinel共8witness。cleanup=true、descendants=[]、pre/postflight ok。此identity重算工件不是另一个reviewer的live通过声明。
- **raw身份**：[raw.jsonl](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-transition-20261120/live/original/cases/s1-cft-42937c8-d146-20261120.0000.S1.topology.program/runtime/inner-runtime/raw.jsonl)，SHA-256 `eac03b1373800d430a358afb6c121b1cc7ee1e7fa7b95e70bee1b08be641a16a`，writer仅静态重算hash匹配，不重放runtime。
- **仍未闭环**：whole runtime completed_with_unknown、canonical execution_stage=aborted、oracle=unknown、audit evidence_valid=false；matched barrier timeout、delivery window incomplete。初empty set失败，reject set/get matched；restore set成功但get失败，不授restore。[occurrence观察](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-transition-20261120/live/occurrence-observations.json)仅capture：MessageInfo publication_sequence_number 1..6 observed，repeat occurrence仍unknown，不能按declared seq0凑独立occurrence，也不冒称直接per-sample publisher GID。dynamic滤效拒绝原因 `independent_api_call_evidence_invalid` 是下一消费接线缺口：API事件beforeGID与update afterGID需显式transition合同，不当SUT候选。现有audit还保留worker_exit_token_mismatch:endpoint_gid等boundary资格错误，不能被局部identity链通过覆盖。
- **诊断失败与后续独审边界**：据main固定cut，首版verify_identity.py因state_step_before.action部分为字符串触发AttributeError；原script/log保留，v2增加dict shape检查后成功，未改原runtime/raw/report。本卡不预先宣称稍后live/independent-review.json已通过，不以其作为本次依据。
- **账本结论**：旧b9失败不追认；5b2并发仍历史，待最终source统一rerun。L04及其他既有缺口保持。自然candidate=0、required分母=72、新增成熟=0、无新seal、未strict replay、goal active非complete；不改target/revision/rule/分母。本卡未收到新的target revision，不推定扩展required。
- **文档writer静态交付**：仅本批三docs append-only及自有目录 `/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-docs-20261120`；旧dirty/prefix保留，不改README/AGENTS/业务/测试，不stage/commit、不运行ROS/campaign/业务tests/replay、不派下级。静态记录见[static-check.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-docs-20261120/static-check.json)；只证明文件prefix、链接及证据hash，不证明业务通过。


## 2026-11-20 CFT exit consumer第二固定cut（新run，不追认上卡）

- **main来源与source**：[exit commit GO](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-transition-20261120/exit-commit-GO.json)、[exit source冻结](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-transition-20261120/exit-source-frozen.json)：HEAD `02b2427a4ccd75230c92e783935f84c0f2b5c6f5`，behavior `bcef992019bd83f1fa608d87bdaa0ce932942be5a42b4fb803f8f25783db6998`，source_frozen=true、dirty_behavior_paths=[]。writer只读git show确认新增4path提交：boundary_audit.py、replay.py及2tests，不修改代码。
- **局部consumer修补**：据main固定cut，上卡尚缺的exit consumer现按完整transition+ACK的raw-prefix解析exit新身份；creation/binding GID不改，metadata current GID/ordinal/ref严格验证，不能借用未来transition。[main相关集](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-transition-20261120/exit-consumer-related-tests.txt)258pass/96.21s；最终future-transition guard后[最终局部回归](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-transition-20261120/exit-future-regression.txt)32pass/0.09s，集合重叠不相加，writer未运行tests。局部合同与整体S1成熟分开记账。
- **新真实run**：[main exit验收](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-transition-20261120/exit-live/main-exit-acceptance.json)、[main identity重算](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-transition-20261120/exit-live/independent-identity-acceptance.json)、[main整行验收](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-transition-20261120/exit-live/main-acceptance.json)及[main固定audit工件](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-transition-20261120/exit-live/independent-audit.json)：domain148、RMW `rmw_fastrtps_cpp`、run_id `s1-cft-02b2427-d148-20261120`。6send/11receive、3updates/2transitions，r0唯一creation、logical/process generation不改；81条r0 snapshots、4sentinel共8witness、guards=[]。cleanup=true、descendants=[]、pre/postflight ok。以上只引用main固定工件，不提前宣称另一reviewer的exit live独审通过。
- **局部结果与raw**：新run lineage_errors=[]；nested boundary consistency valid=true、errors=[]，旧worker_exit_token_mismatch:endpoint_gid和worker binding chronology错误在新run消失。raw为[raw.jsonl](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-transition-20261120/exit-live/original/cases/s1-cft-02b2427-d148-20261120.0000.S1.topology.program/runtime/inner-runtime/raw.jsonl)，SHA-256 `a648149dd2491e553c2c3dde11add33924a25cb62c90e8064d3e4565c663b7b8`，writer静态重算匹配。新run不是旧429 run追认，不修改旧runtime/raw/report或失败结论。
- **仍未闭环**：whole runtime completed_with_unknown、oracle=unknown、audit=false，canonical execution_stage=aborted；nested consistency通过不等于whole audit通过。matched barrier timeout、delivery window incomplete、重复occurrence与filter effect API before/after身份消费合同仍缺；不能按重复declared seq0凑独立occurrence，不能以set成功授restore/滤效或归因SUT候选。该cut仍未strict replay、自然candidate=0、无新seal、required分母72固定、新增成熟0、L04及其他既有缺口保持、goal active非complete。未提供新target revision，不修改rule/scope/分母。
- **append-only静态边界**：本次仅三docs追加，完整保留上卡每份4,379 bytes及此前dirty prefix；自有交付[static-check.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-exit-docs-20261120/static-check.json)及[independent-verification.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-identity-exit-docs-20261120/independent-verification.json)记录prefix/hash/bytes和链接检查。不改其他文档/业务/测试，不stage/commit，不跑tests/ROS/campaign/replay，不派下级；完成本cut后停写。


## 2026-11-20 API incarnation与publish occurrence两固定cut（新增观察，不授整体成熟）

### Cut A：0cf112a API before/after incarnation消费

- **来源与冻结**：[main GO](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-effect-incarnation-20261120/main-commit-GO.json)、[冻结](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-effect-incarnation-20261120/source-frozen.json)：HEAD `0cf112aa2b9bfee4fb65eba2ad67b61e5a9bb27e`，behavior `ea9c54c59489836b3f99d1e373b69f36a17d6c92d8e4517c8b331bf3c67b156b`，source_frozen=true、dirty_behavior_paths=[]。据main，CFT API before/after incarnation消费合同修复，相关260pass（GO记29.86s），不是旧429/02卡重写。
- **新run与独审边界**：domain150、RMW `rmw_fastrtps_cpp`、run_id `s1-cft-0cf112a-d150-20261120`；[main identity重算](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-effect-incarnation-20261120/live/independent-identity-acceptance.json)、[main exit重算](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-effect-incarnation-20261120/live/main-exit-acceptance.json)、[main API验收](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-effect-incarnation-20261120/live/main-api-effect-acceptance.json)所列局部检查通过。[独立local API incarnation review](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-effect-incarnation-20261120/live/independent-local-api-incarnation-review.json) local_review_pass=true、errors=[]，仅局部API身份/readback观察及记录的cleanup，不是滤效、execution-boundary资格、oracle或成熟通过。[whole验收](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-effect-incarnation-20261120/live/main-acceptance.json)仍unknown/audit false/candidate0。旧raw仅只读诊断，非replay；旧失败保留。

### Cut B：9654504 原正文重复的独立occurrence观察

- **source与实现归属**：[main GO](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-publish-occurrence-20261120/main-commit-GO.json)、[冻结](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-publish-occurrence-20261120/source-frozen.json)：HEAD `965450467397a26f8567c41de27e92dd785d0b06`，behavior `484f66e7e8bea04c25feab579cac77acc05a722e307ed4b70634b1f56b6b8113`，source_frozen=true、dirty_behavior_paths=[]；writer只读git show确认9个精确代码/tests路径。main实现以既有ROS UST tracepoint同线程显式publish scope包围原publish once；ABI-pinned公开rcl_take单一owner保留真实publisher_gid，无新publisher、不改String.data。独立scope+GID+精确native source stamp+control pubseq join拒绝碰撞、复用、坏receipt downgrade，不按seq offset或近似时间关联。[最终19files相关集](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-publish-occurrence-20261120/final-related-tests-v3.txt)575pass/9.05s，仅引用main结果，不与旧测试相加。
- **真实原场景**：[输入profile provenance](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-publish-occurrence-20261120/live/input-profile-provenance.json)保留initial空→reject→空restore和完整正文重复，无场景简化；FastDDS `rmw_fastrtps_cpp`、domain152、run_id `s1-cft-9654504-d152-20261120`。[运行返回](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-publish-occurrence-20261120/live/returncode.json)为30.712936s（约30.713s）、rc0，rc0不表示whole通过。[main occurrence验收](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-publish-occurrence-20261120/live/main-occurrence-acceptance.json)证明6 producer scopes/11 sample joins；r1同正文original/rejectrepeat/restorerepeat分别独立观察pubseq1/3/5，仅本次观察，非通用偏移推导。2条repeat observations为`identical_input_occurrences_observed`，effect=None、candidate=false，不升级滤效。
- **identity/exit及剩余门槛**：[main identity独立重算](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-publish-occurrence-20261120/live/independent-identity-acceptance.json)与[main exit独立重算](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-publish-occurrence-20261120/live/main-exit-acceptance.json)局部pass，nested boundary consistency=true/errors=[]；cleanup=true、descendants=[]、guards=[]、pre/postflight正常。[whole验收](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-publish-occurrence-20261120/live/main-acceptance.json)、[固定audit](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-publish-occurrence-20261120/live/independent-audit.json)仍runtime completed_with_unknown、oracle=unknown、canonical execution_stage=aborted、audit=false；matched barrier timeout、filter窗口未闭合，initial empty与restore readback不可用原状保留，未strict replay。不因独立occurrence观察而授restore/滤效或把缺口归为SUT候选。
- **失败、审查与诊断保留**：据main固定cut，native探针初次未source ROS导致lib依赖缺失，v2激活通过；receiver首native加载误向librmw.so查符号，vendor pin修复。review的2个P1及2个P2有红绿修复与[review-v3-final](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-publish-occurrence-20261120/independent-producer-review-v2/review-v3-final.json)局部确认，不是SUT候选，也非全consumer/live独审。main验收脚本初以纯observation与含envelope runtime直接比较触发AssertionError，原verify_occurrence.py/log保留，v2按semantic key比较通过；未改raw/runtime/report、未重跑campaign。
- **另一个live独审**：本卡读取时independent-occurrence-review.json已存在，但不纳入本卡固定证据，不据其宣称通过。
- **总账不变**：自然candidate=0、required分母72固定、新增成熟0、无新seal、goal active非complete。L01–L08、64广度/13链及最终72验证等既有缺口不删除；旧source运行与失败保留，不跨source追认，不改target revision/rule/scope。
- **静态raw身份**：[raw.jsonl](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-cft-effect-incarnation-20261120/live/original/cases/s1-cft-0cf112a-d150-20261120.0000.S1.topology.program/runtime/inner-runtime/raw.jsonl) SHA-256 `ff52675181303e286bf9fb581cefc8d8ebfeb05f76973f90b89dad60f4e21ebb`，writer仅重算hash，不运行replay。
- **静态raw身份**：[raw.jsonl](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-publish-occurrence-20261120/live/original/cases/s1-cft-9654504-d152-20261120.0000.S1.topology.program/runtime/inner-runtime/raw.jsonl) SHA-256 `35a3295d9a316aa984ab6d2e9d1508b399ecf3fba2dbfd3309743b0910c64b36`，writer仅重算hash，不运行replay。
- **writer边界**：本卡仅三docs append-only，旧两卡及既有dirty prefix完整保留；自有[static-check.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-publish-occurrence-docs-20261120/static-check.json)与[independent-verification.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-publish-occurrence-docs-20261120/independent-verification.json)记录hash/bytes/链接。未改业务/tests/README/AGENTS，不tests/ROS/campaign/replay、不stage/commit、不派下级，交付后停写。

- **2026-11-20 occurrence独审补记**：[independent-occurrence-review.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-publish-occurrence-20261120/live/independent-occurrence-review.json) 已核读local_review_pass=true、errors=[]，覆盖6 producer scopes/11 direct-GID＋精确stamp＋完整正文joins、显式unfiltered control、同正文pubseq1/3/5及两repeat mapping、source前后与5行为源码hash、cleanup顺序/producer unregister和原件hash不变；仅局部occurrence独审，非L04成熟。whole unknown/audit=false、effect=null、candidate=false不提升；72固定、新增成熟0、无新seal、goal active非complete。


## 2026-11-20 FastDDS native peer/runtime固定截面（仅本消息cutoff）

- **首native真实run失败保留**：source `808f040c17c1e5adf866c605b67a418a7c691327`、domain154、run_id `s1-cft-808f040-d154-20261120`；[失败记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-fastdds-peer-runtime-20261120/raw-envelope-failure-record.json)保留inner `KeyError: event_index`（原错误KeyError:'event_index'），归因harness/tool integration，不是SUT候选。[失败raw局部重算](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-fastdds-peer-runtime-20261120/failed-run-native-local-recheck.json)仅3条native snapshot合同通过，whole_execution_passed=false，不追认原run。[envelope红绿](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-fastdds-peer-runtime-20261120/raw-envelope-red-green.json)与[独立review](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-fastdds-peer-runtime-20261120/raw-envelope-independent-review.json)记录修复、无P1/P2；34pass/2.41s仅相关suite，writer未运行。
- **main两路径提交与冻结时点**：main显式提交 `49a1d279c306b0fe375f222f53c4559d417fe56e`；[live-v2 source冻结](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-fastdds-peer-runtime-20261120/live-v2/source-frozen.json)记录behavior `2804c7a68a5d7a8e4ca05f084eaad55117e196cc5fa68dcab8bc872720fd6ee0`、source_frozen=true、dirty_behavior_paths=[]，仅该时点身份。按main本消息，新源码此后已再次dirty，正于独占写集修空CFT独立状态观察与replay metadata，未交付；本卡不追逐源码、不预填修复成功、不把历史冻结当当前clean声明。
- **live-v2终态分层**：domain156、run_id `s1-cft-49a1d27-d156-20261120`；[main验收](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-fastdds-peer-runtime-20261120/live-v2/main-acceptance.json) actual_run=true、runtime_status=completed，但canonical row execution_stage=aborted、oracle=unknown、audit=false（real_judged_execution_missing），返回码0不等验收通过；cleanup confirmed。[main native peer raw复算](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-fastdds-peer-runtime-20261120/live-v2/main-native-peer-acceptance.json)45/45 native snapshots及8完整窗口通过，仅native full-set合同与既有窗口重算，不代表filter effect/typed oracle成熟。本卡读取时live-v2独审文件尚无，独立复核待验，不能写独审已通过。
- **三条CFT effect unknown原文保留**：initial empty与remove/restore分别reason=`update_api_or_actual_readback_unavailable`；reject为`positive_control_predicate_or_sentinel_missing`，均filter_effect_observed=null、candidate_eligible=false。两条empty路径真实API错误原文：initial set `RCLError:Failed to set cft expression parameters: current subscriber has no content filter topic, at ./src/rmw_subscription.cpp:119, at ./src/rcl/subscription.c:500`；remove/restore get `RCLError:Failed to get cft expression parameters: this subscriber has not created a ContentFilteredTopic, at ./src/rmw_subscription.cpp:229`。reject set/get matched不单独授滤效；不把empty API不可用解释成disabled已证。
- **prior control消费者修补仅离线重算**：据main本cutoff，topology_pair补用早先完整同正文control窗口；[v2相关集](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-fastdds-peer-runtime-20261120/cft-prior-control-tests-v2.txt)48pass/1.18s；v1五fail为新fixture phase/request不匹配，修复fixture，不是SUT。[dirty consumer对旧raw重算](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-fastdds-peer-runtime-20261120/cft-prior-control-recalculation.json)为delivery_contrast、control_equivalence=complete_String.data、filter_effect=null、candidate=false、raw_unchanged=true、original_run_remains_unknown=true。它使用冻结后新dirty消费者，不是新execution或strict replay，不改原runtime/raw/report的unknown。
- **下一缺口与完整目标**：disabled真实状态证据、filter决策独立归因/typed oracle、严格replay仍待；全范围64广度/13链/最终72及L01–L08义务不删、不缩scope。自然candidate=0、required分母72不变、新增成熟0、无新seal、L04未成熟、完整goal active非complete。当前开发未交付不预填，旧失败保留；不跨source沿用成熟结论。
- **writer静态边界**：本卡只三docs append及自有[delivery.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-fastdds-peer-runtime-20261120/docs-followup/delivery.json)；完整旧prefix/dirty保留，指定JSON与链接静态核对、两份raw hash重算，不运行ROS/build/campaign/tests/replay，不改业务/tests/README/AGENTS，不stage/commit、不派下级，交付后停写。


- **2026-11-20 CFT native decision接续**：见[固定cut短卡](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-cft-native-decision-progress-2026-11-20.md)。d4f1098/domain162为completed/judged unknown、audit evidence_valid=true；2条actual evaluate capture＋main局部核查不等typed effect完成。seq2（新raw ref395）actual reject receipt尚缺facts/history消费；须严格限于实际reject构造per-send exemption、缺证保留unknown，再新source冻结重跑原场景＋strict replay。独审未交付不预填；candidate0/72固定/新增成熟0/无seal/L04未成熟/goal active，64/13/L01–L08/最终required与完整审核保留。


- **2026-11-20 typed链路固定增量**：见[短卡末尾v6/v7接续](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-cft-native-decision-progress-2026-11-20.md)。v6 signature=None原失败不追认；16eee46/domain166原run及domain167 strict replay main验收pass、comparison匹配/audit有效，missing[2]保留而exact reject受typed证明；最终独审待交付，不授L04全能力或S1成熟。64/13离线身份非执行；domains168–171运行中不读取/预报。candidate0/72固定/新增成熟0/无seal/goal active，全范围及2–3真实候选目标保持。


- **2026-11-21固定cut**：见[接续短卡末尾](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-cft-native-decision-progress-2026-11-20.md)。db4f12e/v8原172＋strict173经21checks局部独审，非L04/S1成熟；旧source73/77与新9e98615/P16四请求终态分账，不能拼统一77成熟。94pass1fail历史保留＋2 targeted pass不等全95重跑。current-source剩余73运行中，仅记录main状态，不读取/推断。candidate0/72固定/新增成熟0/无seal/goal active，全范围义务保留。


- **2026-11-21 final固定cut**：见[短卡末尾](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-cft-native-decision-progress-2026-11-20.md)。9e98615同source77已处置（52actual/32qualified pass/20nohit/25非执行），33行operator gaps保留；current CFT88/89与Phase3三组strict bounded通过，wrapper误报及更正分存。Fast并发96 unknown/aborted/auditfalse与wrapper KeyError保留，非candidate；batch stage未合入未验证，perf不合入不称提速。candidate0/72固定/新增成熟0/无seal/goal active，全范围义务不删。

- **Fast130非终态checkpoint**：见[短卡末尾](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-cft-native-decision-progress-2026-11-20.md)：5bc831f batch代码P2闭合；domain98 raw130/130及proof gate v2仅局部观察，canonical cycle/report未完成、replay99未启动，非L02闭环。离线单receive profile不能解释全部延迟，未生产优化；旧source证据历史，candidate0/72固定/整体未完成。

- **更近期status补记（不覆盖前一瞬间pending）**：5bc831f/domain98的[cycle.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-phase3-fast-concurrency-20261121/live-v2/original/cycle.json)及[report.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-phase3-fast-concurrency-20261121/live-v2/original/report.json)已落盘，main记录returncode0；row judged/pass、actual_run=true、execution_qualification.real_ros_execution=true、abort=None，原130新source正式原运行已pass。wrapper独立audit仍补充中；本cut strict-replay-invocation.json不存在、replay99尚未启动，不能提前写原＋重放闭环；旧checkpoint及失败保留，candidate0/72/整体未完成不变。

- **Fast130/Episode固定增量**：见[短卡末尾](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-cft-native-decision-progress-2026-11-20.md)：5bc原98＋strict99 main AND通过，最终独审待；Episode新9e6f62a离线67pass、0live，L01 session/checkpoint/reset统一runner未完，旧运行不授新seal。性能补丁完整projector变慢而停止，未合入不称提速；candidate0/72/整体未完成。

- **最新L01开发切面**：见[短卡末尾](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-cft-native-decision-progress-2026-11-20.md)。Fast130历史5bc最终独审已完成/main accepted；L01 opt-in输入独审限定通过、default bytes保持，HEAD9e6f62a＋dirty的runtime持久化接线仍开发中、独审pending且未live。110/1/120相关测试分记、不算全仓或成熟；candidate0/72/无seal/完整S1未完成，全scope保留。

- **L01终态增量**：见[短卡末尾](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-cft-native-decision-progress-2026-11-20.md)：977f7bd原102＋strict103 bounded session/persistence/fresh-reset通过，644checks独审/main accepted；旧100 unknown及4原件保持。非checkpoint续跑或distributed DDS snapshot restoration，L01整体未完；candidate0/72固定/新增成熟0/无新seal，L02–L08/controls/最终审核及goal active保持。

- **checkpoint重建终态增量**：见[短卡末尾](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-cft-native-decision-progress-2026-11-20.md)：32759d8原104＋两checkpoint前缀重建105/106 bounded通过，1201checks/148files独审、main accepted。正常完成origin，不是突发crash恢复；无terminal读取为离线验证，不授DDS/hidden-state恢复或L01整体成熟。测试失败/两个P2历史保留，旧977证据不继承；candidate0/required72/新增成熟0/无seal/goal active及L02–L08/controls义务不变。
