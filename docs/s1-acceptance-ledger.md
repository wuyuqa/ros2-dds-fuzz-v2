# 完整 S1 逐项验收账

核对时间：2026-11-15T00:24:53+08:00（Asia/Shanghai）。来源：原任务书、主agent工程裁定/验收派遣消息、文档子agent只读核对的本地文件。仅更新文档工作树，不暂存/commit，不启动ROS/campaign；当前无需要用户确认的事项。

目标口径纠正（2026-11-15T00:45:07+08:00，Asia/Shanghai；来源：本轮用户明确指令及主agent审阅意见）：当前用户目标为“S1模块完全成熟且有2–3个真实SUT候选”。已记录的真实自然候选为0，因此候选数量目标尚未达成。自然数量无法预先保证，只能由真实运行产生；负控、工具错误和合成案例不得凑数。原taskbook允许zero-candidate推进Phase，不代表用户完整目标完成；可以交中间报告，但不能据此将整个goal标记complete。

## 本轮文档 freeze-ready 收束

记录时间：2026-11-15T01:09:04+08:00（Asia/Shanghai）。来源：main最新工程进展及独立测试报告，由本轮消息提供；文档子agent未复跑测试或运行ROS，未取得本条消息对应的新增产物路径，不补造路径/hash。

- third v4 两RMW runtime+ingest 已报告 judged/pass，但 `capsule_execution=null`，输入为 `reconstructed_input`；仅为development证据，不能写成真实capsule驱动或capsule执行链已贯通，更不能替代正式18+6/control/seal。下一步仍需真实capsule-driven neutral/fault执行及其独立raw、输入/执行身份、receipt和归因证据。
- main独立新增20项replay测试、31项third runtime/campaign测试通过；这些是离线测试结果，尚未取得真实replay证据。F07真实健康重放与可控工具失败重放仍待验收，原始run保持不变。
- Phase2 bridge正在新增；接口或合同实现不能替代真实trace、lifecycle及blackbox对照，Phase2→Phase4闭环仍有缺口。
- 当前用户2–3个真实SUT候选目标仍未达成：实测自然候选0，不以control、工具错误或开发pass充数；72格成熟分母不变，旧source seal仍只支持旧source。
- 本轮文档已整理为freeze-ready，含义仅为可交main审阅；不代表源码已冻结或S1已成熟。本次不暂存/commit。交付后停止文档轮询和自动追加，等待main下一里程碑明确派遣。

## 已验收source身份与当前开发边界

已验收基准HEAD：`11ec76c249cb75605fd70c5d7efb5610e70514b1`；behavior SHA-256：`c1daa3a066e69229f73c76a3d52123a8090173bca7abd4b56d951eb85539f72e`。相应运行manifest记录source_frozen=true/dirty=[]；主agent现已解冻third开发，四方在独立写集推进，不能把运行时冻结状态当成此后工作树始终冻结。文档不commit，HEAD未变也不免除行为内容校验。

更新（2026-11-15T00:31:41+08:00，Asia/Shanghai）：first r1/domain180、second r2/domain80均在上述source上满足per-target seal：各18 required judged pass、6 negative judged fail、四control6/6、unknown0，独立verify rc0、sealed=true/errors=[]。旧first失败及second domain181预检阻断均保留。**这些通过只属于11ec76c版本，后续source变化须在最终source冻结后重跑；不作为新source的成熟声明，也不等于Phase2/4或完整S1完成。** 详细身份、hash及精确legacy合同见[正式证据快照](formal-target-snapshots-2026-11-15.md)。

`late-endpoint-r1-20261114T161459Z-7094038e`中的r1是attempt名称，manifest的target_revision仍为0；第二target同理。UTC目录原名保留，不因本地已进入2026-11-15重命名。

证据分层：正式证据仅支持其记录source与合同范围内的seal；旧source正式结果仅支持历史结论；真实开发smoke/可行性探针只支持当次有限观察；离线合同测试不代替真实执行。下列“已执行/已通过”均限定具体路径，不把没有异常的结果写成自然candidate已验证。

最终只读源码快照（2026-11-15T00:34:02+08:00，Asia/Shanghai）：HEAD仍为11ec76c，但source_frozen=false，dirty_behavior_paths已有10项，行为摘要为 `0503ce9ed8789db4e292cbba761e6f10d943a7fbc4b22479f59f4ea2aa246bd6`。这与主agent已解冻第三target开发一致，不是本轮文档修改导致；工作树仍在变化，不把此摘要当未来冻结身份。first/second旧source通过仍保留为历史证据，最终source rerun要求已经生效。

## 证据索引

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

E01文件字节SHA-256：report=`3d1556ff3a78285b54b3c88cb185a5c3c7fba36f1d786fb15b180d0da5deaf4c`；manifest=`b55e127f9aba78d1974df4ec418a076c4fcea4bac33314b2ff30a56548481f2b`；ledger=`c712dec3857183a6d04fc39f3f1ca9a72439c01341a04c3dc6e7f1e15e094322`。E03 report字节hash为`844d064b88b826022d0c5982fea80aefa59910848c0027cc5be4e6653110c272`。E04 report字节hash为`dcffe78ebf7074a531682b7b0cef397123e1468609b83c08d3ae8bb9cf704200`。文件字节hash与report内部规范化摘要分开。

## F01–F08：框架能力与真实路径

全部八项都须离线合同；F01–F06/F08还须真实pair集成。F07须覆盖离线input、真实execution和replay failure保留。以下证据映射不把测试数量当验收结论。

| 项目 | 当前实际证据与实现入口 | 当前判断 | 缺口 / 下一份必要证据 |
| --- | --- | --- | --- |
| F01 module/target注册 | E01 manifest.catalog/target；[runner](../src/r2f/campaign/runner.py)与[registry](../src/r2f/core/registry.py)；E07 test_registry_is_generic_and_rejects_duplicate | E01/E08在11ec76c上证明first/second统一注册路径贯通 | 四target注册不代表第三/第四target真实carrier可执行；后续各自run不能绕开registry |
| F02 SemanticTarget→scenario | E01 manifest.target/rule；E02 capsule.input、report中state_steps；[scenario](../src/r2f/core/scenario.py)；E07 target/scenario与参数拒绝合同 | E01/E08已证明first/second编译结果在11ec76c上真实使用 | 第三/第四target仍须各自状态/slot/expected effect/receipt映射；E04不用于判定模型不兼容，第二target已由E08补充真实证据；最终source须重跑 |
| F03 requires/state receipts | E02 precondition_receipts和observation_receipts/raw；E01逐行stage_history；E07 steps/forged_reach/ACK回归 | E01/E08 first/second真实writer前置及后置观察已执行于11ec76c | 其他target各自时序仍待验证；第三targetmatched-before-drop不可套用late_joiner writer-only前置 |
| F04 carrier/mutation/effect | E01六mutated行及各raw；E02七项嵌套mutation receipt全true；E07 receipt/hash/neutral合同；[S1模块](../src/r2f/modules/s1/module.py) | late_joiner实际提交/命中/effect及neutral不伪hit已由当前正式run覆盖 | D015 third v4两RMW runtime+ingest已报告judged/pass，但capsule_execution=null/reconstructed_input，仅development；真实capsule-driven neutral/fault与正式hit/restore验收证据仍缺；D014 endpoint_qos_variation真实两源字段变化未证明；不能用non-DDS探针替代 |
| F05 raw→typed facts→primary oracle | E01 rows.typed_facts/oracle及24份raw/hash；[facts](../src/r2f/modules/s1/facts.py)、[oracles](../src/r2f/modules/s1/oracles.py)；E07 residual/unknown/闭合合同 | E01/E08各18 pass及6已知harness fail检出；原tool unknown保留E03 | 第三/四target独立primary/闭合/适用性仍待；first/second不得用旧source结果支持最终新source。control fail不证明自然SUT缺陷 |
| F06 candidate lifecycle与审计 | E01/E08真实zero-ledger、各六control candidate_eligible=false；E03原始失败保留；[lifecycle](../src/r2f/core/candidate.py)、[ledger](../src/r2f/reports/candidate_report.py)；E07 admission/triage/排除合同 | 按主agent工程裁定，零自然候选分支的真实审计路径已执行；不凑候选 | 自然candidate admission/triage仍未获真实异常证据；若未来出现须完整记录转换、raw/provenance、责任与处置，不能拿合成测试或control充数 |
| F07 capsule与真实replay | E01/E03共48份正式capsule；当前run24份input/execution齐备；[replay](../src/r2f/core/replay.py)；E07 fail保留、异常签名比较、fresh domain合同 | input/execution持久化已证明；真实replay验收未完成 | 48份capsule合计replay_attempts=0、execution_history=0、non_replayable=0。下一phase需真实健康重放＋可控工具失败真实重放，独立产物与attempt/failure记录，不改原capsule/raw。E03首跑失败≠replay failure |
| F08 完整report及独立审计 | E01/E08 report/manifest/ledger/capsule/raw与独立verify rc0；E03/E04失败报告；[report审计](../src/r2f/reports/run_report.py)；E07 report/ledger篡改合同 | first/second在11ec76c上的完整报告链与seal路径已证明；失败报告亦保留 | 第三/第四target正式报告/seal、完整S1处置与Phase2/4关联仍缺；registry disposition=expressible不取代seal字段，也不能扩写第一target seal |

## Phase2、Phase4及后续顺序

| 要求 | 实际证据 | 当前状态与缺口 |
| --- | --- | --- |
| Phase0/1 初始化、合同框架 | 当前git HEAD、E07、E01纵向真实pair | 已有实现/合同及第一target贯通；不能覆盖未执行路径 |
| Phase2 至少一个RMW源码/内部观察 | E05真实vendor trace、进程provenance/graph与18内部事件 | 内部观察开发证据真实存在；未声称独立源码build插装已完成。需保留插装配置、loaded binary/source/process identity及其适用范围；旧开发run不能直接支持当前source seal |
| Phase2 candidate ledger及black-box任务 | E05 control_task.status=not_run、candidate_admission=not_performed；E01 ledger.instrumentation_status=not_run、instrumentation_candidates=null、linkage=not_linked_to_this_blackbox_run | 尚未形成明确连接的Phase2→Phase4闭环；null不能写成已核实的instrumentation_candidates=0。自然候选不可制造 |
| Phase3 第一18+6及四control | E01，E03历史失败保留 | 当前第一target满足per-target seal；同revision0，新attempt不覆盖旧report |
| Phase4 有instrumentation candidate | 当前未有该类候选；E05仅真实内部观察 | 若发现候选，需同输入两个RMW×3repeat black-box validation，原candidate不因replay失败删除，补责任/原因/disposition |
| Phase4 无instrumentation candidate的bounded blackbox | E01是真实公开API required矩阵，ledger明确未链接instrumentation；E05对照任务未运行 | **不能自动把E01当Phase4已完成。** 下一步需具名bounded campaign的输入/范围/次数或时间上限、隔离blackbox环境、与Phase2关联、明确instrumentation_candidates=0及完整zero-candidate/blackbox disposition |
| Phase5A 第二target | E08 domain80已正式18+6/四control通过，独立seal=true；E04 domain181阻断历史保留 | 通过仅属于11ec76c；不推断181已无冲突，最终source改变后须重跑；不强抢域/旧owner |
| Phase5B 第三target | E06 non-DDS控制、D015工程裁定、当前registry能力blocked处置 | D015尚未实现/验证，需真实endpoint-attached discovery drop/restore链和18+6；非先验不可解 |
| Phase6/7 第四target与各target seal | D014字段定义、E05/E06及E09被动捕获开发证据、当前registry独立观察处置 | 第四target未正式执行/封存；先完成两源事实/控制，按固定顺序推进。每个target单独同revision/current source重跑18+6及seal |
| Phase8 S2入口 | 无完整S1全部稳定证据 | 不以第一target成功提前宣称S1成熟或进入S2 |

## 四 target 固定分母与控制账

每target required=两个RMW×baseline/transparent/mutated×repeat0/1/2=18；独立negative fault=6。health/mutation/transparency可各引用六条required；fault必须独立，不能重复增加执行或成熟计数。四target合计72 required＋24独立fault＝至少96条唯一执行行；历史attempt、replay和开发probe不加进72分母。

| target（均revision0） | 当前证据路径 | required / 独立fault | health / mutation / fault / transparency | seal与缺口 |
| --- | --- | --- | --- | --- |
| discovery.late_endpoint | E01 report，domain180；E03保留历史失败 | 18/18 judged pass；6/6 negative judged fail | 6/6 / 6/6 / 6/6 / 6/6 | 11ec76c版本per-target seal=true，unknown0；E03仍false |
| discovery.late_join_delivery | E08 report/domain80；E04 report/domain181保留 | 18/18 judged pass；6/6 negative judged fail | 6/6 / 6/6 / 6/6 / 6/6 | seal=true仅对11ec76c版本；E04仍false，原始preflight及后续step-count错误保留 |
| discovery.interruption_recovery | [scope](../config/s1_scope.json)、E01 scope_dispositions、E06与D015 | 尚无此target正式18+6报告 | 均待正式证据 | 当前能力处置blocked:awaiting_endpoint_attached_discovery_capability；D015待实施，不删18格 |
| builtin.discovery_projection_reconciliation | [scope](../config/s1_scope.json)、E01 scope_dispositions、E05/E06/E09与D014 | 尚无此target正式18+6报告 | 均待正式证据 | 当前能力处置blocked:no_independent_builtin_observation；D014字段定义不等于可运行，不删18格 |

11ec76c版本已有first/second各18格的per-target seal支持，共36个required格，不是72/72。主agent已解冻后续开发；最终source一旦变化，这36格不能自动转作新版本通过，须最终冻结后rerun。不得通过历史attempt累加、blocked排除或control复用伪造完整成熟。

## 跨项验收：身份、归因、replay与证据约束

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
| D014字段适用性 | 决策与第四target计划；16个本地source hash核对 | 必需GUID/role/topic/type/reliability/durability/deadline/liveliness及lease；history/depth缺失仅supporting N/A，不配置补值；其余缺失仍unknown。尚未实施/正式验收 |
| D015第三target恢复合同 | 主agent新工程裁定；仅E06非DDS载体可行性 | wire_proxy标识＋显式implementation；只有mutated真drop，hit需本pair真实discovery counter>0；restore删规则≠恢复；恢复需身份关联discovery＋fresh双向count/graph＋postrestore delivery。独立fault中间omit，不伪drop/applied |

## 下一份需要的证据

1. F07健康真实重放和可控工具失败真实重放，独立产物不改原run；当前已有48份capsule不能代替两条真实replay路径。
2. 明确Phase2→Phase4 linkage；无自然instrumentation候选时，真实bounded blackbox及具名zero-candidate disposition，不能用null默认为0。
3. 保留第二target domain181 blocked及domain80通过两份报告；第三/第四target依D015/D014实施、离线验证、冻结和正式18+6矩阵。first/second已seal revision0 manifest保持不变，core精确legacy fault兼容；新target显式receipt_kind/window及projection schema。
4. 后续每次source更新都重新评估哪些证据可用于新seal，不回填旧receipt/hash，不减少72分母，不保证候选数量。

以上由主agent日常工程协调，无新增需用户确认事项。此账不是运行调度授权，文档子agent不启动任何campaign。

最新兼容约束：first/second的target manifest不变不等于全局source身份不变；不得因shared helper增加字段而漂移既有target hash。第三/第四首次seal前初版合同修正不等于new rule已完成，不能提前标绿。精确target hash和legacy合同见正式证据快照。

补充核查（2026-11-15T00:38:53+08:00，Asia/Shanghai）：main逐字段比较当前S1Module first/second target.to_dict()与两份11ec76c正式manifest均true；文档只记录来源，不声称复跑。source dirty仍使旧seal不能用于当前成熟声明。main发现scan_processes旧repo substring把v2共享前缀路径误判为旧repo，runtime修真实路径边界；旧repo真实路径/进程/锁保护不放宽，回归验证仍待代码owner。E09已证明正常被动SEDP开发捕获/解析，但默认值策略及完整projection/control/seal尚未完成。
