# 完整 S1 逐项验收账

核对时间：2026-11-15T00:24:53+08:00（Asia/Shanghai）。来源：原任务书、主agent工程裁定/验收派遣消息、文档子agent只读核对的本地文件。仅更新文档工作树，不暂存/commit，不启动ROS/campaign；当前无需要用户确认的事项。

目标口径纠正（2026-11-15T00:45:07+08:00，Asia/Shanghai；来源：本轮用户明确指令及主agent审阅意见）：当前用户目标为“S1模块完全成熟且有2–3个真实SUT候选”。已记录的真实自然候选为0，因此候选数量目标尚未达成。自然数量无法预先保证，只能由真实运行产生；负控、工具错误和合成案例不得凑数。原taskbook允许zero-candidate推进Phase，不代表用户完整目标完成；可以交中间报告，但不能据此将整个goal标记complete。

## 本轮文档 freeze-ready 收束

记录时间：2026-11-15T01:09:04+08:00（Asia/Shanghai）。来源：main最新工程进展及独立测试报告，由本轮消息提供；文档子agent未复跑测试或运行ROS，未取得本条消息对应的新增产物路径，不补造路径/hash。

- third v4 两RMW真实development runtime+ingest均为 `judged/pass`：domain173的FastDDS/Cyclone分别观察到614/162个drop packet，restore后seq 2–7均发送并接收；但输入是 `reconstructed_input`、`capsule_execution=null`，因此只记开发证据。正式D015与18+6/control/seal仍未验收；后续仍需capsule-driven neutral/fault及完整正式矩阵。
- fourth已实际产生24行run：17 required pass、FastDDS `transparent.0` 1 unknown、6 fault行fail，transparency control 5/6，seal=false。该unknown及失败保持原样，不升级为成熟证据；D014和正式18+6/control/seal仍未验收。
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
| E10 third/fourth当前run证据 | [third v4 summary](../results/runtime-smoke/third-development-v4/summary.json)、[FastDDS ingest](../results/runtime-smoke/third-development-v4/fastdds-ingest-development.json)、[Cyclone ingest](../results/runtime-smoke/third-development-v4/cyclone-ingest-development.json)；[fourth report](../results/s1/builtin-projection-r0-20261115T050653+0800/report.json)、[manifest](../results/s1/builtin-projection-r0-20261115T050653+0800/manifest.json) | third两RMW真实development运行及restore交付观察存在，但ingest为`reconstructed_input`且`capsule_execution=null`，不能证明正式D015验收。fourth 24行中17 required pass、1 unknown（FastDDS `transparent.0`，原始记录保留；main重算为缺少graph快照对应writer GUID）、6 fault fail；transparency 5/6、seal=false。两者均是not mature状态，不改变72 required分母或first/second旧source边界 |

本次状态同步（2026-11-15，来源：用户最新指令及本地只读文件核对）：third summary字节SHA-256=`52d7ab0a8c87ae279c2c26d533a9d7a7df6c2b815d3d04651b3f7ba793800f77`；fourth report字节SHA-256=`73487d00d4ca9393712089544cc197443e41a21d5c0ede4e3628a77f26fe86b2`。main重算unknown为`no_live_wire_endpoint_at_graph_snapshot:010f7f01b56a3f980000000000000603`；不覆盖原report/raw/pcap，不把观察缺口作为自然candidate。

E01文件字节SHA-256：report=`3d1556ff3a78285b54b3c88cb185a5c3c7fba36f1d786fb15b180d0da5deaf4c`；manifest=`b55e127f9aba78d1974df4ec418a076c4fcea4bac33314b2ff30a56548481f2b`；ledger=`c712dec3857183a6d04fc39f3f1ca9a72439c01341a04c3dc6e7f1e15e094322`。E03 report字节hash为`844d064b88b826022d0c5982fea80aefa59910848c0027cc5be4e6653110c272`。E04 report字节hash为`dcffe78ebf7074a531682b7b0cef397123e1468609b83c08d3ae8bb9cf704200`。文件字节hash与report内部规范化摘要分开。

## F01–F08：框架能力与真实路径

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
| Phase5B 第三target | E06 non-DDS控制、E10 third v4真实development runtime+ingest、D015工程裁定 | endpoint-attached drop/restore已有开发运行证据；但capsule execution为空且输入重建，正式D015及18+6/control/seal仍待验收 |
| Phase6/7 第四target与各target seal | D014字段定义、E09被动SEDP开发证据、E10第四target 24行report | 已执行run但17 required pass+1 unknown、6 fault fail、seal=false；保留unknown/失败，需完成D014正式18+6/control/seal。各target仍按72分母，不删格 |
| Phase8 S2入口 | 无完整S1全部稳定证据 | 不以第一target成功提前宣称S1成熟或进入S2 |

## 四 target 固定分母与控制账

每target required=两个RMW×baseline/transparent/mutated×repeat0/1/2=18；独立negative fault=6。health/mutation/transparency可各引用六条required；fault必须独立，不能重复增加执行或成熟计数。四target合计72 required＋24独立fault＝至少96条唯一执行行；历史attempt、replay和开发probe不加进72分母。

| target（均revision0） | 当前证据路径 | required / 独立fault | health / mutation / fault / transparency | seal与缺口 |
| --- | --- | --- | --- | --- |
| discovery.late_endpoint | E01 report，domain180；E03保留历史失败 | 18/18 judged pass；6/6 negative judged fail | 6/6 / 6/6 / 6/6 / 6/6 | 11ec76c版本per-target seal=true，unknown0；E03仍false |
| discovery.late_join_delivery | E08 report/domain80；E04 report/domain181保留 | 18/18 judged pass；6/6 negative judged fail | 6/6 / 6/6 / 6/6 / 6/6 | seal=true仅对11ec76c版本；E04仍false，原始preflight及后续step-count错误保留 |
| discovery.interruption_recovery | E10 third v4 [summary](../results/runtime-smoke/third-development-v4/summary.json)与双RMW ingest；D015 | 两RMW development runtime/ingest均judged/pass；输入为`reconstructed_input`、`capsule_execution=null`，非正式18+6 | 正式四control均待验收 | development evidence / not mature；正式D015、18+6/control/seal待验收，不删18格，72分母不变 |
| builtin.discovery_projection_reconciliation | E09被动SEDP及E10 [24行report](../results/s1/builtin-projection-r0-20261115T050653+0800/report.json) | 17 required pass、1 unknown；6 fault fail | 开发run：6/6 / 6/6 / 6/6 / 5/6，非正式验收通过 | 当前run `seal=false`；FastDDS `transparent.0` unknown及原始证据保留，正式D014与18+6/control/seal未验收，不删18格，72分母不变 |

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
| D014字段适用性 | 决策与第四target计划、E09、E10第四target report | 必需GUID/role/topic/type/reliability/durability/deadline/liveliness及lease；history/depth缺失仅supporting `not_applicable_by_endpoint_schema`，禁止配置补值。实际24行run有17 required pass、1 unknown、6 fault fail，seal=false；unknown与失败保留，正式验收未完成 |
| D015第三target恢复合同 | 主agent工程裁定、E10 third v4 runtime/ingest | 两RMW development运行观察到真实drop与restore后交付，但输入重建且无capsule execution，正式D015及18+6/control/seal未验收。原合同不变：wire_proxy＋显式implementation；仅mutated真drop，hit需本pair真实discovery counter>0；删规则不等于恢复，恢复需身份关联discovery＋fresh双向count/graph＋postrestore delivery；独立fault中间omit，不伪drop/applied；72分母不变 |

## 下一份需要的证据

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
