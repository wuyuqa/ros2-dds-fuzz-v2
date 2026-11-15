# S1 验收要求与证据矩阵

日期：2026-11-14。此页是验收计划，不是测试结果。来源为 [任务书原文](taskbook.md)，范围与冲突解释以 [决策记录](decisions.md) 为准。用户最新范围由主agent转述为完整 S1；公共 API 正常互操作、可靠性及证据工具的当前执行边界由主agent声明。原规格中的其余工作逐项保留要求、核查可表达性并记录差距，不能因合同测试通过就宣称原 S1 全部成熟。

状态口径：`待验证` 表示此文档尚未审计到满足要求的证据；`已裁定/待验证` 仅表示主agent已经解释合同。没有真实检查前不标 `blocked`。所有表格的通过条件都是待证明的要求。

原规格 §2 的旧 HEAD `e18e93b1`、Batch-02 计数和 confirmed candidate=0 均是交接历史，不是本仓库实测；本轮不复用旧未提交代码、旧 raw 作为新运行，或旧 seal。§3 的目录树是建议布局，验收以统一接口和真实证据为准，不以空文件数量代替能力。

最新证据账更新（2026-11-15）：[完整S1逐项验收账](s1-acceptance-ledger.md)与[正式证据快照](formal-target-snapshots-2026-11-15.md)记录first/second各18+6及四control仅在11ec76c source上通过独立seal；first失败与second domain181阻断历史保留。当前third v4有两RMW development runtime+ingest（drop计数614/162，restore后seq 2–7送达），但输入为`reconstructed_input`且`capsule_execution=null`；D015正式18+6/control/seal未验收。Fourth run有24行：17 required pass、1 unknown、6 fault fail，transparency 5/6、seal=false；unknown/失败保留，D014正式18+6/control/seal未验收。第三/第四均为not mature，72格分母不变；first/second旧seal不适用于后续source。

目标口径更新（2026-11-15T00:45:07+08:00，Asia/Shanghai；来源：本轮用户明确指令）：当前用户目标为“S1模块完全成熟且有2–3个真实SUT候选”。已记录的真实自然候选为0，因此候选数量目标尚未达成。自然数量无法预先保证，只能由真实运行产生；负控、工具错误和合成案例不得凑数。原taskbook允许zero-candidate推进Phase，不代表用户完整目标完成；可以交中间报告，但不能据此将整个goal标记complete。

## 1. 八项主框架能力

统一接口包括 `Module`、`TargetProvider`、`MutationProvider`、`Carrier`、`FactProducer`、`Oracle`、`CandidateLifecycle`。S1 通过 registry 注册，复用主 runner、状态机、candidate 和报告系统，不另建业务独立 runner。

| ID | 能力 / 原文位置 | 离线合同测试必须证明 | 真实运行证据必须证明 | 当前证据状态 |
| --- | --- | --- | --- | --- |
| F01 | 注册 module/target；§3 | 重复/未知注册处理，S1 可由统一 registry 取得 | 至少一个真实 ROS2 pair 从 registry 路径进入 | first/second在11ec76c的真实映射见验收账；不代替最终source或新target |
| F02 | SemanticTarget 编译 scenario；§3–4 | 九个必要字段、step 依赖、requires、参数及不支持项不被静默丢弃 | 至少一个真实 pair 使用编译结果，而非单独硬编码绕过 target | first/second在11ec76c的真实映射见验收账；不代替最终source或新target |
| F03 | 带前置条件的真实 state steps；§3–4、7.4 | 合法阶段、缺 receipt/前置未达的拒绝；neutral 分支见 D002 | 至少一个真实 pair 每 step 有 before/after receipt，真实 requires 求值 | first/second在11ec76c的真实映射见验收账；不代替最终source或新target |
| F04 | 统一 carrier 应用 mutation 并验 effect；§3、7 | 嵌套 receipt、层顺序、hit gate、input hash、neutral 不伪造 hit | 至少一个真实 pair 的 mutated 输入实际提交/配置/命中，effect 可回到 raw | first/second在11ec76c的真实映射见验收账；不代替最终source或新target |
| F05 | raw → typed facts → oracle；§3、9 | 四态 verdict、缺事实不当 pass、primary 与 diagnostic 分离、residual 优先 | 至少一个真实 pair 有 raw/hash、typed facts、指定 primary rule verdict | first/second在11ec76c的真实映射见验收账；不代替最终source或新target |
| F06 | candidate lifecycle 创建/更新/审计；§3、7–8 | admission、合法状态转换、来源/归因完整、control 排除、失败不删候选 | 至少一个真实 pair 的候选审计路径；无自然候选时如实 zero-candidate，不能造 SUT candidate 来凑集成测试 | 主agent裁定零候选审计路径已真实执行；自然admission/triage仍缺真实异常证据 |
| F07 | replay capsule 与尝试；§3、8.2 | 离线 input metadata、hash 确定性、失败分类和保留 | 真实 execution metadata 写入；replay failure 可保留且可审计，不以生成 capsule 冒充重放 | input/真实execution已有；48份正式capsule无replay attempt，健康及工具失败真实重放待下一phase |
| F08 | 完整 run report；§3、14 | 必要字段/缺口、计数与证据引用、拒绝不一致 identity | 至少一个真实 pair 产出的 report 能反查 raw、manifest、capsule 和 verdict | first/second在11ec76c的真实映射见验收账；不代替最终source或新target |

八项全部要求离线合同测试。F01–F06、F08 各要求至少一个真实 pair 集成测试；可由同一条贯通测试覆盖多项，但必须逐项给证据映射。F07 另外同时覆盖离线 input、真实 execution metadata、replay failure 保留。

## 2. 第一条 slice 的精确 required matrix

固定 target=`discovery.late_endpoint`，mutation=`late_joiner`，carrier=`ros2_endpoint`，campaign_kind=`required`，target_revision 初始 `0`。不得混入 `endpoint_qos_variation`。每格包含三个唯一 repeat：`0,1,2`。

| RMW | baseline | transparent | mutated | required 行数 |
| --- | --- | --- | --- | ---: |
| `rmw_cyclonedds_cpp` | repeat 0/1/2 | repeat 0/1/2 | repeat 0/1/2 | 9 |
| `rmw_fastrtps_cpp` | repeat 0/1/2 | repeat 0/1/2 | repeat 0/1/2 | 9 |
| 合计 | 6 | 6 | 6 | 18 |

唯一 cell key 至少可区分 target/revision、RMW、arm、repeat 和所属 run。不得用同一执行 raw 假扮三个 repeat 或另一个 RMW。所有 18 格必须有 input capsule；已执行格必须有 execution metadata 和 raw/hash。planned/registered 不是已执行。

固定时序（D010）：environment/locks 健康 → 创建 writer → writer 发布 prejoin 序号样本 → raw 写 `mutation_begin` 与已达 writer 前置 snapshot → delay/create reader → 证明 reader 创建晚于 writer → 证明匹配 → graph 观察 → 发送匹配后样本 → 记录接收集合/顺序 → oracle → raw/replay。late_joiner 的 mutation 前置仅为 writer_created + writer_prejoin_published；reader/match/graph 属于 mutation 后观察。runtime 必须分开 precondition_receipts 与 observation_receipts。sleep、静态 plan 或事后倒填 reached 都不能证明 mutation 前已到达。

primary rule 唯一固定为 `discovery.late_endpoint.match_and_receive_after_join`，要求晚创建、正确匹配、post-match 窗口的 delivery contract、endpoint identity/generation 一致、发送/接收序列关联。diagnostic progress 不能替代 primary rule。

第一 slice 的 expected effect 合同：

| 字段 | 固定内容 |
| --- | --- |
| mutation_kind / carrier | `late_joiner` / `ros2_endpoint` |
| effect_id / window | `reader_created_after_writer` / `before_reader_match` |
| allowed_observations | `reader_created_after_writer`、`match_established` |
| forbidden_residuals | `post_match_delivery_loss`、`wrong_endpoint_match`、`identity_generation_mixup` |
| fact_sources | `endpoint_tokens`、`graph_snapshot`、`delivery_tokens` |

允许影响与 residual 同时出现时执行 D004：residual 优先进入核验，allowed 不能短路。窗口完整性、事实类型和 endpoint identity 都必须有运行证据。D009 补充：finite-window 的末尾缺失不能单独判违约；中间 missing 要有更晚 sentinel/ACK 的明确闭合证据，缺闭合时保留 unknown/deferred，不能仅以超时证明丢失。

## 3. 四类 control，各 6 条

每类都固定两个 RMW × repeat 0/1/2。required control 中 unknown 按失败处理；fault 的 primary fail 是校准成功，不是自然 SUT candidate。

| control_kind | arm | 数量与预期 | 可引用 required 行？ | 必须保留的证据 |
| --- | --- | --- | --- | --- |
| health | baseline | 6/6 primary pass | 是，引用六条 baseline | primary verdict、raw/hash、endpoint 健康、neutral receipt |
| fault | negative | 6/6 primary fail | 否，另存独立六行 | D012 raw-backed fault_control_receipt：fault_id/applied/omitted_sequence/raw_event_ref、harness归因、candidate_eligible=false；更晚闭合witness及primary fail |
| mutation | mutated | 6/6 hit | 是，引用六条 mutated | 嵌套执行 receipt、真实 hit、最终输入 hash、raw effect |
| transparency | transparent | 6/6 `no_effect_or_expected_pass` | 是，引用六条 transparent | target control contract、mutation 声明、输入透明比对及观察结果 |

这是主agent D004 的计数解释：18 条 required 唯一执行行 + 独立 6 条 fault = 第一 target 至少 24 条唯一执行行；四类各 6 的控制检查可复用 required 行，不要求另增 24 条执行。引用必须能定位原 cell，不重复计算执行/成熟分母。独立 control 行不入成熟分母；required 行的 control 投影不另加分母且不进入自然 candidate。

第一 target 的 fault 固定 `fault_id=late_endpoint_delivery_omission`，仅在 reader 已创建并匹配后受控省略一个 post-match delivery，预期 primary fail。故障施加点和 oracle 不能循环使用同一个人工 verdict。D009 明确故意 omit 中间 seq，附明确 harness provenance 和更晚闭合 witness；不入 SUT candidate。D012 明确这不是 late_joiner mutation：negative+fault 只能通过受审计 fault-control 路径 reached→effect_observed→judged，不经过 mutation_applied，不伪造 late_joiner hit。fault receipt须具备raw_event_ref并与原始故障事件和时序绑定；具体施加层以receipt说明。

## 4. 完整 S1 scope 与推进顺序

初始 scope 清单保存位置为 `config/s1_scope.json`（规格要求，是否已实现须另核查）。清单顺序不自动等于执行顺序；以下是 §5 固定执行顺序。

| 顺序 | target | mutation | 必要的独立证明 / 当前风险 |
| ---: | --- | --- | --- |
| 1 | `discovery.late_endpoint` | `late_joiner` | §9 第一 slice 的完整 18-cell 和四类 control |
| 2 | `discovery.late_join_delivery` | `late_joiner` | Phase 5A 同样 18-cell；独立 primary rule 与 expected effect，不把第一 target 改名复用 |
| 3 | `discovery.interruption_recovery` | `discovery_drop` + `restore_after_discovery_interruption` | Phase 5B 同样 18-cell 和独立 fault；已有third v4两RMW development runtime/restore证据，但非capsule-driven正式验收；仍需D015、18+6/control/seal；停发用户数据不算discovery_drop |
| 4 | `builtin.discovery_projection_reconciliation` | `endpoint_qos_variation` | 独立wire/builtin + graph对账，禁止graph自比较；已有24行run，17 required pass、1 unknown、6 fault fail、seal=false；保留unknown/失败，D014正式验收未完成 |

第四target字段合同（D014，主agent工程裁定）：GUID/role/topic/type/reliability/durability/deadline/liveliness及lease必需；history/depth只作supporting diagnostic，缺失明确为`not_applicable_by_endpoint_schema`，禁止配置补值。generation由独立capture和进程生命周期关联，不是虚构wire字段。原taskbook未枚举depth，旧catalog不能强行将超标准字段继承为全部RMW必需。revision0已有24行实际run：17 required pass、FastDDS `transparent.0` 1 unknown、6 fault fail，transparency 5/6、seal=false；unknown与失败保留。D014正式验收及18+6/control/seal仍未完成，不删已失败rule、不改18/72分母；任何配置值不得补wire字段。

每个 target 都要登记四类 control 和 per-target disposition/seal。若四个 target 全部按两个 RMW 的 18-cell 执行，计划为 72 条 required 和 24 条独立 fault，共至少 96 条唯一执行行；这是全 scope 计划计数，不是已运行数量，也不是无条件完成承诺。

不在初始 scope：`discovery_delay`、`discovery_reorder`、`discovery_duplicate`、lease expiry/renewal、endpoint remove/recreate。新增前须 owner escalation，更新 scope、semantic model/rule、成熟分母与 run identity。不得从旧仓库 taxonomy 隐式导入。

## 5. Phase 顺序与当前交付边界

| Phase | 原规格要求 | 本轮处理与验收凭据 |
| --- | --- | --- |
| 0 | 初始化、registry、identity、环境 manifest、pair、锁和最小测试 | 初始化及合同实现已有开发记录；实际 source HEAD 见 git log 及运行 manifest，真实执行与冻结检查以证据为准 |
| 1 | SemanticTarget/Condition/Step/Slot、状态、receipt、oracle、candidate/replay 合同 | fake runner 至少覆盖 pass、unknown、not_applicable、mutation_ineffective、tool_error、deferred、rejected；fake 不计真实矩阵 |
| 2 | 至少一个 RMW 源码/内部事件观察、候选账本、provenance、black-box task | 与当前公共 API 执行边界的关系待主agent明确；不能以普通 graph 观察冒充内部插装，也不能制造候选 |
| 3 | 第一 18-cell + 四类各六个 control + capsule/execution/raw/report | 当前HEAD重跑18 pass+6预期fault fail、四control6/6、unknown0、独立seal=true；首跑false/raw不覆盖 |
| 4 | instrumentation candidate 有则同输入两 RMW × 三 repeat black-box validation；无则 bounded black-box campaign 与零候选报告 | 无候选不阻塞后续 scope；当前仅按主agent边界做正常 API 有界验证，是否满足原发现性要求须独立判断 |
| 5A / 5B | 第二 target，再第三 target | 按固定顺序，分别完整矩阵，不以第一 target 的 receipt 替代 |
| 6 | per-target seal，同 revision 完整重跑 required matrix 和全部 gate | 只在证据齐全时封存；第四 target 也需同等核查 |
| 7 | 按 scope 执行顺序逐个扩展 S1 | 完整 S1 范围包含第四 target；与 5A/5B 重叠的具体调度由主agent明确，不能重复凑数 |
| 8 | S1 disposition/model/preconditions/lifecycle/replay/seal 稳定后进 S2 | S2 不是当前默认执行内容；不能仅因第一 slice 完成就进入 |

原文“第一轮到 Phase 3、暂不进入 Phase 5/7”是历史里程碑边界。主agent转述用户本轮要求完整 S1，D001 显式覆盖该停止点；其余依赖和证据要求仍保留。

## 6. 状态、schema、输入身份要求

| ID / 原文 | 精确验收要求 | 必要证据 / 状态 |
| --- | --- | --- |
| S01 / §4 | SemanticTarget 含 `intent_and_scope`、`target_revision`、`state_preconditions`、`state_steps`、`mutation_slots`、`observation_contract`、`expected_effect_set`、`rule_contract`、`evidence_policy` | 序列化/校验合同；待验证 |
| S02 / §4、7.4 | 唯一 `execution_stage`：modelled → expressible → planned → precondition_executed → reached → mutation_applied → effect_observed → judged；未完成终态 aborted | 逐转换日志；neutral 跳过 mutation_applied 依 D002；negative fault分支依 D012；待验证 |
| S03 / §4 | plan 仅计划；每前置 step before/after receipt、requires 求值；未达到前置不得 mutation | 失败注入合同与真实 step receipt；待验证 |
| S03a / D010 | raw 的 writer 前置完成及 snapshot 先于 mutation_begin/delay/create reader；reader/match/graph receipt 只作后续观察。discovery_drop 则须先 matched 再 drop | 必查原始事件索引、时间与操作依赖，不能只查 receipt 存在；初次 seal 前 revision0 纠错，分母不变；待逐路径验证 |
| S03b / D012 | negative+fault 可 reached→effect_observed→judged，不经mutation_applied；不伪造late_joiner hit | 必须raw-backed fault_control_receipt含fault_id/applied=true/omitted_sequence/raw_event_ref、responsibility_layer=harness、candidate_eligible=false；核原始时序；首份formal六条negative均按预期fail；完整target seal仍false |
| S03c / D013 | reader创建后双向新鲜snapshot及各自唯一command_done ACK齐备才可open post_match_delivery；按role/command_id/API/GID/generation/worker绑定 | request→snapshot→ACK→window_open，创建/请求≤采样≤snapshot≤ACK≤窗口；分批stdout须等ACK，缺失/重复/提前ACK不能放行或回退旧match；首次unknown保持，不改rule/revision/分母 |
| S04 / §7.4 | modelled 有 manifest/revision；expressible 被 carrier/RMW/runtime 接受；planned 编译 scenario/steps；reached 全前置 true | 不以静态 facts 代 runtime；待验证 |
| S05 / §7.2、7.4 | mutation receipt 只准嵌套七字段：planned/submitted/carrier_admitted/configured/applied/hit/effect_observed；真实 mutation_applied 要 carrier_admitted/configured/applied/hit 四项全 true | 不支持平铺混用；hit=true 且 configured/applied=false 要 abort；待验证 |
| S06 / §7.4 | effect_observed 有 expected/residual 的 raw observation；judged 有四态 oracle；aborted 有 abort_reason 且不能继续 | neutral 也必须有真实观察，不自动升级阶段；待验证 |
| S07 / §7.2 | cell 有 run/target/revision/campaign/RMW/arm/repeat、mutation/carrier/parameters、expected/observed、receipt/hash、facts/verdict、raw/replay、candidate 与 adjudication 字段 | 无候选时 candidate_status=none；缺失与不适用显式表达，不伪造 source；待验证 |
| H01 / §7.3 | hash 至少含 target/revision、kind/carrier/parameters、RMW/repeat、层序、deterministic input_layer_receipts、最终 payload/config | D003 对“各层 receipt”的显式解释；输入变更可区分；待验证 |
| H02 / §7.3、8.2 | campaign_kind/domain 均排除；真实执行 receipt、事件时间不污染输入身份 | 固定 repeat 的 required/exploratory、换域和不同时刻同输入 hash 一致；待验证 |
| H03 / §8.2 | 所有 required 格有离线可生成 input capsule；实际提交输入与 hash 核验，execution receipt 独立 | 离线创建不冒充已执行，实际差异保留；待验证 |
| H04 / §4.2 | target_revision 初始 0；首次 seal 前最多改一次到 1，附证据/字段/分母影响 | 对该 target 旧 evidence 失效；其他 target 不因该 revision 改动失效；待验证 |
| H05 / §4.2、12 | Phase 3 后该 target revision 变化，18 个 required 格全部重跑，新 run_id/manifest/capsule | revision=1 seal 后再修改须新 identity 或 major revision；待验证 |

cell 的 adjudication 记录包括 `adjudicator_id`、`adjudication_method=single_agent|peer_review|rule_engine`、`adjudicated_at`、`confirmation_basis`。target/rule/report 身份不一致不能由人工改状态掩盖。

## 7. oracle、candidate、replay、provenance

| ID / 原文 | 要求与通过条件 | 当前证据状态 |
| --- | --- | --- |
| C01 / §7.1 | oracle 仅 `pass|fail|unknown|not_applicable`；缺事实列具体来源，不能空列表当 pass | 待验证 |
| C02 / §7–8 | lifecycle 为 observed → admitted → triaged → confirmed，triaged 可 deferred/rejected；派生标签不另造 lifecycle | 待验证 |
| C03 / §8.1 | observed 已落盘异常/witness，稳定 identity、raw/source/timestamp/target/RMW/run_id；不能把计划异常当 observed | 待验证 |
| C04 / §7.5 | admitted 有 run/target/revision/RMW/arm/repeat、稳定 endpoint/instrument identity、raw/hash、effect/fact、rule/内部合同/differential assertion、source/tier/provenance；排除 harness/reach/mutation ineffective | 待验证 |
| C05 / §7.5 | mutation 来源另有 carrier_admitted/configured/applied/hit/effective hash；instrumentation 来源另有点位、source/binary identity、时间、control | 待验证 |
| C05a / D011 | 本机 Jazzy MessageInfo 无 publisher_gid：endpoint_correlated 与 direct per-sample GID 分开；unavailable_api/null 不归 SUT mismatch，facts/report/provenance 保留限制 | 双 RMW smoke 已见 endpoint_correlated；直接逐样本 GID 未达成；按 target/rule 判断证据充分性 |
| C06 / §8.1 | triage 只受理 effect_observed/judged 且 observed/admitted/triaged/deferred；未达前置、真实mutation未applied/hit先在执行层结束；D012 negative admission无条件排除 | 待验证；neutral 分支不用于 bypass mutation admission |
| C07 / §8.3 | responsibility 仅 sut/observation/harness/mutation；disposition 仅 candidate/pass/deferred/rejected；每条有 reason_code | 待验证 |
| C08 / §8.3 | 顺序先环境健康、真实前置、mutation gate，再 identity/window、effect 合同、typed oracle、residual/allowed、controls/provenance 归因 | D004 显式覆盖 allowed 提前结束分支；待验证 |
| C09 / §7.1 | replay status 仅 not_run/pass/fail_timing/fail_environment/fail_artifact/inconclusive；失败保留原因和 attempt | 待验证 |
| C09a / D009 | 场景一致性、replay outcome 和 oracle verdict 分开；保留首次 execution 作基线，原 fail 后新 pass 不能确认原异常；异常匹配须有 typed signature | 已裁定；只读审查已见核心实现，真实 adapter 链路仍待验证 |
| C10 / §7.1 | replay_unconfirmed 仅 admitted/triaged/deferred 且 replay≠pass；confirmed 不生成该标签，失败要 non_replayable_evidence 或其他具名 confirmation_basis | 待验证 |
| C11 / §8.2 | input：target/revision/seed/RMW/arm/repeat/topic/name/type/QoS/mutation/carrier/steps/source manifest/hash；origin_domain 仅环境记录 | 待验证 |
| C12 / §8.2 | execution 只能真实运行写：stage/precondition receipts/mutation_receipt/effect/facts/raw/hash/attempt/failure | 待验证 |
| C13 / §8.2、12 | admitted/triaged candidate 必有 replay attempt 或具名 non_replayable；seal 时所有 candidate 同样审计 | 待验证；observed/rejected 等最终处理见歧义 A06 |
| C14 / §6 | instrumentation 可直接产候选，不以 black-box replay 成功作入账前置；same seed/mutation/RMW/前置做 black-box validation，失败不删候选，只更新 validation/tier | 原要求保留；与当前边界关系见 O004 |
| C15 / §6、13 | instrumentation 独立 build root/binary/compile manifest/process group；provenance 记录 source commit、patch digest、compiler flags、loaded libraries、symbol manifest，以及插装点/时间/endpoint/control | 待验证；black-box 不加载插装 binary/override |
| C16 / §10、12、D012 | negative admission无条件排除；control不入自然候选；无 instrumentation candidate 仍交完整有界执行和 zero-candidate/disposition 报告，不造候选 | 待验证；数量不可先验保证 |

reason code 至少覆盖：`environment_or_process`、`reach_or_planning`、`mutation_ineffective`、`tool_error`、`unknown_evidence`、`window_incomplete`、`oracle_pass`、`not_applicable`、`mutation_expected_effect`、`attribution_insufficient`、`sut_contract_violation`。树中描述执行失败如何归因，不表示允许先把无效 mutation 入账为 SUT candidate。

## 8. unknown、成熟分母与 seal

| ID / 原文 | 精确 gate | 所需证据 / 状态 |
| --- | --- | --- |
| G01 / §11 | 第一 18 格中 applicable unknown=0；任意 required unknown 阻止 target seal | 每 RMW/arm/repeat 四态 verdict，不能筛掉 unknown；待验证 |
| G02 / §11 | exploratory unknown ≤ applicable rows 的 10%，且单 target ≤1；超限修 producer/观察，停止扩展 mutation | 预算、分母与逐行原因，不取整放宽上限；待验证 |
| G03 / §11 | 成熟基准：required target × applicable RMW × required arm × repeat；registered/blocked/input-surface/control-only/exploratory 不当通过分子 | D004：blocked 不减少已冻结分母；第一 slice 仍保留 18 格；待验证 |
| G04 / §11 | applicability 同时满足 scope 声明、环境真实可用、endpoint/carrier 可表达、rule predicate 为 true | manifest、RMW identity、endpoint/step/mutation receipt、raw/hash、verdict；不可用具名 blocked/not_applicable 并保留基准缺口；待验证 |
| G05 / §12 Phase 6 | 同 target_revision 完整 required 18 格真实执行，不混 revision | 完整重跑记录与 run/manifest/capsule 对应；待验证 |
| G06 / §12 Phase 6 | 四 control 各满足 6/6、applicable unknown=0 | 控制引用和独立 fault 证据；待验证 |
| G07 / §12 Phase 6 | 每 executed 格 raw/path/hash/execution；每 mutation 格 input hash/receipt | 路径存在且内容可核 hash；待验证 |
| G08 / §12 Phase 6 | 每 candidate replay attempt/non_replayable、triage disposition/responsibility/reason | 不删除失败或 deferred 账目以通过 gate；待验证 |
| G09 / §12 Phase 6 | report/manifest/catalog/rule/target revision 的 hash 一致，并与当前完整source identity绑定 | 各文档引用同一套对应对象digest，并非不同对象hash字面相等；当前已见source_manifest比较与source_frozen gate，冻结及真实seal待验证 |
| G10 / §12 | 任一条件不满足保持未封存；不得把合同子集 seal 等同完整 S1 成熟 | 可审计 per-target disposition；待验证 |

## 9. 环境、锁、abort 与升级

| ID / 原文 | 必须满足的要求 | 当前证据状态 |
| --- | --- | --- |
| E01 / §13.3 | `environment_mode=ros_overlay`；先 Jazzy `/opt/ros/jazzy/setup.bash` 后新仓库 overlay；依赖锁定 | 待环境owner实测；README 不猜 overlay 路径 |
| E02 / §13.3 | run 前 campaign registry lock、domain lock、state-directory lock 均成功；检查旧 campaign 进程、旧 run lock/owner | 待验证；不能只检查本进程的内存标记 |
| E03 / §13.3 | ROS_DISTRO/RMW/source repo/HEAD/revision 校验；environment manifest 写入 | 待验证；无有效 frozen HEAD 不能宣称正式 seal 身份成立 |
| E04 / §13.3 | domain 三重检查：锁与 owner payload、RTPS derived port block bind/probe、活动进程命令行/环境身份 | 待验证；任一冲突或无法判定都 `domain_conflict` + abort |
| E04a / D008 | 主agent实测 uid1000/CapEff=0，unshare user+net 内 lo UP、rc0；只证明 namespace/loopback 可行性，不计 ROS matrix/carrier evidence | 主agent报告实测，文档未复跑；第三 target 继续核查 |
| E05 / §13.3 | 插装 build/binary/compile/process 隔离，只来自新仓库或明确只读源码副本；不改旧 worktree | 待验证；black-box 插装污染必须拒绝 |
| E06 / §13.1 | run 中 source repo/HEAD/revision 改变、缺/错 control、unknown 超限、hash 碰撞最终输入、hit/configured/applied 矛盾、plan 冒充 step、缺候选 provenance/raw/归因、锁冲突、排除不了旧 owner 时 abort | 对每种错误保留 abort_reason、已完成阶段和失败证据；待验证 |
| E07 / §13.2 | scope 外 target/rule/slot/carrier、分母变化、已 seal revision 修改、native/security、当前 RMW 无可行 endpoint carrier 先 owner escalation | 日常主agent处理，需用户项仅记录；待验证 |

文档子agent不启动 ROS，不运行 campaign。真实运行由主agent协调单一 campaign owner；不并发争用 domain/state directory，不以杀掉未知 owner 的方式通过 preflight。

## 10. 每次交付报告字段

§14 必须逐项出现且能回查：HEAD/worktree；target/revision/domain/RMW/run_id/campaign_kind；execution_stage 与映射；state sequence 和每 step receipt；mutation kind/carrier/parameters/层数与七项嵌套 receipt；expected/observed effect；effective input hash；typed facts/rule_id/verdict；raw path/hash、replay input metadata/execution status/failure；candidate_status/source/tier/responsibility/reason；测试命令/结果；abort/escalation；是否改变成熟分母。

无运行时写“未运行/待核查”并说明原因，不能填写虚构 run_id、raw/hash、成功次数或 candidate 数量。本轮文档审阅不证明这些 gate 已通过。

## 11. 原文歧义、显式解释与主agent待裁定项

| ID | 原文冲突/遗漏 | 处理状态 |
| --- | --- | --- |
| A01 | §7.4 mutation_applied require hit 与 baseline/transparent 的透明要求冲突 | 已裁定 D002：neutral/no-op 可 reached→effect_observed/judged，不假 hit；待实现验证 |
| A02 | §7.3 各层 receipt 入 hash 与 §8.2 input capsule 离线生成有时序冲突 | 已裁定 D003：deterministic input_layer_receipts + 最终输入；execution mutation_receipt 独立；待验证 |
| A03 | §8.3 allowed 提前 rejected 会吞掉同时发生的 forbidden residual | 已裁定 D004：residual 优先，保留 primary 违约；待验证 |
| A04 | §9–10 controls 是否额外执行、§11 blocked 是否可减少分母不明确 | 已裁定 D004：三类可各复用六条 required、fault 独立六条；blocked 不减分母 |
| A05 | 原“第一轮仅 Phase 3”与本轮完整 S1；§5 顺序和 Phase 5/7 重叠，第四 target 无单独 phase 定义 | D001 覆盖停止边界；具体每 target 的 Phase 4/6 时点由主agent排程 |
| A06 | §8.1 triage 限定 effect_observed/judged，归因树又含前置/hit 失败；§8.2 仅 admitted/triaged 要 replay，seal 又要求每 candidate | 主agent待统一执行拒绝审计与 triage 入口，及全部 candidate 的最终 replay/non_replayable disposition |
| A07 | §4.1 expected_effect_set 是对象，§7.2 示例为数组；§7.1 错指“4.2 定义状态”（实际 §4） | 主agent/core 待固定 schema；引用错误不新增状态，需对象/集合兼容解释 |
| A08 | primary rule 违约可入候选（§4.1），归因树却似乎强制必须 forbidden residual；allowed/residual 均为空或未知怎么判断 | D004 禁止吞违例；primary-only 的明确判定分支/事实完备条件仍待主agent固定 |
| A09 | first fault 的“省略 delivery”未明确注入点；公开 API 验证与 instrumentation/发现性要求有范围差距 | 主agent待固定控制施加层与独立 oracle，不把工具丢记录当 SUT 丢包；见 O004–O006 |
| A10 | seal “hash 一致”未定义 manifest schema、canonical serialization、代码/规则变更失效粒度；revision=0 seal 后修改的路径不完整 | 主agent待统一 identity/重跑策略；不能把 revision=0 已 seal 视作可随意改成 1 |
| A11 | 多 RMW 分开运行不能自动证明 cross-vendor pair 互操作 | 报告区分两种证据；是否需混合 vendor pair 不从旧参考默默新增 |
| A12 | F06 真实 pair 集成要求在 zero-candidate 场景下如何验证 admission/update/audit | 主agent待定义真实 run 证据与离线合成生命周期的关联；不得造自然 SUT 候选 |
| A14 | negative故障省略delivery并非late_joiner mutation，严格mutation_applied gate与独立fault控制冲突 | 已裁定D012：negative+fault用raw-backed受审计分支，不伪hit；negative admission无条件排除；不增scope/分母 |
| A13 | late_joiner 把 reader/match/graph 当 mutation 前置，会在执行后倒填 reached，receipt 齐全也可能时间倒置 | 已裁定 D010：writer-only 前置与后续观察拆开；drop target 保持 matched 前置；验 chronology，初次 seal 前 revision0 纠错，不改分母 |

以上“待裁定”是验收合同需要明确的事项，不自动等同运行阻塞。实际实现/环境结果应持续更新 [待核实事项](open-items.md)。

## 12. 已报告的开发验证快照

[验证记录](verification-snapshot-2026-11-14.md) 分别记录主agent报告的最新 144 tests / 37.032s / OK（主agent独立验收、ACK逻辑审查通过）、历史 131 tests / 13.485s / OK（negative+identity 集成通过）及 112 tests / 4.232s / OK、真实 non-DDS carrier control 的 33 packets drop 与 70/70 恢复，以及只读核对的双 RMW contract-smoke raw/hash。三类验证不互换：离线测试数不计真实矩阵，non-DDS control 不计 discovery/S1 evidence，smoke 不等于完整 18-cell/四类 6/6 或 seal。MessageInfo 无 publisher_gid 作为观察限制保留，不判 SUT mismatch。

首次冻结前更新：现有runner和独立audit已检查source_frozen/dirty_behavior_paths及当前source identity，候选ledger投影重建路径已补入，历史review保留并注明进展。主agent最终GO已确认代码freeze并派遣首次提交，实际HEAD见git log及运行manifest；源码冻结后的旧开发evidence保持历史，不通过首次commit追认。D011指定单次自动化提交身份，不修改用户Git配置；results不提交且保留可见untracked。

## 首份正式矩阵与字段裁定更新

本次记录时间：2026-11-15T00:01:03+08:00（Asia/Shanghai，TZ=Asia/Shanghai date --iso-8601=seconds实测）。

[首次矩阵记录](first-formal-matrix-2026-11-14.md)保存报告路径、字节hash、manifest source、异常raw事件和精确时差。主agent独立verify rc1；24条runtime completed不等于24条judged，required的1条unknown和transparency5/6使seal=false。原报告immutable，D013修复后不能覆盖旧结果。D014字段定义见上文，不将旧开发instrumentation的limited pass/extended unknown提升为第四target正式证据。

第二次冻结前更新（2026-11-15T00:14:58+08:00，Asia/Shanghai）：主agent独立全仓144 tests / 37.032s / OK、diff --check及ACK逻辑审查通过，并给本次第二commit最终GO。此为工具时序修复的合同验证，不更改首次formal失败记录；第二次提交后的真实矩阵尚未由本记录证明。D014保持待实施。

## D015第三target附加验收条件（development evidence exists；正式验收未完成）

2026-11-15T00:26:18+08:00（Asia/Shanghai），主agent工程裁定：wire_proxy标识保留、implementation=isolated_netns_nft_discovery_udp_drop；仅mutated真装drop，其他arm审计neutral，negative独立omit中间postrestore seq。要求matched前置、真实本pair discovery counter>0（合成UDP不计）、规则删除与恢复观察分开；允许match continuity retained，但必须restore后身份关联discovery＋fresh双向count/graph＋postrestore delivery。父进程锁持有到整个netns子树清理。third v4已有两RMW开发runtime/ingest，drop计数614/162且restore后seq 2–7送达；其`reconstructed_input`与`capsule_execution=null`限制意味着正式D015及18+6/control/seal仍未验收。此项保持scope/72分母。

## 已seal模型与新target合同兼容

2026-11-15T00:32:53+08:00（Asia/Shanghai），主agent裁定：first/second已seal revision0 manifest保持不变，core精确兼容其legacy fault合同；第三/第四新target显式receipt_kind/window及projection schema。必须核对保存的target_sha256及fault字段，不能用shared helper升级悄悄改写已seal manifest。初次seal前合同修正不等于new rule完成；target manifest不变也不能豁免全局source变化后的最终rerun。


## 本轮冻结前修复状态补充（2026-11-15T08:42:44+08:00，Asia/Shanghai；尚未冻结）

来源：主agent本轮派遣与工程报告。本节仅追加当前验收状态，不改写前文历史结果，不把主agent报告冒称本执行员实测或原任务书字面要求。

| 项目 | 本轮状态 | 后续验收边界 |
| --- | --- | --- |
| scope 与成熟状态 | scope 从随 run 更新的动态状态改为静态 capability；成熟证据由各次 per-run report 及独立 per-target audit 提供 | capability 声明不等于实际执行、hit、oracle pass 或 seal；不改变四 target 顺序及 18/72 required 分母 |
| capture 分类与原始证据 | 主agent报告：非标准端口的 potential discovery RTPS 已保留 raw，普通 user-only DATA 已有安全分类 | 不能把普通 user DATA 计作 discovery hit；potential discovery 的 raw 留存不自动补足身份/解析缺口或提升 unknown；第三/第四 target 仍需新 source 真实验收 |
| execution_input 与 blackbox | execution_input 绑定及真实 Cyclone/FastDDS blackbox 兼容修复进行中 | maps 多 segment 与污染 trace 回归尚待 Ptolemy 关闭；不得写成修复完成、资格化通过或最终 freeze |
| 离线检查 | 主agent报告 capture/projection/recovery 89 tests / 5.557s / OK；此前完整 334 tests / 48.376s / OK 属更早工作树 | 本执行员未复跑；上述报告均不等于最终修复树的完整测试或真实 ROS 验收；待 Ptolemy 最终 freeze 后追加确切结果 |

本次只读观察 HEAD 为 `e5a1a73921b176e60bba14cb41b9ed7f18c0a538`，行为文件仍有未提交改动，当前 source 未 freeze；新的冻结 HEAD/behavior SHA-256 未给定。正式四 target、F07 健康/可控工具失败重放、Phase2 同 input instrumentation/clean-blackbox 与 Phase4 bounded campaign 均未在新 source 验收。历史 source 的 seal、失败与 unknown 保留原判，不能用于新 source seal，也不回填旧 receipt/raw/hash。自然 SUT candidate 仍为 0，2–3 候选目标未达成；完整 S1 required 分母仍为 72，独立 negative 不计自然候选或重复成熟计数。本补充不授予运行、staging 或 commit GO。


### 最终修复树离线验证与代码 owner freeze ACK 补记（2026-11-15T08:49:44+08:00，Asia/Shanghai）

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
