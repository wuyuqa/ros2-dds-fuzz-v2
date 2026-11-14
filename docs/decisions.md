# 决策记录

日期：2026-11-14。原规格见 [任务书原文](taskbook.md)。本页记录用户本轮范围和主agent解释，不改写原文，不表示功能已经实现或验收通过。日常工程问题由主agent裁定；确需用户决定的事项仅登记在 [待核实事项](open-items.md)，文档子agent不自行请求确认，也不假设它们已经阻塞。

## D001：本轮覆盖完整 S1，执行内容受当前要求约束

来源：主agent派遣消息；其中用户最新范围由主agent转述，工程执行边界由主agent声明。状态：已确定。

- 用户最新范围（主agent转述）：本轮继续完整 S1 的四个 scope target，不能把原文“第一轮到 Phase 3”当成本轮停止边界。第一条 slice 仍先完成，后续按原规格固定顺序推进。
- 主agent声明的执行边界：当前实际工作只实施 ROS2 公共 API 的正常互操作、可靠性测试和证据工具；不实施自动漏洞搜寻、恶意 wire payload 或漏洞复现。
- 原规格中的 instrumentation、bounded campaign、wire carrier、replay、candidate、seal 要逐项区分合同实现、真实运行、范围兼容性和证据缺口。保留原要求并核查，不用模拟结果充当真实运行。
- 原 S1 全部成熟和“2–3 个真实 SUT candidate”不得预先承诺。候选数量取决于真实观察，零候选是可如实报告的结果，不等于已证明无缺陷或全部成熟。任务书本身没有必须产出 2–3 个候选的配额。
- health/fault/mutation/transparency 只用于校准。control 不进入自然 SUT candidate，不能为了凑数改 source、伪造 hit、放宽 primary rule 或把工具问题归为 SUT。
- 旧参考中的 S1–S5、组合变异、native 路径和旧成熟数字不是新仓库的自动新增范围。

## D002：baseline/transparent 使用可审计 neutral/no-op 路径

来源：主agent工程裁定/派遣消息。状态：已裁定，须由实现和合同测试验证。

原文冲突：§7.4 将 `mutation_applied` 定义为 carrier_admitted/configured/applied/hit 全为 true，§9–10 又要求 baseline 和输入透明的 transparent；若强行让三臂全部通过同一线性 mutation 阶段，就会逼出虚假的 hit。

主agent解释（不是宣称原文字面如此）：

- baseline/transparent 严格不伪造 hit；可使用有审计的 neutral/no-op 路径：`reached → effect_observed → judged`，不经过 `mutation_applied`。
- 保留唯一字段 `execution_stage`，不得另造第二套执行状态；在 step/transition receipt 中记录 arm、neutral/no-op 原因、实际输入与观测。
- `mutation_applied` 的严格 gate 对真实 mutation 路径保持有效；mutated 的无 hit 不能借 neutral 分支越过 gate。
- transparent 必须有 mutation 声明、最终输入透明的比对和观察合同结果，只有 arm 名称不能证明透明。
- candidate admission/triage 中要求真实 mutation 的门槛仍适用于 mutation 来源；neutral 不构成 mutation 命中或自然 candidate 的替代证据。control 行始终排除自然 SUT candidate。

## D003：确定性输入身份与执行 receipt 分离

来源：主agent工程裁定/派遣消息。状态：已裁定，须由实现和合同测试验证。

原文冲突：§7.3 把“各层 receipt”纳入 effective hash，§8.2 又要求 input capsule 可离线生成，而真实执行 receipt 只能运行后生成。

主agent解释（不是宣称原文字面如此）：

- 输入 hash 使用 deterministic `input_layer_receipts` 与最终 payload/config；另含 target、target_revision、mutation kind、carrier、parameters、RMW、repeat、层顺序等输入身份。
- 真实 `mutation_receipt` 独立存放于 execution metadata；事件时间、运行进程等非确定性执行字段不得污染 input identity。
- `campaign_kind` 和 domain 始终不计入输入 hash；`origin_domain` 只作环境记录，replay 使用新的隔离 domain。同一输入的 required/exploratory、换域重放应得到相同 hash。
- 离线 input capsule 只证明输入已生成，不证明执行、hit 或 effect。真实执行必须核验实际最终 payload/config 与 capsule/hash 对得上；执行差异不能靠覆盖原始 capsule 隐去。
- 不同最终输入必须可区分；同一输入的重复执行不能因 execution receipt 的事件时间不同而改变输入身份。`repeat` 仍是规格规定的输入字段，固定 repeat 后比较确定性。
- 序列化规范、deterministic receipt 字段和 actual-vs-input 核验接口由 core/campaign 统一，并作为验收证据，不能由报告器临时猜测。

## D004：residual 优先；control 复用与分母保持明确

来源：主agent工程裁定/派遣消息。状态：已裁定，须由实现和合同测试验证。

原文冲突：§8.3 的逐项判定树会在 allowed effect 时提前 rejected，可能吞掉同时发生的 forbidden residual；§9–11 的 required/control 行是否复用及 blocked 分母也需要明确解释。

主agent解释（不是宣称原文字面如此）：

- allowed effect 与 forbidden residual 同时出现时，residual 优先；不得用 `mutation_expected_effect` 提前结束而掩盖违例。
- residual 先进入证据、primary rule 与责任归因检查，不自动等于 confirmed SUT candidate。只有 allowed effect 且无 residual/primary 违约时，才可解释为预期影响。
- health、mutation、transparency 可分别引用各 6 条 required 行；引用须携带同一 cell identity/raw/hash/contract，不能在计数时重复增加自然执行次数。
- fault 独立执行并独立保存 6 行。第一 target 最少为 18 条 required 唯一行加 6 条独立 fault 行；四类 control 各 6 条是 24 个控制资格/检查，不必解释成额外 24 次执行。
- 被 control 引用的 required 行可以保留 required 覆盖资格；其 control 投影不进入自然 candidate，也不额外增加成熟分母。
- 成熟分母不因 blocked 减少。两个 RMW、三个 required arm、三个 repeat 的第一 slice 固定保留 18 个预期 cell；不可用或不可表达应具名记录缺口，不能静默移除对应 cell 后宣称全通过。
- 正式改变 applicability/scope/成熟分母须有显式主agent裁定和版本化证据；已有 blocked 记录本身不是缩小分母的授权。

## D005：主agent负责工程裁定，用户事项仅登记

来源：主agent工程裁定/派遣消息。状态：已确定。

主agent处理接口、实现顺序、日常失败、证据格式和规格冲突的工程裁定。需要用户决定的范围/约束变化，只记录事项、原因、证据与可选结果，不由文档子agent直接发问或擅自执行。原文 §13.2 的 owner escalation 要保留审计；当前完整 scope 内推进不需要反复请求权限。

scope 外 target/rule/slot/carrier、成熟分母变化、已 seal revision 修改、native/security 扩展、无可行 endpoint-attached carrier 都必须有具名升级记录。升级不等于自动获准突破主agent声明的当前执行范围。

## D006：新增工程未知继续核查，不先验宣告不可解

来源：主agent工程核对/派遣消息。状态：待真实检查，见 O001–O003。

1. `builtin.discovery_projection_reconciliation` 原合同要求独立 builtin 与 graph 对账。主agent报告旧 Fast DDS 公共 API 路径该面 blocked；旧 Cyclone native probe 与新任务 no-native participant 约束冲突。不得用 graph 自我比较冒充独立对账。
2. `discovery.interruption_recovery` 的 `discovery_drop` 必须有真实 endpoint-attached carrier，不能用停发用户数据冒充发现中断。
3. 继续全部 scope；上述事实是要核实的工程未知，不是本轮已经证明的不可解结论。原合同不降低，缺口按真实检查结果更新。

## D007：只初始化新仓库，HEAD 后续统一冻结

来源：主agent工程裁定/派遣消息。状态：已执行初始化，未提交。

文档子agent于 2026-11-14 在 `/home/wuyuqa/ros2-dds-fuzz-v2` 执行 `git init` 成功，未 commit。代码仍由其他 agent 并行写入；待主agent协调接口和代码后冻结 HEAD。无首个 commit 时不能宣称已有可用于正式 seal 的 source commit；工作树内容与后续运行 manifest 仍须记录。此操作只针对新仓库，不修改旧仓库。

## D008：user/network namespace 实测仅证明环境可行性

来源：主agent工程实测/派遣消息。状态：主agent已报告实测结果；文档子agent未重复执行。

- 实测身份：`uid=1000`，`CapEff=0`。
- 实测命令：`unshare --user --map-root-user --net sh -c 'ip link set lo up && ip -brief link show lo'`。
- 实测退出码：`rc=0`；输出包含 `lo UP`。主agent消息未给 raw 文件路径/hash，本页不编造原始产物。
- 该结果证明在此环境可以创建 user/network namespace 并启用隔离网络内的 loopback；它不是 ROS endpoint、发现中断、carrier hit 或第三 target 的 ROS evidence。
- `discovery.interruption_recovery` 仍继续工程检查，不是先验不可解。真实 endpoint 附着、发现流量受控中断、restore 及两 RMW receipt 仍须实测。

## D009：finite-window 闭合与 replay 原异常一致性

来源：主agent工程裁定/派遣消息。状态：已裁定，须逐路径验证实现和证据。

- finite-window 末尾未收到样本不能单独判 delivery 违约；不能将测量结束、有限等待或末尾缺失等同永久丢失。
- 中间 missing 必须有更晚 sentinel/ACK 等明确闭合证据，并满足对应窗口、端点身份及 primary rule 的适用条件。等待超时本身不替代闭合证据；证据不足保留 unknown/deferred。
- fault control 故意 omit 中间 seq，应有明确 harness provenance、被省略 seq、先前匹配和更晚闭合 witness。受控遗漏只证明检测通道，不入 SUT candidate，不伪装 SUT delivery loss。
- core replay 正常公开 API 场景的一致性与 `oracle_verdict=pass` 必须分离。相同输入正常跑通并不等于复现原异常；原 execution 为 fail、新 execution 为 pass 时，保留两次结果并记录与原异常不一致，不能确认原异常。
- 原 execution 保留为比较基线；replay 需记录比较依据和结果。确认原异常需要相同适用规则和 typed anomaly signature 的一致证据；单纯“这次也执行成功”或单纯 fail 字样都不充分。其他具名 non_replayable evidence 的确认路径仍按原合同单独审计，不能被场景跑通替代。

## D010：按 target 区分 mutation 前置与后续观察，校验真实时间顺序

来源：主agent工程裁定/派遣消息。状态：已裁定；属于实现纠错、初次 seal 前 revision=0 的定义修正，成熟分母不变。

- late_joiner 的 mutation 前置只包含 `writer_created`、`writer_prejoin_published`，以及 environment/locks 健康。`reader_created`、late-reader match、graph 是 mutation 后观察，不能把它们当作 mutation 前 condition，再事后倒填 reached。
- runtime 分开记录 `precondition_receipts` 与 `observation_receipts`。delay/create reader 前，raw 必须先写 `mutation_begin` 及已达前置 snapshot；随后以原始事件顺序和时间证明实际 mutation、reader 创建、匹配、graph 观察与判定。
- 验收必须比对每项 receipt 与 raw 的时间、事件索引、对应操作和依赖。仅有完整 receipt 列表或事后遍历列表生成的 stage history，不证明 mutation 之前已真实 reached。
- `discovery.interruption_recovery` 的 discovery_drop 前置仍是 matched-before-drop；不能把 late_joiner 的 writer-only 前置泛化到该 target。各 target 分别编译和验证其前置/观察边界。
- 此项不改变 18-cell/72-cell 冻结成熟分母。已有开发 evidence 是否仍符合修正后的 chronology 需逐项复核；revision=0 不等于可以把时间顺序不符合的旧 raw 解释为合格。

## D011：验证边界、source frozen 与首次自动化提交安排

来源：主agent独立验证/工程派遣消息；文档子agent已只读核对现有产物。状态：开发验证快照及首次冻结工程安排；主agent最终 GO 已下达，实际提交与 source HEAD 见 `git log` 及运行 manifest。详见 [验证记录](verification-snapshot-2026-11-14.md)。

- 主agent历史离线命令 `PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -m unittest discover -s tests -q` 返回 112 tests / 4.232s / OK；最新独立验收报告为 131 tests / 13.485s / OK，negative+identity 集成通过。两次结果分别保留，仅记离线合同/集成测试，不记真实 S1 完成。
- 网络 helper 真实 non-DDS control 报告 33 packets drop、70/70 恢复；原结果 `/tmp/r2f-discovery-network-control.json`。此项不计 DDS/ROS/S1 discovery 证据，results 归档由产物 owner 完成。
- 主agent报告 runtime 两 RMW 健康/negative smoke 已正常；smoke 与 full required matrix/control/seal 分开统计。
- runtime 核实本机 Jazzy MessageInfo 没有 `publisher_gid`；当前依据为 `identity_basis=endpoint_correlated`，direct per-sample GID 未达成。缺失应记 `publisher_gid_status=unavailable_api`、direct consistency 为 null/未证，不写成 SUT mismatch。
- endpoint graph/run payload/序列关联不得冒称逐样本直接 GID。规则适用性和候选 provenance 应保留关联依据及观察限制；不能通过字段默认值把相关性升级为直接身份。
- source frozen 工程检查已进入 `campaign.identity.source_manifest()`、真实 `run_campaign()` 和独立 `audit_report()`：要求有效 HEAD、相关行为文件已跟踪且与 HEAD 一致，`source_frozen=true`、`dirty_behavior_paths=[]`；独立审计重新比较当前 source manifest。主agent最终 GO 已确认所有代码agent freeze；提交后仍须实际调用 source_manifest 核对，不能以派遣消息替代 source_frozen 的实测结果。
- source/HEAD/行为输入变更后的旧 evidence、开发 smoke 和本页历史源码快照保留用于诊断，不自动迁入当前 seal。新冻结 HEAD 上的真实矩阵与 evidence identity 需重新满足合同；不通过复制旧 hash 或修改旧 run metadata 补齐。
- 主agent明确指定首次提交用自动化身份，仅该次命令使用 `git -c user.name='Codex Agent' -c user.email='codex-agent@localhost' commit -m 'Initialize S1 conformance framework and evidence contracts'`。这不是用户姓名/邮箱，不改全局、用户或仓库 user 配置，不需用户输入身份。
- 文档子agent负责实际提交但必须等待主agent最终 GO。预定 staging 仅为 `git add .gitignore AGENTS.md README.md pyproject.toml config docs scripts src tests`，禁止 `git add -A`；results raw/locks 不提交。results 不加入 gitignore，保留 untracked 以便用户看见产物。
- 主agent后续工程派遣明确：首次提交后不为更新 docs 立即追加第二次 commit。首次正式 matrix 期间保持 HEAD 不变，docs 可工作树更新但不得 commit；后续 phase 实现统一新 commit，再按新 source identity 重跑相关矩阵。此安排不豁免现有 source identity/frozen 校验，也不将文档工作树更新视为已进入冻结提交。README 使用开发状态表述，不自引用首次 commit hash；历史待 GO 和审阅快照继续保留其时间语境。


## D012：negative fault-control 独立审计路径，不伪造 late_joiner hit

来源：主agent工程裁定/派遣消息。状态：已裁定；主agent最新独立验收报告 negative+identity 集成通过、131 项测试通过，最终 GO 已下达。真实矩阵及 raw chronology 仍单独验收，不由离线结果替代。

原文冲突：任务书 §9–10 将 negative/fault 独立保存，故障是省略一个 post-match delivery；§7.4 的 mutation_applied 又要求 carrier_admitted/configured/applied/hit 全为 true。若把 fault 也当作 late_joiner mutation，会迫使故意 omission 被错误写为 late_joiner hit。

主agent解释（不是宣称原文字面如此）：

- negative fault control 是受控省略 post-match 中间 delivery，不是 late_joiner mutation，禁止伪造 late_joiner applied/hit。
- 增加受审计的 fault-control 路径：`reached → effect_observed → judged`，不经过 `mutation_applied`。继续使用唯一 canonical execution_stage，不另造状态机。
- 该路径仅适用于 `arm=negative` 且 `control_kind=fault`，必须有 raw-backed `fault_control_receipt`，包含 `fault_id`、`applied`（实际施加成功为 true）、`omitted_sequence`、`raw_event_ref`、harness 归因（`responsibility_layer=harness`）及 `candidate_eligible=false`。
- `raw_event_ref` 必须能定位 raw 内真实故障事件，并核对 fault_id、被省略的中间 seq 与施加时序；不能只有 JSON 声明或 controller 自报成功。D009 的更晚 sentinel/ACK 闭合证据与 D010 的 target-specific reached 前置仍需满足。
- candidate admission 无条件排除 negative arm，不以漏填 control_kind、修改 candidate_source 或其他旁路把 fault 纳入 SUT candidate。
- mutated 的严格 mutation_applied gate 不变；fault 分支不能成为真实 mutation 无 hit 时的通用豁免。health/baseline、transparent 的 neutral/no-op 分支仍按 D002。
- 此项解决 negative 独立控制与严格 mutation 阶段的合同冲突，不新增 scope、不改变成熟分母；fault 仍独立六行，预期 primary fail，只证明检测通道。

## 参考来源和阅读范围

以下四份旧仓库文档已完整阅读，仅作只读设计/证据参考；它们不是对新仓库完成情况的证明，也不递归导入其中的旧执行命令。

| 旧参考 | 内容边界 | 本次读取 SHA-256 |
| --- | --- | --- |
| `研究内容与当前困难详述-ros2-dds-fuzz copy.md` | 历史研究与证据快照；计划不等于执行、观测不等于缺陷 | `5ca726371a6a5ccd145c02c7a5b30c6829675515b05d78bb71f5e10c4cbf391f` |
| `AGENTS.md` | 真实 receipt、证据和旧 seal 纪律 | `e23dc2ed0d115c6ae3c7455702c547429344f4d57f46abaf821d55b067e4cccb` |
| `docs/CODEBASE_GUIDE.md` | 旧入口及原证据链；旧命令不可直接当 v2 CLI | `3233491bc2b9ca90491a52fba2d5a9029d326c9d35ce54bc855142089356f5b8` |
| `docs/plans/2026-11-12-full-fuzz-maturity-rebaseline.md` | 旧 S1–S5 成熟计划；不自动扩大 v2 的 S1 scope | `822601bac088085e444e3844e74df6094124ffa2dae4c35a2f64604175e458c6` |

以上相对旧参考路径均以 `/home/wuyuqa/ros2-dds-fuzz` 为根。旧计划要求“完整证据 + replay 才算候选”的历史口径不能覆盖新任务允许 instrumentation witness 入账、保留 replay failure、具名 non_replayable 的新合同。建议技能清单只是任务书原文内容。文档子agent未再派遣下级agent；主agent当前已派遣6个gpt-6.1-sol（新增网络隔离载体、vendor内部观察）。
