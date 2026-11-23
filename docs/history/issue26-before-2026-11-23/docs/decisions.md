# 决策记录

日期：2026-11-14。原规格见 [任务书原文](taskbook.md)。本页记录用户本轮范围和主agent解释，不改写原文，不表示功能已经实现或验收通过。日常工程问题由主agent裁定；确需用户决定的事项仅登记在 [待核实事项](open-items.md)，文档子agent不自行请求确认，也不假设它们已经阻塞。

## D001：本轮覆盖完整 S1，执行内容受当前要求约束

来源：主agent派遣消息；其中用户最新范围由主agent转述，工程执行边界由主agent声明。状态：已确定。

- 用户最新范围（主agent转述）：本轮继续完整 S1 的四个 scope target，不能把原文“第一轮到 Phase 3”当成本轮停止边界。第一条 slice 仍先完成，后续按原规格固定顺序推进。
- 主agent声明的执行边界：当前实际工作只实施 ROS2 公共 API 的正常互操作、可靠性测试和证据工具；不实施自动漏洞搜寻、恶意 wire payload 或漏洞复现。
- 原规格中的 instrumentation、bounded campaign、wire carrier、replay、candidate、seal 要逐项区分合同实现、真实运行、范围兼容性和证据缺口。保留原要求并核查，不用模拟结果充当真实运行。
- 当前用户目标为 S1 模块完全成熟且有 2–3 个真实 SUT candidate（本轮用户明确纠正目标口径）。实测自然候选为0，候选数量目标未达成；自然数量不能预先保证，只能由真实运行产生。原taskbook未设硬配额、允许零候选推进Phase，均不能降低当前用户目标，也不能据此宣称整个goal complete；可以如实交付中间报告。
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

## D013：reader 创建后重新采集双向 match，纠正工具时序误判

本次记录时间：2026-11-15T00:01:03+08:00（Asia/Shanghai，TZ=Asia/Shanghai date --iso-8601=seconds实测）。

来源：主agent工程裁定/派遣消息；原始报告及时间锚点由文档子agent只读核对，见 [首次正式矩阵记录](first-formal-matrix-2026-11-14.md)。状态：runtime ACK补丁freeze-ready；本次第二commit最终GO已收到，独立验收及精确提交范围见本节后补记录。

- 首份正式报告24条真实完成，但18条required为17 pass+1 unknown；六条fault按预期fail，health/mutation/fault各6/6、transparency 5/6，seal=false。主agent独立verify rc1。
- FastDDS transparent.1 的 writer match=1 早于 reader create_subscription 返回锚点747,061ns，六条post-match delivery完整。该工具采样/锚点误判不能变成SUT mismatch、自然candidate或真实delivery loss。
- reader创建返回后必须请求/采集**新鲜双向 match snapshots**：writer侧和reader侧各自实际观察，绑定本run、endpoint identity/generation、raw事件引用和时间。校验快照产生在reader创建之后，不能只证明receipt字段存在。
- 原来的早期match保留为历史事件，不篡改时间、不倒填reached、不用最终交付推造match。D010 writer-only mutation前置保持；匹配仍属于late_joiner的mutation后观察，不能倒置为前置。
- 第二次提交的行为修改范围由主agent限定为match snapshots/runtime adapter及对应回归tests；D014仅文档记录待实施，不随本次补丁修改第四target实现。须等代码agentfreeze与全仓测试后的新GO，届时按新的单次授权使用自动化身份提交。
- 这是工具时序纠错，不改SUT rule、target revision或18/72成熟分母。补丁改变source身份，按D011新提交/冻结后重跑；原正式report/raw/ledger immutable，unknown与seal=false保留。

D013 ACK补充及第二次GO（2026-11-15T00:14:58+08:00，Asia/Shanghai）：

- reader创建返回后发出本轮snapshot请求；两端各有唯一、命令关联的新鲜match_snapshot及command_done ACK，且matched_count为整数1，才可打开post_match_delivery窗口。此ACK是snapshot命令完成回执，不是DDS交付ACK。raw须满足request→snapshot→ACK→window_open的事件顺序，以及reader创建/请求≤采样≤snapshot≤ACK≤窗口开启的单调时间关系；核对角色、API、command_id、GID/generation及worker身份。snapshot和ACK分两次stdout读取也必须等齐；缺失、重复、提前ACK或不匹配不得放行，不得退回旧match事件。
- 主agent已独立验收全仓 **144 tests / 37.032s / OK**，diff --check通过、ACK逻辑审查通过；文档子agent只读检查上述实现，没有复跑测试。112项、131项结果保留为历史，不累计为真实矩阵计数。
- 本次第二commit最终GO已下达，范围仅为 `src/r2f/runtime/worker.py`、`src/r2f/runtime/ros2_pair.py`、`src/r2f/campaign/runtime_adapter.py`、`tests/test_campaign_evidence.py`、`tests/test_runtime_match_snapshot.py` 及当前docs/README/AGENTS更新；排除results。仅本次commit命令使用 `Codex Agent <codex-agent@localhost>`，不改Git配置，提交信息为 `Fix fresh match snapshot acknowledgement race`。
- 实际提交HEAD与冻结结果见git log及只读source_manifest；本段在提交前写入，不预先宣称提交成功。提交后不因文档追加commit，不运行ROS。首次失败report/raw保持immutable，D014只记录待实施，不随本次补丁修改第四target实现或分母。

## D014：第四 target 标准 endpoint 字段适用性与 generation 依据

本次记录时间：2026-11-15T00:01:03+08:00（Asia/Shanghai，TZ=Asia/Shanghai date --iso-8601=seconds实测）。

来源：主agent工程裁定/派遣消息；依据 [第四target可行性计划](../results/instrumentation-feasibility/fourth-target-plan.md)，文件SHA-256 `809cf69673d621126390ba305b2d109b1a1eaa1a8ab7d357cf56aa117314ea90`。文档子agent只读复核该计划列出的16个本地source hash，16/16匹配；未重新访问其外部引用。状态：owner主agent已裁定字段合同，实施稍后，第四target尚未正式执行/封存，无需用户另行确认。

复核补记（2026-11-15T00:02:39+08:00，Asia/Shanghai）：上述计划在并行工作期间追加了§11；现文件SHA-256为 `ec937a7e69c708415677671e6d9c231ef88b56f52e0918e259745e1f1e3f4c9d`。最初读取的28,628字节仍是逐字节一致的前缀，旧hash作为历史来源保留；16个本地source hash仍全部匹配。计划的接口提议不视为已实施，也不据此扩大本次match补丁范围。

- 必需对账字段：GUID、role、topic、type、reliability、durability、deadline、liveliness及lease。两来源独立，不能graph自比较或用测试配置反填独立观察。
- 按该计划核查的标准endpoint builtin schema，history/depth不作为Publication/Subscription endpoint的必需字段；PID_HISTORY对应Topic。history/depth仅作supporting diagnostic，缺失明确标记 `not_applicable_by_endpoint_schema`，禁止用配置填值。已真实观察到的vendor扩展保留其raw/basis，不能冒称跨RMW标准保证。
- history/depth的字段级不适用不能推广到deadline、liveliness或lease；必需字段缺失/解析不足仍记unknown，不能借N/A掩盖采集缺口。durability_service.history_depth也不能替代endpoint history.depth。
- GUID generation由独立capture与进程/endpoint生命周期关联：明确GUID、进程身份、创建receipt和观察窗口。不得编造不存在的wire generation字段，也不把capture到公告自动当成对端已经处理该公告的内部证据。
- 这是第四target revision0的首次模型定义；尚未执行或封存该target，不是删除已失败rule，不改第一target18或完整S1 72分母。原taskbook未枚举depth，旧catalog不能把超出标准endpoint schema的字段强行继承为全部RMW必需。
- 该计划的“待owner批准”、建议revision0→1及旧N/A命名属于提议时点；本裁定在上述字段/代际边界内取代这些提议，使用revision0及精确状态 `not_applicable_by_endpoint_schema`。其余capture实现、默认省略推导、control施加层仍须实现与验证，不能把字段裁定扩写为整份方案已实现/已验收。

## D015：第三target的真实发现drop/restore与netns生命周期

记录时间：2026-11-15T00:26:18+08:00（Asia/Shanghai）。来源：主agent工程裁定/派遣消息。状态：**尚未实现/验证**；既有scope、四target及完整S1 72 required分母不变，不据此宣称第三target已expressible或通过。

- 沿用既有carrier标识 `wire_proxy`，implementation显式记录为 `isolated_netns_nft_discovery_udp_drop`，不能隐去实际实现路径或把名称当能力证据。
- 只有mutated arm真正安装drop规则；baseline、transparent、negative走受审计neutral路径。negative独立omit中间postrestore seq，记录harness fault provenance、闭合证据并排除candidate。不能实际装drop却声明applied=false，也不能把negative omission伪造为discovery mutation hit。
- drop前置为本pair已matched。hit只接受本次真实ROS2 pair的discovery counter>0及关联raw，不得用合成UDP探针增加计数来证明DDS discovery命中；既有non-DDS控制只说明载体可行性。
- restore删除规则只证明恢复动作执行，**不等于恢复已经观测**。不要求必须出现unmatched/rematch，可以保持match continuity retained。
- 恢复须同时有restore后与本pair身份关联的真实discovery活动、新鲜双向count/graph以及postrestore delivery。仅有缓存graph或数据交付不够；不可用cached match冒充真实恢复观察。保留counter、规则安装/删除、活动、快照、数据及原始时间顺序。
- 父进程持有相关campaign/domain/state-directory锁，直到整个netns子树清理完成；不能以单个worker退出或先释放锁来宣称资源已回收。具体实现及异常清理路径须后续真实验证。

## F06/F07验收补充：零候选路径与真实重放缺口

记录时间：2026-11-15T00:26:18+08:00（Asia/Shanghai）。来源：主agent工程裁定/独立验收消息；详细证据账见 [完整S1逐项验收账](s1-acceptance-ledger.md)。

- 主agent确认第一target rerun在当前HEAD的独立verify：sealed=true/errors=[]、18 required judged pass、6 negative judged fail、四control6/6、unknown0。r1仅attempt名，同target_revision0；首次失败不覆盖。第二target已获执行GO，但完成状态必须以实际报告为准。
- F06在零自然候选时，可以真实zero-ledger、control排除及原始失败保留证明该审计路径已经执行；自然admission/triage仍没有真实异常证据，不为补验收而制造候选。此裁定仅涉及零候选审计路径，不能替代当前用户2–3个真实SUT候选目标；实测0，该目标未达成。
- core只读审计及文档子agent本轮复核：两次第一target正式run共48份capsule，replay_attempts、execution_history、non_replayable全部为0。首跑失败留存不等于replay failure；新attempt矩阵不自动等于core replay。
- F07下一phase必须分别有真实健康重放和可控工具失败真实重放；生成独立产物并记录attempt/failure及来源关联，不改原run/capsule/raw。健康pass不用于确认原异常；原fail后新pass不能据此宣称异常已复现。
- 现无需要用户确认事项；日常排程、环境冲突处置和验收由主agent协调。

## 已seal revision0合同兼容与开发解冻（主agent工程裁定）

记录时间：2026-11-15T00:30:42+08:00（Asia/Shanghai）。来源：主agent独立验收/工程派遣消息，文档子agent已只读核对。[正式证据快照](formal-target-snapshots-2026-11-15.md)列明四次attempt和hash。

- second r2/domain80独立verify通过，first/second都在source HEAD `11ec76c249cb75605fd70c5d7efb5610e70514b1`上达到per-target seal：各18 required judged pass、6negative judged fail、四control6/6、unknown0。domain181预检阻断、旧first时序失败全部保留。r1/r2均非target revision。
- 主agent已解冻third开发，runtime/campaign/core/instrumentation四方按独立写集推进；文档子agent仍仅写docs/README/AGENTS，不commit。后续source变化后上述通过只作原版本证据，须最终source冻结后rerun，不沿用为当前成熟声明。
- 不改first/second已seal revision0 target manifest。core为其精确legacy fault合同兼容，不把兼容扩成任意旧格式/缺receipt的通用放行；快照保存各target_sha256及完整legacy fault字段。
- 第三/第四新target显式声明receipt_kind/window及projection schema；首次seal前修正初版合同不等于新rule已完成或通过。D014/D015仍须真实实现、合同检查和18+6/control验收，不改scope/72分母。

## 主agent后续只读核查补记

记录时间：2026-11-15T00:38:53+08:00（Asia/Shanghai）。来源：主agent工程核查消息；probe产物另由文档子agent只读核对，见[验证快照](verification-snapshot-2026-11-14.md)。

- 当前开发中的first/second target.to_dict()与两份11ec76c正式manifest逐字段比较true，这是主agent报告的检查，不冒称本子agent复跑。target未变不等于source冻结；source dirty时原seal仍仅支持原版本。
- domains.scan_processes旧repo substring前缀误判新v2绝对脚本路径，runtime正在按真实路径边界修复。旧repo只读、真实子路径/进程/owner/锁保护不放宽；不能把修复解释成忽略旧仓库扫描或强抢资源。
- 两RMW被动SEDP attempt2的netns status=ok，parsed events为56/48且errors=[]；development_only/default policy pending，不计正式四target成熟或SUTcandidate。前次失败、原始raw和不同分析版本均保留；不把计划/解析器通过升级为rule或seal完成。

## 参考来源和阅读范围

以下四份旧仓库文档已完整阅读，仅作只读设计/证据参考；它们不是对新仓库完成情况的证明，也不递归导入其中的旧执行命令。

| 旧参考 | 内容边界 | 本次读取 SHA-256 |
| --- | --- | --- |
| `研究内容与当前困难详述-ros2-dds-fuzz copy.md` | 历史研究与证据快照；计划不等于执行、观测不等于缺陷 | `5ca726371a6a5ccd145c02c7a5b30c6829675515b05d78bb71f5e10c4cbf391f` |
| `AGENTS.md` | 真实 receipt、证据和旧 seal 纪律 | `e23dc2ed0d115c6ae3c7455702c547429344f4d57f46abaf821d55b067e4cccb` |
| `docs/CODEBASE_GUIDE.md` | 旧入口及原证据链；旧命令不可直接当 v2 CLI | `3233491bc2b9ca90491a52fba2d5a9029d326c9d35ce54bc855142089356f5b8` |
| `docs/plans/2026-11-12-full-fuzz-maturity-rebaseline.md` | 旧 S1–S5 成熟计划；不自动扩大 v2 的 S1 scope | `822601bac088085e444e3844e74df6094124ffa2dae4c35a2f64604175e458c6` |

以上相对旧参考路径均以 `/home/wuyuqa/ros2-dds-fuzz` 为根。旧计划要求“完整证据 + replay 才算候选”的历史口径不能覆盖新任务允许 instrumentation witness 入账、保留 replay failure、具名 non_replayable 的新合同。建议技能清单只是任务书原文内容。文档子agent未再派遣下级agent；主agent当前已派遣6个gpt-6.1-sol（新增网络隔离载体、vendor内部观察）。


## D016：P0观测完整性、状态迁移与持久反馈闭环（主agent工程裁定）

记录日期：2026-11-17。来源：主agent本轮派遣/补充裁定及[dispatch-and-gates.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/p0-feedback-closure-20261117/dispatch-and-gates.json)。状态：**工程约束已裁定，P0实施中，未验证完成**；不是人类用户原话，不改taskbook原文、既有required target/rule/合同或72分母，不授真实ROS/campaign/commit。实现owner及证据门见[主审计§12](/home/wuyuqa/ros2-dds-fuzz-v2/docs/main-audit-2026-11-17.md)。

1. **分层完整性**：统一base observation complete，须为judged＋actual、环境/进程健康、前置真实达到、endpoint身份及窗口证据完整且无evidence/tool error；不以oracle pass替代，也不能把正常规则fail自动当不完整。state transition在base之外另需raw-backed before/after快照、边界/身份引用。缺state快照表示迁移证据不足，不伪称endpoint identity mismatch；真实raw身份错误仍fail closed，不降格或用配置补值。
2. **真实边界与抽象限度**：before/after分别绑定原始事件边界，不可用after匹配结果倒填before；canonical固定pair八组件只作最小状态抽象，不声明Nendpoint拓扑/代际历史模型成熟。缺证据保留unknown/incomplete，不手塞edge制造反馈收益。
3. **epoch与恢复**：feedback保持append-only且显式推进epoch；完整checkpoint保存/恢复corpus、反馈prefix、连续sequence、warmup进度及采样选择状态。每selection记录可复算的prefix与corpus身份，split/resume须在同序观察和seed下与连续执行等价。旧证据只读，resume须核source/profile/target/contracts；不能删除prefix guard或编辑旧run冒充连续性。
4. **真实预算**：实施cycle monotonic deadline，到期停止新input并按既有归属/清理合同收尾；selection尝试、actual执行、有效非重复输入分别计数，拒绝不算actual。80输入/30min是预算而非自然候选数量保证，未验证整条反馈/恢复/预算链前不以加长运行代替修复。
5. **执行入口source身份**：主agent新检查发现原source_manifest遗漏`scripts/run_s1.py`，本轮要求纳入该行为入口并验证变更检测/漂移处理。dispatch的before_files只是修复前参考，不证明source monitor已覆盖；后续必须新source冻结、整合与真实验证，旧source evidence/seal不迁移。前轮1143项离线pass不背书本轮变更，owner停写也不等于source_frozen。

此裁定是P0工具证据与闭环修复约束，不扩大mutation/carrier/scope，不给控制/合成fixture新增自然候选资格，不变更canonical execution_stage/嵌套receipt。三owner代码、局部测试、整合回归和真实验证须分别出证；当前无新执行/候选/成熟声明。


## D017：显式F2 windows探索合同与raw锚点时序（主agent工程裁定；实施中）

2026-11-17，依据主agent[dispatch-and-contract.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/f2-windows-activation-20261117/dispatch-and-contract.json)（SHA-256=`69416f20f07fda4bb376cbb955bfdb6009186b4a2879a1a7d0c204f188d4c86f`）及[before.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/f2-windows-activation-20261117/before.json)。这是main工程裁定，不是用户原规格字面要求或完成证明；保存P0的170文件稳定dirty快照后，main仅解冻指定三代码写集，未授ROS/commit。

1. **显式新合同，不改旧合同**：新增探索target `discovery.interruption_recovery.fuzz_windows_v1` revision0、capability `f2_windows_v1`；四required及五旧fuzz完整manifest必须与before逐字等价，分母72不变。新探索catalog/hash/source身份须显式记录，不迁移旧checkpoint/seal。旧F1拒绝路径、F2 generation-only API/seed与原template provenance维持，不把generation或seed升级为execution。
2. **受限路由与未决guard**：新增`f2_live_surface` API仅接E02/M07、E03/M06；新profile为enabled=false、runtime_capability=f2_windows_v1、仅新target、chain_length=1及精确E02/E03 cycle白名单。E03允许1..3窗，首窗fresh_match、后窗previous_restore_confirmed须一致；不开放G01/W01或无限制组合。`needs_observation`是未决runtime guard，不是pass或计划成功；仅新target可结构化prepare，动作前必须由真实raw前置/锚点/前窗恢复证据消解，禁止向mutation context硬塞anchor_observed/post_match_observed=true。
3. **锚点与not-before语义**：仅新capability使用新时序。首锚点为绑定fresh双向match的controller `discovery_precondition_snapshot`，不是未观测的最早network match；后锚点为真实前窗`discovery_recovery_confirmed`。offset是相对锚点的not-before下界；有前窗时最早eligible drop为`max(anchor_mono+offset, previous_recovery_mono+previous_restore_interval)`，既有interval等待可消耗offset预算，不重复相加。记录精确anchor、eligibility deadline、actual start/install及overshoot，不保证精确时刻执行。
4. **确定性身份与harness归因**：最终input纳入`anchor_timing_policy=f2_anchor_not_before_v1`及`anchor_lateness_tolerance_sec=0.5`，二者进入input hash。实际install不得早于eligible、迟到不得超过0.5s；超限为harness timing miss/incomplete，不能算SUT候选。runtime与adapter须各自从raw时间戳独立计算，不从计划值或cleanup倒填。
5. **决定性验收门，均待本轮证据**：真实registry/runner生成确定性M06/M07计划且旧路径仍拒绝；离线fake clock覆盖max非sum、错/未来/stale anchor拒绝及超时归因；adapter独立拒绝伪offset/窗数/interval/identity/drop hit并接纳raw-backed健康fixture。E02/E03 hit必须逐个active window有本pair非零真实drop counter、对应计划窗数/interval或anchor+offset，且有restore后活动/fresh graph/match/delivery闭合，不能借E01、aggregate或仅窗数充hit；plan与execution receipts分开。停写后须新source最终离线整合；旧P0通过不覆盖新source，真实执行还须独立预检及明确GO。

owner（均gpt-6.1-sol）：Hypatia负责target/catalog/module/provider合同；Plato负责corpus/runner/input/checkpoint/profile路由；Dirac负责runtime/adapter/state projection及时序raw；Halley仅文档。具体路径边界以dispatch.file_write_scopes为准。当前E02/E03真实路由/锚点时序仍实施中，未声明通过；live预检障碍保持、source_frozen=false、自然候选0、goal active，不改原spec/acceptance/ledger。


## D018：独立执行边界supervisor与跨namespace归属（main工程裁定；实施中）

2026-11-17，来源为main本轮显式派遣/补充消息、[environment-worker-dispatch.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/f2-windows-activation-20261117/environment-worker-dispatch.json)（SHA-256=`9b388f0b58cc9c30364fcf6ff68b23e6cd6ca738554d8bb0b4b3f82e6f13f09d`）及[boundary disposition](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/f2-windows-activation-20261117/environment-boundary-disposition.json)。main新增Godel，id=`01a1e93d-1b2f-74b0-ae9e-dfc56ab68ad9`、model=`gpt-6.1-sol`，仅写新文件`/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/runtime/execution_boundary.py`、`/home/wuyuqa/ros2-dds-fuzz-v2/tests/test_execution_boundary.py`、`/home/wuyuqa/ros2-dds-fuzz-v2/scripts/probe_execution_boundary.py`，不碰三个F2代码写集。是main工程授权，不是用户原规格字面要求，也不是Halley再派遣。

- **状态与集成边界**：研究性实现完整独立边界supervisor，目标先以非DDS证据验证；目前实施中、implementation_not_yet_verified，既有namespace/preflight可行性不等于supervisor已通过。未来`run_isolated_runtime`仅由统一runner的runtime callable选择，不另建runner/候选/报告体系，不因新增文件自动激活live。
- **两层锁与kernel身份**：host campaign/domain/state三lease必须持有至整个inner PID namespace子树退出；inner使用独立命名lease-root，不混用共享host lockroot。host PID/start ticks、inner PID及user/net/pid/mount namespace关系须kernel认证；禁止以内层PID冒充host owner、修改他人owner JSON，或让继承flock重复/提前释放。
- **失败清理与scanner**：PDEATHSIG、kill-child、timeout及取消/崩溃路径须证明后代清理、PIDNS全树退出后才释放host lease。仅在完整独立执行边界内用未改scanner作真实内层预检，不是隐藏proc、白名单或虚构host clearance；旧仓库只读，历史host失败不改写。边界外/内PID、锁、清理链及必要loopback readiness均须出具非DDS证据，不能用探针成功替代。
- **后续门**：新源码需停写后最终冻结、相关回归及独立main live GO；当前无ROS/commit GO，source_frozen=false。D017/F2合同、原spec、四required＋五旧fuzz完整manifest及72分母不变，P0旧snapshot不迁移；自然候选0、goal active，不宣称新execution/candidate/seal。

当前main直接协调五名owner：Hypatia、Plato、Dirac、Halley及Godel，均gpt-6.1-sol；前三F2写集不变，Halley保持docs/README/AGENTS独占。旧“六名”人数是历史轮次，不是本轮名册；本记录不授权文档owner另派下级或运行业务测试。


## D019：S1接续离线修复与N端点统一接入证据门（main工程裁定；实施/复审中）

日期：2026-11-18，Asia/Shanghai。来源：main本次派遣/进展补充消息、[主审计](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/main-review.md)、[纯registry查询](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/topology-registration-probe.json)；逐项产物及核证时点见[2026-11-18接续核证](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md)。以下为main工程解释，不是用户原话、原taskbook新增原文或完成证明；最新子agent型号要求为 `gpt-6.1-sol`，此前e2ee记录已被覆盖，旧失败不删除。

1. **局部验证不等于最终冻结**：422pass/11skip是修复前相关离线快照，208个行为文件before/after映射相同；87pass的netem检查独立计账，不相加宣称全仓或成熟。输入ID/native有最终局部离线产物，其余修复/复审以实际日志为准；owner ACK、单个final文件名或局部pass不等于shared source_frozen。新增Halley（id=01a1ebfe-3eb0-7c60-8386-ed6c247c4bc7）的identity性能优化仍待证，只能测量后批量git读HEAD、保持等价校验，不放宽source gate；不是旧文档Halley。
2. **统一registry/输入/replay**：纯查询证明审计时点未注册topology target，假设capability `topology_v1` 被拒绝；字符串检索仅辅助定位。继续统一Registry/Scenario/MutationPlan/Carrier/FactProducer/Oracle/ReplayCapsule，不把run_topology当独立campaign。正式路线在执行前封存最终program、层顺序、final_input与确定性input_layer_receipts；拒绝runtime再次mutate_chain后冒称相同输入。oracle_ids仅bindings、无resolver不算独立oracle。隔离路由须核身份、cleanup、raw/source及默认host兼容，不只扩allowlist。
3. **预测不等于观察**：semantic或runtime按program/QoS计算的兼容图属于prediction，不是observed match/novelty。N图须有按定向writer/reader pair、generation、窗口绑定的真实GID/PID/start_ticks、graph和delivery事实；总count、配置或首对endpoint不能补齐其余边。有限窗口未match不自动fail，多writer不设全局有序义务。scenario carrier不等于实际mutation requires/begin/applied/hit/effect接线；不能直接把原false receipt改true。
4. **状态须版本化扩展**：既有state_signature与state_projection仍是canonical pair八分量模型，不是N端点拓扑/代际历史。扩展统一state abstraction、facts/projection及episode依赖时保持旧pair身份不变，逐分量引用before/after raw chronology。剥离raw GUID/PID/time不取消独立身份审计；state身份与history身份分离，prefix保留有界不同路径，不用raw身份噪声或预测图指纹制造新状态。
5. **复用载体，不伪造并发**：优先复用DiscoveryNetemController、namespace_pair._run_netem_window及既有environment_mutation/replay路径；孤立UDP queue测试不是DDS附着/命中证明。reorder约束、层/seed绑定及真实pair计数仍待验证。批量发布一次command ACK不是跨writer真实重叠；有界异步开始/完成屏障与实际重叠区间须独立出证，batch变大或顺序执行不等于并发压力。
6. **范围/分母与最终门不偷换**：扩展target/capability须显式版本化、与原四required manifest分离并另列验收义务；不能永久贴exploratory标签后宣称完整S1成熟。原四target revision0合同、72分母及历史失败不变；涉及范围/约束变化只登记open-items交main裁定。当前HEAD42fcec65…且dirty、source_frozen=false、无live/commit GO、自然candidate=0，整体目标未complete。最终稳定源码仍需main整合、明确GO、source冻结及按新身份安排唯一live owner；旧seal不迁移。

补充（main最新直接能力清单）：[current-input-mechanism.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/current-input-mechanism.json)的14surface/33slot/24operators与26authored/5pending/2unsupported/1out_of_scope均为offline input_only；10个oracle引用仍 `unresolved_no_resolver`，不得写成24算子已在runner fuzz。指定接线路径未见三新模块直接源引用只作辅助，registry拒绝才是独立查询证据；下一段主缺口是统一输入消费/计划/事实/oracle/replay接线，不是简单延长运行。已有netem delay/reorder/duplicate基础明确保留，清单pending不外推成全仓缺实现。

最终路由门（main最新补充）：Feynman接线保留isolated资格unverified/fail-closed，不能宣布正式隔离replay成熟；Darwin恢复后新增boundary_audit纯artifact一致性核验写集，不是kernel再认证或真实执行证明，native两文件仍freeze。Kant的summary/freeze ACK已实证两项模型修复及360pass/0xfail，仅为纯模型局部结项。


## D020：opt-in topology接口、观察归因与正式路线门（main工程裁定；局部离线正证据，整合待验）

来源：main本卡移交及两次明确补充、[main-topology-interface-decisions.md](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/docs-topology-owner/snapshot/main-topology-interface-decisions.md)与[09:11派遣快照](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/docs-topology-owner/snapshot/continuation-dispatch-20261118T0910.json)。固定cutoff：`2026-11-18T09:49:17.467623+08:00`。这是main工程解释，不冒充用户原话或taskbook原文；历史D019及失败保留，分层实证见[本卡固定核证截面](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md)的§10。

1. **显式扩展，不改默认**：target=`discovery.endpoint_topology.fuzz_v1`、capability=`topology_v1`、kind=`endpoint_program`、carrier=`ros2_endpoint`、surface=`S1.topology.program`、spec=`I2.topology_program.v1`；沿用统一registry/runner/MutationPlan，binding=`topology.program`。默认旧5项、F2独立授权、四required manifest/hash及72分母不变；新拓扑须单列checkpoint/矩阵/成熟义务，不能以exploratory永久免验。
2. **输入先封存，验证分层**：final_config仅`{"topology_program": program}`，逐层raw内容/连续hash在`operations[i].parameters.input_layer_receipts[0]`；v2去重envelope、raw program、config/plan/effective-input hash不混同。core只做通用action_program有限JSON、内容/hash连续与合同验证，不import S1；provider/runtime必须显式`validate_topology_plan`，provider/runtime/replay均不得封存后再变异。同路径累积组合必须有真实predecessor→successor及逐步changes，不靠标签跳过冲突检查。
3. **N图不由总count补造**：2W2R反例中同topic graph可见且每端count=1仍对应两种不同边集；helper的matched不是独立pair证据。缺per-peer或具名controlled-isolation证据则unknown，非SUT异常。状态/投影显式版本`r2f.s1-topology-state/v1`、`r2f.s1-topology-state-projection/v1`，旧pair身份及八分量不变，history另记。
4. **交付仅按实际归因**：允许`endpoint_correlated_payload`以本run唯一send raw的payload hash＋run/writer/generation/seq和worker身份链对账；不是direct per-sample GID，`unavailable_api/null`保留。歧义/缺send/跨run-generation仍unknown；单pair交付不证明其余边，不跨writer排序，不因finite尾missing判fail；中间missing仍须同writer-generation更晚sentinel，negative归harness不入候选。
5. **正式route不得claim放行**：canonical `run_pair`接既有carrier/ingest/ExecutionRecord/report/candidate/replay。真实三lease、environment/source/owner/port/process检查与整个endpoint子树cleanup不可被`live_execution_go=true`或非空字符串替代。当前无live/commit GO是调度状态，不是永久disabled实现；离线helper一致性不是kernel再认证或真实执行资格。
6. **capsule P0按增量证据更新**：main此前静态指出`core/replay.py`约190/430行强制单pair topic/type/qos；这是静态发现，不编造实际失败次数。Feynman已实现通用`payload_layout`（`r2f.replay-action-program-payload/v1`）；main Registry→真实provider→ReplayCapsule局部成功，`main-topology-capsule-before.json`实际为`capsule_constructed`，名字before不是失败，其`schema:null`据main为probe查错key。已获局部增量正证据，独立replay/完整整合仍待验；随后fixture统一ingest两层工具错误见§10，不能推导真实hit或SUT candidate。


## D021：required/exploratory 同一语义类型与统一家族目录（main 工程澄清；实施中）

日期：2026-11-18，Asia/Shanghai。来源：本卡 main 直接派遣及后续 Hooke／进程状态补充、[本轮 dispatch 固定副本](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-owner/main-message-supplement-1/dispatch.json)、[任务卡来源记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-owner/task-contract.json)。固定证据截面 `2026-11-18T13:19:50.494563+08:00`，明确新消息另记 `2026-11-18T13:24:34.359693+08:00` 补充截面；不是 taskbook 新原文，不把 main 工程裁定冒充人类用户原话。实现及实际验收状态见[接续核证 §13](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-resume-fixed-cutoff-1311)。

1. **同一语义类型，不分两套模型**：required 与 exploratory 都使用 `core.SemanticTarget`；区别在 campaign 角色与输入／覆盖策略，不是两套语义类型、执行状态机、oracle 或 candidate 系统。required 是固定 conformance 基准；exploratory 扩展 fuzz 输入并复用适用的规则、状态与统一 registry/runner/facts/replay。D019/D020 的“分列证据／成熟义务”不是把二者永久隔离成两套语义系统。
2. **统一目录／家族解析尚待实现验收**：Hume 编码统一目录、family resolver 和 CLI 消费；本决定不是目录整合已完成或所有 runtime 算子已接入的证明。保留旧 target 身份仅为仍在使用的验收／replay 兼容，不继续将 legacy 名称误解释为语义类型。不能借“融合”重写旧 manifest、搬运旧 seal、重标旧 run、合并不同 identity 的执行账或隐藏历史失败。
3. **冻结基准不变**：原四 required target 的 revision0、完整旧 manifest 及 `4 × 2 RMW × 3 arm × 3 repeat = 72` 分母不改；control 引用不重复计数，blocked 不减分母。探索扩展仍有真实执行／receipt／规则适用性／unknown／replay／独立成熟义务，不能因 exploratory 标签长期免验，也不能将扩展行折算成旧 72 格通过。
4. **身份与复用边界不偷换**：D003 的 `campaign_kind` 不入输入 hash 继续有效；同一 target/revision、RMW/repeat、层序及最终 payload/config 相同时才按该合同比较输入身份。共享 `SemanticTarget` 或同一家族不意味着不同 target ID／revision 的 hash、rule、窗口或旧 evidence 可互换。规则／状态复用须保留实际适用条件和 provenance。
5. **决定性核验点**：Hume 停写后由 main 对旧 required／legacy exploratory manifest 做逐字对账，核统一目录→family/CLI 解析→真实 provider/计划/capsule/replay 兼容；算子接入及 delivery-window producer 分别核 raw chronology、事实／投影消费和拒绝路径。目录可列出／CLI 可解析仍不是 runtime hit、完整回归或 seal。当前工程 owner 及 CFT 范围以本轮六人 dispatch 为准，旧不可访问 handle 不算活动 owner。

状态：**工程解释已记录，代码整合与成熟核证未完成**。HEAD `42fcec65fe66d1dff8dd761bf03ee96a427277c7` / dirty / `source_frozen=false`，goal **active，非 complete**；自然 candidate=0，2–3 真实 SUT 候选目标未达。本轮无新增 live／commit GO，无新 RMW/domain/run_id 执行；D021 不改变 scope、72 分母、revision0 或旧 manifest。CFT 原“无 API”判断与 §12 的证据越级仅作事实勘误，不据文档裁定宣称 Hooke 已交付或当前 source 已通过。


### D021 后续离线里程碑核收（2026-11-18；不新增语义／规则合并决定）

来源：main明确小型follow-up派遣与[固定15件证据](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/cutoff.json)，cutoff=`2026-11-18T13:44:10.558816+08:00`；原D021“实施中”为其当时截面，不回写。现在仅接受**离线目录整合**：Hume owner124pass（含39新增）及freeze ACK，main已关闭Hume；main独立85pass、CLI exit0实际列出11个exact identity／6个family，21条分组manifest对账全部相同（不是21个不同target）。[main catalog result](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/snapshot/main-catalog-review/result.json)／[JUnit](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/snapshot/main-catalog-review/junit.xml)有据。family是目录关系，不把旧ID互相alias，不声称原规则／版本合同已合并或删除，不改变required72/revision0/旧manifest，也不等于新source真实覆盖或成熟。

Descartes owner138pass、旧三`param.enum/queue.depth/param.boundary`的9/9完整audit等价及netem原字节保留有交付记录；main独立125pass、七个source/测试依赖在该测试窗口前后无漂移。**新三`relation.pair/timing.shift/lease.value`仅configuration audit已接入**，不给match、timing／lease lifecycle effect或hit资格；旧三原合同／hit路径不变。[算子main复核](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/snapshot/main-operator-review/result.json)只支持该离线窗口，owner freeze不等于全source freeze。

下一critical path由main本卡明确为：**当前producer/consumer稳定收口 → 源码冻结 → 精确新路径短真实DDS闭环**。Mencius／Hooke仍在写pair／worker(CFT)及指定测试，尚未验收；继续写未被消费者调用的helper不算完成。能力计数分层、无成熟百分比，详见[接续核证§14](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-resume-docs-followup-1344)。HEAD42fcec65…dirty/source_frozen=false、候选0、goal active，无新增live/commit GO或scope变化。


**明确main消息独立补截面（`2026-11-18T13:48:47.092310+08:00`）**：Hooke初始CFT卡已有[delivery](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/cft-main-message-supplement/delivery.json)／[freeze ACK](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/cft-main-message-supplement/freeze-ack.json)／[final test result](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/cft-main-message-supplement/test-result-final.json)，owner65pass（21新增）、11个直接依赖在该测试窗before/after一致；main已关闭Hooke。仅核收初始reader配置／实际读回的**离线交付**，真实RMW尚未测，无dynamic filter／sample-effect／lease主张；不等于source_frozen或S1成熟。本补充不回写13:44旧截面。main当前明确Mencius是唯一代码writer，窗口producer仍在收口、未验收；`/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/main-integrated-regression/run.py`仅**已准备、未启动**，等Mencius freeze才做全仓。本docs卡未读取／执行该脚本，不借准备状态预填回归结果；无新增live/commit或scope。


**后续main消息独立补截面（`2026-11-18T13:55:10.821258+08:00`）**：Mencius已有[summary](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/mencius-main-message-supplement/summary.json)／[freeze ACK](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/mencius-main-message-supplement/freeze-ack.json)／[最终记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/mencius-main-message-supplement/producer-expanded-followup.json)，owner最终288pass/39.91s、8依赖在该测试窗稳定；31聚焦与288重叠不累计。写集仅`/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/runtime/topology_pair.py`及`/home/wuyuqa/ros2-dds-fuzz-v2/tests/test_topology_delivery_windows.py`、`/home/wuyuqa/ros2-dds-fuzz-v2/tests/test_topology_runtime_route.py`三文件，pair SHA=`25de6fa16ed37eae311513fd1e9c544a0ca3e410e2cd450ea7055511dfb2450b`。main已关闭Mencius，当前所有代码owner均停写；这只是owner离线交付／写集freeze，不等于全source冻结、真实DDS窗口effect或S1验收。main已启动`/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/main-integrated-regression/run.py`的实际全仓`tests/`，**本消息时仍运行、结果未知**；`R2F_BOUNDARY_KERNEL_TESTS=1`是non-DDS kernel对照，不是ROS/DDS/campaign或成熟证明。本docs卡不运行／等待／借用该测试，不回写13:44与13:48历史或已freeze证据；仍需main整合核证→源码冻结→精确新路径短真实DDS闭环，无新增live/commit GO、候选或72分母变更。


<a id="s1-configuration-intervention-d022"></a>

## D022：真实配置干预hit与语义effect分阶段（main工程裁定；实施中）

日期：2026-11-18，Asia/Shanghai；固定cutoff=`2026-11-18T14:18:26.873740+08:00`。来源为main本次明确工程裁定及[main派遣与裁定来源记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-intervention-owner/task-contract.json)，**不是原规格字面新增条款或人类用户原话**。[taskbook §7.4](/home/wuyuqa/ros2-dds-fuzz-v2/docs/taskbook.md)已将`mutation_applied`（carrier_admitted/configured/applied/hit）与`effect_observed`（expected/residual effect有raw observation）分阶段；main明确指出现有core允许`hit=True/effect=False`，本docs卡只记录该工程依据，不声称重新运行core验证。

1. **旧局部阶段结论不回写**：此前`relation.pair/timing.shift/lease.value`只交配置审计、无hit，是当时局部阶段freeze；其旧ACK、局部结论和全部红日志原样保留，不能追认旧run已干预成功。下一阶段不能永久把实际配置干预与lease生命周期／匹配效果混为一谈，也不能为认hit而伪造这些效果。
2. **configuration intervention hit的联合门**：仅当sealed before/after经真实API映射后仍不同（排除rounding aliases／等价映射），原始实际after readback齐全且可追溯、endpoint/worker身份与代际及时序正确、没有后层覆盖，并且**formal real qualification满足**时，才可认configuration intervention hit。缺任一项即不能由本裁定授hit；本卡无该资格的真实运行证据。fixture、预设booleans、配置回声或仅requested值变化均不授hit。
3. **hit不推出effect**：未知语义response不自动算effect，更不推出expiry、renewal或match变化。`effect_observed`无独立证据保持false，语义结果仍由typed facts／既有oracle独立判断；不得把hit当effect、oracle pass、candidate或seal，也不另建执行状态机或放宽原规则。
4. **既有算子与CFT消费边界**：旧三`param.enum/queue.depth/param.boundary`既有parity／合同保持不变。将**已有**`filter.expr`按同样强证据门接入现有初始CFT raw消费路径；不是新增算子，不声明动态filter、样本效果或完整RMW支持。初始CFT配置／读回的旧离线交付不因此自动取得真实hit或effect资格。
5. **实施与成熟义务**：main显式解冻Banach与Sartre两卡；Banach负责环境观察与隔离1W1R闭合，Sartre负责真实配置干预gate及`filter.expr` consumer，均**实施中、未交付、未真实执行**。不新增target／rule／revision／算子或72分母；新增scope的独立成熟义务仍在，不能用required72通过代替，也不将旧回归迁移到后续源码。

回归独立补截面：[main result](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-intervention-owner/snapshot/main-integrated-regression/result.json)／[JUnit](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-intervention-owner/snapshot/main-integrated-regression/junit.xml)／[skips](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-intervention-owner/snapshot/main-integrated-regression/skips.json)确认main先前全仓**3406passed/3skipped**，wrapper411.87s（原值411.86933430598583s），JUnit410.821s；result记录253个source/test文件在该测试窗稳定、HEAD前后不变。两skip是真实bare non-ROS DDS native opt-in未开，一skip需retained real provider report。non-DDS kernel boundary对照已启用，不是ROS/DDS/campaign、真实效果或成熟证明。此前“仍运行”保持历史；[Banach dispatch](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-intervention-owner/snapshot/next-observation-dispatch.json)已显式标记新batch解冻及旧结果不可用于新改动。HEAD `42fcec65fe66d1dff8dd761bf03ee96a427277c7` dirty／`source_frozen=false`，candidate0、goal **active非complete**；原四required revision0／旧manifest／72分母不变，无新增live/commit GO。本决定**实施中**，非代码完成或验收声明。


### D022既有filter.expr输入生成工程缺口增量（main派遣；实施中）

cutoff=`2026-11-18T14:24:02.984960+08:00`；[本卡单件证据/SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-filter-increment-owner/cutoff.json)，不另立新规则。main已发现当前valid filter `seq >= %0` 与worker使用的`std_msgs/msg/String`仅有`data:string`不符；这是**工程缺口／实施中**，不是原规格新增要求，不能据“valid”标签宣称可执行。Rawls `01a1ed4e-08f6-77b1-b709-dbb21cd25064`，main显式请求`gpt-6.1-sol`，不推actual backend；来源[main派遣来源记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-filter-increment-owner/task-contract.json)及[implementation-contract固定副本](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-filter-increment-owner/snapshot/implementation-contract.json)。其写集仅`/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/modules/s1/fuzz_mutators_v2.py`、新`/home/wuyuqa/ros2-dds-fuzz-v2/tests/test_topology_filter_generation.py`；`/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/modules/s1/input_model.py`仅必要的局部filter元数据／字段描述，非一般模型重写授权。

要求依选中endpoint声明的message schema生成合法、非no-op的`data`字符串SQL，literal／参数类型正确；未知type必须具名MutationRefused或显式unknown，不能冒称executable valid。显式empty类和invalid负类保留，invalid期望guard reject。任务卡计划核profile/corpus I2 provider→prepare/MutationPlan→最终topology_program并与既有filter_backend.validate_filter对照，含两层组合、参数类型、unknown与确定性hash；**这是待做验证点，不是已通过结果**。本docs卡不读／追踪该卡代码、不跑测试。

只涉及输入生成／离线合同，不授filtering／match／sample-effect／hit或RMW支持，也不升级[D022](/home/wuyuqa/ros2-dds-fuzz-v2/docs/decisions.md#s1-configuration-intervention-d022)的formal real qualification。旧hash／执行证据和红日志保留，不借先前3406回归证明后续source通过；不改target/rule/revision/算子／required72。其他三owner Banach、Sartre及Helmholtz沿用现任务，无新结果推断。HEAD42fcec65…dirty／source_frozen=false、candidate0、goal active非complete；无live/commit GO、无新增真实RMW/domain/run_id／raw/replay。


<a id="s1-fair-base-input-scheduler-d023"></a>

## D023：显式fair基础输入调度与运行反馈消费分层（main工程裁定；实施中）

日期2026-11-18，Asia/Shanghai；cutoff=`2026-11-18T15:39:18.528072+08:00`。用户质疑为需求来源，以下具体策略／接口边界来自**main工程裁定**（[main裁定／任务来源记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/task-contract.json)、[main pre-dispatch固定记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/snapshot/pre-dispatch-evidence.json)），不是原规格字面或用户原话，不写成用户已批准默认更换。证据为[7件原字节/SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/cutoff.json)，此决定不表示代码已实现／验收。

1. **显式新增fair策略，不替换旧默认**：新增可显式选择的、无运行结果反馈驱动的基础输入scheduler策略。原省略策略时的feedback路径／旧默认保留；省略新选择的旧log／replay字节兼容必须保持，不用重写历史产物来“兼容”。具体接口由main协调代码owner，本docs不自造CLI flag、默认切换或公平性算法。
2. **固定输入条件下的选择不受结果反馈改变**：fair基于固定corpus／seed／history；在这些条件固定时，observed edge／coverage／oracle等运行结果不能改变所选seed、sampling或变异RNG。既要核scheduler选择，也要核runner mutation RNG消费，不能只忽略一个scheduler字段却让下游结果反馈改变输入。它是基础输入采样策略，**不是BFS／DFS状态planner，也不证明真实新状态发现**。
3. **不是全campaign完全无反馈**：反馈ledger记录与corpus retention准入仍保留；调度选择无运行反馈与结果记账／保留准入是不同层。上述固定corpus条件不等于保证retention改变之后整个campaign轨迹也独立于反馈，不禁用原反馈系统来伪造对照。
4. **实施分工与决定性验证点**：Rawls负责scheduler、两旧测试及新tests；Sartre负责runner窄接线、mutation RNG／CLI策略与resume及新tests；Banach负责真实boundary callable的non-DDS纠错probe；Helmholtz为本sole docs。main均请求`gpt-6.1-sol`，actual backend不推断。当前**实施中、未有本卡fair交付或验证**。待代码owner交付后main核固定corpus/seed/history、只变observed反馈的选择／sampling／RNG不变，旧省略路径／历史log-replay兼容，以及ledger／retention仍有效；这些是后续验证义务，不预填通过。
5. **身份与成熟分层不变**：不新增target／rule／revision／算子或72分母；扩展scope仍有独立成熟义务，不能用required72或离线测试数代替。input capsule、计划、fair选择与日志元数据不构成S1执行／effect／candidate／seal，也不授live/commit。

**固定的旧离线交付，不是新scheduler交付**：[Rawls JUnit1](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/snapshot/Rawls-filter-run-1.junit.xml)／[JUnit2](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/snapshot/Rawls-filter-run-2.junit.xml)各281项、0fail/error/skip；是两次同子集核证，不能累计成562个独立通过。[Rawls旧filter ACK](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/snapshot/Rawls-filter-freeze-ack.json)的原字节SHA与main内嵌ACK一致，记录20依赖在两测试窗稳定及provider重现输出相同；本docs不读／rehash源码。[Sartre JUnit](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/snapshot/Sartre-affected-173.junit.xml)173项、0fail/error/skip，[Sartre旧干预ACK](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/snapshot/Sartre-affected-freeze-ack.json)同样精确匹配main hash，记录旧干预写集／测试窗稳定、netem block保留；仅离线，不给execution evidence或candidate资格。两owner的7个source/test精确hash与netem块hash见[owner精确source/test hash索引](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/owner-source-identities.json)，只是ACK封存身份，不等于当前source身份或真实RMW/hit/effect。

**main观察等级分列**：[main pre-dispatch固定记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/snapshot/pre-dispatch-evidence.json)记录3538pass/12skip/451.13s、exit0仅来自exec观察，**无saved main JUnit／完整stdout及before/after稳定行为hash**，不外推新source通过，12skip原因未捕获、不猜测。另209pass/67.66s、418pass/4.74s也仅exec子集观察，范围重叠不累计；根目录无路径pytest曾rc2／169 collection errors（历史results测试快照被收集），原失败和快照保留，main后续改为显式tests/，不清理删除历史。先前3406/3的saved-artifact截面也原样保留，不与本次12skip互换原因。

**Banach旧失败保留**：[旧失败probe-assessment](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/snapshot/Banach-failed-probe-assessment.json)记录outer simulated boundary loopback `SIOCSIFFLAGS` EPERM／rc1；nested child未创建，nested PID/mount/proc／UID-map证明未获得，proc inventory未运行，不是nested验证通过或通用环境不可解证明。main提出“外层模拟在exec非零UID可能丢capabilities”仅为**待诊断假设，未证实**，不能写成根因；新卡应核真实execution_boundary callable路径，但本卡不运行或追踪纠错probe。

HEAD `42fcec65fe66d1dff8dd761bf03ee96a427277c7` dirty、source_frozen=false；source当前解冻，candidate0、goal **active非complete**；原四required target/revision0／旧manifest／72分母不变，无live/commit GO。 本docs仅追加与被动核证，不读改代码、不跑pytest/ROS/netns/staging/commit、不派下级、不追后续修复；完成精确append审计后freeze停写。D023状态为**实施中，未完成**。


### D023后续固定cut离线milestone接受（main工程裁定；不覆盖旧实施中截面）

固定cutoff=`2026-11-18T16:03:44.254141+08:00`；[12件必要原字节/SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/cutoff.json)；[main milestone acceptance](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/milestone-acceptance.json)原字节SHA=`1eef8020a1a2341da84794d163887230d8116b2f33279b7a3cecf06db231503a`精确匹配。 来源为main当前接续消息及[main派遣／边界来源](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/task-contract.json)，不是原规格新增或用户默认更换授权。此前D023“实施中／未交付”保留为当时状态；**本cut仅接受离线scheduler／runner／CLI milestone，不宣称D023全域完成或S1成熟**。

[main integration result](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/integration-result.json)／[JUnit](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/integration-junit.xml)核为346项中的**337passed/9skipped、0fail/error**，9skip全部为`opt-in non-DDS kernel namespace tests`，不是上一cut的未知12skip或旧native/provider三skip。result记录262个source/test文件before/after稳定、changed=[]、HEAD不变；仅该整合合同子集测试窗口有效，本docs不读或rehash这262源码文件。wrapper179.42461072304286s、JUnit178.651s分列，**不是全仓通过或source正式freeze**，不可迁移到Banach随后改动。

[CLI corrected assessment](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/CLI-corrected-assessment.json)为实际CLI、no mock source、纯planning：first2＋resumed新增2的10项具体输入字段（content/parent/effective/plan hash、seed、stage、final_input、mutation_plan、input_layer_receipts、sampling_kind）等于whole4，resumed完整selection log也等于whole4，累计selected=4；本docs对保存的三cycle JSON被动重算一致。显式feedback resume策略错配在输出创建前拒绝（main评估及保存stderr有据）。main checker先误把resume仅新增2行与whole4直接比较，报`AssertionError at assert rows(resumed)==rows(whole)`；改为原2＋新2后通过，**无产品改动／无CLI重跑**。错误原记录保留，不当产品bug或真实runtime replay；all planned／actual_run_rows=0。

**兼容性资格分列**：默认legacy／省略策略未改，fair仍显式opt-in。main记录原mutation RNG公式分支单独测试；[36 scheduler bytes记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/legacy-36-byte-comparison.json)记录36组旧／新scheduler states/events/contexts/resume及v1/v2 replay bytes离线一致。本docs核该JSON的36记录及声明，不运行公式分支或旧代码。**24-input legacy全路径capture parity仍未验证**，不能把公式分支／36 scheduler bytes／fair 2+2对照合并成该资格。

fair资格仅在固定corpus／RNG seed／selection history条件下；运行edge/coverage/oracle不驱动基础输入选择／sampling／变异RNG，feedback ledger与corpus retention准入仍保留，不能声称全campaign完全无反馈。不是BFS/DFS状态planner或自动状态发现，main接受milestone不等于用户批准默认更换。

[main probe artifact review](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/probe-artifact-review.json)接受non-DDS feasibility milestone，[bounded probe summary](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/probe-summary.json)原字节与其内嵌summary、summary SHA一致；[32项只读hash复核](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/probe-hash-check.json)复核全部32项artifact hash一致，只读现有结果、不复制整批或重跑kernel。该bounded probe已证outer UID1000/cap0的kernel认证、inner root＋net/PID/mount/proc证明与active PIDNS-scoped proc、内层严格完整inventory（controller PID1及detached sleeper PID9，零missing/unreadable）、detached child与两层树退出，以及host3＋nested3共6lease释放／owner bytes不变。lease身份依绑定namespace／packet proof，不靠裸PID1或collector pid_scope猜测。**host原scanner仍有不可读同UID进程、host_clearance=false**；这只是固定non-DDS controller/sleeper，不是真实ROS backend inventory、canonical 1W1R闭环、候选或成熟。旧EPERM／未入nested及旧cap-loss假设保留为此前版本，不由新probe改写旧失败或确认旧根因。

main已将Rawls／Sartre owner写集freeze并关闭；随后仅显式解冻Banach指定producer写集做canonical nested topology bridge。该bridge在本消息截面**尚未实现交付**，不是bounded probe已接入ROS路线；无DDS/live/commit GO。本sole docs沿用main请求gpt-6.1-sol、不推actual backend，不追随后续源码。 HEAD `42fcec65fe66d1dff8dd761bf03ee96a427277c7` dirty／source_frozen=false、candidate0、goal **active非complete**；原四required target/revision0／旧manifest／72分母不变，无新增target/rule/revision/算子或成熟计数；扩展scope独立义务不能用72代替。 337测试窗、CLI与probe各有版本／source身份，不能互相替代或迁移至新source；本docs只被动核JSON/hash/JUnit与追加，交freeze后停写。


<a id="s1-input-search-d028"></a>
## D028：有界input_search_bfs与novelty corpus分离（main工程决定；实施中）

来源：[main design-decision](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/docs-owner/snapshot/main/design-decision.json)及本卡main明确派遣；不是taskbook原话或用户原规格字面。固定cutoff=`2026-11-18T19:19:38.520960+08:00`，[cutoff／12件必要artifact SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/docs-owner/cutoff.json)；追加前本文件未占用D028。

- 模型有界`input_search_bfs`工作队列与novelty corpus分离；feedback只记账／辅助优先级，不再决定可搜索派生准入；同任务／生成历史下不能以feedback改变模型job选择。非novel有效child可在有界派生深度内继续搜索，no-change／duplicate需独立终态记账，不等于有效派生。
- 不等于ArmGo完整action/state planner，不推断SUT隐藏新状态。content语义投影v2与full replay identity分开，lineage外置、旧artifact不迁移；顺序、实际window及输入参数差异不能误合并。
- 生成no-op、有效输入重复、相对baseline的实际载体hit、独立观察的语义effect四者分开；no-op／overridden不给mutated hit，plan-only仍actual=false／execution=null。
- 决定性验收仍需同语义异seed／parent合一、不同顺序／window／参数区分、非novel派生、feedback不变性、checkpoint续跑等价及canonical真实入口且不弱化namespace qualification；当前仅离线局部证据，不预填实现完成。

HEAD=`979da1d4ba606e903f78fb44af8c34954e809b24`、behavior dirty／`source_frozen=false`；四required revision0／原manifest／72不变，candidate0、goal **active非complete**，无新live/commit GO。source状态来源为本卡main消息与artifact，不是docs重扫行为树。 详见[本卡接续核证](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-frontier-repair-fixed-cutoff)。
