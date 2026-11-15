# 待核实事项与证据更新

日期：2026-11-14。初始状态不假设阻塞。以下项目来自原规格审阅、主agent工程核对和派遣消息，尚未等同于本机运行结论。主agent要求继续全部 S1 scope；工程未知须经真实检查后更新，不能现在认定不可解。

记录规则：每次更新附检查命令或调用入口、时间、source/target revision、RMW、run_id（若有）、退出码/结果、raw path/hash。未运行写“未运行”，不能虚构证据。只有真实证据证明某条路径当前不满足条件时才记具名 `blocked:*`；保留失败尝试、可继续的路径及下一步。blocked 不能静默减少冻结成熟分母。

目标口径更新（2026-11-15T00:45:07+08:00，Asia/Shanghai；来源：本轮用户明确指令）：当前用户目标为“S1模块完全成熟且有2–3个真实SUT候选”。已记录的真实自然候选为0，因此候选数量目标尚未达成。自然数量无法预先保证，只能由真实运行产生；负控、工具错误和合成案例不得凑数。原taskbook允许zero-candidate推进Phase，不代表用户完整目标完成；可以交中间报告，但不能据此将整个goal标记complete。

## 本轮文档 freeze-ready 收束

记录时间：2026-11-15T01:09:04+08:00（Asia/Shanghai）。来源：main最新工程进展及独立测试报告，由本轮消息提供；文档子agent未复跑测试或运行ROS，未取得本条消息对应的新增产物路径，不补造路径/hash。

- third v4 两RMW runtime+ingest 已报告 judged/pass，但 `capsule_execution=null`，输入为 `reconstructed_input`；仅为development证据，不能写成真实capsule驱动或capsule执行链已贯通，更不能替代正式18+6/control/seal。下一步仍需真实capsule-driven neutral/fault执行及其独立raw、输入/执行身份、receipt和归因证据。
- main独立新增20项replay测试、31项third runtime/campaign测试通过；这些是离线测试结果，尚未取得真实replay证据。F07真实健康重放与可控工具失败重放仍待验收，原始run保持不变。
- Phase2 bridge正在新增；接口或合同实现不能替代真实trace、lifecycle及blackbox对照，Phase2→Phase4闭环仍有缺口。
- 当前用户2–3个真实SUT候选目标仍未达成：实测自然候选0，不以control、工具错误或开发pass充数；72格成熟分母不变，旧source seal仍只支持旧source。
- 本轮文档已整理为freeze-ready，含义仅为可交main审阅；不代表源码已冻结或S1已成熟。本次不暂存/commit。交付后停止文档轮询和自动追加，等待main下一里程碑明确派遣。

## 当前进展与最近待办

更新时间：2026-11-15T00:27:18+08:00（Asia/Shanghai）。逐项实际路径与缺口见 [完整S1验收账](s1-acceptance-ledger.md)。已验收基准HEAD为 `11ec76c249cb75605fd70c5d7efb5610e70514b1`；主agent已解冻third开发，本子agent仍仅改文档工作树，不暂存/commit。

第一target r1/domain180和second r2/domain80均在11ec76c上各18 required judged pass、6 negative judged fail、四control6/6、unknown0，独立verify rc0、seal=true。目录r1/r2不等于target revision，均为revision0。旧first unknown/seal=false及second domain181端口preflight阻断报告保持原样；见[正式证据快照](formal-target-snapshots-2026-11-15.md)。后续行为source改变后须最终source冻结再rerun，不能沿用上述通过为当前成熟声明。

F06按主agent裁定，真实zero-ledger/control排除/原始失败保留证明零候选路径执行；自然admission/triage未获真实异常证据。F07两次第一target共48份capsule的replay_attempts/execution_history/non_replayable均0；下一phase需真实健康及受控工具失败重放，原run独立保留。Phase4仍缺明确instrumentation linkage及bounded blackbox处置；当前ledger的instrumentation_candidates=null不能写成0。

D015第三target已有两RMW真实development runtime+ingest，D014第四target已有24行run；当前状态是development evidence / not mature，不再沿用旧`blocked:*`能力结论。third输入为`reconstructed_input`且`capsule_execution=null`，正式18+6/control/seal未验收；fourth为17 required pass+1 unknown、6 fault fail、seal=false，D014及正式18+6/control/seal未验收。失败与unknown保留，完整72 required分母不变。没有新增需要用户确认事项。下方历史待GO/待冻结等记录保留当时语境，以本节和证据账为准。

## 1. 工程未知

| ID | 事项、来源与现有证据边界 | 下一项可验证动作 / 所需证据 | 协调人 / 状态 |
| --- | --- | --- | --- |
| O001 | 第四target已有24行实际run；17 required pass、FastDDS `transparent.0` 1 unknown、6 fault fail，seal=false。unknown原始raw/hash保留；main重算指出graph快照时没有对应writer GUID（wire parse仅见reader SEDP），不据此判pass | 保留unknown及raw/pcap，修复或定位该writer SEDP缺失/解析覆盖问题；继续完成D014正式18+6/control/seal，不补配置字段，不减分母 | development/run evidence exists；not mature；主agent协调 |
| O002 | 主agent核对发现 Cyclone 旧 native probe 与本任务 no-native participant 约束冲突 | 核查旧 probe 是否创建 native participant、当前 ROS2 公共/endpoint-attached 只观察路径能否提供合同事实；保留 source identity、participant/endpoint provenance；不得把两个 graph 调用作为两个独立来源 | 主agent协调 runtime；待真实检查，不先验判定不可解 |
| O003 | third v4已有两RMW真实development运行：FastDDS drop 614包、Cyclone drop 162包，restore后seq 2–7均发收；但ingest为`reconstructed_input`、`capsule_execution=null`，仅开发证据 | 保留本次raw/hash；仍需capsule-driven neutral/fault、真实D015和完整18+6/control/seal证据。non-DDS控制及该开发run均不能替代正式验收 | development evidence exists；not mature；正式D015验收待主agent安排 |
| O004 | 原 Phase 2 要内部事件/源码插装，Phase 4 要有界发现性验证；主agent当前执行声明限正常公共 API 验证和证据工具 | 主agent逐项声明当前实现内容与原要求的差距；核查能否在当前边界内达到必要观察；不能用 graph 观察冒充内部插装或自动把 bounded 正常验证称为原 fuzz maturity | 主agent；待工程裁定和证据 |
| O005 | 第一target独立六条negative已真实完成并按预期primary fail；其余target的fault contract仍须分别审查 | 保留D009闭合证据、D012 raw-backed fault receipt和harness归因；后续target明确施加层与独立primary oracle，不能冒充SUT故障 | 第一份正式报告fault6/6；不等于整体seal通过 |
| O006 | first/second在11ec76c上独立per-target seal通过；third开发已由主agent解冻 | 不改已seal revision0 target manifest；core精确legacy兼容，新target声明receipt_kind/window/projection schema；后续source变化须最终source rerun | 四方独立写集开发；文档不commit，旧seal只支持旧source |
| O007 | 四 target 的双 RMW 表达性、环境可用性和 applicability 不能靠安装清单推定 | 两 RMW 分别检查真实 endpoint creation、rule applicability 与 carrier receipt；不可用具名记录并保留原 18 格预期缺口，不删分母 | 主agent协调 runtime；待真实检查 |
| O014 | runtime核实本机 Jazzy MessageInfo 无 publisher_gid，direct per-sample GID 未达成；smoke 使用 endpoint_correlated | 保留 unavailable_api/null 与关联依据，按 target/rule 裁定 identity/generation 证据充分性；不能判 SUT mismatch，也不能伪称直接 GID | 主agent协调 runtime/campaign；观察限制已确认，验收影响待按规则核查 |
| O008 | registry/domain/state-dir 三锁与旧仓库 owner 排除、derived port probe 还未由文档审计 | 使用真实 owner payload、端口 bind/probe、活动进程的 domain/run/state/source 身份交叉检查；不确定必须 domain_conflict/abort，记录失败而非绕过 | 主agent协调 runtime；待真实检查；本子agent不启动 ROS |
| O009 | D002 neutral 阶段分支、D003 输入/执行 receipt 分离、D004 residual 优先及计数复用已裁定，尚需统一接口 | core/campaign 合同覆盖 neutral 不伪 hit、mutated 无 hit 不能越关、allowed+residual 不短路、同输入换域/换 campaign hash 相等、control 引用可追踪且不重复计数 | core/campaign；已裁定、待实现验证 |
| O010 | 第二/三/四 target 的 primary rule、expected effect、observation/control contract 和 window 参数不能只替换 target 名 | 核对每 target 语义、message/QoS、post-match/recovery 窗口、身份/代际、两个独立源等字段，提供合同测试与真实 receipts 映射 | 主agent协调 campaign；待逐 target 核查 |
| O011 | 主agent已裁定F06零自然候选路径可用真实zero-ledger/control排除/原始失败保留证明执行 | E01/E03证据见完整验收账；自然admission/triage仍缺真实异常，不用control/合成数据凑候选 | 零分支执行已有证据；自然候选分支不预先保证 |
| O012 | 原规格没有正式定义候选去重键、状态更新审计格式和证据保留策略 | 固定 stable identity、更新历史、重复 raw 的归并规则；保留 rejected/deferred/replay failure，不能靠去重或裁剪抹掉责任证据 | 主agent协调 core/campaign；待接口审查 |
| O013 | first与second r2/domain80正式report/独立verify均rc0；旧first及second domain181 rc1保留 | 验收账逐项保留Phase2/4/F07缺口；两个target通过不等于完整S1或最终新source通过 | 11ec76c版本两target已封存，后续source rerun仍必要 |
| O015 | D013 ACK补丁已提交，当前第一target rerun18 pass+6预期fail、四control6/6、unknown0、独立seal=true | 首次FastDDS transparent.1工具unknown/raw保持immutable；后续代码变化须按新source身份重跑 | 本次竞态纠错已有当前正式证据，旧失败不删除 |
| O016 | D014字段合同已固定，且第四target已有24行run：17 required pass、1 unknown、6 fault fail；seal=false。FastDDS `transparent.0` unknown及原始证据保留，history/depth仍为`not_applicable_by_endpoint_schema` | 针对缺失writer GUID/unknown继续核查独立wire与graph证据；协议默认值必须保留精确来源，不能配置补值；完成正式18+6/control/seal | 已有run evidence；not mature；D014正式验收未完成 |
| O017 | F07两次第一target48份capsule的replay_attempts/execution_history/non_replayable均0；首跑失败不是replay failure | 下一phase真实健康重放＋可控工具失败真实重放，独立产物、attempt/failure与原输入关联，不改原run | 主agent已裁定后续验收要求；尚缺真实replay证据 |
| O018 | Phase2内部事件开发证据存在，但当前ledger instrumentation_candidates=null/status=not_run/linkage=not_linked；Phase4未闭环 | 无候选时明确Phase2结论、执行具名bounded blackbox并记录instrumentation_candidates=0及完整disposition；不能拿required矩阵自动替代 | 主agent工程协调，无需用户确认 |
| O019 | third v4已观察到真实pair drop计数（FastDDS 614、Cyclone 162）及restore后seq 2–7交付；这是development run，输入重建且capsule execution为空 | 完整验证D015 capsule-driven neutral/fault、drop前置/恢复活动和fresh双向快照、锁至netns子树清理，并完成18+6/control/seal；失败证据保留 | development evidence exists；not mature；scope/72分母不变 |
| O020 | second r1/domain181 runtime port preflight阻断，adapter另有step-count错误；r2/domain80已18+6和四control通过 | domain181历史及raw保持，不推断该域已无冲突；不把工具ingestion错误当已真实执行后的语义不兼容 | 采用新域的正式尝试通过；阻断历史不删，最终source仍须rerun |
| O021 | main发现domains.scan_processes用旧repo substring误判新v2绝对脚本路径共享前缀；runtime正修真实路径边界 | 覆盖旧repo本身/真实子路径、v2相邻前缀、argv与cwd边界及保守owner检查；不放宽旧repo只读/锁保护，不将此误判直接归为domain181端口冲突根因 | runtime负责修复/回归；文档只读见边界判断代码，未验证完成 |

D008 的环境检查仅为主agent报告：uid1000、CapEff=0；`unshare --user --map-root-user --net sh -c 'ip link set lo up && ip -brief link show lo'` 返回 rc0、lo UP。未提供 raw 路径/hash，不计 ROS evidence。网络隔离载体与 vendor内部观察已有专属agent，本页不将“它们尚在实现”重复列作新增验收缺口。

D010 已裁定 chronology：late_joiner 使用 writer-only 前置，reader/match/graph 拆为 observation_receipts，mutation_begin 与前置 snapshot 必须先于 delay/create reader；drop target 则保持 matched-before-drop。此项已有主agent安排实现，本轮只读审阅不将“尚在拆分”重复计入新增缺口，后续验收须按真实 raw 时间顺序复核。

D011 验证更新见 [验证快照](verification-snapshot-2026-11-14.md)：离线112 tests/4.232s/OK；non-DDS控制33 packets drop、70/70恢复（非ROS/S1）；两RMW健康/negative smoke。观察限制：本机 Jazzy MessageInfo 无 publisher_gid，当前 endpoint_correlated，不是 direct per-sample GID，不判 SUT mismatch。网络JSON现已归档至results/network-control/2026-11-14-carrier-control/raw.json，文档只读核对与/tmp源逐字节一致；不入首次提交。

## 2. 主agent待解释的其余规格差异

[验收矩阵 A05–A12](acceptance.md) 记录全部具体差异。当前需要明确：Phase 5/6/7 的 per-target 排程；执行失败审计与 triage 的入口边界；seal 对每类 candidate 的 replay/non_replayable 终态要求；expected_effect_set 对象/数组 schema；primary-only violation 的判定分支；canonical serialization；revision=0 已 seal 后如何修改；zero-candidate 的 F06 集成证明。

这些事项默认由主agent处理。原文中的交叉引用笔误不构成新状态或新增功能。未裁定不自动变成“用户必须确认”。

## 3. 本次只读验收新增发现

详见 [只读审阅记录](read-only-review-2026-11-14.md)，仅三个具体 gate 缺口：R001 空成员 endpoint identity 可 admitted（内存合同反例已复现）；R002 独立离线 seal 审计缺少当前完整 source identity 比较（静态）；R003 candidate ledger 与已验证 cell 的 evidence/lifecycle 未重新绑定（静态）。均针对已记录源码快照，不作为最终验收失败结论。首次冻结前追加复核已见R002当前source比较和R003候选投影重建路径补入；最终合同/真实验收待主agent汇总，R001未因其余修复自动关闭。D010、网络载体、vendor内部观察和已补健康 gate 不重复列为新增缺口。

## 4. 需要用户决定的事项

当前无已经证实必须交由用户处理的新增事项。若主agent检查后发现范围、输入约束、native 禁令或成熟分母必须变更，只登记请求的具体变化、触发证据、对既有 evidence/seal 的影响和主agent建议；文档子agent不直接向用户发问，不擅自放宽合同。

完整 scope 的推进已由主agent派遣，不反复询问是否继续。工程未知保留原目标，不能在没有证据时把它标成不可能。

## 5. 不可先验保证的目标

- 自然 SUT candidate 数量、2–3 个真实候选、confirmed 数量、replay 必然成功，都不能在执行前保证。零候选须交执行漏斗与证据，不是“无缺陷”证明。
- 原 S1 全部成熟不能由文档、注册四 target、离线测试数、单个 pair、24 条 control 资格或第一 slice 完成推出。
- builtin 两源对账、两个 RMW 的真实 discovery carrier、插装/black-box 证据隔离、applicable unknown=0，只能用相应真实检查证明。
- 正常公共 API 验证不能无条件等价为原规格全部 fuzz/发现性要求。报告须列明差距，保留未满足项。
- git init 不等于已有 HEAD；输入 capsule 不等于真实执行；两个 RMW 分别跑通不自动等于混合 vendor pair 互操作成功。

## 6. 本轮文档检查记录

已完整阅读 `/tmp/ros2-dds-fuzz-v2-s1-handoff.md` 和其中四份旧仓库参考。source hashes 见 [决策记录](decisions.md)，原任务书副本的源 hash 见 [原文存档](taskbook.md)。本子agent仅写 `docs/**`、`README.md`、`AGENTS.md`，另按主agent派遣初始化新仓库 Git；不写业务代码、不启动 ROS、不启动 campaign、不再派遣下级agent。主agent后续已派遣本子agent执行首次提交，但必须等最终GO；截至本次更新未暂存/commit。

已执行文档校验：原任务书副本与源文件逐字节一致（SHA-256 `2b476e5903d39f6665b1055cebc01556bd2336f786d4c8089ef8446a21afecc2`）；17 个本地 Markdown 链接有效；八项能力及 receipt/hash/replay/provenance/seal 核心字段检查通过。新仓库 `git rev-parse --verify HEAD` 显示尚无 commit。未运行安装、业务测试、ROS 或 campaign。此页不代表全仓测试结果；代码agent仍在并行工作。

追加检查：D008/D009/D010、当前6名agent记录及三项只读发现校验通过；审阅记录所列五份源码 digest 在最终文档校验时仍一致。仅执行不落盘的内存合同探针，未改业务代码或运行证据，未启动 ROS/campaign。R001 已复现，R002/R003 为静态发现，不等于完整 seal 的端到端失败。

最新文档验证：新增D011与开发验证快照后，本地Markdown链接共21个有效；原任务书逐字节未变。文档子agent未复跑主agent的112项测试，仅核对网络JSON及六份smoke的raw SHA-256。

首次冻结安排：主agent指定Codex Agent <codex-agent@localhost>，仅commit命令的-c身份，不改Git用户配置。results保留可见untracked，显式staging排除，不使用git add -A。source_frozen最终检查待runner ready；首次提交与冻结成功状态均待实际命令结果，不提前登记。

D012更新：negative+fault的独立审计路径已由主agent裁定；故意omit中间delivery不是late_joiner mutation，不伪hit。core/adapter正小修，必须核fault_control_receipt的raw_event_ref、施加时序、harness归因及candidate_eligible=false，admission无条件排除negative。此为既有scope/分母内解释，不重复列为新增缺口。首次提交继续等最终GO。

首次 matrix 的 HEAD 约定（主agent工程派遣补充）：最终 GO 仍未收到，当前不暂存/提交。首次提交后不因文档更新立即第二次 commit；正式 matrix 期间文档可工作树更新，HEAD 保持不变。后续 phase 实现统一新提交并重跑相关矩阵，旧 evidence 保留原身份。该约定记录于 D011 和 AGENTS，不代表当前两项接口小修已经完成冻结。

最终 GO 更新：主agent独立验收报告全仓 131 tests / 13.485s / OK、negative+identity 集成通过，所有代码agent明确 freeze；前述“待 GO/正小修”属于当时快照。首次提交由文档子agent按已授权路径执行；实际 HEAD、提交状态和 source_frozen 以 git log、只读 source_manifest 检查及运行 manifest 为准。历史 112 项及审阅反例保留，不能用离线测试结果代替真实矩阵。首次正式 matrix 期间不追加文档 commit。

第二次提交准备（2026-11-15T00:01:03+08:00（Asia/Shanghai，TZ=Asia/Shanghai date --iso-8601=seconds实测））：主agent说明行为源码修复仅为match snapshots/runtime adapter及对应回归tests；D014目前只记录待实施，不修改第四target实现。等待代码agent明确freeze及全仓测试后的新GO，届时才执行本次单独授权的自动化身份commit；本轮不暂存/提交，历史first run的UTC目录不重命名。

第二次提交最终GO（2026-11-15T00:14:58+08:00，Asia/Shanghai）：主agent独立全仓144 tests / 37.032s / OK，diff --check与ACK逻辑审查通过；本次单独授权已收到。ACK前不得打开post_match_delivery窗口，验收细节见D013/S03c。显式staging仅五个已审查runtime/adapter/回归测试文件及当前docs/README/AGENTS；results排除。D014仍待实施。实际提交及冻结状态以git log/source_manifest为准；此更新覆盖前文待GO状态，不改变首份matrix失败记录。

正式证据更新（2026-11-15T00:32:53+08:00，Asia/Shanghai）：second r2/domain80通过已核对；first/second精确target hash及legacy fault字段保存于正式快照。主agent已解冻第三target开发，首次seal前第三/第四初版合同修改不能写成new rule完成；当前无用户确认事项。

最新开发证据（2026-11-15T00:38:53+08:00，Asia/Shanghai）：main逐字段核对first/second target.to_dict()与11ec76c正式manifest均true，但source已dirty，最终source rerun不豁免。被动SEDP attempt2两RMW netns status=ok、events56/48/errors=[]已只读核对；default policy pending、development_only，不能关闭第四target成熟缺口或新增SUTcandidate，详见完整验收账E09。


## 本轮冻结前待核实项补充（2026-11-15T08:42:44+08:00，Asia/Shanghai）

来源：主agent本轮明确派遣与工程报告；本节追加当前快照，不覆盖旧失败、unknown、seal 或历史待办，也不将工程解释冒称原任务书原话。

| 待核实项 | 当前已知状态 | 关闭所需证据 / owner |
| --- | --- | --- |
| 静态 scope 与 per-run maturity 分离 | scope 已从 run 动态状态改为静态 capability；成熟状态按 per-run report 与独立 target audit 判断 | 最终冻结源码上的正式报告及独立 seal；静态 capability 不替代执行证据，分母仍为 72 |
| 非标准端口 potential discovery 留证 | 主agent报告 capture 已保留相关 raw，普通 user-only DATA 已安全分类；此前仅 metadata 的保守 unknown 不升级 | 新 source 上真实 capture/identity/解析与第三/第四 target 证据；普通 user DATA 不算 discovery counter hit，raw 留存本身不证明成熟 |
| execution_input / Cyclone / FastDDS blackbox | 绑定与真实 blackbox 兼容修复进行中 | Ptolemy 最终 freeze、maps 多 segment 与污染 trace 回归的确切结果；目前不标通过 |
| 最终源码冻结与测试 | 当前 source 未 freeze；本次观察 HEAD 为 `e5a1a73921b176e60bba14cb41b9ed7f18c0a538` 且行为源码 dirty | 等代码 owner 最终 freeze、主agent独立审查及唯一新 HEAD/behavior SHA-256；文档稍后按实际结果更新，运行仍需明确 GO |
| 新 source 真实证据 | 正式四 target、F07、Phase2–4 均未在新 source 验收；Zeno 已获派准备 26-pair 路径，不等于执行授权或执行完成 | 单一 owner 按派遣运行，保存 input/execution/raw/replay/provenance、独立 verify 与 cleanup；任何失败原样保留 |

主agent当前报告 capture/projection/recovery 的 89 tests / 5.557s / OK；此前完整 334 tests / 48.376s / OK 来自更早工作树，不能作为最终修复通过证明。本执行员未复跑这些业务测试，未取得本条报告对应的完整命令、测试日志路径或 raw，相关字段保持未提供，不补造。待 Ptolemy 最终 freeze 后再追加实际结果；本次不先行关闭待办。

自然 SUT candidate 仍为 0，用户 2–3 个真实候选目标未达成；72 required 分母不变。旧 source 的 seal 仅支持旧 source，历史 unknown/失败不删除或改判。当前无新增需用户决定的范围变化；工程协调由主agent处理，本节不是运行、staging 或 commit 授权。


### 最终离线审查已完成；正式 source freeze 与运行仍待 GO（2026-11-15T08:49:44+08:00，Asia/Shanghai）

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
