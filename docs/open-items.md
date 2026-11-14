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
