# 待核实事项与证据更新

当前逐项验收入口（2026-11-15）：见[c884437完整验收表](s1-final-acceptance-2026-11-15.md)。HEAD `c884437be86c2082d449bafade8185c132138955` / behavior `ceaee506bc189a6de9764bbcdd64c6c6753ed782a92c550e40d86b98b6e39205`：统一provider/carrier/fact/oracle已真实接线；新formal四seal、72required pass+24negative预期fail、unknown0/192worker rc0；新F07健康与受控失败、Phase2→4真实zero分支已执行。QoS72原辅助rc1/71of72保留，main已按独立旁证接受72既有观察链。seconds-edge三配置另有分层72格/144不同worker实证：首24原rc1保留并获独立合同旁证，后48新执行及独审通过，不称原首attempt整体通过。自然/instrumentation/confirmed仍0，非空生命周期及2–3自然候选目标未达，不标完整S1/goalcomplete，不进入S2。147及更早source均为历史，seal不迁移；主agent公共API执行边界不是原任务书原话。

main接续历史说明（以下保留main原文；403与“最终逐项验收尚未完成”均为其当时记录，以本页当前入口更新后续状态）：

> 当前接续状态（2026-11-15）：见[主agent接续审查与执行故障记录](main-supervision-2026-11-15.md)。147af87正式四target各seal=true（72 required pass、24预期negative fail、unknown0）；F07及Phase2→4真实zero-candidate分支已有证据，旧文中的待验收描述保留为历史。自然/confirmed候选仍0，最终逐项验收尚未完成；指定6.1-sol子agent服务返回403余额不足，具体接续工作与需用户后续处理事项已记录。

日期：2026-11-14。初始状态不假设阻塞。以下项目来自原规格审阅、主agent工程核对和派遣消息，尚未等同于本机运行结论。主agent要求继续全部 S1 scope；工程未知须经真实检查后更新，不能现在认定不可解。

记录规则：每次更新附检查命令或调用入口、时间、source/target revision、RMW、run_id（若有）、退出码/结果、raw path/hash。未运行写“未运行”，不能虚构证据。只有真实证据证明某条路径当前不满足条件时才记具名 `blocked:*`；保留失败尝试、可继续的路径及下一步。blocked 不能静默减少冻结成熟分母。

目标口径更新（2026-11-15T00:45:07+08:00，Asia/Shanghai；来源：本轮用户明确指令）：当前用户目标为“S1模块完全成熟且有2–3个真实SUT候选”。已记录的真实自然候选为0，因此候选数量目标尚未达成。自然数量无法预先保证，只能由真实运行产生；负控、工具错误和合成案例不得凑数。原taskbook允许zero-candidate推进Phase，不代表用户完整目标完成；可以交中间报告，但不能据此将整个goal标记complete。

## 2026-11-16 当前待决与未完成（e448feb）

本节是当前待决/未完成入口；其后的“当前缺口与主决策（c884437）”及更早O项保留为历史，旧source的formal/F07/Phase24/seal不迁移。当前source为 HEAD `e448febec4fa9f3c24f22eaac7954517e7b4a301` / behavior `e7f92897dd6d122a874f7686a24419173d110abd765106ce5e16e1289e732cfa`，`source_frozen=true`、`dirty_behavior_paths=[]`。修复提交只改`src/r2f/runtime/namespace_pair.py`及两项测试，`activity_limit=32768`，全局默认仍4096，严格retained/adapter/hit门未放宽。

当前source已核收的证据（详见[e448feb验收段](s1-acceptance-a3b1941-2026-11-15.md#2026-11-16-e448feb终态四targetf07与phase24-zero分支已核收-候选仍0)）：

- 正式四target [batch-20261115T165015Z-45cc1bed](/home/wuyuqa/ros2-dds-fuzz-v2/results/final-validation/batch-20261115T165015Z-45cc1bed/summary.json) summary SHA `9869601920d99e5f9d80d483cbabaa0117d1dd8e60fb4f395132ecf447b31146`：固定顺序、每target24cells/48workers、domain111；96 actual judged=72 normal pass＋24 negative预期fail，unknown0，四controls各6/6，四seal=true，四cleanup ok/`owned_live_after=[]`；96唯一raw/capsule/effective input、192唯一worker PID rc0并全部raw `process_exit`绑定；48条namespace controller退出非worker。正式第三target为`drop_duration_sec=3.0`、trajectory全`no_loss_at_sampled_points`，不得写成long25/observed_loss_then_recovery。
- 单独e448feb long25 [execution](/home/wuyuqa/ros2-dds-fuzz-v2/results/final-validation/recovery-long-interruption-e448feb-20261115T160916Z-8cc7731d/execution-20261115T161302Z-011ed32b)：24/24 judged、Fast/Cyclone mutated `observed_loss_then_recovery`、Fast activity32768/126 retained/0 eviction、seal true；外层cleanup误报PID2995803为Codex app-server瞬时子进程，非本次worker，原`summary.passed=false`保留不改。只作长期失配覆盖，不并入正式batch，不证明lease expiry。
- e448feb全离线[full log](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/e448feb-full-20261115T164801Z-d1fd8e3f/full-unittest.log) SHA `dbc09ea35fce0c187255fe5ec9fd3429fab2fa194a341d89369380c212328cf4`：457 tests、OK(skipped=1)、rc0；skip为既有`R2F_PROVIDER_AUDIT_REPORT` fixture，不得写成456/0skip。
- e448feb [F07 execution](/home/wuyuqa/ros2-dds-fuzz-v2/results/final-validation/f07-replay-preparation-20261114T223955Z-f04b2d7f/execution-20261115T171147Z-70f8a013) summary SHA `d3ed65890836eb0d58cd78d1d1a23150d06c42e16023e915de333d2fb32dbf11`、driver rc0：healthy domain112 pass/real ROS/verify valid；tool_failure domain113 `fail_environment/runtime_tool_failure/aborted/real_ros_execution=false`，writer PID3071543 rc-9，intent/sent harness `candidate_eligible=false`；原340文件/capsule/report/source/CLI不变、cleanup ok。不是候选，分母增量0。
- e448feb [Phase24 execution](/home/wuyuqa/ros2-dds-fuzz-v2/results/final-validation/phase24-e448feb-20261115T171430Z-80d06a02/execution-e448feb-p24-20261115T1730Z) summary SHA `be4e3406e66dcdb5789893b309e91bfc424b56209f73f8dbd0da2b477279d470`、driver rc0、`completed_zero_candidate_branch`：instrumentation domain115 intake/candidate0/lifecycle completed；blackbox domain116 observed/errors[]/same-input executed；bounded domain117 24cells/48workers 18pass+6neg/unknown0/four controls6/6/seal true/report SHA`dd68e29e39265b16e38c6ac0f3847f0d0d096175cc57a00d24d96adf8c3c0f5b`；三stage audit ok/candidate_hold=false/cleanup ok/52 workers退出。只证明zero分支。
- 独立终审Ramanujan只读复核三块证据均acceptable，未发现hash/identity/矩阵/worker退出/cleanup矛盾；并纠正正式第三target为3s、F07受控失败非候选、control/expected effect不入账。

**当前未完成/未实测：**

1. 用户目标“S1模块完全成熟且有2–3个真实SUT候选”未达成。当前natural candidate=0、confirmed=0，四target及Phase24 ledgers的candidates/failures均空；成熟分母仍72，不改变。
2. 非空candidate admission、triage、confirmation、逐候选replay或`non_replayable`处置尚未有真实对象；候选级两RMW×3 repeat validation未发生。
3. 当前source四target per-target seal＋F07＋Phase24 zero-branch证据完整，但这不等于完整用户目标；S2未进入，不标goal complete。
4. 正式第三target的3s矩阵与单独long25不可混写；long25的drop 1→0匹配下降和恢复是allowed expected effect，不是候选，不能声称vendor内部lease expiry。
5. 旧c884437/a3b1941/147af87及更早source的formal、F07、Phase24、long25和seal只历史，不迁移到e448feb。

**需main后续决定（不是用户已授权）：**

- 路径A：现有scope内有界压力实验，例如recovery `post_match_count=64`或更高交付义务，单次最多24cells/48workers；仍可能继续0候选，不能保证数量。
- 路径B：扩展scope（mixed-vendor pair、endpoint remove/recreate、lease expiry、discovery delay/reorder/duplicate等）。该路径需owner escalation和用户确认；可能新增target/rule/mutation/carrier、改变run identity，乃至72成熟分母，旧seal失效。
- 不能为达到2–3候选而使用负控、unknown、工具错误、expected drop effect或控制故障凑数；若仍0，应如实保留zero-candidate分支。

**工具问题与执行约束：**

- long25 cleanup的`run_processes` cmdline子串误报应作为独立工具问题诊断/修复，不重跑已封存campaign，不改原cleanup/summary或把`passed=false`改成true。
- 若修改helper/脚本，须生成新helper SHA并有main新GO；旧GO/旧helper不得复用为新source执行授权。
- 新的真实ROS运行仍需唯一owner、串行窗口、domain/preflight/source/target/revision/run_id绑定；不自动重试，不并发。

## 当前缺口与主决策（c884437）

当前实证见[逐项验收表](s1-final-acceptance-2026-11-15.md)。旧O项/失败/服务故障保持历史；本表不发执行GO。

| 项目 | 已确认事实 | 具体主决策/剩余 |
|---|---|---|
| M01统一接口 | [真实调用链](s1-final-acceptance-2026-11-15.md#interfaces)已实现并接入campaign/replay/audit，新sourceformal/F07/Phase24已执行 | 原147明确实现缺口关闭，不回改147旧验收；不因全仓测试数宣称任意provider均完成 |
| M02非空候选/完整成熟 | 自然/instrumentation/confirmed0，真实zero分支/control排除有证 | 原目标内非空admission/triage/确认与2–3数量目标未达，main决定后续有界路径，不能凑数/goalcomplete |
| M03内部观察/build边界 | [新Phase2](s1-final-acceptance-2026-11-15.md#modes)installed vendor内部事件有真实provenance；source build条件路线未选 | 若另要求源码插装，须独立build/binary/compile/patch/symbol/对照；不是本轮已完成项，也不把允许内部事件路线误列实现缺口 |
| M04 QoS辅助失败与旁证 | 原controller rc1/aux71of72/incomplete保留；main按[Popper旁证](../results/main-review/qos72-cutoff-adjudication-20261115T093129Z-ebc642cf/freeze.json)接受既有72观察链 | 仅额外close_unread0误报；不写原run controller0、不重跑掩盖，不计SUT候选/分母 |
| M05非空validation/S2/seconds-edge | 原候选级两RMW×3repeat validation、异常replay归因与确认无对象；S2不进 | [seconds-edge](s1-final-acceptance-2026-11-15.md#seconds-edge)已分层实证72格/144不同worker，原首24 rc1不改；后续仅只读QoS覆盖研究、最多2个未覆盖正常组合建议，尚无新计划/ROS授权，不预报候选 |

source为c884437/ceaee506、正式分母72；主agent公共API执行边界并非原规格原话。旧147四seal、F07/Phase24/supplement仅历史支持，各原件保留。main监督文件及其历史原文未改。

## 历史文档 freeze-ready 收束（当时状态）

记录时间：2026-11-15T01:09:04+08:00（Asia/Shanghai）。来源：main最新工程进展及独立测试报告，由本轮消息提供；文档子agent未复跑测试或运行ROS，未取得本条消息对应的新增产物路径，不补造路径/hash。

- third v4 两RMW runtime+ingest 已报告 judged/pass，但 `capsule_execution=null`，输入为 `reconstructed_input`；仅为development证据，不能写成真实capsule驱动或capsule执行链已贯通，更不能替代正式18+6/control/seal。下一步仍需真实capsule-driven neutral/fault执行及其独立raw、输入/执行身份、receipt和归因证据。
- main独立新增20项replay测试、31项third runtime/campaign测试通过；这些是离线测试结果，尚未取得真实replay证据。F07真实健康重放与可控工具失败重放仍待验收，原始run保持不变。
- Phase2 bridge正在新增；接口或合同实现不能替代真实trace、lifecycle及blackbox对照，Phase2→Phase4闭环仍有缺口。
- 当前用户2–3个真实SUT候选目标仍未达成：实测自然候选0，不以control、工具错误或开发pass充数；72格成熟分母不变，旧source seal仍只支持旧source。
- 本轮文档已整理为freeze-ready，含义仅为可交main审阅；不代表源码已冻结或S1已成熟。本次不暂存/commit。交付后停止文档轮询和自动追加，等待main下一里程碑明确派遣。

## 历史进展与当时待办

更新时间：2026-11-15T00:27:18+08:00（Asia/Shanghai）。逐项实际路径与缺口见 [完整S1验收账](s1-acceptance-ledger.md)。历史已验收基准HEAD为 `11ec76c249cb75605fd70c5d7efb5610e70514b1`；主agent已解冻third开发，本子agent仍仅改文档工作树，不暂存/commit。

第一target r1/domain180和second r2/domain80均在11ec76c上各18 required judged pass、6 negative judged fail、四control6/6、unknown0，独立verify rc0、seal=true。目录r1/r2不等于target revision，均为revision0。旧first unknown/seal=false及second domain181端口preflight阻断报告保持原样；见[正式证据快照](formal-target-snapshots-2026-11-15.md)。后续行为source改变后须最终source冻结再rerun，不能沿用上述通过为当前成熟声明。

F06按主agent裁定，真实zero-ledger/control排除/原始失败保留证明零候选路径执行；自然admission/triage未获真实异常证据。F07两次第一target共48份capsule的replay_attempts/execution_history/non_replayable均0；下一phase需真实健康及受控工具失败重放，原run独立保留。Phase4仍缺明确instrumentation linkage及bounded blackbox处置；当前ledger的instrumentation_candidates=null不能写成0。

D015第三target已有两RMW真实development runtime+ingest，D014第四target已有24行run；当前状态是development evidence / not mature，不再沿用旧`blocked:*`能力结论。third输入为`reconstructed_input`且`capsule_execution=null`，正式18+6/control/seal未验收；fourth为17 required pass+1 unknown、6 fault fail、seal=false，D014及正式18+6/control/seal未验收。失败与unknown保留，完整72 required分母不变。没有新增需要用户确认事项。下方历史待GO/待冻结等记录保留当时语境，以本节和证据账为准。

## 1. 历史工程未知（当时记录，非当前待办）

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

## F3 触发条件：recovery 窗口内无 discovery 流量（2026-11-17）

- 当前 E01 单窗真实运行（source `4d63912`、domain 172、3s window）中，nft counter packets=0，window hit=false，生命周期为 `mutation_ineffective`，不是 SUT 缺陷。
- 原因：稳定匹配后 writer/reader 不再发 SEDP，仅 drop 不足以产生 discovery 包。需要窗口内创建新 reader/participant 或重建 endpoint，才能形成真实命中。
- 该能力属于 F3（多 endpoint/生命周期交错），不在当前 F1/F2 admitted 范围。未实现前不得把 E01 标为真实 hit，也不得靠延长窗口伪造。
- 另：`mutation_ineffective` 已禁止进入 corpus 覆盖与自然候选；该行为已验证。

## F3 reader 重建身份合同（2026-11-17）

- 已在 `7d26e3c` 加入受控 reader recreation scaffold（`reader_recreate_in_window`，默认 false）以及 `reader2` worker 别名。
- 真实实验证明：在 drop 窗口内启动第二 reader 可让 nft discovery counter 命中 true，恢复后主 pair activity 也可观察到；但当前 pre-drop binding 与 precondition snapshot 仍只描述初始 writer/reader 代际，直接替换 active reader 会触发 `discovery binding worker identity differs` / identity mismatch。
- 因此 recovery_adapter 现在显式拒绝混合代际：`reader recreation requires F3 generation contract`，不把新 reader 静默当成旧 reader。
- 需要 F3 设计：endpoint generation 合同、旧/新 GID 与 PID 的显式映射、pre-drop binding 与 post-restore active pair 的分离、逐代际 raw receipt。完成前不得开启该路径，也不得把 counter 命中当作 SUT 候选。


## 2026-11-17 主审计更正与修复待验（本轮当前入口）

本节追加，不覆盖前文历史；本轮事实与逐项文件引用以[主审计报告](/home/wuyuqa/ros2-dds-fuzz-v2/docs/main-audit-2026-11-17.md) S0–S3、MA01–MA08为准。审计HEAD `42fcec65fe66d1dff8dd761bf03ee96a427277c7` / behavior `f8533be090ecf9ff8a8921dc6b1f28f7ba608f5f5c0549c8628c97b58ed88230` 是审计时点快照；42fcec65是f44f83b后继，不称HEAD矛盾，不把当时source_frozen=true外推到并发修复工作树。

| 本轮待办 | 已知证据／下一验收点 |
|---|---|
| 输入身份（MA01） | 主agent离线6组行为config不同却同input hash、binding均preserved，均planned；修复后需区别行为输入或明确拒绝，完整绑定后再验真实消费。不是SUT候选。 |
| exploratory replay（MA02） | 同source capsule仍被_check_source报`target_contract_mismatch`，属preflight/fail_artifact，未发生真实ROS重放；需版本化registry和保存输入重放，不能删source/target校验。 |
| 生命周期/组合（MA03） | netem-multi-1两行各仅2 workers，无churn/recreate/burst；配置声明不证明组合执行。需各层raw/identity/时序/命中，未支持则显式拒绝。 |
| 真多窗core/provider（MA04） | multiwindow-6实际3窗但E01 plan1窗；旧core对真实>1窗还无条件拒绝。Kepler第4代码写集修门槛，必须真实2/3窗计划贯穿hash/capsule/runtime，不能假单窗绕过；不能计E02/E03闭环已成。 |
| real source gate（MA05） | runner需在真实runtime前强制source_frozen/HEAD/行为clean检查。mixed-vendor-4历史source_frozen=false只能留开发历史，不迁移冻结验收。具体runner写集归属由主agent协调。 |
| Fast0原因（MA06） | extreme-4 Fast facts仅data_plane_observed=false，`possibly_shm_or_non_udp_transport`是猜测；根因未知，待独立传输路径/计数证据，不计命中。 |
| required回归（MA07） | [baseline-required-contracts.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/identity-replay-audit-20261117TUvZvdI/baseline-required-contracts.json)已存四完整manifest/hash；required catalog=`947757406e7a23662b576b5cf5ab615773f36a83ccdff9a1e19579fe569e4eb6`，修复后逐字回归，分母72；旧source验收/seal不迁移。 |

模型：原deepseek spawn确报Unknown model，S0中主agent转述用户后续明确改`gpt-6.1-sol`，不再是阻塞；旧报告`replacement_used=false`保留其审计时点。主agent当前派遣为4代码（输入身份/replay/生命周期/Kepler多窗）+本文档共5名，来源为主agent在本spawn子会话中的派遣/补充消息，不是终端人类用户原话；用户模型选择由主agent转述，人数、写集与工程安排不称“用户派遣事实”，也不冒称本writer已核对全部spawn日志或代码完成。此记录更正旧计划的deepseek成功派工及旧阶段人数，不改原日志。（来源：主审计§1/S0–S1。）

主agent已执行`84 passed in 25.90s`仅记定向离线检查；文档writer未复跑测试、ROS/campaign/replay，未commit。没有新增需用户改变范围/分母的决定；工程修复由主agent继续协调。自然候选0、整体目标active、72 required分母不变，原acceptance/ledger dirty及旧失败原样保留；详细修复验收卡和raw/source引用见主审计§3–8。


## 2026-11-17 六项质疑main只读审计：状态/反馈/预算P0仍未修

本节是主agent本轮只读审计与Hypatia侧审补充，不是用户原话/结论，不改现合同；详见[主审计§11](/home/wuyuqa/ros2-dds-fuzz-v2/docs/main-audit-2026-11-17.md)。原始证据：[state-feedback summary](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/state-feedback-audit-20261117/summary.json)、[report](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/state-feedback-audit-20261117/report.json)、[audit.py](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/state-feedback-audit-20261117/audit.py)，仅历史混合source/development统计，不是当前成熟/候选确认。

| 当前未修缺口 | 已核事实／下一验收边界 |
|---|---|
| P0 raw-backed状态投影＋统一complete | 124cycles/273rows/246actual；246raw存在且hash匹配。167judged及167feedback coverage，但actual八组件/state_before/state_after/observation_complete存在数全0、state eligible/recorded及computed edge全0；124feedback edge_counts均空，actual corpus admission0。不能以raw/coverage/pass替状态闭环；需共用投影与统一完整性且receipt可追溯，未知不补真 |
| 原用户P1由main策略升P0：restore＋refresh＋warmup连续性 | 239/273selection warmup，全部124cycle起始feedback_event_count=0，rare请求3实际0。当前和特定历史HEAD42f都是逐次next，非预build；scheduler冻结feedback，后续record不可见；corpus共享活对象理论可影响池，但历史admission0。新cycle需新scheduler/显式epoch＋持久序号/warmup，复用旧scheduler会被prefix guard拒绝，不能删guard |
| CLI/wall-clock预算 | CLI每次新corpus＋两个空FeedbackLog，warmup=min(N,K)；K≤N整轮warmup，换seed不推进排序。30min壁钟未enforce，budget.max_inputs/warmup_inputs未消费；--max-inputs80仅80次selection，拒绝也占，不是80actual；max_seconds只是单input恢复窗静态sum校验，不是elapsed deadline |
| P2组合与profile白名单 | 每cycle最多6actual，实际双算子plan6/246不等于逐层hit。runner缺字段回1、显式opt-in、profile.get(...,1)三处不能只改默认；同target sorted挑可组合surface，无pair配额，_chain_surfaces未接profile whitelist为额外待修。E01仅一surface强制2拒绝，两窗不等于两算子 |
| W01与未live面 | E02/E03/G01/W01 actual均0。fuzz_catalog.surface_to_core:884显式拒绝W01；报错“not implemented”不能推出M04全无，core已有F2 generation函数/路径但未live。不得uniform替代relation或E01多窗冒称E02/E03/M07 live |
| P3长跑及测试证据 | 先确认状态→反馈→选择→corpus完整持久链与壁钟预算，再由main给长跑GO。手造八组件/observed_edge的纯模块fixture不能证明真实adapter接线；单pair八组件仍不表达N-endpoint拓扑/代际历史 |

main此前等待Hypatia的scheduler/CLI事实已由本次侧审补充，但持久格式、epoch接口、预算兼容语义及其他未核细节仍待main协调，不擅自定论。拟修排序为状态P0与原P1同修→预算边界→P2路由/白名单/配额及逐层证据→P3有界长跑；这是主agent工程策略，不冒称用户自行重定优先级，不扩大scope或改72分母。

[final-freeze summary](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-repair-final-freeze-20261117/summary.json) valid=true、148文件前后稳定；[pytest.stdout.log](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-repair-final-freeze-20261117/pytest.stdout.log)为1143passed/1skipped/297.85s，source仍dirty/frozen=false、无commit/ROS。该最终离线回归已完成，但**不消除上述P0**，不构成新seal或真实状态反馈验证；若后续修复改source须新整合/冻结/真实验证，旧验收不迁移。

本writer仅追加本文及主审计note，不改acceptance/ledger/代码/测试，不运行ROS/campaign/业务测试、不commit。自然候选0、总体goal active、required72不变，无新execution/replay/candidate/seal。文档核对结果随本次交付报告，完成后停止写入。


### 交付前finding定稿（不新增任务）

以main [findings.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/state-feedback-audit-20261117/findings.json)为准：STATE-P0/COMPLETE-P0/PERSIST-P0/WARMUP-P0未修；CHAIN-P2、SURFACES-P1、WALLCLOCK-P1亦待修，不因前文“原P1升P0”统改其优先级。拟修顺序为状态/complete→持久feedback与warmup→elapsed/attempt/distinct预算→小型真实反馈/重启闭环→未live面及逐层组合证据→有界长跑。Hypatia侧审只读且未重扫历史；verified为审计发现成立，不是修复完成，不否定既有raw/oracle证据。source仍未冻结、1143pass/1skip只离线；自然候选0/goal active/required72不变，不保证80输入或30分钟出候选。仅append记录，完成后停写。


## 2026-11-17 P0实施派遣更新（未修复验收/无ROS或commit GO）

以主agent[dispatch-and-gates.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/p0-feedback-closure-20261117/dispatch-and-gates.json)及[工程决定D016](/home/wuyuqa/ros2-dds-fuzz-v2/docs/decisions.md)为准：Dirac（状态投影/三adapter）、Hypatia（统一complete/corestate/feedback/runner预算与脚本source身份）、Plato（corpus/scheduler epochs/checkpoint/CLI resume）三代码owner均gpt-6.1-sol，已进入实施，不等于完成。docs不代改代码/测试。

待验：①base观测complete与raw-backed transition快照分层，oracle pass不替complete、缺快照不伪报identity mismatch，真raw身份错fail closed；②before/after原始边界不倒填，pair八组件不冒称Nendpoint代际图；③显式append-only epoch、完整checkpoint及连续sequence/warmup/采样选择状态，selection可重算prefix/corpus，旧证据只读；④cycle monotonic deadline与selection/actual/有效非重复计数分离；⑤修复原source_manifest漏`scripts/run_s1.py`的新增身份缺口并做新source冻结/真实验证。以上均未由本次文档关闭，80/30min不保证候选。

1143pass/1skip仅前一稳定dirty快照，不覆盖本轮实施source；无ROS/commit授权，无新run/receipt/replay/candidate/seal。旧source验收不迁移，完整S1成熟及自然2–3候选目标仍未达；自然0、goal active、required72不变。详见[主审计§12](/home/wuyuqa/ros2-dds-fuzz-v2/docs/main-audit-2026-11-17.md)，本轮仅append并文档校验后停写，最终证据另派。


## 2026-11-17 P0离线更新／run-001 coverage缺口与live环境未放行

来源为主agent补充工程消息及[主审计§13](/home/wuyuqa/ros2-dds-fuzz-v2/docs/main-audit-2026-11-17.md)，非用户原话/原规格改写。限以下证据范围更新，不关闭P0整体或S1成熟缺口：

- **离线已证**：[历史raw重投影](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/p0-feedback-closure-20261117/historical-reprojection-after-boundary-fix.json)9 raw/8 observed receipt/6 edge、0异常/0引用错、source稳定，三个原incomplete均无edge；[CLI v2](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/p0-feedback-closure-20261117/cli-resume-verification-v2/report.json)9/9 checks，以真实CLI子进程验证whole4＝split2＋resume累计4，但planned/actual=false、无DDS/反馈；[raw反馈链](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/p0-feedback-closure-20261117/raw-feedback-chain-verification/report.json)12/12 checks、原input hash及artifact不变，新edge admit/corpus+1/下一prefix更新/恢复next5相同。皆非新current-source execution/candidate/seal。CLI原失败和main审计fixture enabled纠错分别保留，见[correction](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/p0-feedback-closure-20261117/cli-resume-verifier-correction.json)；ACK合并index/mono误拒反例亦[保留](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/p0-feedback-closure-20261117/boundary-merge-order-defect.json)，主agent报告纠正采用真实controller观测边界，不cleanup倒填。
- **最终离线整体验证尚不成立**：[run-001 summary](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/p0-feedback-closure-20261117/final-integration/run-001/summary.json)单次pytest1245pass/1skip（日志486.39s）、170文件/HEAD/source稳定、AST/worktree及index diffcheck/四required对账exit0；但原summary.valid=false、coverage exit1，四class映射未命中。main所指JUnit前缀遗漏尚待Plato用现成JUnit新增精确supplement核实；不预判、不重跑、不改原失败产物，不写整体验收通过。
- **live仍未跑**：[Hypatia预检侧审](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/p0-feedback-closure-20261117/owner-logs/hypatia/preflight-review/review.json)确认domain181四PID仍活/start_ticks未变/ns不可读；原预检exit2虽conflicts=[]且旧锁通过，仍不能排除owner，不等于已证DDS冲突。同host frozen代码无已验证不降门槛放行法，不whitelist/skip/kill；独立环境可选但未部署。新source冻结、四surface live、组合每层证据、带反馈live resume、壁钟预算长跑及Nendpoint语义状态扩展仍待证，不因离线增长关闭。

主agent通知代码停写，无ROS/commit GO；HEAD42fcec65…、source_frozen=false、四required revision0完整manifest/catalog及72分母不变，自然候选0、goal active，旧source证据不迁移。D016不改；acceptance/ledger既有dirty未触碰。本次仅append两文件、文档自检后停写，等待main补充结果。


## 2026-11-17 最后补记：P0稳定dirty快照离线门通过，live门不变

Plato[v3 summary](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/p0-feedback-closure-20261117/final-integration/supplemental-coverage-v3/summary.json)及[精确source/JUnit补审](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/p0-feedback-closure-20261117/final-integration/supplemental-coverage-v3/exact-source-bound-coverage.json)确认六节点各唯一passed、AST/SHA绑定已测snapshot，170行为文件与110项既有产物SHA不变（含原104份完成轮次产物）。已证原classifier漏`tests.`前缀；v2报告字段NameError原失败保留，v3纠正后exit0，无新增pytest，原summary.valid=false/coverage exit1不改。原1245pass/1skip exit0＋稳定snapshot＋六节点补审可记**P0该dirty快照离线验证通过**；唯一skip因未设`R2F_PROVIDER_AUDIT_REPORT`，不算通过或真实复验，完整节点见[主审计§13.1](/home/wuyuqa/ros2-dds-fuzz-v2/docs/main-audit-2026-11-17.md)。

主审定稿索引：[main-p0-gate-review.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/p0-feedback-closure-20261117/main-p0-gate-review.json)，SHA-256=`98f7b0cd02e3c01ddf1a51a665bcb29a94feb8b84edcf2eb003d0ccad29961b4`。仅关闭上述离线coverage待核，不关闭live/四surface/组合/长跑或完整S1成熟缺口；source_frozen=false、无commit/ROS、候选0、goal active、required72及原合同不变。acceptance/ledger既有dirty与D016不动，文档owner核对后停写。


## 2026-11-17 F2 windows显式激活进入实施（无live/commit GO）

据main[派遣合同](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/f2-windows-activation-20261117/dispatch-and-contract.json)、[before基线](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/f2-windows-activation-20261117/before.json)及[工程决定D017](/home/wuyuqa/ros2-dds-fuzz-v2/docs/decisions.md)，P0的170文件稳定dirty快照已保存；main解冻Hypatia（合同）、Plato（campaign/profile）、Dirac（runtime/raw）三个gpt-6.1-sol代码写集。新增`discovery.interruption_recovery.fuzz_windows_v1` rev0/capability `f2_windows_v1`；四required＋五旧fuzz完整manifest须保持，72分母不改，不迁移旧checkpoint/seal。

待证：E02/M07、E03/M06真实registry/runner路由与raw锚点时序；新profile enabled=false/chain1/精确白名单，`needs_observation`不是pass。首fresh双向match controller snapshot、后previous recovery锚点；offset not-before与前窗restore interval取max不叠加，policy与0.5s容差入hash，迟到归harness incomplete不算候选。须逐窗pair-bound真实drop/restore/graph/match/delivery及adapter独立复算；fake-clock/拒绝路径/九份旧manifest对账与新source最终离线整合均待出证，不借E01多窗或generation/seed provenance冒充E02/E03闭环。

详见[主审计§14](/home/wuyuqa/ros2-dds-fuzz-v2/docs/main-audit-2026-11-17.md)。旧P0离线通过只支持原snapshot，不覆盖解冻后source；G01/W01、组合/长跑及live预检缺口保持。无ROS/commit GO、source_frozen=false、自然候选0、goal active；本次仅三doc append，不改原规格/acceptance/ledger，不运行业务测试，停写等待最终结果。


## 2026-11-17 隔离环境可行性补证（不放行live）

main的[完整namespace probe](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/f2-windows-activation-20261117/environment-probes/full-namespace-probe.json)exit0；[nonroot nested probe](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/f2-windows-activation-20261117/environment-probes/nonroot-nested-probe.json)外层uid1000/CapEff0，现有内层discovery wrapper及assert通过、loopback UP、双层net/user inode不同。[两RMW原脚本隔离预检](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/f2-windows-activation-20261117/environment-probes/isolated-preflights-summary.json)domain181均exit0/ok=true，conflicts/unreadable/旧锁空、RTPS端口块可用、Jazzy包库SHA对齐lock；无context/endpoint/lease/campaign/commit。事实仅证明完整user/mount/pid/net独立边界路线可行，不是current-source live或clearance。

新增待实现门：生产supervisor、host/inner PID关联、host owner与锁/全子树清理合同；不得仅隐藏proc或用内层PID替代host owner。历史host预检失败及原结论保留，D017不动；live GO=false、source未冻结，旧证据不迁移。详见[主审计§14.1](/home/wuyuqa/ros2-dds-fuzz-v2/docs/main-audit-2026-11-17.md)；自然候选0、goal active、required72不变，文档writer未重跑探针/业务测试/ROS，核对后停写。


## 2026-11-18 S1接续核证入口（分层状态；不覆盖历史）

来源：main本次派遣及进展补充、[独立审计](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/main-review.md)；核证时点/实际日志/SHA见[2026-11-18接续核证](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md)。前文“当前”保留其当时source/日期语境，不回写旧失败。当前HEAD `42fcec65fe66d1dff8dd761bf03ee96a427277c7` / dirty / source_frozen=false；无本轮live/commit GO。

| 跟踪项 | 已知事实与下一验收点 |
|---|---|
| C18-01 确定性输入ID | [input owner交付](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/input-determinism-owner/delivery.json)和实际logs为93pass，另次unittest45/OK；三文件测试前后SHA一致且本writer读取时相符，不累加统计。自动ID/catalog digest改变，显式旧ID保留，重复声明拒绝；resolver未实现。局部修复有据，main最终整合/冻结另验。 |
| C18-02 native | [native report](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/native-owner/report.json)为73项中71pass/2skip，main初审finally与uncertainty并报告独立hash核对通过、owner freeze ACK/关闭。本writer核6个相关source SHA及实际日志；真实DDS loan/allocator、完整共享树回归/矩阵未验证。原ABI负回归失败保留。 |
| C18-03 隔离路由 | Bohr errored关闭，不算成功；main转交Feynman。[routing before](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/routing-owner/before.json)和first-fix局部19pass已落盘，不是最终交付；隔离/注入冲突入口拒绝、身份/cleanup/raw/source与host兼容仍待最终日志及main复审。 |
| C18-04 composed/双层链 | main报告Cicero恢复composed与全局mutators逐字HEAD，本writer已字节核对相符；新增endpoint-chains和实际双层内容测试。main扩写集至test_fuzz_corpus.py，要求更新硬编码白名单、不deselect；composition-owner最终产物核证时尚未取得，不标整合通过。 |
| C18-05 topology模型 | 初始257pass/3 strict xfail及TM-01分区模式保守性、TM-02 list/dict role TypeError原失败保留。main后续批准修源码/去xfail，覆盖早期纯测试限制。新落盘局部final日志/XML为360pass/0skip/0failure，after最小复现六例pass、模型SHA相符；只证明局部模型修复；[summary](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/topology-owner/summary.json)及[freeze ACK](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/topology-owner/freeze-ack.json)已核：TM-01/TM-02 fixed、360pass/0xfail、两源码SHA及summary SHA匹配、owner停写。main整合及registry/runtime接线另验。 |
| C18-06 新输入层未统一消费（下一段主缺口） | [current-input-mechanism](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/current-input-mechanism.json)：14surface/33slot/24operators均offline input_only；seed=26authored/5pending/2unsupported/1out_of_scope；10个oracle引用仍unresolved_no_resolver、independent_verdict=false。main及本writer源引用检查未见指定接线路径直接引用三新模块，仅作辅助；[registry probe](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/topology-registration-probe.json)确证当时topology capability拒绝。不能写24算子已在runner fuzz；须接统一输入→计划→执行→facts/oracle/replay链，不是再长跑即可补齐。清单pending针对该输入消费路线，不否认既有F2/netem实现。 |
| C18-07 N图/状态/载体/并发 | prediction≠observed，canonical pair八状态不覆盖N端点。D019的版本化扩展、逐pair/generation raw、projection/receipts/oracle/replay各自待证；复用已有netem delay/reorder/duplicate基础，87pass不是当前DDS hit，孤立queue不是绑定。批ACK不是跨writer重叠。 |
| C18-08 新Halley identity优化 | id=01a1ebfe-3eb0-7c60-8386-ed6c247c4bc7，gpt-6.1-sol，非旧文档Halley；main新派独占identity.py及source batch专属测试。测量后批量git读HEAD并保持等价身份校验/source gate，尚未完成，性能与拒绝/漂移语义都待最终产物。 |
| C18-09 最终source与真实目标 | 收齐patch后main统一six-gap新产物复审/全仓相关回归、明确commit GO、source冻结，再安排唯一live owner/checkpoint。旧seal不迁移；四required分母72不变、新candidate0、完整S1及2–3真实候选目标未达，不能complete。 |

**需用户决定事项**：本轮未新增已证实必须由用户改变范围/约束的请求。D019实施若触发新增target/rule/slot/carrier、native/security边界、已seal revision或分母变动，main须在此补具体变化/证据/影响；工程修复和证据待核不先验标blocked，文档owner不自行发问或扩scope。本次无campaign abort；Bohr服务失败、fixture首次导入失败及各before失败保留为工具/模型历史，不计SUT候选。

最终ownership/路由限制：main已恢复Darwin，新增独占boundary_audit.py及test_execution_boundary_audit.py，纯artifact一致性核验实施中；native写集保持冻结，不升级为kernel认证/真实执行。Feynman保留isolated资格unverified/fail-closed，正式隔离replay未成熟。文档owner完成本截面后停写，后续由main另派续写，不无限追随并发owner变化。


## 2026-11-18 D020拓扑接线固定待核截面（不追随后续并发修复）

cutoff=`2026-11-18T09:49:17.467623+08:00`，来源为main本卡/补充与[本卡固定核证截面](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md)§10；证据字节及SHA见[cutoff/SHA索引](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/docs-topology-owner/cutoff.json)。下列均为工程待核，未新增必须用户改变范围/约束的阻塞；历史C18、首次fullsuite及并发drift不删。

| ID | 当前有据状态 | 下一验证点／归属 |
|---|---|---|
| C20-01 输入与capsule | core桥接main365pass/5文件前后stable，owner934pass；通用payload_layout已获main真实provider构造capsule局部正证据，不再写成capsule必然失败 | Feynman独立replay与全链消费最终整合待验；provider/runtime显式validate_topology_plan且不再mutate |
| C20-02 统一fixture ingest | main canonical route已有61raw、fixture_application_observed，canonical7receipt全false；ValueError returned events differ from raw被缺primary_rule的KeyError掩盖 | main已分别派Tesla/Cicero/Pasteur修；核event0 receipt别名、rule_contract与错误保留链。本卡不追修复，无SUT归因 |
| C20-03 N观察／交付 | 总count+graph的两图反例是工具歧义；允许唯一send-raw关联payload，不是direct GID | Mencius/Pasteur/Tesla：逐pair/generation/raw窗口、歧义unknown、单writer闭合、版本化state/projection及独立oracle；声明不等于resolver资格 |
| C20-04 正式执行门 | routing331pass最终580.38s，但corpus外部漂移；main75pass仅owner8文件stable，main接受离线milestone | 真实3lease/environment/source/cleanup及隔离资格仍待验；trueflag/字符串不放行，无live GO |
| C20-05 算子诊断 | 单parent2W2R/seed11的21已声明topo ops：20生成、1 lease_refresh拒绝；不是全面支持率 | main指出liveliness enum slot可被lease delegate拒绝，正确lease slot可能可用，模型slot/implementation适配待核；lease_expiry/refresh改配置不证明生命周期；harness guard拒绝不是SUT安全拒绝 |
| C20-06 最终验收 | 原2237pass/4fail/12skip与624pass3fail、631pass并发drift保留；原4fail已有main独立4pass，不迁移成全仓通过 | main统一freeze/稳定完整回归、新commit GO/source_frozen与唯一live owner另行安排；扩展单列成熟义务，旧72不变、自然candidate0、目标未complete |


## 2026-11-18 follow-up固定工程缺口（不新增规则／不覆盖C20）

cutoff=`2026-11-18T10:25:44.648924+08:00`；来源为main明确补充、[cross-module findings](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-continuation-audit-20261118T074855/docs-topology-followup-owner/snapshot/main-topology-cross-module-findings.json)与[接续核证§11](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md)。以下是工程核查项，不先标用户阻塞；旧红记录、D020及首次fullsuite不改，后续并发修复不纳入本卡完成声明。

| 项 | 本截面已记录进展 | 未闭合验证点 |
|---|---|---|
| C21-01 X01–04 | main56pass/108.93s、五facts文件记录前后stable；两层1W1R/2W2R raw79/135与returned相等/稳定，缺primary_rule不再掩盖原错；fixture admit/bind在mutation_begin前，canonical state_projection_receipt到FeedbackLog离线合同有据 | fixture presteps均false且missing environment正确abort，不证明真实前置/交付/novelty；main原错禁所有ingestion_error的断言失败保留 |
| C21-02 X09／IP-01 | persisted audit仅复核既有产物，明确topology_state_boundaries_missing未解决；Mencius snapshot action有真实请求/观察/ACK但缺对应state_step_after，造成3fail | Tesla须按真实chronology产出snapshot完成及专用topology_state_boundary，完整性不能补造；projector未知不凭配置变成alive/absent |
| C21-03 IP-02/03/04 | retirement_identity_or_cleanup_mismatch 1fail；后端不能独立构造全graph可见/count1歧义 1fail；不能产生无publisher_gid的实际自动接收 1fail | 准确retire代际/关闭身份、独立graph/count fixture控制、保留实际payload/send链的GID不可用fixture；不手工替raw或放宽consumer |
| C21-04 X05/06/08 | environment持久化绑定、N路线通用qualification gates仍待接；graph可见而两端count0非逻辑矛盾 | backend receipt不能覆盖已测environment；metadata/source/worker谱系与cleanup门不可早退绕过；不反向推断正N边 |
| C21-05 Cicero／X07 | 原529pass伴4外部文件漂移，两个旧profile/route断言须修的历史处置保留；当前两测试修正ACK自报23pass/停写，旧11文件map仍一致 | 本卡未读该卡delivery/JUnit/源码；不升级为main独立修复通过。capsule局部正证据仍不是完整cycle/replay/audit成熟，calls0旧根因未明保留 |
| C21-06 最终门 | Mencius集成22/28pass、6普通fail，无skip/xfail，单测试freeze；Pasteur关闭；Mencius仅做只读checkpoint准备 | owner/source统一freeze后main稳定完整回归、明确commit/source/live GO、唯一live checkpoint另派；四required72不变、自然candidate0、目标未complete |


## 2026-11-18 13:11 恢复卡当前工程待核（六 owner；required/exploratory 同类型）

固定 base cutoff=`2026-11-18T13:19:50.494563+08:00`，main 两条明确新消息另记 `2026-11-18T13:24:34.359693+08:00`；来源及 §12 勘误见[接续核证 §13](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-resume-fixed-cutoff-1311)、[固定证据索引](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-owner/cutoff.json)、[六人 dispatch 收件](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-owner/main-message-supplement-1/dispatch-current-six.json)。D021 为 main 工程澄清，不改 taskbook。原 §12 与全部历史失败保留，本表不随并发结果自动变绿。

| ID／归属 | 当前有据状态 | 下一决定性验证点（均未闭合） |
|---|---|---|
| C22-01 Hume／统一目录 | required/exploratory 同 `core.SemanticTarget`；required 固定 conformance，exploratory 扩展 fuzz 复用适用规则／状态，非两套类型；目录／家族解析实施中 | 统一目录→family/CLI→provider/capsule/replay 的实际消费与旧 manifest 逐字兼容；legacy ID 仅为仍使用的验收／replay保留，不能搬旧 evidence 或重写 required72/revision0 |
| C22-02 Descartes／算子接入 | 旧 Mill 独立模块局部反射不等于 route 已接；12:59与13:02 ACK版本不同，不挪用；旧9/21诊断不代表当前能力 | 每层封存输入→对应 endpoint raw 实际反射→auditor 消费；预测关系不作 observed match，计时／lease配置不作生命周期；netem不回退，最终接入后相关回归另验 |
| C22-03 Mencius／delivery window | 旧87／147 raw能发前后boundary，但 projection incomplete、两boundary window_complete=false，before观察亦不全；环境缺失不是唯一成熟缺口 | 真实发送／接收／窗口／sentinel chronology 与代际关联，producer/projector不补真；实际运行／恢复与当前source整合须单独出证 |
| C22-04 Hooke／CFT错误纠正 | 本机 create_subscription有content_filter_options、Subscription有is/set/get；worker244–248原“无API”拒绝是工程误判，不再写产品不支持 | Hooke独占worker/filter backend/新test，**仅初始reader配置和实际读回，实施中**；动态filter、样本效果／match规则、两RMW验证不属于已完成。API静态存在亦不等于RMW过滤通过 |
| C22-05 Galileo／能力audit | 本卡已有固定artifact/saved-field分层；Galileo当前只读audit卡尚不能由目录文件名当完成 | 按 source/receipt/raw/replay/层与实际入口分级，不以旧9/21、注册名或summary总数代表已执行；main最终审阅另记 |
| C22-06 netem carrier层 | 旧non-DDS记录只支持安装／计数／恢复；reorder无逐包顺序断言；host无proof拒绝为局部receipt | 真实ROS端点流量绑定、实际效果与恢复、隔离/三lease/cleanup全链另验；不得计S1 hit或借non-DDS放行live |
| C22-07 回归／最终source | 3189pass3skip只支持新netem/Mill前243文件历史快照；308pass为后续拓扑子集（main说明，本卡未获其独立日志） | 所有owner停写后current-source稳定整合、旧manifest/输入/replay对账、main commit GO与source_frozen实测；不把旧ACK、子集或文档检查写成最新全仓通过 |
| C22-08 候选／历史统计 | 124 cycle的246 actual_run=true只是saved-field统计，未复审raw；netem-multi-1为旧fuzz_v1、非fuzz_windows_v1 | 不能由保存residual字段推“未触发违约且排除筛选丢失”；自然candidate0、非空生命周期／2–3目标未达，只能由真实新运行与归因形成证据 |

当前六 owner为 Descartes／Galileo／Hume／Helmholtz（唯一docs）／Mencius（新handle）／Hooke，均显式请求 `gpt-6.1-sol`；完整ID及来源见上述dispatch。旧handle不计活动，main close Mill的`not found`不作完成证明。外部WorkBuddy PID2301092已更新为 **last_seen_absent**（main约13:22、本卡13:24只读ps无行），退出结果未知、不借用测试，不据此宣称环境clearance。

HEAD `42fcec65fe66d1dff8dd761bf03ee96a427277c7` dirty、source_frozen=false、goal active非complete、自然candidate0；本轮无新增live/commit GO。四required revision0／旧manifest／72分母不变，本卡新增执行／成熟分子均0；RMW/domain/run_id未运行。本表均为main工程待核，不新增必须由用户改变范围／约束的阻塞，不授运行／扩scope；唯一docs owner完成freeze ACK后停写。


## 2026-11-18 小型follow-up增量待核（固定截面，不覆盖C22）

cutoff=`2026-11-18T13:44:10.558816+08:00`；main明确重新派遣同`gpt-6.1-sol`原docs handle，证据见[§14](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-resume-docs-followup-1344)／[15件SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/cutoff.json)。不新增规则或scope。

- **C22-01更新**：Hume124pass（含39新增）／freeze、main85pass/CLIexit0、11identity/6family、21组视图manifest对账相同，main已关闭Hume。仅核收离线目录整合，原“尚未交付”成为历史；不声称规则合同合并删除、新source覆盖或成熟。
- **C22-02更新**：Descartes owner138pass、旧三完整audit9/9等价、netem原字节保留；main独立125pass，七依赖在其测试窗口稳定。新三relation.pair/timing.shift/lease.value仅配置审计接入，无match/timing/lease生命周期effect或hit资格；旧三原合同不变，不能外推全source稳定。
- **C22-03/04仍待验收**：Mencius与Hooke仍写pair／worker(CFT)及指定测试；producer／consumer真实闭合、初始CFT配置／实际读回的main整合尚未核收。继续产未消费helper或只增加独立测试不能关单。
- **C22-05计数澄清**：能力矩阵输入24/24、至多11/24审计（新三仅配置）、v2真实DDS0/24；旧12surface10路由、8有历史代表raw；current-source seal证据0/72不是代码完成度0。各层不得相加成成熟百分比，旧seal不迁移。
- **C22-07/08下一门**：**当前producer/consumer稳定收口→源码冻结→精确新路径短真实DDS闭环**。F2 exacttarget `discovery.interruption_recovery.fuzz_windows_v1`仍无本卡所见新v2真实运行，不能借旧fuzz_v1。85/125是合同子集，非当前全仓；当前dirty/source_frozen=false、候选0/goal active，required72/revision0与旧manifest不变，无新增live/commit。日常验证由main安排，无新增需用户改变范围／约束事项。


**明确main消息独立补截面（`2026-11-18T13:48:47.092310+08:00`）**：Hooke初始CFT卡已有[delivery](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/cft-main-message-supplement/delivery.json)／[freeze ACK](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/cft-main-message-supplement/freeze-ack.json)／[final test result](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/cft-main-message-supplement/test-result-final.json)，owner65pass（21新增）、11个直接依赖在该测试窗before/after一致；main已关闭Hooke。仅核收初始reader配置／实际读回的**离线交付**，真实RMW尚未测，无dynamic filter／sample-effect／lease主张；不等于source_frozen或S1成熟。本补充不回写13:44旧截面。main当前明确Mencius是唯一代码writer，窗口producer仍在收口、未验收；`/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/main-integrated-regression/run.py`仅**已准备、未启动**，等Mencius freeze才做全仓。本docs卡未读取／执行该脚本，不借准备状态预填回归结果；无新增live/commit或scope。


**后续main消息独立补截面（`2026-11-18T13:55:10.821258+08:00`）**：Mencius已有[summary](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/mencius-main-message-supplement/summary.json)／[freeze ACK](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/mencius-main-message-supplement/freeze-ack.json)／[最终记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-followup-owner/mencius-main-message-supplement/producer-expanded-followup.json)，owner最终288pass/39.91s、8依赖在该测试窗稳定；31聚焦与288重叠不累计。写集仅`/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/runtime/topology_pair.py`及`/home/wuyuqa/ros2-dds-fuzz-v2/tests/test_topology_delivery_windows.py`、`/home/wuyuqa/ros2-dds-fuzz-v2/tests/test_topology_runtime_route.py`三文件，pair SHA=`25de6fa16ed37eae311513fd1e9c544a0ca3e410e2cd450ea7055511dfb2450b`。main已关闭Mencius，当前所有代码owner均停写；这只是owner离线交付／写集freeze，不等于全source冻结、真实DDS窗口effect或S1验收。main已启动`/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/main-integrated-regression/run.py`的实际全仓`tests/`，**本消息时仍运行、结果未知**；`R2F_BOUNDARY_KERNEL_TESTS=1`是non-DDS kernel对照，不是ROS/DDS/campaign或成熟证明。本docs卡不运行／等待／借用该测试，不回写13:44与13:48历史或已freeze证据；仍需main整合核证→源码冻结→精确新路径短真实DDS闭环，无新增live/commit GO、候选或72分母变更。


## 2026-11-18 D022实施中与回归完成增量（不覆盖旧截面）

来源为main本卡直接派遣／工程裁定及[本卡4件必要证据/SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-intervention-owner/cutoff.json)，不是原规格新增字面要求。完整分层见[接续核证§15](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-intervention-regression-cutoff)。

- **C24-01 旧batch回归已完成，不是新batch验收**：[main result](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-intervention-owner/snapshot/main-integrated-regression/result.json)／[JUnit](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-intervention-owner/snapshot/main-integrated-regression/junit.xml)确认3406passed/3skipped，wrapper411.87s、253source/test测试窗稳定、HEAD不变；两skip为真实native opt-in未开、一skip需retained real provider report（[skips](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-intervention-owner/snapshot/main-integrated-regression/skips.json)）。non-DDS kernel对照不算ROS/DDS/campaign或成熟。旧“仍运行”原文保留；随后main解冻两新卡，旧3406不证明后续source通过。
- **C24-02 [D022](/home/wuyuqa/ros2-dds-fuzz-v2/docs/decisions.md#s1-configuration-intervention-d022)干预hit门待实施／真实核证**：sealed映射差异须排除rounding alias；完整原始actual after readback、身份／代际／时序、无覆盖、formal real qualification须齐。fixture／booleans／回声不授hit；无独立effect证据保持false，typed facts/oracle另判expiry／renewal／match。不能永久将配置干预等同生命周期效果，也不能据本裁定补旧run hit。
- **C24-03 两新卡与既有filter.expr消费仍未交付**：Banach环境观察／隔离1W1R，Sartre配置干预gate／初始CFT raw consumer；既有filter.expr不代表动态filter／样本效果，旧三parity和红日志不变。新增scope独立成熟义务不能由72代替，不新增target/rule/revision/算子／分母。

HEAD `42fcec65fe66d1dff8dd761bf03ee96a427277c7` dirty／`source_frozen=false`，candidate0、goal **active非complete**；原四required revision0／旧manifest／72分母不变，无新增live/commit GO。本卡只记当前明确决策与回归，后续整合／冻结／精确真实DDS由main另行安排；无新增需用户改变范围／约束事项，不把日常待验项冒称用户阻塞。


## 2026-11-18 filter schema工程缺口小增量（main发现；实施中）

**C24-04**（cutoff=`2026-11-18T14:24:02.984960+08:00`）：main已发现当前valid filter `seq >= %0` 与worker使用的`std_msgs/msg/String`仅有`data:string`不符；这是**工程缺口／实施中**，不是原规格新增要求，不能据“valid”标签宣称可执行。Rawls `01a1ed4e-08f6-77b1-b709-dbb21cd25064`，main显式请求`gpt-6.1-sol`，不推actual backend；来源[main派遣来源记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-filter-increment-owner/task-contract.json)及[implementation-contract固定副本](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-resume-20261118T1311/docs-filter-increment-owner/snapshot/implementation-contract.json)。其写集仅`/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/modules/s1/fuzz_mutators_v2.py`、新`/home/wuyuqa/ros2-dds-fuzz-v2/tests/test_topology_filter_generation.py`；`/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/modules/s1/input_model.py`仅必要的局部filter元数据／字段描述，非一般模型重写授权。

要求依选中endpoint声明的message schema生成合法、非no-op的`data`字符串SQL，literal／参数类型正确；未知type必须具名MutationRefused或显式unknown，不能冒称executable valid。显式empty类和invalid负类保留，invalid期望guard reject。任务卡计划核profile/corpus I2 provider→prepare/MutationPlan→最终topology_program并与既有filter_backend.validate_filter对照，含两层组合、参数类型、unknown与确定性hash；**这是待做验证点，不是已通过结果**。本docs卡不读／追踪该卡代码、不跑测试。

只涉及输入生成／离线合同，不授filtering／match／sample-effect／hit或RMW支持，也不升级[D022](/home/wuyuqa/ros2-dds-fuzz-v2/docs/decisions.md#s1-configuration-intervention-d022)的formal real qualification。旧hash／执行证据和红日志保留，不借先前3406回归证明后续source通过；不改target/rule/revision/算子／required72。其他三owner Banach、Sartre及Helmholtz沿用现任务，无新结果推断。HEAD42fcec65…dirty／source_frozen=false、candidate0、goal active非complete；无live/commit GO、无新增真实RMW/domain/run_id／raw/replay。 本项为main日常工程收口，不新增需用户裁定的范围变化。


## 2026-11-18 D023 fair策略实施中与失败probe待诊断（固定增量）

cutoff=`2026-11-18T15:39:18.528072+08:00`；来源main当前明确工程裁定／[main pre-dispatch固定记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/snapshot/pre-dispatch-evidence.json)，不是原规格新增字面或用户默认切换授权。详见[接续核证§16](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-scheduler-baseline-cutoff)、[7件原字节/SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/cutoff.json)。

- **C25-01 [D023](/home/wuyuqa/ros2-dds-fuzz-v2/docs/decisions.md#s1-fair-base-input-scheduler-d023)未完成**：显式fair按固定corpus/seed/history选择，observed edge/coverage/oracle不影响selected seed／sampling／mutation RNG；Rawls scheduler与Sartre runner/CLI窄接线均待交付。省略选择的旧feedback路径、旧默认与log/replay字节兼容须验，不把fair当BFS/DFS planner／真实新状态发现。
- **C25-02 无反馈边界须验**：只对固定条件下基础输入选择作无运行反馈声明；feedback ledger/corpus retention准入保留，不能称整个campaign无反馈或关闭原系统来满足fair。
- **C25-03 旧局部交付有限核收**：Rawls281×2 JUnit、Sartre173 JUnit及两ACK SHA有据，source/test精确hash见[owner精确source/test hash索引](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/owner-source-identities.json)；两次281不累计，旧offline freeze不证明当前fair源码或RMW/effect/hit。
- **C25-04 失败probe仍待真实路径纠错**：[旧失败probe-assessment](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-owner/snapshot/Banach-failed-probe-assessment.json)为外层模拟loopback EPERM／未入nested；非零UID exec可能丢caps只是main待诊断假设，不宣告根因或环境不可解。Banach新non-DDS callable probe未有本卡结果。
- **C25-05 3538/12只有exec观察**：无saved main JUnit及稳定hash，skip原因未知；209/418也仅exec子集、不累计。root collection rc2/169失败保持，后改显式tests/不删历史；不借旧3406的三skip原因解释这12项。

HEAD `42fcec65fe66d1dff8dd761bf03ee96a427277c7` dirty、source_frozen=false；source当前解冻，candidate0、goal **active非complete**；原四required target/revision0／旧manifest／72分母不变，无live/commit GO。 新source已解冻，旧结果不外推。日常策略／probe验证由main安排，本卡不新问用户、不冒称需用户批准才能做现任务；但也不声称用户已批准默认更换。扩展scope独立成熟义务仍在，72不代替；本docs freeze后停写。


## 2026-11-18 D023离线milestone核收与canonical bridge新待验（固定截面）

固定cutoff=`2026-11-18T16:03:44.254141+08:00`；[12件必要原字节/SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/cutoff.json)；[main milestone acceptance](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/milestone-acceptance.json)原字节SHA=`1eef8020a1a2341da84794d163887230d8116b2f33279b7a3cecf06db231503a`精确匹配。 详情[接续核证§17](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-scheduler-milestone-followup)，只补本cut，不覆盖旧失败或旧ACK。

- **C25-01增量核收**：main接受[D023](/home/wuyuqa/ros2-dds-fuzz-v2/docs/decisions.md#s1-fair-base-input-scheduler-d023)离线scheduler/runner/CLI milestone；337pass/9skip与262-file测试窗稳定有saved result/JUnit，新source未正式freeze。不能以337代替全仓或迁移到后续改动。
- **C25-02边界不变**：fair资格仅在固定corpus／RNG seed／selection history条件下；运行edge/coverage/oracle不驱动基础输入选择／sampling／变异RNG，feedback ledger与corpus retention准入仍保留，不能声称全campaign完全无反馈。不是BFS/DFS状态planner或自动状态发现，main接受milestone不等于用户批准默认更换。
- **C25-06 legacy全路径仍缺**：**兼容性资格分列**：默认legacy／省略策略未改，fair仍显式opt-in。main记录原mutation RNG公式分支单独测试；[36 scheduler bytes记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/legacy-36-byte-comparison.json)记录36组旧／新scheduler states/events/contexts/resume及v1/v2 replay bytes离线一致。本docs核该JSON的36记录及声明，不运行公式分支或旧代码。**24-input legacy全路径capture parity仍未验证**，不能把公式分支／36 scheduler bytes／fair 2+2对照合并成该资格。
- **C25-07 CLI主审纠错不是产品修复**：[CLI corrected assessment](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/CLI-corrected-assessment.json)为实际CLI、no mock source、纯planning：first2＋resumed新增2的10项具体输入字段（content/parent/effective/plan hash、seed、stage、final_input、mutation_plan、input_layer_receipts、sampling_kind）等于whole4，resumed完整selection log也等于whole4，累计selected=4；本docs对保存的三cycle JSON被动重算一致。显式feedback resume策略错配在输出创建前拒绝（main评估及保存stderr有据）。main checker先误把resume仅新增2行与whole4直接比较，报`AssertionError at assert rows(resumed)==rows(whole)`；改为原2＋新2后通过，**无产品改动／无CLI重跑**。错误原记录保留，不当产品bug或真实runtime replay；all planned／actual_run_rows=0。
- **C25-04增量与新缺口分列**：[main probe artifact review](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/probe-artifact-review.json)接受non-DDS feasibility milestone，[bounded probe summary](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/snapshot/probe-summary.json)原字节与其内嵌summary、summary SHA一致；[32项只读hash复核](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-scheduler-baseline-20261118/docs-followup-owner/probe-hash-check.json)复核全部32项artifact hash一致，只读现有结果、不复制整批或重跑kernel。该bounded probe已证outer UID1000/cap0的kernel认证、inner root＋net/PID/mount/proc证明与active PIDNS-scoped proc、内层严格完整inventory（controller PID1及detached sleeper PID9，零missing/unreadable）、detached child与两层树退出，以及host3＋nested3共6lease释放／owner bytes不变。lease身份依绑定namespace／packet proof，不靠裸PID1或collector pid_scope猜测。**host原scanner仍有不可读同UID进程、host_clearance=false**；这只是固定non-DDS controller/sleeper，不是真实ROS backend inventory、canonical 1W1R闭环、候选或成熟。旧EPERM／未入nested及旧cap-loss假设保留为此前版本，不由新probe改写旧失败或确认旧根因。 main已将Rawls／Sartre owner写集freeze并关闭；随后仅显式解冻Banach指定producer写集做canonical nested topology bridge。该bridge在本消息截面**尚未实现交付**，不是bounded probe已接入ROS路线；无DDS/live/commit GO。本sole docs沿用main请求gpt-6.1-sol、不推actual backend，不追随后续源码。

HEAD `42fcec65fe66d1dff8dd761bf03ee96a427277c7` dirty／source_frozen=false、candidate0、goal **active非complete**；原四required target/revision0／旧manifest／72分母不变，无新增target/rule/revision/算子或成熟计数；扩展scope独立义务不能用72代替。 下一门为Banach指定bridge producer／consumer实施和独立核证，之后main整合／源码冻结／精确真实DDS资格；本卡不把新任务当已完成，不新问用户、不追后续实现。


## 2026-11-18 topology输入／路由及bridge合同固定待核（main docs-only增量）

cutoff=`2026-11-18T16:42:20.973929+08:00`；[本卡15件原字节/SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-topology-integration-20261118/docs-followup-owner/cutoff.json)，[main派遣／来源边界](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-topology-integration-20261118/docs-followup-owner/task-contract.json)；本卡只追加三文件，不改decisions／acceptance-ledger，旧原文／ACK保留。详见[接续核证§18](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-topology-input-route-integration-cutoff)。

- **C26-01 输入模型／路由解耦未实施**：用户最新质疑由main转述为seed↔surface是否一一对应；[seed-surface固定review](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-topology-integration-20261118/docs-followup-owner/snapshot/seed-surface-review.json)是main工程核证，不把工程推论写成用户原话／具体新schema批准。该review范围内**旧catalog12 surfaces／12 authored seeds均同名singleton ref**，corpus拒绝多ref时原文为`seed P01 must reference exactly one surface in this slice`；这不是全topology只有12种输入或12个唯一payload的证明。P01属于`discovery.late_endpoint`，topic`/r2f/s1/late_endpoint`、reader delay0.15s、publish interval0.03s；W02属于`discovery.late_join_delivery`，topic`/r2f/s1/publish_interval`、reader delay0.02s、interval0.01s；两者baseline reader delay0.02s、counts2/6及所列QoS相同，不等于原P01/W02本就是同一输入。 另行synthetic metadata-only离线复现中，同一content/payload挂两surface被dedupe仅保留首路由P01（same_content_identity=true、retained_count=1）；这证明当前缺route union／输入身份与路由解耦的工程约束，**不是现有content dedupe identity错误，也不是SUT执行**。chain profile可组合P01/W02且从entry surface起步，不等于已实现一payload多路由。main判为输入模型／路由解耦**尚未实施**的缺口；不新增已获用户批准的schema、target或规则。
- **C26-02 fair4只规划**：[planning topology fair review](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-topology-integration-20261118/docs-followup-owner/snapshot/planning-topology-fair-review.json)／[planning命令／稳定窗记录](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-topology-integration-20261118/docs-followup-owner/snapshot/planning-topology-fair-command.json)是main独立无mock CLI topology fair4 **仅规划**：4unique、每行两层input receipt的before/after hash均改变，stage=planned／actual_run=false；四个保存capsule execution均null，无execution_history/replay_attempts，无execute flag／live GO／真实raw或capsule execution。isolated_netns配置标签／planned runtime路径不证明namespace、worker或SUT实际运行；input层hash变化不是mutation_applied／hit／effect，四规划输入不证明算子广度、候选或成熟。本docs只被动对保存cycle／receipt／capsule关联复核，不执行CLI或runtime。
- **C26-03 旧producer交付与后发现缺陷分列**：[旧producer freeze review](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-topology-integration-20261118/docs-followup-owner/snapshot/producer-freeze-review.json)／[14项artifact review](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-topology-integration-20261118/docs-followup-owner/snapshot/producer-artifact-index-review.json)记录main对旧producer交付核7个source hash／14件artifact一致，[旧producer JUnit](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-topology-integration-20261118/docs-followup-owner/snapshot/old-producer-junit.xml)独立解析548项=539pass/9skip。此处7/14是main旧审查记录，本docs不读源码或重复rehash当前文件；不由旧539推定后续源码通过或全source freeze。随后[bridge_id独立静态缺陷](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-topology-integration-20261118/docs-followup-owner/snapshot/bridge-id-contract-defect.json)保存main对实际producer request赋值keys与consumer严格schema的AST提取：producer有config/config_sha256/outer/source_manifest/output_root/options，consumer另必需`bridge_id`，缺项确认为bridge_id。**只属source序列化／consumer合同静态实证，不是kernel或ROS失败**，不得放宽consumer gate来关单。旧539窗口／旧freeze／red日志保持immutable。
- **C26-04 当前修复未最终整合核收**：main已恢复Banach `01a1ed40-0927-76d2-a068-2a0a8829041e`，本修复卡独占仅`/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/runtime/topology_namespace.py`＋`/home/wuyuqa/ros2-dds-fuzz-v2/tests/test_topology_namespace.py`，做bridge_id producer修正及producer-driven正／负consumer合同检查；尚无本cut最终main整合核收。Sartre `01a1ed4a-c0ca-7002-85b0-73ba5fbf4962`修runner execute-only intent；Rawls `01a1ed4e-08f6-77b1-b709-dbb21cd25064`修replay/audit inner root、qualification与consumer；两卡**实施中，无最终main整合核收**。[main dispatch/inspection](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-topology-integration-20261118/docs-followup-owner/snapshot/dispatch-and-inspection.json)／[旧静态intent call-site inspection](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-topology-integration-20261118/docs-followup-owner/snapshot/intent-callsite-inspection.json)只支持当时静态接线／派遣观察，不把call-site存在当执行证据或新修复完成；后续代码本docs不追踪。

HEAD `42fcec65fe66d1dff8dd761bf03ee96a427277c7` dirty、source_frozen=false；candidate0、goal **active非complete**；原required72／四target revision0／旧manifest不变，无新增live/ROS/kernel probe或commit GO，无新增target/rule/revision／成熟计数。 下一门为bounded producer→strict consumer正／负真实合同核证、execute-only intent与replay/audit root整合→owner freeze／main独立相关检查→正式source冻结；真实DDS仍需main另行明确安排。输入/路由新schema只登记缺口，不假定用户批准、也不自行新增用户阻塞问题。


## 2026-11-18 seed路由／大request transport新截面（不覆盖旧C26与失败记录）

cutoff=`2026-11-18T17:37:44.882043+08:00`；[固定30件原字节／输入SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-seed-route-and-transport-20261118/docs-owner/cutoff.json)／[本卡派遣与来源边界](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-seed-route-and-transport-20261118/docs-owner/task-contract.json)／[被动核证60项](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-seed-route-and-transport-20261118/docs-owner/evidence-review.json)，详见[接续§19](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-seed-route-transport-fixed-cutoff)。本卡只追加三文件，不写decisions／acceptance，不追随后续代码。

- **C27-01 历史source冻结不等于当前已冻结**：[freeze-receipt](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-seed-route-and-transport-20261118/docs-owner/snapshot/source-freeze-receipt.json)证明e35c913曾source_frozen=true；3920pass12skip为提交前稳定字节窗口（266before/after，完整behavior仅during→after，main凭相同digest关联提交），提交后69pass仅identity/intent合同。均离线、非DDS成熟，不能搬到当前解冻后源码。HEAD元数据为`e35c913520888716aeb129cb34a9da9f178a7b0f`；main当前已解冻transport修复，**dirty／source_frozen=false**（来源为main本卡工程报告，非本docs重新扫描行为树）。四required target revision0／原72分母不变，自然candidate0、goal **active非complete**；无第三次live GO／新commit GO。
- **C27-02 大request控制transport未修复验收**：[runtime_returned](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-seed-route-and-transport-20261118/docs-owner/snapshot/attempt2-runtime_returned.json)／[replay_report](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-seed-route-and-transport-20261118/docs-owner/snapshot/attempt2-replay_report.json)保留attempt2 invoked但blocked／real execution=false／raw null／candidate=false；[control-size](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-seed-route-and-transport-20261118/docs-owner/snapshot/cleanup-recheck-control-size.json)99042>60000、实际未发送，非SUT执行。attempt1按main报告为output拒绝、未runtime，[finish](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-seed-route-and-transport-20261118/docs-owner/snapshot/attempt1-invocation-finish.json)仅核exit2。[main repair decision](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-seed-route-and-transport-20261118/docs-owner/snapshot/transport-repair-decision.json)sealed-memfd严格门仍实施中；Avicenna主修，须独立回归与新source/GO，不能借历史3920/69销项。
- **C27-03 cleanup仍未authenticated**：[recheck summary](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-seed-route-and-transport-20261118/docs-owner/snapshot/cleanup-recheck-summary.json)／[process-identities](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-seed-route-and-transport-20261118/docs-owner/snapshot/cleanup-recheck-process-identities.json)／[lease-state](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-seed-route-and-transport-20261118/docs-owner/snapshot/cleanup-recheck-lease-state.json)仅已知PID缺席、三lease锁0且原inode/owner相符；原cleanup_confirmed=false、quarantine／未release receipt、host_clearance=false保持，不能推全树／domain清场。guardian旧start_ticks未知；17:26 source_frozen=true不推广当前。没有第三次live GO或清理授权。
- **C27-04 baseline多路由尚未交付**：用户质疑与main方案来源分开。[旧seed audit](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-seed-route-and-transport-20261118/docs-owner/snapshot/seed-surface-review.json)12是旧catalog而非全部topology输入，丢route union不能归罪content hash；[共同baseline proof](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-seed-route-and-transport-20261118/docs-owner/snapshot/common-baseline-core-proof.json)仅证现有core可同baseline独立／组合P01/W02，real0、guards unknown。[静态semantic map](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-seed-route-and-transport-20261118/docs-owner/snapshot/semantic-route-map.json)非覆盖。Popper首次stream断开后同handle接续，本cut无schema/代码交付；具体新schema不是用户已批准，不能称实施成功，不需另造state machine或target规则。

当前Avicenna／Popper＋Helmholtz docs按main请求gpt-6.1-sol；Banach results-only已main关闭、旧source权限不续。下一门仍是当前consumer/producer收口→owner停写与独立回归→明确新GO/source冻结/fresh输入→精确真实路径闭环，不用继续堆未消费helper或旧72填新增scope成熟义务。本卡不source/tests读写、不tests/ROS/netns/campaign/stage/commit，不派下级；freeze ACK后停写。


## 2026-11-18 frontier repair待验增量（固定cut，不覆盖历史）

cutoff=`2026-11-18T19:19:38.520960+08:00`，[cutoff／12件必要artifact SHA](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/docs-owner/cutoff.json)，详情见[本卡接续核证](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-continuation-2026-11-18.md#s1-frontier-repair-fixed-cutoff)。D028将有界input_search_bfs与novelty corpus解耦；identity内容投影v2／full replay、no-change submission终态门仍实施中，parents API约定不是已交付验收。需独立验证feedback不变性、续跑等价和no-op／duplicate不提交；topology既有拒绝门保持。entry仅owner报告305pass＋main独立7pass离线里程碑，不能销DDS qualification。历史159／543／246／29为saved统计＋owner4raw抽查，26跨5HEAD／2RMW、scoped超额19，不能归为26同源漏去重。准备owner工具errored未交付，由main协调，不推环境不可行、不新增用户阻塞问题。HEAD=`979da1d4ba606e903f78fb44af8c34954e809b24`、behavior dirty／`source_frozen=false`；四required revision0／原manifest／72不变，candidate0、goal **active非complete**，无新live/commit GO。source状态来源为本卡main消息与artifact，不是docs重扫行为树。


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

主oracle两行仍unknown（finite_tail_or_delivery_contract_unknown）；正式report audit仍`evidence_valid=false`／4errors／rows_verified=0：两拒绝行先被要求execution资格，两执行行错套generic F1 binding。audit汇总unknown_count=0不能替代主oracle原行的两个unknown。main定位facts重建覆盖已验证no_obligation为open/missing，以及report拒绝分支顺序／topology submitted-normalized绑定合同接线；Hegel／Archimedes requested medium shadow修复实施中，未完成、未APPLY，不回改原unknown或auditfalse。canonical执行资格与正式report失败分别记账；隔离资格不是host clearance（false）。

待main审查两shadow修复并显式决定APPLY／新源码身份及相关验证；保留两原unknown与4项原audit错误。新F1卡仅Mencius live owner，尚无最终结果，本卡不追扫、不预填pass。

14paths已commit；HEAD=`43031903c8b81044919ca5f27157e26cd3ad984b`、behavior SHA=`fa30e09c0cb7db37ddf895d789046fa09c82f8029395fc47ba5d9101bd572a01`，main核证`source_frozen=true`／dirty_behavior_paths=[]、精确14paths及内容hash一致，CLI全窗source不变。317pass是实际树相关窗口；main344／127／17／53／42等是独立窗口（本卡用户消息来源），不能相加或改写成一次全仓绿；旧4515pass／12skip／1fail及失败历史原样保留。

新真实topology：`rmw_cyclonedds_cpp`、domain186、run_id=`topology-bfs-domain186-43031903`；4selected=2生成拒绝＋2actual，CLIexit0，cycle/report存在。两actual均独立canonical real_ros_execution=true／qualification_errors=[]、stage=judged；合计72native samples complete、32window complete、state projection observed。两行hit=true，但effect_observed分别true／false，不把hit等同全部语义effect。两份raw仅核SHA匹配，未逐事件复审／replay。

自然candidate0、goal active非complete；原四required revision0、catalog SHA=`947757406e7a23662b576b5cf5ab615773f36a83ccdff9a1e19579fe569e4eb6`、72分母不变，new seal=无，新增required成熟格0。


## 2026-11-19 a06c42aa 固定增量（cut 2026-11-19T02:03:40.802680+08:00）

来源：本卡main明确派遣及具名独立review，属于工程核证，不冒充原规格或新增用户范围。八件artifact原字节与SHA固定见[cutoff索引](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/docs-a06c42aa-owner/cutoff.json)，不追随后续结果。

本cut只登记工程剩余，不新增规则／范围裁决：
- 新a06c42aa/e465 frozen正证据限domain187/seed41/Cyclone的2真实qualified/2oraclepass、report4/4有效（[review](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/topology-a06c42aa-domain187-final-independent-review.json)）；保留430旧topology unknown/fail，旧F1六pass/4root2derived不得迁移新source。108pass与real4pass独立窗不作全仓绿。
- 扩面旧operators首32 invalid-root全拒绝（main消息）；保留正常基线64samples与guard。新32/300 profile仅离线22new/5reject/5guard_terminal、0duplicate，600 validator拒绝，尚未live。main消息14请求算子与[固定summary](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/next-breadth-preparation-owner/normal-input-followup/summary.json)13键/32次请求不一致，待main核对；不影响“请求不等真实effect”的结论。
- guard_terminal report形状接线为main静态发现，Archimedes model-terminal shadow修复中，Mencius准备下一批shadow集成；不预填完成。性能[shadow核证](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-frontier-repair-20261118/main/checkpoint-fullgrowth-independent/summary.json)14mainpass/29.6s，32snapshotCPU40.48%与8every-step29.58%分离；every-step32未验证、main未重跑benchmark、未APPLY。须main审查／整合／冻结／相关回归后，再显式分配live owner/domain/run与新GO；不得执行准备命令当证据。

当前live GO已消费、无活动liveowner，cleanup/3host3innerreleased不等hostclearance（false）。原四required revision0、72分母、catalog SHA=`947757406e7a23662b576b5cf5ab615773f36a83ccdff9a1e19579fe569e4eb6`不变；new_required_seal=false、新增required成熟格0、candidate0、goal **active非complete**。 本卡无须新增用户裁决，无新live/commit授权；freeze后不追写后续结果。


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
