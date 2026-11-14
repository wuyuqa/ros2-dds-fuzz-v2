# 待核实事项与证据更新

日期：2026-11-14。初始状态不假设阻塞。以下项目来自原规格审阅、主agent工程核对和派遣消息，尚未等同于本机运行结论。主agent要求继续全部 S1 scope；工程未知须经真实检查后更新，不能现在认定不可解。

记录规则：每次更新附检查命令或调用入口、时间、source/target revision、RMW、run_id（若有）、退出码/结果、raw path/hash。未运行写“未运行”，不能虚构证据。只有真实证据证明某条路径当前不满足条件时才记具名 `blocked:*`；保留失败尝试、可继续的路径及下一步。blocked 不能静默减少冻结成熟分母。

## 1. 工程未知

| ID | 事项、来源与现有证据边界 | 下一项可验证动作 / 所需证据 | 协调人 / 状态 |
| --- | --- | --- | --- |
| O001 | builtin target 原合同需独立 builtin + graph 对账；主agent核对发现旧 Fast DDS 公共 API 面 blocked。文档子agent未对新实现/本机复验，不直接继承旧 blocked 结论 | 定位旧 catalog/rule 的独立性、字段和身份合同；检查当前 Fast DDS 公共入口能否产生独立 builtin receipt，对应 graph snapshot/raw/hash；若只有 graph，明确缺少第二来源 | 主agent协调 vendor内部观察/runtime；待真实双源合同核查 |
| O002 | 主agent核对发现 Cyclone 旧 native probe 与本任务 no-native participant 约束冲突 | 核查旧 probe 是否创建 native participant、当前 ROS2 公共/endpoint-attached 只观察路径能否提供合同事实；保留 source identity、participant/endpoint provenance；不得把两个 graph 调用作为两个独立来源 | 主agent协调 runtime；待真实检查，不先验判定不可解 |
| O003 | 第三 target 的 discovery_drop 必须有真实 endpoint-attached carrier；主agent明确不能停发数据伪装发现中断 | 核查实际 discovery/metatraffic 的附着、命中、限定范围、restore 释放及恢复窗口；两 RMW 分别有 carrier admission/逐层 receipt/raw。当前执行边界是否容纳所需 carrier 由主agent明确 | 主agent协调网络隔离载体/runtime/campaign；D008及真实non-DDS控制已证明环境/载体可行，真实DDS发现附着与恢复待核查 |
| O004 | 原 Phase 2 要内部事件/源码插装，Phase 4 要有界发现性验证；主agent当前执行声明限正常公共 API 验证和证据工具 | 主agent逐项声明当前实现内容与原要求的差距；核查能否在当前边界内达到必要观察；不能用 graph 观察冒充内部插装或自动把 bounded 正常验证称为原 fuzz maturity | 主agent；待工程裁定和证据 |
| O005 | 第一 fault_id 固定，但“省略一个 post-match delivery”施加点未定义；其他三个 target 的 fault contract 尚待审查 | 按 D009 固定中间 omit seq、harness provenance、match 前置及更晚 sentinel/ACK 闭合；说明发送/交付/观察施加层，独立 primary oracle 检出已知遗漏；故障注入本身不能伪装 SUT 违约 | 主agent协调 core/campaign/runtime；D009/D012已裁定，主agent报告negative集成已通过，raw事件绑定及真实六行仍按各自产物验收 |
| O006 | source_frozen 检查已实现、依赖锁及源码overlay已有；主agent最终GO已确认代码freeze，实际HEAD见git log及运行manifest | 按已收到的最终GO及D011单次自动化身份显式staging/commit，再核对HEAD及相关行为文件与HEAD一致，旧开发evidence只保留历史 | 主agent；待冻结，不假设当前已阻塞 |
| O007 | 四 target 的双 RMW 表达性、环境可用性和 applicability 不能靠安装清单推定 | 两 RMW 分别检查真实 endpoint creation、rule applicability 与 carrier receipt；不可用具名记录并保留原 18 格预期缺口，不删分母 | 主agent协调 runtime；待真实检查 |
| O014 | runtime核实本机 Jazzy MessageInfo 无 publisher_gid，direct per-sample GID 未达成；smoke 使用 endpoint_correlated | 保留 unavailable_api/null 与关联依据，按 target/rule 裁定 identity/generation 证据充分性；不能判 SUT mismatch，也不能伪称直接 GID | 主agent协调 runtime/campaign；观察限制已确认，验收影响待按规则核查 |
| O008 | registry/domain/state-dir 三锁与旧仓库 owner 排除、derived port probe 还未由文档审计 | 使用真实 owner payload、端口 bind/probe、活动进程的 domain/run/state/source 身份交叉检查；不确定必须 domain_conflict/abort，记录失败而非绕过 | 主agent协调 runtime；待真实检查；本子agent不启动 ROS |
| O009 | D002 neutral 阶段分支、D003 输入/执行 receipt 分离、D004 residual 优先及计数复用已裁定，尚需统一接口 | core/campaign 合同覆盖 neutral 不伪 hit、mutated 无 hit 不能越关、allowed+residual 不短路、同输入换域/换 campaign hash 相等、control 引用可追踪且不重复计数 | core/campaign；已裁定、待实现验证 |
| O010 | 第二/三/四 target 的 primary rule、expected effect、observation/control contract 和 window 参数不能只替换 target 名 | 核对每 target 语义、message/QoS、post-match/recovery 窗口、身份/代际、两个独立源等字段，提供合同测试与真实 receipts 映射 | 主agent协调 campaign；待逐 target 核查 |
| O011 | zero-candidate 情况下 F06 真实 pair 生命周期集成如何满足尚需定义 | 区分离线合成生命周期合同与真实 run 的零候选审计；主agent确定证据连接方式，不将 control/fake 作为自然 SUT candidate | 主agent协调 core/campaign；待工程裁定 |
| O012 | 原规格没有正式定义候选去重键、状态更新审计格式和证据保留策略 | 固定 stable identity、更新历史、重复 raw 的归并规则；保留 rejected/deferred/replay failure，不能靠去重或裁剪抹掉责任证据 | 主agent协调 core/campaign；待接口审查 |
| O013 | 现有run_s1.py/verify_s1.py及源码overlay已可定位，两个help已验证；自动replay CLI未声明 | README已按当前脚本参数更新；首次冻结后由主agent协调真实运行并报告结果，不能把help成功等同矩阵通过 | 主agent协调所有代码agent；入口文档已更新，冻结/真实矩阵待证据 |

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
