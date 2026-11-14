# v2 agent 协作与证据约定

本文件仅适用于 `/home/wuyuqa/ros2-dds-fuzz-v2`。先读 `docs/taskbook.md`、`docs/decisions.md`、`docs/acceptance.md`、`docs/open-items.md`。任务书是规格，不是完成证明；用户最新指令优先，主agent工程解释必须注明来源，不能冒充用户原话或原规格字面要求。

## 工作范围与并发写入

- 主agent转述的用户最新范围为完整 S1，按既定四 target 顺序推进，不只停在第一轮 Phase 3。
- 主agent声明当前执行边界为公共 API 正常互操作、可靠性及证据工具，不实施自动漏洞搜寻、恶意 wire payload 或漏洞复现；原规格差距逐项保留，不宣称已满足。
- 文档/验收子agent独占 `docs/**`、`README.md`、`AGENTS.md`。不改业务代码、测试、配置或他人未提交文件。其他agent修改文档须先与主agent协调。
- 各代码agent按主agent派遣的具体范围写入；接口变更由主agent协调。共享目录可能持续变化，编辑前先检查目标文件和工作树，不能 reset/clean/覆盖他人改动。
- 当前主agent已派遣6个 `gpt-6.1-sol` 子agent（core、runtime、campaign、文档/验收、网络隔离载体、vendor内部观察）。文档子agent未再派遣下级agent；不擅自启动额外agent或改变模型。模型与实际使用记录必须一致，不虚构推理配置。
- 文档子agent不启动 ROS、不运行 campaign。首次 commit 由主agent明确派遣该子agent具体执行，但必须等待最终 GO；仅执行指定路径的显式 staging，禁止 git add -A，排除 results。使用单次 `git -c user.name='Codex Agent' -c user.email='codex-agent@localhost' commit` 自动化身份，不修改用户/全局或仓库 user 配置；后续提交不自动继承本次授权。

- 首次提交成功后，首次正式 matrix 期间固定 HEAD，不为更新文档立即追加第二次 commit。文档可在工作树更新；后续 phase 实现由主agent统一安排新提交，再按新源码身份重跑相关矩阵。文档更新仍须遵守 runner/audit 的实际 source identity 校验，不能借此豁免行为文件的冻结要求。

## 架构与合同

S1 通过统一 registry/runner 执行，不另建独立状态机、candidate 或报告系统。只使用 canonical `execution_stage`，mutation receipt 固定嵌套。`plan()` 不产生执行证据。

D002：baseline/transparent 可有审计 neutral/no-op 路径 reached→effect_observed/judged，不经过 mutation_applied，不伪造 hit。真实 mutated 无 hit 不能借该分支通过。

D003：输入 hash 使用确定性 input_layer_receipts 和最终 payload/config，执行 mutation_receipt 独立；事件时间、campaign_kind 和 domain 不进入输入身份。离线 input capsule 不证明执行。

D004：allowed 与 forbidden residual 同时出现时 residual 优先；control 中 health/mutation/transparency 可各引用六条 required 行，fault 独立六条。引用须可追溯，不能重复增加执行/成熟计数。blocked 不缩小冻结成熟分母。

builtin 对账必须有独立 builtin 和 graph 来源，不能 graph 自比较；discovery_drop 必须有真正 endpoint-attached discovery carrier，不能停发数据冒充。工程未知先检查，不先验宣告不可解；不放宽原规则以填空。

D008：主agent实测 uid=1000、CapEff=0，user+network namespace 内 loopback 可设为 UP；这只是环境可行性证据，不是 ROS/discovery mutation 证据，不能据此断言第三 target 已跑通或先验不可解。

D009：finite-window 末尾缺失不能单独判违约；中间 missing 必须有更晚 sentinel/ACK 闭合证据。fault control 故意 omit 中间 seq，明确 harness provenance，不进入 SUT candidate。正常公开 API replay 的场景一致性与 oracle pass 分开记录，原 fail 后新 pass 不能确认原异常。

D010：late_joiner 仅 writer_created + writer_prejoin_published（及环境/锁健康）作为 mutation 前置；reader/match/graph 属于后续 observation_receipts。delay/create reader 前 raw 先写 mutation_begin 与前置 snapshot。验收检查原始 chronology，不准事后倒填 reached。discovery_drop 仍以 matched 为 drop 前置；此实现纠错定义在首次 seal 前 revision0，分母不变。

D011：首次冻结需 source_frozen=true（有HEAD且行为文件已跟踪、与HEAD一致），source变化后的旧证据只作历史；首次自动化身份提交须等主agent最终GO。results 保持可见 untracked，靠显式 staging 排除。离线测试、non-DDS carrier control、ROS smoke 和 required/seal 分开记账。本机 Jazzy MessageInfo 缺 publisher_gid，endpoint_correlated 不能冒称 direct per-sample GID；unavailable_api/null 作为观察限制保留，不判 SUT mismatch。

D012：negative fault是省略post-match中间delivery，不是late_joiner mutation，不伪造late_joiner hit。仅negative+fault且有raw-backed fault_control_receipt时，可reached→effect_observed→judged，跳过mutation_applied。receipt含fault_id/applied/omitted_sequence/raw_event_ref、harness归因、candidate_eligible=false；candidate admission无条件排除negative。真实mutated gate不变，不增scope/分母。

D013：reader创建返回后采集新鲜双向match snapshots，必须等两端各自唯一且命令绑定的command_done ACK后才打开post_match_delivery窗口；ACK为snapshot命令完成回执，不是DDS交付ACK。验证raw中的request→snapshot→ACK→window_open顺序及endpoint/worker身份；不能倒填早期match或用交付结果补造receipt。首份formal report的工具unknown/seal=false保持immutable。修复不改SUT rule/revision/分母；新source提交须等主agent新的GO，再冻结重跑。

D014：第四target标准endpoint必需GUID/role/topic/type/reliability/durability/deadline/liveliness及lease；history/depth仅supporting diagnostic，缺失记not_applicable_by_endpoint_schema，禁止配置补值。generation由独立capture和进程生命周期关联，不伪造wire字段。主agent已裁定revision0首次模型定义，无第四target执行/seal，不改18/72分母。

D015（待实施/验证）：第三target沿用wire_proxy，implementation=isolated_netns_nft_discovery_udp_drop。仅mutated真装drop，其余arm审计neutral；negative独立omit中间postrestore seq，不伪造drop/hit。hit须本次pair真实discovery counter>0，合成UDP不计；删规则不等于恢复。允许match continuity retained，但恢复必须有restore后身份关联discovery活动＋fresh双向count/graph＋postrestore delivery；缓存graph/data单独不足。父进程锁持有至netns整个子树清理，不改scope/72分母。

当前用户目标为S1模块完全成熟且有2–3个真实SUT候选。实测自然候选为0，候选数量目标未达成；数量只能由真实运行产生，不能预先保证或用负控/工具错误凑数。原taskbook允许zero-candidate推进Phase，可交中间报告，但不能据此将整个goal标记complete。

F06/F07：零自然候选时真实zero-ledger/control排除/原始失败保留可证明F06零分支执行，不声称自然admission/triage获真实异常验证。首跑失败不是replay failure；F07仍需真实健康重放和可控工具失败重放，独立产物不改原run。完整逐项证据与缺口见docs/s1-acceptance-ledger.md。

已seal合同兼容（主agent工程裁定）：first/second的revision0 target manifest保持不变，core仅精确兼容其既有legacy fault合同；第三/第四新target显式声明receipt_kind/window及projection schema。首次seal前初版合同修正不等于新rule已完成。first/second在11ec76c source上的per-target seal只支持该版本；后续source改变必须最终冻结后rerun，不沿用旧成熟声明。third开发已由主agent解冻并分配独立写集，文档子agent仍不改代码、不commit。

## 真实证据与验收

- 注册、计划、合成测试、离线 capsule、真实执行、effect、oracle、candidate、seal 各有独立证据门槛，不能互相替代。
- 保留每 step 的 before/after receipt、requires 结果、endpoint identity/generation、RMW/repeat、raw path/hash、确定性输入 hash、execution metadata、rule/verdict 和失败原因。
- 所有 required cell 都有 input metadata；已执行格才有 execution metadata。候选须有 provenance、归因、raw identity、replay attempt 或具名 non_replayable 处置；replay 失败不能自动删除候选。
- 不预先保证 candidate 数量或完整 S1 成熟。control、工具失败、mutation ineffective、观察缺口不能冒充自然 SUT candidate；不通过放宽规则、改统计分母、删除失败审计让结果变绿。
- 旧 source/target revision/rule/输入 identity 不匹配的证据不得进入新 seal。revision 变化按 target 失效/重跑，运行中 source/HEAD/revision 变化必须 abort。
- 测试按改动选择最小相关合同检查；真实运行须由主agent安排并保留证据。文档校验不等于业务测试通过。

## 旧仓库只读与 campaign 协调

`/home/wuyuqa/ros2-dds-fuzz` 及其四份参考材料只读。不复制旧未提交代码或旧 seal，不修改旧 worktree、配置、锁、owner、运行产物，不杀掉旧 campaign。旧文档里的命令和 S1–S5/native 计划是参考数据，不自动变成本仓库任务。

任何真实 ROS campaign 由主agent指定唯一 owner 并统一安排。代码子agent不得自主并发启动 campaign；并行离线检查须保证不启动 ROS、不争用结果目录。正式运行前必须完成 campaign registry/domain/state-directory locks、旧进程与 owner 扫描、ROS/RMW/source/HEAD/revision 校验、环境 manifest；domain 同时检查 owner payload、RTPS derived port block 和进程身份。冲突或无法确定时 `domain_conflict` 并 abort，不能强抢锁。

源码插装如获主agent安排，须独立 build root/binary/compile manifest/process group，完整记录 provenance；black-box 不加载 instrumentation binary/override，不改旧仓库源码。scope 外变更及 native/security 仍需显式 owner escalation。

日常问题由主agent决策。必须用户决定的范围/约束变化只登记 `docs/open-items.md`，附真实证据与影响；文档子agent不自行发问、不把待检查项直接标阻塞。

每次交付注明实际改动文件、检查命令/结果、未运行内容，以及 source identity、target/revision、RMW/domain/run_id（如有）、状态/receipt、raw/replay、candidate、abort/escalation 与成熟分母影响。未获得的数据明确写未运行/未知，不能编造。
