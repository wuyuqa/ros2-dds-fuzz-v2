# S1 变异面扩展实施计划：可派工版

- 日期：2026-11-16（初稿）／2026-11-17（执行状态更新），Asia/Shanghai。版本：review-v2-executing。
- 状态：**F1/F2 已获用户批准开工**；F3–F7 属后续成熟范围，**尚未冻结逐阶段预算、接口与验收矩阵**。本文是工程计划与派工依据，不是完成证明。
- 来源分工：变异面清单、任务卡、测试矩阵、配额与运行预算均为主 agent 工程提案，不是原任务书字面要求；用户批准的是“开始执行目标”并以 F1/F2 起步，不等于批准 F3–F7 立即并发实现。
- 本文取代此前大计划作为当前工作包；原 P0–P7 仅保留长期路线。
- 直接目标：将已跑通的 S1 执行链改造成**有初始 corpus、真实 mutator、有界组合链、状态反馈、持续生成及候选独立审核**的 fuzzer。不是重建通用平台，也不是再增加几套固定参数矩阵。

## 执行状态（2026-11-17）

| 项目 | 当前状态 | 证据／限制 |
|---|---|---|
| 用户批准 | 已批准“开始执行目标”，以 F1/F2 为首个交付包 | 用户消息；批准执行意图，不等于批准 F3–F7 预算或全部并发 |
| 子 agent 模型 | 本轮**指定 `deepseek-v4.1-flash-1017`** | 取代此前 `gpt-6.1-sol` 表述；实际调用须以调度工具回执记录，未取得回执前不得声称已使用 |
| 本轮已派工 | 4 个 `deepseek-v4.1-flash-1017` 子 agent（离线最小切片）＋文档／验收 owner | 仅记录离线切片范围；尚未有代码落盘，未跑 ROS，未 commit |
| 离线切片 A（core） | T01 最小 schema fixture ＋ T03 单算子纯函数骨架（boundary／enum／range） | 离线；无 runtime 绑定；状态：离线最小切片实施中 |
| 离线切片 B（semantic／observer） | T02 目录／seed 模板与 fact／rule 引用清单（先目录与校验，不接真实执行） | 离线；模板不等于已执行；状态：离线最小切片实施中 |
| 离线切片 C（campaign） | T04/T05 只读接线评估与最小 corpus／反馈接口草案 | 离线；不改既有 runner 行为；状态：离线最小切片实施中 |
| 离线切片 D（runtime／network） | T06 单窗口环境变异的离线接口评估（复用现有控制器，不新增真实能力） | 离线；不启动 ROS／namespace；状态：离线最小切片实施中 |
| 实施阶段 | 离线最小切片实施中 | 尚无代码落盘；T07–T10 未开始 |
| 真实 DDS 新能力 | 0 | 新 target、新环境变异、多窗口、G01 均未实现、未执行 |
| 自然候选 | 0 | 历史候选亦为 0；不得用负控、工具错误或计划条目凑数 |
| 新 seal | 未产生 | exploratory 无成熟矩阵，不得借用 required `sealed=true` |
| F1/F2 真实运行 | 未运行 | 运行须主 agent 单独 GO、唯一 owner、锁与 source 检查 |
| F3–F7 | 已列范围，未冻结 | 预算、接口、验收矩阵与 scope 变更仍需逐项冻结 |

> **范围补充（2026-11-16 提出，2026-11-17 状态更新）**：用户提出将多 endpoint、生命周期、发现时序、持续 episode 等纳入成熟路线。F1/F2 保留为首个交付包，不再代表整个成熟路线；参见 [S1 扩展范围与成熟版本判定](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-expanded-scope-and-maturity-2026-11-16.md)。下文“暂缓”仅指不阻塞 F1/F2，不是永久排除。F3–F7 的预算与接口尚未冻结，也未启动实现。

### F1 真实探索扩展（source 4b7f7ea/5907b0c）

- 真实 late endpoint 覆盖 4 个 authored 变体（0.001/0.01/0.15/quantized），CycloneDDS 与 FastDDS 各 4 条全部 `judged`/oracle pass；raw 保留在 `results/fuzz-cli/late-final-*`、`late-fastrtps-real`。
- 真实 late join 覆盖 publish interval 变体，CycloneDDS 与 FastDDS 均有 `judged`/pass；W02 zero 因没有精确 M01/special-value 支持，已从 real 变体中移除，不以 0.005s 冒充 zero。
- 公开结果中自然 candidate 仍为 0；正常路径不伪造候选，corpus 对 `success_only` 明确拒绝。
- projection/recovery fuzz_v1 的 `execute=True` 仍为 planned-only：runtime admission 未获 live raw，不打开。
- 已将 scheduler 的覆盖维度加入 `variant_id`，warmup 先跨变体交错；M03 对宽域保留范围采样，对窄域做父值邻域，避免把 authored 变体压成一个值。
### 2026-11-17 真实 F1 与 legacy 基线（source e6af301）

- 真实 F1 late endpoint 两条通过：`fuzz-cli-late-007`、`fuzz-cli-late-008`，均 `judged`、`actual_run=true`、mutation receipt 七项真、oracle pass；raw 分别在对应 `results/fuzz-cli/.../runtime/raw.jsonl`，SHA256 见 cycle/row。
- 先前的 `late-002` 至 `late-006` 失败 run 全部保留：包括 qos observer race、0-delay no-effect、binding_event 下标 bug；修复后重跑，不删除/不覆盖原失败。
- 当前 source `e6af301` 的 legacy baseline：projection 17/18 required、1 unknown（`rmw_fastrtps_cpp.transparent.0`），recovery transparency 5/6、1 unknown（`rmw_fastrtps_cpp.transparent.2`），fault 均 6/6；seal=false，原因均为 FastDDS environment/process，非本轮 fuzz 代码新增。
- projection.fuzz_v1 与 recovery.fuzz_v1 的 `execute=True` 仍为 planned-only：runtime 无可用的 fuzz_v1 live raw；不得开启 `execution_admitted`。真实 F1 当前只覆盖 late endpoint/late join 参数面。
- 这些结果不改变 required 72/24 分母，不构成新 seal；只有 F1 探索闭环与受限真实 carrier 通过。

## 执行进展与真实执行阻断（2026-11-17）

- 离线 F1 闭环已打通：profile/seed→corpus→scheduler→mutator/plan→v2 capsule→carrier→fake runtime→ingest→feedback→corpus。全仓离线回归实测 **733 tests, OK, skipped=1**（skip 仍为既有 `R2F_PROVIDER_AUDIT_REPORT` fixture）。
- 12 份 seed 均真实加载且通过 schema；F1 可路由面为 P01–P06、E01、W01、W02；E02/E03/G01 仍为 F2/capability pending。旧四 target manifest 与原 catalog hash `947757406e7a…4eb6` 未变。
- 三类 exploratory 路由已生成 planned 且配置绑定 `preserved`：late_endpoint/late_join、projection P02–P06（真实 QOS 字段）、recovery E01（单窗 drop_duration_sec）。projection/recovery 的 `execute=True` 当前仍显式返回 `unsupported_capability:execution_backend_not_admitted`；没有伪造 judged/候选/seal。
- F1 **尚未完成真实 DDS 执行**：本机 `CapEff=0`，netns/nft 只具受限环境证据；当前工作树 `source_frozen=false`（新增 fuzz 行为文件未入 HEAD）。必须先完成主 agent 代码审查、显式提交 GO、冻结 source，并由唯一 campaign owner 运行，旧 72/24 回归与 raw/seal 才可重算。离线 fixture 或 planned 行不能替代真实执行。
- 需要主 agent 继续裁决：runtime admission 的最小证据门槛；projection/recovery 的 live preflight/lease/observer 能力；多窗口 E02/E03 与 G01 仍留 F2。



- 已按用户批准开始执行；实际派遣子 agent 使用 `deepseek-v4.1-flash-1017`，未替换模型。
- 已完成离线最小切片：可选 exploration contract、MutationSurface/FactSpec/RuleSpec/MutationPlan、M01–M03 纯 mutator、固定 pair 状态签名、corpus/feedback/20-40-40 选择、12 面 catalog（10 核心+2 辅助）、P01/E01/G01 三份 seed、G01 oracle、exploratory target/provider 注册、v2 输入身份与逐层 receipt、exploratory 报告分派和 review packet。
- 全仓离线回归实测 `647 tests, OK, skipped=1`（skip 仍为既有 `R2F_PROVIDER_AUDIT_REPORT` fixture）；旧四 target manifest 与原 catalog hash 未变；`git diff --check` rc0。
- 能力边界：F1 可路由 surface 为 P01–P06/E01/W01/W02；E02/E03/G01 仍为 F2 pending；M04–M08 未实现；P02–P06/W01/W02/E02/E03 的 seed 仍 pending；runner 探索闭环仍在实施。
- 尚未启动 ROS/campaign/nft，未产生真实新执行、自然候选或 seal；已完成项均属离线合同/夹具，不得当作 DDS 或 SUT 证据。

### E01 probe 实验结论（45a3e19）

- 在 drop 窗口内创建独立 probe participant 可以产生真实 discovery 公告；raw 中 `discovery_window_counter.hit=true`，说明 counter 机制可用。
- 但恢复后 `discovery_activity` 仍要求覆盖原 writer/reader 双方；probe 不属于主 pair，不能替代该证据，因此 adapter 正确拒绝 `does not cover both matched participants`。
- probe 默认关闭（`discovery_probe=false`），不参与正式 F1/F2 结果；它只是诊断/未来 F3 的载体。
- 结论：E01 要在单窗内真实命中并恢复，必须让主 pair 在窗口内重新发现（endpoint 重建/新 participant 加入主 pair），这是 F3 范畴。不能把 probe 当主 pair 活动或候选。

## 阶段推进：projection/recovery 真实准入（4d63912）

- 已用真实运行把 projection.fuzz_v1 与 recovery.fuzz_v1 从 planned-only 推进到可执行：
  - projection：P02–P06，含 best_effort、deadline/lease 的 zero 与 infinite；CycloneDDS/FastDDS 均产生 judged raw，`qos_binding_observed` 两角色齐全，`projection_observation` 存在。
  - recovery：E01 单窗真实 `completed`，window ledger、restore boundary、recovery confirmed 均有 raw；但稳定 match 后 3s 窗口内 discovery counter 仍为 0，mutation currently ineffective，非 SUT 缺陷。
- 修掉的 runtime/adapter 真实问题：projection manifest 误报、duration 0=INFINITE 等价类、FastDDS depth/history 观察限制、projection adapter 分派、mutation ineffective 分支、recovery window_hits 未初始化、emit event 键冲突、restore_mono_ns 作用域。
- 当前真实 judged 累计 68 条（三类 target、双 RMW、多 surface/极值），自然 candidate 仍为 **0**。
- E02/E03/G01 仍不可执行：G01 缺独立关系观察；E01 窗口内无新 discovery 流量，需 F3 的 endpoint 重建/新 participant 来产生真实命中。
- 不把 `mutation_ineffective`、FastDDS 观察限制、zero=INFINITE 等价归为 SUT 候选；不打开未验证的多窗口执行。

## F3 最小真实能力：reader 重建恢复（0bb6780）

- 在 drop 窗口内启动第二 reader 代际（`reader_recreate_in_window`，profile 显式启用），产生真实 SEDP 刺激；nft discovery counter 命中 true，恢复后主 pair activity 成立。
- 真实结果：`fuzz-cli-rec-recreate-009` judged/pass（CycloneDDS）、`fuzz-cli-rec-recreate-010` 两条 judged/pass（CycloneDDS + FastDDS）。
- 身份处理：原始 writer/reader 仍是交付与 binding 合同；reader2 作为显式附加代际记录在 `facts.recreated_reader`，`pair_member=false`，不冒充旧 reader。
- 修正：drop 窗口内不再要求主 pair discovery activity 覆盖双方（drop 的预期效果就是 discovery 不可见）；恢复后 activity 仍必须覆盖主 pair 双方。
- 该路径是 F3 最小闭环，不是通用多 endpoint/反复重建平台；E02/E03 多窗口、G01 关系守卫仍 pending。
- 自然 candidate 仍为 0；所有 reader 重建结果都是正常 pass，不得当候选。

## 当前冻结身份与阶段结论（af90c13）

- HEAD `af90c130…`，behavior `116f52ae…`，`source_frozen=true`，`dirty_behavior_paths=[]`。
- F1 真实闭环：late endpoint 4 个 authored 变体在 CycloneDDS/FastDDS 各 4 条 judged/pass；late join 4 条 judged/pass；raw 保存在 `results/fuzz-cli/`。自然候选仍为 **0**，没有伪造候选或 seal。
- F2 仅 generation-only：M04–M08 与 E02/E03/G01 可生成计划，`execution_admitted=false` 未改；W01 关系类未进入 F1 封合同，避免改变 contract hash。
- 全仓离线回归 **807 tests, OK, skipped=1**；旧四 target hash 与原 catalog hash `947757406e7a…4eb6` 未变，catalog digest `28dff571…47c6`。
- 尚不能称 S1 完整成熟或有 2–3 个真实候选；未完成项是 projection/recovery fuzz_v1 的真实 live、E02/E03/G01 的执行。

## 0. 阅读与批准入口

本文按“交付范围 → 变异面 → 数据合同 → 执行机制 → 派工 → 测试 → 运行预算 → 候选审核”组织。

用户 2026-11-17 批准开工的对象是本轮 **F1＋F2**：F1 做可持续变异闭环；F2 在该闭环上补多窗口环境扰动、参数×环境组合、受限守卫测试。两者均完成才称本交付包完成，不能只交 F1 参数扫描就宣称已满足。多 endpoint／churn、高吞吐、多前缀通用规划、快照和大规模论文实验不纳入 F1/F2，但保留在 F3–F8 成熟路线中另行冻结。

用户整体目标“S1 完全成熟且 2–3 个真实 SUT 候选”不变；F1/F2 交付完成不等于整体目标完成，零候选如实保留。候选发现数量不能预先保证；F3–F7 完成后仍须按 [范围文档](s1-expanded-scope-and-maturity-2026-11-16.md) 的 M1–M6 冻结验收，才可讨论版本成熟。

## 1. 已核实的起点与必须补的断点

核查源码 HEAD=`e448febec4fa9f3c24f22eaac7954517e7b4a301`，behavior=`e7f92897dd6d122a874f7686a24419173d110abd765106ce5e16e1289e732cfa`。本轮此前已只读重算 source_frozen=true、dirty_behavior_paths=[]；开工与运行前还须重新检查。

| 断点 | 已核实位置 | 本轮必须改变什么 |
|---|---|---|
| slot 只标种类／载体，缺生成合同 | `core/semantic_target.py:MutationSlot`、`modules/s1/targets.py` | 变异字段、值域、边界、算子、组合条件、fact/rule 引用可被程序读取 |
| seed 仅进入元数据 | `core/scenario.py`、`campaign/runner.py:plan_campaign` | corpus seed 与 RNG seed 分离；RNG 真正影响展开输入 |
| provider 只包装固定参数 | `modules/s1/providers.py:S1MutationProvider.prepare` | 新路径实际应用合格算子，形成确定性 mutation plan |
| 单层说明，没有组合执行器 | `Scenario.effective_input`、provider 返回 `[layer]` | compile-time patch 链与 runtime 环境步骤均可验证，并区分二者 |
| 状态只有固定步骤与回执 | `core/state_machine.py` | 提取观察状态／迁移签名，用于 corpus 留存与选择 |
| 固定矩阵控制整个执行 | `campaign/scheduler.py`、`runner.py` | 同 runner 中新增预算驱动 exploratory 策略，逐输入接收反馈 |
| 报告／审核／重放假设固定 target 与 24 格 | `reports/run_report.py`、`campaign/replay.py`、CLI | 按版本／campaign_kind 分派审计合同，保留旧 required 审计 |
| 候选不能只靠自动 fail | `core/candidate.py`、`reports/candidate_report.py` | 工具准备包，独立审核 agent 给依据，主 agent 裁决 |

已完成的 source/raw/hash/identity、真实 carrier、facts/oracle、replay、四 target 控制不推倒重写。旧 72 required＋24 negative 是回归基线，不是新的 fuzz 输入空间。

## 2. 范围：两次可见交付，四类空间

### 2.1 F1：现有 runtime 上的最小 fuzz 闭环

实现：

1. 变异面／fact／rule 合同与 12 份初始结构化 seed。
2. 6 类参数算子；先单算子，再兼容的参数×参数组合。
3. 一个实际调用 mutator 的生成循环、corpus 血缘及语义去重。
4. 固定 pair 的小型状态／迁移反馈，不做通用状态规划。
5. 将 existing discovery drop 的时长作为环境变异接入该循环，不仅是手工参数覆盖。
6. 同 runner 的 exploratory 审计、capsule/replay 和候选证据包。

F1 必须有新生成输入的真实执行，不能只交离线 generator。

### 2.2 F2：环境序列与跨层组合

实现：

1. 1–3 个 discovery drop→restore 窗口，每窗口有独立触发、计数差分、恢复证据。现有 DiscoveryDropController 已注明支持后续窗口，本任务优先复用并验证它，重点补 runner/runtime 编排和逐窗事实，不重复开发底层 nft 控制器。
2. 以“匹配已确认”或“上一窗口恢复已确认”为事件锚点的启动偏移；保持工具健康门槛。
3. late-join 参数／有限 QoS 配置参数与 matched 后环境窗口组合。
4. 一个边界清楚的守卫违例族：**合法 endpoint 配置导致 matching compatibility=false**；不把它冒称 create API 非法，也不要求 write API 必须拒绝。
5. 每个组合的真实层级证据、独立控制校准、状态反馈及 replay。

### 2.3 不纳入 F1/F2，但纳入后续范围评估

- 多 endpoint、反复 endpoint create/destroy、lease expiry/renewal、ContentFilteredTopic、mixed-vendor。
- wire delay/reorder/duplicate、随机畸形报文、native participant。
- 同进程持续 episode、通用 ActionModel、自动前缀规划、分布式快照、高吞吐压力平台。
- 大型 B0–B3／消融研究。F1/F2 只验证 fuzz 机制和变异覆盖，不提前投入约 30 小时论文对照。

不以暂缓项从“完整 S1”地图消失来宣告成熟。这里暂缓的是工程排期，不是断言它们无价值。

## 3. 变异面清单：输入、真实施加、事实和规则一起定义

### 3.1 分层与能力状态

- P：配置／时间参数；E：真实运行环境；G：关系守卫；W：辅助 workload。工具 timeout、observer interval 是 H 类（harness），固定预算或校准，不算 SUT 变异面。
- 每个 surface 记录 `declared → generatable → executable → hit_verified → oracle_qualified` 的**能力证据**。这是覆盖账标签，不是第二套 execution_stage。
- 下列数值均为 F1/F2 拟定工程采样预算，不是 DDS 规范上下界。取值必须在实际 SDK／RMW 能力确认后冻结；不支持值保留显式结果，不能静默截断。
- 0、无限值及不适用值分成独立语义类别，不凭数值邻近推断含义。测时参数实际精度有限；边界±1 的生成只证明输入不同，未有观测前不证明 runtime 时序可分辨。

| ID／交付 | 输入维度和初始采样域 | 算子／组合 | 真实施加与观察 | 判定边界 |
|---|---|---|---|---|
| P01／F1 | late reader 延迟：0、1ms、10ms、20ms、150ms、1s、3s，另在[0,3s]按1ms单位抽样 | boundary、range、relative_interval | create 调用前后／writer prejoin/raw 顺序 | 只在实际目标前置满足时判原交付合同；0延迟仍需真实 chronology |
| P02／F1 | projection reliability：reliable、best_effort；先两端同值 | enum_replace | 配置提交、endpoint实际设置、独立 wire＋graph | 不把可靠交付规则套给best_effort；此处主要判字段投影 |
| P03／F1 | projection durability：volatile、transient_local；先两端同值 | enum_replace | 同上 | 不新增“必须收到全部历史”的未经审核断言 |
| P04／F1 | deadline_ns：0、1、999999999、1000000000、1000000001、1250000000、有限采样、INFINITE_NS | boundary、special、range | 公共API转换、wire／graph实际值 | 0／无限语义、量化／默认展开按规范和适配器确认；创建拒绝分层记录 |
| P05／F1 | liveliness：automatic、manual_by_topic | enum_replace | 两来源字段观察 | 不宣称已测试lease到期或主动assert行为 |
| P06／F1 | lease_duration_ns：与P04相同的类别，另有短／中有限值 | boundary、special、range | 实际QoS＋独立观察 | lease配置投影≠lease生命周期覆盖 |
| W01／F1 | depth：1、2、3、8、32、64、65、128；pre_count：1、2、8；post_count：3、6、32、64、65、128 | relation_boundary、range | 实际QoS／send／receive序列 | depth不是标准SEDP必需字段；不冒充wire验证，超过history预算时原交付规则必须重新审核适用性 |
| W02／F1 | publish_interval：0、1ms、10ms、30ms、100ms | enum／range | 实际publish事件间隔和send数量 | 不叫高吞吐、不据配置频率推算实际吞吐 |
| E01／F1 | 单次discovery drop时长：50ms、200ms、1s、3s、8s | boundary／log_range | 真实pair绑定＋该窗口counter差分 | 短窗无包为ineffective，不造hit；drop影响本身不是bug |
| E02／F2 | drop窗口数：1、2、3；restore间隔：0、20ms、200ms、1s | sequence_count／interval | 每周期独立begin/end与raw；下一窗只能在上窗恢复证据后启动 | 不把删规则等同恢复；间隔0也不能跳过恢复检查 |
| E03／F2 | 触发：fresh_match或previous_restore_confirmed；锚点后0、10ms、100ms、500ms | phase_offset | 锚点event_ref＋实际drop_begin | 不以定时sleep冒充已达状态；暂不做pre-match drop |
| G01／F2 | offered/requested reliability两端独立：RR、RB、BB、BR（R=reliable，B=best_effort） | pair_relation | 独立创建配置／图证据，fresh单pair身份关联的实际匹配／delivery | BR是不兼容关系，不是非法API参数；无匹配只作窗口内安全观察，不能证明永久无匹配 |

核心维度计数为6项P＋3项E＋1项G，共10项；另有2项W辅助维度，不能以“12个核心S1变异面”宣传。G01是配置诱导的关系守卫false，不是完整的“先到达guard=true，再在运行中改变外界令其false后执行任意动作”的通用环境守卫破坏器；后者仍未纳入完成声明。

P01/P02/P03/P04/P05/P06 的字段位置、单位和 capability 必须逐一声明；不能合并成一个“qos fuzz”数字掩盖未接通项。W01/W02 用于组合刺激，单独报告，不冒充新增核心 S1 语义面。

### 3.2 非法值路径的限度

增加 `parameter_class=valid / semantic_invalid / representation_invalid / unsupported`。第一轮运行真实非法输入须满足：

1. 原始参数可由声明的 carrier 到达被测公开 API；工具可安全承载与语义合法性校验分开。
2. 对预期拒绝有准确 API／版本依据；异常的责任层与副作用可观察。
3. `negative` 仍是工具故障控制，不能拿来表示非法输入测试。

初版必做非法值**生成／路由测试**：数值边界-1/+1、错类型、缺字段和枚举外值。若在工具／SDK处被拒绝，明确 `rejected_at=harness|binding`；不计DDS有效变异，也不作为SUT候选。真正到达DDS的非法值尚待逐面可表达性检查，不能承诺数量。G01 是F2必须落地的关系守卫路径，不以这些离线非法值代替。

## 4. 初始 corpus：不是一个随机整数

目录建议：`config/s1_fuzz/seeds/`，纳入 source identity。以下12份是**待创建模板**，不能标为已有真实执行证据。

| seed_id | target族／展开配置 | 目的 |
|---|---|---|
| L01 | late_endpoint，reliable/volatile，delay150ms，pre2/post6/depth64 | 现有健康基线形状 |
| L02 | late_endpoint，同L01，delay0 | create顺序边界 |
| L03 | late_join_delivery，delay20ms，pre2/post6/depth64 | 短前缀 |
| L04 | late_join_delivery，delay150ms，pre8/post32/depth64 | 有界workload组合种子 |
| Q01 | projection，常规基线；deadline1250000000ns变体 | 已有QoS形状 |
| Q02 | projection，best_effort变体 | reliability枚举 |
| Q03 | projection，transient_local变体 | durability枚举 |
| Q04 | projection，manual_by_topic变体，lease有限值 | liveliness／lease组合；先验证API支持 |
| D01 | recovery，drop200ms一次 | 短窗口，允许无hit |
| D02 | recovery，drop3s一次 | 已有环境形状 |
| D03 | recovery，drop1s两次，恢复确认后再进入 | F2多窗口，F1不可路由 |
| G01 | compatibility，writer best_effort／reader reliable | F2关系false |

每份含：`corpus_seed_id`、`schema_version`、`target_id/revision`、`contract_hash`、`logical_config`、`operation_windows`、`provenance`、`capability_requirements`。来源使用既有配置或公开规范，不直接把旧raw／run身份当新执行；从历史输入导入必须去除真实PID/GUID/domain等执行标识。

F1加载可执行的前10份；F2激活D03/G01。静态seed预验证与真实执行资格分开；新运行需要新run_id/source/raw，不能复用旧seal。

身份分三种：

- `corpus_seed_id`：初始／留存输入身份。
- `rng_seed`：决定抽样，明确使用稳定的生成器算法和版本。
- `parent_content_sha256`：父输入语义指纹；与完整input identity分开。

`content_sha256`由最终配置、确定性操作窗口／顺序、合同及RMW profile计算，排除随机seed、repeat、文件路径、wall time与真实GUID。两次生成同一内容仍保留两条attempt日志，但corpus只留一个内容条目；相同最终配置但不同实际环境操作计划不合并。

## 5. 模型与数据合同：字段、引用、兼容方式

### 5.1 复用结构，不新增通用 ActionModel

仍以 SemanticTarget、MutationSlot、StateStep 为入口。新增版本化 `exploration_contract` 是**可选扩展**：只在新exploratory target中序列化；原四target的to_dict字节级canonical内容保持不变。不能给旧target自动补空字典导致旧manifest hash变化。

拟新增探索target（revision=0）：

1. `discovery.late_endpoint.fuzz_v1`
2. `discovery.late_join_delivery.fuzz_v1`
3. `builtin.discovery_projection_reconciliation.fuzz_v1`
4. `discovery.interruption_recovery.fuzz_v1`
5. `discovery.match_compatibility.fuzz_v1`（F2）

原 EXECUTION_ORDER／72分母／原四target manifest不改。新target通过同一registry机制显式启用，required默认注册／catalog仍只含原四target，exploratory才加载扩展集合；否则即使四target本身未变，整体catalog hash仍会变化。required审计保持原集合，exploratory按声明的集合审核。resolve_services和replay也须按明确schema/profile选择注册集合，不能盲目全量注册导致旧catalog对账失败。禁止在runtime偷偷把new target_id改写成old target_id以绕过检查；使用显式 `runtime_capability` 映射执行后端，输入／执行身份始终是真实新ID。

### 5.2 MutationSurface 定义

| 字段 | 约束／消费者 |
|---|---|
| `surface_id, version, source_refs` | 唯一、版本化；规范引用与工程预算来源分开 |
| `input_paths, value_type, unit` | 指向真实可消费配置／操作字段；无runtime binding则不可宣称可执行 |
| `domain, special_values, boundary_classes` | 明确范围、特殊值与等价类；供生成器使用 |
| `mutator_ids, value_relations` | 必须能解析到注册算子和约束；未知引用拒绝加载 |
| `tool_requires, sut_guards` | 工具门槛不能破坏；SUT守卫有true/false/unknown |
| `carrier_id, runtime_phase, runtime_binding` | 配置时或事件锚点执行；关联能力清单 |
| `allowed_with, conflicts_with, order_after` | 组合白名单、冲突和偏序；不可凭字符串拼接组合 |
| `fact_ids, rule_ids, effect_id` | 必须引用存在的事实／规则／效果 |
| `missing_policy, unsupported_policy` | unknown／拒绝的原因明确，不能默认pass |

扩展可内联在新MutationSlot，也可引用S1目录中的共享定义；实现必须选择一种规范化表示，hash使用解析后的固定合同，不让同一引用在运行中可变。

### 5.3 FactSpec 与 RuleSpec

每个 FactSpec 至少：`id/version/value_type/unit/scope/sources/producer_id/freshness/identity_binding/missing_policy`。scope明确run、endpoint、window、sample；来源独立性写进合同。

本轮必需事实族：

- submitted 与实际observed endpoint QoS／创建身份；
- writer_prejoin、reader_create前后顺序；
- 每窗口drop counter_before/after/delta、pair绑定、drop begin/end；
- 每窗口restore后新鲜discovery activity、双向graph/count、闭合delivery；
- 参数／环境层actual effect和override/no-op原因；
- 单pair匹配证据、observer健康／完整性；
- 辅助send/receive序列及窗口闭合。

每个 RuleSpec 至少：`id/version/spec_refs/applies_when/required_fact_ids/evaluator_id/allowed_effects/forbidden_residuals/limitations/responsibility_layer/primary_or_supporting`。

Oracle保持注册Python evaluator，不新增表达式解释器。加载时校验所有引用、evaluator签名、事实类型与缺失策略；审计器从raw重新produce/evaluate，不能只相信报告里写了pass。

**G01的oracle**：判“观察窗口内是否出现有充分身份关联证据的禁止匹配／交付”，不是判“是否在8秒内完成所有发现”。若窗口完整且未观察到禁止事件，可输出该窗口安全属性pass；若无法观察实际关系，unknown。预期兼容pair一直未匹配不能只因超时判DDS违约。

### 5.4 生成与执行记录

`mutation_plan`输入侧记录：`plan_version / rng_algorithm / rng_seed / parent_content_sha256 / operations[] / final_config / runtime_windows / contract_hash`。

每个operation：`op_id / surface_id / mutator_id / parameters / input_before_sha256 / input_after_sha256 / application_phase / applicability_result`。

执行侧另存 `mutation_step_receipts[]`：每步有`op_id`、原七字段嵌套`mutation_receipt`、before/after raw refs、actual_effect、reason。总体仍只有canonical execution_stage和总体嵌套MutationReceipt，不改旧七字段语义。

compile-time的P02→P04若只在最终一次create时提交，不声称中间QoS也经历过DDS；两个参数的命中都必须由最终值证据支持。若后一步覆盖前一步则记overridden，不双算hit。

runtime窗口则必须逐步有真实before/after／hit证据。总体mutation_applied要满足该输入声明的必需施加步骤，不能最后一层有hit就把全链变绿；effect判定与guard拒绝的成功检查分开处理。

### 5.5 输入schema与旧证据兼容

- 原capsule/input-identity版本读写保持；新路径显式使用版本化schema，不能让新字段未进入hash。
- model、surface、mutator版本、展开计划、输入与窗口定义全部绑定；执行时间和真实ID不进入确定性输入。
- replay加载保存的final input与操作计划，不重新运行随机mutator，不因当前corpus变化改输入。
- source manifest需覆盖新增config/seed/model与行为CLI；当前只监控src/config及setup脚本不够覆盖新增CLI行为，须显式扩展参与文件清单并版本化审计。旧报告继续按原source策略核验，不回写旧hash。

## 6. Mutator 与组合器：明确算法，不靠改名字

### 6.1 第一版算子

| mutator_id | 具体行为 | 验证点 |
|---|---|---|
| M01 boundary_pick | 从声明的min/max、邻点及语义边界代表值轮换；整数±1，时间用声明量化单位 | 不把工程预算边界当规范边界；特殊值独立 |
| M02 enum_replace | 选择与当前不同的合法枚举；允许invalid测试时由单独类别输出枚举外值 | 全枚举可达；单值域报告no-op |
| M03 range_sample | 从有界整数域均匀或log桶抽样，再量化；策略随seed/version冻结 | 固定RNG下可重建，不生成NaN/不可序列化配置 |
| M04 relation_boundary | 对post_count与depth等已声明关系取小于／等于／大于；同时输出rule适用性 | 触达关系边界，不错误沿用delivery义务 |
| M05 special_value | 从zero、finite、infinite等声明类别选择 | 不是任意超大值代替INFINITE；实际转换可追溯 |
| M06 timing_offset | 改late创建时刻或锚点后偏移，记录单位及事件锚点 | 配置差异与真实时序差异分开统计 |
| M07 environment_windows | 变异窗口数、每窗时长、恢复间隔、触发锚点 | 真施加，逐窗口counter差分和恢复证据 |
| M08 compatibility_pair | 枚举offered/requested关系并生成匹配guard预期 | 创建成功与匹配成功分开；先只做G01四格 |

F1完成M01–M06及M07的单窗口；F2完成M07多窗口与M08。算子注册项需含id/version/accepted_surface_kinds/mutate/validate，不在provider内部不断堆target_id分支来冒充可扩展算子。

拟内部接口（设计签名，不是现有API）：

- `mutate(input, surface, rng, context) -> MutationProposal`：纯函数，不访问ROS／文件／时间。
- `compose(parent, proposals, contract) -> MutationPlan | Rejection`：验证字段冲突和顺序，生成最终input。
- `validate_plan(plan, capabilities) -> ValidationResult`：区分不合法、能力不支持、需真实观察才能判的前置。
- 现有 `MutationProvider.prepare(scenario)` 消费确定plan并返回现有准备信封；不能在prepare里二次随机选择。

### 6.2 七个首批组合配方（全部需独立验收）

| recipe | 组合 | 目标／注意事项 |
|---|---|---|
| C01 | P02 reliability＋P04 deadline | projection；同一次endpoint创建，两个字段均有真实投影证据 |
| C02 | P05 liveliness＋P06 lease | projection；manual模式支持及观察能力先检查 |
| C03 | P01 late delay＋W02 publish interval | late-join；不是并发压力测试 |
| C04 | P01 late delay＋E01 matched后drop | 新recovery探索合同；不得复用旧matched-before-drop前置之外的时间窗口 |
| C05 | W01有界depth配置＋E01 drop | reliable/volatile保持；交付义务适用性检查不能省略 |
| C06 | E01每窗时长＋E02窗口数量 | 2–3个真实drop/restore，逐窗口恢复与counter差分 |
| C07 | E02多窗口＋E03锚点偏移 | 序列相对时序；上窗未恢复则不能执行下窗 |

不允许的首版组合：best_effort＋原可靠交付全量断言；窗口未恢复就再drop；guard未知却强制当false；不同oracle语义仅拼接名字；多次写同字段却声称每次都命中；同时修改observer健康条件以制造guard违例。

第一版每输入最多2个变异proposal；后续仅在交付内已验证的配方上允许3个proposal。一个environment_windows proposal可展开最多3个窗口，每窗口是实际执行步骤，不靠“链长度2”隐藏无界执行。

每次compose后重新验证最终target适用性。默认拒绝冲突并记录reason；不静默修复回合法值。若算子有声明的关联字段补全，必须进入plan及hash。

### 6.3 一个完整的亲子输入例子

- 父：D02，single drop3s，pair reliable/volatile，创建延迟150ms。
- proposal 1：M06将reader delay变成10ms，保存前后input hash。
- proposal 2：M07将环境计划变成matched后drop200ms→恢复确认→20ms→drop1s→恢复确认。
- compose：判两层可组合，生成具体窗口及锚点引用；此时没有任何hit。
- execute：真实create、fresh match；第一窗若counter_delta=0，记未命中，不能补造；恢复证据满足才走第二窗。
- judge：第二窗有hit不能让第一窗自动有hit。部分环境变异生效、共享健康事实及可判定子规则分开报告；总体链成熟不通过，但异常raw仍保留。
- corpus：不因不同run_id/seed纳新；只有真实新关系／迁移、合同边界类或不同有效语义输入才按策略保留。
- replay：用保存的最终配置与两窗口计划，不重跑M06/M07随机选择。

## 7. 最小状态图与反馈，不建设通用规划器

### 7.1 状态定义

对固定pair仅记录下列抽象分量：

`live_roles`、`creation_order_class`、`compatibility_class`、`observed_match_class`、`network_phase`、`recovery_cycle_bucket`、`delivery_closure_class`、`observation_completeness`。

- live_roles：none/writer/reader/both；不把process存活直接当endpoint存活。
- creation_order：writer_first/reader_first/unresolved；当前只生成已支持writer_first。
- compatibility：compatible/incompatible/unknown，来自审核合同和实际配置。
- observed_match：bidirectional/single_side/none/unknown，事实来源和窗口明确。
- network：normal/drop_active/restore_pending/restored_verified。
- cycle：0/1/2plus，用于防止每次周期自然生成无限状态。
- delivery closure：no_obligation/open/closed_within_contract/unknown，不把任何tail缺失当fail。

state_id=canonical_hash(abstraction_version＋上述分量)，去除真实GUID/PID/绝对时间但保留身份一致性检查结果；同一state_id不等同完整DDS内部状态。出现same-state不同后继，保留trace证据，不自动增加规范允许边。

edge_id=hash(before_state_id、operation_family、parameter_class、after_state_id)。执行证据错误、不完整观察不纳“新状态到达”奖励；单独进入诊断队列。

产物放在既有run目录的 `exploration/`：`states.jsonl`、`transitions.jsonl`、`corpus-index.jsonl`、`selection-log.jsonl`。它们是runner报告的附属数据，不形成第二个运行／candidate系统。

### 7.2 Corpus 保留、选择与预算

- corpus上限初版256；初始seed固定保留，候选关联输入只做引用固定，不被淘汰。
- 同content hash去重；按 `target + boundary_class + observed_edge` 组最多保留3个不同代表，优先证据完整且更短／便宜输入。性能更差但产生独特迁移的输入不能单凭成本删除。
- 入库理由：新合同分支、新参数关系类、新可确认状态／边、较短等价复现输入。单纯运行成功不是无限入库理由。
- 调度选择：20%从全部可执行seed均匀选，40%选尚未覆盖的surface×boundary类，40%选稀有已观测edge关联seed；某池空则按固定顺序回退并记录。
- 首轮10个F1种子、12个F2种子的deterministic warm-up不受随机权重影响。
- mutation份额：60%单proposal，30%已审核双proposal，10%未覆盖边界优先；F1没有已验证组合时双proposal份额回退单proposal并记录，不能假装组合已运行。
- 同source/RMW/contract下某配方连续5次工具原因失败，暂时隔离配方，记录诊断，不缩小声明分母；属于预期API拒绝不计工具故障。全局锁／source／observer失效立即中止受影响run。

不预先优化权重，不引入bandit或LLM在线改规则。调度决策由确定性RNG＋已保存反馈决定，日志足以离线重建选择过程。

## 8. 同一 runner 的执行、控制、报告与重放改造

### 8.1 运行模式与CLI

保留 `scripts/run_s1.py` 单入口，新增拟议参数：

- `--campaign-kind required|exploratory`，默认required；
- `--profile config/s1_fuzz/profiles/<profile>.json`；
- `--max-inputs`、`--max-seconds`、`--seed`；
- `--execute` 仍为明确执行开关，不出现默认启动ROS行为。

profile声明target集合、surface集合、RMW、资源限制、负控/基线策略、corpus和budget；不同预算override记录为manifest override。新参数属于计划，不是当前可执行命令。

`run_campaign`按模式选择固定矩阵iterator或反馈iterator，但均调用同一 `_execute_planned_case`（从现有循环提取），继续使用registered services和ingest_execution。不要新建 `fuzz_runner.py` 复制identity/锁/归因流程。

每生成一个输入先写capsule＋planned记录，再执行，再写execution和反馈；崩溃不能只留下内存队列。planned数、generated数、attempted数、executed数各自有计数。

### 8.2 控制策略：不把每个输入展开成24格，也不省略可信门槛

- required模式不变。
- exploratory每个 `source + RMW + target_contract + carrier_recipe` 开始时跑对应baseline/transparent/mutation/fault校准；这些控制有独立case_id/raw与控制合同，不计自然输入覆盖。
- 校准的mutation控制使用独立参考输入，不用正在调查的异常输入充当其自身“必须通过”的控制；真实违约不能因为自己的oracle fail就循环失去候选资格。
- 运行中每20个完成探索输入做一次健康baseline检查；observer异常或恢复不完整时立即检查／中止，不等20个。
- 同一控制可以被多条报告引用但只计一次执行，引用范围不跨source/RMW/合同／不相容配方。需要共享控制必须明确等价类，而不是“都叫drop”就共用。
- 发现疑似SUT违约时，原输入及raw立即保全，并补同配置的针对性baseline／transparent／fault控制和重放。控制未齐的候选留为observed/needs_evidence，不先confirmed。
- 如现有candidate gate只能接受固定矩阵controls，必须显式扩展为具名控制引用校验。不能填 `controls_support_sut=true` 绕过函数。新的任何例外都先由主agent审核合同并写测试。

projection每种字段规则的负控校准与recovery中间序列省略的负控不可互换。G01的负控只能改变derived fact以测试oracle，并保留原raw；不得真的把兼容配置当不兼容SUT异常入账。

### 8.3 审计与replay

- `reports/run_report.py:audit_report` 根据schema/campaign_kind分派；required原72口径不变。
- 新审计复算model引用、每层input hash、执行引用、raw/事实/规则、控制适用范围、corpus计数。`evidence_valid`与`all_oracles_pass`分开：真实bug存在不等于报告无效。
- exploratory没有成熟矩阵时报告 `seal:not_applicable_exploratory` 或等价明确字段，不能借required的`sealed=true`宣称成熟；退出码区分审计失败、发现待审候选和正常预算结束，具体常量在接口冻结时写入CLI合同测试。
- replay通过同registry解析新target，保存source/contract/input；不同source重放必须显式标识差异，不迁移旧seal。
- 健康replay与受控工具失败replay继续单列；不改原失败／不删原候选。重放只要求可观测语义与异常签名符合合同，不保证DDS内部线程调度相同。

## 9. 任务卡：文件、owner、依赖与交付

新文件名为拟定写集；实施时如改名须主agent先协调。表内相对路径均以 `/home/wuyuqa/ros2-dds-fuzz-v2/` 为根。

| ID | 任务／交付物 | 独占owner／写集 | 依赖 | 完成门槛 |
|---|---|---|---|---|
| T00 | 冻结manifest基线、source策略、scope扩展清单和接口fixtures（不修改旧raw） | 主agent裁决；文档agent `docs/**`；campaign agent `config/s1_fuzz/profiles/` | 用户已批准（2026-11-17） | 旧4target canonical hash入golden；声明F1/F2范围与预算；不得改旧raw |
| T01 | 可选exploration_contract、Surface/Fact/Rule校验 | core：`core/semantic_target.py`、新`core/mutation_plan.py`、`core/scenario.py`、对应core tests | T00 | 旧to_dict不变；未知引用／循环依赖／冲突拒绝；新字段进入hash |
| T02 | S1目录、12 seeds、适用规则映射 | semantic/observer：新`modules/s1/fuzz_catalog.py`、`config/s1_fuzz/seeds/`、`modules/s1/facts.py`、`oracles.py`、专属tests | T01接口fixture | 12份seed，对应10核心维度＋2辅助维度各自与事实／规则可解析；不写成“12个核心语义面”；现有实际合同不被泛化 |
| T03 | M01–M08纯生成／组合；F1先M01–06 | core：新`core/mutators.py`、`mutation_plan.py`、专属tests | T01＋T02最小fixture | RNG影响展开输入；链不可伪造；no-op/override/unsupported分别记录 |
| T04 | 新target注册与provider，参数绑定，F1 runtime适配 | campaign：`modules/s1/module.py`、`targets.py`、`providers.py`、`campaign/identity.py`；runtime：`runtime/ros2_pair.py`、`projection_pair.py`、`worker.py` | T01–03 | 全字段实际消费或拒绝；显式runtime_capability，不改写target identity |
| T05 | runner共用执行器、corpus与探索策略 | campaign：`campaign/runner.py`、`scheduler.py`、新`corpus.py`、`feedback.py`；core：新`core/state_signature.py` | T03–04 | 无第二runner；反馈选择可离线复算；256上限及warm-up有效 |
| T06 | F2多窗口、event锚点、逐窗恢复回执 | network：`runtime/discovery_network.py`、`namespace_pair.py`；runtime配合`ros2_pair.py` | T04稳定＋F1离线通过 | 1–3窗；计数非累计冒领；恢复后才允许下一窗；finally清理 |
| T07 | 新组合facts/oracles、guard false与输入适用性 | semantic/observer：`modules/s1/facts.py`、`oracles.py`；campaign：recovery/runtime/projection adapters各自集成 | T02＋T06 | C01–07和G01都有真/假/unknown测试；非兼容不强求API拒绝 |
| T08 | 新schema input/replay／报告／控制绑定 | core：`core/evidence.py`、`replay.py`、`candidate.py`；campaign：`campaign/replay.py`、reports、CLI | T01起可并行，T04–07集成 | 旧capsule可审计；新链重放不再生成；控制不能跨合同乱用 |
| T09 | 离线验收、F1和F2最小真实验收、最终冻结回归 | 主agent统筹；唯一campaign/runtime owner执行；其他owner不跑ROS | 对应阶段T01–08 | 按第10节出机器可审计结果，失败不删 |
| T10 | 有界fuzz runs、候选审核包、独立结论 | campaign执行；独立review agent只读审查；文档汇总 | T09最终冻结GO | 第11–12节；自然候选0也如实报告 |

### 9.1 防止同文件并写

- core/mutation_plan由core唯一持有；campaign和semantic仅提交所需fixture／接口反馈。
- S1 targets/providers/module由campaign唯一持有；semantic只维护fuzz_catalog/facts/oracles，不双写。
- runtime ros2_pair/worker由runtime唯一持有；network改namespace/discovery_network，不直接改worker。
- adapters与reports由campaign唯一持有；semantic提供转换／rule函数与测试数据，不同改adapter。
- 每个agent只写自己的新测试文件；现有测试若需更新，按被测源码owner分配并先登记具体文件。
- review阶段禁止审核agent修改被审原始产物。已有未提交文件先inventory，不能reset/clean或覆盖。

### 9.2 派遣轮次

1. 轮A：core做T01，semantic整理T02规范与seed fixture；campaign先做T00身份/审计改点清单。主agent当场裁决接口，不等待所有研究完成。
2. 轮B：core做T03/state签名；campaign做T04/T05；runtime做F1适配；semantic做规则。网络agent仅评估并补T06的离线窗口控制。
3. F1 checkpoint：一次最小真实验收，展示实际生成→施加→反馈→再生成。若不通，暂停该链修复，不假装转入F2。
4. 轮C：完成T06/T07/T08，真实验证跨参数／环境组合和G01。
5. 最终冻结：相关全测试、显式提交GO、旧4target矩阵回归、新fuzz运行和独立候选审核。

最多维持6个活跃子agent，候选独立review在相关编码owner交付后释放名额再安排，或由未参与该代码／oracle的现有agent承担；不另起一组常驻reviewer。

子agent均指定 `deepseek-v4.1-flash-1017`（2026-11-17 用户指定，取代此前 `gpt-6.1-sol` 计划表述）；实际工具返回必须记录。未取得回执前不得声称模型已使用。若显式模型调用失败，不默认换模型；主agent记录准确失败和需用户处理事项，继续可独立做的审阅／协调。截至本次文档更新，本轮已派遣 4 个 `deepseek-v4.1-flash-1017` 离线最小切片子 agent（core、semantic／observer、campaign、runtime／network）及文档／验收 owner；均未落盘代码、未跑 ROS、未 commit。

不预承诺日历完成时间。每任务交付时报告已完成验收、剩余项和实际耗时，主agent据此更新下一阶段估算，而不是让agent无期限“继续研究”。

### 9.3 主agent应在派工前先做的本地工作

开工后主agent首先核对HEAD／现有写集、确认无并行owner冲突、记录原四target与catalog的canonical hash，选定T01所需最小schema fixture，并验证用户指定模型可调用。然后派独立写集，不把关键接口裁决全部交给子agent后空等。运行前锁／环境问题由唯一owner采证，主agent裁决。

## 10. 测试矩阵：每项都有可观察的成败条件

以下测试文件／测试项是待实现任务，不是当前已通过测试。合成fixture只验证工具合同，不计真实DDS变异覆盖。

| 测试ID | 输入／操作 | 必须验证的结果 | owner／拟测试文件 |
|---|---|---|---|
| V01 | 原4target序列化 | canonical manifest与冻结golden完全一致 | core，`test_fuzz_model_contracts.py` |
| V02 | fact/rule/mutator引用不存在 | 注册失败，原因含具体引用 | core，同上 |
| V03 | surface声明有field无runtime binding | generatable可声明，executable拒绝；不进live队列 | semantic，同上由core收fixture |
| V04 | 同seed/config/version重复生成 | 全展开plan、content hash、算子顺序相同 | core，`test_fuzz_mutators.py` |
| V05 | seed0–31，在至少64候选的非退化域 | 至少16个不同final content；若种子不同内容相同仍正确去重 | core，同上 |
| V06 | 改run_id/domain/time但语义输入不变 | content hash不变；execution绑定不能串用 | core，`test_fuzz_identity.py` |
| V07 | 枚举、finite/zero/infinite、边界邻点 | 预定义有限代表集100%可生成，无类别漏报 | core，`test_fuzz_mutators.py` |
| V08 | depth与count的关系三类 | 输出关系分类及rule适用性，不静默回退 | core，同上 |
| V09 | 非法错类型/越界 | 生成结果与实际rejection layer分开；不计DDS hit | runtime，`test_fuzz_runtime_bindings.py` |
| V10 | 两算子写同一字段 | 默认拒绝；显式override时前层不算hit | core，`test_fuzz_composition.py` |
| V11 | C01–C07合法配方及非法配对 | 合法计划顺序固定，非法组合带理由拒绝 | core，同上 |
| V12 | 一层命中、另一层无hit | 总体链不通过mutation_applied；原始部分结果保留 | core，`test_fuzz_receipts.py` |
| V13 | input_layer_receipts塞执行时间/hit | 拒绝；执行证据只在execution receipt | core，同上 |
| V14 | compile-time两QoS patch | 不产生虚假的两次endpoint创建；最终两字段需真实观察支持 | runtime，同bindings测试 |
| V15 | 2–3窗counter累计 | 每窗使用begin/end差值，前窗包不能算后窗hit | network，`test_fuzz_discovery_windows.py` |
| V16 | 删规则成功但无恢复discovery活动 | restore_pending/unknown，不启动下窗 | network，同上 |
| V17 | 第二窗异常／取消 | finally移除规则、关子树、释放锁；原始错误保留 | network＋runtime各自tests |
| V18 | planned/seed变化，无实际执行 | 没有状态到达／迁移奖励 | core，`test_fuzz_state_signature.py` |
| V19 | GUID/PID/time变化、抽象关系不变 | state_id不变；身份校验仍能发现错误代际 | core，同上 |
| V20 | 完整观察与缺事实观察 | 不同 completeness；不完整图不刷新覆盖 | core，同上 |
| V21 | 构造新edge反馈 | 下一轮选择受反馈影响；20%均匀分支可达 | campaign，`test_fuzz_scheduler.py` |
| V22 | corpus>256及候选输入 | 容量受控、初始seed保留、候选证据不删除 | campaign，`test_fuzz_corpus.py` |
| V23 | 同反馈日志＋RNG | selection log可离线重建；反馈顺序变化明确影响身份 | campaign，同scheduler |
| V24 | 不同source/RMW/recipe共用control | 拒绝不适用引用，不能靠布尔true绕过 | campaign，`test_fuzz_controls.py` |
| V25 | raw或plan被改一个字段 | 独立audit失败，旧raw不回写 | campaign，`test_fuzz_audit.py` |
| V26 | 重放时mutator mock设置为一调用即失败 | replay成功读取保存input，不调用mutate/prepare重新生成 | campaign，`test_fuzz_replay.py` |
| V27 | capsule旧版与新版 | 旧合同继续验证；新字段完整参与hash；未知版本拒绝 | core，`test_fuzz_identity.py` |
| V28 | G01 BR不兼容，创建成功 | 不能仅因create成功判bug；证据不足unknown | semantic，`test_fuzz_s1_oracles.py` |
| V29 | G01有identity-backed禁止边／投递 | 对应rule fail；health/provenance缺失不能confirmed | semantic，同上 |
| V30 | compatible但finite timeout未匹配 | unknown／未完成义务，不按预算直接判DDS违约 | semantic，同上 |
| V31 | 负控oracle fail | 排除自然候选；保留harness来源 | core/campaign，候选tests分配 |
| V32 | 源码/配置/CLI在run中变更 | abort当前run；不继续生成；旧证据保留 | campaign，`test_fuzz_campaign.py` |
| V33 | 预算到达／中断重启 | 最后planned/执行状态可审计，未执行不当已执行 | campaign，同上 |
| V34 | 内部异常、observer dropped logs | 记录工具错误，不自动作为SUT候选或新覆盖 | campaign＋semantic专属tests |
| V35 | 支持surface的配置被adapter静默规范化掉 | 绑定检查失败，不能仍计生成／执行成功 | runtime，同bindings测试 |
| V36 | candidate review无reviewer或证据引用 | 不能转confirmed；needs_evidence保留候选 | core/campaign，review合同tests |

建议检查命令（待测试文件实现后才执行）：

```bash
cd /home/wuyuqa/ros2-dds-fuzz-v2
PYTHONPATH=src python3 -B -m unittest discover -s tests -p 'test_fuzz_*.py' -v
PYTHONPATH=src python3 -B -m unittest discover -s tests -p 'test_provider_integration.py' -v
PYTHONPATH=src python3 -B -m unittest discover -s tests -p 'test_core_contracts.py' -v
git diff --check
```

各owner先跑自身最小测试；集成里程碑再跑上述集合，最终冻结前跑一次全测试。真实运行不靠unittest隐式启动，必须由唯一owner显式执行并记录锁／source。

## 11. 实际运行顺序、预算和成功门槛

所有预算为提议默认；用户批准后冻结profile，主agent可因已记录的工具瓶颈降低单轮资源而不能缩小声明覆盖分母，扩大总运行预算须记录。单位明确区分input、actual execution、control、replay。

### 11.1 离线生成检查（F1/F2各一次）

- 每profile生成2,000个proposal，保存摘要和异常样例；不启动ROS。
- 验证每声明边界类都有生成机会、seed影响内容、去重率、unsupported/no-op原因。
- 变异空间有限时允许内容重复；不得以“2,000条hash”宣布2,000个不同内容。
- 对7个配方逐一编译正反例；未支持配方不得进入live profile。

### 11.2 F1最小真实checkpoint

两RMW分别运行：

1. 10个可路由初始seed，各一次，共20次seed执行。
2. generator产生的参数输入各12条，共24次，不使用手写替代输入。
3. generator产生的E01环境输入各6条，共12次，包含短／中／长窗口。
4. 对应控制按第8节另记，不纳上述56次输入执行。

若某seed被SDK拒绝，作为实际拒绝结果保留，不称DDS变异已验证；若期望合法seed连续拒绝，应主agent裁定能力／适用性，不能直接删除以达成56次pass。F1门槛是生成循环真实接线、至少参数和环境各一族在两RMW上有真实hit、绑定和审计通过；不是56次必须全部oracle pass。

### 11.3 F2组合及守卫checkpoint

- C01–C07 × 两RMW × 3次独立执行 = 42次组合尝试。每配方／RMW至少1次所有必需层hit且证据完整；另外两次失败／ineffective保留并解释。
- G01四个关系 × 两RMW × 3次 = 24次守卫输入；检查实际配置、观察能力、窗口内关系判定及不兼容不等于创建失败。
- 真实健康replay两RMW各2例（参数、环境／组合），共4次；受控工具失败replay各1例，共2次；不计自然候选。
- 低概率 hit 的配方最多补 6 次尝试／RMW，然后交主 agent 诊断。尚未 hit 的能力为未验证，不得在 F1/F2 交付中假装完成；主 agent 调整有证据支持的触发条件后重新登记测试，而不是无限重跑或放宽 hit。

### 11.4 最终冻结与有界探索

1. 完成代码集成／测试后主agent明确GO，指定owner显式staging行为路径，排除results；不继承旧单次commit授权。
2. 固定HEAD/source/model/mutator/profile，再按旧四target顺序跑72 required＋24 negative；旧证据不迁移。
3. 新探索profile：`parameter`、`environment`、`composed` 三种；每profile×两RMW×两个独立rng_seed共12轮。
4. 每轮最多80个新探索输入、2,000次生成attempt或30分钟，任一先到为止；合计最多960个探索输入、约6小时预算上限。控制及重放单列执行数，控制开销计入轮次wall-clock；初始seed和重复内容不计新增输入数，但计执行成本。
5. 全局每例最长180秒（含受控操作／观测预算，cleanup保留额外有界窗口），最多3个drop周期、单窗≤8秒；剩余轮次预算不足时不启动新输入，正在执行项保留实际结束原因。超预算清理只操作已验证归属的子树／命名空间。
6. 执行异常先保全证据并安排review。不能为跑满960条而忽略source漂移、observer故障或未回收进程。

这6小时不是承诺找到候选，也不是论文效能实验。若连续两轮只生成重复内容／无新可判定覆盖，主agent先诊断seed空间、算子路由或观测瓶颈，不机械增加时间。

### 11.5 F1/F2 交付包完成门槛（全部列项，不互相抵消；不等于 S1 成熟）

- A：旧四target的合同、输入schema兼容、锁/归因/重放不退化。
- B：第 3 节每个 F1/F2 必需 surface 都有声明、生成、路由记录；实际能力和局限准确，P/E/G 核心族的真实证据满足 checkpoint；unsupported 不得默认为已完成。
- C：M01–M08、7个组合配方通过相关离线合同；F1/F2实测门槛满足；无用重复mutator不能凑数量。
- D：corpus亲子血缘、内容去重、真实生成循环和最小状态反馈已接线，有日志证明反馈改变后续选择。
- E：每层input与execution分离，聚合hit正确，raw/audit/replay可复算；无shadow runner或第二candidate系统。
- F：自然异常都进入独立审核流程；0异常时报告未验证的非空自然候选部分，不造测试候选。
- G：交付变异面覆盖表、运行摘要、未知原因、输入多样性／hit率／成本、候选审核记录和下一轮明确缺口。

“工程交付完成”和“2–3真实候选目标完成”分别报告。任一必需面未完成要列原因与下一步，不能只拿总体通过率掩盖。

## 12. 候选审核：工具准备证据，独立agent审核，主agent裁决

### 12.1 审核包

出现规则fail或有证据的异常时，工具先保留原row/raw与统一candidate生命周期，生成review包索引，包含：

- source/target/revision/model/rule/mutator/profile身份；
- parent seed、最终input、完整chain／窗口、每层receipt、raw hash；
- primary与supporting规则、具体fact及规范出处／适用条件；
- 控制引用、observer／工具健康、实际施加层，否定候选的已知替代解释；
- replay命令／capsule、已有尝试、未执行原因；
- 已知相似候选的去重信息，不能仅凭相同错误字符串合并根因。

`review_packet.json`是现有candidate的附件，不引入平行candidate状态机。审核动作与结论另写append-only review记录，不能编辑原raw令其符合叙述。

### 12.2 审核角色与标准

由**未直接编写该次触发链／oracle的独立 `deepseek-v4.1-flash-1017` agent**只读审核。若只剩原作者可用，先标待独立审核，不让其自审替代。主agent调度所需重放，review agent不得自行并发起ROS。

审核必须回答七个问题：

1. 输入是否确实到达声称的层？
2. 变异是否命中，环境效果是否本来允许？
3. 规则／规范是否适用于该QoS、时窗和身份？
4. raw是否支持事实，是否存在缺失／误关联／重复采集？
5. 负控／工具故障／资源饱和能否解释异常？
6. 重放是否保持同语义输入，成功／失败次数与结论是什么？
7. 是否与已有异常相同根因；需要何种补证才能确认？

输出 `supported / rejected / needs_evidence`、理由、source/raw引用和实际reviewer模型信息。该标签是review结论，不取代已有candidate lifecycle状态。明确的控制故障无条件排除，不能因为reviewer意见重新入自然候选。

主agent综合review与证据决定admission/triage/confirmation。`supported`不自动等于confirmed；confirmed仍须满足现有证据政策及replay或具名non_replayable依据。一次replay失败不自动rejected。没有真实对象时只可测试review包schema，不能声称已实测真实候选审核。

## 13. 开工前需登记的工程决策，不留给子agent各自猜测

| 决策 | 本文默认方案 | 若遇事实不符如何处理 |
|---|---|---|
| target／schema兼容 | 新fuzz target identity＋可选扩展；旧字节不变 | 主agent裁决版本，不覆写旧manifest |
| 规范引用 | 优先已获取DDS/ROS原始资料＋本机API行为；未核实规则不上强断言 | 文档列精确缺口；该面保持未验证 |
| 自动探索scope开关 | 用户批准后在新profile／scope明确启用，原required policy保留 | 不通过改campaign名绕过原限制 |
| 0／infinite QoS表示 | 按实际API与wire/graph转换校准 | 归一化规则进入contract，不事后配值让对账通过 |
| 组合后的交付合同 | 只在条件和闭合证据满足时适用 | unknown／not_applicable有原因，不硬判fail |
| 多窗口计数 | 使用逐窗delta，限制本pair实际discovery | synthetic UDP和前窗累计不得计hit |
| 旧source治理 | 新行为文件纳入source monitor，运行中改动abort | 历史报告只按旧source审计，不迁移成熟声明 |
| 模型选择 | 所有子agent显式 `deepseek-v4.1-flash-1017` | 实际不可用则准确记录，不擅自替换；未派遣即写“未派遣” |
| 需要用户的新决定 | 仅真正改变范围／依赖权限／模型选择 | 写open-items及证据；主agent继续不被阻塞部分 |

## 14. 每个agent派工单必须包含的模板

```text
任务ID：Txx
模型：deepseek-v4.1-flash-1017（以工具回执核实实际使用；未派遣写“未派遣”）
目标：一个可验收交付，不写“完善模型”等泛词
输入：已冻结schema/fixture/源HEAD及依赖任务结果
唯一写集：逐文件列出；禁止目录和已有未提交改动列出
接口：要实现的函数/字段、调用方和兼容性要求
检查：Vxx测试ID、具体命令；禁止启动ROS的约束
完成证据：文件清单、测试输出、未运行项、source差异、残余问题
禁止：修改旧raw/分母，伪造hit，替换模型，自行扩大scope或并发campaign
```

主agent在每个里程碑审核diff、接口与实际测试，不仅接受子agent“已完成”文字。文档agent只据证据更新台账；commit与ROS始终需要主agent显式GO。

## 15. 交付文件与本次计划检查

实施后的预期用户可见产物：

1. 版本化surface/fact/rule目录、12份初始seed和运行profile。
2. 实际mutator／组合器、统一runner探索模式、corpus和最小状态图数据。
3. 原合同回归报告；F1/F2 checkpoint与有界fuzz报告。
4. 按surface/算子/配方分层的generated/executable/hit/oracle覆盖表，不以单个总体pass率替代。
5. 每个候选的review包、独立agent意见、主agent裁决；零候选同样有完整台账。

### 15.1 编号与状态约定（2026-11-17 核对）

- 任务卡固定 T00–T10 共 11 项，测试固定 V01–V36 共 36 项；本轮只更新状态与 owner，不重排、不删减、不复用编号给新能力。
- F3–F8 若进入实施，必须追加新任务卡与新测试 ID（例如 T11+／V37+），不得改写本表语义。
- 能力标签 `declared → generatable → executable → hit_verified → oracle_qualified` 是覆盖账，不是 `execution_stage`，也不能替代 candidate lifecycle。
- “已批准开工”“已派遣”“已实现”“已真实执行”“已审核候选”“已成熟”是五个不同状态，文档与汇报不得合并。

本次计划文档检查只验证章节、路径、任务／测试编号、依赖和来源说明；没有新业务实现或 DDS 执行。相较前版，工程范围没有扩回“大平台”，只是将最小 fuzz 闭环细化到能够派工、集成和逐项验收的程度，并区分“已获准开工”与“已完成”。

## 执行状态更新（2026-11-17，HEAD d748be1）

本节只记录真实完成与真实缺陷，不构成 seal 或成熟声明。

### 已完成并真实执行

| 能力 | 状态 | 真实证据 |
|---|---|---|
| 宽采样 `mutate_wide`（boundary/representative/log_uniform/linear_uniform/neighbor/jump） | 已实现且已接线 runner | `results/fuzz-cli/wide-chain-1`：6 条真实 running，4 条 judged/pass，`sampling_kind` 覆盖 jump/linear_uniform/boundary；2 条 zero-delay 正确落 `reached`（0 延迟无可观测时机效果，非缺陷） |
| 多层变异链 `compose_chain`（1..4 层） | 已实现且已接线 | 同 run 每行 `chain_depth=2`、2 条 operations；单一 `effective_input_sha256`、单一 capsule、单一 config |
| 覆盖账 | 已实现 | `run_report.exploratory_coverage_table` 按 target×surface×sampling_kind×chain_depth 分档；corpus bucket key 同步纳入，避免不同采样类互相压掉 |
| endpoint churn（reader2..reader9、创建/销毁、writer2） | 已实现 | `tests/test_fuzz_runtime_lifecycle.py` 13 项；`results/fuzz-cli/churn-deep-005` 真实 judged/pass（双 RMW、3 代 reader + 1 次销毁 + writer2 churn + 256 包 burst） |
| 多窗 E02/E03（1..3 窗、逐窗 delta） | runner 与 adapter 已实现 | `tests/test_fuzz_multiwindow_admission.py` 22 项；真实多窗尚未执行 |
| netem carrier（discovery 面） | 已实现 + 真实自检 | `scripts/check_discovery_netem.py`：隔离 netns 内 delay 50.187ms、duplicate 2 copies、reorder 9 inversions、restore 后 clean |
| netem carrier（data 面） | 已实现 + 真实自检 | `scripts/check_netem_data_scope.py`：data 端口 delay 50.07ms、duplicate 2、reorder 10 inversions、restore clean |
| mixed-vendor（writer/reader 不同 RMW） | 已实现 | `tests/test_runtime_mixed_vendor.py` 27 项；真实跨厂商互操作尚未执行 |

### 本轮修复的真实缺陷（均由真实运行暴露，非离线构造）

1. **window-chain facts 缺 match proof**：多窗路径未写 `facts['match']`，导致 recovery oracle 全部 unknown。修复后 `results/fuzz-cli/churn-deep-005` 真实 pass。
2. **primary 窗口取错**：曾用「中断开始」当交付窗口起点，导致 `boundary < activity <= window.start` 必然不成立。修为 post-restore 交付窗口。
3. **match 证明须在最终 restore 之后**：pre-window 快照不能替代恢复后重建；改为以最终 `discovery_restore_boundary` 为 anchor。
4. **netem 单窗被误拒**：`environment_windows` 存在即拒绝，而 runner 对 recovery 总会生成单窗，导致 netem 路径完全不可用。改为只拒绝 >1 窗。
5. **netem restore 边界变量未初始化**：`_run_netem_window` 分支下 `restore_mono_ns` 未定义，真实运行 `UnboundLocalError`。修为把 netem 自身边界传给共享 final graph step。
6. **carrier 实现硬编码**：adapter 把 carrier 实现写死为 nft drop，netem 恒被拒。改为按 implementation 选择对应 receipt 契约。
7. **窗口名不匹配**：adapter 期望 `discovery_netem_window`，运行期实际发 `discovery_netem`。
8. **netem 时序**：netem 窗口 `install < open < restore < close`，与 drop 的 `install < open < close < restore` 不同；adapter 现按 carrier 区分。
9. **namespace readiness 用 drop schema 校验 netem**：netem setup 没有 `drop_installed`/`initial_nft`。改为按 carrier 校验各自的 neutral 证据。
10. **data 面 netem 从未覆盖交付流量**（本轮最重要）：真实 raw 证明 netem 窗口 1.08s 结束于交付发布之前，交付的 128 条消息全部在窗口外，data 组仅 3–6 个stimulus 包。这不是「变异无效」，而是工具缺陷。已实现 `netem_covers_delivery`：stimulus 段之后另起一段覆盖真实交付窗口的 netem，并新增 `netem_delivery_overlap` 证明 `install < delivery_open < delivery_close < restore < boundary`。
11. **观察限制被误报为 `mutation_ineffective`**：FastDDS 数据面计数为 0 时被记成「变异无效」，会误导归因。现区分 `data_plane_observed=false` + `data_plane_observation_reason`，并写入 `observation_limit` 字段。
12. **host 侧 summary 重建抹掉 carrier facts**：`_body` 算好的 netem 事实在 host finally 重新 `summarize_discovery` 时被丢弃。修为保留 `delivery_overlap` 与 `data_plane_*`。

### 真实运行汇总（截至 d748be1）

```text
judged/pass      137
judged/unknown    12   （均为 adapter 修复前的历史 run，raw 保留）
aborted/unknown   47   （均为工具缺陷，已逐项定位并修复；raw 保留）
reached/n-a       28   （zero-delay 等语义正确分支）
planned/unknown   15   （planning-only 样本）
自然候选           0
```

`results/fuzz-cli/netem-delivery-extreme-4` 是本轮最有价值的一次真实运行：

```text
CycloneDDS: delivery_inside_mutation_window=True, data_plane_observed=True,
            data_netem_delivery packets=258（真实业务包经过 netem），
            oracle pass（128/128 有序、无重复）
FastDDS:    delivery_inside_mutation_window=True, data_plane_observed=False,
            reason=data_plane_not_observed_inside_covered_window:possibly_shm_or_non_udp_transport
```

结论：在 400ms 延迟 / 100% 重排 / 100% 重复下，CycloneDDS 仍满足可靠有序交付合同——这是**正确的 DDS 行为**，不是漏检；oracle 已被真实压到。

### 仍未完成

- **多 endpoint 的 S1 发现图**：`builtin.discovery_projection_reconciliation` 仍硬编码 1 writer + 1 reader（`projection_adapter.py:138` 要求 `len(items)==1`）。S1 的定义核心是发现图投影，单端点对无法覆盖多 endpoint 的遗漏/重复/身份混淆。这是当前最可能产出真实候选的缺口。
- FastDDS 数据面端口推导：实测其 user-data socket 落在 `PB+DG*domain+11+2p`，但 tc 计数为 0，说明其实际流量路径与推导不一致或走了 SHM；已在 facts 中如实记录为观察限制，未伪判。
- 真实多窗（E02/E03）运行、mixed-vendor 真实互操作、netem 与 churn 组合的真实运行。
- 自然候选仍为 0；不得用任何控制/工具错误凑数。
