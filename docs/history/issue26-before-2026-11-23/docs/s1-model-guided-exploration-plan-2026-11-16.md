# S1 模型驱动探索：研究假设与分阶段实施计划

> **详细派工版（待批准）**：当前开工依据以 [S1 变异面扩展实施计划](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-fuzz-expansion-implementation-plan-2026-11-16.md) 为准；本页保留源码核查／长期背景，不是另一个并行实施任务。

> **2026-11-16 范围调整（待批准）**：根据用户指出的 seed、mutator、状态图和组合变异缺口，本文 P0–P7 降为长期路线，不再作为本轮直接实施包。当前优先方案见 [S1 fuzz 能力源码核查与收窄建议](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-fuzz-gap-review-2026-11-16.md)。本轮先补变异面目录、corpus/mutator/组合链、最小反馈循环与独立候选审核，不先建设通用 ActionModel 或大型实验平台。

- 日期：2026-11-16，Asia/Shanghai。
- 状态：**待用户批准；不是已实施、已验收或新增 campaign 的授权记录。**
- 来源：用户最新要求是论证分类／功能文档的方法可行，并在同意详细计划后由子 agent 实施。本文其余架构、任务拆分、数值预算与门槛均为主 agent 工程提案，不冒充原规格或用户原话。
- 文档写集协调：本轮主 agent 仅新增本文；不修改历史验收、原任务书、旧仓库或已有未提交文件。批准后由文档／验收子 agent 接管本文和相关台账。
- 本轮只读核查已有源码、结果和论文，未启动 ROS、未运行业务测试、未派子 agent、未提交 Git。

## 1. 结论：模型是核心，但不是充分条件

用户的研究方向成立为一个**值得检验的研究假设**，不能提前写成已证明的结论：

> 把 DDS 功能分类转成可执行的语义合同、输入语法和状态／迁移模型，利用模型规划到达目标状态，再做参数、环境和时序变异，并用合同检查真实执行结果，可以比固定场景或无模型生成更有效地探索实现。

“模型足够好”需要拆成可以检验的含义，而不是遇到零候选时无限归因于模型还不够好：

1. **语义模型**：定义行为约束、适用版本／层级、允许结果和禁止结果，且有规范来源。
2. **输入模型**：约束可生成、可变异、可真正提交的拓扑、动作、参数、环境和调度程序。
3. **状态模型**：识别可观察状态、可执行迁移以及不可观察部分；不把计划状态当实际状态。
4. **执行闭环**：规划器、carrier、观测器、oracle、反馈调度和重放必须实际消费同一模型及输入。
5. **实验设计**：区分模型收益、执行效率收益、观测收益与偶然遇到 bug，避免只凭功能数量或候选数量下结论。

模型很丰富但 runtime 只执行一个固定 writer/reader 流程，无法验证方法；runtime 很复杂但生成／oracle 未使用模型，也无法验证模型的贡献。模型还可能遗漏隐藏状态、规范存在歧义或观测不足，因此不能承诺语义等价于全内部状态，也不能承诺穷尽或必然发现缺陷。

### 1.1 论文对应关系与非对应关系

已直接下载并核对用户指定的《Property-Based Testing of Wafer Handling Robots via Model-Based Planning and Fuzzing》（ArmGo），本地文件：

- [论文 PDF](/tmp/r2f-brainstorm-research/ArmGo.pdf)
- [可检索正文](/tmp/r2f-brainstorm-research/ArmGo.txt)
- PDF SHA-256：`1cafce5452723f0c728d6bc0a5e928a9a9ffc630964d809d2f518e0e65acba3e`。
- 获取自作者公开 PDF；沙箱 DNS 失败后，经工具升级访问成功。临时目录不是长期资料库；P0 应归档来源、hash 和必要摘录／段落索引，不把全文当作项目原创。

核实内容：§3–4（PDF 第 3–6 页）使用动作参数约束、守卫和效果；§4.1 规划到达前置状态；§4.2 做参数和环境变异，结合广度／深度探索；§4.3 每动作后检查属性和效果。§4.2 明确每条完整测试从初始状态执行，避免不可逆状态变化污染其他测试。本文不引用其 bug 数量或实验提升比例来预测 DDS 结果。

**需要纠正的简单类比：**论文不是只有“合法接受／非法接受”两条 oracle；它还有共享安全属性、动作效果及逐步检查。DDS 又是异步系统，API 创建成功、发现可见、QoS 兼容、实际匹配和交付不是一个布尔事件，不能照搬单一接受／拒绝判定。

本文新增的前缀多样性、观察状态图、反馈调度、并发部分顺序、持续 episode 是针对 DDS 的工程提案，不是声称 ArmGo 已直接验证这些 DDS 设计。

## 2. 当前能力与证据基线

### 2.1 源码与真实结果

当前 HEAD：`e448febec4fa9f3c24f22eaac7954517e7b4a301`。

历史正式执行记录绑定的 behavior hash：`e7f92897dd6d122a874f7686a24419173d110abd765106ce5e16e1289e732cfa`，执行时 `source_frozen=true`。本轮只读重新计算 source identity 与该版本一致，`source_frozen=true`、`dirty_behavior_paths=[]`；这不是新执行证据，实施前仍须重新计算身份。

| 项目 | 已有证据／准确结论 |
|---|---|
| 全测试 | [日志](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/e448feb-full-20261115T164801Z-d1fd8e3f/full-unittest.log)：457 tests，456 pass、1 skip、无失败；不是本轮重跑 |
| 四个原 target | [正式汇总](/home/wuyuqa/ros2-dds-fuzz-v2/results/final-validation/batch-20261115T165015Z-45cc1bed/summary.json)：逐 target 18 正常 pass＋6 负控 fail，共 96 行；72 required、24 负控，四 target 通过 |
| F07／Phase2→4 | [当前台账](/home/wuyuqa/ros2-dds-fuzz-v2/docs/open-items.md)顶部记录健康重放、受控工具失败保留及 zero-candidate 分支；不代表非空自然候选链已被验证 |
| 128 条交付补充 | [补充汇总](/home/wuyuqa/ros2-dds-fuzz-v2/results/final-validation/candidate-probe-latejoin128-e448feb-20261115T173823Z-a485c029/summary.json)：24 行通过对应审计，候选 0；不是正式 72 格替代，也不是高并发证据 |
| 候选 | natural=0，confirmed=0；不能据此说实现安全，也不能说候选机制已由真实异常完整验证 |
| 完整 S1／用户目标 | 原四 target 合同的证据已做实；广义 S1 探索覆盖和 2–3 个真实候选目标未达成 |

### 2.2 代码现状与预期差异

| 维度 | 当前实现 | 本计划完成后的能力 |
|---|---|---|
| 语义模型 | `SemanticTarget`、固定 steps、requires、effect/rule/evidence 合同 | 可执行 ActionSpec，分离工具要求、SUT 守卫、参数约束、允许结果及跨步骤不变量；来源与适用性可追溯 |
| 输入模型 | target-bound 参数／capsule，固定 pair 和顺序 | 有界拓扑＋动作程序＋前缀＋环境变化＋调度约束，确定性编译；每个生成字段有实际消费或明确拒绝 |
| 状态模型 | step before/after 与静态前置检查 | 归一化观察状态、代际／拓扑迁移、历史类别、未知字段；状态图与规范合法模型分离 |
| 规划与调度 | 固定 required 矩阵；没有 adaptive scheduler | 有界到达规划、同状态多前缀 corpus、反馈选择、随机探索保底 |
| 执行器 | worker 单 endpoint、进程级 generation、逐条 publish 等命令 ACK | endpoint registry、每次创建独立 generation、自主 workload、场景内并行和 barrier／部分顺序 |
| 场景寿命 | 基本为每 cell 独立 runtime | 独立 testcase 与持续 episode 两种输入单位；长历史可重放、可缩减、可验证 reset |
| 环境变异 | 已有真实 discovery drop/restore | 守卫导向的环境变异，并与工具故障、控制故障分开；先复用现有 discovery carrier |
| oracle | 四 target 精细 evidence gate | 保留可信门槛，复用共享属性与声明式动作合同；不以“简化”之名降低判定标准 |
| 实验 | 正式矩阵／补充实验 | 固定基线＋无模型／无反馈／无环境变异消融，同预算比较有效探索和成本 |

直接核查入口：

- [SemanticTarget](/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/core/semantic_target.py)
- [StateExecutor](/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/core/state_machine.py)：`requires=false` 当前直接 blocked。
- [scheduler](/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/campaign/scheduler.py)：当前明确为固定矩阵。
- [worker](/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/runtime/worker.py)：单 endpoint、SingleThreadedExecutor。
- [ros2_pair](/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/runtime/ros2_pair.py)：逐条 publish 回执等待。
- [input identity](/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/campaign/identity.py)

**工程判断：当前候选难发现首先有明确的工程覆盖不足，尚不足以判断 S1 本身“没 bug”或这个方法技术上不可行。基础验收已经足以推进新覆盖，不应继续用相似绿色矩阵替代探索建设。**

## 3. 待批准的范围包与不变边界

### 3.1 本轮建议批准的范围包 E1

1. 语义／输入／状态模型可执行化及统一 runner 接线。
2. 基于公共 ROS2 API 的有界自动缺陷探索：参数合法／非法边界、明确有定义的守卫违例及环境扰动；候选仍按现有证据链处理。
3. 有界多 writer/reader、多个进程／节点、多个 topic，以及 endpoint remove/recreate 引出的发现／匹配图变化。
4. 同状态多前缀、持续 episode、逐步检查、历史重放及缩减。
5. worker 自主 workload、进程间并发与明确 API 支持范围内的线程并发；并发作为 S1 图变化触发条件，不随意扩大交付合同。
6. 反馈调度与方法对照实验。

旧文档“只做正常互操作、不自动搜寻”的表述来自早期主 agent 工程边界，不是研究方法的完整目标。E1 若获批准，应由文档 agent 明确记录该范围更新；不能一边保留狭窄执行边界，一边声称已实现模型驱动 fuzz。批准前不开始 E1 实现。

### 3.2 保留与暂缓

- 原四 target revision0 的 manifest／合同保持不变，原 required 分母仍为 72。新程序化探索先使用**新 target identity**（最终名称由 P0 固定），不能借已有 target 名字悄悄变更合同。
- exploratory 数量另记，不把每步、每前缀或重放计入旧 72。新 target 进入 required/seal 前单独冻结其矩阵与分母；整个项目的扩展覆盖单列，不宣称总范围永远只有 72。
- 本轮不切换到 S2–S8 主模块，不默认加入 mixed-vendor pair、畸形 wire payload、native participant 或所有 DDS API。
- lease expiry/renewal、discovery delay/reorder/duplicate、ContentFilteredTopic、复杂类型可赋值规则、完整多 participant 身份验证、分布式快照列入后续 E2 评估。对已有公共 API 可表达性先查证，不先验声称可做或不可做。
- 同一测试可涉及 S1/S3/S8；按**违反的合同**确定主归属，触发因素可打多个标签，不因发生并发就全归 S8。
- 旧仓库只读，不复制旧未提交实现或旧 seal。

### 3.3 “完整 S1”不偷换含义

[旧分类文档](</home/wuyuqa/ros2-dds-fuzz/DDS_three_level_spec_family_semantic_taxonomy(1).md>) §9.3 的 S1 包含多 participant/topic/type/reader/writer、端点出现消失、租约和过滤等内容，比初始四 target 广。

P0 必须建立全 S1 coverage map：每项标记 `implemented_verified / planned_E1 / deferred_E2 / unsupported_observation / not_applicable_with_basis`，附规范与真实证据。**E1 完成只能称“模型驱动 S1 扩展能力完成”，不能直接称完整 S1 成熟。**若用户目标继续是广义完整 S1，E1 之后按该 map 制定 E2，不删除未覆盖项来达标。

## 4. 统一模型和执行接口

本节字段／文件为拟实现设计，不是当前已经可调用的 API。

### 4.1 规范模型：ActionSpec 与 PropertySpec

每个动作至少包含：

| 字段 | 含义 |
|---|---|
| `action_id / model_version` | 稳定动作及模型版本 |
| `scope / applies_to` | ROS API／RMW／DDS 层，版本、能力与配置限制 |
| `spec_refs` | 规范版本、章节、命题和适用条件；工程假设独立标注 |
| `parameter_constraints` | 类型、枚举、关系与边界；不只独立字段范围 |
| `tool_requirements` | 锁、owner、source、observer 健康、可提交性；不可故意绕过 |
| `sut_guards` | 被测语义条件，可真／假／未知，和工具要求分离 |
| `allowed_outcomes` | 正常、指定拒绝、异步 pending、允许不匹配等结果集合 |
| `effects / invariants` | 动作效果与跨动作不变量，含时间适用区间 |
| `observation_contract` | 事实来源、新鲜度、关联身份、证据充分性条件 |

建议先把动作限定为 create_endpoint、destroy_endpoint、start/stop_workload、observe_graph、discovery_drop/restore、barrier。字段值用结构化表达式，不执行任意模型字符串。

`StateExecutor.requires` 继续只承担真实执行所需门槛。对守卫违例测试，必须有“目标 guard=false”的真实前置 receipt；不能把所有 requires 绕过，也不能用离线模型预测代替 false 观测。guard=unknown 时该合同不可判，不自动当 false。

一个重要区分：QoS 不兼容的两个合法 endpoint 可以分别创建成功；测试目标是不得形成禁止的匹配／交付，不是必须拒绝创建。网络不可达也不推出 write API 必须返回错误。对未定义行为不强行指定“安全拒绝”。SDK 先拒绝的参数记录到 SDK 层，不能冒称已经触达 DDS。

### 4.2 输入模型：ScenarioProgram

扩展现有 capsule/schema，旧版读取和重放不变。新输入需保存：

- `program_schema_version`、target/model/oracle identity。
- 初始 topology：逻辑 endpoint ID、role、topic/type、QoS、进程／节点 placement。
- `prefix_actions` 与 `test_actions`，每个动作稳定 ID，参数和引用显式展开。
- `test_intent`：合法探索、参数违例、守卫违例、环境扰动等；独立于 arm。
- `schedule_constraints`：happens-before、barrier、允许重叠的组、启动偏移及预算。
- 背景 workload、环境变化、episode 预算、停止条件。
- generator/policy 版本、生成 seed 和**实际展开程序**；不能只留 seed。

输入身份包含最终确定性程序／参数／调度约束；不包含真实 PID、GUID、domain、wall time、实际交错或执行 verdict。运行时 `run_id` 只作为隔离／关联元数据，应用消息内的运行标记需与确定性 payload 模板分开。

复现并发时，计划的偏移／偏序是输入；真实开始／结束顺序是 execution trace。后者可以形成新的 replay schedule recipe，但不能倒写原 input hash，更不能宣称重放能完全控制 DDS 内部线程。

非法参数或守卫违例是正常的被测输入类别，**不设为现有 `negative` 负控**。negative 仍专指工具注入的控制故障并永久排除候选。新增 test_intent 与正常 baseline/transparent/mutated 的组合须在 P0 的 schema 表中固定，不能混用。

### 4.3 状态模型：规范、观测、历史三层

- `state_id`：归一化观察状态，含逻辑 endpoint 存活、代际关系、可观察图、兼容关系、环境状态、进行中的动作及观察完整性。
- `prefix_id`：到达路径的动作程序 hash；同状态不同历史不自动合并为同前缀。
- `history_class`：首次创建、恢复后、重建后等；先保存每类有限代表，不保存无界重复日志为无限新状态。
- `trace_id`：实际运行与 raw 的引用，保存真实 ID 和交错，不混入抽象 state_id。

归一化去除随机 GUID/PID/time 的绝对值，但保留同一性／不同性、活跃与已删除代际、相对先后和指定时间区间。未知值有明确标记；观察不完整不与完整健康状态碰撞。

规范预测图、实际观察图和调度 corpus 分开。新状态／迁移可加入观察图并获得探索分数，**不能自动修改规范允许集合或降低 oracle**。若两个同 state_id 的路径表现持续不同，先记录 abstraction_collision／隐藏状态嫌疑，再版本化细化抽象；旧 corpus 按版本隔离。

### 4.4 规划与反馈

第一版用有限动作集合的有界搜索／目标导向前置搜索，不先引入大依赖。示例上限：规划深度 12，最多展开 2,000 个抽象节点；超限输出明确 `planning_budget_exhausted`，不等同于不可达。

选择顺序：目标状态／合同 → 历史类别和前缀 → 动作 → 参数／环境／调度变异。先实现模型指导、无反馈版本，再加入自适应策略，便于消融。

初版反馈采用可审计的分层策略，避免过早堆复杂权重：

1. 保留 20% 均匀探索概率（拟定默认，实验前冻结）。
2. 其余优先未覆盖的合同分支／迁移／拓扑关系组合，再选稀有且较可达状态。
3. 同一目标按前缀类别轮换，兼顾历史多样性与执行成本。
4. 持续 unreachable、mutation ineffective、observer unknown 降低重复预算，转入独立诊断队列；不能删除失败账。
5. SUT 异常进入 triage，但工具异常、guid 噪声和不完整观察不计作正向新覆盖。

调度策略可以自动改变后续输入，不能自动改变合同、分母或已封存 raw。可选内部覆盖反馈单独列来源；没有插装就不声称代码覆盖提高。

## 5. 多 endpoint、并发、持续历史与 oracle

### 5.1 多 endpoint 执行与身份

先 1W/1R 打通新程序链，再扩展 1W/2R、2W/1R、2W/2R 和兼容／不兼容混合拓扑。初版有界随机建议：writer/reader 各 0–4、总 endpoint≤8、worker process≤4；包含无 writer 或无 reader 的有意义状态，但每个动作须满足其自身可执行条件。

每次 create 分配新 endpoint generation，即使在同进程中同名重建也不同；保存逻辑 ID→generation→实际 GUID 的来源映射。node、process、context 不能直接等同 participant；只有确证关联时报告 participant 身份，否则显式未知。

将以下事实分别记录：

- discovered endpoint 集合；
- 规范推导的可兼容关系；
- API 匹配数量；
- 真实可关联的逐对匹配／交付证据。

count 正确不证明逐对边正确；QoS 推导出的期望边不是实际观察边。Jazzy 缺 per-sample publisher_gid 时，payload 的 writer/generation 标记只证明应用来源关联，不冒称 middleware direct GID。多个 writer 的边无法独立观察时用受控隔离 probe 或记 unknown，不从数量补造边。

### 5.2 workload 与并发

worker 接受 start_workload 一次后自主执行批量发布，控制器收 started/progress/completed，不再每条发布等待命令 ACK。自主任务必须有取消、预算、backpressure、计数和日志上限。

分三档验证，不同时增加所有扰动：

1. 多进程自主 workload，无 endpoint 生命周期交错。
2. workload 运行时其他 worker 创建／销毁 endpoint；用 barrier 对齐交错窗口。
3. 明确接口线程安全及 callback-group 前提后，才启用同进程多线程／重入 executor 组合。不能把 executor 换成 MultiThreadedExecutor 当作并发证明。

并发证据是两个操作真实时间区间重叠、参与对象身份和观测覆盖；时间戳只能证明观测到的边界，不能证明内部竞态已经发生。分别报告请求发布率、实际 API 调用率、接收率、队列积压、CPU/RSS、observer 丢失和延迟。不给“高吞吐”设未经测量的固定标签。

负载采用低／中／高三级实测阶梯；先在健康 observer 条件下校准上限，随后冻结实验 profile。工具饱和不判 SUT 错。保持每 writer 的序列与代际，排序合同按 QoS／instance 适用性定义，不要求跨 writer 全局有序。

### 5.3 独立与持续模式

- **独立模式**：干净起点→前缀→测试段→检查→结束；用于隔离、回归和重放。
- **持续模式**：从干净起点开始的一条完整 episode；例如初版最多 100 个动作、60 秒、20 次 create/destroy、8 个活跃 endpoint，任一预算先到即结束。它是一个序列输入，不把其中 100 步算作 100 次独立 testcase。
- 每步保存 before/after、动作返回、guard、effect 与 oracle，检查不只在 episode 最后做。
- 崩溃、身份混乱、工具健康失效或不可解释状态出现时停止该 episode、保全 raw，不在污染状态继续积累“独立测试”。允许健康且已定义的拒绝后继续，但先验证拒绝没有禁止的副作用。
- 主动重启与预算 reset 单独记成本和原因。第一版 reset 使用进程／网络环境完整重建＋起点检查，不称快照恢复。
- 后续无污染复用必须证明 endpoint、workload、环境规则、待处理消息和观察基线已恢复；有意保留状态的 episode 不需要伪装成 reset。

重放保存完整历史而不只最后一个动作；缩减先删独立段，再删动作、endpoint、参数复杂度和调度约束，始终保留前置可达、异常签名及证据充分性。竞争候选用多次概率重放记录成功次数／总数，不能因一次冷启动未复现删候选。

### 5.4 oracle 不做“一律变粗”，改为三层

1. **共享属性**：进程存活、身份／代际一致、禁止跨域／不兼容边等；hang 仅在明确 watchdog 及健康证据下称为 liveness suspicion，不能把一般超时直接当协议违约。
2. **动作／关系合同**：允许返回和副作用、发现／匹配关系、删除后的残留、恢复后的合法行为。
3. **证据有效性**：source、input、raw、observer、窗口、身份、施加层和 mutation hit。

先判证据是否足以应用合同，再输出 pass/fail/unknown/not_applicable。期望效果与 forbidden residual 同现时保留 residual 优先规则。drop 导致暂时不匹配属于允许效果，不因“状态新颖”自动判 bug。

异步没有规范硬时限的收敛预算只用于调度与 unknown/suspicion；窗口尾部缺失不能单独 fail。每项强断言都要列出确切闭合证据。期望图来自冻结规范／输入，不从 observed graph 复制出来“自比较”；builtin 对账继续要求独立来源。

### 5.5 一个具体测试程序示例（设计示例，尚未执行）

统一 topic/type、其余相关 QoS 兼容，W0 为 reliable、W1 为 best_effort，R0 为 reliable、R1 为 best_effort。依据 [ROS QoS 兼容性说明的本地副本](/tmp/r2f-brainstorm-research/ROSQoS.txt) 的 reliability 表，期望兼容边为 W0→R0、W0→R1、W1→R1，不含 W1→R0。**这只是期望兼容关系，不是实际匹配证据，也不是规定必须在固定毫秒数内匹配。**

1. 选择首次到达前缀，或经过 discovery drop/restore 的到达前缀；保存不同 prefix_id。
2. 用 API 创建拓扑，取得真实身份和独立观察；不把预计状态写成 reached。
3. 自主启动 W0/W1 workload，记录真实开始／结束时间。
4. 在明确 barrier 后销毁 W1，再以相同逻辑名、新 generation、reliable QoS 重建；不调用已经销毁的旧句柄。
5. 检查新 W1 的兼容关系、旧代际残留／错误关联及与 R0/R1 的实际可观察活动。QoS 从 best_effort 到 reliable 通过重建实现，不假设 immutable policy 可以原位修改。
6. 若出现禁止边／代际混淆并有充分证据，进入候选链；只有 count 或窗口尾部缺失时不能补造逐对 fail。
7. 保存完整序列，尝试删去 W0 背景负载、缩短前缀、去掉某 reader，观察异常是否仍成立；每次缩减是新的重放产物，不改原证据。

此例说明：模型负责期望关系与适用条件，输入模型负责生成拓扑和变化，状态模型负责观察迁移与代际，runtime 负责真实重叠，oracle 负责区分 bug、允许变化和证据不足。没有任何一层能单独替代其余层。

## 6. 分阶段任务、写集与验收

以下 P0–P7 是本文的**工程工作包编号**，不是原 taskbook Phase 编号，也不是 S1–S8 功能分类。

### P0：冻结研究合同与扩展接口

**交付**：S1 全覆盖 map、ActionSpec/Program/receipt schema、规范追溯表、范围与 identity 变更记录、实验指标定义。

- 先明确 E1 新 target identity，不改旧四 target manifest；新增 scope 扩展清单，旧配置保持兼容。
- 每条拟上线 oracle 必须具备规范／API 依据、适用层、观察方式、允许结果及未知处理。
- 规定工具要求与 SUT 守卫分离；固定非法测试与 negative 控制的区别。
- 清理两个小遗留：cleanup argv 子串误报用精确进程身份／归属验证修复；给 provider audit 单项测试补可追溯 fixture。只修真实问题，不为了“无 skip”编造 fixture。

**拟写集**：core 的 `semantic_target.py`、`scenario.py`及新 `action_model.py`；runtime 的进程检查相关代码；各自专属测试；文档 agent 写覆盖 map／规范追溯与决策。runtime 修复文件由派遣前实查归属，不能凭历史路径猜测。

**验收**：schema 正反例；未知 guard、未定义 outcome、缺 spec_ref 不生成强断言；旧 capsule hash 不变；新输入字段未知／未消费明确报错。无需重跑全矩阵。

### P1：1W/1R 的模型到真实执行纵切

**依赖**：P0 核心 schema。

- 先把现有 late endpoint 和 drop/restore 的可表达操作编译成程序，复用统一 registry/runner/carrier。
- 同一目标状态保留至少两种有意义前缀类别（首次创建、drop恢复后；重建后在 P2 增加）。
- 完成合法输入、参数边界和明确守卫违例的合同路径；没有合适 DDS 拒绝合同就先做“合法但不得匹配”，不硬造例子。
- 证明模型字段改变导致计划与真实 effect 相应改变，而不只是报告多几个字段。

**拟写集**：core 新 `planner.py`、`program.py`；campaign `identity.py`、`runner.py`；S1 `providers.py`、`targets.py`；runtime 保持 pair adapter；专属新测试文件。

**验收**：离线可达／不可达／预算耗尽；两 RMW 各真实最小正反路径；原 guard=false blocked 行为不被全局放开；真实 mutated 无 hit 仍不能通过。development 证据独立，不提前 seal。

### P2：多 endpoint 与图合同

**依赖**：P1 纵切通过。

- worker 改 endpoint registry；新 `topology_runtime.py` 作为现有 carrier 的后端，不建第二 runner。
- endpoint generation、创建／删除回执、逻辑／真实身份关联接线。
- 实现各拓扑族、合法／不兼容关系、remove/recreate 后的代际和 stale 观察。
- 定义逐对不可观测时的 probe／unknown 处理，先不混厂商。

**拟写集**：runtime `worker.py`、`ros2_pair.py`、新 topology runtime；S1 `facts.py`、`oracles.py`、新图合同；instrumentation 仅在所需身份／观察接口范围内扩展。

**验收**：1W/2R、2W/1R、2W/2R、混合兼容图；同名重建 generation 必须变化；变造 count 不能骗过逐对证据测试；两 RMW 真实动作／raw 绑定。未确认 participant 关联不能计该覆盖。

### P3：自主 workload 与并发交错

**依赖**：P2 的身份和观测闭合。

- 一次命令启动批量 workload；加入 barrier、并发组、取消／结束／回收协议。
- 从多进程交错开始，再做有定义的线程并发。
- 保存真实动作区间、观察丢失、节流和 workload 完成度。

**拟写集**：runtime 新 `workload.py`、`worker.py`；core 的 schedule schema；S1 合同只消费已审核的相关事实。

**验收**：真实 workload 与 create/destroy 存在重叠证据；取消和异常退出不泄漏 worker；控制器不再每条 publish 等 ACK；S1 oracle 不要求跨 writer 全局排序。吞吐改善给测量值，不预设倍数。

### P4：持续 episode、重放与缩减

**依赖**：P2；可在 P3 workload 稳定后集成。

- 一条 episode 一个输入身份，step 仍用现有执行模型和嵌套 receipt。
- 历史程序、预算 reset、合法拒绝后检查、完整 replay 和程序 shrink 接入统一 candidate lifecycle。
- 故障保全不依赖完美 clean shutdown；原始异常与重放分目录保存。

**拟写集**：campaign `runner.py`、`replay.py`；core `replay.py`、新 `program_reducer.py`；现有 reports/candidate 链最小扩展。

**验收**：独立／持续可各自执行与重放；预算触发只结束 episode 不删证据；缩减不能丢掉异常所需前缀或把 observer failure 换成原异常；真实健康 replay、受控 failure retention 分开。控制故障测试不计自然候选。

### P5：前缀 corpus 与反馈调度

**依赖**：P1 的模型／计划、P2 的状态观察、P4 的稳定历史输入。

- 规范图、观察图、前缀 corpus 分开持久化。
- 实现目标→前缀→动作的选择策略及 20% 探索保底；固定矩阵 scheduler 继续保留。
- provenance、状态抽象版本、调度分数／理由、成本和 mutation 命中反馈可追溯。

**拟写集**：campaign `scheduler.py`、新 `corpus.py`、`feedback.py`；core 新 `state_abstraction.py`；统一 report 增加探索指标。

**验收**：相同反馈日志／seed 得到相同调度决策；GUID/PID/time 变化不刷覆盖；同目标不同历史前缀可被实际选中；新异常不修改合法模型；tool unknown 不获覆盖奖励。

### P6：统一冻结、回归和研究对照

**依赖**：P1–P5 完成，规范与观测合同审查通过。

- 集成后运行相关全测试；一次主 agent GO 后提交行为源码、冻结 source，再按原四 target 顺序重跑 72 required＋24 negative。
- 新探索 smoke／实验使用独立新 target/run identity，不套旧 seal；正式实验期间不改 source/model/oracle。
- 运行第 7 节预登记实验；每个 RMW 分别分析，不把同进程中的动作当独立重复。
- 真实异常进入统一 admission→triage→replay/具名 non_replayable 流程；收集最小程序、规范理由、raw、环境身份与复现率。

**验收**：旧合同无退化；新功能真实证据齐全；实验预算、版本、指标与失败完整；候选为 0 时也产生诚实结论，不改统计规则。仅一次最终全矩阵不是禁止发现集成问题后必要重跑，而是避免每次小改动机械重跑。

### P7：广义 S1 剩余覆盖裁决与 E2 设计

**依赖**：P6 的覆盖地图与瓶颈证据。

逐项处理 lease、filter、topic/type、participant、可选网络时序、混厂商与内部观察缺口，明确哪些是公共 API 未接线、哪些是规范／观测能力不足、哪些是覆盖预算不足。对暂缓项制定具体下一包，而不是宣布完整 S1 已成熟。

快照只有在测量显示启动／前缀恢复确为主要开销时立项。建议触发条件为该开销占端到端预算超过 30% 且 batch／episode 优化后仍显著；这个阈值是工程决策，不是理论界限。方案须覆盖所有参与方、网络、timer、epoch与外部副作用；不能把多线程单 worker 的 fork 称作分布式一致快照。

## 7. 如何论证方法可行，而不是只交付一个工具

### 7.1 预登记研究问题

- **RQ1 可执行性**：模型生成的计划有多少真正到达目标，并形成可判定的动作结果？
- **RQ2 模型收益**：相同动作空间和预算下，模型约束／到达规划是否比语法合法随机生成提高有效迁移和目标覆盖？
- **RQ3 反馈／历史收益**：多前缀与状态反馈是否比均匀选择更快覆盖新关系、稀有迁移和历史组合？
- **RQ4 效率与真实性**：批量／持续执行节省多少成本，是否维持证据充分性及重放能力？
- **RQ5 缺陷发现能力**：是否产生可归因的真实 SUT 候选、可复现异常及独立根因？没有真实异常时该问题仍未被正向证明。

### 7.2 对照组

| 组 | 设置 | 用途 |
|---|---|---|
| B0 | 当前固定场景策略，通过兼容 adapter 运行 | 工程基线；因空间较小，不作为模型因果收益的唯一对照 |
| B1 | 相同动作／拓扑边界、语法合法随机程序，无语义规划 | 检验模型到达与约束贡献；仍保留工具健康门槛 |
| B2 | 模型规划＋变异，均匀前缀／动作选择 | PBT 核心方法，不使用反馈 |
| B3 | 完整方法：模型＋多前缀＋反馈 | 目标实现 |
| A-env | B3 去掉环境变异，保持其余预算 | 分离环境变异贡献 |
| A-history | B3 限制单前缀／短历史，其他条件尽量相同 | 检查历史收益；明确它同时改变可达历史空间，不能只解释成吞吐差异 |

先跑 B1/B2/B3 的试点；正式比较再加入 B0 和两项消融。独立／持续、串行／并发的性能比较作为单独成对实验，不在模型消融时同时换 runtime，以免混淆。

所有组使用相同 SUT 版本、RMW 配置、硬件、动作上限、oracle 和外部评估指标。完整方法用于调度的自身 state_id 计数不能成为唯一胜出指标；另用预登记的合同分支、动作关系组合、身份已确认的图迁移作为共同评价，并保留原始 trace 以便统一离线重算。

### 7.3 预算与重复（提议默认值，批准后 P0 固定）

- 试点：B1/B2/B3 × 两 RMW × 3 个独立 seed × 每轮 15 分钟，约 4.5 小时纯执行预算，不含设置、回归和 triage。目的为校准预算与观测稳定性，不宣称统计充分。
- 正式：六组 × 两 RMW × 5 个独立 seed × 每轮 30 分钟，约 30 小时纯执行预算；主 agent 唯一安排 campaign，轮次顺序随机／交错，防止时间段负载偏差。
- 每轮主要按 wall-clock 限额；同时输出固定已执行动作数的截断比较，避免“有效动作多”和“单位动作探索好”混为一谈。重置／规划／观测开销都计入 wall-clock，候选 triage 单列预算。
- 5 个 seed 只是起始重复数，不保证功效。报告每轮原始值、中位数和区间，不把每个动作／endpoint 当独立样本；结论不稳定时增加预先记录的重复，不挑选有利 seed。
- 试点结束后只允许基于预先写明的工具可用性／观测成本调整正式预算；调整先登记再运行，不用正式结果反向挑胜出指标。

### 7.4 必须报告的指标

1. 生成数、schema-valid、可提交数、实际动作数、目标到达率（含明确分母）。
2. mutation submitted/configured/hit/effect 比例；合法与违例测试分层，不因预期拒绝而算“无效输入”。
3. 完整／不完整观察、可判定比例、unknown 原因分布、工具失败及预算结束。
4. 合同分支、关系组合、真实观察迁移、历史类别覆盖及随时间曲线。
5. 规划／启动／前缀／测试段／观察／reset 耗时，吞吐、CPU/RSS、日志量和 observer drops。
6. candidate observed/admitted/confirmed、根因去重、每个候选的重放成功数／尝试数；同一 bug 不按输入个数膨胀。
7. 独立 episode 数与 episode 内动作数分开；controls、故障注入和历史已知缺陷数据单独展示。

### 7.5 结论门槛

- **工程可行**：模型字段真实驱动输入和执行，主要路径具备真实可观察证据；旧合同不退化；独立／持续、重放和失败保留可用。
- **探索效果有支持**：共同指标下 B2/B3 相比合适基线在多个独立轮次有一致增益，代价和不确定性已报告；若没有增益就如实否定本次实现的收益假设。
- **真实缺陷发现得到支持**：存在真实 SUT 异常、规范／故障依据和可信归因；若没有，不能用合成负控证明该结论。
- **2–3 个候选目标**：仍作为研究目标追踪，不作为可以预先保证的软件功能验收项。零候选时可以完成工程阶段与提交研究报告，但不能标记用户整体目标完成。
- 可选历史真实 bug benchmark 用于验证灵敏度，必须绑定真实版本／修复与独立来源，单列结果；不能算新发现或冒充当前源码自然候选。

## 8. 子 agent 分工与主 agent 决策

批准后沿用六个职责，按阶段启用而不是六个 agent 无依赖地同时改接口。用户指定 `gpt-6.1-sol`；派遣时显式使用该模型并记录实际工具返回。历史会话曾报告显式模型不可用；本轮没有验证可用性，不能宣称已经成功派遣，也不能默默替换模型。

| owner | 主写集／责任 | 禁止交叉写入 |
|---|---|---|
| core agent | core 模型、program、planner、state abstraction；独占对应新 tests | 不改 runtime/campaign/docs |
| runtime agent | worker、pair/topology、workload、process 身份清理；对应 tests | 不改 core schema、网络隔离实现、docs |
| campaign agent | identity/runner/scheduler/corpus/replay 接线、reports、CLI；对应 tests | 不另建 runner/candidate，不改其他 owner 私有实现 |
| 网络隔离 agent | discovery network/namespace carrier、环境回执；对应 tests | 不与 runtime 同改 worker；不自主发起 ROS |
| vendor观察 agent | instrumentation、S1 facts/oracles、规范到观测核验；对应 tests | target/provider 由 core/campaign 接口定稿后指定单一 owner，不双写 |
| 文档／验收 agent | docs、README、AGENTS、scope配置变更说明、证据与实验报告 | 不改业务代码，不跑 ROS，不自行 commit |

接口协调：P0 由主 agent 审核 contract；每轮派遣列具体文件、输入输出 schema、禁止写集和检查命令。`config/s1_scope.json` 与新 scope 配置、S1 targets/providers 初始由 campaign agent 独占，core 提供 schema，观察 agent 提供合同，不同 owner 不直接编辑同一文件。测试文件同样独占，不能只按目录口头分工。

主 agent 负责分析、接口裁决、review、唯一本地 campaign 调度和最终 GO；真实执行可以派唯一 runtime/campaign owner 子 agent，其他 agent 不同时启动 ROS。每次正式 run 前检查 registry/domain/state locks、owner/process/RTPS port、source/HEAD/revision/environment manifest；不强抢旧 owner 或杀旧仓库任务。

普通工程问题由主 agent 决策。模型不可用、必须改变用户范围／新增依赖权限等真正需用户决定事项记录到 open-items，保留准确报错；可推进的其他分支继续。不能记录问题后无限重试相同失败或换模型后假称原模型。

## 9. 暂停、回退与完成报告

- 当 source/model/oracle 在正式运行中变化、domain/owner不明、observer失去可信性时中止受影响 run，保留原始记录并定位；不能用降门槛恢复绿色。
- 失败发生在某阶段的局部链路时修复最小范围，相关离线／smoke验证后继续；不反复从四 target 全矩阵开始。
- 原始失败、旧证据、旧 source seal 不覆盖。源码变更后的正式证据以新 source 重跑为准。
- 若新能力出现但缺原始 receipt／身份／source，则状态为部分验证，不计已完成。
- 每阶段交付：实际文件、测试命令／结果、未运行内容、source identity、新旧 target/revision、RMW/domain/run_id、mutation/effect、raw/replay、候选、范围与成熟分母影响。

## 10. 批准点与本轮实际产物

请用户批准的是 **E1 范围包＋P0–P7 的实施顺序及上述默认试点／正式实验预算**；其中 P7 只交付 E2 的证据化方案，E2 未自动获准。批准包含 endpoint remove/recreate、模型导向自动探索和场景内并发这几个相对初始范围的明确扩展。

批准不等于保证 2–3 个候选，也不等于授权篡改旧合同、绕过身份／锁或自动切换模型。新行为提交和真实 campaign 仍由主 agent 在检查通过后发出阶段 GO，无需用户处理日常工程细节。

本轮实际产物只有本文；不改业务代码、测试、配置、历史验收或旧仓库，无新 ROS run、无新 candidate、旧 72 分母增量为 0。文档静态核查与链接检查不能当作业务能力验证。
