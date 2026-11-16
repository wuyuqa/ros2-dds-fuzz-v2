# S1 fuzz 能力源码核查与收窄建议

> **详细派工版（待批准）**：当前开工依据以 [S1 变异面扩展实施计划](/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-fuzz-expansion-implementation-plan-2026-11-16.md) 为准；本页保留源码核查／长期背景，不是另一个并行实施任务。

日期：2026-11-16。状态：主 agent 源码核查及待批准建议；不是新增实现或运行授权。本轮主 agent 协调写集仅限本文及此前自行新增计划的状态提示，不修改其他 agent 文档、业务代码、配置或旧仓库。

## 1. 总结

用户指出的四类缺口基本属实，需精确区分：不是完全没有语义约束、参数化输入、状态执行和环境施加，而是这些能力主要服务四个固定 target 的验收，尚未组成持续生成、组合变异和反馈迭代的 fuzz 主循环。

源 HEAD：`e448febec4fa9f3c24f22eaac7954517e7b4a301`；本轮只读重算 behavior：`e7f92897dd6d122a874f7686a24419173d110abd765106ce5e16e1289e732cfa`，source_frozen=true，dirty_behavior_paths=[]。以下是当前 v2 结论，不把旧仓库描述的能力当作 v2 已实现。

## 2. 四项核查

### 2.1 target 字段变少：不能凭数量判断，但声明式完整性确实不足

[SemanticTarget](/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/core/semantic_target.py)已有 state_preconditions、state_steps、mutation_slots、observation_contract、expected_effect_set、rule_contract、evidence_policy 等；不是一个空壳。

但 MutationSlot 只有 mutation_kind、carrier、parameters。target 的 slot 实例大多只有前两个字段，未给出足够的可生成值域、边界分类、变异算子、可组合条件、顺序依赖与每层 effect 关联。规范引用／适用限制、fact 与 rule 的完整声明、引用完整性检查也没有形成原功能文档描述的统一目录。

部分真实语义在 [identity.py](/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/campaign/identity.py)、runtime 校验、[facts.py](/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/modules/s1/facts.py)、[oracles.py](/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/modules/s1/oracles.py)里，不能把 target 未列出的内容全部说成未实现。但生成器不能仅靠当前 slot 自动获知这些空间和约束。

[旧功能文档](/home/wuyuqa/ros2-dds-fuzz/研究内容与当前困难详述-ros2-dds-fuzz%20copy.md) §2.2.2 明确写了 mutation_slots、rule_ids、fact 的类型／来源／缺失策略。它是方法和历史实现参考，不是必须原样复制旧字段数或旧代码。

**应补：**语义模型声明／引用 mutation、fact、rule 合同；输入模型按合同实例化并执行变异。FactProducer 与 Oracle 保持实际采集／判定实现，不把采集结果硬编码进模型，也不要求把全部 Python oracle 重写成新语言。

### 2.2 seed 与 mutator：有默认输入，无 corpus 演化和通用算子选择

[Scenario](/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/core/scenario.py)的 seed 是整数标记；[runner.py](/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/campaign/runner.py)第 46–60 行按固定矩阵生成输入并选择固定 mutation_kind；semantic_input/projection_input 提供内置默认配置及用户参数覆盖。

[providers.py](/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/modules/s1/providers.py)的 S1MutationProvider.prepare 包装已有参数与确定性层说明，并不执行 corpus 选择、边界抽样、随机变异、亲子派生和反馈入库。[core/mutation.py](/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/core/mutation.py)只定义执行 MutationReceipt，不是 mutator 算法库。

本轮离线调用真实 compile_scenario 与 provider.prepare：late_endpoint mutated 在其余参数相同、seed 分别为 1 和 999 时：

- prepared_equal=true；final_input_equal=true；steps_equal=true；
- effective input 除 seed 外全部相同；effective_hash_equal=false。

结论：当前该路径改变 seed 不改变测试内容；不能拿不同 seed/hash 的数量当实际输入多样性。后续需区分 corpus_seed_id（结构化原始输入）、rng_seed（生成随机种子）和 parent_input_id（变异血缘）。保留旧 identity，不为去重改写旧 hash；另设语义输入指纹做多样性统计。

### 2.3 状态模型：有步骤执行，缺探索状态图

[StateCondition/StateStep/StateExecutor](/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/core/state_machine.py)有前置求值、顺序执行、before/after receipt 及 blocked/failed 行为；不能说“完全没状态模型”。

但没有供 fuzz 使用的归一化 state_id、迁移边 corpus、新状态／新边覆盖、同状态多前缀管理或基于这些反馈的调度。[scheduler.py](/home/wuyuqa/ros2-dds-fuzz-v2/src/r2f/campaign/scheduler.py)明确为固定 required 矩阵。

必须区分三者：execution_stage 是工具执行证据阶段；ROS graph_snapshot 是端点发现图观测；fuzz state graph 是抽象 SUT 状态及操作迁移图。前两者已存在，不等于第三者已建成。

本轮最小补充应是从真实 receipt 提取小型状态／迁移签名用于 corpus 留存，先不做通用规划器。观察新状态不自动修改规范允许集合；不完整观察不计成功到达新状态。

### 2.4 变异链：字段预留不等于多层组合执行

Scenario.effective_input 已有 mutation_layer_order、input_layer_receipts；但 provider.prepare 返回一个 `[layer]`，provider 的 `_KINDS` 为每 target 固定主 mutation，runner 也固定选择。未找到逐层算子应用、资格／冲突检查、前后输入衔接、组合迭代和反馈繁殖的实际执行循环。

额外离线核查：第三 target 声明 restore_after_discovery_interruption slot；把它作为单独 mutation_kind 构造 Scenario 能通过 slot 校验，但 provider.prepare 返回 `ValueError: unsupported S1 mutation scenario`。这不否认 runtime 已真实实现固定 discovery_drop→restore，只证明该 slot 不是可独立选择的通用变异算子。

已有 late reader 延迟、QoS variant、真实 discovery drop/restore；所以参数变异、环境变异不是零。缺的是系统化边界／等价类生成、环境维度选择、可组合步骤及持续迭代。固定 drop→restore 或一个 QoS dict 改多个字段，不等价于可组合变异链。

## 3. 这轮先做什么：最小 fuzz 闭环，不建通用平台

此前 P0–P7 大计划降为长期路线，不作为当前开工清单。当前建议仅四项，尚待用户批准：

### A. 补可执行变异面目录

在现有 SemanticTarget/MutationSlot 上扩展或引用版本化合同，列出：surface_id、字段／阶段、值域和单位、边界／等价类、算子、SUT 守卫与工具要求、carrier、事实来源、rule、允许效果、禁止残留、组合限制。

优先已有 carrier 可表达的 late-join timing、public endpoint QoS、discovery interruption 参数；按 target 合同限定，不能把 reliable/volatile 的交付断言直接套给所有 QoS。timeout、observer 采样间隔等工具预算单列，不冒充新增 SUT 变异面。非法值如被 SDK 或工具截断，明确实际到达层，不算 DDS hit。

### B. 初始 corpus＋实际 mutator＋有界组合

初始 seed 从已验证配置、代表性合法等价类／边界构造，均保存结构化输入和来源。输入模型注册真正的边界、枚举替换、范围采样及环境参数算子；同预算保留边界覆盖与普通随机探索，不能只围绕历史 issue。

先实现单算子，再实现少量有意义的两层链；参数→环境只是类别示例，须有真实 runtime 支持且组合不破坏目标适用性。每层保存 before/after 输入指纹、参数、顺序和来源；later layer 抵消 earlier layer 应明确 no-op／overridden，不虚报两个 hit。

执行仍走原 runner/carrier。每个施加步骤单独保留实际 receipt，沿用既有嵌套 MutationReceipt 七字段合同，不把 input_layer_receipts 当执行回执，也不为支持组合破坏旧 capsule。聚合成功须满足该链各必需施加步骤，不能只看最后一层。

### C. 持续生成循环＋最小状态反馈

循环为：选 corpus seed → 按 slot 资格选算子／链 → 编译／验证 → 真实执行 → fact/oracle → 记录新输入／状态边／异常 → 更新 corpus → 下一轮。

固定 required matrix 保留作回归，exploratory 路径添加生成循环，不把 repeat 改名叫 fuzz。首版用有限状态签名＋覆盖/稀有性反馈即可，不先做通用 ActionModel、复杂前缀规划、长生命周期平台或快照。

这里“持续生成”不要求先实现“持续复用同一 ROS 进程”；第一版可独立场景执行，把生成能力先做实。多 endpoint/churn、重复环境切换在核心闭环验证后，以具体变异面增量接入，而不是本轮先重建全部 runtime。

### D. 最小真实验收＋独立 agent 审核候选

验收重点：不同 seed 产生实际不同输入；边界／等价类有覆盖账；每条宣布支持的链有真实施加证据；generation-only、rejected、ineffective 和未知原因单列；新观察能影响下一轮输入选择；重放保留最终输入与链。

不以 mutator 名称数、input hash 数或多次固定 pass 代替覆盖。旧四 target 的 scope／72 分母不动；新增组合若超出旧合同，使用明确新 target identity／reviewed scope，不能静默复用旧 seal。现有配置中的自动探索限制需在用户批准后显式更新，不能绕过开关。

工具负责异常收集、证据闭合和候选入账；**真实与否由独立审核 agent 检查 raw、规范适用性、施加归属、工具健康、重放／non_replayable 及替代解释，主 agent 裁决**。审核 agent 不修改被审核原始证据；结果为 supported/rejected/needs_evidence 并附依据，工具 fail 或 LLM 判断都不能单独当 confirmed。明确的 harness control 始终排除。

## 4. 本轮验证范围

执行内容：源码定向读取、seed/slot 两项离线探针、source identity 重算。未启动 ROS、未跑完整测试、未改代码或配置、未派 agent、未 commit。新增文档结论以源码证据为依据，不形成新的执行成熟计数或自然候选。
