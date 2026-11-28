# 任务书原文存档与适用顺序

存档日期：2026-11-14。任务来源是规格文件 `/tmp/ros2-dds-fuzz-v2-s1-handoff.md`，不是实现完成证明。用户本轮指令及后续主agent裁定优先；原文保留，包括其中与当前执行范围冲突的历史交付边界。解释见 [决策记录](decisions.md)，验收映射见 [验收矩阵](acceptance.md)。

用户最新要求（主agent转述）覆盖完整 S1，不仅是第一轮 Phase 3；主agent声明当前执行限于公共 API 正常互操作、可靠性及证据工具，不实施自动漏洞搜寻、恶意 wire payload 或漏洞复现。原任务书中全部成熟、插装/候选探索等目标须区分当前范围和待核实证据，不得预先承诺原 S1 全部成熟或 2–3 个真实 SUT candidate。

原文 SHA-256：`2b476e5903d39f6665b1055cebc01556bd2336f786d4c8089ef8446a21afecc2`。下列分隔标识之后的字节为原文件的完整副本，未修改原文。

<!-- BEGIN ORIGINAL TASKBOOK -->
# ROS2 DDS Fuzz v2 重建与 S1 纵向闭环交接任务

日期 2026-11-14

## 1. 目标

建立一个完整的 ROS2 DDS fuzz 工具框架，再逐步完成 S1 小模块、S1 整体和后续模块。S1 是主框架注册的第一个模块，不能成为独立 runner、独立状态机或独立报告系统。

第一条 required slice 固定为 `discovery.late_endpoint` 加 `late_joiner`。完成后再按固定顺序扩展 S1 的其他 target。

旧仓库只作为证据和设计参考，不直接复制未提交代码和旧 seal。

参考材料

- `/home/wuyuqa/ros2-dds-fuzz/研究内容与当前困难详述-ros2-dds-fuzz copy.md`
- `/home/wuyuqa/ros2-dds-fuzz/AGENTS.md`
- `/home/wuyuqa/ros2-dds-fuzz/docs/CODEBASE_GUIDE.md`
- `/home/wuyuqa/ros2-dds-fuzz/docs/plans/2026-11-12-full-fuzz-maturity-rebaseline.md`

## 2. 旧仓库交接事实

旧仓库 HEAD 是 `e18e93b1`，worktree 有未提交改动。最新 S1 Batch-02 主要结果如下。

- target-bound 输入类别 8/10
- 有真实 channel evidence 的类别 5/10
- 可判定 target 4/4
- S1 专属 known-fault 18/18
- target-bound 行 94
- 实际执行行 80
- registered 但未完整执行 14
- primary rule fail 0
- supporting rule fail 2
- confirmed candidate 0

旧仓库暴露出 mutation receipt、effective hash、proxy carrier、replay、unknown 和 candidate attribution 同时混乱的问题。新仓库必须从最小纵向 slice 开始。

## 3. 完整框架和模块边界

建议目录如下。

```text
src/r2f/
  core/
    scenario.py
    semantic_target.py
    state_machine.py
    mutation.py
    execution.py
    observation.py
    facts.py
    oracle.py
    evidence.py
    replay.py
    candidate.py
    registry.py
  runtime/
    ros2_pair.py
    endpoint_observer.py
    wire_proxy.py
    process.py
    domains.py
  instrumentation/
    base.py
    fastdds.py
    cyclonedds.py
  modules/
    s1/
      module.py
      targets.py
      mutations.py
      carriers.py
      facts.py
      oracles.py
      instrumentation.py
    s2/module.py
    s3/module.py
  campaign/
    runner.py
    scheduler.py
    triage.py
  reports/
    run_report.py
    candidate_report.py
```

主框架提供统一的 `Module`、`TargetProvider`、`MutationProvider`、`Carrier`、`FactProducer`、`Oracle` 和 `CandidateLifecycle` 接口。S1 通过 registry 注册到主 runner。

第一轮只实现 S1 一个业务纵切片，但主框架必须具备以下八项能力。

1. 注册 module 和 target
2. 根据 SemanticTarget 编译 scenario
3. 执行带前置条件的 state steps，并写 receipt
4. 通过统一 carrier 应用 mutation，并验证 effect
5. 采集 raw、typed facts 和 oracle verdict
6. 创建、更新和审计 candidate lifecycle
7. 生成 replay capsule 并记录 replay 尝试
8. 生成完整 run report

这八项都必须有离线合同测试。第 1、2、3、4、5、6、8 项还必须有至少一个真实 ROS2 pair 集成测试。第 7 项必须同时测试 capsule input metadata 的离线生成、真实 execution metadata 的写入和 replay failure 的保留。

## 4. SemanticTarget 和状态模型

每个 target 必须包含以下字段。

```text
intent_and_scope
target_revision
state_preconditions
state_steps
mutation_slots
observation_contract
expected_effect_set
rule_contract
evidence_policy
```

状态执行器唯一使用 `execution_stage`。

```text
modelled
→ expressible
→ planned
→ precondition_executed
→ reached
→ mutation_applied
→ effect_observed
→ judged
```

`aborted` 是未完成阶段的终态。

`plan()` 只产生计划。runner 必须真正执行每个 step，写入 before/after receipt，并求值 `requires`。没有达到前置条件时不能进入 mutation，也不能把静态计划写成 reached。

### 4.1 expected_effect_set

`expected_effect_set` 说明 mutation 允许造成的效果以及窗口外禁止出现的 residual。第一条 `late_joiner` slice 使用以下格式。

```json
{
  "mutation_kind": "late_joiner",
  "carrier": "ros2_endpoint",
  "effect_id": "reader_created_after_writer",
  "window": "before_reader_match",
  "allowed_observations": ["reader_created_after_writer", "match_established"],
  "forbidden_residuals": ["post_match_delivery_loss", "wrong_endpoint_match", "identity_generation_mixup"],
  "fact_sources": ["endpoint_tokens", "graph_snapshot", "delivery_tokens"]
}
```

判定树检查的是 `observed_effect` 是否属于 `allowed_observations`，以及是否出现 `forbidden_residuals`。允许窗口内的预期影响不能直接算 SUT violation。只有窗口外 residual 或违反 primary rule 的行为才能进入 SUT candidate 评估。

### 4.2 target_revision

`target_revision` 是 per-target 字段。

- 初始版本为 `0`
- 每个 target 在第一次 seal 前最多修订一次，变为 `1`
- 修订必须记录触发证据、改动字段和成熟分母影响
- 该 target revision 改变后，该 target 的旧 evidence 失效，其他 target 不受影响
- 如果 revision 在该 target Phase 3 之后改变，该 target 的 18 个 required cell 全部失效，必须使用新 run_id、新 manifest 和新 replay capsule 重跑后才能进入该 target Phase 6
- revision=1 seal 后继续修改必须创建新 target identity 或 major revision

## 5. 初始 S1 scope 和执行顺序

初始 scope 固定存放在 `config/s1_scope.json`。

初始 target 清单

```text
discovery.late_endpoint
discovery.late_join_delivery
discovery.interruption_recovery
builtin.discovery_projection_reconciliation
```

初始 mutation 清单

```text
late_joiner
endpoint_qos_variation
discovery_drop
restore_after_discovery_interruption
```

以下内容暂不属于初始 S1 scope。

```text
discovery_delay
discovery_reorder
discovery_duplicate
lease expiry/renewal
endpoint remove/recreate
```

scope 清单只表示允许范围，不表示执行顺序。第一阶段执行顺序固定为：

1. `discovery.late_endpoint` 加 `late_joiner`
2. `discovery.late_join_delivery` 加 `late_joiner`
3. `discovery.interruption_recovery` 加 `discovery_drop` 加 restore
4. `builtin.discovery_projection_reconciliation` 加 `endpoint_qos_variation`

Phase 7 必须按这个顺序逐个增加 target。`discovery_delay`、`discovery_reorder`、`discovery_duplicate` 不在初始 scope 内。加入前必须走 13.2 的 owner escalation，更新 `config/s1_scope.json`、semantic model 或 rule、成熟分母和 run identity。

## 6. 两种执行模式

### 6.1 instrumentation mode

研发阶段允许源码插装、vendor trace、内部事件 hook 和内部状态观察。插装可以直接产生候选。

插装候选必须记录 candidate source、evidence tier、插装点、source 或 binary identity、事件时间、endpoint identity 和 control/对照。

黑盒复现是验证和证据增强，不是候选入账前置条件。

### 6.2 black-box mode

最终 black-box coverage 只使用 ROS2 公共 API、真实 ROS2 endpoint 和 endpoint-attached wire/config 路径。

插装候选使用相同 seed、mutation、RMW 和前置状态进入 black-box validation。black-box 失败不能删除候选，只更新 `blackbox_validation` 和 `evidence_tier`。

## 7. 统一状态和执行记录

### 7.1 canonical 状态

执行状态唯一使用 4.2 定义的 canonical `execution_stage`。

oracle verdict

```text
pass | fail | unknown | not_applicable
```

candidate lifecycle

```text
observed → admitted → triaged → confirmed
                      ↘ deferred
                      ↘ rejected
```

replay status

```text
not_run | pass | fail_timing | fail_environment | fail_artifact | inconclusive
```

`replay_unconfirmed` 是派生标签，不是 lifecycle state。它表示 candidate status 为 `admitted`、`triaged` 或 `deferred`，且 replay status 不是 pass。`confirmed` 不生成该标签。confirmed 的 replay 失败必须使用 `confirmation_basis=non_replayable_evidence` 或其他具名依据。

### 7.2 cell schema

```json
{
  "run_id": "...",
  "target_id": "...",
  "target_revision": 0,
  "campaign_kind": "required|exploratory",
  "execution_stage": "modelled|expressible|planned|precondition_executed|reached|mutation_applied|effect_observed|judged|aborted",
  "rmw": "...",
  "arm": "baseline|transparent|mutated|negative",
  "repeat": 0,
  "mutation_kind": "late_joiner",
  "carrier": "ros2_endpoint|wire_proxy|instrumentation",
  "mutation_parameters": {},
  "expected_effect_set": [],
  "mutation_receipt": {
    "planned": true,
    "submitted": true,
    "carrier_admitted": true,
    "configured": true,
    "applied": true,
    "hit": true,
    "effect_observed": true
  },
  "effective_input_sha256": "...",
  "observed_effect": {},
  "typed_facts": {},
  "oracle_verdict": "pass|fail|unknown|not_applicable",
  "raw_path": "...",
  "raw_sha256": "...",
  "replay_path": "...",
  "replay_status": "not_run|pass|fail_timing|fail_environment|fail_artifact|inconclusive",
  "candidate_status": "none|observed|admitted|triaged|confirmed|deferred|rejected",
  "candidate_source": "instrumentation|blackbox|differential|control",
  "evidence_tier": "instrumented|blackbox|blackbox_replayed|verified",
  "blackbox_validation": "not_run|pass|fail|inconclusive",
  "adjudicator_id": "...",
  "adjudication_method": "single_agent|peer_review|rule_engine",
  "adjudicated_at": "...",
  "confirmation_basis": "..."
}
```

`mutation_receipt` 固定使用嵌套形式，不支持平铺和平铺/嵌套混用。

### 7.3 effective input hash

hash 输入至少包括 target、target revision、mutation kind、carrier、mutation parameters、RMW、repeat、mutation layer order、各层 receipt 和最终提交 payload/config。

`campaign_kind` 不进入 effective input hash。它是 run 属性，不是输入属性。required 和 exploratory 在输入完全相同时必须得到相同 hash。

### 7.4 execution_stage 映射

| execution_stage | 必须满足的字段 |
|---|---|
| `modelled` | target manifest 和 target revision 存在 |
| `expressible` | carrier、RMW 和 runtime adapter 接受 scenario |
| `planned` | scenario 和 state steps 已编译 |
| `precondition_executed` | 每个前置 step 有 before/after receipt |
| `reached` | 所有 state preconditions 为 true |
| `mutation_applied` | mutation_receipt carrier_admitted、configured、applied、hit 都为 true |
| `effect_observed` | expected effect 或 residual effect 有 raw observation |
| `judged` | oracle 已返回四态 verdict |
| `aborted` | 有 abort_reason，且不得进入下一阶段 |

### 7.5 candidate minimum evidence

`observed → admitted` 必须满足：

- run_id、target_id、target_revision、RMW、arm、repeat
- 稳定 endpoint 或 instrumentation identity
- raw path 和 raw sha256
- observed effect 或 typed fact
- 对应 rule、内部合同或 differential assertion
- candidate source、evidence tier 和 provenance
- 未被判定为 harness、reach_or_planning 或 mutation_ineffective

mutation 来源还必须有 `carrier_admitted`、configured、applied、hit 和 effective hash。instrumentation 来源必须有插装点、源码或 binary identity、事件时间和 control/对照。

## 8. candidate、replay 和 triage

### 8.1 candidate 进入条件

`observed` 表示已经落盘一条带稳定 identity 的异常观察或 instrumentation witness。它需要 raw、source、timestamp、target、RMW 和 run_id。

`admitted` 表示 candidate minimum evidence 已满足，且责任尚未判为 harness、reach_or_planning 或 mutation_ineffective。

只有 `execution_stage=effect_observed` 或 `judged`，并且 candidate status 为 observed、admitted、triaged 或 deferred 的记录才能进入 candidate triage。前置条件未达到、mutation 未 applied 或未 hit 的 cell 在执行层结束，不进入 triage。

### 8.2 replay capsule

replay capsule 分为 input 和 execution 两部分。

input 部分可离线生成，包含 target、target revision、seed、RMW、arm、repeat、topic/name、message type、QoS、mutation、carrier、state steps、source manifest 和 effective hash。`origin_domain` 只作环境记录，不进入 hash，replay 使用新的隔离 domain。

execution 部分只能由真实运行写入，包含 execution_stage、precondition receipts、mutation_receipt、observed effect、typed facts、raw path/hash、replay attempt 和 failure reason。

所有 required cell 必须有 input metadata。已执行 cell 必须有 execution metadata。admitted 或 triaged candidate 必须有 replay attempt，或有具名的 `non_replayable` disposition。replay 失败不能自动删除 candidate。

### 8.3 责任归因树

`responsibility_layer` 只允许：

```text
sut | observation | harness | mutation
```

`triage_disposition` 只允许：

```text
candidate | pass | deferred | rejected
```

判定顺序固定如下。

```text
环境和进程健康吗
  否 → triage_disposition=rejected, responsibility_layer=harness, reason_code=environment_or_process
前置状态真实达到吗
  否 → triage_disposition=rejected, responsibility_layer=harness, reason_code=reach_or_planning，结束 triage
carrier_admitted、configured、applied、hit 都成立吗
  否 → triage_disposition=rejected, responsibility_layer=mutation 或 harness, reason_code=mutation_ineffective 或 tool_error，结束 triage
观察窗口和 endpoint identity 完整吗
  否 → triage_disposition=deferred, responsibility_layer=observation, reason_code=unknown_evidence
effect window 完整且 expected_effect_set 可求值吗
  否 → triage_disposition=deferred, responsibility_layer=observation, reason_code=window_incomplete
typed oracle 是否判定合同违约
  否 → triage_disposition=pass 或 rejected, responsibility_layer=observation, reason_code=oracle_pass 或 not_applicable
observed_effect 是否属于 expected_effect_set.allowed_observations
  是 → triage_disposition=rejected, responsibility_layer=mutation, reason_code=mutation_expected_effect
是否出现 expected_effect_set.forbidden_residuals
  否 → triage_disposition=deferred, responsibility_layer=observation, reason_code=attribution_insufficient
对照和 instrumentation provenance 支持 SUT 责任吗
  是 → triage_disposition=candidate, responsibility_layer=sut, reason_code=sut_contract_violation
  否 → triage_disposition=deferred, responsibility_layer=observation, reason_code=attribution_insufficient
```

## 9. S1 第一条 required slice

### 9.1 固定场景和 mutation

第一条 slice 固定使用 `discovery.late_endpoint` 和 `mutation_kind=late_joiner`。`endpoint_qos_variation` 留给 Phase 7 或后续 slice，不能和第一条 slice 混跑。

```text
创建 writer
→ writer 发布带序号样本
→ 延迟创建 reader
→ 确认 reader 创建在 writer 之后
→ 确认匹配
→ 发送匹配后的样本
→ 记录接收集合和顺序
→ 运行 oracle
→ 保存 raw 和 replay
```

### 9.2 required matrix

```text
target
  discovery.late_endpoint

mutation_kind
  late_joiner

RMW
  rmw_cyclonedds_cpp
  rmw_fastrtps_cpp

arm
  baseline
  transparent
  mutated

repeat
  0
  1
  2
```

总计 18 个 required cell。`campaign_kind=required`。negative/fault control 单独保存，不进入自然 candidate。

### 9.3 oracle

primary rule 固定为：

```text
discovery.late_endpoint.match_and_receive_after_join
```

合同是 reader 晚于 writer 创建、endpoint 建立预期匹配关系、匹配建立后窗口内样本满足 delivery contract、endpoint identity/generation 一致、发送和接收序列可关联。

diagnostic rules 可以记录 progress，但不能被报告器当成 primary rule。

## 10. known-fault control 规格

每个 target 必须登记 health、fault、mutation 和 transparency 四类 control。

| control_kind | arm | 预期 |
|---|---|---|
| health | baseline | primary rule pass |
| fault | negative | primary rule fail |
| mutation | mutated | mutation receipt hit |
| transparency | transparent | 输入保持不变，用于差分对照 |

四类 control 的 repeat 固定为 `0`、`1`、`2`，两个 RMW。required control 中 unknown 一律按 failure 处理。control 行不能进入 SUT candidate。

control 通过标准固定为：

```text
health       6/6 pass
fault        6/6 fail
mutation     6/6 hit
transparency 6/6 no_effect_or_expected_pass
```

transparency 的具体预期由 target control contract 声明。它必须证明 mutation 声明存在但最终输入和观察结果保持透明，不得只记录 arm 名称。

`discovery.late_endpoint` 的 fault control 使用 `fault_id=late_endpoint_delivery_omission`，在 reader 已创建并匹配后受控省略一个 post-match delivery，预期 primary rule fail。

## 11. unknown 和成熟分母

S1 第一条 18-cell required matrix 中 applicable cell 的 unknown 必须为 0。任意 required unknown 都 abort target seal。

exploratory campaign 的 unknown 默认上限为 applicable rows 的 10%，单 target 不超过 1 个。超过上限先修事实 producer 或观测链，停止扩展 mutation。

成熟分母为：

```text
required target × applicable RMW × required arm × repeat
```

registered、blocked、input-surface、control-only 和 exploratory 行不进入成熟分母。

`applicable RMW` 是同时满足以下条件的 RMW：

- 在 target scope 的 `rmw_applicability` 中声明
- 环境 manifest 中真实可用
- target 的 endpoint path 和 carrier 对该 RMW 可表达
- target rule 的 applicability predicate 对该 RMW 为 true

每个 applicable RMW 必须有环境 manifest、RMW identity、endpoint creation receipt、target identity、state sequence receipt、mutation receipt、raw/hash 和 rule verdict。RMW 不可用时必须写具名 `blocked:*` 或 `not_applicable`，不能静默减少分母。

## 12. Phase 0 到 Phase 8

### Phase 0  框架初始化

创建新仓库、registry、run identity、环境 manifest、ROS2 pair、锁和最小测试。

### Phase 1  核心合同

实现 SemanticTarget、StateCondition、StateStep、MutationSlot、canonical execution_stage、嵌套 mutation receipt、oracle、candidate lifecycle、replay capsule 和合同测试。

fake runner 必须覆盖：

```text
pass
unknown
not_applicable
mutation_ineffective
tool_error
candidate_status=deferred
candidate_status=rejected
```

### Phase 2  instrumentation

接入至少一个 RMW 的源码或内部事件观察，把候选写入账本，记录 provenance 和 black-box validation task。

### Phase 3  S1 第一条 slice

只执行 `discovery.late_endpoint + late_joiner` 的 18-cell required matrix。所有 cell 有 input capsule metadata，已执行 cell 有 execution metadata，candidate 有 replay attempt 或 non_replayable disposition。同期必须运行该 target 的 health、fault、mutation 和 transparency controls，分别达到 6/6 pass、6/6 fail、6/6 hit 和 6/6 no_effect_or_expected_pass。

### Phase 4  instrumentation 到 black-box 验证

如果存在 instrumentation candidate，使用相同输入做两个 RMW、三个 repeat 的 black-box validation，agent 根据责任树给出 disposition。

如果没有 instrumentation candidate，Phase 4 不得自行制造候选，也不需要阻塞后续 scope。此时执行一个 bounded black-box campaign，记录 `instrumentation_candidates=0` 和完整的 zero-candidate 或 blackbox candidate disposition，然后进入 Phase 5。

### Phase 5A  S1 第二条 slice

完成 `discovery.late_join_delivery + late_joiner`。它使用与 Phase 3 相同的 18-cell 矩阵，只替换 target、primary rule 和 target-specific expected_effect_set。

### Phase 5B  S1 第三条 slice

完成 `discovery.interruption_recovery + discovery_drop + restore`。它必须使用两个 RMW、baseline/transparent/mutated 三个 arm 和 repeat `0/1/2` 的 18-cell required matrix，另行保存 fault control。

`discovery_delay`、`discovery_reorder`、`discovery_duplicate` 不在初始 S1 scope 内。加入前必须走 13.2 owner escalation，更新 `config/s1_scope.json`、semantic model/rule、成熟分母和 run identity。未经 escalation，不得执行这些 mutation。

### Phase 6  per-target seal

每个 target 的全部 evidence 必须来自该 target 的同一个 target_revision。该 target revision 的 required matrix 必须完整重跑，并且同时满足：

- required matrix 的所有 18 个 cell 都有真实执行记录
- applicable RMW 的 unknown 为 0
- health control 6/6 pass
- fault control 6/6 fail
- mutation control 6/6 hit
- transparency control 6/6 no_effect_or_expected_pass
- 每个 executed cell 都有 raw path 和 raw hash
- 每个 mutation cell 都有有效 input hash 和 mutation receipt
- 每个 executed cell 都有 execution metadata
- 每个 candidate 都有 replay attempt 或具名 non_replayable disposition
- 每个 candidate 都有 triage disposition、responsibility layer 和 reason code
- report、manifest、catalog、rule、target revision 的 hash 一致

任一条件不满足，target 保持未封存状态。

### Phase 7  扩展 S1

按 5 节固定顺序逐个处理 scope 内 target。scope 清单顺序不自动代表执行顺序，任何 scope 外扩展都必须先走 owner escalation。

### Phase 8  进入 S2

只有 S1 target disposition、语义模型、状态前置、candidate lifecycle、replay contract 和 per-target seal 都稳定后，才进入 S2。

### 第一轮交付边界

第一轮必须完成到 Phase 3 结束。交付物包括：

1. 新仓库初始化和 module registry
2. SemanticTarget、StateCondition、StateStep、canonical execution_stage 和嵌套 mutation receipt
3. candidate lifecycle、replay capsule、unknown gate、control contract 和 attribution tree 的合同测试
4. 一个真实 ROS2 pair
5. `discovery.late_endpoint + late_joiner` 的 18-cell required matrix
6. input capsule metadata、execution metadata、raw/hash、oracle verdict 和 run report
7. zero-candidate 或 candidate disposition 报告

第一轮不进入 Phase 5、Phase 7 或 S2。Phase 4 只有在 Phase 3 的 target evidence 完整后才开始。

## 13. abort、升级和环境隔离

### 13.1 abort 条件

- source repo、HEAD 或 target revision 中途改变
- required control 缺失或结果不符合规格
- required unknown 超限
- mutation hash 无法区分不同最终输入
- hit=true 但 configured/applied=false
- state step 只存在于 plan
- candidate provenance、raw identity 或 responsibility 缺失
- domain 或 state directory lock 冲突
- 旧仓库 campaign 进程或 owner 无法排除

### 13.2 owner escalation

以下情况必须先升级给 owner。

- 新增 S1 target、rule、mutation slot 或 carrier
- 改变 S1 scope 或成熟分母
- 修改已 seal 的 target revision
- 使用当前范围外的 native DDS participant 或安全模块
- 当前 RMW 没有可行的 endpoint-attached carrier

### 13.3 环境隔离和 domain 冲突

第一阶段固定 `environment_mode=ros_overlay`。加载 `/opt/ros/jazzy/setup.bash`，再加载新仓库 overlay，依赖使用锁定文件。

run 开始前必须完成：campaign registry lock、domain lock、state-directory lock、旧仓库进程扫描、旧仓库 run lock/owner 检查、ROS_DISTRO/RMW/source repo/HEAD/target revision 校验和 environment manifest 写入。

源码插装还必须使用独立的 instrumentation build root、独立 binary path、独立 compile manifest 和独立 process group。插装源码只能来自新仓库或明确的只读源码副本，不能直接改写旧仓库 worktree。instrumentation binary 的 source commit、patch digest、compiler flags、loaded library path 和 symbol manifest 必须写入 candidate provenance。black-box run 不得加载 instrumentation binary 或 instrumentation-only environment override。

domain 冲突检测同时使用：

- domain lock 和 owner payload
- RTPS derived port block 的 bind/probe
- 活动进程命令行中的 ROS_DOMAIN_ID、run_id、state directory 和 source repo

任何一层冲突或无法确定都返回 `domain_conflict` 并 abort。

## 14. 每次交付报告

- HEAD 和 worktree
- target、target_revision、domain、RMW、run_id、campaign_kind
- execution_stage 及其映射
- state sequence 和每一步 receipt
- mutation kind、carrier、参数和层数
- mutation_receipt.planned/submitted/carrier_admitted/configured/applied/hit/effect_observed
- expected_effect_set 和 observed_effect
- effective input hash
- typed facts、rule_id 和 oracle verdict
- raw path/hash、replay input metadata、replay execution status 和失败原因
- candidate_status、candidate_source、evidence_tier、responsibility_layer、reason_code
- 测试命令和结果
- abort 或 escalation 状态
- 是否改变成熟分母

## 15. 建议技能

- `mattpocock-skills:codebase-design`
- `mattpocock-skills:tdd`
- `mattpocock-skills:implement`
- `mattpocock-skills:diagnosing-bugs`
- `mattpocock-skills:writing-for-agents`
