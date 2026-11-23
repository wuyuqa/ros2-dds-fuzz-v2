# 只读验收审阅：现有证据 gate 的三个具体缺口

日期：2026-11-14。来源：主agent派遣，要求对照 `docs/acceptance.md` 最多列五项重大具体缺口。本页仅列三个；代码仍在开发，结论针对下列源码快照，不是最终验收失败或整仓不可运行结论。文档子agent未改代码、配置、测试、raw/运行产物，未启动 ROS 或 campaign。

方法：只读源码、现有 runtime-smoke 产物；以 `python3 -B` 执行不落盘的内存合同探针，禁写 pyc。探针构造的假数据只用来验证合同拒绝条件，不写候选账本、不计 ROS evidence。没有完整已冻结真实矩阵用于本次 seal 端到端反证，静态结论不宣称完整 seal 已被实际绕过。

## R001：空成员的 endpoint identity 可以进入 admitted

关联验收：C03–C05、E06；稳定 endpoint identity 是 observed/admitted 的必要证据。

位置：`src/r2f/core/candidate.py` 的 `CandidateLifecycle._validate_observed()`（审阅时第 47 行，关键检查第 52 行）与 `_validate_admission()`；`src/r2f/reports/candidate_report.py` 的 `candidate_ledger()` 会把 writer/reader 从 typed_facts 组装成一个字典。

检查只判断 endpoint_identity/instrumentation_identity 的整体 truthiness，没有校验其成员是否为实际身份。`{"writer": null, "reader": null}` 是非空字典，所以能满足该 gate。内存反例在现有合同 helper 构造的有效 execution 上仅替换此字段，并调用构造器及 `admit()`，实际返回：

```json
{"candidate_status": "admitted", "endpoint_identity": {"writer": null, "reader": null}}
```

raw 只读引用现有 `/home/wuyuqa/ros2-dds-fuzz-v2/results/runtime-smoke/cyclone-smoke-v4/raw.jsonl` 以满足字节 hash 检查；该 raw 不构成反例中合成 candidate 的真实观察证明，探针没有保存 candidate。

影响：核心 minimum evidence gate 会把“有容器、无实际身份”误当稳定 identity；报告器的组装方式也能产生该结构。应按 source 规范校验身份结构、必需成员和非空值，并与 execution typed facts/raw 关联。核查标准为此反例被拒绝；不应通过虚构 writer/reader 值解决。

状态：核心合同反例已只读复现；未声称自然运行已经产出缺失身份的候选。

## R002：离线 seal 审计未核对当前完整源码身份

关联验收：H04–H05、G09–G10、E03/E06；代码/rule/资产 identity 变化不能沿用旧证据宣称当前通过。

位置：`src/r2f/reports/run_report.py` 的 `audit_report()`，审阅时第 73–81 行及第 133–134 行；完整身份计算已有 `src/r2f/campaign/identity.py:source_manifest()`。

当前审计检查保存的 head/behavior_tree_sha256 非空，比较保存环境与保存 manifest 的 HEAD/repo，并与当前 S1 target manifest 比较。它未调用完整 `source_manifest(repo)` 与保存 source identity 比较。因此当 core/runner/非 target 的行为源码或 HEAD 发生变化、target manifest 保持相同，离线审计缺少显式 stale-source 拒绝条件。

区分已有保护：`campaign/runner.py:run_campaign()` 已在每 cell 前后核对完整 source_manifest，不能把它说成“没有源码变化检查”。本项只指出独立 `audit_report()` 在运行之后复审旧报告时，没有执行同样的当前完整身份绑定。

影响：自洽的历史 report/manifest/capsule 不足以证明当前代码身份对应的 seal。应明确历史验证与当前 seal 验证模式；当前 seal 必须比对正确的 source repo、HEAD 与 behavior_tree_sha256，发生变化时具名拒绝。静态检查已确认此比较缺失；未创建/修改源码或历史报告来做端到端 seal 试验。

状态：源码静态审阅发现；主agent待决定验证模式并补合同检查，不是“HEAD 尚未冻结”这一已知事项的重复报告。

## R003：candidate ledger 审计未重新绑定候选证据与生命周期

关联验收：F06、C03–C15、G08–G09；candidate 的 raw/input/source/provenance/归因/replay 需要可回查，不能只保证 ledger 文件自身 hash 自洽。

位置：`src/r2f/reports/run_report.py:audit_report()`，审阅时第 193–209 行。该段检查 ledger hash、候选 cell_id 集合、排除 control、归因字段非空、replay attempt 或 non_replayable 存在。

缺口：候选中的 `evidence` 未与已验证 cell 的 run/target/revision/RMW/arm/repeat、raw/hash、effective input、provenance 等逐项绑定；也没有从 cell 重新验证 CandidateLifecycle 的 admission/history/triage/confirmation。因此带正确 cell_id、但 evidence 或责任字段属于另一份数据的 ledger，不能仅靠文件 hash 和字段存在证明符合 candidate 合同。重算 ledger/report hash 只证明修改后自洽，不证明候选与执行一致。

已有 cell raw 重建和 oracle 重算检查是有效保护，本项不否定它们；问题是这些结果没有继续绑定到 ledger 中的 candidate evidence 与生命周期。应从已验证 cell 重算/校验投影及稳定身份，再审计候选状态转换、归因和 replay 关联。对缺失 admitted evidence、换 raw/input identity、篡改 history 或责任归因的 ledger，审计应给出具体错误。

状态：源码静态审阅发现；未修改真实 ledger，未声称完整 seal 已实际接受了篡改产物。

## 未重复列入的问题与 chronology 复核边界

- 网络隔离载体和 vendor内部观察已有两个专属agent实现，不把“尚未完成”当新增缺口。
- D009 的有限窗口末尾缺失、原 fail 后新 pass 的 replay 区分已有明确裁定和实现推进，不再作为同一个新增发现重复报告。
- D010 的 writer-only 前置、precondition/observation receipts 拆分、mutation_begin 时序是已知实现纠错。本次已见源码正在调整；没有新 ROS raw 能证明修正后 chronology 已通过，不能拿旧五步 raw 为新合同背书。
- 初审曾探查 runtime failed/process unhealthy 是否仍进入 judged；审阅期间 `runtime_adapter.ingest_execution()` 已新增环境/进程/前置/identity 的显式健康 gate，因此不将该初审疑点列入本页新增缺口。
- 对 D010 的后续验收应逐 target 核对 raw：late_joiner 的 writer/prejoin 完成与 snapshot 先于 mutation_begin/delay/create reader；match/graph 属于其后观察。discovery_drop 须先 matched 再 drop。不能只检查 receipt 数量、统一套用一个 target 的前置，或用事后生成的 stage_history 证明原始顺序。

## 审阅源码快照

共享目录仍由其他agent开发，以下 digest 用于界定观察时点。文件变化后应复核，本页不自动将新版本判为同样存在问题。

| 文件 | SHA-256 |
| --- | --- |
| `src/r2f/core/candidate.py` | `60a2c287ec9380f1068cb9a4bd7ef9bcdceefa101927de042d726e3b9c2b6ff1` |
| `src/r2f/reports/run_report.py` | `f8993a8f85cca2388029f3ca76b3abc272abb9e96fc53a17fbc579e746ce7bae` |
| `src/r2f/campaign/identity.py` | `07bd21c03c6ba3400c9cb69e23c6085b74c49aa7e2043f6a9efbe7150e81be4f` |
| `src/r2f/campaign/runner.py` | `6325164b6deaced6450b919f46e2fef225b6eec649040fbd38563d0d7ad740f6` |
| `src/r2f/campaign/runtime_adapter.py` | `308f51191e2b5320b66b5eb3b189a3e166bf1c44e7e52cee854ff55e69b4313e` |

## 首次冻结前追加复核：保留历史发现与修复进展

主agent已派遣 runner 修复并安排首次冻结。原 R001–R003 和原 digest 全部保留，描述的是当时快照，不改写为当前版本结论。

- R002：当前 `audit_report()` 已调用 `source_manifest(Path(source["source_repo"]))` 比较当前与保存的 source，并检查 `source_frozen`、`dirty_behavior_paths`；真实 runner 同样要求 frozen source。这是已见源码修复进展，最终合同结果及冻结后真实验证待主agent汇总，不再把“缺少当前源码比较”表述为仍未修。
- R003：当前审计已从 rows 重建 candidate_ledger，经 `audit_projection()` 比较 candidate evidence/lifecycle 投影，并校验派生 confirmed 数量；源码已补绑定路径。冻结后的完整证据验证仍待进行，不预先宣称 seal 通过。
- R001：本次追加检查仍见 `_validate_observed()` 的整体 truthiness 判断；此前反例留作已报告问题，未在本次再次运行探针，也未擅自扩大修复范围。主agent应在冻结处置中明确状态，不能因为 R002/R003 已补就默认 R001 同步关闭。
- 文档/CLI 入口、网络 raw 归档状态已更新；source_frozen 最终检查由 runner agent完成。此页为审阅历史，不是最终冻结报告。首次 GO 前没有执行 staging 或 commit。

最终冻结前进展（主agent独立验证消息）：全仓 131 tests / 13.485s / OK，negative+identity 集成已通过，所有代码agent明确 freeze。以上 R001–R003 的原始发现、反例与源码快照保持历史记录；此补充不声称文档子agent已逐一复跑反例，也不等同真实矩阵或最终 seal。实际源码 HEAD 见 git log 及运行 manifest。
