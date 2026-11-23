# S1 当前接续入口

固定截面：2026-11-23，issue #26 owner 只读核对 + 本次用户派遣；这是导航和候选下一步，不是运行 GO 或完成证明。由文档 owner 在里程碑替换本页状态，不追加长篇状态史。开始操作前重验 source/status 与当轮派遣；路径存在不代表交付已整合。

## 有效规格与来源

| 层次 | 入口与适用含义 |
|---|---|
| 用户确认规格 | [#6 完整 S1](https://github.com/wuyuqa/ros2-dds-fuzz-v2/issues/6)，本地 [spec.md](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-issue6-implementation-20261121/spec.md)。含 15 条 S1 案例、18 个覆盖维度、Testing Decisions；实现/真实验证/历史检出/新发现分别验收。 |
| 当前范围依据 | 用户确认的 #6 与本轮获确认的 #26 明确：最新 #6 完整 S1 覆盖旧 MAIN-only／正常互操作边界；不削弱证据、GO、成熟门。保存的 [issue-26.json](/tmp/s1-retro-implementation-20261123/issue-26.json) 是本票验收依据。 |
| 当前工程安排 | MAIN 协调共享写集、提交与运行；本页交付顺序是本次派遣的工程接续建议，不是新增用户规格或跨票依赖。R1–R4 可独立推进，不是所有 S1 工作的新前置门。 |
| 合同与历史 | [taskbook](taskbook.md) 原文不改；[decisions](decisions.md)、[acceptance](acceptance.md)、[ledger](s1-acceptance-ledger.md)、[open-items](open-items.md) 按任务查相关合同。旧文的公共 API 限制、source/status/GO 只属于旧截面。 |

旧工程边界不能限制 #6 的模型引导探索、源码插装、历史缺陷验证与异常复核义务；本票仍只写文档、不取得 live/提交权限。18 张实施票依赖见原 [graph.json](/home/wuyuqa/ros2-dds-fuzz-v2/results/main-review/s1-issue6-implementation-20261121/graph.json)，[实施账本](s1-issue6-implementation-2026-11-21.md) 是历史检查点记录，不是实时 tracker 状态。

## 集成身份与活动交付

MAIN：`/home/wuyuqa/ros2-dds-fuzz-v2`，branch `codex/s1-issue6-integration`，本轮实施起点 HEAD `4be53e9e92dd5b91a27f06a1ae5d63511298a761`。受保护文档 dirty，另有 untracked 历史 docs/results；HEAD 正确不等于整个工作树 clean/source_frozen。当前行为 hash、当轮环境与 live 资格未核定。

本票 WT：`/dev/shm/r2f-retro-issue26-20261123`，base 同上；写集只有 `docs/**`、`README.md`、`AGENTS.md`。六份 MAIN dirty 文档已复制到此 WT，编辑前 [归档清单](history/issue26-before-2026-11-23/manifest.json) 保存原字节/hash；MAIN 的其他 untracked 历史文件没有复制，也不得删除。

| 活动 | 交付入口／owner WT | 当前含义与下一步 |
|---|---|---|
| #17 CP2 | [DELIVERY.md](/tmp/r2f-s1-issue6-20261121/issue17-cp2-adoption-audit-owner/DELIVERY.md)、[source-identity.json](/tmp/r2f-s1-issue6-20261121/issue17-cp2-adoption-audit-owner/source-identity.json)；WT `/dev/shm/r2f-s1-issue17-cp2-adoption-audit` | MAIN #28 已完成隔离应用与相关检查，见下节实测结果；必要审查/整合仍按原票执行。修改 `src/r2f/campaign/observations.py` + 新 `tests/test_campaign_adopted_config_audit.py`，base 为本页 MAIN HEAD。不能写成已整合。 |
| #12 consumer continuation | [REPORT-consumer-continuation.md](/tmp/r2f-s1-issue6-20261121/issue12-ros-group-control-cp1-v9-owner/consumer-next-cp-20261123T094225/REPORT-consumer-continuation.md)、[delivery.json](/tmp/r2f-s1-issue6-20261121/issue12-ros-group-control-cp1-v9-owner/consumer-next-cp-20261123T094225/delivery.json)；WT `/dev/shm/r2f-s1-offline-worktrees-20261122/issue12-ros-group-control-cp1-v9` | 待整合；owner base `d4c1070`，声明在 MAIN `4be53e9` 干净 archive 测试。按 manifest 区分 COMBINED 与 DELTA，不能混用；213 pass 是交付历史结果。 |
| #23 v11 | [REPORT.md](/tmp/r2f-s1-issue6-20261121/issue23-v11-fix-owner/REPORT.md) | 仍是 template only；不授 batch GO、不代表实际运行/清理验收完成。 |
| #27 | [issue-27.json](/tmp/s1-retro-implementation-20261123/issue-27.json) | 入口、依赖及 CI 配置已整合至 MAIN 工作树；标准 `python3 scripts/check_offline.py`、依赖 `.[test]`。审查前诊断全套已中断并保留2个过期测试期望失败，相关期望已修正；最终源码全套及托管 CI 仍待实际结果，不预填通过。 |

## 本轮空间回收

#29 清理命令已在结束的本轮复现副本上实际执行，删除逻辑132,291字节／文件分配167,936字节，文件系统观测 free delta 为 +176,128字节（并发活动下不作独占归因）。[回执](/tmp/s1-retro-implementation-20261123/issue29/final-reproduction-apply.json) 确认保护对象仍在；[限定盘点](/tmp/s1-retro-implementation-20261123/issue29/bounded-inventory.json) 列出活动worktree、待整合交付、原始运行证据、失败记录和重复scratch的占用、引用及保留理由。旧副本因活动引用覆盖尚不完整而保留；本次回收未解决磁盘压力。使用 `python3 scripts/cleanup_task_artifacts.py --help` 查预览/重验入口；新清理须重新给定限定目录、完成记录、相同保留副本及当前引用，不能套用旧预览。

## 接续演示与下一步

**接续演示已完成。** 新上下文仅从 AGENTS 与本页定位 #17 交付，实际运行下述 CLI，退出0、134项通过（8.34s）。见 [新上下文实测报告](/tmp/s1-retro-implementation-20261123/issue26-fresh-context/REPORT.md) 与 [实际输出](/tmp/s1-retro-implementation-20261123/issue26-fresh-context/cli.stdout.json)。未修改原工作树；这只验收本票的接续行为。

MAIN 首次动作： MAIN 实际运行得到 [issue28-real-issue17.json](/tmp/s1-retro-implementation-20261123/issue28-real-issue17.json)，本票已只读核对：`status=verified`，base/source 均为 `4be53e9e92dd5b91a27f06a1ae5d63511298a761`、`base_changed=false`，补丁 SHA-256 为 `e2631e5e26846f55ed351ce8caa8203f37473268a57ee1aafa24dfeeaeb14ae6`，两条声明路径准确；隔离检查 134 tests / 8.67s、rc0，scratch 已清理。MAIN 检查记录未修改原件/commit/runtime；结果明确 `commit_go=false`、`runtime_qualified=false`。这支持该 base 的离线整合检查，不证明已整合 MAIN、#17 业务验收或完整 S1 成熟。结果没有记录隔离目录、完整环境或时间，本票不补造。

以下是已使用的接续步骤，保留给后续需要重新验证该交付的任务。没有 source、补丁或验收要求变化时，不机械重复检查：

1. 读取本页、#17 `DELIVERY.md`/`source-identity.json` 和 [#28 规格](/tmp/s1-retro-implementation-20261123/issue-28.json)。记录 MAIN 与 owner WT 实际 HEAD/status、当轮写集；若 source 已变，报告实际值并重新评估整合影响，不能直接沿用历史通过。
2. 使用 #28 的 `scripts/check_delivery.py` 入口（MAIN 当前文件已只读检查 argparse 参数；它不属于本票 base，集成后先用 `--help` 核对），读取既有 manifest 校验路径、base、两个声明路径及补丁 hash。完整 [CP2.patch](/tmp/r2f-s1-issue6-20261121/issue17-cp2-adoption-audit-owner/CP2.patch) 声明 SHA-256 为 `e2631e5e26846f55ed351ce8caa8203f37473268a57ee1aafa24dfeeaeb14ae6`；核实新测试确实包含在补丁内。不得仅应用不含新测试的 tracked.patch。
   集成 #28 后，从已核对的 MAIN 根目录执行以下候选命令；工具自行创建并清理隔离 clone，输出保存在 MAIN 为复验指定的新结果文件，勿覆盖首次结果：

   ```bash
   python3 scripts/check_delivery.py \
     /tmp/r2f-s1-issue6-20261121/issue17-cp2-adoption-audit-owner/source-identity.json \
     --source /home/wuyuqa/ros2-dds-fuzz-v2 \
     --patch /tmp/r2f-s1-issue6-20261121/issue17-cp2-adoption-audit-owner/CP2.patch \
     --allow-path src/r2f/campaign/observations.py \
     --allow-path tests/test_campaign_adopted_config_audit.py \
     -- python3 -m pytest -q tests/test_campaign_adopted_config_audit.py tests/test_campaign_observations.py tests/test_campaign_observation_intake.py tests/test_runtime_instrumentation.py
   ```

3. 在 MAIN 指定的隔离验证目录应用到实测集成 base，保留 patch/输入身份与应用结果；不修改 MAIN dirty 文档、原 owner WT 或原交付。上面的 CLI 已在隔离源码执行相关公开入口合同检查，不需另跑相同 suite。交付的 15 项新增、134 项相关通过只属于 owner 当时源码；新一次调用须实际记录退出码/日志，不能称为真实 DDS 验证。依赖通过 #27 `.[test]` 统一协调，不为凑通过改 gate。

4. 将真实校验结果保存在 #28 已有交付目录；结果须含时间、入口/命令、base/source/patch 身份、隔离工作区、声明/实际写集、测试与必要 review、具体失败/未知及剩余义务。MAIN 将结果路径和简短结论交文档 owner 更新本节，#26 交付报告只链接该结果，不复制产物。原票未完成/无 GO 时照实保留；本轮新上下文已实际使用入口，结果与 MAIN 首次验证分别保存。

完成演示的判据：新上下文从该入口定位所有上述输入，并实际完成一次可核验的隔离应用/相关检查或保留具名失败及后续处置；不以读完导航、旧测试日志或计划替代动作。无法找到 #28 入口时是待 MAIN 定位的工程未知，本票未判定环境终局阻塞；可先只读核验 #17 输入，不跳过工具验收。

## 检查与审查

以下是本票收拢的工程执行口径，沿用用户 #6 Testing Decisions 和既有必需审查，不取消要求：

- 检查点测试：按改动选择最小相关检查，以最高可调用入口及持久化可见行为为主；内部算法只验证难从公开入口定位的确定性边界。不把离线 fixture 算真实 carrier；原失败及 unknown 保留。
- 每票代码审查：验票的最终差异、原规格、写集、证据合同及声明测试；局部检查点通过只支持其范围，不能替代整票 review/业务验收。用户指定的独立审查继续执行。
- 最终集成审查：检查实际整合 source、跨票接口、回归、原18票依赖与完整验收；最终冻结后相关 required/control/replay/seal 仍须完成。历史 per-target seal 不迁移。
- 追加重复审查需写明新增风险、影响路径和已有 review 未覆盖处（例如 base 改变、合同接口变化或新失败），再选检查范围；不因改导航机械重做所有历史 review，也不豁免用户要求的检查。

## 未完成与证据边界

required 四 target/revision0/72 不变；自然 candidate=0、用户确认独立新发现=0；无本 source 新成熟声明，完整 S1 未完成。功能完整、可靠运行、2–3 个经用户确认且根因独立的真实新 bug 仍须同时满足。负控、harness/tool 错误、条目数与历史复现不充新发现。

#17 CP2 只声明离线 adoption audit：`semantic_effect=unknown`、`worker_byte_read=not_observed`；真实 Cyclone 配置输出/parser 兼容、严格 replay/恢复、binary/source 等价及真实语义效果仍待验证。#12 的真实 SUT 行为未因离线 213 pass 获证。#22/#23/#24 不由本票自动验收/关闭。

已知下一步是原 #12/#17 的交付审查与整合；新上下文入口复验已完成。当前 live RMW/domain/run_id、receipt/raw/replay 未运行；真实锁/环境/cleanup 未检查，无运行 GO。更广缺口按 [验收账](s1-acceptance-ledger.md) 与 #6 各原票逐项核查，不能将历史 status 当实时 blocker。
