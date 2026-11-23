# Agent 协作约定

适用于本仓库及其派遣 worktree。新上下文先读 [当前 S1 接续入口](docs/s1-current.md)，从中确定有效规格、集成 source、活动交付与下一步；只按任务触发读取历史，不以旧文中的“当前”接管工作。

## 范围与来源

- 用户最新指令与已确认的 #6 完整 S1 规格优先。历史 MAIN-only／公共 API 正常互操作边界已被覆盖；工程安排注明来源，不冒充用户原话。具体任务仍受当轮写集与运行授权约束。
- 沿用统一 registry/runner、canonical `execution_stage`、嵌套 mutation receipt 及既有 candidate/report；`plan()`、离线 capsule 与合成控制不产生真实执行证据。
- required 固定四 target/revision0/72；blocked 不缩分母。完整 S1 功能、可靠运行与 2–3 个经用户确认且根因独立的新发现分别验收；零候选可推进阶段，不能宣称整体完成。

## 写集与权限

- MAIN 统一协调 owner、共享接口与写集；文档 owner 独占 `docs/**`、`README.md`、`AGENTS.md`，其他 owner 改文档前协调。先检查目标文件与 status，保留所有未提交内容；不 reset/clean 或覆盖他人修改。
- 只执行本次派遣；不继承旧 owner、模型声明、commit GO 或 live GO。禁止擅自派 agent；模型与实际调用一致。
- 提交需当轮明确 GO；仅显式 staging 指定路径，排除 results，禁止 `git add -A`。获准使用自动化身份时仅单次 `git -c user.name='Codex Agent' -c user.email='codex-agent@localhost' commit`，不修改 user 配置。
- 文档 owner 不启动 ROS/campaign。代码/离线检查通过不授 live 资格。真实运行由 MAIN 安排唯一 owner，核对 registry/domain/state locks、owner payload、RTPS 端口块、进程身份、ROS/RMW/source/HEAD/revision 和环境 manifest；冲突或无法确认时 `domain_conflict` abort，不强抢锁。
- 运行中固定 source/HEAD/revision；变化即 abort。source_frozen 要求行为文件已跟踪且与 HEAD 一致；最终 source 改变后重跑相关矩阵，旧 seal 不迁移。锁保持至整个运行子树清理确认。
- `/home/wuyuqa/ros2-dds-fuzz` 与四份参考材料只读；不复制旧未提交代码/旧 seal，不改锁、owner、配置、产物或停止旧 campaign。插装须独立 build/binary/manifest/process provenance；black-box 不加载插装 override。

## 证据与按需阅读

- 保留 before/after、requires、身份/generation、RMW/repeat、raw path/hash、确定性输入身份、执行 metadata、rule/verdict 与失败。control、工具错、mutation ineffective、观察缺口均不能充自然 candidate；残留 forbidden 优先，不放宽 gate 填空。
- 候选保留 provenance、归因、raw identity 与 replay attempt/具名 non_replayable；负控不授 SUT 资格，后续 pass 不删除原失败。用户最终确认与独立根因计数不可由 agent 代授。
- 修改 target/carrier/oracle/replay 前：读 [决策合同](docs/decisions.md) 与 [验收要求](docs/acceptance.md) 的相关条目（D002–D015 等），核查 input/source/revision 身份和原始 chronology；历史实现状态不是当前通过证明。
- 查原规格时读 [任务书原文](docs/taskbook.md)；查成熟差距时读 [验收账](docs/s1-acceptance-ledger.md)；需用户决定的范围/约束变化登记 [open-items](docs/open-items.md)，日常工程问题由 MAIN 裁决。未知先检查，记录证据后才判具名阻塞。
- 测试、每票代码审查和最终集成审查按 [当前入口](docs/s1-current.md#检查与审查) 执行。交付注明改动、检查/未运行项、source/target/revision、RMW/domain/run_id（如有）、receipt/raw/replay/candidate、abort/escalation 与成熟分母影响；未知如实保留。

修改前完整约定（含历史进展）见 [逐字节归档](docs/history/issue26-before-2026-11-23/INDEX.md)。历史仅按需读取，当前状态只更新接续入口，避免追加平行状态史。
