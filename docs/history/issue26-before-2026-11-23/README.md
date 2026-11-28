# ROS2 DDS Fuzz v2：S1 框架与证据链重建

本仓库正在重建统一的 target、状态执行、carrier、事实/oracle、candidate、replay 和报告合同。S1 是第一个注册模块。当前主agent协调 6 个 `gpt-6.1-sol` 子agent，分别负责 core、runtime、campaign、文档/验收、网络隔离载体和 vendor内部观察；此 README 不表示完整 S1 已实现、运行或成熟。

主agent转述的用户最新范围覆盖完整 S1，第一条切片固定为 `discovery.late_endpoint + late_joiner`，随后按 late_join_delivery、interruption_recovery、builtin reconciliation 的顺序推进。主agent声明当前执行只涉及 ROS2 公共 API 的正常互操作、可靠性与证据工具，不实施自动漏洞搜寻、恶意 wire payload 或漏洞复现。原规格的额外能力和证据差距仍逐项登记。

当前进展（2026-11-15T00:32:53+08:00，Asia/Shanghai）：first r1/domain180与second r2/domain80已在source `11ec76c249cb75605fd70c5d7efb5610e70514b1`上分别完成18 required judged pass、6 negative judged fail、四control6/6、unknown0，独立verify rc0、per-target seal=true。r1/r2仅attempt名，target_revision均为0；首跑时序失败和second domain181预检阻断历史保留。

主agent已解冻third开发。上述seal只属于11ec76c版本，后续source改变后必须在最终source冻结后rerun，不能沿用为新版本成熟声明。first/second已seal revision0 target manifest保持不变，core精确兼容legacy fault合同；第三/第四新target显式声明receipt_kind/window及projection schema，首次seal前合同修正不等于new rule完成。

完整S1仍缺Phase2→Phase4关联、bounded blackbox、F07真实健康/工具失败重放及第三/第四target正式证据。F06零候选审计路径已有真实执行，自然admission/triage未获真实异常证据。当前用户目标包含2–3个真实SUT候选，实测0，该项未达成；数量不能预先保证，zero-candidate允许阶段推进但不代表完整目标完成。见[完整S1验收账](docs/s1-acceptance-ledger.md)与[正式证据快照](docs/formal-target-snapshots-2026-11-15.md)。

最新开发进度（2026-11-15T01:09:04+08:00）：main报告third v4两RMW runtime+ingest judged/pass，但`capsule_execution=null`且使用`reconstructed_input`，仅development，真实capsule-driven neutral/fault仍待执行。新增20项replay、31项third runtime/campaign离线测试通过不等于真实replay；Phase2 bridge新增不替代真实trace/lifecycle/blackbox对照。文档本轮freeze-ready，不commit，停止轮询与自动追加，等待main下一里程碑派遣。

文档导航：

- [任务书原文副本](docs/taskbook.md)：含原文 hash；原规格与当前指令有冲突时按来源顺序解释。
- [决策记录](docs/decisions.md)：D002–D004 规定 neutral 分支、确定性输入身份、residual 优先、control 引用与分母。
- [验收矩阵](docs/acceptance.md)：八项框架能力、第一 slice 18 格、四类各六个 control、全部 S1 顺序与 seal 要求。
- [待核实事项](docs/open-items.md)：记录工程未知与所需证据，未实测不先验认定 blocked。
- [只读验收审阅](docs/read-only-review-2026-11-14.md)：针对记录的源码快照列出三个具体证据 gate 缺口，区分内存复现与静态审阅。
- [验证快照](docs/verification-snapshot-2026-11-14.md)：区分最新离线144 tests、历史131/112 tests、non-DDS网络控制及双RMW smoke，记录逐样本GID限制。
- [协作约定](AGENTS.md)：写入范围、真实证据和 campaign 协调。

## 当前安装与离线测试入口

目前实际存在的 `/home/wuyuqa/ros2-dds-fuzz-v2/pyproject.toml` 使用 setuptools 构建、`src` 布局，声明 Python `>=3.10`、构建依赖 `setuptools>=61`、无 Python 运行时依赖。目标 ROS 环境为 Jazzy。现有 `config/dependencies.lock.json` 记录本机已安装 ROS/RMW 的版本、package.xml 与 RMW library SHA-256；它是环境身份核验锁，不会自动安装依赖，也不证明其他机器环境已满足。

可选的开发安装方式（本次文档任务未执行安装）：

```bash
python3 -m venv /home/wuyuqa/ros2-dds-fuzz-v2/.venv
/home/wuyuqa/ros2-dds-fuzz-v2/.venv/bin/python -m pip install -e /home/wuyuqa/ros2-dds-fuzz-v2
```

现有 `/home/wuyuqa/ros2-dds-fuzz-v2/tests/test_core_contracts.py` 使用标准库 unittest，可不安装包而从源码加载，入口如下。此处是根据已检查文件给出的命令，本次文档任务未运行该测试，不声称通过：

```bash
PYTHONPATH=/home/wuyuqa/ros2-dds-fuzz-v2/src python3 -m unittest discover \
  -s /home/wuyuqa/ros2-dds-fuzz-v2/tests -p 'test_core_contracts.py' -v
```

完整离线合同入口（主agent报告的最新144项及历史131/112项结果见验证快照，文档子agent未复跑）：

```bash
cd /home/wuyuqa/ros2-dds-fuzz-v2
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -m unittest discover -s tests -q
```

离线合成 raw/receipt 只用于合同测试，不进入真实 required matrix 或 seal。不能把旧仓库 CLI 当作 v2 入口。

## 真实运行与验收状态

现有 overlay 为 `scripts/setup_overlay.bash` 的 Python 源码 overlay，不是 colcon install overlay。运行环境按顺序加载 `/opt/ros/jazzy/setup.bash` 与 `/home/wuyuqa/ros2-dds-fuzz-v2/scripts/setup_overlay.bash`。CLI `scripts/run_s1.py` 默认仅生成离线计划；只有显式 `--execute` 才请求真实运行，当前可选 target 为 late_endpoint、late_join_delivery。`scripts/verify_s1.py <report.json>` 独立重算证据与 per-target seal。文档子agent已验证两个脚本 `--help` 返回成功，未生成计划或启动真实运行；这些入口不表示后两个 target 已接通，也不提供自动 replay CLI。文档子agent未启动 ROS、未执行真实 pair 或 campaign。主agent现已报告双 RMW runtime smoke，并有 raw/hash 可查；详见验证快照。已有 smoke 不等于完整矩阵或 seal。

真实运行由主agent协调：先冻结 source identity，获取 campaign/domain/state-directory 三锁，排除旧仓库 campaign owner，检查 derived RTPS 端口和活动进程，并保存环境 manifest。仓库处于开发与验收阶段，实际 source HEAD 见 `git log` 及运行 manifest。真实 runner 与独立 seal 审计已加入 `source_frozen` 检查：要求有 HEAD，相关行为文件已跟踪且内容与 HEAD 一致；代码身份改变后旧证据保留为历史，不能用于当前 seal。正式 matrix 期间固定 HEAD，文档可在工作树更新，但不得追加 commit；后续 phase 的实现统一形成新提交后重跑相关矩阵，并按新身份验收。

第一 target 的计划规模为 18 条 required 行；health/mutation/transparency 可分别引用其中六行，fault 另存独立六行。blocked 不得缩小已冻结分母。baseline/transparent 不伪造 hit；negative 的中间 delivery omission 属于独立 fault-control，通过 raw-backed 审计路径跳过 mutation_applied，不伪造 late_joiner hit，且 admission 无条件排除 negative。allowed effect 不得掩盖同时发生的 forbidden residual。

候选必须有真实来源、输入/执行证据和归因。control 不进入自然 SUT candidate；候选数量不能预先保证，零候选可如实报告，但不能等同完整成熟或无缺陷。

主agent最新独立验证报告（2026-11-15T00:14:58+08:00，Asia/Shanghai）：全仓离线 144 tests / 37.032s / OK，diff --check及ACK逻辑审查通过；历史131项negative+identity集成结果和112项结果保留于验证快照；non-DDS 网络控制 drop 33 packets、恢复70/70。两者不计真实 S1 完成。本机 Jazzy MessageInfo 无 publisher_gid，runtime smoke 的 identity_basis 为 endpoint_correlated；直接逐样本 DDS writer GID 尚未达成，作为观察限制保留，不归因 SUT mismatch。

results 作为可见的 untracked 产物保留，通过显式 staging 排除，不提交 raw 或 locks。首次自动化提交的身份与授权约定见 [协作约定](AGENTS.md) 和 [D011](docs/decisions.md)。
