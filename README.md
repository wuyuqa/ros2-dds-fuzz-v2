# ROS2 DDS Fuzz v2

在统一 registry/runner 下建设完整 S1 的模型引导发现、异常分析、用户确认与证据验收闭环。有效规格、集成 source、活动交付和下一项工作统一从 [S1 接续入口](docs/s1-current.md) 读取；协作约束见 [AGENTS.md](AGENTS.md)。

当前 required 为四 target/revision0/72，candidate=0、用户确认独立新发现=0；完整 S1 尚未成熟。导航更新、离线通过和历史 seal 均不增加成熟计数。

## 安装与标准离线检查

在已核对的仓库根目录执行：

```bash
python3 -m venv .venv
. .venv/bin/activate
python3 -m pip install -e '.[test]'
python3 scripts/check_offline.py
```

该入口收集 unittest 和 pytest 风格的离线测试，并显示失败、错误和跳过原因；CI 使用同一命令。可在命令后传入测试路径或 `--collect-only`。独立证据验收使用 `python3 scripts/verify_s1.py REPORT`。离线合同、真实 ROS/DDS、历史缺陷检出和 required/seal 各自验收，真实运行仍需当轮 GO、唯一 owner 与环境/锁核对。

## 按需阅读

- [当前规格与接续动作](docs/s1-current.md)：先读此页，不从历史“当前状态”猜测。
- [任务书原文](docs/taskbook.md)、[工程决策](docs/decisions.md)：区分原规格、已覆盖边界与合同。
- [验收要求](docs/acceptance.md)、[验收差距](docs/s1-acceptance-ledger.md)、[待核实事项](docs/open-items.md)：按具体任务读取。
- [修改前完整文档归档](docs/history/issue26-before-2026-11-23/INDEX.md)：原 README、AGENTS 和历史正文含既有 dirty 内容，逐字节保留。
