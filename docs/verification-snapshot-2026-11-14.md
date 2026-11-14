# 主agent验证快照与证据边界

日期：2026-11-14。来源：主agent独立验证/派遣消息；文档子agent只读核对现有产物，不重新执行测试、网络控制、ROS 或 campaign。以下是开发阶段快照，不表示完整 S1 或 per-target seal 已通过。

## 离线合同测试

主agent报告执行：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src python3 -m unittest discover -s tests -q
```

历史快照：工作目录 `/home/wuyuqa/ros2-dds-fuzz-v2`；结果 **112 tests / 4.232s / OK**。文档子agent未复跑，也未获得该次测试的独立日志路径或测试时源码 digest；不编造。此数字仅为该次离线合同总数，不是 112 次真实 ROS 执行，不证明 18-cell、四类各六 control 或完整 S1 达成。代码继续开发后，其他测试总数或耗时应另记新快照。

最新快照：主agent在最终冻结前独立验收报告 **131 tests / 13.485s / OK**，并明确 negative+identity 集成已通过。文档子agent未复跑；该次独立日志路径与测试时 source digest 未随消息提供，不补造。上面的命令保留为已知离线测试入口，131 项结果按本次派遣消息记录。此结果仅证明报告范围内的离线合同/集成检查，不计真实 S1 required/control matrix 或 seal 完成。历史 112 项结果保留，不能把两次数量相加。

## 真实 non-DDS 网络载体控制

主agent原结果：`/tmp/r2f-discovery-network-control.json`。文档子agent读取时 SHA-256：`87e3acecd4c4ece2168fb815fd45f6d3f5d74db1144240e25430720e3628e8ca`。

| 项目 | 文件内结果 |
| --- | --- |
| 总状态/范围 | `ok=true`；`scope=carrier_control_non_dds` |
| 健康阶段 | sent=70，received=70 |
| drop 阶段 | nft counter=33 packets；blocked=33，received=37/70 |
| restore 后恢复 | received=70/70；recovery_verified=true |
| 证据边界 | `ros2_execution=false`；`s1_execution=false`；`host_network_unchanged=true` |

这是实际隔离网络内 UDP carrier control，证明本次 non-DDS 控制路径的丢弃/恢复可观察。它没有证明 DDS discovery 报文命中、真实 ROS endpoint-attached carrier、第三 target 的匹配/恢复或 semantic oracle。`recovery_confirmation` 的命名不能覆盖文件中明确的 non-DDS 范围。

归档更新：已只读核对 `/home/wuyuqa/ros2-dds-fuzz-v2/results/network-control/2026-11-14-carrier-control/raw.json` 与 `/tmp/r2f-discovery-network-control.json` 逐字节一致，SHA-256 同为 `87e3acecd4c4ece2168fb815fd45f6d3f5d74db1144240e25430720e3628e8ca`。文档子agent未写 results。归档不改变 non-DDS 证据边界；results 保持 untracked，首次冻结显式 staging 排除。

## 双 RMW runtime smoke

主agent报告两 RMW 健康与 negative smoke 已正常。下表是文档子agent当次实际找到并核对的 contract-smoke；`runtime_status=completed`、`real_execution=true`、两个 worker returncode 均为 0。每行仅是开发 smoke，未按 required repeat 0/1/2 补齐，不计作已通过完整矩阵。

| run_id / results/runtime-smoke 子目录 | RMW | arm | raw SHA-256 |
| --- | --- | --- | --- |
| `cyclone-baseline-contract-v1` | `rmw_cyclonedds_cpp` | `baseline` | `4509823bcbbdfb714b185ae3108be15155697649be44c849fa331a2c62070e98` |
| `cyclone-mutated-contract-v1` | `rmw_cyclonedds_cpp` | `mutated` | `7794e85a88a5aadda76e57511dfbc00cd1cdad8cea04060617123151de6eeacd` |
| `cyclone-negative-contract-v1` | `rmw_cyclonedds_cpp` | `negative` | `7b45a93785c877c9eb4c1f534490893fea45aede0760c2b55b706093ceaf98b5` |
| `cyclone-transparent-contract-v1` | `rmw_cyclonedds_cpp` | `transparent` | `6d6736ef1a0a7b68702baaf98760afb61a8536d688fb2ae8fb0de45e0990b179` |
| `fastdds-mutated-contract-v1` | `rmw_fastrtps_cpp` | `mutated` | `1ad2442be09418a8ec74bc46b0f05837c006c56cf46146f28fd2a056c0338f01` |
| `fastdds-negative-contract-v1` | `rmw_fastrtps_cpp` | `negative` | `68d7627b1b8a1f4131cec8e10c9ac29adcd1f3c7cc18253b559872515f085798` |

原始产物路径模式：`/home/wuyuqa/ros2-dds-fuzz-v2/results/runtime-smoke/<run_id>/runtime_result.json`、`execution.json`、`environment.json`、`raw.jsonl`。这里只检查已有文件和 raw hash，未重跑、未重算完整 oracle/chronology/seal。上述两个 negative 产物均记录 harness omit 中间 seq=5、sentinel_received=true，归因仍是 harness control，不是自然 SUT delivery loss。

## MessageInfo 身份观察限制

主agent runtime 已核实本机 Jazzy `MessageInfo` 没有 `publisher_gid`。上述 smoke 的文件字段一致为：

```json
{"identity_basis":"endpoint_correlated","publisher_gid_status":"unavailable_api","publisher_gid_consistent":null,"endpoint_identity_correlated":true}
```

关联依据是 bounded endpoint graph、endpoint lifecycle、独立进程、run payload 与序列等实际观察；它不是 callback 对每个 sample 直接返回的 DDS writer GID。direct per-sample GID 尚未达成，须在 facts、report、candidate provenance 中保留此限制，不填成 true、不丢弃限制，也不把缺字段写成 SUT mismatch。

只有确实拿到可比较的 direct GID 且与预期不符时，才可以讨论 direct mismatch；`unavailable_api` 与 `mismatch` 是不同观察结果。endpoint_correlated 是否足以满足某条 identity/generation 规则，应由该 target/rule 的合同和证据强度明确裁定，不凭 smoke 自动提升 evidence tier 或完整成熟。

## 首次冻结前的工程更新

`source_frozen` 和当前完整 source identity 比较已有实现。主agent最终 GO 已确认所有代码agent freeze、131 项测试通过，并派遣文档子agent执行首次显式 staging/commit。实际提交状态与 source HEAD 见 `git log` 及运行 manifest；本页在提交前编写，不提前宣称提交或 source_frozen 检查成功。现有 smoke 来自提交前开发快照，保存为历史，不能仅靠首次 commit 为其补出冻结来源身份或直接用于新 HEAD seal。首次提交采用 D011 指定的单次自动化身份；首次正式 matrix 期间固定 HEAD，文档不追加 commit。
