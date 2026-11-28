# First / second 正式证据快照与source边界

记录时间：2026-11-15T00:30:42+08:00（Asia/Shanghai）。来源：主agent独立验收消息及文档子agent只读核对的report、manifest、verify execution/stdout、raw hash。文档子agent未重新执行verify、ROS或campaign，不修改results，不暂存/commit。

主agent已解冻third开发，runtime/campaign/core/instrumentation按各自写集推进。下列first/second通过结论只属于 `11ec76c249cb75605fd70c5d7efb5610e70514b1` 源码身份；后续source改变后不能沿用为当前成熟声明，须在最终source冻结后重跑并重新独立验收。保持HEAD不变不等于工作树行为源码仍冻结。

## 四次attempt的独立账目

均为target_revision=0；目录r0/r1/r2是attempt名，不是模型revision。UTC目录和原始文件保持原样。controls顺序为health/mutation/fault/transparency，分别以六为期望。

| attempt | domain | source简写 | 实际结果 | controls satisfied | seal / verify rc |
| --- | ---: | --- | --- | --- | --- |
| [first首跑](../results/s1/late-endpoint-r0-20261114T155220Z-f03e5741/report.json) | 180 | 21f0e267… | 24真实runtime完成；required17 pass+1 unknown；fault6预期fail | 6/6/6/5 | false / 1 |
| [first重跑](../results/s1/late-endpoint-r1-20261114T161459Z-7094038e/report.json) | 180 | 11ec76c… | 18 required judged pass＋6 negative judged fail | 6/6/6/6 | true / 0 |
| [second预检阻断](../results/s1/late-join-delivery-r1-20261114T162102Z-067b2a2d/report.json) | 181 | 11ec76c… | 24行runtime blocked/real_execution=false；required18 unknown | 0/0/0/0 | false / 1 |
| [second重跑](../results/s1/late-join-delivery-r2-20261114T162346Z-2642f6ef/report.json) | 80 | 11ec76c… | 18 required judged pass＋6 negative judged fail | 6/6/6/6 | true / 0 |

第一target重跑与第二target r2均为errors=[]、unknown_count=0、四control6/6；主agent独立verify确认，落盘verify记录亦为rc0。旧first unknown和second domain181阻断不删除、不回填pass，不用多次attempt累计分母。

## 精确证据路径与文件字节hash

- **first首跑**：`late-endpoint-r0-20261114T155220Z-f03e5741`；[manifest](../results/s1/late-endpoint-r0-20261114T155220Z-f03e5741/manifest.json)、[ledger](../results/s1/late-endpoint-r0-20261114T155220Z-f03e5741/candidate_ledger.json)、[verify执行记录](../results/s1/late-endpoint-r0-20261114T155220Z-f03e5741.verify.execution.json)、[verify输出](../results/s1/late-endpoint-r0-20261114T155220Z-f03e5741.verify.stdout.log)。report字节SHA-256：`844d064b88b826022d0c5982fea80aefa59910848c0027cc5be4e6653110c272`。
- **first重跑**：`late-endpoint-r1-20261114T161459Z-7094038e`；[manifest](../results/s1/late-endpoint-r1-20261114T161459Z-7094038e/manifest.json)、[ledger](../results/s1/late-endpoint-r1-20261114T161459Z-7094038e/candidate_ledger.json)、[verify执行记录](../results/s1/late-endpoint-r1-20261114T161459Z-7094038e.verify.execution.json)、[verify输出](../results/s1/late-endpoint-r1-20261114T161459Z-7094038e.verify.stdout.log)。report字节SHA-256：`3d1556ff3a78285b54b3c88cb185a5c3c7fba36f1d786fb15b180d0da5deaf4c`。
- **second预检阻断**：`late-join-delivery-r1-20261114T162102Z-067b2a2d`；[manifest](../results/s1/late-join-delivery-r1-20261114T162102Z-067b2a2d/manifest.json)、[ledger](../results/s1/late-join-delivery-r1-20261114T162102Z-067b2a2d/candidate_ledger.json)、[verify执行记录](../results/s1/late-join-delivery-r1-20261114T162102Z-067b2a2d.verify.execution.json)、[verify输出](../results/s1/late-join-delivery-r1-20261114T162102Z-067b2a2d.verify.stdout.log)。report字节SHA-256：`dcffe78ebf7074a531682b7b0cef397123e1468609b83c08d3ae8bb9cf704200`。
- **second重跑**：`late-join-delivery-r2-20261114T162346Z-2642f6ef`；[manifest](../results/s1/late-join-delivery-r2-20261114T162346Z-2642f6ef/manifest.json)、[ledger](../results/s1/late-join-delivery-r2-20261114T162346Z-2642f6ef/candidate_ledger.json)、[verify执行记录](../results/s1/late-join-delivery-r2-20261114T162346Z-2642f6ef.verify.execution.json)、[verify输出](../results/s1/late-join-delivery-r2-20261114T162346Z-2642f6ef.verify.stdout.log)。report字节SHA-256：`4e900470d20ea4815f4275702fd2ef4462d4cc2fe372e4a02684a272d86d5cb2`。

通过的两次run共48份raw已分别按report引用核对字节hash。first两次共48份capsule的replay_attempts/execution_history/non_replayable均0；second r2另24份亦均0。这是指定run范围内的只读检查，不把矩阵重跑写成F07真实replay。

## domain181原始阻断与后续处理边界

second r1的runtime preflight原因为`domain_conflict:rtps_port_block`，real_execution=false；adapter随后另报`runtime state step count differs from target plan`。两层错误都保留，不能用后者覆盖原始环境原因，也不能据此推断已经真实运行后证明target模型不兼容。second r2使用domain80成功，不证明domain181当前已无冲突，不清理/强抢旧锁或owner。

## 已seal revision0 target manifest保持不变

主agent工程裁定：不修改已seal first/second的revision0 target manifest；core对其精确legacy fault合同兼容。当前记录的target identity如下：

| target | report.identity.target_sha256 | 已保存fault合同 |
| --- | --- | --- |
| discovery.late_endpoint | `c67eb5837a34bb6970d4d59bf2cb339d1b6a07243af9d0ee949dbdc789533940` | negative / candidate_eligible=false / expected=primary_fail / fault_id=late_endpoint_delivery_omission / one intermediate postmatch sequence followed by delivered sentinel |
| discovery.late_join_delivery | `1f65b4dbfc7bc5c9be454251fa536705597c75a36981f4e23ba9e6e60076002d` | 同一已保存legacy fault合同；不得为新接口重命名fault_id或补字段导致旧manifest漂移 |

兼容仅针对精确既有合同，不是未知receipt通用放行，也不能用legacy路径接纳缺少新target所需证据的结果。第三/第四新target明确声明receipt_kind/window及projection schema。首次seal前可修正其初版合同，但这种声明或修正不等于新rule已完成、实际mutation已命中、恢复已观察或新target已验收。

保持first/second target manifest不变只解决模型身份兼容，不豁免全局source身份校验。第三/第四实现导致行为source改变时，first/second也需按最终source重跑后才能用于最终S1验收；历史报告仍可按原source解释，不能改其manifest/raw/hash。

后续只读核查补记（2026-11-15T00:38:53+08:00，Asia/Shanghai；来源主agent）：第三target开发开始后，S1Module的first/second target.to_dict()与两份11ec76c正式manifest逐字段Python比较均true。文档子agent未重跑该比较；该结果仅支持target合同未漂移，source已经dirty，因此旧source seal不能沿用为当前成熟声明，最终source rerun要求不变。
