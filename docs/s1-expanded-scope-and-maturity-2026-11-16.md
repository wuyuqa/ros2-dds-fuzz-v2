> **当前导航（2026-11-23）**：先读 [S1 接续入口](s1-current.md)。本页保留原规格/工程决定/历史截面；下文“当前”、旧 source、owner、GO 和待批准边界均按其记录日期解释。用户确认的 #6 完整 S1 规格覆盖旧 MAIN-only／公共 API 正常互操作执行限制，不能据此缩小范围；历史通过不迁移到新 source。修改前全文及 dirty 内容见 [逐字节归档](history/issue26-before-2026-11-23/INDEX.md)。

# S1 扩展范围与成熟版本判定：补充分析

- 日期：2026-11-16；状态：主 agent 工程提案，待范围确认，未实施。
- 来源：用户认为此前暂缓项同样丰富变异面，要求分析是否纳入以及完成后能否称 S1 成熟。用户没有在本条消息中授权启动 ROS、提交代码或执行新 scope。
- 主 agent 本轮仅协调修改自己维护的计划入口及新增本文；不修改历史验收台账、原任务书、业务代码或旧仓库。后续代码和实际执行仍交由指定 gpt-6.1-sol 子 agent。
- 本文扩展成熟路线，不废弃 F1/F2 详细任务卡，不把长期路线当已经实现。此前“暂缓”解释为不阻塞 F1/F2，不再表示排除在 S1 成熟路线之外。

## 1. 结论

这些方向大多与有效输入空间、可到达状态或探索效率直接有关，应分批纳入。此前为补最小 fuzz 闭环收窄到 F1/F2 有交付价值，但不能把这种里程碑称作完整 S1 成熟。

建议分别报告四种结论，禁止相互替代：

1. 原四 target 的固定合同成熟：只针对其冻结 source、revision、required matrix 和 controls。
2. fuzz 机制就绪：真实 corpus→mutator→执行→状态反馈→再变异；F1/F2 对应这一层。
3. 版本化 S1 覆盖成熟：批准的发现/匹配/投影合同、拓扑、生命周期、环境和 carrier 有真实验收，报告明确支持及不支持范围。
4. 研究目标达成：探索效果有合适对照支持，且用户要求的 2–3 个真实 SUT 候选经独立审核。候选数量目标不能通过增加工具能力预先保证。

有限测试不能证明所有 DDS 状态或全部输入已覆盖。成熟是对批准、冻结、可追溯的版本范围作工程声明，不是无缺陷证明。

## 2. 本地依据与使用边界

- 分类文件：`/home/wuyuqa/ros2-dds-fuzz/DDS_three_level_spec_family_semantic_taxonomy(1).md`，§2、§3.1。
- 该文件将 S1 定义为远端 participant/reader/writer/topic/type 的发现图、endpoint/topic/filter 匹配谓词、builtin/discovered data 投影和可见性；S1.B1 明确包括 participant 缓存、lease 与发现更新。
- 分类文件同时把本地 Entity 生命周期放在 S8，QoS 策略在 S2，交付时序在 S3，类型解释在 S4，线缆编码在 S5，可达性在 S6。
- 原任务书：`/home/wuyuqa/ros2-dds-fuzz-v2/docs/taskbook.md` §11、Phase 5B、Phase 6、§13.2。delay/reorder/duplicate 和 native 是原 scope 外事项，扩展必须显式更新 scope、合同、身份及相应成熟矩阵，不能改 campaign 名绕过。
- F1/F2 依据：`/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-fuzz-expansion-implementation-plan-2026-11-16.md`。
- 较早长期设计：`/home/wuyuqa/ros2-dds-fuzz-v2/docs/s1-model-guided-exploration-plan-2026-11-16.md` P2–P5/P7，作为复用设计，不是第二套同时执行的任务。

上述是本地分类与工程资料，不代替 DDS/RTPS 规范条文。具体兼容谓词、filter 行为、lease 定时、允许残留、API 线程约束需实施前绑定一手规范和实际 SDK 能力；未核实条件不得新增强 oracle。本轮在线查询未得到可引用的一手返回，因此不宣称完成了这些规范校准。

## 3. 每类扩展如何纳入

| 扩展 | 增加的真实空间 | 建议归属与优先级 | 验收重点／不能偷换的结论 |
|---|---|---|---|
| 多 endpoint，数量可变 | 一对多、多对一、多对多、混合兼容子图 | S1 直接能力，F3 必做 | 逐对关系与身份；仅总 matched count 相同不能证明边集合正确 |
| create/destroy/recreate | 创建次序、撤销传播、同名新代际、旧状态残留 | S1 图生命周期刺激，F3 必做 | 本地 API 成功与远端移除分开；本地释放/线程故障另标 S8，不强塞 S1 |
| participant lease expiry/renewal | 远端发现缓存活跃、更新、过期与再发现 | S1.B1，F5 必做 | 精确区分 participant lease 和 writer liveliness；失配/长时间 drop 本身不证明 lease expiry |
| writer liveliness lease | 匹配/可见性的关联刺激，以及 liveliness 自身语义 | S1 只消费相关事实；S2/S3 关联合同独立 | 不把 QoS 配置字段变化当生命周期已测试 |
| ContentFilteredTopic | filter 配置、更新及关联可见性 | S1 的适用 filter/topic 关系纳入 F5 | 不默认 filter=false 必须 unmatch；样本过滤与匹配分开判，表达式/类型错误分层归因 |
| mixed-vendor | 同厂商之外的发现/匹配/投影差异 | F5 支持矩阵 | writer A→reader B 和 B→A 分别校准；真实能力不支持则保留缺口，不能靠同厂商结果替代 |
| discovery delay/reorder/duplicate | 公告时序、旧公告晚到、重复处理和恢复 | S1 环境变异，F4 必做 | 每个受影响包、端点/participant 归属、排队/放行/丢弃记录；共享控制信道影响显式记录 |
| 结构化畸形 discovery 报文 | 参数/长度/枚举/组合非法输入及接收路径 | F7 独立协议健壮性扩展 | 先有合法结构基线再变异；被网关/解码器挡住不算命中发现语义，崩溃也须进程与 raw 归因 |
| 任意随机 wire bytes | 低层解析和噪声压力 | F7 可选算子，不作为完整 S1 的唯一代表 | 与结构化生成分账；不以发送字节数或大量无效包宣称语义覆盖 |
| native participant | ROS 公共接口无法表达的发现/participant/filter 控制 | F5 按 capability 引入，共用 runner | 独立 API/build/binary/source 身份；ROS 路径不被悄悄替换；不可与 RMW 路径结果混算 |
| 同进程持续 episode | 历史积累、缓存残留、重复周期、低重启成本 | F6 必做，并保留独立场景 | episode 程序与每个子 case 均有身份；无可信 reset 不得把前史清零 |
| 多前缀与受限自动规划 | 同目标状态但不同历史的测试 | F6 必做 | 从已支持动作/前缀选取和有界搜索开始；同抽象状态不证明历史等价 |
| 通用 ActionModel | 多模块统一规划的架构 | 不作为 S1 首版成熟硬前置 | 必须有类型化操作、前置/效果/资源合同；可扩展现有 StateStep，不必另造平行模块 |
| 并发 workload/生命周期交错 | 发布、发现、创建、删除重叠及竞争窗口 | F3 起引入，F6 稳定化 | 实际动作区间重叠、每 writer 序列、取消/回收；配置成0间隔不等于并发 |
| 大型高吞吐平台 | 容量/压力与资源边界 | 定向并发必做；独立通用压测平台不是前置 | 测量实际速率、观察丢失、CPU/RSS；不以资源耗尽或日志丢失自动认定 SUT 缺陷 |
| 分布式快照 | 更快恢复测试前缀 | 先测瓶颈再立项，不是语义覆盖前置 | 多进程/网络/时钟/外部副作用一致性需证明；单进程 fork 不冒称分布式快照 |
| B0–B3/消融 | 评估模型、环境变异与反馈的贡献 | F8 研究阶段 | 不增加执行能力本身；共同空间、相同预算和多次独立重复，避免比较不同 runtime 后归因给模型 |

分类原则：操作属于哪种 carrier，不直接决定异常属于哪个 S 模块。先记录可观察的失败合同和 supporting facts，根因未定位则保持未归因/多种假设；确认后按根因分类。跨模块候选不得丢弃，也不得为凑 S1 数量强行改标签。

## 4. 实施顺序与明确出口

下面 F3–F8 是新增提案。具体文件接口先由 T00 扩展设计冻结，避免六个 owner 各自造一套。F1/F2 的 11 张任务卡和 36 项验收仍有效，新增能力必须追加验收而非复用编号冒充。

### F1/F2：先把真正 fuzz 闭环接通

- 沿用详细计划：12 seed、10 核心维度+2辅助、8算子、7组合配方、单 pair 状态反馈。
- 先交付可实际生成和反复变异的工具；不等所有后续平台完成才开始真实探索。
- 出口：保存 input content、chain、实际 binding/hit、facts/rules、反馈与 replay；不以变更 RNG seed/hash 冒充新输入。

### F3：可变拓扑、代际与定向并发

- runtime owner 扩展现有 worker 为 endpoint registry，逻辑 endpoint_id/generation 映射真实 GUID/进程/participant。
- 初始工程边界建议每角色 0–4 个 endpoint、总数不超过8；1–4 participant 仅在能确认真实 participant 身份和可控制映射后启用。数值是可调资源预算，不是 DDS 限制。
- seed 覆盖空角色、1W/2R、2W/1R、2W/2R、不同兼容子图；数量取边界与有界随机值，不硬编码只跑四种拓扑。
- 生成 add/remove/recreate、并发组和 barrier；发布任务由 worker 自主批量执行，不再逐 sample 等控制命令 ACK。
- 语义 owner 提供每对允许/禁止边、freshness/generation 合同。用精确身份观察或审核后的 probe 建立逐对证据；无法分辨时 unknown，不拿 aggregate count 补齐。
- 出口：两 RMW 的逐对图事实、创建删除与发布真实重叠、同名重建身份不串代、取消和异常清理、旧单 pair 回归。
- 拟写集：runtime `worker.py`/`ros2_pair.py`/新拓扑后端；semantic `facts.py`/`oracles.py`；campaign 接线。文件仍各有唯一 owner。

### F4：发现通道的可验证时序变异

- network owner 复用隔离载体，新增有界 delay queue、release order、duplicate count；以实际可识别 discovery traffic 为单位，不把停发业务 data 冒充发现变异。
- 每个包记录原始 hash、来源/目的及关联置信度、计划操作、实际收发时刻、队列位置、复制序号；真实读取的来源和执行轨迹不可由 plan 生成。
- 先单算子透明对照，再两个算子组合，再与 F3 的 create/recreate 组合。保留零延时/单副本/原序的透明路径。
- 对部分 RTPS datagram 混合承载的情况显式记录影响；不能宣称只改 discovery 而实际连带其他流量。
- 出口：至少一条可控且真实命中的重排/重复/延迟路径及恢复证据；无实际包命中标 ineffective；所有队列资源有有界清理。
- 不能沿用仅证明 nft drop 的旧 carrier seal 来覆盖新能力。

### F5：participant/lease/filter/mixed-vendor 语义覆盖

- 先做 capability 与规范矩阵：哪些 ROS API 可表达、哪些需原生 adapter、哪些观察不到。不得先验宣告 native 不可做，也不预先承诺每个 RMW 功能相同。
- participant 族区分正常刷新、停止刷新、过期、重新发现和身份更替；记录实际配置、活动和定时证据，不从一次长期失配反推内部过期。
- filter 族分别声明匹配谓词、配置变更与样本过滤的适用规则。未核实的强断言保持禁用，不用自定义假设填空。
- mixed-vendor 两个方向分别执行；先固定简单类型和已验证公共 QoS，避免把类型系统差异与发现故障混在同一首例。
- 原生 carrier 如必需，作为同一个 registry/runner 的后端，记录独立 participant/build/runtime 身份；成熟支持矩阵按 API/厂商组合定义，不强套旧“两个RMW”的计数单位。
- 出口：声明范围中的 participant、filter、投影与混厂商合同均有逐项真执行及反例控制；不能表达或观察的必需项明确阻塞该版本相应覆盖成熟。

### F6：持续 episode、历史变异和多前缀调度

- 保留 isolation=per_case 和 isolation=episode 两种 profile，不以持续场景替换独立重放。
- 一条 episode 是完整输入程序；其中子 case/action 有自己的序号、输入、前后状态、效果和 oracle。失败必须引用前史，不拿失败子 case 单独 hash 假装可复现。
- 初版建议每 episode 最多50动作或60秒，以先到者为界；健康/日志/内存预算越限立即收尾。阈值是工程提案，校准后冻结。
- 重置优先用验证过的清理/重建及 reset receipt；不能恢复到基线则新建 runtime。每个 episode 重新建立身份，不假装回到未存在的快照。
- 同一目标状态至少保留首次创建、网络恢复后、删除重建后三类实际支持前缀；按覆盖、命中率和成本选择。前缀可做替换/插入/删除，但须满足依赖与资源约束。
- 状态模型分规范允许状态、实际观察状态、前缀历史索引；新观察不自动修改允许集合。观察新边可加分，unknown/工具异常不能刷奖励。
- 完整 episode replay 与缩减：先复现整段，再依赖保持地删减动作；并发重放记录逻辑调度与实际区间，不承诺完全复现 OS 调度。
- 出口：独立/持续均可运行和重放，至少两类前缀实际被反馈调度选中、历史不丢失、缩减不会改变异常归因。

### F7：discovery 协议健壮性扩展

- 与 F1–F6 分开 scope/profile，优先合法基线上的结构化字段/边界/关系变异，再接任意 bytes 算子。
- 用同一 carrier/input/receipt/report/candidate 接口；输入模型区分合法性、可路由性、被哪层接受或拒绝。
- 明确接收层证据：发到网卡、进入解析器、进入发现状态更新不能相互替代；解析层崩溃与语义图错误分别归因。
- 如果批准的“完整 S1 版本”包含此项，F7未验收时只能发布标明不含协议健壮性面的 S1 semantic profile，不能称整体范围已完成。
- 本文不实施任何报文构造或新 campaign；F7有自己的后续详细任务卡、规范核对和冻结运行预算。

### F8：研究对照与效果报告

- 复用长期计划中的对照思想；先小规模等预算试点，再决定大型 B0–B3/消融预算，不立即继承约30小时数字。
- 若要评价模型贡献，对照使用共同输入空间与相同可观测指标；若比较原固定系统和扩展系统，只能先报告整体工程收益，不能将所有收益归因给语义模型。
- 报告有效输入率、真实 mutation hit、状态/边/关系覆盖、每单位时间覆盖、候选及独立审核结果、重放率和观察开销。
- 候选0允许得出部分工程可行结论，不允许声称已证明真实缺陷发现收益；只发现1例也不等于统计证明优于基线。

## 5. 成熟验收：必须预先冻结的覆盖矩阵

新增 `s1-maturity-vNext` 提案，不改写历史72分母；其机器可读清单待批准后由文档/campaign owner共同冻结。

每条能力行至少记录：

`capability_id / semantic_family / target_revision / surface_ids / topology_class / history_class / carrier / vendor_api_pair / required_or_exploratory / applicability / rule_refs / evidence_requirements / status`。

- 历史72 required＋24 negative仍是旧四target回归，不足以支持F3–F7的成熟声明。
- 新required矩阵基于批准的合同和代表性组合；全笛卡尔积通常不可穷尽，未跑组合应有明确coverage账，而不是默认继承pass。
- 新增target/rule/carrier或必要交互测试会增加新的验证义务。不能把所有新增行永久标exploratory，再靠旧72/72称完整成熟。
- 每个核心surface至少完成声明、可生成、可执行、真命中、适用oracle及健康重放/具名不重放处置；有语法但无runtime绑定仍是缺口。
- 必需边界、关系和交互类须在scope冻结前列明。抽样轮数/候选数量不能替代这些最低义务。
- required观察缺失不能按pass计；适用性必须事先有依据，事后遇失败不能临时删行或缩分母。
- 严格区分：工具证据链有效、被测合同通过、真实异常正在审核。发现有效SUT异常不等于工具质量不合格；原有seal规则若需支持已裁定SUT失败，必须另开显式政策决策，不能静默放宽原规则。

### 建议六道发布门槛

| 门槛 | 必须展示的证据 |
|---|---|
| M1 模型可执行 | 每条surface/fact/rule可解析，值域/守卫/算子/依赖有正反例，真正绑定runtime |
| M2 搜索不是固定矩阵 | seed影响内容；组合/序列变异真实执行；corpus留存、去重和反馈选择可复算 |
| M3 S1覆盖符合声明 | participant、逐对endpoint图、图生命周期、projection、filter适用部分及已批准carrier/vendor矩阵逐项验收 |
| M4 历史与并发可信 | 有真实重叠和历史差异；episode可保全/重放；不假装观察全部内部状态 |
| M5 证据/运行可靠 | controls、source冻结、独立raw对账、锁/清理、未知处理、异常保全与缩减均通过对应测试 |
| M6 异常审核可用 | 独立agent审核、主agent裁决、replay/non_replayable依据；零候选时如实注明真实非空链未验证 |

M1–M6满足且批准scope全部必需项完成后，可以称“该版本与支持矩阵内的S1成熟”。快照/独立通用ActionModel/大型压测平台不是天然前置；若用户选择把它们列为该版本必交，则应增加工程交付义务，但不要错误称它们就是S1语义本身。

## 6. 主agent派工与预算控制

- 主agent负责规范边界、接口和scope裁决、最终GO、证据核对；子agent执行代码和真实运行。
- 子agent指定gpt-6.1-sol，实际派遣时核验；不可用如实登记，不默换模型。
- 按现有core/runtime/campaign/network/semantic/docs六写集滚动推进，不新增第二套runner或常驻并行campaign；独立审核使用释放出的名额/未参与触发链与oracle实现的agent。
- F3运行前必须先具备逐对identity与观察能力；F4真实运行仅唯一owner；F5需要native等scope变化先登记；F6不借状态重用绕开source/health/owner检查。
- F1/F2既有运行预算只适用F1/F2。F3–F8要各自给出资源、attempt、wall-clock、cleanup及停止条件，不将原约6小时探索预算冒充全部扩展估时。
- 不在未测新后端耗时前承诺完成日期；依赖未知可通过独立capability验证推进，不能所有事项一律变成用户阻塞。

## 7. 本轮检查与未运行

本轮只更新计划与范围解释；未派子agent、未改代码/配置、未启动ROS/campaign、未提交Git、未产出新candidate或seal。

已核对本地分类、原任务书相关成熟/范围条款、现行计划和历史验收入口。规范级API细节与新增能力均待实施前验证。计划文档完整不等于工具完成。
