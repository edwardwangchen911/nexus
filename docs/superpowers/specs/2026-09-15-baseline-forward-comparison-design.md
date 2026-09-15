# baseline_v1 同窗前向对照暂缓决策

状态：已审查并暂缓；不作为实施依据

日期：2026-09-15

上位路线图：`docs/plans/2026-09-12-nexus-simulation-evolution-roadmap.md`

## 1. 结论

本轮不开发 `baseline_v1` 同窗前向对照。第二阶段继续使用 Agent、`hold_current` 和 SPY 市场基准
形成最小真实对照，`baseline_v1` 行保持 `UNAVAILABLE`。这不是删除固定策略，而是避免为了补一行
指标提前扩张价格用途政策、迁移和前向状态。

此前提出的“增加一个隔离 baseline 比较账户并复用现有 forward 链”只有账户隔离部分成立；进一步
核对真实代码后确认，现有 forward 链不能消费真实 provider 的 baseline 特征，因此该方案不能直接
实施，原 97 分结论撤回。

## 2. 已确认的硬断点

截至 2026-09-15 的代码和业务库只读核查显示：

- 业务库有 5 条 Agent 决策，没有 `baseline_v1` 前向决策、rank checkpoint、订单或成交；
- `backtest_runs`、`backtest_authority_bundles` 和 `backtest_authority_collections` 均为空，不能拿
  历史回测冒充同窗前向事实；
- `paper_once.forward` 的 `require_forward_adjusted_authority()` 只接受冻结的
  `source_policy_v2`，但该政策明确是 raw-only，复权资格固定返回 false；
- `strategy.snapshot_reader._bars()` 明确只接受 fixture。真实 provider 尚未在该合同中证明
  USD/share 和 share-volume 单位，因而返回 `STRATEGY_SNAPSHOT_PRICE_UNITS_UNAVAILABLE`；
- `persist_forward_strategy_decision()` 再次强制新 registered decision 使用 `source_policy_v2`，随后
  因复权资格关闭而拒绝；
- 已有 Agent paper 日线政策只授权 Alpaca SIP、delayed、1Day、`adjustment=all`，而现有 baseline
  特征合同要求独立 raw/all 引用，二者不是同一个可直接接通的合同；
- 当前 AAPL 只有一根已发布的 Alpaca SIP all-adjusted 日线，质量报告为零。即使补齐 200 根日线，
  上述用途和单位门禁仍会先拒绝。

因此真实阻塞属于“产品政策尚未支持”，不是简单数据缺失或页面接线缺陷。不能修改旧政策开关、
重标历史 authority 或放宽 fixture 门禁来通过验收。

## 3. 当前第二阶段范围

当前 `/paper` 连续模拟总览保留四行稳定结构：

1. **Agent**：读取主内部模拟账户的持久化绩效；
2. **hold_current**：全现金起点下为零收益、零回撤、零换手；
3. **baseline_v1**：显示 `UNAVAILABLE / SAME_WINDOW_FORWARD_FACTS_UNAVAILABLE`；
4. **market_benchmark**：读取已持久化的 Alpaca SIP all-adjusted SPY 日线。

第二阶段的主线完成条件收敛为：持续产生 Agent cycle，观察跨槽恢复和停止条件，并取得真实
order-capable 路径证据；三方可用结果使用相同的持久化时间口径。baseline 不再阻塞这一阶段，也不在
Agent 失败时接管模拟交易。

页面刷新继续只读，不触发行情、模型、策略或订单；`execution_locked=true` 保持不变。

## 4. 本轮明确不做

- 不新增 baseline 比较账户、账本或初始化入口；
- 不新增或修改价格用途政策、数据库迁移和 provider capability；
- 不修改 `FrozenStrategyReferences` 的 raw/all 合同；
- 不允许同一 all-adjusted bar set 同时冒充 raw 与 adjusted 输入；
- 不用历史回测、当前价格倒推或事后重放填充 baseline；
- 不合并或复制 Research Shadow 分支；
- 不新增比较事件总线、调度平台、Redis、Kafka 或券商写入。

## 5. 后续重新启动条件

只有同时出现以下条件，才重新设计 baseline 前向对照：

1. 已积累足够的 Agent 连续运行和真实订单路径样本，固定策略对照确实会改变版本保留或淘汰判断；
2. 明确选择一份新的、不可变的 baseline forward 价格政策，而不是修改已应用政策；
3. provider 合同能够持久化并验证价格币种、每股价格和成交量单位；
4. 明确 raw/all 的输入语义以及复权价格与复权成交量如何进入 ADV20，不复用同一事实冒充两种口径；
5. 有可执行的前向迁移、历史兼容和回滚设计；
6. 先以一个未来时点的真实冻结案例证明可达，再讨论独立比较账户和公共绩效窗口。

届时仍优先复用现有策略、风控、订单、成交、账本和绩效服务；不得因为重新启动而默认建设 Shadow
或第二套执行系统。

## 6. 验收与恢复

本暂缓决策的验收是：

- `/paper` 的 Agent、hold 和 SPY 行继续读取现有持久化事实；
- baseline 行保持稳定 unavailable，不影响其他行；
- 没有新表、迁移、账户、策略决策、订单、成交、模型调用或 provider 调用；
- 路线图不再把 baseline 写成第二阶段当前阻塞；
- 后续执行者能从本记录定位真实拒绝点，不再重复提出“只补 200 根日线即可”的方案。

恢复旧展示无需操作，因为产品当前已经是 unavailable。若未来重新启动，必须新建设计和实施计划；
本记录不得直接转成开发清单。

## 7. 决策审查

| 维度 | 满分 | 得分 | 依据 |
| --- | ---: | ---: | --- |
| 主线目标贴合 | 20 | 20 | 优先连续 Agent 与真实订单证据 |
| 事实真实性 | 20 | 20 | 不把回测、事后数据或 fixture 冒充前向事实 |
| 范围控制 | 20 | 20 | 不为单行指标扩张政策、迁移和账户状态 |
| 风控与数据合同 | 20 | 20 | 保留 immutable policy、单位门禁和执行锁 |
| 可恢复性 | 20 | 18 | 暂缓无业务回滚；未来重启仍需新设计 |
| **总分** | **100** | **98** | **达到 95 分门槛，无 P0/P1 未决项** |

占位符扫描：无。矛盾扫描：已撤回原实施建议和 97 分结论。范围扫描：当前行动只有文档纠偏，不开发。
