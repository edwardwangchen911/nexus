# Bark 可行动通知契约

状态：产品与数据契约提案，尚未实施。

## 目标

Bark 不是后台事件日志，而是个人用户的交易结果提醒。通知只在用户需要理解一次仓位变化、异常或
当日结果时打断；同一笔内部模拟订单的生命周期只占用一条可更新通知。通知必须明确标注
`内部模拟`，不产生审批、下单、重试或解除 `execution_locked=true` 的能力。

## 当前实现的问题

- `strategy_decision.persisted`、`paper.order_batch.evaluated`、订单提交和成交分别通知，一次交易可能
  连续产生多条提醒。
- Bark 文案只按事件类型生成，完全忽略 payload，因此用户无法知道标的、方向、数量、价格和影响。
- Bark `id` 当前使用 outbox event ID，只能收敛同一事件的重投，不能把同一订单的状态变化更新到
  同一条通知。
- `NEXUS_BARK_READ_ONLY_URL` 为空时没有跳转入口，所以固定的“可在 NEXUS 查看”也没有实际操作价值。

## 打断策略

| 业务状态 | 是否即时推送 | 规则 |
| --- | --- | --- |
| 决策已持久化 | 否 | 只保留审计与页面记录 |
| 批次已评估 | 否 | 只保留审计与页面记录 |
| 观察、维持仓位、无订单 | 否 | 进入当日摘要 |
| 风控拒绝 | 是 | 立即说明原计划、拒绝原因和账户未变化 |
| 订单已提交 | 条件推送 | 等待 30 秒；若期间已成交，跳过等待态，直接发送成交结果 |
| 部分成交 | 条件推送 | 若 120 秒内继续成交，不单独打断；超过 120 秒仍未完成或临近收盘时更新通知 |
| 全部成交 | 是 | 立即用实际成交结果更新同一条通知 |
| 取消或终态拒绝 | 是 | 更新同一条通知，说明已成交与未成交数量及原因 |
| 执行结果未知、对账异常、持续运行停止 | 是 | 独立高优先级提醒；当前缺少完整生产事件，列为第二阶段能力 |
| 交易日完成 | 一次 | 生成一条当日摘要；当前缺少通知事件，列为第二阶段能力 |

同一 Agent 周期如果在一分钟内产生超过三笔订单，前三笔按订单通知，其余合并为一条周期摘要，
避免候选范围扩大后形成通知风暴。第一阶段可先保持现有有界候选范围，并用测试证明不会超过上限；
不得在没有合并能力时静默丢弃事件。

## 通知身份与更新规则

- 订单通知使用 `id=<order_id>`，从风控结果到提交、部分成交、成交或取消都更新同一条 Bark 通知。
- 每个 outbox event 仍独立记录 delivery attempt；通知 ID 合并不改变 outbox 的至少一次投递和审计。
- 同一事件重投必须生成完全相同的正文；较早状态不得覆盖数据库中已经确认的较新订单终态。
- 当日摘要使用稳定的 `id=<account_id>:<market>:<session>`，同一交易日只更新同一条摘要。

## 订单通知字段契约

通知快照在业务事实的同一数据库事务内生成并写入现有 outbox payload 的
`notification_snapshot`；Bark worker 只验证、格式化和加密，不在延迟发送时重新查询“当前余额”来
冒充事件发生时余额。

```text
notification_snapshot.schema_version = paper_notification_v1
notification_snapshot.mode = internal_paper
notification_snapshot.order_id
notification_snapshot.agent_cycle_id
notification_snapshot.occurred_at
notification_snapshot.symbol
notification_snapshot.side                 # buy / sell
notification_snapshot.status               # risk_rejected / submitted / partially_filled / filled / cancelled
notification_snapshot.requested_quantity
notification_snapshot.approved_quantity
notification_snapshot.filled_quantity
notification_snapshot.remaining_quantity
notification_snapshot.price.kind           # reference / worst_case / average_fill
notification_snapshot.price.value
notification_snapshot.price.currency
notification_snapshot.notional              # 预计或实际，随 status 固定含义
notification_snapshot.fee                   # 只有成交事实可用，否则为 null
notification_snapshot.cash.kind             # projected_after / actual_after / unchanged
notification_snapshot.cash.available
notification_snapshot.position.kind         # projected_after / actual_after / unchanged
notification_snapshot.position.quantity
notification_snapshot.position.portfolio_ratio  # 无可靠同一时点净值时为 null
notification_snapshot.agent_reason
notification_snapshot.invalidation
notification_snapshot.risk_reason
```

所有 Decimal 在 payload 中使用规范十进制字符串，时间使用 UTC，币种显式保存。缺失值保持 `null`，
通知显示“暂不可用”，不得用零或发送时查询到的较新状态填充旧事件。

## 字段来源与可达性

| 通知字段 | 已有持久化来源 | 结论 |
| --- | --- | --- |
| 股票代码 | 已验证 `ResearchIntent.display_symbol`；也可由 `instrument_id` 关联 instruments | 已具备 |
| 买卖方向、计划数量 | `PaperOrderRecord.side/quantity` | 已具备 |
| 风控后数量、预计现金 | `RiskEvaluationRecord.approved_quantity/projected_available_cash` | 已具备 |
| 参考价、最坏价格 | `PaperOrderRecord.reference_close/worst_price` | 已具备，但必须标注价格含义 |
| 成交数量、剩余数量 | `PaperAdvanceResult` 与订单事件 | 已具备 |
| 成交价、成交额、费用 | `PaperFillRecord.price/notional/fee` | 已具备 |
| 实际剩余现金、实际持仓 | 成交事务完成后的账本与持仓投影 | 可计算，但必须在同一事务中冻结到通知快照 |
| Agent 理由 | 已验证 `ResearchIntent.synthesis_assessment.claims` 或 `theses` | 已具备，确定性截取，不再次调用模型 |
| 失效条件 | `ResearchIntent.invalidation_conditions` | 已具备；无条件时明确“未提供” |
| 风控拒绝原因 | `RiskEvaluationRuleRecord` | 已具备，需要固定中文规则映射 |
| 当日净值与盈亏 | `PaperPerformanceSnapshotRecord` | 已具备快照，但尚无通知生产事件 |

`agent_reason` 取已验证 synthesis 的第一条 claim；没有 synthesis claim 时取第一条 thesis。只做
Unicode 安全的长度限制，不生成新结论、不拼接原始证据正文。`invalidation` 由固定 signal/operator/unit
字典渲染；无法渲染时显示“失效条件暂不可用”，不得把内部表达式直接展示给用户。

## 展示模板

### 等待成交

```text
[内部模拟] AAPL 买入 12 股 · 等待成交

参考价：$187.40｜最坏价格：$188.10
预计金额：$2,257.20
预计成交后：AAPL 42 股｜可用现金 $21,397.30
Agent：盈利预期上修，价格突破近期区间，仓位仍在风险上限内。
失效：价格跌破既定阈值或关键基本面证据变化。
```

### 已成交

```text
[内部模拟] AAPL 买入 12 股 · 已成交

成交均价：$187.36｜成交额 $2,248.32｜费用 $0.00
成交后：AAPL 42 股｜可用现金 $21,406.18
Agent：盈利预期上修，价格突破近期区间，仓位仍在风险上限内。
失效：价格跌破既定阈值或关键基本面证据变化。
```

### 风控拒绝

```text
[内部模拟] AAPL 买入 20 股 · 风控拒绝

参考价：$187.00｜计划金额 $3,740.00
原因：成交后单一标的仓位将超过 20% 上限。
账户未变化：AAPL 30 股｜可用现金 $23,654.50
Agent：盈利预期上修，但确定性仓位限制不允许本次增仓。
```

模板中的数字只是版式示例，不得作为测试或运行事实。

## 当日摘要契约

摘要仅在交易日运行已经终止且对应绩效快照已持久化后产生；市场日历、绩效或对账任一不可用时，
显示“摘要不可用”及固定原因，不发送猜测数字。

```text
[内部模拟] 今日运行完成 · 2026-09-16

运行 6 次｜形成 3 个决策
买入 1｜卖出 0｜维持 2
成交 1｜风控拒绝 0｜异常 0
组合净值 $50,328.40｜今日 +$126.20（+0.25%）
可用现金 $21,406.18
```

摘要必须区分零与未知，并明确币种、收益分母和交易日。没有可靠前一交易日快照时，不显示日收益率。

## 隐私与安全

- 允许展示当前内部模拟账户的标的、数量、价格、现金和持仓，因为这是通知可用性的核心；仍不展示
  account ID、hash、证据 ID、模型 prompt、原始证据正文、密钥或执行 token。
- `notification_snapshot` 只能存在于已授权本地 PostgreSQL 和加密后的 Bark payload，不进入普通
  日志、错误信息、测试快照或 Git。
- 推荐 iPhone 将 Bark 通知预览设置为“解锁时”，降低锁屏暴露资金和仓位的风险。
- 通知仍是只读结果，不提供批准、重试交易或修改风控的按钮；结果未知时只引导核对。

## 分阶段实施

### 第一阶段：高价值订单通知

1. 停止即时消费决策持久化和批次评估事件。
2. 在订单/执行事务中生成闭合的 `paper_notification_v1` 快照。
3. Bark mapper 验证快照并生成详细中文通知。
4. 使用 order ID 更新同一条通知，覆盖风控拒绝、等待成交、部分成交、成交和取消。
5. 保留 AES、activated-at、有限重试、死信和 localhost-only 网络边界。

第一阶段不引入新表、迁移、模型调用或全局通知偏好系统。

### 第二阶段：降噪与每日摘要

1. 增加 30 秒等待态延迟、120 秒部分成交合并和每周期突发上限。
2. 为执行结果未知、对账异常和持续运行停止补生产事件。
3. 在交易日结束和绩效快照提交后生产一次当日摘要。
4. 再评估是否需要通知类别开关；不在第一阶段预建设置中心。

## 验收标准

- 一笔成功成交的订单在 iPhone 上最多出现一条通知，最终显示实际成交价、数量、现金、持仓和
  Agent 理由；数据库中仍保留每个 outbox event 的独立投递记录。
- 风控拒绝不会显示预计成交后的状态为实际状态，且明确账户未变化。
- 部分成交、成交和取消的旧事件不能覆盖较新的订单终态。
- 任一缺失或不可比字段显示未知，不回查较新状态填充历史通知。
- 通知正文不包含账户 ID、hash、证据正文、密钥或执行 token。
- 通知失败不回滚业务事实，不触发决策、行情、模型、订单或执行重放。
- 真实验收至少覆盖：风控拒绝一例、完整成交一例、iPhone 解密显示、outbox/attempt 持久化回读。
