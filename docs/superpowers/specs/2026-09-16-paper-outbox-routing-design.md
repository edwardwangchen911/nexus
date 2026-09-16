# Paper outbox 路由收敛设计

## 背景与问题

Paper 批次当前会在同一事务内同时写入业务事实、`audit_events` 和 `outbox_events`。
`strategy_decision.persisted@paper_event_v1` 与
`paper.order_batch.evaluated@paper_event_v1` 已经由审计事件持久化，并被风险审计页面读取；
仓库内没有生产消费者读取对应 outbox event。

Bark worker 只消费风控拒绝、最终成交和最终取消。它不应领取决策持久化或批次评估事件。
因此每个无订单批次仍会新增两条永远保持 `queued`、`attempt_count=0` 的 outbox event，造成
无界积累，并容易被误判为通知投递失败。

## 目标与非目标

目标：

- 新批次不再为上述两个没有消费者的事实创建 outbox event；
- 同事务内的业务事实和 `audit_events` 保持不变；
- Bark 的事件白名单、通知内容和投递语义保持不变；
- 历史 outbox、delivery attempt 和业务事实保持原样；
- 没有订单的成功批次不再制造待消费 outbox backlog。

非目标：

- 不删除、改写或伪造发布历史 queued、published 或 dead-letter event；
- 不增加空操作消费者来把未实际交付的事件标记为 published；
- 不新增 outbox 状态、迁移、定时清理或保留策略；
- 不扩展 Bark 到普通决策、批次、订单提交或部分成交通知；
- 不改变订单、风控、账本、模拟成交或实盘边界。

## 方案选择

对本次已确认的两个批次级事实，采用“没有异步消费者时只保留审计、不创建 outbox”方案。
其余 Paper 事件的消费者合同不在本切片内重新分类。

未采用通用空操作消费者，因为它没有交付目标，却会制造虚假的 published 语义。未采用继续无限
排队，因为当前没有未来消费者合同、运行入口或容量边界。未采用历史回填或清理，因为历史状态是
诊断旧版 Bark Worker 行为的重要证据，且删除或改写不可逆。

## 代码与数据流

Paper 服务把当前同时写审计和 outbox 的内部操作拆成两个明确入口：

1. 审计必需事实：调用现有 `append_audit_event(...)`，继续写入
   `strategy_decision.persisted` 和 `paper.order_batch.evaluated`。
2. 可投递事实：在审计之外按既有合同创建 `OutboxEvent`。

创建批次时，两个批次级事实走审计入口，不创建 outbox。订单级风控拒绝以及现有订单生命周期中
确有消费者或兼容合同的事件继续沿用既有 outbox 行为。本次不借机重构其余事件路由。

`nexus.paper.outbox` 的兼容注册表同步移除这两个事件，避免测试、维护脚本或未来误调用把历史
queued event 当作已交付。Bark 注册表无需修改，因为当前已经不包含它们。

## 兼容性与历史数据

这是本地单用户应用的生产者合同收窄。仓库中不存在这两个 outbox contract 的生产消费者；风险审计
页面读取 `audit_events`，因此用户可见审计不受影响。

已有 outbox event 保持原状态：

- 旧版 worker 形成的 published/dead-letter 记录继续作为历史投递证据；
- queued 记录不重试、不删除、不改成 published；
- 当前 Bark 状态只按 Bark 白名单统计，不能把这些历史 queued event 计作待通知。

如果未来出现明确的外部消费者，应以新版本事件合同和独立启用时间重新设计，而不是复用这些历史
queued event 或恢复无目标生产。

## 错误处理与回滚

业务事务的原子性不变：业务事实或审计写入失败时，整个批次仍回滚。移除两条 outbox 写入不会增加
新的失败模式。

代码回滚可恢复生产这两类 outbox event，但不会自动补发本设计生效期间未生成的事件。审计事实仍在，
若未来确需迁移，应从明确版本的审计合同设计一次性迁移，而不是临时重放业务流程。

## 验证

测试覆盖以下合同：

1. 无订单/无变化批次仍原子写入两个审计事件，但不创建这两类 outbox event；
2. 风控拒绝等仍需异步消费的事件继续创建对应 outbox event；
3. 故障注入仍证明业务事实与审计同事务回滚；
4. Paper outbox 兼容注册表不再领取这两个历史事件；
5. Bark 白名单测试继续证明只领取风控拒绝、最终成交和最终取消；
6. 定向 pytest、Ruff 和 Mypy 通过，并核对真实数据库没有被测试修改。

验收只针对新生产行为；历史队列数量不会因本改动归零，也不作为失败条件。
