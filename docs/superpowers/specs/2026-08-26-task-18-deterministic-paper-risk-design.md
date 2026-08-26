# Task 18：确定性硬风控与内部模拟账本设计规格

- 日期：2026-08-26
- 状态：已获用户长期授权，可直接生成并执行实施 Plan
- 适用范围：阶段 1 美股内部模拟账户的决策、风控、订单、资金预留和会计事实
- 前置提交：616293d docs: record Task 17 shadow acceptance
- 后续任务：Task 19 读取已提交订单，模拟成交并生成持仓与绩效

## 1. 结论

Task 18 实现一个与复星只读账户完全隔离的、初始资金为 USD 10,000 的内部现金模拟账户。每次有效的 baseline_v1 前向策略决策，先绑定同一决策会话的完整时点数据权威，再由纯函数硬风控按固定顺序计算；通过或缩减的候选自动成为内部 submitted 订单，同时以追加式双重记账冻结现金或可卖数量。

本任务不模拟成交，不形成持仓事实，不调用复星，不访问任何真实账户或密钥，也不增加人工批准或实盘能力。Task 17 的 validated 提案不能自动生成订单。Agent 只能提供已审计研究证据，不能改变标的、方向、数量、价格、风控规则、排序、账户或状态转换。

架构采用独立事件域，而不是复用 broker snapshot：

~~~text
同会话完整 PIT 权威 + baseline_v1 前向决策
  -> 持久化不可变 StrategyDecision
  -> 构建 PaperPortfolioProjection
  -> 纯函数 evaluate_order_batch
  -> 原子保存 Batch / Order / RiskEvaluation / Event
  -> Cash 或 Position Reservation
  -> 平衡 LedgerTransaction
  -> AuditEvent + OutboxEvent
  -> Task 19 读取 submitted 订单
~~~

## 2. 范围与非目标

### 2.1 本次包含

- 单一稳定身份 internal_usd_v1，USD 现金账户，初始资本 10,000。
- baseline_v1 前向策略决策及目标行的不可变持久化。
- 单标的 15%、行业 30%、最低现金 10%、回撤锁、整数股、最低买入额和可卖数量等确定性规则。
- 数据、交易日历、白名单、价格、行业穿透和质量权威的 fail-closed 校验。
- approved、reduced、rejected 三种风险结果及逐规则证据。
- proposed、risk_rejected、risk_approved、submitted 的内部模拟订单事件。
- 现金和仓位预留；Task 18 只创建 reserved 事件。
- 追加式双重记账和数据库平衡约束。
- 请求幂等、同一策略决策唯一批次、并发锁顺序、审计和 outbox。
- 单元、属性、迁移、集成、并发和数据库绕过测试。

### 2.2 本次不包含

- 不实现 Task 19 的分钟 VWAP、成交、撤单、持仓、绩效或页面。
- 不实现复星模拟盘、实盘、awaiting_approval 或任何券商写路由。
- 不读取 BidAskInfo，不用最新报价替代决策会话收盘价。
- 不实现融资、融券、卖空、期权、碎股、多币种或外汇换算。
- 不把 ShadowPortfolio、PortfolioSnapshot 或 broker order 当作内部模拟账本。
- 不允许 HTTP 请求直接创建订单；订单只能来自版本化策略决策。
- 不允许 Agent、前端或调用者提交目标数量和风险结果。
- 不因 Task 17 validated、回测胜出或 Agent 建议自动建立策略决策。

## 3. 核心不变量

1. 账户只允许 account_kind=internal_paper、base_currency=USD、cash_account、long_only。
2. 订单只能引用一个已持久化、完整且未消费的前向策略决策。
3. 风控使用决策会话最晚完整可用的 selected raw daily close，不使用调用时最新价。
4. 缺少任一所需权威、数据陈旧、来源冲突、待决公司行动、停牌、未对账或投影失败时，整批拒绝且不建立部分订单。
5. 通过后的最坏价格投影仍满足：现金比例至少 10%，单名不超过 15%，行业穿透不超过 30%，持仓与现金不为负。
6. 卖单数量不超过已持仓数量减未完成卖出预留；买单不使用待卖出订单的潜在现金。
7. 订单状态、预留、回撤状态和会计事实只追加事件，不覆盖历史。
8. 每个 ledger transaction 的借方合计等于贷方合计且大于零。
9. 相同 Idempotency-Key 和相同请求哈希返回原结果；相同 Key 不同哈希稳定冲突。
10. 同一 account_id 与 strategy_decision_id 最多产生一个 order batch。
11. 任意异常都不能产生未审计 submitted 订单，也不能留下孤立预留或不平衡记账。
12. Task 18 验收时 paper_fills、position adjustment、position snapshot 和 performance snapshot 均为零行。

## 4. 信任边界与模块

| 模块 | 职责 | 禁止依赖 |
| --- | --- | --- |
| nexus.risk.models | 不可变风险输入、结果、规则证据 | SQLAlchemy、HTTP、Agent |
| nexus.risk.policy | risk_policy_v1 常量与自校验 | 数据库、系统时钟 |
| nexus.risk.authority | 把持久化权威转换为完整风险快照 | LLM、前端输入 |
| nexus.risk.evaluate | 确定性纯函数批次计算 | 数据库、网络、随机数、系统时钟 |
| nexus.paper.models | 订单、事件、预留和账本领域对象 | ORM、broker SDK |
| nexus.paper.orders | 订单状态机和事件序列校验 | 数据库状态变更 |
| nexus.paper.ledger | 双重记账纯函数与余额投影 | 成交模拟、LLM |
| nexus.paper.service | 锁、幂等、权威加载、原子持久化 | broker gateway |
| nexus.persistence.models.paper | ORM 映射与数据库约束 | 业务计算 |

依赖方向固定为 strategy -> risk -> paper。risk 只接受领域对象，不读取 agents 内部实现。paper 不调用 LLM，也不导入复星网关包。

## 5. 内部模拟账户与身份

现有 Account 和 Portfolio 继续作为跨域只读身份根；新增 paper_accounts 以 account_id 和 portfolio_id 一对一引用它们。是否为内部模拟账户由 paper_accounts 记录决定，不能从 provider 名称、外部账户号或前端 kind 推断。

迁移创建一个稳定 seed：

| 字段 | 值 |
| --- | --- |
| account code | internal_usd_v1 |
| portfolio code | internal_usd_v1 |
| currency | USD |
| account model | cash |
| position model | long_only |
| initial capital | 10000.000000000000 |
| capability | simulation_write_internal |

seed UUID 使用固定命名空间的 UUIDv5，迁移重放结果一致；运行时业务 UUID 继续使用项目 UUIDv7 生成器。paper account 不保存 broker_account_id、provider account ID 或凭据。

初始化同时创建账本科目、第一笔 funding 事务和 unlocked 回撤事件：

- 借：asset.cash.available.usd 10000
- 贷：equity.contributed_capital.usd 10000

## 6. 前向策略决策

Task 16 的 StrategyDecision 目前是纯函数结果。Task 18 增加不可变 strategy_decisions 和 strategy_decision_targets，固定字段至少包括：

- account_id、portfolio_id、strategy_id、strategy_version；
- decision_session、decision_time、execution_session；
- source_policy_id、calendar_authority_id、universe_snapshot_id、quality_report_id；
- 当前仓位投影哈希、输入权威哈希、策略参数哈希和 decision_hash；
- cash_weight、reason、created_at；
- 每个 instrument_id 的 target_weight、exit 标记、strategy_priority 和理由。

前向决策只允许 baseline_v1 的 WEEKLY_REBALANCE 结果。NOT_WEEK_END 可以记录审计事实但不能创建可消费决策。decision_session 必须是冻结交易日历中的 session，execution_session 必须是严格下一个 session。

决策目标从 baseline_v1 输出构造，拒绝重复标的、负权重、权重和大于 1、非有限 Decimal、非白名单标的和请求方附加目标。退出标的目标权重固定为零。相同输入必须得到相同 decision_hash；同一账户、策略版本和决策会话只允许一个有效决策。

策略决策持久化与订单批次分开：先生成并提交决策事实，随后批次服务引用它。订单失败不改写策略决策，重试仍引用同一 decision_id。

## 7. 数据和时点权威

风险批次必须绑定同一 decision_session 的精确集合：

- ExchangeCalendarAuthorityRecord：证明 decision_session 与 execution_session；
- UniverseSnapshot 和 UniverseMember：证明标的在该会话白名单；
- PriceBarSetAuthority：每个当前持仓和目标标的恰有一个 selected raw close；
- SectorExposure：每个标的完整 NEXUS_SECTOR_V1 穿透，权重和为 1；
- QualityReport：覆盖上述完整集合且状态合格；
- 公司行动、停牌和退市查询证明：截至 decision_time 没有阻塞事件；
- 组合投影证明：账本、订单、预留和 Task 19 事实扫描达到同一事务快照。

集合要求是 exactly-once：多一个、少一个、重复、跨 session、跨 policy 或 authority hash 不匹配都拒绝整批。available_at 必须不晚于 decision_time。raw close 必须来自 source_policy_v1 选择结果，adjusted close、分钟价、broker quote 和页面缓存均不合格。

风险评估保存所用 price、sector 和 query proof 行，不只保存外键。这样历史评估在来源修订后仍能重算原哈希。来源实际 delivery_mode 和 available_at 进入风险输入摘要，不得宣称实时。

每个 query proof 必须外键引用不可变的 `risk_query_authorities` 根。该根固定
source policy、universe、decision session、查询种类、完整 required instrument
集合、blocked instrument 集合、来源水位哈希、available_at 和 content hash；空结果
也必须通过该权威根证明查询范围和数据水位，不能只保存一个无来源的随机 UUID。

## 8. 组合投影

PaperPortfolioProjection 是风险内核唯一接受的账户状态，字段至少包含：

~~~python
class PaperPortfolioProjection:
    account_id: UUID
    as_of: datetime
    currency: Literal["USD"]
    available_cash: Decimal
    reserved_cash: Decimal
    positions: tuple[PaperPositionBalance, ...]
    open_orders: tuple[PaperOpenOrder, ...]
    nav: Decimal
    peak_nav: Decimal
    drawdown_state: Literal["unlocked", "locked", "recovery_eligible"]
    projection_hash: str
~~~

Task 18 尚无成交，因此初始投影为现金 10,000、零持仓。接口和数据库必须支持 Task 19 后续追加 fill、position adjustment、position snapshot 和 performance snapshot；但 Task 18 不能自行写入这些表。

NAV 使用 available_cash + reserved_cash + 持仓数量乘同会话权威 close。待买订单按 worst_price 占用现金并计入最坏风险暴露；待卖订单只减少可卖数量，不提前增加现金或降低当前风险暴露。

projection_hash 覆盖所有参与投影的 ledger entries、reservation events、order events 和 Task 19 事实的稳定摘要。只要事件链存在序号缺口、非法转换、负余额、不平账或未知事实，投影构建失败，风险整批拒绝。

## 9. risk_policy_v1

| 规则 | 固定值 |
| --- | --- |
| single name limit | NAV 的 15% |
| sector look-through limit | NAV 的 30% |
| minimum cash | NAV 的 10% |
| drawdown lock | drawdown >= 8% |
| recovery eligible | 已锁定且 drawdown < 6% |
| unlock | recovery_eligible、用户确认且服务端复核仍 < 6% |
| buy minimum | reference close 计价至少 USD 100 |
| quantity | 正整数股 |
| buy reserve | quantity × reference close × 1.01 + fees |
| commission | v1 为 0，但字段必须保存 |
| supported market | US |
| supported instruments | common_stock、ETF |
| supported currency | USD |

阈值使用精确 Decimal，不接受 float。比例比较不提前舍入；数量始终向下取整到整数股。15% 和 30% 是包含边界，现金 10% 是包含边界。买入最低额只按 reference close 计算，不用 1% reserve markup 伪造达标。

RiskPolicy 是不可变版本记录。运行时加载后必须重新计算 content_hash 并校验全部字段；未知版本、重复 active 版本、哈希不符或非法阈值均以 RISK_POLICY_INVALID 拒绝。

## 10. 确定性候选与批次算法

目标数量按以下公式计算：

~~~text
target_quantity = floor(target_weight × nav / reference_close)
desired_delta = target_quantity - current_quantity
~~~

exits 的 target_quantity 固定为零。desired_delta 为零时保存 NO_CHANGE 评估证据但不创建订单。

候选稳定排序：

1. strategy_priority 升序：退出/减仓为 0，现有目标调整为 1，新建仓为 2；
2. target_weight 降序；
3. instrument_id 的 UUID 数值升序。

先处理卖出/减仓，再处理现有目标和新建仓，但风险计算不得把未成交卖单当作释放现金或暴露。每个买入候选的最大可批准数量是以下上限的最小值：

- desired quantity；
- 单标的剩余额度可买整数股；
- 所有行业穿透剩余额度可买整数股；
- 保持 10% 最低现金后，按 worst price 可预留的整数股。

若最大数量等于 desired，结果 approved；在 1 到 desired-1 之间为 reduced；等于 0 为 rejected。缩减后若 reference notional 小于 USD 100，结果改为 rejected。卖出不受 USD 100 最低额限制，但受可卖数量限制且绝不建立空头。

批次是 all-authority-or-none、per-candidate-risk。即：权威或投影异常会拒绝整个批次且不创建订单；权威完整时，不同候选可以分别 approved、reduced 或 rejected。后一个候选必须看到前面已批准买单的最坏预留和暴露。

纯函数接口固定为：

~~~python
def evaluate_order_batch(
    candidates: Sequence[OrderCandidate],
    portfolio: PaperPortfolioProjection,
    market: RiskMarketSnapshot,
    policy: RiskPolicy,
) -> tuple[RiskEvaluation, ...]: ...
~~~

输入顺序不能影响输出。结果按稳定候选顺序返回，并保存 projected_available_cash、projected_cash_ratio、projected_name_ratio、projected_sector_ratios、approved_quantity、worst_price、reserve_amount 和逐规则证据。

## 11. 回撤锁事件

paper_drawdown_lock_events 是按 account_id 连续序号的不可变事件链：

~~~text
unlocked -> locked             drawdown >= 0.08
locked -> recovery_eligible    drawdown < 0.06
recovery_eligible -> locked    drawdown >= 0.06
recovery_eligible -> unlocked  用户确认且事务内复核 drawdown < 0.06
~~~

locked 和 recovery_eligible 均拒绝新买入，但权威完整的退出或减仓可以继续。用户确认接口属于 Task 20；Task 18 只实现纯状态机、数据库约束和初始 unlocked 事件，不能伪造确认。

drawdown 使用最新合格 paper performance snapshot 的 nav/peak_nav；若 Task 19 尚无绩效，使用初始资金并得出 0。缺失、陈旧、未对账或 hash 不符时不能默认 unlocked，而是以 PAPER_PROJECTION_INVALID 拒绝批次。

## 12. 订单事件状态机

通用 OrderStatus 保留总规格的未来状态，但 internal_paper 在 Task 18 只允许：

~~~text
proposed -> risk_rejected
proposed -> risk_approved -> submitted
~~~

awaiting_approval 只属于未来复星模拟盘或实盘；internal_paper 在领域层和数据库触发器都拒绝它。Task 18 也不能写 partially_filled、filled、cancelled、rejected 或 reconciling；这些由 Task 19 或未来外部执行域的专用应用服务写入。

每个 paper_order_events 使用 order_id 内连续 event_seq，从 1 开始。事件保存 from_status、to_status、occurred_at、actor_kind、reason_code、payload_hash。当前状态由事件折叠得出，paper_orders 不保存可覆盖的 current_status 真相。

数据库 deferred constraint trigger 在事务提交前校验：

- 第一个事件 from_status 为空且 to_status=proposed；
- 后续 from_status 等于前一 to_status；
- event_seq 无重复、无缺口；
- terminal 状态后不能追加；
- internal_paper 转换在 allowlist 内。

领域接口固定为：

~~~python
def transition_order(
    order: PaperOrder,
    target: OrderStatus,
    event: OrderEventInput,
) -> PaperOrder: ...
~~~

服务在同一事务中依次追加 proposed、risk_approved、submitted，或 proposed、risk_rejected。不能跳过风险事件，不能用更新语句纠正历史；纠错必须追加未来规格允许的补偿事件。

## 13. 预留

paper_reservations 是每个订单一个稳定 root，类型为 cash 或 position。paper_reservation_events 是连续序号事件：

~~~text
reserved -> consumed
reserved -> released
~~~

Task 18 仅写 reserved：

- 买单 cash reservation = approved_quantity × reference_close × 1.01 + fee reserve；
- 卖单 position reservation = approved_quantity。

同一订单只能有一个 root。cash reservation 币种必须 USD；position reservation 必须与订单 instrument_id 一致。余额由事件折叠，不保存可修改 balance。数据库和投影共同防止现金或仓位超额预留。

风险拒绝订单没有预留。Task 19 成交后用 consumed 和 released 事件分配实际成本并释放差额；取消全部释放。任何孤立预留、负剩余或终态后未清理预留都在后续对账中标记异常。

## 14. 双重记账

科目固定为：

| code | normal side | 用途 |
| --- | --- | --- |
| asset.cash.available.usd | debit | 可用现金 |
| asset.cash.reserved.usd | debit | 已冻结现金 |
| asset.position_cost.usd | debit | Task 19 持仓成本 |
| equity.contributed_capital.usd | credit | 初始投入 |
| income.realized_pnl.usd | credit | Task 19 已实现盈亏 |
| income.dividend.usd | credit | Task 19 股息 |
| expense.fees.usd | debit | Task 19 费用 |

买单预留生成一笔 ledger transaction：

- 借 asset.cash.reserved.usd；
- 贷 asset.cash.available.usd。

卖单只冻结数量，不提前产生现金记账。风险拒绝不记账。

paper_ledger_transactions 与 entries 都是 append-only。每个 entry 只允许 debit_amount 或 credit_amount 之一大于零，另一个为零；币种和科目必须匹配。deferred trigger 在事务末校验每笔 transaction 借贷相等且总额大于零。数据库触发器防止 UPDATE 和 DELETE，测试必须证明绕过 ORM 仍失败；本任务不新增角色、GRANT 或 REVOKE。

纯函数接口固定为：

~~~python
def post_ledger_transaction(
    ledger: Ledger,
    transaction: LedgerTransaction,
) -> Ledger: ...
~~~

纯函数也拒绝不平衡、重复 entry ID、未知科目、错误 normal side、负数、float 和超精度数值。数据库约束是第二道防线，不替代领域校验。

## 15. 数据模型与迁移

迁移固定为 0014_paper.py，down_revision=0013_shadow。它新增以下表：

- risk_policies
- strategy_decisions
- strategy_decision_targets
- paper_accounts
- paper_drawdown_lock_events
- paper_order_batches
- risk_no_change_evaluations
- paper_orders
- paper_order_events
- risk_evaluations
- risk_evaluation_rules
- risk_evaluation_price_inputs
- risk_evaluation_sector_inputs
- risk_evaluation_query_proofs
- risk_query_authorities
- paper_ledger_accounts
- paper_ledger_transactions
- paper_ledger_entries
- paper_reservations
- paper_reservation_events

为避免 Task 19 在已部署环境修改关键事件外键和 append-only guard，本迁移同时创建但不写入以下 Task 19 所有表：

- paper_fills
- paper_position_adjustment_events
- paper_position_snapshots
- paper_performance_snapshots

所有金额、权重、价格和数量使用 NUMERIC(38,12)，所有时间使用 UTC timestamptz，业务 ID 使用 UUID。hash 使用 64 字符小写十六进制。每个事件表都有业务唯一键、连续 seq 约束、created_at 和 payload_hash。

关键唯一约束至少包括：

- strategy_decisions(account_id, strategy_version, decision_session)
- paper_order_batches(account_id, strategy_decision_id)
- risk_no_change_evaluations(batch_id, instrument_id)
- paper_orders(account_id, client_order_id)
- paper_order_events(order_id, event_seq)
- risk_evaluations(order_id)
- paper_reservations(order_id)
- reservation events(reservation_id, event_seq)
- ledger entries(transaction_id, entry_seq)

旧迁移不修改。downgrade 只按依赖逆序删除 0014 自有表和触发器；不会删除或改写已有账户、组合、行情、策略、Shadow、审计或 outbox 数据。

## 16. 原子服务、幂等与并发

应用服务接口为：

~~~python
def create_paper_order_batch(
    account_id: UUID,
    strategy_decision_id: UUID,
    idempotency_key: str,
    request_time: datetime,
) -> PaperOrderBatchResult: ...
~~~

request body 不包含标的、权重、方向、数量、价格或风险结果。request_time 只用于确定审计时间和数据新鲜度，必须由受信应用层提供。

固定事务顺序：

1. 获取 account advisory transaction lock；
2. 锁定最新 drawdown event；
3. 按 UUID 锁定涉及的 order root；
4. 锁定 reservation root；
5. 锁定 ledger account；
6. 检查或建立 IdempotencyRecord；
7. 重新加载策略决策、完整权威和组合投影；
8. 执行纯风控；
9. 原子追加 batch、order、risk、event、reservation、ledger、audit 和 outbox；
10. 提交后由既有 outbox 机制发布。

同一个 key 与同一个 canonical request hash 返回原 batch，不产生新事件。相同 key 不同 hash 返回 IDEMPOTENCY_CONFLICT。同一 decision 使用不同 key 时，由唯一约束和事务重读收敛到原 batch；不能创建重复订单。

崩溃发生在提交前则全部回滚；提交后即使 outbox 尚未发布，dispatcher 也可重试。不会出现已提交订单但无风险评估、已冻结现金但无订单、或有账本但无预留的半状态。

## 17. 规范化哈希

统一使用 canonical JSON UTF-8、键排序、紧凑分隔符和 SHA-256。Decimal 固定 12 位字符串，UUID 小写，UTC 时间固定 6 位微秒并以 Z 结尾。数组按规格业务键排序。随机 row ID、created_at、数据库插入顺序不进入可重算摘要。

版本固定为：

- forward_strategy_decision_sha256_v1
- paper_projection_sha256_v1
- risk_batch_input_sha256_v1
- risk_evaluation_result_sha256_v1
- paper_ledger_transaction_sha256_v1
- paper_order_batch_sha256_v1

哈希版本升级必须新增 v2，不能静默改变 v1。持久化后集成测试从原始行重算每个哈希并逐值相等。

## 18. 稳定错误与规则代码

批次级：

- PAPER_ACCOUNT_INVALID
- STRATEGY_DECISION_INVALID
- RISK_POLICY_INVALID
- RISK_AUTHORITY_INCOMPLETE
- PAPER_PROJECTION_INVALID
- PAPER_BATCH_CONFLICT
- IDEMPOTENCY_CONFLICT

候选规则：

- NO_CHANGE
- UNSUPPORTED_ACCOUNT
- UNSUPPORTED_INSTRUMENT
- UNSUPPORTED_CURRENCY
- NOT_WHITELISTED
- MARKET_SESSION_INVALID
- PRICE_AUTHORITY_INVALID
- SECTOR_AUTHORITY_INVALID
- PENDING_CORPORATE_ACTION
- SUSPENDED
- DRAWDOWN_LOCKED
- INSUFFICIENT_POSITION
- SINGLE_NAME_LIMIT
- SECTOR_LIMIT
- CASH_FLOOR
- BELOW_MINIMUM_ORDER

规则证据保存 limit、observed、remaining、通过状态和参与的 authority IDs。错误响应不泄露 SQL、路径、provider payload、账户外部 ID 或密钥。

## 19. 审计与 outbox

每个批次至少产生：

- strategy_decision.persisted；
- paper.order_batch.evaluated；
- paper.order.risk_rejected 或 paper.order.submitted；
- paper.reservation.created；
- paper.ledger_transaction.posted。

事件 payload 只保存内部 UUID、版本、稳定 reason code 和内容哈希，不复制 provider 原始 payload 或外部账户标识。AuditEvent 记录 actor、correlation_id、idempotency key hash 和 request/result hash；不保存原始 key。

outbox aggregate key 固定为 account_id 或 order_id，事件 ID 全局唯一。业务事务不直接发布消息。重复 dispatcher 交付必须由消费方按 event_id 幂等处理。

关键指标包括批次延迟、批准/缩减/拒绝数、规则拒绝数、projection 失败、authority 不完整、幂等重放、并发冲突、ledger imbalance 尝试和 drawdown 状态。日志只使用结构化内部 ID。

## 20. 与既有域和后续任务的契约

- PortfolioService 可以识别 paper_accounts 并返回 kind=internal_paper，但 USD 模拟资产和复星 HKD/其他币种资产永不求和。
- Task 17 Shadow 继续是归一化研究证据；Shadow outcome、fill 和 NAV 不进入 paper ledger。
- Task 19 只能读取 submitted 内部订单，追加 fill/order/reservation/ledger/position/performance 事件；不能改写 Task 18 事实。
- Task 20 可调度 baseline_v1 决策和调用本服务，并提供回撤解锁人工确认 API；不能把浏览器调度或前端状态作为权威。
- Task 21 的验收证据要分别显示真实只读账户、内部模拟账户、Shadow，不能以汇总净值掩盖币种和来源。
- 未来复星模拟盘使用独立 account subtype、approval 和 gateway domain，不复用 internal paper 自动 submitted 路径。

## 21. 测试策略

### 21.1 纯函数与属性测试

- risk_policy_v1 阈值、边界等号、非法 policy 和 Decimal 精度。
- 候选输入全排列输出相同。
- 单名 15%、行业 30%、现金 10% 在边界允许，越界缩减或拒绝。
- 买单最小 USD 100，卖出豁免；整数股且永不超买、超卖或做空。
- 待买订单占现金和最坏暴露，待卖订单不释放现金或暴露。
- drawdown 8% 锁定、低于 6% 恢复资格、确认前仍锁定。
- 任意 Hypothesis 组合中，批准结果不产生负现金、不越硬上限。
- ledger 事务始终平衡；任意不平衡、重复、float 或未知科目均拒绝。
- 订单和预留状态机拒绝跳转、缺口、重复和终态追加。

### 21.2 迁移与数据库测试

- 从 0013 upgrade 到 0014，再 downgrade 到 0013，schema 与 seed 符合预期。
- seed 重跑不会生成第二个账户或第二笔 funding。
- SQL 直接 UPDATE/DELETE append-only 行失败。
- SQL 直接建立非法订单转换、事件 seq 缺口、不平账 transaction、负预留失败。
- 唯一约束防止重复 decision、batch、client_order_id 和 evaluation。
- Task 19 自有表存在但保持零行。

### 21.3 集成与并发测试

- 完整 fixture 从 baseline_v1 决策得到 approved/reduced/rejected、submitted 订单、预留、平衡账本、audit 和 outbox。
- 缺价格、行业、白名单、日历、质量、公司行动查询或 projection 证明时零订单、零预留、零新增账本。
- 相同 idempotency replay 返回相同 IDs 和相同 hashes，行数不变。
- 相同 key 不同请求冲突；相同 decision 不同 key 收敛到一个 batch。
- 两个数据库连接并发提交同一 decision，最终只有一个批次和一组订单。
- 在每个持久化阶段注入异常，事务后无半状态。
- 从数据库原始行重算所有 canonical hash。

### 21.4 回归

- Task 16 baseline_v1、回测和现有 fixture 语义不变。
- Task 17 Shadow 的 63-session 验收不创建 paper 行。
- 全部 engine 测试通过。
- 若 PortfolioService 增加 internal_paper 投影，相关 API/Web 契约测试通过且不混合复星资产。
- 默认测试无外网，并证明复星 gateway mock 从未被调用。

## 22. 验收门禁

Task 18 只有同时满足以下条件才完成：

1. 独立 USD 10,000 internal_usd_v1 seed 和平衡 funding 可由数据库重算。
2. 完整 fixture 能从 baseline_v1 前向决策原子生成确定性订单结果。
3. 每个 submitted 订单都有同决策、同权威、同风险版本、连续状态事件和精确预留。
4. 所有批准或缩减结果在 worst price 下仍满足现金、单名、行业、long-only 和整数股约束。
5. 缺失或异常数据产生零新订单，重复和并发调用不重复下单。
6. 数据库直接绕过不能改写追加式事实、建立非法转换或不平衡账本。
7. Task 19 自有事实为零，Task 17 validated 不自动产生订单。
8. 目标测试、全量 engine 回归、迁移往返、静态检查和独立审查均通过。
9. 审查重点前视偏差、时点错配、重复事件、幂等、审批边界、现金/仓位约束、hash 可重算和审计完整性，严重问题为零。
10. 没有新增复星写调用、真实账户访问、交易密钥、部署或权限变更。

## 23. 失败与回滚

- 运行时失败：事务回滚，保留原策略决策；修复数据后以相同 idempotency key 重试。
- 发布回滚：停止 Task 20 调度即可阻止新批次；已提交内部订单保留审计事实，不删除。
- schema 回滚：仅在 0014 尚无业务事件时执行 downgrade；已有事件时先停写并导出校验摘要，不能直接删除模拟账本。
- policy 回滚：创建新的 active policy 版本，不改写 risk_policy_v1 或历史评估。
- 任何真实账户或券商写能力仍保持物理和配置关闭，不属于本任务回滚面。
