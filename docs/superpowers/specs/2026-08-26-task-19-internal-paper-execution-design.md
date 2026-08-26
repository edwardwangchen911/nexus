# Task 19 内部模拟成交、绩效与模拟盘设计

**状态：** 可直接编码  
**范围：** 仅 `internal_paper` 美股现金账户  
**明确排除：** 复星或其他券商写操作、真实订单、部署、密钥、Agent 直接下单

## 1. 目标和安全不变量

Task 19 消费 Task 18 已经通过硬风控并处于 `submitted` 的内部模拟订单，使用冻结的 1 分钟行情生成可重演成交，在同一数据库事务内写入成交、订单事件、预留消耗/释放、持仓调整事件和双重记账。然后从追加事实投影持仓、绩效和 `/paper` 只读工作台。

必须始终成立：

- 只有 `paper_accounts.capability = simulation_write_internal` 可由 Task 19 推进；任何 `real_read_only` 或 `fosun_simulation` 对象均拒绝。
- 成交价、成交量、费用、订单状态和账本分录只由确定性代码生成；LLM 输出不是输入。
- 执行只读取订单已选定数据源的冻结分钟数据；不换源，不回退 IEX、最新报价、前收盘或网络请求。
- 任何数据在 `available_at > evaluated_at` 时都属于未来信息，必须拒绝。
- 成交根、证据成员、账本和事件链只追加；不允许 `UPDATE`/`DELETE` 修改历史。
- 幂等键和请求哈希绑定操作语义；同键同请求重放原结果，同键异请求返回冲突。
- 数据缺失、时点不合格、证据集不完整、预留不足或投影无法对账时 fail closed：不成交，且写入可审计的原因。
- 终态 `filled` / `cancelled` / `rejected` 不可再生成成交或事件。

## 2. 执行时间和数据权威

### 2.1 交易日历

`exchange_calendar_sessions` 在向前迁移中增加非空 `market_open_at` 和 `market_close_at` UTC 时点，并约束：

- `market_open_at < market_close_at <= decision_at`；
- 两个时点的纽约本地日期必须等于 `session`；
- 正常日与半日市均从权威行读取，执行器不硬编码 UTC 偏移或 16:00 收盘。

旧迁移不回改；`0015` 根据 `session` 用 `America/New_York` 规则为已有 fixture 行回填常规 09:30–16:00。非 fixture 的历史行如无法证明半日市收盘，Task 19 执行准入拒绝，由后续日历摄取修复，不猜测。

### 2.2 分钟窗口

- 第一个窗口是开盘后 `[09:30, 09:35)`，后续是连续的 5 分钟窗口。
- 窗口必须含 5 根每分钟一根、时间连续、同一 `PriceBarSetAuthority`、同一 instrument/source/feed/revision 的 `1Min` bar。
- 第一窗口最早在纽约 09:55 评估。后续窗口只有在其所有 bar 均已 `available_at <= evaluated_at` 时可评估。
- 如 10:15 前第一个合格窗口仍不存在，订单以 `missing_fill_data` 取消；不换源。
- 已有部分成交的订单在市场收盘时取消余量，原因 `paper_eod_remainder`。
- 窗口权威根持久化精确 bar ID 集、最大 `available_at`、成交量、VWAP 和内容哈希。已引用窗口不得换源或换成员。

### 2.3 VWAP、价差和滑点

Task 7 标准 bar 目前没有 provider VWAP 字段。为避免伪造交易级 VWAP，Task 19 把规则明确版本化为 `close_volume_vwap_v1`：

`window_vwap = sum(bar.close * bar.volume) / sum(bar.volume)`

一旦未来摄取并冻结 provider VWAP，必须发布新执行模型版本，不能改写 v1。

- 总成交量为 0 的窗口不可成交。
- 窗口容量 `floor(sum(volume) * 0.01)`，再与剩余订单量、预留可支持量取最小值。
- 只有当同 source/feed 的报价在窗口内观测、在评估时已可用、`0 < bid <= ask`、无质量错误时，才复制为不可变报价证据。
- 有合格报价：方向性半价差 `half_spread = (ask - bid) / 2`，基础滑点 10 bps。
- 无合格报价：`half_spread = 0`，保守滑点 25 bps。
- 买入价 `VWAP + half_spread + VWAP * slippage_bps / 10000`；卖出价反向扣减。
- 价格按 instrument tick 向不利于账户的方向取整（买入向上、卖出向下）；缺少有效 tick 时按 12 位小数保守取整。

## 3. 成交模型和持久化

### 3.1 新增权威表

`0015_paper_execution.py` 新增：

1. `paper_execution_models`：版本、schema 版本、内容哈希、完整参数 JSON；预置唯一 `execution_model_v1`。
2. `paper_execution_window_authorities`：订单/账户/标的、窗口序号、UTC 边界、评估时点、价格权威、source/feed/delivery mode、VWAP、volume、capacity、可选报价拷贝、最大可用时点和内容哈希。
3. `paper_execution_window_members`：窗口与精确 `price_bar_records` 成员的多对多关系，并由延迟约束触发器验证 5 根、连续、同权威且哈希匹配。
4. `paper_request_idempotency`：账户、操作类型、幂等键、请求哈希、结果资源和结果事件序号；用于取消和内部 worker 重试。
5. `paper_performance_attributions`：绩效快照下的 `cash` / `instrument` / `sector` 维度，持久化市值、已实现/未实现损益、费用和滑点影响。

`paper_fills` 向前增加：

- `execution_model_id` 和 `execution_window_authority_id`；
- `window_vwap`、`half_spread`、`slippage_bps`、`slippage_amount`；
- `commission`、`regulatory_fee`，且 `fee = commission + regulatory_fee`；
- `evaluated_at`；
- 数据库约束验证成交与窗口的 order/account/instrument/source authority 一致。

所有新表和 Task 19 会写入的原 Task 18 事实表保持只追加。数据库延迟触发器校验：

- order event 序列连续且 actor/transition 合法；
- fill 数量之和不超过 order quantity；
- fill 不超过对应窗口 capacity；
- 同一订单的所有窗口与 fill 固定同一 source/feed/price authority 系列；
- fill、预留事件、持仓调整和账本事务的一对一原子根完整；
- 预留累计消耗加释放恰好不超过根金额/数量，终态后恰好归零。

### 3.2 不可变执行模型

`execution_model_v1` 完整内容：

- `bar_interval=1Min`
- `window_minutes=5`
- `first_evaluation_local=09:55 America/New_York`
- `missing_data_cancel_local=10:15 America/New_York`
- `volume_participation=0.01`
- `price_method=close_volume_vwap_v1`
- `quoted_slippage_bps=10`
- `unquoted_slippage_bps=25`
- `commission_bps=0`
- `regulatory_fee_policy=zero_v1`
- `cash_reserve_markup_bps=100`
- `remainder_cancel=calendar_market_close`

模型哈希使用规范 JSON 计算；运行时必须重算并与持久化哈希一致。

## 4. 纯领域模型

`engine/src/nexus/paper/fills.py` 不访问网络或全局时钟，提供：

- `qualify_execution_window(model, calendar, bars, quote, evaluated_at)`；
- `calculate_fill(order, existing_fills, reservation, window, instrument)`；
- `build_buy_fill_posting(...)` / `build_sell_fill_posting(...)`；
- `advance_order_state(...)` 和终态取消决策。

输入是严格、冻结、`extra=forbid` 的 Pydantic 值对象，Decimal 拒绝 binary float，datetime 必须带时区。输出包含所有派生数据和规范哈希，持久化层不重算另一套业务规则。

## 5. 原子执行服务

`advance_internal_paper_order(session, order_id, evaluated_at, idempotency_key)` 是内部应用服务，不暴露为公共成交 API。

1. 用事务级 advisory lock 和 `SELECT ... FOR UPDATE` 锁定 account/order/reservation。
2. 验证账户能力、订单状态、执行日、日历权威和执行模型。
3. 根据冻结 source authority 选择下一个未处理窗口，仅读 PostgreSQL 中的精确 bar。
4. 若窗口不合格，在时限前返回 no-op；达 10:15/收盘则追加取消事件并释放全部余留。
5. 若合格，持久化窗口权威和成员，计算不超过窗口/订单/预留的成交。
6. 在一次 commit 中追加 fill、position adjustment、reservation event、ledger transaction/entries 和 order event。
7. 在返回前重投影账本、持仓、订单和预留；任一不平或超限都回滚整个事务。

买入 fill 记账：借记持仓成本和费用，贷记预留现金；终态后借记可用现金、贷记未用预留。

卖出 fill 按事前平均成本分配 cost basis：借记净现金和费用，贷记持仓成本，差额贷记或借记已实现损益。卖出持仓预留按成交量消耗，终态释放余量。

## 6. 持仓、绩效与归因

`engine/src/nexus/paper/performance.py` 从追加事实投影：

- 持仓：`quantity = sum(quantity_delta)`，`cost_basis = sum(cost_delta)`，卖出使用事前平均成本。
- 现金：双重记账中 available + reserved，不从 fill 重复计算。
- 日末市值：只用对应 session、已在 `as_of` 前可用、完整的 Task 7 `1Day` 收盘权威。任一持仓缺价时不发布新绩效快照。
- `NAV = cash + sum(quantity * qualified_close)`。
- `realized_pnl` 来自账本的已实现损益，`unrealized_pnl = market_value - cost_basis`，`fees` 来自费用账户。
- `peak_nav` 是已发布快照与当前 NAV 的最大值，`drawdown=(peak-nav)/peak`。
- 日 TWR 是相邻日末 NAV 收益，无外部入金时与简单收益一致；年化因子 252，无风险利率 0。MVP 不伪造不足样本的年化指标，样本不足返回 `null`。
- instrument 归因由 fill/持仓快照计算；sector 归因只用同 `as_of` 的冻结 sector authority 分配。缺 sector 权威时绩效总量可发布，sector 归因标记 `partial` 而不猜测。
- 复星真实账户损益始终在另一账户/组合域显示，不合并进内部模拟 NAV。

绩效快照内容哈希覆盖账本事务哈希、持仓调整哈希、价格权威哈希、执行模型和归因集。

## 7. API 合约

Engine 只暴露模拟盘读模型和用户取消，不暴露“创建 fill”：

- `GET /internal/v1/paper/portfolio`：现金、NAV、绩效、持仓及其价格权威/source/feed/as-of。
- `GET /internal/v1/paper/orders?cursor=&status=&limit=`：有界、签名 keyset cursor；订单附事件时间线和 fills，服务端给出 `cancellable`。
- `POST /internal/v1/paper/orders/{order_id}/cancel`：严格 JSON `{ "expected_event_seq": n }`，必须 `Idempotency-Key`；只能取消 `submitted` / `partially_filled` 的内部模拟订单。

API 不返回账户内部 ID、密钥、数据库字段名、provider 请求体或原始异常。任何 Decimal 以规范字符串返回，time 为 UTC aware datetime。错误使用 NEXUS 稳定 envelope：401/404/409/422/500。

Web BFF 映射为：

- `GET /api/v1/paper/portfolio`
- `GET /api/v1/paper/orders`
- `POST /api/v1/paper/orders/{orderId}/cancel`

BFF 沿用 HMAC 服务身份、严格路径/查询允许列表、固定响应上限、请求体上限、受保护头重建和响应 schema 清洗。

## 8. `/paper` 产品与页面设计

信息层级：

1. 页头明确“内部模拟、USD 10,000 初始资金、不是实盘”，同时显示数据状态和最新 as-of。
2. 概要带：NAV、可用现金、预留现金、已实现/未实现损益、费用和最大回撤；数字使用 tabular figures。
3. 持仓表：标的、数量、可用/预留、平均成本、市价、市值、未实现损益、source/feed/as-of。
4. 订单与成交：状态筛选、游标分页、订单详情及只追加 `OrderTimeline`。时间线直接渲染服务端事件，不在浏览器推断状态。
5. 只对 `cancellable=true` 显示取消按钮；点击后二次确认，带当前 `event_seq` 和稳定幂等键。取消成功后重新读取服务端事实。

页面实现 `loading / empty / ready / partial / stale / error / forbidden` 全状态；partial/stale/error 不会伪造 0 值。所有交互可键盘操作，焦点可见，状态通知用 `aria-live`，表格在窄屏可水平滚动且保留语义。

## 9. 测试与验收

### Engine

- 单元：窗口连续性、时区/DST/半日市、VWAP、价差、10/25 bps、tick 保守取整、1% 容量、预留上限、买卖记账、平均成本、终态。
- 属性：任意正值数量/价格/费用下双重记账平衡、现金/持仓/预留不为负、累计 fill 不超 order/window。
- PostgreSQL 集成：迁移往返、数据库守卫、并发 worker，同键重试，取消/fill 竞争，故障注入全回滚，缺失/未来/换源拒绝，买卖成交和部分成交到收盘。
- API：认证、严格 schema、有界游标、状态过滤、数据脱敏、取消版本/幂等/竞争。

### Web

- BFF 路由、路径/查询/请求体边界、HMAC 转发和合约清洗。
- 页面全状态、时间线顺序、数字格式、取消确认和冲突恢复。
- Playwright 真实浏览器覆盖 ready/partial/stale/error、取消与分页；axe 无 critical/serious 问题。

### 门禁

- `ruff check .`
- `mypy src`
- Engine 全量 `pytest`
- `npm run lint`
- `npm run typecheck`
- `npm test`
- `npm run check:generated`
- `npm run build`
- Task 19 Playwright + axe
- 独立代码审查与安全审查均无未解决 Critical/Important

## 10. 任务边界

- Task 19 实现领域模型、原子应用服务、投影、API 和页面，但不启动定时 worker。
- Task 20 负责调度在正确纽约交易时点调用该服务、绩效日结、监控、重试和对账。
- Task 21 负责最终安全、故障演练和上线验收；实盘仍默认关闭。
