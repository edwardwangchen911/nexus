# Task 17：三方案前向影子验证设计规格

- 日期：2026-08-26
- 状态：聊天设计已获用户确认，待用户审阅本书面规格
- 适用范围：Task 12 三方案提案组的静态前向验证
- 前置提交：`2db9c1b feat(engine): add deterministic baseline backtesting`
- 后续门禁：本规格经用户书面审阅批准后，才生成 Task 17 实施 Plan

## 1. 结论

Task 17 实现独立的 **Proposal Shadow（静态提案影子）**：用户对一个处于 `compared` 的提案组启动验证后，系统从同一个未来交易时点、以 NAV=100 同时创建 `hold_current`、`minimum_change`、`strategy_target` 三个归一化组合；每个组合只在首个会话按冻结权重执行一次确定性调仓，随后只随合格收盘价、现金、费用、公司行动和退市事件漂移。

Task 17 不把 `strategy_target` 每周重新计算，也不要求静态方案产生至少 20 笔成交。63 个固定 XNYS 会话、完整可追溯的数据覆盖和可解释的首日模拟执行，是静态 Proposal Shadow 的验证条件。

需要周频再决策、Agent overlay、至少 20 笔完成成交以及冠军/挑战者晋级的能力属于阶段 2 的 **Strategy Evolution Shadow（动态策略进化影子）**，必须使用独立实验语义和生命周期实现，不能偷偷混入 Task 17。

影子验证是研究证据，不是交易批准。`validated` 不创建模拟盘订单、券商订单、交易意图或实盘授权。

## 2. 本规格修正的原始矛盾

原平台总规格和旧实施 Plan 同时包含以下两组要求：

1. `strategy_target` 是最多 6 个入选标的的一次性冻结提案；
2. Task 17 必须在 63 日内完成至少 20 笔 `strategy_target` 模拟成交。

如果一次性方案只有首日调仓，成交笔数由持仓数量和权重变化决定，通常不可能稳定达到 20。为了凑笔数而只让 `strategy_target` 周频重算，又会使三条曲线不再比较同一种对象，并把静态组合比较和动态策略评估混为一体。

本规格因此替代以下旧要求：

- `docs/superpowers/specs/2026-08-22-nexus-ai-investment-platform-design.md` 中“63 日且至少 20 个模拟成交”对三方案提案影子的约束；
- `docs/superpowers/plans/2026-08-22-nexus-phase-0-1.md` Task 17 中“少于 20 笔不能 validated”的红测和实施条目。

其余安全边界、三方案同起点、5/21/63 检查点和禁止自动下单的要求继续有效。

### 2.1 两类 Shadow 的固定边界

| 维度 | Task 17 Proposal Shadow | 阶段 2 Strategy Evolution Shadow |
| --- | --- | --- |
| 被评估对象 | 一个已冻结提案组 | 一个可重复产生决策的策略/Agent overlay 版本 |
| 权重生成 | 启动前已冻结一次 | 按固定调度和版本化输入重复生成 |
| 调仓 | 首个未来会话一次 | 按策略频率执行 |
| 最小观察期 | 63 个固定 XNYS 会话 | 至少 63 个会话 |
| 最小成交数 | 不设人为笔数门槛 | 至少 20 个已完成模拟成交 |
| 结论 | 提案验证完成/过期 | 挑战者是否具备晋级资格 |
| Agent | 不参与会话循环 | 只能在受控 DSL/overlay 边界内参与 |
| 自动交易 | 永不创建订单 | 也不得自动启用实盘 |

阶段 2 可以复用 Task 17 提取的确定性执行内核，但不能复用 Proposal Shadow 根记录来伪装动态实验。

## 3. 范围与非目标

### 3.1 本次包含

- 从一个满足资格的 `compared` 提案组原子启动三条静态影子曲线。
- 从 Task 16 私有实现提取公开、纯函数、确定性的模拟执行内核，并由历史回测和 Shadow 共同使用。
- 固定 63 个 XNYS 观察会话以及 5 个会话的数据补齐宽限期。
- 追加式保存会话权威、NAV、完整账本状态、首日模拟结果、检查点和封存原因。
- 生成 5/21/63 会话的成本后收益、回撤、波动、最差会话、换手和费用指标。
- 用一个专用 GET API 和现有提案详情页展示三条同轴 NAV 曲线、精确数据表和数据质量状态。
- 自动完成 `shadowing -> validated|expired`，并让 `shadowing -> rejected` 同时封存 Shadow。
- 为数据库约束、并发、幂等、时点正确性、API、UI、响应式和无障碍增加测试。

### 3.2 本次不包含

- 不实现周频或每日重新选股、重新优化或 Agent 再决策。
- 不实现冠军/挑战者晋级、策略参数自修改、提示词进化或自动回滚；这些属于阶段 2。
- 不创建 Task 18 模拟盘账本、订单或成交事实；Shadow 模拟结果不是订单领域对象。
- 不调用复星交易接口，不访问真实账户，不新增或使用交易密钥。
- 不增加实盘、复星模拟盘或自动交易权限。
- 不回填启动事务之前的影子绩效，不把今天的提案伪装成过去已知。
- 不修改 `baseline_v1` 规则、Task 16 历史回测口径或历史回测结果 Schema。
- 不在浏览器中计算财务指标、推进会话或决定生命周期。
- 不在 Task 17 内实现 Task 20 的定时调度器；Task 17 只提供可由租约任务调用的应用服务。

## 4. 信任边界与模块结构

```text
OptimizationProposalGroup(compared) + 三个不可变 Proposal + BaselineSnapshot
  -> start_proposal_shadow()：锁、重新验证、固定未来会话、原子建组
  -> ShadowGroup + 三个 ShadowPortfolio(NAV anchor=100)
  -> outbox: shadow.started

Task 20 租约任务（浏览器不可调用）
  -> 加载下一个固定 XNYS 会话的完整 PIT 数据权威
  -> advance_shadow_session()
  -> 共享 nexus.simulation 纯执行内核
  -> 追加 SessionAuthority / PortfolioState / Fill / Outcome / Checkpoint
  -> 第 63 会话或宽限期结束时原子 validated|expired + seal + audit/outbox

GET shadow
  -> 只读投影追加式事实
  -> BFF 校验/脱敏
  -> 提案详情页 ShadowPerformance
```

模块职责固定为：

| 模块 | 职责 | 禁止依赖 |
| --- | --- | --- |
| `nexus.simulation` | Decimal 账本、目标权重执行、公司行动、退市、估值；纯函数 | SQLAlchemy、HTTP、LLM、系统时钟 |
| `nexus.backtest` | 历史 PIT 数据准备、策略循环、回测结果适配 | Shadow 持久化/API |
| `nexus.shadow` | 静态三方案会话推进、指标、生命周期应用服务 | Agent、paper、broker、live gateway |
| `nexus.persistence.models.shadow` | Task 17 ORM 映射 | 业务计算 |
| `nexus.api.routes.optimization` | 启动/读取/拒绝 HTTP 适配 | 会话推进和指标重算 |
| Web BFF | 同源身份、HMAC、契约校验、错误脱敏 | 财务计算、状态转换 |
| `ShadowPerformance` | 展示后端结果和精确表格 | 指标重算、交易动作 |

### 4.1 共享执行内核

Task 16 当前私有 `_Ledger`、`_rebalance`、公司行动、退市和估值逻辑迁入 `nexus.simulation`，对外暴露领域无关的固定接口：

```python
def execute_target_weights(
    state: SimulationState,
    command: TargetWeightCommand,
) -> ExecutionResult: ...

def apply_session_events(
    state: SimulationState,
    events: SessionEvents,
) -> EventApplicationResult: ...

def mark_to_market(
    state: SimulationState,
    marks: Mapping[UUID, Decimal],
) -> ValuationResult: ...
```

要求：

- 输入和输出均不包含数据库 Session、全局变量、网络客户端或当前时间。
- 输入拒绝 binary float、非有限 Decimal、负价格、重复标的和权重和大于 1。
- 内部始终按 UUID 数值顺序处理标的和事件，结果量化到 `NUMERIC(38,12)` 的现有 `RESULT_QUANTUM`。
- `execute_target_weights` 返回新的不可变 `SimulationState`，不原地修改调用方状态。
- 成本后目标 NAV 求解、下一会话 open 成交、`missing_bar`、`untradable` 和成交费用语义与 Task 16 完全相同。
- 历史回测通过适配器把共享 `SimulationFill/SimulationOutcome` 映射为现有 `BacktestFill/BacktestOrderOutcome`；Task 16 的结果哈希和固定 fixture 输出不得改变。
- Shadow 不复制另一套成交公式。

## 5. 时间与前向语义

### 5.1 启动时点

`started_at` 使用 PostgreSQL 事务时间，UTC 保存。`start_session` 是冻结 XNYS 日历中，严格晚于 `started_at` 所在纽约市场日期的第一个会话；启动事务至少预留一个市场日期边界，不使用同日盘前捷径。因此：

- 任意交易日启动都使用下一个 XNYS 会话；
- 休市日或周末启动使用其后的第一个 XNYS 会话；
- 永不把已开盘的会话作为模拟成交会话；
- 永不为 `started_at` 之前的会话生成 Shadow NAV。

`signal_session` 是提案 `valuation_time` 对应的最后一个已完整会话。若提案的数据、诊断、来源或成本权威在 `started_at` 已按现有策略过期，启动失败，用户必须创建新提案，不能用旧信号伪装实时决策。

### 5.2 固定观察窗口

- `evaluation_sessions` 是从 `start_session` 起连续 63 个 XNYS 会话，启动时一次性冻结。
- `evaluation_end_session` 是其中第 63 个会话。
- `grace_sessions` 是 `evaluation_end_session` 之后连续 5 个 XNYS 会话。
- `grace_end_session` 是第 5 个宽限会话。
- 宽限期只能补齐原 63 个观察会话的数据，不能增加新的绩效观察会话。

三个组合在 `start_session` 开盘前的锚点 NAV 均为精确 Decimal `100.000000000000`。第 1 个 NAV 点是首日执行、事件处理和收盘估值后的结果。检查点指标使用 `[100, session_1_nav, ..., session_n_nav]`，因此第 n 个检查点恰好包含 n 个会话收益。

### 5.3 目标权重

- `hold_current` 使用冻结提案位置的 `current_weight`。
- `minimum_change` 和 `strategy_target` 使用各自冻结位置的 `target_weight`。
- 目标现金权重是 `1 - sum(instrument_weights)`。
- 权重全部来自不可变提案行；Shadow 不能接受请求体权重、Agent 权重或页面编辑值。
- 每个 ShadowPortfolio 从 NAV=100 的全现金状态开始，以便三条曲线在同一尺度比较，而不是复制真实账户币值或份额。

### 5.4 会话推进与缺口

内部应用服务固定为：

```python
advance_shadow_session(
    group_id: UUID,
    session: date,
    observed_at: datetime,
) -> ShadowAdvanceResult
```

它只接受当前应处理的下一个固定会话：

- `session` 等于已成功处理会话时，返回原结果且不新增任何事实；
- `session` 早于当前进度但没有对应权威时，返回稳定冲突；
- `session` 晚于下一个应处理会话时，以 `SHADOW_SESSION_GAP` 拒绝；
- 数据缺失、来源冲突、不可接受的修订或陈旧数据只追加失败 attempt，不生成 NAV，也不把上一日价格冒充当日价格；
- 后续会话不能越过缺口。数据恢复后，Task 20 按原顺序逐个补齐已过去的固定会话；每次补齐仍使用一个独立事务。

第 63 个固定会话过去但覆盖不全时进入宽限状态。到 `grace_end_session` 仍不完整则过期。宽限期内不允许为第 64 个及以后的会话生成 NAV。

“Provider 查询未完成”和“完整查询证明某个首日执行 Bar 不存在”必须区分：前者是 failed attempt，不能推进；后者可以产生不可改写的 `missing_bar` 首日 outcome，其余可执行标的继续处理，未买入名义金额保留现金。该负面证据必须进入 session hash，后续即使出现迟到修订也不能倒填首日成交。首日 `missing_bar|untradable` 不制造虚假持仓或 NAV，但会在第 63 会话阻止 validation。

## 6. 会话数据权威

每个成功会话必须有一个不可变 `ShadowSessionAuthority`，把以下内容绑定到同一会话：

- proposal group、Shadow group、会话日期和 1..63 ordinal；
- 冻结 XNYS calendar authority ID、版本和内容哈希；
- 提案组的 `source_policy_id` 与 `cost_policy_id`；
- 每个需要执行或估值标的的选定 raw 日 Bar authority、source、delivery mode、revision、`available_at` 和内容哈希；
- 当日生效或结算的公司行动、退市和现金恢复 authority；
- 本次权威集合的 `content_hash` 与最大 `available_at`。

Authority 还保存应用该会话时的 `observed_at`、`late_backfill` 和完整来源查询摘要。`available_at` 晚于该次 `observed_at` 的记录绝不能进入本次权威；宽限期补齐使用新的实际 `observed_at` 并如实标记 late backfill，不篡改原始 available time。公司行动仍必须在其生效会话前已可用，不能借宽限期引入事后才公布的交易决策信息。

首个会话中，每个实际成交标的要求同一条合格 raw Bar 提供 open 和 close；完整负面查询证明 Bar 不存在的正目标标的按第 5.4 节生成 `missing_bar`，保留现金且不要求虚构 close。后续会话要求所有仍持有且未被确定性退市事件终止标的的 raw close。不得用 all-adjusted close 执行或估值，不得跨来源字段级拼接。

公司行动处理顺序与 Task 16 相同：split → dividend entitlement/settlement → resolved delisting → initial execution（仅首日）→ close mark。已知的现金并购恢复或零恢复破产可以确定性结算；`unresolved` delisting、缺失 close 或不完整事件集合不能生成该会话的成功权威。

### 6.1 规范化摘要

`shadow_session_sha256_v1` 使用规范化 JSON UTF-8 的 SHA-256。顶层固定包含：

```json
{
  "schema": "shadow_session_sha256_v1",
  "shadow_group_id": "UUID",
  "session": "YYYY-MM-DD",
  "ordinal": 1,
  "calendar": {"authority_id": "UUID", "version": "...", "content_hash": "..."},
  "policies": {"source_policy_id": "UUID", "cost_policy_id": "UUID"},
  "bars": [],
  "missing_execution_inputs": [],
  "corporate_actions": [],
  "delistings": []
}
```

数组按 `instrument_id/source/provider_event_id/revision/authority_id` 的适用组合稳定排序；Decimal 使用固定 12 位字符串，UTC 时间使用固定 6 位微秒 `Z` 字符串。随机记录 ID 和 `created_at` 不进入摘要。版本升级必须新增 v2，不能静默改变 v1。

### 6.2 来源与延迟展示

Task 17 服从提案绑定的版本化来源政策，不自行声称“实时”。GET API 返回本组实际 source、delivery mode、最近成功会话、数据最大 `available_at` 和最近失败 attempt。页面必须明确显示 delayed/fixture 等实际交付模式；未来复星成为合格美股日线权威后，必须通过新的来源政策版本接入，不能修改既有 Shadow 权威。

## 7. 数据模型

迁移版本固定为 `0013_shadow.py`。所有金额、权重、数量、价格、NAV 和指标使用 `NUMERIC(38,12)`；所有时间使用 UTC timestamptz；业务 ID 使用 UUIDv7。除现有 proposal group 生命周期字段和 outbox 发布时点外，Task 17 自有事实均为 append-only。

### 7.1 根与组合

#### `shadow_groups`

| 字段 | 约束与语义 |
| --- | --- |
| `id` | PK，UUIDv7 |
| `proposal_group_id` | FK，唯一；一个提案组最多一个 Shadow |
| `baseline_portfolio_snapshot_id` | FK；必须等于提案组 baseline |
| `proposal_bundle_hash` | 64 位 SHA-256；覆盖三个提案及全部冻结权重 |
| `source_policy_id` / `cost_policy_id` | FK；必须等于提案组绑定 |
| `calendar_authority_id` / `calendar_version` / `calendar_hash` | 冻结 68 个会话所需日历权威 |
| `signal_session` | 提案 valuation 对应最后完整会话 |
| `started_at` | 数据库事务 UTC 时间 |
| `start_session` | 第 1 个固定观察会话 |
| `evaluation_end_session` | 第 63 个固定观察会话 |
| `grace_end_session` | 第 5 个宽限会话 |
| `evaluation_sessions` / `grace_sessions` | 有序、唯一的日期数组，长度分别为 63/5 |
| `initial_nav` | 固定 `100.000000000000` |
| `schema_version` | 固定 `proposal_shadow_v1` |
| `created_at` | 数据库生成，不可修改 |

该表不保存 `validated/rejected/expired` 副本；`optimization_proposal_groups.status` 是唯一业务生命周期事实。

#### `shadow_portfolios`

| 字段 | 约束与语义 |
| --- | --- |
| `id` | PK，UUIDv7 |
| `shadow_group_id` | FK |
| `proposal_id` | FK，唯一；必须属于同一 proposal group |
| `variant` | `hold_current|minimum_change|strategy_target` |
| `target_weights_hash` | 覆盖该组合 instrument/weight/cash 的 SHA-256 |
| `initial_nav` / `initial_cash` | 均固定为 100 |
| `created_at` | 不可修改 |

唯一约束为 `(shadow_group_id, variant)`。一个 DEFERRABLE INITIALLY DEFERRED 约束触发器在提交时验证一个 ShadowGroup 恰有三个组合、顺序语义固定、proposal ID/variant 匹配、同一 root authority 且无额外组合。

### 7.2 推进、权威和账本状态

#### `shadow_advance_attempts`

追加首次成功推进和每次失败推进尝试：`shadow_group_id`、请求 session、当时 expected session、`observed_at`、结果 `applied|data_partial|data_stale|source_conflict|session_gap`、稳定 reason code、可选 authority hash、`created_at`。重复读取已成功 session 或 seal 后的调用直接返回已有结果，不追加 attempt。失败记录不得包含原始 Provider 响应或凭据。

#### `shadow_session_authorities`

每个成功会话一行：`shadow_group_id`、session、ordinal、calendar/source/cost authority、规范化 `content_hash`、`available_at`、实际 `observed_at`、`late_backfill`、完整来源查询 hash、source summary 和 delivery mode summary。唯一约束 `(shadow_group_id, session)` 与 `(shadow_group_id, ordinal)`；ordinal 只能为 1..63。

#### `shadow_session_price_inputs`

把 session authority 与实际使用的 `price_bar_record_id`、`price_bar_set_authority_id`、instrument、用途 `execution_and_mark|mark`、source、delivery mode、revision 和 content hash 显式绑定。唯一约束 `(session_authority_id, instrument_id)`；首日 `execution_and_mark` 必须覆盖 open/close，后续 `mark` 只消费 close。被确定性退市结算的标的不再创建后续 price input。

#### `shadow_session_event_inputs`

把 session authority 与实际应用的 `corporate_action_id`、事件种类 `split|dividend|delisting`、provider event identity、revision、available_at 和 evidence hash 显式绑定。无事件会话允许零行；有事件时 exact-set hash 必须覆盖全部成员。共享内核只能消费这些成员，不能另查一组未绑定事件。

#### `shadow_portfolio_states`

每个组合每个成功会话一行：`shadow_portfolio_id`、`session_authority_id`、session、ordinal、NAV、cash、累计成交名义金额、累计费用、累计已实现损益、state hash。唯一约束 `(shadow_portfolio_id, session)`。同一 session authority 必须恰好对应三个 portfolio states。

#### `shadow_instrument_states`

每个 portfolio state 对所有已触达标的保存：instrument ID、quantity、cost basis、realized PnL、cumulative cost、last close、untradable。即使已清仓但存在 realized/cost 历史也保留一行，使当前状态可以只从最新快照重建。

#### `shadow_receivable_states`

保存尚未结算的 dividend receivable：portfolio state、corporate action authority ID、instrument ID、amount、payable session。它与 instrument state 一起覆盖完整模拟账本，不允许把应收股息藏在不可复算 JSON 中。

### 7.3 首日执行事实

#### `shadow_simulated_outcomes`

每个首日目标意图保存 portfolio、instrument、signal session、target session、side、`filled|missing_bar|untradable`、稳定 reason。只允许 `ordinal=1`；后续会话不得产生再平衡 outcome。

#### `shadow_simulated_fills`

只为 `filled` outcome 保存 side、quantity、price、notional、cost 和 execution price kind=`open`。fill 必须唯一引用一个 outcome；不能引用 Task 18 paper order/fill 表。

全现金方案没有正目标权重时，零 outcome 也属于“首日执行完整”。其他方案的每个正目标权重必须有且只有一个 `filled` outcome 才具备验证资格；`missing_bar` 或 `untradable` 是终止的模拟意图，但会阻止最终验证，不能在未来日期倒填成交。

### 7.4 检查点与封存

#### `shadow_checkpoints`

每个 Shadow group 的 ordinal 只能为 5、21、63，唯一约束 `(shadow_group_id, ordinal)`。字段包含 session、`complete|incomplete`、coverage count、reason、metrics hash 和 created_at。成功推进到阈值时创建 complete；到过期时仍无法形成的应有检查点创建 incomplete，指标为 unknown。

#### `shadow_checkpoint_metrics`

每个 checkpoint 恰有三个 variant 行，保存第 9 节定义的精确指标、unknown reason 和 worst sessions JSON。DEFERRABLE 触发器验证 variant 完整且全部引用对应 ShadowPortfolio。

#### `shadow_seals`

一个 Shadow group 最多一行：`reason=validated|rejected|expired`、稳定 `reason_code`、`sealed_at`、最终 coverage、final authority hash。它是“此后不得追加 Shadow 事实”的 append-only 封条，不是 proposal status 的副本。数据库 guard 在存在 seal 后拒绝新的 authority、input、state、fill、outcome、checkpoint 和 attempt。DEFERRABLE commit-time trigger 要求 seal reason 与最终 proposal status 完全一致，单独插入 seal 而不完成 lifecycle CAS 的事务不能提交。

所有 Task 17 表的 UPDATE/DELETE 由数据库触发器拒绝。当前页面投影必须能从上述事实重建；本次不新增需要原地更新的 Shadow projection 表。

## 8. 原子事务与并发

### 8.1 启动事务

`start_proposal_shadow()` 按以下顺序在一个数据库事务内执行：

1. 使用现有 idempotency claim 锁定 `actor + route + key`；同 key 不同请求哈希立即冲突。
2. `SELECT ... FOR UPDATE` 锁 proposal group。
3. 验证 If-Match/version、状态严格为 `compared`，并重新加载三个提案、位置、比较、baseline snapshot、diagnostic、data/source/optimizer/cost/selection authority。
4. 以 `started_at` 重新执行 stale、partial、source conflict、FX、cost complete、strategy evidence 和 executable gate；不能只相信旧布尔值或前端资格。
5. 冻结日历、63+5 会话、proposal bundle hash 和三个 target weight hash。
6. 插入一个 ShadowGroup 和恰好三个 NAV=100 ShadowPortfolio。
7. CAS 更新 proposal group 为 `shadowing`、version + 1。
8. 写 `shadow.started` audit event、outbox event 和 idempotency response payload。
9. flush 并让 deferred constraints 检查完整性后提交。

任一步失败全部回滚；绝不能留下半个 Shadow、部分 variant 或只更新生命周期。

同一 key/同一 body 重放返回同一个 `shadow_group_id`、status、version 和会话边界，不创建新记录。不同 key 的并发启动由 proposal row lock 和 `shadow_groups.proposal_group_id` 唯一约束收敛为一个成功、一个稳定 version/state conflict。

### 8.2 推进事务

每次 `advance_shadow_session()`：

1. 先锁 proposal group，再锁 ShadowGroup，所有 start/reject/advance/finalize 使用同一锁顺序。
2. 若已 seal，返回 sealed/replay，不追加会话事实。
3. 验证请求 session 与固定序列的 expected session。
4. 加载并规范化完整会话权威；失败时只写 attempt 并提交。
5. 对三个 portfolio 从上一 append-only state 执行同一 session input；首日调用一次目标权重内核，之后禁止再平衡。
6. 原子写一份 session authority、三份完整 state 及其 instrument/receivable 子行、首日 outcome/fill、成功 attempt。
7. ordinal 为 5/21/63 时在同一事务写 checkpoint。
8. ordinal=63 且满足最终 gate 时，同事务先写最终 seal，再 CAS `shadowing -> validated` 并写 audit/outbox；存在永久 blocker 时以同样顺序转 `expired`。任一步失败整笔回滚。

重复成功 session 只返回已保存结果。两个 worker 同时推进时，一个持锁写入，另一个读取唯一会话事实后 replay；不能重复 NAV、fill 或 checkpoint。

### 8.3 拒绝与最终化竞争

`reject_proposal_group()` 保留现有 `draft|compared|shadowing -> rejected` 语义。若当前状态为 `shadowing`，它必须在同一事务先创建 `shadow_seals(reason=rejected)`，再更新 proposal status 并创建 audit/outbox。

由于 reject、advance 和 automatic finalize 先锁同一 proposal row：

- reject 先提交时，后续推进看见 rejected/sealed，不再写数据；
- 第 63 会话 finalize 先提交时，reject 看见 validated 后按现有终态规则失败；
- 不存在“proposal 已 rejected 但 Shadow 仍继续”或“双终态”。

## 9. 执行、估值与指标口径

### 9.1 首日执行

- 以首日 raw open 作为模拟成交价。
- 使用提案绑定的 `cost_policy_id`。`cost_policy_v1` 适配为 commission/slippage 的精确 Decimal；该版本未声明 spread 时，版本适配器显式使用 spread=0，不能从 UI 或环境变量补值。
- 目标 NAV 使用 Task 16 的成本后固定点求解。
- 允许 fractional quantity，不做整股取整。
- 现金收益固定 0%，与阶段 0–1 口径一致。
- `tax_treatment=unknown` 不估算税，不把 unknown 当作 0 税成本结论。

### 9.2 后续会话

- 不重算权重，不调用 optimizer、baseline strategy 或 Agent。
- 仅按共享内核处理公司行动、退市、应收股息和收盘估值。
- 缺失当日合格 close 时不 carry forward、不生成 NAV。
- 已被完整退市 authority 确定性结算的标的不再要求后续 close。

### 9.3 检查点指标

所有值用 Decimal 计算并固定到 12 位，前端只格式化显示：

| 指标 | 精确定义 |
| --- | --- |
| `net_cumulative_return` | `checkpoint_nav / 100 - 1` |
| `return_delta_vs_hold` | `variant_return - hold_current_return`；hold 自身为 0 |
| `max_drawdown` | 从 `[100, nav...]` 逐点计算的最小 `nav/running_peak - 1`，范围 `[-1,0]` |
| `drawdown_improvement` | `abs(hold_max_drawdown) - abs(variant_max_drawdown)`；正数表示改善 |
| `annualized_volatility` | 日收益样本标准差（n-1）乘 `sqrt(252)`；少于 2 个日收益为 0 |
| `volatility_improvement` | `hold_volatility - variant_volatility`；正数表示改善 |
| `worst_sessions` | 日收益按 `(return asc, session asc)` 排序后最多 5 项，首日收益相对 NAV=100 |
| `cumulative_turnover` | 从启动至检查点的 `sum(abs(traded_notional)) / 100`，不年化 |
| `fee_drag_index_points` | 累计费用；因初始 NAV=100，其单位是 NAV 指数点，不是 USD/HKD |

5 日和 21 日检查点只标记 `observation`。63 日 complete checkpoint 可标记 `validation_complete`，但 API 和 UI 都不生成“冠军”“推荐交易”“预计收益”或综合投资分数。

指标无法形成时返回 `status=unknown, value=null, reason=<稳定 reason>`，不能回填 0。已知值返回 `status=known, value=<Decimal string>, reason=null`。

## 10. 生命周期与最终门禁

`OptimizationProposalGroup.status` 继续是唯一业务生命周期：

```text
compared --start atomically--> shadowing
shadowing --63 sessions + complete gate--> validated
shadowing --permanent blocker or grace exhausted--> expired
draft|compared|shadowing --user reject--> rejected
```

`validated` 必须同时满足：

1. 成功会话恰好覆盖启动时冻结的 63 个 session，无额外 session；
2. 每个成功 session 有一份完整 authority、三个 state 和可重算 state hash；
3. 5/21/63 三个 complete checkpoint 及每个 checkpoint 的三个 variant metrics 完整；
4. 三个 portfolio 的首日正目标权重全部获得 `filled` outcome，或该组合为全现金而无需 intent；
5. 不存在 `missing_bar`、`untradable`、unresolved delisting、source conflict、stale 或 hash mismatch；
6. Shadow 根 authority、proposal bundle 和 policy 仍与不可变上游事实一致；
7. proposal group 仍为 `shadowing` 且未 seal。

第 63 会话覆盖完整但首日执行有永久 blocker 时，立即 `expired`，reason=`SHADOW_INITIAL_EXECUTION_INCOMPLETE`。只有原 63 会话的数据覆盖缺口可以使用 5-session grace；宽限结束仍不完整时 `expired`，reason=`SHADOW_DATA_INCOMPLETE`。

不存在公开或内部的“手工 validate” HTTP 路由。测试通过应用服务注入冻结时钟和 fixture authority，不绕过最终门禁。

### 10.1 数据库级 lifecycle guard

`0013` 必须替换 `0009` 的 `nexus_optimization_group_lifecycle()`，保留原有 authority 检查并增加：

- `compared -> shadowing` 只有在同一事务已经存在一个完整 ShadowGroup 和恰好三个 ShadowPortfolio 时才允许；
- `shadowing -> validated` 只有在已存在 `shadow_seals(reason=validated)`，且数据库重新计数 63 authorities、189 portfolio states、三个完整 checkpoints、九个 checkpoint metrics 和完整首日 outcomes 时才允许；
- `shadowing -> expired` 必须已有匹配 `expired` seal；
- `shadowing -> rejected` 必须已有匹配 `rejected` seal；
- 任何 seal reason 与目标 proposal status 不一致都拒绝；
- 其他旧状态转换规则保持不变。

应用层不能通过直接调用现有泛型 `transition_group(..., "validated")` 绕过 Shadow。Task 17 将自动最终化收敛为专用 repository 函数；数据库 trigger 是最后一道不可绕过门禁。`0013` downgrade 必须恢复 `0009` 的原函数定义，而不是留下引用已删除 Shadow 表的函数。

## 11. API 契约

### 11.1 启动

保留：

```http
POST /internal/v1/optimization-proposals/{group_id}/shadow
Idempotency-Key: <1..255 ASCII>
If-Match: "<version>"
Content-Type: application/json

{"version": <same integer>}
```

成功响应固定为 200：

```json
{
  "data": {
    "proposal_group_id": "UUID",
    "shadow_group_id": "UUID",
    "status": "shadowing",
    "version": 2,
    "start_session": "YYYY-MM-DD",
    "evaluation_end_session": "YYYY-MM-DD",
    "grace_end_session": "YYYY-MM-DD"
  },
  "meta": {"request_id": "UUIDv7", "schema_version": "1", "as_of": "UTC"}
}
```

它替换现有只改 lifecycle 的泛型实现；响应不含 order ID。相同 idempotency key/body 重放的 HTTP body 必须相同，`meta.request_id` 除外。

### 11.2 读取

新增：

```http
GET /internal/v1/optimization-proposals/{group_id}/shadow
```

响应至少包含：

- proposal/shadow group IDs、proposal status/version；
- start/evaluation/grace 会话；
- `coverage={completed, due, required:63, next_expected_session, missing_sessions}`；
- `freshness={status:current|stale|unknown, latest_session, available_at, last_attempt_at, reason}`；
- 实际 source/delivery mode 摘要；
- 固定顺序的三个 portfolio，每个最多 63 个 `{session, nav}` 点；
- 首日 execution outcome/fill 摘要；
- 5/21/63 checkpoint 及固定顺序的三个 variant metrics；
- `sealed` 和 seal reason；
- 明示 `creates_orders=false`、`trading_approval=false`。

`coverage.due` 是冻结 calendar 中 `decision_at <=` 当前数据库时间的 evaluation session 数，上限 63；`missing_sessions` 是已 due 但尚无成功 authority 的固定会话。`freshness=current` 仅在 `completed == due` 时成立，`completed < due` 为 stale，尚无 due session 时为 unknown。页面显示用于判定的数据库时间和最近 attempt 时间，不从浏览器时钟推断新鲜度。

数据库中尚无 Shadow 时返回 404 `NOT_FOUND`，details reason=`SHADOW_NOT_STARTED`。根或子事实不完整、hash 不匹配时返回 409 `DATA_PARTIAL`，不返回看似正常的空曲线。

本次不向 group GET 嵌入全部 Shadow 数据。提案详情页并行请求 group GET 与 shadow GET；shadow 404 表示尚未启动，而不是页面错误。

### 11.3 内部推进

不增加浏览器可调用的 `/advance`、`/checkpoint` 或 `/validate` HTTP 路由。Task 20 在引擎进程中调用 `advance_shadow_session()`；工作任务只持最小数据库和只读数据权限，不持 broker/live gateway 权限。

### 11.4 稳定错误

| 场景 | HTTP / code | details.reason |
| --- | --- | --- |
| group/shadow 不存在 | 404 `NOT_FOUND` | `PROPOSAL_GROUP_NOT_FOUND` / `SHADOW_NOT_STARTED` |
| If-Match/body version 不一致 | 422 `VALIDATION_ERROR` | `IF_MATCH_INVALID` |
| 数据库版本已变化 | 409 `VERSION_CONFLICT` | `PROPOSAL_VERSION_CHANGED` |
| key 被不同 body 使用 | 409 `IDEMPOTENCY_CONFLICT` | `IDEMPOTENCY_BODY_MISMATCH` |
| 不满足启动 authority | 409 `PROPOSAL_NOT_EXECUTABLE` | 现有精确 eligibility reason |
| 非法 lifecycle transition | 422 `VALIDATION_ERROR` | `PROPOSAL_STATE_INVALID` |
| Shadow 事实不完整/冲突 | 409 `DATA_PARTIAL` / `SOURCE_CONFLICT` | 精确 Shadow reason |

BFF 继续只返回稳定安全消息，details 不透传 Provider 内容、表名、账号或凭据。

## 12. 页面设计

`ShadowPerformance` 放在现有提案详情页 `ProposalComparison` 下方，仅当 shadow GET 成功时显示；不新增顶级导航、策略晋级页或交易按钮。

### 12.1 信息结构

1. 顶部摘要：状态、start/latest session、完成会话数、当前 checkpoint、数据 `available_at`。
2. 常驻声明：“验证完成不代表交易批准，也不会创建订单”。
3. 三条 NAV=100 同轴折线。
4. 5/21/63 checkpoint 指标表。
5. 数据覆盖、来源、交付模式、最近失败原因。
6. 首日模拟执行结果表。

### 12.2 图表

- 复用现有 `lightweight-charts`，不新增图表依赖。
- `hold_current` 使用实线，`minimum_change` 使用虚线，`strategy_target` 使用点线；图例同时显示中文名称和线型，不能只依赖颜色。
- 添加 NAV=100 参考线和曲线末端直接标签。
- 同一纵轴只展示归一化 NAV，不混入 USD、HKD 或真实账户余额。
- 提供上一日/下一日/首日/末日键盘控制和可读的当前日详情。
- canvas 设为视觉补充；紧邻的语义 `<table>` 必须包含全部 session 和三个精确 Decimal 值，使屏幕阅读器和测试不依赖 canvas。
- 窄屏可以减少坐标轴 tick 和压缩图例布局，但不能删除数据表或隐藏某一 variant。
- `prefers-reduced-motion` 下关闭非必要过渡。

### 12.3 状态与动作

- loading：保留标题和固定高度 skeleton，避免页面跳动。
- 尚未启动：不渲染空图；现有 ProposalComparison 继续显示启动入口。
- partial/stale：保留最近成功曲线，显著显示时间和原因，不绘制虚构点；禁止启动、验证、晋级等增加风险的动作。
- reject 是现有风险降低生命周期动作；当 proposal group 仍为 `shadowing` 且读取到最新 version 时继续可用，并按第 8.3 节原子封存。
- 失败：显示可重试读取按钮和 request ID，不显示空坐标轴。
- sealed：显示 validated/rejected/expired 与 reason；validated 文案只写“63 会话验证完成”，不写“建议买入”。

## 13. 可重放与审计

必须可从数据库事实证明：

- 谁、何时、基于哪个 version 启动或拒绝；
- 三个组合绑定了哪些 proposal、权重、来源、成本和日历；
- 每个 session 当时使用了哪些 Bar 和事件修订、何时可用；
- 账本状态如何从上一 session 和纯内核得到；
- checkpoint 指标如何从 NAV 序列复算；
- 为什么 validated、rejected 或 expired；
- 整个过程中没有创建 Task 18 或 broker 领域对象。

审计事件固定至少包括：`shadow.started`、`shadow.session_applied`、`shadow.session_failed`、`shadow.checkpoint_created`、`shadow.validated`、`shadow.expired`、`shadow.rejected`。高频 session event 的 payload 只保存 ID、hash、session、ordinal 和 reason，不复制完整行情。

## 14. 测试与验收

### 14.1 共享内核单元测试

- 在同一固定数据上比较抽取前 Task 16 黄金结果与抽取后历史回测结果：NAV、fill、outcome、turnover、cost、attribution 和 result hash 必须逐值相同。
- Decimal、成本后目标 NAV、fractional quantity、missing bar、untradable、split、dividend、cash recovery、zero recovery 和 unresolved delisting。
- Shadow 首日只调仓一次，后续 session 即使权重偏离也不得产生 fill。
- 5/21/63 指标公式、tie-break、unknown 和 NAV index-point fee 单位。

### 14.2 属性测试

- 一个 Shadow 恰有三个固定 variant、同一 start session 和 authority。
- 任意合法会话序列的 NAV/state hash 可从初始状态和 append-only inputs 重建。
- 同一成功 session 重放不增加 row、NAV、fill、checkpoint 或 version。
- `available_at > 本次 observed_at` 的数据不能进入 session authority；宽限补齐必须保留真实 available/observed time 和 late 标记。
- 缺失 session 不 carry forward，后续 session 不能越过缺口。
- 任何 validation 路径都不创建 paper/broker order、fill、ledger 或 execution intent。

### 14.3 PostgreSQL 集成测试

- `0013` upgrade/downgrade、约束名、FK、唯一键、append-only trigger 和 deferred exact-three guard。
- 启动事务任一步失败全部回滚。
- 相同/不同 idempotency key、并发 start、start/reject race、advance/reject race、双 worker 同 session。
- session 顺序、失败 attempt、宽限期只补原窗口、63 成功 auto validate、5-session 后 auto expire。
- rejected/sealed 后数据库直接插入也被拒绝。
- 根/authority/state/checkpoint hash 篡改或上游绑定漂移 fail closed。

### 14.4 API 与 BFF 契约测试

- POST body/If-Match/idempotency、稳定错误、同 key 重放相同 shadow ID。
- GET 固定 variant 顺序、63 点上限、精确 Decimal string、unknown reason、source latency 和 seal reason。
- OpenAPI 生成、TypeScript runtime contract 和 BFF UUID 语义比较同步更新。
- GET 404 与数据损坏 409 明确区分。
- API 成功响应没有 order/trade/approval 字段；`creates_orders` 和 `trading_approval` 恒为 false。

### 14.5 前端与端到端

- 实线/虚线/点线、图例、直接标签、NAV=100 参考线和全量数据表。
- 指标正负语义、费用单位、unknown reason、观察/验证文案。
- 键盘逐日导航、焦点可见、表格 caption/header、aria live 状态。
- loading/not-started/active/grace/partial/stale/error/validated/rejected/expired 全状态。
- 375、390、768、1024、1440 px 和 mobile landscape 截图/交互。
- `@axe-core/playwright` serious/high 为 0，reduced motion 生效。
- 页面不存在买入、卖出、模拟下单、实盘批准或自动进化控件。

### 14.6 回归门禁

完成实施后必须运行并记录：

```bash
uv run --package nexus-engine pytest engine/tests/unit/simulation engine/tests/unit/backtest engine/tests/unit/shadow engine/tests/property engine/tests/integration -q
uv run --package nexus-engine ruff check engine
uv run --package nexus-engine mypy engine/src
npm test
npm run typecheck
npm run lint
npm run check:generated
npm run build
npm run test:e2e
```

若全量 PostgreSQL 集成测试需要本地容器，只要求 Docker-compatible runtime 和 Compose；不要求必须安装 Docker Desktop。最终还需独立代码审查，重点检查前视偏差、会话缺口、幂等、并发封存、单位、订单边界和数据库不可变性。

## 15. 实施切分边界

本规格足以生成一个直接编码的 Task 17 Plan，但实现应按以下依赖顺序拆成可独立验证的切片：

1. 共享 simulation 纯内核抽取并证明 Task 16 结果零漂移；
2. Shadow 领域模型、指标和会话状态机；
3. `0013` append-only persistence 与事务服务；
4. start/reject/advance 并发和自动最终化；
5. GET/POST API、OpenAPI、BFF 契约；
6. `ShadowPerformance`、页面状态、响应式和无障碍；
7. 全量回归与独立审查。

任何切片失败都不得通过放宽数据、权威、风险或人工批准边界来绕过。Task 17 完成也只表示静态提案影子可验证，不表示阶段 2 自进化、Task 18 模拟交易或完整 NEXUS MVP 已完成。
