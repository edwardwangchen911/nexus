# Bark Actionable Notifications Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace noisy fixed Bark event notices with one detailed, encrypted notification per meaningful internal-paper order outcome.

**Architecture:** Paper producers freeze a closed `paper_notification_v1` snapshot in the existing outbox transaction. The Bark consumer validates that snapshot, renders deterministic Chinese copy, and uses the order UUID as the Bark notification ID. The first deliverable consumes only risk rejection and terminal fill/cancel outcomes; delayed pending and partial-fill alerts remain the separate second phase from the spec.

**Tech Stack:** Python 3.12, Pydantic v2, SQLAlchemy async, PostgreSQL outbox, httpx, Bark V2, pytest.

**Spec:** `docs/superpowers/specs/2026-09-16-bark-actionable-notifications-design.md`

## Global Constraints

- Notifications remain `internal_paper`, read-only, AES-128-CBC encrypted, and incapable of approving, retrying, or placing an order.
- No new table, migration, dependency, model call, or notification settings system.
- Decimal values serialize as exact strings; UTC and currency remain explicit; missing values stay missing.
- Event-time cash and position facts are frozen inside the business transaction and never reconstructed from a later account state.
- No account ID, hash, evidence body, key, prompt, or executable token may appear in notification copy or logs.
- Existing activated-at filtering, bounded retries, dead letter behavior, and localhost-only Bark transport remain unchanged.

---

### Task 1: Closed notification snapshot and deterministic renderer

**Files:**
- Create: `engine/src/nexus/paper/notification.py`
- Modify: `engine/src/nexus/notifications/bark.py`
- Test: `engine/tests/unit/paper/test_notification.py`
- Test: `engine/tests/unit/notifications/test_bark.py`

**Interfaces:**
- Produces: `PaperOrderNotificationSnapshot`, `notification_snapshot_payload(snapshot)`, and `parse_notification_snapshot(payload)` in `nexus.paper.notification`.
- Produces: `map_bark_notification(message, read_only_url)` that accepts only a valid `notification_snapshot` and uses `str(snapshot.order_id)` as `id`.
- Consumes: existing `OutboxMessage.payload` and optional `/paper` read-only URL.

- [ ] **Step 1: Write failing closed-model tests**

Add literal fixtures covering a filled buy and risk rejection. Assert exact Decimal parsing, UTC enforcement, status/price/cash-kind bindings, quantity conservation, and rejection of extra fields or missing currency.

```python
def test_filled_snapshot_requires_actual_values() -> None:
    snapshot = PaperOrderNotificationSnapshot.model_validate(FILLED_PAYLOAD)
    assert snapshot.status == "filled"
    assert snapshot.price.kind == "average_fill"
    assert snapshot.cash.kind == "actual_after"
    assert snapshot.filled_quantity + snapshot.remaining_quantity == snapshot.approved_quantity
```

- [ ] **Step 2: Run the model test and verify RED**

Run:

```bash
uv run --frozen --package nexus-engine pytest -q engine/tests/unit/paper/test_notification.py
```

Expected: collection fails because `nexus.paper.notification` does not exist.

- [ ] **Step 3: Implement the minimal closed model**

Use frozen Pydantic models with `extra="forbid"`, exact Decimal validators, UTC validation, and a model validator with these bindings:

```python
status in {"risk_rejected", "filled", "cancelled"}
filled_quantity + remaining_quantity == approved_quantity
status == "filled" -> price.kind == "average_fill" and cash.kind == "actual_after"
status == "risk_rejected" -> filled_quantity == 0 and cash.kind == "unchanged"
status == "cancelled" -> cash.kind == "actual_after"
```

`notification_snapshot_payload` must return `{"notification_snapshot": snapshot.model_dump(mode="json")}`. `parse_notification_snapshot` must reject missing or non-object snapshots with a fixed `PaperNotificationError("PAPER_NOTIFICATION_INVALID")` that contains no values.

- [ ] **Step 4: Run model tests and verify GREEN**

Run the Task 1 model command and expect all tests to pass.

- [ ] **Step 5: Write failing renderer tests**

Replace fixed-copy expectations with exact multiline filled and rejected notifications. Assert:

```python
assert rendered["id"] == str(ORDER_ID)
assert rendered["title"] == "[内部模拟] AAPL 买入 12 股 · 已成交"
assert "成交均价：$187.36" in rendered["body"]
assert "可用现金 $21,406.18" in rendered["body"]
assert ACCOUNT_ID not in json.dumps(rendered, ensure_ascii=False)
```

Also assert that `strategy_decision.persisted`, `paper.order_batch.evaluated`, `paper.order.submitted`, and `paper.execution.advance.partially_filled` are absent from `BARK_EVENT_SCHEMAS` in this first deliverable.

- [ ] **Step 6: Run renderer tests and verify RED**

Run:

```bash
uv run --frozen --package nexus-engine pytest -q engine/tests/unit/notifications/test_bark.py
```

Expected: failures show fixed generic copy and event-ID notification identity.

- [ ] **Step 7: Implement deterministic rendering**

Keep only these first-deliverable contracts:

```python
BARK_EVENT_SCHEMAS = (
    ("paper.order.risk_rejected", "paper_event_v1"),
    ("paper.execution.advance.filled", "paper_execution_v1"),
    ("paper.execution.advance.cancelled", "paper_execution_v1"),
    ("paper.execution.cancelled", "paper_execution_v1"),
)
```

Render fixed labels for buy/sell, exact quantity, currency, price kind, notional, fee, cash, position, Agent reason, invalidation, and risk reason. Use `暂不可用` for allowed missing reason/invalidation/position ratio; never substitute zero. Preserve optional `/paper` URL validation.

- [ ] **Step 8: Run Task 1 tests and commit**

Run both Task 1 test files plus Ruff and Mypy on changed modules, then commit:

```bash
git add engine/src/nexus/paper/notification.py engine/src/nexus/notifications/bark.py engine/tests/unit/paper/test_notification.py engine/tests/unit/notifications/test_bark.py
git commit -m "feat(notifications): render actionable paper outcomes"
```

---

### Task 2: Freeze order rejection snapshots in the batch transaction

**Files:**
- Modify: `engine/src/nexus/paper/service.py`
- Modify: `engine/src/nexus/paper/notification.py`
- Test: `engine/tests/integration/test_paper_order_batch_pg.py`
- Test: `engine/tests/unit/paper/test_notification.py`

**Interfaces:**
- Produces: `build_order_decision_context(session, strategy_decision_id, instrument_id, decision_session) -> PaperNotificationDecisionContext`.
- Produces: `build_risk_rejected_snapshot(order, evaluation, portfolio, context, occurred_at) -> PaperOrderNotificationSnapshot`.
- Consumes: `PaperPortfolioProjection` already held by the batch service, `RiskEvaluation`, linked `AgentResearchOutputRecord.intent`, and active ticker alias.

- [ ] **Step 1: Write failing decision-context unit tests**

Use a validated `ResearchIntent` literal. Assert the context uses `display_symbol`, the first synthesis claim (falling back to the first thesis), and a fixed renderer for the first invalidation condition. Assert no second model call or evidence body is used.

- [ ] **Step 2: Run unit tests and verify RED**

Run the notification unit file; expect missing builder failures.

- [ ] **Step 3: Implement decision-context and rejection builders**

For Agent decisions, load the linked terminal output and validate `ResearchIntent`; match `instrument_id`. For registered-strategy decisions, resolve the active `InstrumentAlias` and leave Agent-only reason fields absent. Use the first failed `RiskEvaluation.rules` code through a fixed Chinese dictionary; unknown codes render as `风控规则未通过（<safe rule code>）` only when the code matches the existing safe identifier pattern.

For a rejection, freeze:

```python
filled_quantity = Decimal(0)
remaining_quantity = Decimal(0)
price.kind = "reference"
cash.kind = "unchanged"
cash.available = portfolio.available_cash
position.kind = "unchanged"
position.quantity = current position quantity
risk_reason = mapped first failed rule
```

- [ ] **Step 4: Write failing PostgreSQL integration assertion**

Extend the existing rejected-order case to load its `OutboxEvent` and validate `payload["notification_snapshot"]`. Assert symbol, side, requested quantity, unchanged cash/position, reason, and absence of account/hash/evidence fields inside the snapshot.

- [ ] **Step 5: Run the integration test and verify RED**

Run the single existing rejected-order integration test by node ID; expect `notification_snapshot` to be missing.

- [ ] **Step 6: Integrate snapshot production**

Pass the existing `PaperPortfolioProjection` into `_persist_new_batch`. After risk rows are persisted and before `_add_event_pair`, build exactly one decision context per order and add `notification_snapshot_payload(snapshot)` only to `paper.order.risk_rejected`. Keep decision, batch, reservation, and ledger event payloads unchanged.

- [ ] **Step 7: Run Task 2 tests and commit**

Run the focused unit/integration tests, Ruff, and Mypy; then commit:

```bash
git add engine/src/nexus/paper/notification.py engine/src/nexus/paper/service.py engine/tests/unit/paper/test_notification.py engine/tests/integration/test_paper_order_batch_pg.py
git commit -m "feat(paper): freeze rejected-order notification facts"
```

---

### Task 3: Freeze terminal execution snapshots after ledger and position updates

**Files:**
- Modify: `engine/src/nexus/paper/execution_service.py`
- Modify: `engine/src/nexus/paper/notification.py`
- Test: `engine/tests/integration/test_paper_execution_pg.py`
- Test: `engine/tests/unit/paper/test_notification.py`

**Interfaces:**
- Produces: `build_terminal_execution_snapshot(session, order, result, occurred_at) -> PaperOrderNotificationSnapshot`.
- Consumes: all persisted fills for the order, the post-transaction ledger balance, folded position adjustments, and the decision context from Task 2.

- [ ] **Step 1: Write failing aggregate-fill unit tests**

Assert weighted average price is `sum(notional) / sum(quantity)`, total notional and fee are exact sums, and a cancelled partially filled order retains actual filled and unfilled quantities.

- [ ] **Step 2: Run unit tests and verify RED**

Run the notification unit file and expect missing terminal builder failures.

- [ ] **Step 3: Implement terminal snapshot builder**

After all mutations are flushed, load all fills for the order, fold the account ledger with `balance_ledger`, fold the instrument position adjustments, and load the same frozen decision context. Use:

```python
cash.available = balances["asset.cash.available.usd"]
position.quantity = folded_position.quantity or Decimal(0)
price.kind = "average_fill" when filled_quantity > 0 else "reference"
cash.kind = "actual_after"
position.kind = "actual_after"
```

Do not calculate portfolio ratio without a same-time NAV authority; keep it `None`.

- [ ] **Step 4: Write failing PostgreSQL execution assertions**

Extend full-fill and cancel-after-partial-fill tests. Read the matching outbox payload and assert actual average price, notional, fee, available cash, position quantity, filled/remaining quantities, and the same order notification identity.

- [ ] **Step 5: Run integration tests and verify RED**

Run the two exact integration node IDs; expect the terminal snapshots to be absent.

- [ ] **Step 6: Integrate terminal snapshot production**

Extend `_insert_idempotency` with an optional validated `notification_snapshot` argument. Pass it only for `filled` and `cancelled` after `_verify_account_projections` succeeds. Merge the serialized snapshot into the corresponding outbox payload; leave `not_due`, `no_data`, `no_capacity`, and non-terminal idempotency records unchanged.

- [ ] **Step 7: Run Task 3 tests and commit**

Run focused unit/integration tests, Ruff, and Mypy; then commit:

```bash
git add engine/src/nexus/paper/notification.py engine/src/nexus/paper/execution_service.py engine/tests/unit/paper/test_notification.py engine/tests/integration/test_paper_execution_pg.py
git commit -m "feat(paper): freeze terminal execution notification facts"
```

---

### Task 4: End-to-end Bark outbox acceptance and documentation

**Files:**
- Modify: `engine/tests/integration/test_bark_outbox_pg.py`
- Modify: `docs/runbooks/bark-read-only-notifications.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: Task 1 renderer and Task 2/3 producer payloads.
- Produces: verified activation/readback instructions for detailed terminal order notifications.

- [ ] **Step 1: Write failing Bark integration cases**

Insert valid rejected and filled snapshots through normal Paper producers, run one Bark dispatch with an in-memory HTTP transport, decrypt the captured ciphertext, and assert the detailed user-visible copy and `id == order_id`. Assert decision, batch, submitted, and partial-fill events remain queued for other consumers and are not claimed by Bark.

- [ ] **Step 2: Run integration tests and verify RED**

Run:

```bash
uv run --frozen --package nexus-engine pytest -q engine/tests/integration/test_bark_outbox_pg.py
```

Expected: existing generic event expectations fail.

- [ ] **Step 3: Complete registry and integration behavior**

Make only the changes required by the failed tests. Bark must claim registered terminal/rejection events only, while other outbox events remain untouched rather than dead-lettered.

- [ ] **Step 4: Update operator documentation**

Document the exact immediate events, fields shown, lock-screen privacy recommendation, event-time snapshot rule, and the fact that pending/partial coalescing and daily summary are still second phase. Remove claims that all content is fully stripped of symbol/position/amount.

- [ ] **Step 5: Run final verification**

Run:

```bash
uv run --frozen --package nexus-engine pytest -q engine/tests/unit/notifications engine/tests/unit/paper/test_notification.py engine/tests/integration/test_bark_outbox_pg.py engine/tests/integration/test_paper_order_batch_pg.py engine/tests/integration/test_paper_execution_pg.py
uv run --frozen --package nexus-engine ruff check engine/src/nexus/notifications engine/src/nexus/paper/notification.py engine/src/nexus/paper/service.py engine/src/nexus/paper/execution_service.py engine/tests/unit/notifications engine/tests/unit/paper/test_notification.py engine/tests/integration/test_bark_outbox_pg.py
uv run --frozen --package nexus-engine mypy engine/src/nexus/notifications engine/src/nexus/paper/notification.py engine/src/nexus/paper/service.py engine/src/nexus/paper/execution_service.py
docker compose -p nexus-fosun-live --env-file infra/.env.local -f infra/compose.yaml --profile notifications config --quiet
git diff --check
```

- [ ] **Step 6: Commit**

```bash
git add engine/tests/integration/test_bark_outbox_pg.py docs/runbooks/bark-read-only-notifications.md README.md
git commit -m "docs(notifications): describe actionable Bark outcomes"
```

Do not rebuild or restart the live Bark worker until all four tasks pass and the user confirms one external test notification. Do not replay an uncertain Paper decision or order to manufacture acceptance data.

## 2026-09-17 日终简报最小切片交付记录

本节记录后续已批准的日终简报切片，不表示上方等待态/部分成交合并已实施。
分支 `edward/bark-session-summary`，基于 `455fa827`，独立 worktree；不合并、不推送。

- 在正常 `publish_paper_performance_snapshot` 路径完成事实一致性校验后，同事务写入
  `paper.session.summary.ready@paper_session_summary_v1`。当前 XNYS/USD 内部模拟范围内，
  当天有周期才产出；既有绩效重放直接返回，不重建或追补历史摘要。创建未来收盘绩效在入口拒绝。
- 新增冻结模型及事务内聚合，复用既有比较读取；未修改 Paper API 或 read_service。
  周期运行状态以数据库发布时间为界，绩效、当日成交/费用以收盘为界。无前一交易日快照时
  日盈亏和日收益未知；SPY 接受收盘后、冻结前已持久化的当日日线，缺失仍为未知。
- Bark 复用现有 registry、AES-CBC、localhost-only、activated-at、有限重试及 attempt。
  稳定通知 ID 为账户/市场/交易日组合，账户标识不进入正文；worker 只读 outbox 快照。
  投递失败不回滚已提交绩效，也不触发任何业务重放。无新表、迁移、依赖或调度框架。

### TDD 与审查证据

逐组先添加测试并实际观察失败，再添加最小实现：

- mapper：12 个新增场景因 `BARK_EVENT_UNSUPPORTED` 失败；实现版本化摘要映射后转绿。
- 真实 PostgreSQL 发布：修正不合法的测试事件链后，3 个场景明确因 outbox 摘要数量为零失败；
  实现事务内 enqueue 后转绿。无周期和回滚边界使用同一真实发布路径。
- 比例与时间：整数收益率被渲染为 `1%` 而非 `100%`、收盘前仍产生事件均先失败；修复后通过。
  审查进一步发现“提前写入快照后无法补通知”，顺序测试先报告应拒绝却未拒绝，再修复入口。
- 审查发现 SPY 可用性截止点错误：3 个表驱动场景分别得到不可用计数 `0`、SPY 净值/收益
  `None`；使用发布时间冻结后分别得到 `1`、`10100`、`0.01`（全为合成测试数据）。
- 订单全终态的等待市场周期先得到完成数 `0`，增加终态折叠后为 `1`；另覆盖仍有订单未终态。
- 非法金额/比率/时间组合及冻结时点后创建的周期均有先失败后通过的定向回归。
- 独立只读审查一轮，两项时间问题修复后定向复核通过；未发现剩余阻塞问题。

### 最终验证与交接

使用项目既有 Python 3.12 虚拟环境，显式 `PYTHONPATH=engine/src:integrations/fosun_gateway/src`。
PostgreSQL 测试使用专用 `nexus-bark-summary-test-41ee`、端口 `55448`，全部数据库为
`nexus_test_<uuid>`，未连接业务库。Bark HTTP 使用内存 MockTransport，不发送真实通知。

```text
pytest engine/tests/unit/notifications engine/tests/unit/paper
       engine/tests/integration/test_paper_session_summary_pg.py
       engine/tests/integration/test_paper_performance_pg.py
       engine/tests/integration/test_bark_outbox_pg.py
       engine/tests/integration/test_paper_api.py -q --tb=short
264 passed in 56.11s
ruff check engine: All checks passed
mypy engine/src: Success, 471 source files
git diff --check: passed
```

集成时选择性 cherry-pick 本分支交付提交即可；常规触点为 `performance.py`、`bark.py`、通知白名单
测试及本说明，其他实现集中在新增文件。若主线同期修改上述触点，按日终 enqueue/白名单增量保留
双方改动。未修改“运行就绪与阻断解释”任务工作树、配置或共享服务。
回滚无需迁移：回退本次应用改动即可停止新增摘要生产/领取，已提交事实与投递审计保留。
尚未集成、部署、重启或进行真实设备/APNs 验收；须在后续获授权部署后用自然日终事实验收。
