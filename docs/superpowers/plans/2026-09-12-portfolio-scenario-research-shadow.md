# Portfolio Scenario Research Shadow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 将当前用户明确接受的单一组合调整候选接入一个五交易日、只计算毛净值、不会生成订单或成交的研究型 Shadow，并在现有组合页面恢复和展示结果。

**Architecture:** 新增独立的 `nexus.scenario_shadow` 两表聚合，不修改两个既有 Shadow 子系统。启动时从已持久化的场景 claim 和选择记录冻结两组权重、初始价格、专用只读研究行情政策及五个 XNYS 交易日；手工推进时只读取已落库的原始日线和公司行动证据，复用纯 `apply_session_events` / `mark_to_market`，原子保存成功或拒绝事实。

**Tech Stack:** Python 3.12、Pydantic v2、SQLAlchemy 2、Alembic、PostgreSQL、FastAPI、React 19、TypeScript 5.9、vinext、Vitest、Playwright。

**Spec:** `docs/superpowers/specs/2026-09-12-portfolio-scenario-research-shadow-design.md`

## Global Constraints

- 两个固定 leg 仅为 `hold_current` 和 `selected_candidate`，权重分别精确合计为 `1`。
- 五个完整 XNYS 交易日只证明运行连续性，不生成策略有效、晋级或可交易结论。
- 所有响应保持 `execution_locked=true`、`execution_eligible=false`、`execution_authorized=false`。
- 不创建或修改 `paper_orders`、`paper_fills`、券商订单、Proposal Shadow、Strategy Evolution Shadow 或风控事实。
- 页面加载和刷新只执行 GET；开始和推进必须是用户明确点击，推进只读已有持久化行情，不触发供应商采集或模型调用。
- 未知成本保持未知；本功能只输出 gross NAV、gross return、相对基线差和最大回撤。
- 金融计算只使用 `Decimal`，持久化前沿用项目 12 位小数规范化，不使用二进制 float。
- 每个任务先写失败测试并确认失败原因，再提交最小实现；不得通过放宽来源、时点、身份或执行锁检查使测试通过。

---

## Task 1: Add closed research-Shadow contracts and pure gross-NAV calculation

**Files:**

- Create: `engine/src/nexus/scenario_shadow/__init__.py`
- Create: `engine/src/nexus/scenario_shadow/contracts.py`
- Create: `engine/src/nexus/scenario_shadow/calculation.py`
- Create: `engine/tests/unit/scenario_shadow/test_contracts.py`
- Create: `engine/tests/unit/scenario_shadow/test_calculation.py`
- Create: `engine/tests/property/test_scenario_shadow_properties.py`

**Interfaces:**

- Consumes: `PortfolioScenarioEvaluationV2`, `SimulationState`, `SessionEvents`, `SimulationMark`, `apply_session_events`, `mark_to_market`, `canonical_decimal`.
- Produces:

```python
ScenarioShadowLeg = Literal["hold_current", "selected_candidate"]

class ScenarioShadowAnchorPositionV1(BaseModel):
    instrument_id: UUID
    currency: Literal["USD"]
    anchor_price: Decimal
    current_weight: Decimal
    target_weight: Decimal

class ScenarioShadowAnchorV1(BaseModel):
    schema_version: Literal["portfolio_scenario_shadow_anchor_v1"]
    portfolio_snapshot_id: UUID
    scenario_result_hash: str
    valuation_time: datetime
    current_cash_weight: Decimal
    target_cash_weight: Decimal
    positions: tuple[ScenarioShadowAnchorPositionV1, ...]
    content_hash: str

class ScenarioShadowSessionEvidenceV1(BaseModel):
    schema_version: Literal["portfolio_scenario_shadow_evidence_v1"]
    shadow_id: UUID
    session: date
    ordinal: int
    observed_at: datetime
    marks: tuple[SimulationMark, ...]
    events: SessionEvents
    source_refs: tuple[ScenarioShadowSourceRefV1, ...]
    content_hash: str

class ScenarioShadowSourceRefV1(BaseModel):
    evidence_id: UUID
    source: str
    provider_object_id: str
    revision: str
    available_at: datetime
    content_hash: str

class ScenarioShadowStateV1(BaseModel):
    cash: Decimal
    instruments: tuple[SimulationInstrumentState, ...]
    receivables: tuple[DividendReceivable, ...]
    content_hash: str

class ScenarioShadowLegPointV1(BaseModel):
    leg: ScenarioShadowLeg
    session: date
    ordinal: int
    gross_nav: Decimal
    gross_cumulative_return: Decimal
    max_drawdown: Decimal
    state: ScenarioShadowStateV1

def initialize_shadow_state(
    anchor: ScenarioShadowAnchorV1,
    leg: ScenarioShadowLeg,
) -> SimulationState

def apply_shadow_mark(
    previous_state: SimulationState,
    evidence: ScenarioShadowSessionEvidenceV1,
) -> SimulationState

def build_leg_point(
    *,
    leg: ScenarioShadowLeg,
    session: date,
    ordinal: int,
    state: SimulationState,
    prior_navs: tuple[Decimal, ...],
) -> ScenarioShadowLegPointV1
```

- [ ] **Step 1: Write failing closed-contract tests**

Add tests that reject float values, duplicate instruments, non-USD anchors, non-positive prices,
noncanonical hashes, wrong schema versions, ordinals outside `1..5`, and weights not summing to
exactly `1`. Include the positive assertion:

```python
anchor = scenario_shadow_anchor(current_cash="0.10", target_cash="0.30")
assert sum((item.current_weight for item in anchor.positions), anchor.current_cash_weight) == 1
assert sum((item.target_weight for item in anchor.positions), anchor.target_cash_weight) == 1
```

- [ ] **Step 2: Run the contract test and confirm RED**

```bash
uv run --project engine pytest engine/tests/unit/scenario_shadow/test_contracts.py -q
```

Expected: collection fails because `nexus.scenario_shadow.contracts` does not exist.

- [ ] **Step 3: Implement immutable contracts and canonical content hashes**

Use `ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)`, reject floats recursively,
sort positions and source refs by UUID, require UTC datetimes, and calculate each `content_hash` from
sorted compact JSON excluding the hash field. Export only the public contracts and functions from
`nexus.scenario_shadow.__init__`.

- [ ] **Step 4: Write failing calculation and property tests**

Cover both legs, cash preservation, no implicit rebalancing, complete-mark rejection, split then
mark, dividend entitlement/payable cash, deterministic replay, and the gross return delta. The
core no-fill assertion is:

```python
state = initialize_shadow_state(anchor, "selected_candidate")
assert state.cash == Decimal("30")
assert {item.instrument_id: item.quantity for item in state.instruments} == {
    first_id: Decimal("1.0"),
    second_id: Decimal("0.5"),
}
assert all(item.cumulative_cost == 0 for item in state.instruments)
```

The property test generates exact Decimal weight partitions and positive prices, then proves
initial NAV equals exactly `100` and identical evidence yields identical state and point hashes.

- [ ] **Step 5: Run calculation tests and confirm RED**

```bash
uv run --project engine pytest engine/tests/unit/scenario_shadow/test_calculation.py engine/tests/property/test_scenario_shadow_properties.py -q
```

Expected: imports or function assertions fail because calculation is not implemented.

- [ ] **Step 6: Implement calculation without orders or fills**

Initialize quantities directly as `INITIAL_NAV * weight / anchor_price`; set cost basis,
realized P&L, cumulative cost, and last close without invoking `execute_target_weights`. For each
session call `apply_session_events`, then `mark_to_market`; reject nonempty
`missing_instruments`. Calculate drawdown as the minimum of `nav / running_peak - 1`, starting from
the initial NAV. Canonicalize the complete state and point before hashing.

- [ ] **Step 7: Run focused tests and static checks**

```bash
uv run --project engine pytest engine/tests/unit/scenario_shadow/test_contracts.py engine/tests/unit/scenario_shadow/test_calculation.py engine/tests/property/test_scenario_shadow_properties.py -q
uv run --project engine ruff check engine/src/nexus/scenario_shadow engine/tests/unit/scenario_shadow engine/tests/property/test_scenario_shadow_properties.py
uv run --project engine mypy engine/src/nexus/scenario_shadow
```

- [ ] **Step 8: Commit the pure domain slice**

```bash
git add engine/src/nexus/scenario_shadow engine/tests/unit/scenario_shadow engine/tests/property/test_scenario_shadow_properties.py
git commit -m "feat: add scenario research shadow contracts"
```

---

## Task 2: Publish the purpose-specific policy and two-table persistence schema

**Files:**

- Create: `engine/alembic/versions/0056_portfolio_scenario_research_shadow.py`
- Create: `engine/src/nexus/persistence/models/scenario_shadow.py`
- Modify: `engine/src/nexus/persistence/models/__init__.py`
- Modify: `engine/src/nexus/persistence/migrations.py`
- Modify: `engine/src/nexus/quality/source_policy.py`
- Modify: `engine/tests/helpers/migration_schema_expectations.py`
- Create: `engine/tests/integration/test_scenario_shadow_migration.py`
- Modify: `engine/tests/integration/test_source_policy.py`

**Interfaces:**

- Consumes: `SourcePolicy`, `IdempotencyRecord`, `PortfolioScenarioSelectionRecord`,
  `PortfolioSnapshotRecord`, `PortfolioDiagnosticReportRecord`, `ExchangeCalendarAuthorityRecord`.
- Produces:

```python
SCENARIO_SHADOW_SOURCE_POLICY_ID = UUID("12d773c6-6932-4094-934c-9997731746ab")
SCENARIO_SHADOW_SOURCE_POLICY_VERSION = "scenario_shadow_price_policy_v1"

async def load_scenario_shadow_source_policy_v1(
    session: AsyncSession,
    *,
    decision_time: datetime,
) -> ScenarioShadowSourcePolicyV1

class PortfolioScenarioShadowRecord(ImmutableCreatedAt, Base)
class PortfolioScenarioShadowAttemptRecord(ImmutableCreatedAt, Base)
```

- [ ] **Step 1: Write failing policy and migration tests**

Assert migration head `0056_portfolio_scenario_shadow`, exact policy identity/payload, five-session
array constraint, immutable execution lock, unique selection/version, applied-session partial
uniqueness, attempt result/reason checks, all foreign keys, and downgrade refusal while either new
table contains rows. Assert the policy payload exactly includes:

```python
{
    "purpose": "portfolio_scenario_research_shadow_gross_nav",
    "selection": "exact_fosun_raw_daily_series",
    "fallback": "disabled",
    "market_scope": "US",
    "currency": "USD",
    "interval": "1Day",
    "adjustment": "raw",
    "fosun": {
        "feed": "l1",
        "delivery_modes": ["real_time", "delayed"],
        "operation": "fosun_get_bars",
        "market_scope": "US",
    },
    "corporate_actions": "existing_complete_exact_set_authority_required",
    "allowed_consumers": ["portfolio_scenario_research_shadow"],
}
```

- [ ] **Step 2: Run policy/migration tests and confirm RED**

```bash
uv run --project engine pytest engine/tests/integration/test_source_policy.py engine/tests/integration/test_scenario_shadow_migration.py -q
```

Expected: missing policy constants, models, and revision `0056`.

- [ ] **Step 3: Implement the immutable policy loader**

The loader queries by exact ID, key, version, schema version, and payload, requires publication no
later than `decision_time`, and returns a frozen model containing the record ID, version, payload
hash, and `available_at`. Do not change any existing paper, proposal, diagnostic, quote, or
intraday selector.

- [ ] **Step 4: Implement model constraints and migration**

Add the exact columns from spec sections 6.1 and 6.2. Use JSONB documents, ARRAY(Date) sessions,
12-place numeric NAV, canonical-hash checks, `execution_locked IS TRUE`, result/reason consistency,
and:

```sql
CREATE UNIQUE INDEX uq_scenario_shadow_attempts_applied_session
ON portfolio_scenario_shadow_attempts (shadow_id, session)
WHERE status = 'applied'
```

Migration upgrade inserts the exact policy before creating roots that reference it. Downgrade
locks both new tables and `source_policies`, refuses when research facts exist, drops attempts then
roots, disables the immutable policy trigger only around deleting the exact unreferenced policy,
then reenables it.

- [ ] **Step 5: Update schema inventory and run migration round trip**

```bash
uv run --project engine pytest engine/tests/integration/test_scenario_shadow_migration.py engine/tests/integration/test_migration.py -q
uv run --project engine ruff check engine/alembic/versions/0056_portfolio_scenario_research_shadow.py engine/src/nexus/persistence/models/scenario_shadow.py engine/src/nexus/quality/source_policy.py engine/tests/integration/test_scenario_shadow_migration.py
uv run --project engine mypy engine/src/nexus/persistence/models/scenario_shadow.py engine/src/nexus/quality/source_policy.py
```

- [ ] **Step 6: Commit the authority/schema slice**

```bash
git add engine/alembic/versions/0056_portfolio_scenario_research_shadow.py engine/src/nexus/persistence/models engine/src/nexus/persistence/migrations.py engine/src/nexus/quality/source_policy.py engine/tests/helpers/migration_schema_expectations.py engine/tests/integration/test_scenario_shadow_migration.py engine/tests/integration/test_source_policy.py
git commit -m "feat: persist scenario research shadows"
```

---

## Task 3: Reuse verified scenario replay and implement idempotent Shadow startup

**Files:**

- Modify: `engine/src/nexus/agents/portfolio_scenario_selection.py`
- Create: `engine/src/nexus/scenario_shadow/authority.py`
- Create: `engine/src/nexus/scenario_shadow/service.py`
- Create: `engine/tests/unit/scenario_shadow/test_authority.py`
- Create: `engine/tests/integration/test_scenario_shadow_start_pg.py`
- Modify: `engine/tests/integration/test_portfolio_scenario_api.py`

**Interfaces:**

- Consumes: Task 1 contracts, Task 2 models/policy, `claim_or_lock`, `load_frozen_calendar`,
  existing scenario claim selection, audit, and outbox services.
- Produces:

```python
@dataclass(frozen=True)
class VerifiedScenarioSelection:
    selection: PortfolioScenarioSelectionRecord
    scenario_claim: IdempotencyRecord
    scenario: PortfolioScenarioEvaluationV2
    snapshot: PortfolioSnapshot

async def load_verified_scenario_selection(
    session: AsyncSession,
    store: MarketObjectStore,
    *,
    actor_id: str,
    scenario_result_hash: str,
    expected_selection_version: int,
    now: datetime,
    lock: bool,
) -> VerifiedScenarioSelection

async def start_scenario_research_shadow(
    session: AsyncSession,
    store: MarketObjectStore,
    *,
    actor_id: str,
    scenario_result_hash: str,
    expected_selection_version: int,
    idempotency_key: str,
    request_hash: str,
) -> ScenarioShadowStartResult

@dataclass(frozen=True)
class ScenarioShadowStartResult:
    shadow_id: UUID
    scenario_result_hash: str
    selection_version: int
    started_at: datetime
    evaluation_sessions: tuple[date, date, date, date, date]
    replayed: bool
    execution_locked: Literal[True]
    execution_eligible: Literal[False]
    execution_authorized: Literal[False]
```

- [ ] **Step 1: Write failing verified-replay tests**

Seed real-shaped scenario claims and assert accepted selection succeeds while watch/rejected,
wrong actor, stale version, changed scenario hash, changed snapshot/report binding, future claim,
and non-READY source snapshot return exact domain reason codes. Verify `lock=True` adds row locking
without changing the existing selection PUT/GET behavior.

- [ ] **Step 2: Run replay tests and confirm RED**

```bash
uv run --project engine pytest engine/tests/unit/scenario_shadow/test_authority.py engine/tests/integration/test_portfolio_scenario_api.py -q
```

Expected: `load_verified_scenario_selection` is unavailable.

- [ ] **Step 3: Extract one public verified replay path**

Move the existing private selection verification behind `load_verified_scenario_selection` while
retaining the current `put_portfolio_scenario_selection` contract. Return the exact persisted claim
and recomputed `PortfolioScenarioEvaluationV2`; do not accept scenario documents, weights, prices,
or identities from the new POST body.

- [ ] **Step 4: Write failing PostgreSQL start tests**

Cover successful root creation, exact frozen documents/hashes, first full session after start,
exact five sessions, actor isolation, idempotent replay, key conflict, concurrent start, existing
root mismatch, unavailable policy/calendar/anchor, audit event, outbox event, and fixed negative
execution flags. Before and after startup compare counts in:

```python
async def count_rows(session: AsyncSession, model: type[Base]) -> int:
    value = await session.scalar(select(func.count()).select_from(model))
    return int(value or 0)

for model in (PaperOrderRecord, PaperFillRecord, BrokerExecutionIntentOrderRecord):
    assert await count_rows(session, model) == before_counts[model]
```

- [ ] **Step 5: Run start test and confirm RED**

```bash
uv run --project engine pytest engine/tests/integration/test_scenario_shadow_start_pg.py -q
```

Expected: startup service and root persistence are not wired.

- [ ] **Step 6: Implement startup as one transaction**

Use database `transaction_timestamp()`, claim route
`portfolio-scenario-research-shadow:{scenario_result_hash}`, advisory-lock the selection/version,
load the purpose-specific policy and calendar, build the canonical anchor from frozen snapshot
positions, and insert one root. Derive sessions from persisted
`ExchangeCalendarSessionRecord.market_open_at > started_at`; never generate calendar dates in
application code. Store idempotency response, `portfolio_scenario_shadow.started` audit, and
`scenario_shadow.started` outbox event before flush.

- [ ] **Step 7: Run start/replay verification**

```bash
uv run --project engine pytest engine/tests/unit/scenario_shadow/test_authority.py engine/tests/integration/test_scenario_shadow_start_pg.py engine/tests/integration/test_portfolio_scenario_api.py -q
uv run --project engine ruff check engine/src/nexus/agents/portfolio_scenario_selection.py engine/src/nexus/scenario_shadow/authority.py engine/src/nexus/scenario_shadow/service.py engine/tests/unit/scenario_shadow/test_authority.py engine/tests/integration/test_scenario_shadow_start_pg.py
uv run --project engine mypy engine/src/nexus/agents/portfolio_scenario_selection.py engine/src/nexus/scenario_shadow/authority.py engine/src/nexus/scenario_shadow/service.py
```

- [ ] **Step 8: Commit startup**

```bash
git add engine/src/nexus/agents/portfolio_scenario_selection.py engine/src/nexus/scenario_shadow engine/tests/unit/scenario_shadow engine/tests/integration/test_scenario_shadow_start_pg.py engine/tests/integration/test_portfolio_scenario_api.py
git commit -m "feat: start scenario research shadows"
```

---

## Task 4: Bind persisted session evidence, advance contiguously, and recover the read view

**Files:**

- Modify: `engine/src/nexus/scenario_shadow/authority.py`
- Modify: `engine/src/nexus/scenario_shadow/service.py`
- Create: `engine/src/nexus/scenario_shadow/query.py`
- Create: `engine/tests/unit/scenario_shadow/test_query.py`
- Create: `engine/tests/integration/test_scenario_shadow_advance_pg.py`
- Create: `engine/tests/integration/test_scenario_shadow_query_pg.py`
- Create: `engine/tests/property/test_scenario_shadow_no_lookahead.py`

**Interfaces:**

- Consumes: frozen root, prior applied result state, raw daily-bar manifests/authorities,
  corporate-action authorities, Task 1 calculation.
- Produces:

```python
async def load_scenario_shadow_session_evidence(
    session: AsyncSession,
    *,
    root: PortfolioScenarioShadowRecord,
    expected_session: date,
    observed_at: datetime,
) -> ScenarioShadowEvidenceSelection

async def advance_scenario_research_shadow(
    session: AsyncSession,
    *,
    actor_id: str,
    scenario_result_hash: str,
    expected_session: date,
    idempotency_key: str,
    request_hash: str,
) -> ScenarioShadowAdvanceResult

async def load_scenario_research_shadow_view(
    session: AsyncSession,
    *,
    actor_id: str,
    scenario_result_hash: str,
) -> ScenarioShadowView | None

@dataclass(frozen=True)
class ScenarioShadowEvidenceSelection:
    status: Literal["complete", "rejected"]
    reason_code: str | None
    evidence: ScenarioShadowSessionEvidenceV1 | None
    input_document: dict[str, object]
    input_hash: str

@dataclass(frozen=True)
class ScenarioShadowAdvanceResult:
    shadow_id: UUID
    scenario_result_hash: str
    session: date
    ordinal: int
    status: Literal["applied", "rejected"]
    reason_code: str | None
    replayed: bool
    view: "ScenarioShadowView"

@dataclass(frozen=True)
class ScenarioShadowLegView:
    leg: ScenarioShadowLeg
    points: tuple[ScenarioShadowLegPointV1, ...]

@dataclass(frozen=True)
class ScenarioShadowRejectedAttemptView:
    session: date
    ordinal: int
    reason_code: str
    observed_at: datetime

@dataclass(frozen=True)
class ScenarioShadowView:
    shadow_id: UUID
    scenario_result_hash: str
    status: Literal["running", "complete"]
    applied_sessions: int
    total_sessions: Literal[5]
    next_expected_session: date | None
    legs: tuple[ScenarioShadowLegView, ScenarioShadowLegView]
    rejected_attempts: tuple[ScenarioShadowRejectedAttemptView, ...]
    cost_statuses: ScenarioCostBreakdownV1
    execution_locked: Literal[True]
    execution_eligible: Literal[False]
    execution_authorized: Literal[False]
```

- [ ] **Step 1: Write failing evidence and no-lookahead tests**

Cover exact Fosun/raw/1Day/USD selection, all nonzero instruments, latest qualified revision
available by `observed_at`, identical evidence for both legs, complete corporate-action collection,
split/dividend ordering, future-row exclusion, source conflicts, duplicate conflicting revisions,
wrong currency/session/identity, missing close, and unresolved delisting. The no-lookahead property
adds a later revision and proves the earlier `observed_at` selection and hash do not change.

- [ ] **Step 2: Run evidence tests and confirm RED**

```bash
uv run --project engine pytest engine/tests/property/test_scenario_shadow_no_lookahead.py engine/tests/integration/test_scenario_shadow_advance_pg.py -q
```

Expected: session evidence loader and advance service are missing.

- [ ] **Step 3: Implement fail-closed evidence selection**

Query only records whose `available_at <= observed_at`, verify the root's exact policy ID/version/
hash, bind full price identity and corporate-action authority, and return either a complete
`ScenarioShadowSessionEvidenceV1` or a stable rejected selection containing selected evidence IDs,
gaps, and one of:

```python
SCENARIO_SHADOW_PRICE_INCOMPLETE
SCENARIO_SHADOW_CORPORATE_ACTION_INCOMPLETE
SCENARIO_SHADOW_AUTHORITY_INVALID
```

- [ ] **Step 4: Write failing advance/concurrency/recovery tests**

Test ordinal 1 initialization, ordinal 2 previous-state recovery, five applied sessions, out-of-
order rejection, not-due rejection, exact replay, conflicting key, two concurrent applies, rejected
attempt then successful retry, complete status, post-complete rejection, audit/outbox records, and
zero paper/broker writes. Test GET reconstruction with no root, running root, rejected latest
attempt, and complete root after creating a fresh SQLAlchemy session.

- [ ] **Step 5: Implement atomic advance and query**

Use database time as `observed_at`; lock the root and derive the next ordinal from contiguous
applied attempts. Persist deterministic evidence gaps as `status="rejected"` and return a normal
result; persist successful state/metrics as `status="applied"`. Save every idempotency response,
audit event, and outbox event in the same transaction. Query validates every stored document/hash
before producing the view and fails closed on gaps or conflicts.

- [ ] **Step 6: Run advance/query verification**

```bash
uv run --project engine pytest engine/tests/unit/scenario_shadow/test_query.py engine/tests/integration/test_scenario_shadow_advance_pg.py engine/tests/integration/test_scenario_shadow_query_pg.py engine/tests/property/test_scenario_shadow_no_lookahead.py -q
uv run --project engine ruff check engine/src/nexus/scenario_shadow engine/tests/unit/scenario_shadow engine/tests/integration/test_scenario_shadow_advance_pg.py engine/tests/integration/test_scenario_shadow_query_pg.py engine/tests/property/test_scenario_shadow_no_lookahead.py
uv run --project engine mypy engine/src/nexus/scenario_shadow
```

- [ ] **Step 7: Commit advance and recovery**

```bash
git add engine/src/nexus/scenario_shadow engine/tests/unit/scenario_shadow engine/tests/integration/test_scenario_shadow_advance_pg.py engine/tests/integration/test_scenario_shadow_query_pg.py engine/tests/property/test_scenario_shadow_no_lookahead.py
git commit -m "feat: advance scenario research shadows"
```

---

## Task 5: Expose closed Engine API contracts

**Files:**

- Modify: `engine/src/nexus/api/errors.py`
- Modify: `engine/src/nexus/api/schemas/optimization.py`
- Modify: `engine/src/nexus/api/routes/optimization.py`
- Modify: `engine/tests/contract/test_openapi.py`
- Create: `engine/tests/integration/test_scenario_shadow_api.py`

**Interfaces:**

- Consumes: Task 3 start result and Task 4 advance/query results.
- Produces these exact operations:

```text
startPortfolioScenarioResearchShadow
getPortfolioScenarioResearchShadow
advancePortfolioScenarioResearchShadow
```

and schemas:

```text
ScenarioShadowStartRequest
ScenarioShadowAdvanceRequest
ScenarioShadowStartEnvelope
ScenarioShadowAdvanceEnvelope
ScenarioShadowViewEnvelope
```

- [ ] **Step 1: Write failing OpenAPI and API tests**

Assert exact paths/methods, 64-hex path parameter, bodies with `extra=forbid`, required
`Idempotency-Key`, start selection version `>=1`, advance ISO session, both fixed legs, canonical
decimal strings, exact negative execution flags, 404 for no root, 409 state conflicts, and 200
persisted rejection envelopes. Assert POST bodies cannot contain actor, weights, prices,
`observed_at`, or execution flags.

- [ ] **Step 2: Run API tests and confirm RED**

```bash
uv run --project engine pytest engine/tests/contract/test_openapi.py engine/tests/integration/test_scenario_shadow_api.py -q
```

Expected: new operations and schemas are absent.

- [ ] **Step 3: Add stable errors and strict schemas**

Add the eight spec error codes to `ErrorCode`. Response models use Literal schema/status/leg values,
UTC validators, string decimals, chronological-session validators, exact `total_sessions=5`, and
cross-field validation that `applied_sessions + remaining_sessions == 5`.

- [ ] **Step 4: Implement the three routes**

Start and advance use bounded exact-body dependencies and request hashes with distinct prefixes:

```python
start_hash = sha256(b"scenario-research-shadow-start\0" + raw_body).hexdigest()
advance_hash = sha256(b"scenario-research-shadow-advance\0" + raw_body).hexdigest()
```

Use the verified service actor and transactional session. Return deterministic rejected advances
normally; map only identity/version/order/due/completion conflicts to the exact 409 codes. GET uses
the normal read-only session and returns 404 when the actor-scoped root does not exist.

- [ ] **Step 5: Run API and static verification**

```bash
uv run --project engine pytest engine/tests/contract/test_openapi.py engine/tests/integration/test_scenario_shadow_api.py -q
uv run --project engine ruff check engine/src/nexus/api/errors.py engine/src/nexus/api/schemas/optimization.py engine/src/nexus/api/routes/optimization.py engine/tests/integration/test_scenario_shadow_api.py
uv run --project engine mypy engine/src/nexus/api/errors.py engine/src/nexus/api/schemas/optimization.py engine/src/nexus/api/routes/optimization.py
```

- [ ] **Step 6: Commit the Engine API slice**

```bash
git add engine/src/nexus/api engine/tests/contract/test_openapi.py engine/tests/integration/test_scenario_shadow_api.py
git commit -m "feat: expose scenario research shadow API"
```

---

## Task 6: Add BFF proxies and generated runtime validation

**Files:**

- Create: `app/api/v1/portfolio-scenarios/[resultHash]/research-shadow/route.ts`
- Create: `app/api/v1/portfolio-scenarios/[resultHash]/research-shadow/advance/route.ts`
- Modify: `app/lib/engine/client.ts`
- Modify: `app/lib/engine/errors.ts`
- Modify: `contracts/openapi.json`
- Modify: `app/lib/generated/schema.d.ts`
- Modify: `app/lib/generated/runtime-contract.json`
- Modify: `tests/unit/engine/portfolio-scenario-routes.test.ts`

**Interfaces:**

- Consumes: Task 5 generated OpenAPI operations.
- Produces browser routes at the same path without `/internal`, exact proxy templates, and runtime
  validators for all three response envelopes.

- [ ] **Step 1: Write failing BFF tests**

Assert GET forwards no body, POST forwards exact JSON plus idempotency key, result hash is decoded
and validated once, malformed/oversized JSON is rejected locally, upstream `status=rejected`
passes through, and malformed upstream legs/counts/hashes/flags become the existing safe invalid-
data response.

- [ ] **Step 2: Run BFF tests and confirm RED**

```bash
npm run test:unit -- tests/unit/engine/portfolio-scenario-routes.test.ts
```

Expected: research-shadow routes and runtime schemas are absent.

- [ ] **Step 3: Implement routes and client path admission**

Follow the existing scenario selection BFF pattern. Extend `app/lib/engine/client.ts` to admit only:

```text
/internal/v1/portfolio-scenarios/{64-hex}/research-shadow
/internal/v1/portfolio-scenarios/{64-hex}/research-shadow/advance
```

Forward no cookie-derived weights, prices, time, or actor fields.

- [ ] **Step 4: Generate contracts and implement strict runtime checks**

```bash
npm run generate:client
```

Do not edit generated files manually. Add handwritten envelope cross-field checks in
`app/lib/engine/errors.ts` for exact two-leg membership, distinct leg names, ordered session points,
five-session totals, scenario hash equality, and fixed negative execution flags.

- [ ] **Step 5: Run BFF/generated verification**

```bash
npm run test:unit -- tests/unit/engine/portfolio-scenario-routes.test.ts
npm run check:generated
npm run typecheck
```

- [ ] **Step 6: Commit BFF contracts**

```bash
git add app/api/v1/portfolio-scenarios app/lib/engine contracts/openapi.json app/lib/generated tests/unit/engine/portfolio-scenario-routes.test.ts
git commit -m "feat: proxy scenario research shadows"
```

---

## Task 7: Integrate the explicit research-Shadow UI

**Files:**

- Create: `app/components/portfolio-scenario-shadow.tsx`
- Modify: `app/components/portfolio-scenario.tsx`
- Modify: `app/globals.css`
- Create: `tests/unit/portfolio/portfolio-scenario-shadow.test.tsx`
- Modify: `tests/unit/portfolio/portfolio-scenario.test.tsx`
- Modify: `tests/e2e/portfolio-scenario.spec.ts`

**Interfaces:**

- Consumes: generated `ScenarioShadowViewEnvelope`, `ScenarioShadowStartEnvelope`, and
  `ScenarioShadowAdvanceEnvelope`.
- Produces:

```tsx
type PortfolioScenarioShadowProps = {
  scenarioResultHash: string;
  selection: PortfolioScenarioSelectionResponse | null;
  costBreakdown: ScenarioCostBreakdownResponse;
};

export function PortfolioScenarioShadow(
  props: PortfolioScenarioShadowProps,
): React.ReactElement | null
```

- [ ] **Step 1: Write failing component tests**

Test no control before acceptance; start control after `accepted_for_analysis`; GET-only mount and
reload; exact start body/version; two curve/table labels; progress and next session; original
unknown-cost reasons; explicit advance; disabled advance before due/after complete; rejected reason
without erasing prior points; request failure retaining stale data; and fixed notice:

```tsx
expect(screen.getByText("研究模拟，不生成订单，执行保持锁定")).toBeVisible();
expect(screen.queryByText(/净收益|夏普|可以交易|策略有效/)).not.toBeInTheDocument();
```

- [ ] **Step 2: Run UI tests and confirm RED**

```bash
npm run test:unit -- tests/unit/portfolio/portfolio-scenario-shadow.test.tsx tests/unit/portfolio/portfolio-scenario.test.tsx
```

Expected: component and controls do not exist.

- [ ] **Step 3: Implement the isolated component**

On mount GET the view; treat 404 as not started. Generate one idempotency key per explicit start or
advance click and retain it until a definite response. Start sends current selection version;
advance sends only the server-provided `next_expected_session`. Render a compact SVG or existing
chart primitive for two gross-NAV curves, plus an accessible table containing every plotted value.
Use existing decimal/time formatters and do not import `ShadowPerformance`.

- [ ] **Step 4: Mount under the existing selection result**

Pass only the persisted selection, current scenario hash, and cost breakdown from
`PortfolioScenario`. When result/snapshot/report becomes stale, preserve any loaded historical
view as read-only and disable writes. Do not add a new top-level page or navigation item.

- [ ] **Step 5: Add the focused E2E scenario**

The fixture E2E accepts the candidate, starts Shadow, reloads, verifies the same Shadow ID and
`0 / 5`, confirms no automatic advance request occurred, then performs one explicit advance
against fixture evidence and verifies `1 / 5` with both NAV rows. Network assertions must show no
broker order, paper order, provider collection, or model endpoint request.

- [ ] **Step 6: Run frontend verification**

```bash
npm run test:unit -- tests/unit/portfolio/portfolio-scenario-shadow.test.tsx tests/unit/portfolio/portfolio-scenario.test.tsx tests/unit/engine/portfolio-scenario-routes.test.ts
npm run typecheck
npm run lint
npm run test:e2e -- tests/e2e/portfolio-scenario.spec.ts
```

- [ ] **Step 7: Commit the user-visible slice**

```bash
git add app/components/portfolio-scenario-shadow.tsx app/components/portfolio-scenario.tsx app/globals.css tests/unit/portfolio tests/e2e/portfolio-scenario.spec.ts
git commit -m "feat: show scenario research shadow"
```

---

## Task 8: Run cross-layer acceptance and record the exact remaining live gate

**Files:**

- Modify: `docs/plans/2026-09-02-nexus-core-iteration-handoff.md`
- Modify: `docs/superpowers/plans/2026-09-12-portfolio-scenario-research-shadow.md`
- Test: all files listed in Tasks 1 through 7

**Interfaces:**

- Consumes: complete database-to-browser implementation.
- Produces: one reproducible local acceptance record and a narrowly stated eligible-session gate.

- [ ] **Step 1: Run the complete focused Engine suite**

```bash
uv run --project engine pytest engine/tests/unit/scenario_shadow engine/tests/property/test_scenario_shadow_properties.py engine/tests/property/test_scenario_shadow_no_lookahead.py engine/tests/integration/test_scenario_shadow_migration.py engine/tests/integration/test_scenario_shadow_start_pg.py engine/tests/integration/test_scenario_shadow_advance_pg.py engine/tests/integration/test_scenario_shadow_query_pg.py engine/tests/integration/test_scenario_shadow_api.py engine/tests/integration/test_portfolio_scenario_api.py engine/tests/contract/test_openapi.py -q
uv run --project engine ruff check engine/src/nexus/scenario_shadow engine/src/nexus/agents/portfolio_scenario_selection.py engine/src/nexus/persistence/models/scenario_shadow.py engine/src/nexus/quality/source_policy.py engine/src/nexus/api engine/tests/unit/scenario_shadow engine/tests/property/test_scenario_shadow_properties.py engine/tests/property/test_scenario_shadow_no_lookahead.py engine/tests/integration/test_scenario_shadow_migration.py engine/tests/integration/test_scenario_shadow_start_pg.py engine/tests/integration/test_scenario_shadow_advance_pg.py engine/tests/integration/test_scenario_shadow_query_pg.py engine/tests/integration/test_scenario_shadow_api.py
uv run --project engine mypy engine/src/nexus/scenario_shadow engine/src/nexus/agents/portfolio_scenario_selection.py engine/src/nexus/persistence/models/scenario_shadow.py engine/src/nexus/quality/source_policy.py engine/src/nexus/api/schemas/optimization.py engine/src/nexus/api/routes/optimization.py
```

- [ ] **Step 2: Run migration and frontend gates**

```bash
uv run --project engine pytest engine/tests/integration/test_migration.py engine/tests/integration/test_scenario_shadow_migration.py -q
npm run check:generated
npm run test:unit -- tests/unit/engine/portfolio-scenario-routes.test.ts tests/unit/portfolio/portfolio-scenario-shadow.test.tsx tests/unit/portfolio/portfolio-scenario.test.tsx
npm run typecheck
npm run lint
npm run build
```

- [ ] **Step 3: Perform local browser and database acceptance**

Use the normal local Engine/Web entry and the current persisted scenario. Record before/after counts
for the new roots/attempts and all paper/broker order/fill tables. In the browser accept the current
candidate if needed, start one Shadow, reload, and prove the same root recovers with `0 / 5`. With
fixture evidence in the isolated acceptance database only, advance one session and verify both
gross NAV values through database, Engine API, BFF, and page. Do not inject fixture evidence into
the user's real local database.

- [ ] **Step 4: Update the existing handoff, not a new progress document**

Record the user-visible result, exact commits, commands and results, migration head, browser path,
database evidence counts, and remaining gate: one real eligible session must produce a persisted
qualified daily close/corporate-action bundle and an applied Shadow attempt. Keep this distinct
from the separate Schwab display-only intraday acceptance.

- [ ] **Step 5: Final review and commit**

Review the branch diff for execution-boundary changes, hidden provider/model calls, mutable-history
updates, generated drift, secrets, and unrelated refactors. Then commit only acceptance records:

```bash
git add docs/plans/2026-09-02-nexus-core-iteration-handoff.md docs/superpowers/plans/2026-09-12-portfolio-scenario-research-shadow.md
git commit -m "docs: record scenario shadow acceptance"
```

## Plan review

Score: `97/100`.

| Dimension | Score | Evidence |
| --- | ---: | --- |
| Spec coverage | 20/20 | every spec section maps to Tasks 1 through 8 |
| Direct executability | 20/20 | exact files, public signatures, schemas, commands, assertions, and commits |
| Safety and authority | 20/20 | purpose-specific policy, no order/fill path, fixed negative execution flags |
| TDD and verification | 19/20 | RED/GREEN steps, property, PostgreSQL, API, UI, E2E, build, and browser checks |
| Scope discipline | 18/20 | two tables and one existing page; no old-Shadow generalization or adjacent backtest work |

The review found no uncovered spec requirement, placeholder, unresolved interface name, or type
name mismatch. The three deducted points reflect the necessary cross-layer migration/API/UI
surface and the external-time real-session gate. Neither requires broader architecture before
implementation.
