# Task 17 Forward Proposal Shadow Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement an append-only, point-in-time-correct three-portfolio Proposal Shadow that starts atomically from an eligible compared proposal group, advances exactly 63 frozen XNYS sessions, exposes auditable results, and never creates an order or trading approval.

**Architecture:** Extract the deterministic Decimal ledger/execution logic from Task 16 into a pure `nexus.simulation` kernel used by both backtests and Shadow. Keep Shadow orchestration, authority selection, persistence, lifecycle sealing, API projection, and UI in separate modules. PostgreSQL is the final lifecycle/immutability guard; HTTP only starts, reads, or rejects a Shadow, while a future Task 20 worker calls the internal advance service.

**Tech Stack:** Python 3.12, Pydantic 2, SQLAlchemy 2 async ORM, Alembic, PostgreSQL 17 with `pgcrypto`, FastAPI, pytest/pytest-asyncio/Hypothesis, Ruff, Mypy, Next.js/vinext, React 19, TypeScript, Vitest/Testing Library, Playwright, `lightweight-charts` 5.2.1.

**Spec:** `docs/superpowers/specs/2026-08-26-task-17-forward-proposal-shadow-design.md`

## Global Constraints

- Work only in `/Users/wangchen/Documents/ChatGPT/投资/.worktrees/nexus-phase-0-1` on `codex/nexus-phase-0-1`; inspect `git status --short` before every commit and preserve unrelated user changes.
- Task 17 is static Proposal Shadow only: one initial rebalance, no recurring optimizer/Agent calls, no 20-fill gate, no champion/challenger promotion, and no strategy self-modification.
- Do not create Task 18 paper orders/fills, broker orders, execution intents, trade approvals, live routes, credentials, or Fosun API calls. Every success response must keep `creates_orders=false` and `trading_approval=false`.
- `nexus.simulation` is pure: no SQLAlchemy, HTTP, LLM, network, environment reads, mutable globals, or system clock. It consumes frozen values and returns immutable values.
- Preserve Task 16 semantic behavior exactly: next-session execution, `missing_bar`, `untradable`, post-cost target-NAV solving, split/dividend/delisting order, Decimal boundary quantization, fill/outcome order, metrics, attribution, and data/protocol/authority hashes. The implementation `code_hash` must expand to the shared simulation files, so the derived `result_hash` must update reproducibly instead of retaining a stale implementation identity.
- All prices, quantities, cash, weights, NAV, costs, PnL, and metrics use finite `Decimal` and canonical `NUMERIC(38,12)` strings. Reject binary float at every domain/API boundary.
- All Shadow roots and children are append-only. PostgreSQL must reject UPDATE/DELETE and must reject lifecycle transitions that do not have the required complete Shadow facts and matching seal in the same transaction.
- `started_at`, GET freshness time, and seal time come from PostgreSQL UTC time. Browser time is never an authority input.
- The 63 evaluation sessions and 5 grace sessions are frozen at start. Grace may fill only missing sessions from the original 63; it never creates session 64 NAV.
- Provider incomplete-query evidence and complete negative evidence are different states. The first creates only a failed attempt; the second may create an immutable first-session `missing_bar` outcome.
- Every successful session binds an exact set of Bar/event authorities with `available_at <= observed_at`; no stale carry-forward, cross-source field stitching, adjusted-close execution, or look-ahead is permitted.
- Use TDD for each task: add the focused failing test, run it and confirm the intended failure, implement the minimum behavior, rerun focused tests, then run the listed regression gate.
- Migration verification requires a Docker-compatible runtime and Compose, but Docker Desktop is not required. OrbStack, Colima, Rancher Desktop, or another compatible runtime is acceptable.
- Do not mark Task 17 complete from UI state, an active agent, a large diff, or a green focused test alone. Completion requires committed code, full gates, database residual checks, and independent review evidence.

---

## Existing-to-Target File Map

| Concern | Existing source of truth | Task 17 target |
| --- | --- | --- |
| Decimal scale | `engine/src/nexus/backtest/canonical.py` | `engine/src/nexus/simulation/canonical.py`; backtest module re-exports for compatibility |
| Cost model | `engine/src/nexus/backtest/costs.py` | `engine/src/nexus/simulation/costs.py`; backtest module re-exports for compatibility |
| Ledger/execution/events | private `_Ledger`, `_rebalance`, `_apply_*`, `_mark_to_market` in `engine/src/nexus/backtest/engine.py` | public immutable types/functions in `engine/src/nexus/simulation/` plus backtest adapters |
| Shadow domain | none | `engine/src/nexus/shadow/models.py`, `metrics.py`, `canonical.py`, `state_machine.py` |
| Shadow DB facts | none | `engine/src/nexus/persistence/models/shadow.py`, `engine/alembic/versions/0013_shadow.py` |
| Lifecycle | generic `optimizer/lifecycle.py` and `optimizer/repository.py` | `shadow/start.py`, `shadow/service.py`, `shadow/repository.py`; rejection becomes Shadow-aware |
| Session authority | `PriceBarRecord`, `PriceBarSetAuthority`, `CorporateActionRecord`, Task 16 calendar authority | `shadow/authority.py` freezes exact selected rows and negative evidence |
| HTTP | generic POST shadow route in `api/routes/optimization.py` | dedicated POST start + GET projection; no advance/validate route |
| BFF/runtime | one POST route and generic lifecycle schema | GET/POST route, separate start/read runtime schemas, method-aware response boundary |
| UI | proposal detail page and `ProposalComparison` | parallel Shadow fetch plus `ShadowPerformance` below comparison |
| Verification ledger | `.superpowers/sdd/2026-08-22-nexus-phase-0-1/progress.md` | Task 17 acceptance evidence only after all gates pass |

## Fixed Public Interfaces

These names are the implementation contract for the tasks below. If an existing naming collision is discovered, change the plan before implementation rather than silently diverging.

```python
# engine/src/nexus/simulation/engine.py
def execute_target_weights(
    state: SimulationState,
    command: TargetWeightCommand,
) -> ExecutionResult:
    """Return a new post-cost state, deterministic fills/outcomes, and turnover."""

def apply_session_events(
    state: SimulationState,
    events: SessionEvents,
) -> EventApplicationResult:
    """Apply split, dividend entitlement/settlement, then resolved delisting."""

def mark_to_market(
    state: SimulationState,
    marks: tuple[SimulationMark, ...],
) -> ValuationResult:
    """Value cash, holdings, and receivables from one exact close set."""

# engine/src/nexus/shadow/start.py
async def start_proposal_shadow(
    session: AsyncSession,
    *,
    actor_id: str,
    idempotency_key: str,
    request_hash: str,
    group_id: UUID,
    expected_version: int,
) -> ShadowStartResult:
    """Atomically claim, revalidate, freeze, create, transition, audit, and publish."""

# engine/src/nexus/shadow/service.py
async def advance_shadow_session(
    session: AsyncSession,
    *,
    group_id: UUID,
    session_date: date,
    observed_at: datetime,
) -> ShadowAdvanceResult:
    """Apply or replay exactly the next frozen session in one transaction."""

# engine/src/nexus/shadow/query.py
async def load_shadow_view(
    session: AsyncSession,
    *,
    proposal_group_id: UUID,
) -> ShadowGroupView:
    """Verify hashes/cardinality and project append-only facts for GET."""
```

`observed_at` is an explicit Task 20 scheduling input and must be timezone-aware. It controls PIT eligibility for data selection. It does not replace database transaction time for lifecycle timestamps or GET freshness.

---

## Task 1: Extract the pure simulation kernel with zero Task 16 semantic drift

**Files**

- Create: `engine/src/nexus/simulation/__init__.py`
- Create: `engine/src/nexus/simulation/canonical.py`
- Create: `engine/src/nexus/simulation/costs.py`
- Create: `engine/src/nexus/simulation/models.py`
- Create: `engine/src/nexus/simulation/engine.py`
- Create: `engine/tests/unit/simulation/test_canonical.py`
- Create: `engine/tests/unit/simulation/test_execution.py`
- Create: `engine/tests/unit/simulation/test_events.py`
- Modify: `engine/src/nexus/backtest/canonical.py`
- Modify: `engine/src/nexus/backtest/costs.py`
- Modify: `engine/src/nexus/backtest/engine.py`
- Modify: `engine/tests/unit/backtest/test_engine.py`
- Test: `engine/tests/unit/backtest/test_backtest_metrics.py`
- Test: `engine/tests/property/test_no_lookahead.py`

**Interfaces and invariants**

Define frozen models with tuple-based canonical storage; do not expose mutable dict/set state:

```python
@dataclass(frozen=True)
class SimulationInstrumentState:
    instrument_id: UUID
    quantity: Decimal
    cost_basis: Decimal
    realized_pnl: Decimal
    cumulative_cost: Decimal
    last_close: Decimal | None
    untradable: bool

@dataclass(frozen=True)
class DividendReceivable:
    authority_id: UUID
    instrument_id: UUID
    amount: Decimal
    payable_session: date

@dataclass(frozen=True)
class SimulationState:
    cash: Decimal
    instruments: tuple[SimulationInstrumentState, ...]
    receivables: tuple[DividendReceivable, ...]

@dataclass(frozen=True)
class TargetWeightCommand:
    signal_session: date
    target_session: date
    targets: tuple[TargetWeight, ...]
    bars: tuple[SimulationBar, ...]
    fallback_marks: tuple[SimulationMark, ...]
    cost_model: CostModel
    execution_price: Literal["open", "close"]

@dataclass(frozen=True)
class ExecutionResult:
    state: SimulationState
    fills: tuple[SimulationFill, ...]
    outcomes: tuple[SimulationOutcome, ...]
    turnover_notional: Decimal
```

Canonical constructors must sort instruments/events by UUID integer and reject duplicate IDs, negative/non-finite prices, binary floats, weights outside `[0,1]`, and total target weight above one. `RESULT_QUANTUM` remains `Decimal("0.000000000001")`.

Backtest compatibility modules contain only stable re-exports:

```python
# nexus/backtest/canonical.py
from nexus.simulation.canonical import RESULT_QUANTUM, canonical_decimal

__all__ = ["RESULT_QUANTUM", "canonical_decimal"]

# nexus/backtest/costs.py
from nexus.simulation.costs import CostModel

__all__ = ["CostModel"]
```

Backtest adapters convert `_Ledger` inputs to a `SimulationState`, invoke the shared kernel, then replace the private ledger contents and map output records to existing `BacktestFill`/`BacktestOrderOutcome`. Existing public Backtest Pydantic schemas and serialized result payloads do not change.

**Steps**

- [x] Add `test_execution.py` cases for exact post-cost target NAV, fractional quantity, stable UUID ordering, `missing_bar`, `untradable`, and immutability of the input state. Assert Decimal strings rather than approximate floats.
- [x] Add `test_events.py` cases for split before dividend, dividend settlement, cash/zero-recovery delisting, unresolved delisting refusal, and complete close-set marking.
- [x] Compare the fixed pre-extraction Task 16 fixture with the post-extraction result and prove NAV, fills, outcomes, turnover, costs, metrics, attribution, delisting count, and the complete payload excluding `code_hash/result_hash` are identical. Add a test proving `_engine_code_hash()` covers every shared simulation implementation file and the derived result hash is reproducible.
- [x] Run `uv run --package nexus-engine pytest engine/tests/unit/simulation engine/tests/unit/backtest/test_engine.py engine/tests/unit/backtest/test_backtest_metrics.py engine/tests/property/test_no_lookahead.py -q`; confirm the new imports/tests fail before `nexus.simulation` exists.
- [x] Move the canonical Decimal and `CostModel` implementation into `nexus.simulation`; leave the compatibility re-exports above so existing imports and OpenAPI types do not drift.
- [x] Implement the frozen state/event/execution models and pure functions by moving the existing formulas, not re-deriving them. Preserve the 32-iteration post-cost solver and `RESULT_QUANTUM` convergence condition.
- [x] Adapt `_run_simulation()` and `_run_passive_benchmark()` to the new kernel. Keep strategy timing, feature preparation, attribution, and Backtest result assembly inside `nexus.backtest`.
- [x] Run the focused command again; expect exit 0, identical semantic payload, a deliberately updated implementation `code_hash`, and a reproducible derived `result_hash`.
- [x] Run `uv run --package nexus-engine ruff check engine/src/nexus/simulation engine/src/nexus/backtest engine/tests/unit/simulation engine/tests/unit/backtest && uv run --package nexus-engine mypy engine/src/nexus/simulation engine/src/nexus/backtest`; expect exit 0.
- [x] Commit only Task 1 files with `refactor(engine): extract deterministic simulation kernel`.

---

## Task 2: Implement pure Shadow models, hashes, metrics, and state machine

**Files**

- Create: `engine/src/nexus/shadow/__init__.py`
- Create: `engine/src/nexus/shadow/models.py`
- Create: `engine/src/nexus/shadow/canonical.py`
- Create: `engine/src/nexus/shadow/metrics.py`
- Create: `engine/src/nexus/shadow/state_machine.py`
- Create: `engine/tests/unit/shadow/test_canonical.py`
- Create: `engine/tests/unit/shadow/test_metrics.py`
- Create: `engine/tests/unit/shadow/test_state_machine.py`
- Create: `engine/tests/property/test_shadow_properties.py`

**Interfaces and invariants**

Use one fixed variant order everywhere:

```python
ShadowVariant = Literal["hold_current", "minimum_change", "strategy_target"]
SHADOW_VARIANTS: tuple[ShadowVariant, ShadowVariant, ShadowVariant] = (
    "hold_current",
    "minimum_change",
    "strategy_target",
)
CHECKPOINT_ORDINALS = (5, 21, 63)
EVALUATION_SESSION_COUNT = 63
GRACE_SESSION_COUNT = 5
INITIAL_NAV = Decimal("100.000000000000")
```

Metric values are explicit known/unknown states:

```python
@dataclass(frozen=True)
class ShadowMetricValue:
    status: Literal["known", "unknown"]
    value: Decimal | None
    reason: str | None

@dataclass(frozen=True)
class ShadowMetrics:
    net_cumulative_return: ShadowMetricValue
    return_delta_vs_hold: ShadowMetricValue
    max_drawdown: ShadowMetricValue
    drawdown_improvement: ShadowMetricValue
    annualized_volatility: ShadowMetricValue
    volatility_improvement: ShadowMetricValue
    worst_sessions: tuple[WorstSession, ...]
    cumulative_turnover: ShadowMetricValue
    fee_drag_index_points: ShadowMetricValue

def calculate_checkpoint_metrics(
    *,
    nav_by_variant: Mapping[ShadowVariant, tuple[ShadowNavPoint, ...]],
    turnover_by_variant: Mapping[ShadowVariant, Decimal],
    fees_by_variant: Mapping[ShadowVariant, Decimal],
    ordinal: Literal[5, 21, 63],
) -> tuple[VariantMetrics, VariantMetrics, VariantMetrics]:
    """Return hold/minimum/strategy metrics in fixed order."""
```

`shadow_session_sha256_v1`, proposal-bundle hash, target-weight hash, state hash, checkpoint metrics hash, and final-authority hash use canonical JSON with sorted keys, UTF-8, compact separators, UUID lowercase text, dates as ISO, UTC with six fractional digits and `Z`, and Decimal fixed to 12 places. Random row IDs and `created_at` never enter hashes.

The pure lifecycle decision accepts counts/evidence, never queries a database:

```python
@dataclass(frozen=True)
class ShadowFinalGate:
    completed_sessions: int
    complete_checkpoint_ordinals: tuple[int, ...]
    checkpoint_metric_rows: int
    initial_execution_complete: bool
    authority_binding_valid: bool
    has_data_gap: bool
    grace_exhausted: bool

def decide_terminal_action(
    gate: ShadowFinalGate,
) -> Literal["continue", "validate", "expire_initial_execution", "expire_data"]:
    if gate.completed_sessions == 63 and not gate.initial_execution_complete:
        return "expire_initial_execution"
    if (
        gate.completed_sessions == 63
        and gate.complete_checkpoint_ordinals == (5, 21, 63)
        and gate.checkpoint_metric_rows == 9
        and gate.authority_binding_valid
        and not gate.has_data_gap
    ):
        return "validate"
    if gate.grace_exhausted and gate.has_data_gap:
        return "expire_data"
    return "continue"
```

**Steps**

- [x] Add metric tests for `[100, nav_1, ..., nav_n]`, sample standard deviation `(n-1) * sqrt(252)`, max drawdown, positive improvement semantics, worst-session tie-break `(return asc, session asc)`, cumulative turnover, fee index points, and fewer-than-two-return volatility = 0.
- [x] Add unknown tests proving incomplete input returns `value=None` with a stable non-empty reason and never converts missing data to zero.
- [x] Add canonical-hash tests that insertion order does not matter, changing any authority revision/available time/negative evidence changes the hash, and IDs/creation timestamps excluded by schema do not change it.
- [x] Add state-machine tests for observation at 5/21, validation only at complete 63, immediate expiration for permanent first-session execution blockers, grace-only data completion, and expiration at grace exhaustion.
- [x] Add Hypothesis properties for fixed variant order, hash determinism, metric bounds (`drawdown in [-1,0]`, nonnegative turnover/fees/volatility), and no terminal result before the specified gates.
- [x] Run `uv run --package nexus-engine pytest engine/tests/unit/shadow engine/tests/property/test_shadow_properties.py -q`; confirm failure before the package exists.
- [x] Implement the models, validators, canonicalizers, metric formulas, and pure terminal decision. Do not import optimizer, SQLAlchemy, Agent, paper, broker, or API modules.
- [x] Rerun focused tests, Ruff, and Mypy for `nexus.shadow`; expect exit 0.
- [x] Commit with `feat(engine): add proposal shadow domain model`.

---

## Task 3: Add the append-only `0013_shadow` schema and database guards

**Files**

- Create: `engine/src/nexus/persistence/models/shadow.py`
- Create: `engine/alembic/versions/0013_shadow.py`
- Modify: `engine/src/nexus/persistence/models/__init__.py`
- Modify: `engine/alembic/env.py`
- Modify: `engine/tests/helpers/migration_schema_expectations.py`
- Modify: `engine/tests/helpers/migration_assertions.py`
- Modify: `engine/tests/integration/test_migration.py`
- Create: `engine/tests/integration/test_shadow_migration.py`

**Schema contract**

Create these fourteen append-only tables and matching ORM records:

```text
shadow_groups                    ShadowGroupRecord
shadow_portfolios               ShadowPortfolioRecord
shadow_advance_attempts         ShadowAdvanceAttemptRecord
shadow_session_authorities      ShadowSessionAuthorityRecord
shadow_session_price_inputs     ShadowSessionPriceInputRecord
shadow_session_event_inputs     ShadowSessionEventInputRecord
shadow_portfolio_states         ShadowPortfolioStateRecord
shadow_instrument_states        ShadowInstrumentStateRecord
shadow_receivable_states        ShadowReceivableStateRecord
shadow_simulated_outcomes       ShadowSimulatedOutcomeRecord
shadow_simulated_fills          ShadowSimulatedFillRecord
shadow_checkpoints              ShadowCheckpointRecord
shadow_checkpoint_metrics       ShadowCheckpointMetricRecord
shadow_seals                    ShadowSealRecord
```

Use named constraints and composite FKs so database checks can prove same-root ownership. At minimum:

```sql
UNIQUE (proposal_group_id)
UNIQUE (shadow_group_id, variant)
UNIQUE (proposal_id)
UNIQUE (shadow_group_id, session)
UNIQUE (shadow_group_id, ordinal)
UNIQUE (session_authority_id, instrument_id)
UNIQUE (shadow_portfolio_id, session)
UNIQUE (shadow_group_id, ordinal) /* checkpoint */
UNIQUE (checkpoint_id, variant)
UNIQUE (shadow_group_id) /* seal */
CHECK (cardinality(evaluation_sessions) = 63)
CHECK (cardinality(grace_sessions) = 5)
CHECK (ordinal BETWEEN 1 AND 63)
CHECK (checkpoint ordinal IN (5,21,63))
CHECK (initial_nav = 100.000000000000)
CHECK (schema_version = 'proposal_shadow_v1')
CHECK (content_hash ~ '^[0-9a-f]{64}$')
```

Implement three groups of PostgreSQL guards:

1. `nexus_reject_shadow_fact_mutation()` rejects UPDATE/DELETE on all fourteen tables.
2. `nexus_shadow_group_complete()` and `nexus_shadow_checkpoint_complete()` are DEFERRABLE INITIALLY DEFERRED constraint triggers that enforce exactly three fixed variants/states/metric rows and same-root proposal ownership at commit.
3. `nexus_shadow_unsealed()` rejects child INSERT after a seal, and the seal/lifecycle constraint proves `reason` matches the proposal terminal status.

Replace `nexus_optimization_group_lifecycle()` in this migration. Preserve every existing 0009 authority predicate, then add database counts:

```sql
-- compared -> shadowing
shadow_group_count = 1
AND shadow_portfolio_count = 3
AND variants = ARRAY['hold_current','minimum_change','strategy_target']

-- shadowing -> validated
seal_reason = 'validated'
AND authority_count = 63
AND portfolio_state_count = 189
AND complete_checkpoint_count = 3
AND checkpoint_metric_count = 9
AND incomplete_initial_outcome_count = 0

-- shadowing -> rejected|expired
seal_reason = NEW.status
```

`downgrade()` must drop Shadow-dependent triggers/functions/tables in reverse dependency order and recreate the exact 0009 lifecycle function body before the Shadow tables disappear.

**Steps**

- [x] Add all fourteen names to `EXPECTED_TABLES`, add a Shadow optional migration group for downgrade-path assertions, and list every named FK expected after head.
- [x] Add failing migration tests for head table/constraint presence, `0012 -> 0013 -> 0012 -> 0013`, and downgrade restoration of the non-Shadow 0009 lifecycle function.
- [x] Add direct SQL red tests for UPDATE/DELETE rejection, orphan/cross-root rows, noncanonical Decimal/hash/date arrays, a two-portfolio group, a fourth portfolio, incomplete checkpoint metrics, child insert after seal, mismatched seal/status, and direct validated transition without 63/189/3/9 facts.
- [x] Run `uv run --package nexus-engine pytest engine/tests/integration/test_migration.py engine/tests/integration/test_shadow_migration.py -q`; confirm intended failures against migration head 0012.
- [x] Implement ORM records first with explicit `NUMERIC(38,12)`, `UTCDateTime`, `Date`, UUID, JSONB only for query/source summaries and worst sessions, and named composite constraints.
- [x] Register the new ORM module in both model imports and Alembic metadata discovery.
- [x] Implement `0013_shadow.py`, including the deferred exact-three/checkpoint guards, seal guard, append-only triggers, lifecycle replacement, and exact downgrade restoration.
- [x] Run upgrade/downgrade/upgrade and all migration tests; expect exit 0 and no residual Shadow table/function after downgrade to 0012.
- [x] Run `uv run --package nexus-engine ruff check engine/src/nexus/persistence/models/shadow.py engine/alembic/versions/0013_shadow.py engine/tests/integration/test_shadow_migration.py && uv run --package nexus-engine mypy engine/src/nexus/persistence/models/shadow.py`; expect exit 0.
- [x] Commit with `feat(db): add append-only proposal shadow schema`.

---

## Task 4: Implement atomic start and Shadow-aware rejection

**Files**

- Create: `engine/src/nexus/shadow/repository.py`
- Create: `engine/src/nexus/shadow/start.py`
- Create: `engine/src/nexus/shadow/cost_policy.py`
- Create: `engine/tests/unit/shadow/test_cost_policy.py`
- Create: `engine/tests/helpers/shadow.py`
- Create: `engine/tests/integration/test_shadow_start_pg.py`
- Modify: `engine/src/nexus/optimizer/lifecycle.py`
- Modify: `engine/src/nexus/optimizer/repository.py`
- Modify: `engine/tests/integration/test_optimization_lifecycle.py`
- Modify: `engine/tests/helpers/optimization.py`

**Start contract**

```python
@dataclass(frozen=True)
class ShadowStartResult:
    proposal_group_id: UUID
    shadow_group_id: UUID
    status: Literal["shadowing"]
    version: int
    start_session: date
    evaluation_end_session: date
    grace_end_session: date

async def start_proposal_shadow(
    session: AsyncSession,
    *,
    actor_id: str,
    idempotency_key: str,
    request_hash: str,
    group_id: UUID,
    expected_version: int,
) -> ShadowStartResult:
    """Use one transaction and one canonical lock order for all start facts."""
```

Repository lock order is fixed for Task 4 onward: idempotency claim, proposal group `FOR UPDATE`, Shadow root `FOR UPDATE` when present. Start obtains database time with `SELECT transaction_timestamp()` and chooses the first frozen XNYS session strictly after the corresponding New York market date. It loads 68 ordered sessions from one complete `ExchangeCalendarAuthorityRecord` and verifies authority hash/member count before using them.

Proposal targets are computed only from immutable rows:

```python
def target_weight_for(variant: ShadowVariant, position: ProposalPositionRecord) -> Decimal:
    if variant == "hold_current":
        return position.current_weight
    return position.target_weight
```

Adapt the frozen cost policy with an explicit versioned function; never read cost values from UI or environment configuration:

```python
def cost_model_from_policy(policy: CostPolicyRecord) -> CostModel:
    if policy.key != "cost_model" or policy.version != "cost_policy_v1":
        raise ValueError("COST_POLICY_UNSUPPORTED")
    return CostModel(
        commission_bps=Decimal(str(policy.parameters["commission_bps"])),
        spread_bps=Decimal("0"),
        slippage_bps=Decimal(str(policy.parameters["slippage_bps"])),
    )
```

`cost_model` is the existing immutable Task 12 policy key and is also required by the
database lifecycle predicate; `transaction_cost` in the pre-implementation draft was
an unseeded name and must not create a second authority identity.

The adapter validates the exact committed `cost_policy_v1` parameter-key set, schema version, finite Decimal values, evidence refs, and `available_at <= started_at`; the snippet shows the fixed spread behavior, not permission to ignore extra/missing keys.

The start service reruns the full executable gate from persisted authority, comparisons, diagnostics, baseline snapshot, data snapshot, source policy, cost policy, selection authority, and `strategy_target` evidence. Do not call `_shadow_eligibility_reasons()` on an API projection as the authorization check.

Replace only the `shadowing` use of `transition_group_idempotently`; rejection remains the public optimizer operation but becomes Shadow-aware:

```python
async def reject_group_idempotently(
    session: AsyncSession,
    *,
    actor_id: str,
    idempotency_key: str,
    request_hash: str,
    group_id: UUID,
    expected_version: int,
) -> LifecycleTransition:
    """Seal an existing Shadow as rejected before the lifecycle CAS."""
```

Start audit/outbox names are `shadow.started`; Shadow-aware rejection uses `shadow.rejected`. Idempotency stores the canonical response fields so replay returns the original Shadow ID and boundaries.

**Steps**

- [x] Build `seed_shadow_eligible_group()` and `seed_shadow_calendar()` helpers that insert exactly three proposals/comparisons, eligible authorities, and 68 future XNYS sessions without inserting any Shadow row.
- [x] Add red tests for successful start, exact three variants/NAV=100, hold-current weight mapping, proposal/hash binding, database-time session boundary, weekend/holiday behavior, stale/partial/source-conflict/cost/FX/evidence rejection, and rollback after each injected failure point.
- [x] Add cost-policy unit tests for exact `cost_policy_v1` adaptation, explicit spread=0, binary-float/unknown-key rejection, unsupported version, missing evidence, and future `available_at`.
- [x] Add idempotency tests: same actor/key/body returns the same Shadow ID and no extra facts; same key/different body returns `IDEMPOTENCY_BODY_MISMATCH`; different actors may use the same textual key independently.
- [x] Add concurrency tests: two distinct keys on the same version converge to one success and one stable version/state conflict; no partial variant rows remain.
- [x] Add rejection tests for `draft|compared` without Shadow, `shadowing` with atomic rejected seal, rejected start replay, and a direct status write rejected by the database guard.
- [x] Run `uv run --package nexus-engine pytest engine/tests/integration/test_shadow_start_pg.py engine/tests/integration/test_optimization_lifecycle.py -q`; confirm failures before the services exist.
- [x] Implement canonical proposal/target hashes, calendar freezing, persisted eligibility revalidation, idempotency response persistence, CAS, audit, and outbox in `start.py`/`repository.py`.
- [x] Modify rejection to use the same proposal-first lock order and create the matching seal in the same transaction only when current status is `shadowing`.
- [x] Rerun focused tests plus `engine/tests/integration/test_optimization_api.py`; expect exit 0.
- [x] Run Ruff/Mypy for Shadow start/repository and optimizer lifecycle files; expect exit 0.
- [x] Commit with `feat(engine): start and reject proposal shadows atomically`.

---

## Task 5: Freeze point-in-time session authority without carry-forward

**Files**

- Create: `engine/src/nexus/shadow/authority.py`
- Create: `engine/tests/integration/test_shadow_authority_pg.py`
- Create: `engine/tests/property/test_shadow_no_lookahead.py`
- Modify: `engine/src/nexus/shadow/canonical.py`
- Modify: `engine/tests/helpers/shadow.py`
- Reuse: `engine/src/nexus/persistence/models/data.py`
- Reuse: `engine/src/nexus/persistence/models/research.py`
- Reuse: `engine/src/nexus/persistence/models/backtest.py`
- Reuse: `engine/src/nexus/quality/source_policy.py`

**Authority contract**

```python
ShadowAuthorityStatus = Literal[
    "complete",
    "data_partial",
    "data_stale",
    "source_conflict",
]

@dataclass(frozen=True)
class ShadowSessionInput:
    status: ShadowAuthorityStatus
    reason: str | None
    session: date
    ordinal: int
    observed_at: datetime
    late_backfill: bool
    authority_hash: str | None
    bars: tuple[BoundShadowBar, ...]
    missing_execution_inputs: tuple[MissingExecutionInput, ...]
    events: tuple[BoundShadowEvent, ...]
    source_summary: tuple[ShadowSourceSummary, ...]
    delivery_mode_summary: tuple[str, ...]
    maximum_available_at: datetime | None

async def load_shadow_session_input(
    session: AsyncSession,
    *,
    root: LockedShadowRoot,
    session_date: date,
    ordinal: int,
    observed_at: datetime,
    required_instruments: tuple[UUID, ...],
    initial_execution_instruments: tuple[UUID, ...],
) -> ShadowSessionInput:
    """Select one complete exact authority set under the frozen source policy."""
```

The loader joins selected `PriceBarRecord -> PriceBarSetAuthority -> PriceSeriesManifest` and validates source-policy version, instrument, session, revision, row/set content hash, `available_at`, raw adjustment mode, and delivery mode. Selection is deterministic: policy source priority, then latest eligible `available_at`, then revision, then authority UUID integer.

For initial execution, a complete source query with no eligible Bar creates a canonical `MissingExecutionInput`; an incomplete Provider/source query returns `data_partial`. For later marking, every live held instrument requires one raw close or a resolved delisting event. Missing close, unresolved delisting, event exact-set uncertainty, or cross-source conflict prevents a successful authority and NAV.

Event order passed to the shared kernel is fixed: split, dividend entitlement/settlement, resolved delisting, then initial execution outside the event function, then close mark. An action announced after its effective-session decision boundary is not eligible merely because a grace-period `observed_at` is later.

The existing `price_bar_set_sha256_v1` database finalizer admits only Alpaca delayed
and fixture manifests; it deliberately cannot mint a positive Fosun Bar authority
without a frozen capability/status binding. Task 17 therefore keeps Fosun first in
query-proof priority but fails closed on an incomplete Fosun proof and never treats a
raw Fosun manifest as authority. Positive Fosun Bar admission requires a future
versioned authority schema carrying that binding; it must not weaken v1 in place.

**Steps**

- [x] Add fixtures for production/delayed/fixture sources, two revisions with different `available_at`, incomplete query proof, complete negative proof, split/dividend/delisting events, unresolved delisting, and conflicting sources.
- [x] Add red integration tests proving exact eligible row selection, no adjusted close, no mixed open/close sources, no `available_at > observed_at`, and stable canonical source/delivery summaries.
- [x] Add separate tests proving incomplete query produces no authority, complete negative proof can produce first-session `missing_bar`, and a late revision cannot replace a previously frozen negative input.
- [x] Add gap tests: a missing later close prevents authority/NAV; a future session cannot skip it; resolved cash/zero-recovery delisting removes subsequent close requirements; unresolved delisting remains partial.
- [x] Add Hypothesis tests varying record insertion order/revisions and prove selected IDs/hash are stable and no future-available row is ever selected.
- [x] Run `uv run --package nexus-engine pytest engine/tests/integration/test_shadow_authority_pg.py engine/tests/property/test_shadow_no_lookahead.py -q`; confirm failure before the loader exists.
- [x] Implement exact-set selection and canonical `shadow_session_sha256_v1`; keep provider payloads/credentials out of returned reasons and persisted summaries.
- [x] Rerun focused tests, Ruff, and Mypy; expect exit 0.
- [x] Commit with `feat(engine): freeze proposal shadow session authority`.

---

## Task 6: Advance, checkpoint, seal, validate, and expire Shadow groups

**Files**

- Create: `engine/src/nexus/shadow/service.py`
- Create: `engine/src/nexus/shadow/query.py`
- Create: `engine/tests/unit/shadow/test_service.py`
- Create: `engine/tests/integration/test_shadow_advance_pg.py`
- Create: `engine/tests/integration/test_shadow_query_pg.py`
- Modify: `engine/src/nexus/shadow/repository.py`
- Modify: `engine/src/nexus/shadow/metrics.py`
- Modify: `engine/tests/helpers/shadow.py`
- Modify: `engine/tests/property/test_shadow_properties.py`

**Advance and projection contracts**

```python
@dataclass(frozen=True)
class ShadowAdvanceResult:
    shadow_group_id: UUID
    session: date
    ordinal: int | None
    result: Literal[
        "applied",
        "replayed",
        "data_partial",
        "data_stale",
        "source_conflict",
        "session_gap",
        "sealed",
    ]
    reason: str | None
    proposal_status: ProposalGroupStatus
    checkpoint_ordinal: int | None

async def advance_shadow_session(
    session: AsyncSession,
    *,
    group_id: UUID,
    session_date: date,
    observed_at: datetime,
) -> ShadowAdvanceResult:
    """Persist one exact next session or one sanitized failure attempt."""
```

Each call locks proposal then Shadow root, derives expected ordinal from successful authorities, and follows this matrix:

| Input | Result | Writes |
| --- | --- | --- |
| existing successful session | `replayed` | none |
| sealed group | `sealed` | none |
| date before expected without authority | stable conflict | none |
| date after expected | `session_gap` | one failed attempt |
| incomplete/stale/conflicting authority | matching result | one failed attempt |
| exact authority | `applied` | authority + three complete states + children + success attempt |

Ordinal 1 starts each portfolio from all-cash NAV=100, applies frozen events, executes its frozen targets once at raw open, then marks at raw close. Ordinals 2..63 apply events and marks only; the code path must not accept target weights and must assert zero new outcome/fill rows.

Checkpoint creation occurs in the same transaction at 5, 21, and 63. Terminalization order is: insert complete/incomplete checkpoint facts, insert matching seal, CAS proposal status, append audit/outbox. The transaction rolls back entirely on a deferred-constraint or CAS failure.

Read projection verifies root bundle hash, child counts, per-session authority/state hash, checkpoint metric hash, and seal/status match. It returns `DATA_PARTIAL` for corruption/cardinality mismatch instead of a partial success envelope. It computes `coverage.due` and freshness using database time and the frozen calendar.

**Steps**

- [x] Add unit tests that ordinal 1 invokes `execute_target_weights` exactly once per variant, ordinal 2 never invokes it, all three variants consume the same authority hash, and one variant failure aborts the whole successful-session write.
- [x] Add integration tests for session 1 facts/fills/outcomes, an all-cash zero-outcome variant, session replay, gap rejection, failed attempt sanitization, two workers racing the same session, and append-only state reconstruction.
- [x] Add tests for 5/21 observation checkpoints, 63 complete auto-validation, initial `missing_bar|untradable` auto-expiration, data gap grace backfill, no session 64, and grace-exhausted expiration.
- [x] Add advance/reject and finalize/reject races; assert one matching terminal status/seal, one terminal audit/outbox, and no post-seal child rows.
- [x] Add query tests for not-started 404 domain result, fixed variant/NAV ordering, due/missing/freshness/source summaries, last failed attempt, complete metrics, and corruption/hash mismatch fail-closed behavior.
- [x] Add a database boundary test that snapshots counts for paper/broker/order/execution-intent tables before 63-session validation and proves they are unchanged afterward.
- [x] Run `uv run --package nexus-engine pytest engine/tests/unit/shadow/test_service.py engine/tests/integration/test_shadow_advance_pg.py engine/tests/integration/test_shadow_query_pg.py engine/tests/property/test_shadow_properties.py -q`; confirm failures before the service exists.
- [x] Implement state reconstruction from the latest append-only snapshot, one-session simulation, exact child persistence, checkpoint metrics, final-gate calculation, seal/CAS/audit/outbox, and replay behavior.
- [x] Implement `load_shadow_view()` as a verified projection; do not recompute financial metrics in the API or browser.
- [x] Rerun focused tests plus all Shadow/migration tests, Ruff, and Mypy; expect exit 0.
- [x] Commit with `feat(engine): advance and finalize proposal shadows`.

Evidence: the first focused run failed at collection because `nexus.shadow.service`
and then `advance_shadow_session` did not exist. The implemented slice passed 23
focused tests and 104 expanded Shadow/migration tests; `ruff check engine`, `mypy
engine/src`, and `git diff --check` exited 0. PostgreSQL tests also proved that
63-session validation did not change broker-order or backtest execution tables.

---

## Task 7: Replace the generic Shadow API and synchronize OpenAPI/BFF contracts

**Files**

- Modify: `engine/src/nexus/api/schemas/optimization.py`
- Modify: `engine/src/nexus/api/routes/optimization.py`
- Modify: `engine/tests/integration/test_optimization_api.py`
- Create: `engine/tests/integration/test_shadow_api.py`
- Modify: `engine/tests/contract/test_openapi.py`
- Modify: `app/api/v1/optimization-proposals/[proposalGroupId]/shadow/route.ts`
- Modify: `app/lib/engine/client.ts`
- Modify: `app/lib/engine/errors.ts`
- Modify: `scripts/generate-client.mjs`
- Regenerate: `contracts/openapi.json`
- Regenerate: `app/lib/generated/schema.d.ts`
- Regenerate: `app/lib/generated/runtime-contract.ts`
- Modify: `tests/unit/optimize/routes.test.ts`
- Modify: `tests/unit/optimize/runtime-boundary.test.ts`
- Modify: `tests/unit/engine/client-security.test.ts`

**API schema contract**

Add separate start and read models. Do not reuse `OptimizationLifecycleData` for POST Shadow:

```python
class ShadowStartData(_StrictOptimizationSchema):
    proposal_group_id: UUID
    shadow_group_id: UUID
    status: Literal["shadowing"]
    version: int = Field(ge=0)
    start_session: date
    evaluation_end_session: date
    grace_end_session: date

class ShadowStartEnvelope(_StrictOptimizationSchema):
    data: ShadowStartData
    meta: SuccessMetaResponse

class ShadowCoverageResponse(_StrictOptimizationSchema):
    completed: int = Field(ge=0, le=63)
    due: int = Field(ge=0, le=63)
    required: Literal[63]
    next_expected_session: date | None
    missing_sessions: tuple[date, ...]

class ShadowSafetyResponse(_StrictOptimizationSchema):
    creates_orders: Literal[False]
    trading_approval: Literal[False]

class ShadowGroupResponse(_StrictOptimizationSchema):
    proposal_group_id: UUID
    shadow_group_id: UUID
    proposal_status: ProposalGroupStatus
    version: int = Field(ge=0)
    start_session: date
    evaluation_end_session: date
    grace_end_session: date
    coverage: ShadowCoverageResponse
    freshness: ShadowFreshnessResponse
    sources: tuple[ShadowSourceResponse, ...]
    portfolios: tuple[ShadowPortfolioResponse, ShadowPortfolioResponse, ShadowPortfolioResponse]
    checkpoints: tuple[ShadowCheckpointResponse, ...]
    sealed: bool
    seal_reason: Literal["validated", "rejected", "expired"] | None
    safety: ShadowSafetyResponse
```

The `ShadowGroupResponse` validator enforces fixed portfolio order, at most 63 strictly increasing NAV sessions per portfolio, checkpoint ordinals from `{5,21,63}`, known/unknown metric consistency, and seal/status agreement.

Routes:

```text
POST /internal/v1/optimization-proposals/{group_id}/shadow -> ShadowStartEnvelope 200
GET  /internal/v1/optimization-proposals/{group_id}/shadow -> ShadowGroupEnvelope 200
```

POST keeps exact body, `Idempotency-Key`, and `If-Match`; GET has no body/idempotency/If-Match. Both use the same URL but the BFF response allowlist branches on method: POST validates `shadowStartRuntimeSchema`, GET validates `shadowGroupRuntimeSchema`. Reject remains `optimizationLifecycleRuntimeSchema`.

**Steps**

- [x] Add FastAPI red tests for POST start body/boundaries/idempotency/error mapping and GET not-started 404, fixed order, Decimal strings, data-partial 409, freshness, source latency, safety flags, and no order/trade IDs.
- [x] Add OpenAPI contract tests asserting both operations, distinct response schemas, no advance/checkpoint/validate path, and all nested Shadow schemas are closed (`additionalProperties=false`).
- [x] Replace `shadow_optimization_group()` with a direct `start_proposal_shadow()` adapter; keep `_transition_endpoint()` only for rejection.
- [x] Add GET route using `load_shadow_view()` and stable error details: `SHADOW_NOT_STARTED`, exact Shadow corruption/authority reasons, and no provider/table/credential strings.
- [x] Extend `rootNames` in `scripts/generate-client.mjs` with `ShadowStartData` and `ShadowGroupResponse`; export `shadowStartRuntimeSchema` and `shadowGroupRuntimeSchema`, then run `npm run generate:client`.
- [x] Export both `GET` and `POST` from the BFF route. GET forwards only actor/accept; POST preserves exact bytes and guarded headers.
- [x] Split `allowedSuccessData()` by request method/path, verify proposal group UUID semantically, fixed variant order, 63-point cap, safety false constants, and seal/status consistency before returning data.
- [x] Add BFF unit tests for method confusion, malformed nested Decimal/date/UUID/order, oversized arrays, extra fields, unsafe upstream errors, uppercase-equivalent UUIDs, GET 404, and POST response identity.
- [x] Run `uv run --package nexus-engine pytest engine/tests/integration/test_shadow_api.py engine/tests/integration/test_optimization_api.py engine/tests/contract/test_openapi.py -q`; expect exit 0.
- [x] Run `npm run check:generated && npm run test:unit -- tests/unit/optimize/routes.test.ts tests/unit/optimize/runtime-boundary.test.ts tests/unit/engine/client-security.test.ts`; expect exit 0.
- [x] Run Ruff/Mypy for API/Shadow schemas and `npm run typecheck`; expect exit 0.
- [x] Commit generated and handwritten contract files together with `feat(api): expose auditable proposal shadow results`.

Evidence: the first focused API/OpenAPI run failed in five places because POST
still returned the generic lifecycle shape and GET did not exist; the first BFF
run then failed on the missing GET export and both method-specific response
contracts. The implemented slice passed 28 focused Engine tests and 87 focused
Web unit tests. Generated-contract checks, Ruff, Mypy across 179 source files,
TypeScript typecheck, ESLint, and `git diff --check` all exited 0.

---

## Task 8: Add ShadowPerformance and complete proposal-page states

**Files**

- Create: `app/components/shadow-performance.tsx`
- Create: `tests/unit/optimize/shadow-performance.test.tsx`
- Modify: `app/(protected)/optimize/[proposalGroupId]/page.tsx`
- Modify: `app/styles/tokens.css`
- Modify: `tests/unit/optimize/fixtures.ts`
- Modify: `tests/unit/optimize/proposal-comparison.test.tsx`
- Modify: `tests/unit/optimize/pages.test.tsx`
- Modify: `tests/e2e/optimize.spec.ts`
- Reuse: `app/components/price-chart.tsx`

**Component and page contract**

```tsx
type Shadow = components["schemas"]["ShadowGroupResponse"];

export function ShadowPerformance(props: {
  shadow: Shadow;
}): React.ReactElement;
```

`ShadowPerformance` renders, in order:

1. status/start/latest/completed/current-checkpoint/available-at summary;
2. permanent no-order/no-approval notice;
3. one normalized NAV chart with line styles solid/dashed/dotted and a NAV=100 reference;
4. a complete semantic table containing every session and all three exact Decimal NAV strings;
5. 5/21/63 metrics table with known/unknown reason and explicit fee unit “NAV 指数点”;
6. coverage/source/delivery/last-attempt panel;
7. first-session outcome/fill table.

Canvas is `aria-hidden`; keyboard controls operate a semantic selected-session detail. Use Home/End and ArrowLeft/ArrowRight. Keep a visible focus ring, direct end labels, non-color line legend, table caption/headers, and an `aria-live="polite"` status region. Disable nonessential transitions under `prefers-reduced-motion`.

The proposal page uses two independent states:

```tsx
type ShadowLoadState = "loading" | "not-started" | "ready" | "partial" | "stale" | "error";

const [groupResult, shadowResult] = await Promise.allSettled([
  fetch(groupUrl, { headers: { accept: "application/json" } }),
  fetch(shadowUrl, { headers: { accept: "application/json" } }),
]);
```

Shadow 404 maps to `not-started`; group 404 still maps the whole page to empty. A Shadow refresh failure keeps the last verified curve and marks it stale; it does not fabricate points. POST start consumes `ShadowStartEnvelope`, updates group status/version, then refetches GET Shadow. POST reject continues to consume `OptimizationLifecycleEnvelope`.

Separate action gates:

```tsx
const canStartShadow = groupState === "ready" && group.status === "compared";
const canReject = groupState === "ready" && ["draft", "compared", "shadowing"].includes(group.status);
```

Shadow stale/partial state disables any future risk-increasing Shadow action, but does not hide a valid latest-version rejection from a healthy group response. No buy/sell/paper/live/validate/promote/evolve control is added.

**Steps**

- [ ] Add fixture builders for not-started, active, grace, partial, stale, validated, rejected, and expired Shadow responses with exactly ordered variants and known/unknown metrics.
- [ ] Add component red tests for line semantics, reference line, direct labels, all-session table, metric signs/units, unknown reasons, source/delivery disclosure, safety notice, initial outcomes, keyboard navigation, and no trading controls.
- [ ] Add page red tests for parallel fetch, Shadow 404, retained stale data, POST start response/refetch, separate reject response, latest-version rejection during shadowing, error retry/request ID, and group failure precedence.
- [ ] Run `npm run test:unit -- tests/unit/optimize/shadow-performance.test.tsx tests/unit/optimize/proposal-comparison.test.tsx tests/unit/optimize/pages.test.tsx`; confirm intended failures before the component/state split exists.
- [ ] Implement `ShadowPerformance` using the existing dynamic `lightweight-charts` pattern and design tokens; add no dependency.
- [ ] Split page fetch/action types and render the component below `ProposalComparison` only after successful Shadow GET. Render skeleton/not-started/error states without an empty chart.
- [ ] Add responsive CSS for 375, 390, 768, 1024, 1440 px and 390x844 landscape; tables may scroll horizontally but must retain all variants/data.
- [ ] Extend Playwright route fixtures and assertions for all viewports, keyboard selection, reduced motion, zero serious/high axe failures, line-style legend, semantic table, and absence of trade/approval/evolution actions.
- [ ] Run focused unit tests, `npm run typecheck`, `npm run lint`, `npm run build`, and `npm run test:e2e -- tests/e2e/optimize.spec.ts`; expect exit 0.
- [ ] Commit with `feat(web): visualize proposal shadow performance`.

---

## Task 9: Run full acceptance, independent review, and update the ledger

**Files**

- Modify only after acceptance: `.superpowers/sdd/2026-08-22-nexus-phase-0-1/progress.md`
- Modify only after acceptance: `docs/superpowers/plans/2026-08-22-nexus-phase-0-1.md` Task 17 checkbox/evidence lines
- Create: `docs/superpowers/reviews/2026-08-26-task-17-forward-proposal-shadow-review.md`

**Acceptance evidence contract**

The review document records commit range, commands with exit status/counts, migration round-trip, residual database counts, and independent findings by severity. It must explicitly state that Task 17 does not complete Task 18–21, Stage 2 self-evolution, Fosun trading, or the full NEXUS MVP.

**Steps**

- [ ] Inspect `git status --short`, `git diff --check`, and `git log --oneline --decorate -10`; resolve only Task 17 issues and preserve unrelated changes.
- [ ] Start the project’s existing PostgreSQL/MinIO services with `docker compose -f infra/compose.yaml up -d postgres minio`; do not require or install Docker Desktop if another compatible runtime is already working.
- [ ] Run `uv run --package nexus-engine alembic -c engine/alembic.ini downgrade 0012_backtests`, then `upgrade head`, then the migration suite; verify reversible lifecycle restoration and head reapplication.
- [ ] Run the full engine gates:

```bash
uv run --package nexus-engine pytest engine/tests/unit/simulation engine/tests/unit/backtest engine/tests/unit/shadow engine/tests/property engine/tests/integration -q
uv run --package nexus-engine ruff check engine
uv run --package nexus-engine mypy engine/src
```

- [ ] Run the full Web gates:

```bash
npm test
npm run typecheck
npm run lint
npm run check:generated
npm run build
npm run test:e2e
```

- [ ] Query the acceptance database and record zero unexpected rows in Task 18 paper order/fill/ledger tables, broker/live order or execution-intent tables, and zero Shadow facts after a tested rollback transaction. Do not print account data or credentials.
- [ ] Recompute at least one session state hash and one 63-session checkpoint metrics hash from append-only inputs in a test/verification command; compare exactly with persisted hashes.
- [ ] Perform an independent review focused on look-ahead, authority exact sets, gap/carry-forward behavior, Decimal/unit correctness, first-session-only execution, idempotency, lock order, seal races, database bypasses, BFF method/schema confusion, error leakage, accessibility, and order boundary.
- [ ] Fix every Critical/Important finding with a red test and rerun affected/full gates. Record unresolved Minor findings explicitly; do not declare acceptance with unresolved Critical or Important findings.
- [ ] Write `docs/superpowers/reviews/2026-08-26-task-17-forward-proposal-shadow-review.md` with the exact evidence and finding counts.
- [ ] Update the progress ledger and original Task 17 checkboxes only after all gates and review pass. Replace the obsolete 20-fill Task 17 line with a reference to the approved static Proposal Shadow spec; do not modify Phase 2’s separate 20-fill rule.
- [ ] Commit acceptance-only documentation with `docs: record Task 17 shadow acceptance`.
- [ ] Confirm `git status --short` is clean for Task 17-owned files and report the final commit range. Do not mark the overall goal complete; continue next with Task 18 only after the user chooses execution/continuation.

---

## Spec Coverage Matrix

| Spec requirement | Implemented/tested in |
| --- | --- |
| Static three-way NAV=100, one initial rebalance | Tasks 2, 4, 6 |
| Shared deterministic kernel, zero Task 16 semantic drift, correct implementation hash | Task 1 |
| 63 XNYS sessions + 5-session grace | Tasks 2, 4, 6 |
| PIT raw Bar/event exact-set authority | Task 5 |
| Negative evidence versus incomplete query | Tasks 5, 6 |
| Append-only state, exact-three variants, seals | Task 3 |
| Atomic start/idempotency/concurrency | Task 4 |
| Ordered advance, checkpoints, validate/expire | Task 6 |
| Shadow-aware rejection and races | Tasks 4, 6 |
| POST start and GET read; no advance/validate HTTP | Task 7 |
| Safe BFF/generated contracts | Task 7 |
| Chart + semantic table + all UI states/a11y | Task 8 |
| No paper/broker/trade/approval side effects | Global constraints, Tasks 6–9 |
| Full regression, migration round-trip, independent review | Task 9 |

## Plan Self-Review Checklist

- [x] Every accepted specification section 1–14 maps to at least one task/test in the coverage matrix.
- [x] Every task names exact create/modify/test files and fixed interfaces.
- [x] Every implementation slice starts with an observable failing test and ends with focused verification plus a commit.
- [x] No step asks the implementer to invent a route, lifecycle, table, variant, metric, error, or authority rule not fixed by the approved specification.
- [x] No unresolved implementation marker or generic omitted-test instruction remains in this plan.
- [x] Python/TypeScript schema names match across engine OpenAPI, generator roots, runtime contracts, BFF allowlist, page types, and tests.
- [x] Migration revision is exactly `0013_shadow` over `0012_backtests`; downgrade restores the pre-Shadow lifecycle function.
- [x] Task 17 completion is kept distinct from Stage 2 Strategy Evolution Shadow, Task 18 paper trading, Fosun broker integration, and the full MVP.
