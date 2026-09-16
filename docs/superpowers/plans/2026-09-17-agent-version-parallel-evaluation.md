# Agent Version Parallel Evaluation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build an AAPL-only, fail-closed incumbent/challenger Agent Paper evaluation that uses two isolated internal simulation accounts, separates train from out-of-sample slots, and changes the future internal Agent version only after a deterministic PASS.

**Architecture:** Add three append-only Agent-version authority tables, a focused evaluation service, and an explicit frozen-version admission path on top of the existing Agent Paper cycle. Both lanes reuse the current research, sizing, risk, internal order, fill, ledger, and performance pipeline; API/BFF/page changes are read-only. Real-time observations are scheduled only after fixture and product-path verification, and never create broker or live commands.

**Tech Stack:** Python 3.12, SQLAlchemy asyncio, PostgreSQL/Alembic, FastAPI/Pydantic, pytest, React 19, TypeScript, vinext, Vitest, Playwright.

**Spec:** `docs/superpowers/specs/2026-09-17-agent-version-parallel-evaluation-design.md`

## Global Constraints

- Scope is exactly one AAPL challenger; do not add Universe rotation, another data provider, or reuse/extend `strategy_shadow_*`.
- Create two isolated `simulation_write_internal` evaluation accounts from the same USD all-cash initial capital; leave the existing Stage 2 account and its facts unchanged.
- Every related `agent_paper_cycle` must retain `execution_locked=true`; no broker binding, broker command, external account identifier, broker-simulation write, or live-order path may be created.
- Freeze exactly 4 train slots over at least 2 XNYS sessions and exactly 20 OOS slots over at least 10 later XNYS sessions, at most 2 OOS slots per session.
- A prompt or model change requires a new evaluation. Missing or replaced OOS slots, mismatched authorities, or insufficient decision divergence produce `UNKNOWN`, never PASS.
- PASS requires all 20 comparable OOS pairs, at least 3 sizing-action differences, at least 1 challenger internal fill, after-cost excess return of at least `0.005`, positive daily net excess on at least 7 of 10 sessions, drawdown degradation no greater than `0.0025`, and no hard-risk, ledger, duplicate-order, or duplicate-fill defect.
- `FAIL` or `UNKNOWN` keeps the frozen incumbent. PASS can affect only future internal Agent Paper version selection; it does not grant broker-simulation or live authority.
- Before admitting any real model work, inspect persisted `evaluation -> cycles -> runs -> jobs -> outputs`; never replay work because a stream, observer, or worker timed out.
- Preserve the user's dirty `README.md`, `docs/images/`, `docs/user-guide.md`, and untracked 2026-09-13 plan/spec. Do not push or deploy.

---

## File Structure

### New files

- `engine/alembic/versions/0062_agent_version_evaluation.py` — tables, constraints, validation triggers, immutability triggers, and empty-only downgrade fence.
- `engine/src/nexus/persistence/models/agent_versions.py` — ORM records for evaluation, cycle pair, and final decision facts.
- `engine/src/nexus/paper/agent_version_models.py` — frozen request/result objects, v1 policy constants, and canonical hashes.
- `engine/src/nexus/paper/agent_version_evaluation.py` — registration, isolated account creation, progress, finalization, and active internal-version selection.
- `engine/src/nexus/paper/agent_version_runner.py` — one-slot dual-lane preparation/resume without model or provider transport.
- `engine/src/nexus/ops/agent_version_evaluation.py` — local-only `register`, `advance`, `resume`, `finalize`, and `status` CLI.
- Focused migration, PostgreSQL, policy, CLI, component, and page tests named in the tasks below.

### Modified files

- Persistence: `engine/src/nexus/persistence/models/__init__.py`, `engine/src/nexus/persistence/migrations.py`, `engine/src/nexus/ops/backup_restore.py`.
- Admission/cycle: `engine/src/nexus/agents/persistence.py`, `engine/src/nexus/jobs/daily_research.py`, `engine/src/nexus/paper/agent_cycle.py`.
- Product read path: `engine/src/nexus/api/schemas/paper.py`, `engine/src/nexus/paper/read_service.py`, `engine/src/nexus/api/routes/paper.py`, `app/lib/engine/errors.ts`, `app/(protected)/paper/page.tsx`, `app/components/agent-paper-cycle.tsx`.
- Generated client files under `app/lib/generated/` are regenerated, never hand edited.
- Runtime/documentation: `engine/pyproject.toml`, `tests/e2e/agent-paper-cycle-real.spec.ts`, `docs/runbooks/nexus-paper-once.md`.

---

### Task 1: Append-only evaluation authority and database guards

**Files:**
- Create: `engine/alembic/versions/0062_agent_version_evaluation.py`
- Create: `engine/src/nexus/persistence/models/agent_versions.py`
- Create: `engine/tests/integration/test_agent_version_evaluation_migration.py`
- Modify: `engine/src/nexus/persistence/models/__init__.py`
- Modify: `engine/src/nexus/persistence/migrations.py`
- Modify: `engine/src/nexus/ops/backup_restore.py`
- Modify: `engine/tests/unit/ops/test_backup_restore.py`
- Modify: `engine/tests/helpers/migration_schema_expectations.py`

**Interfaces:**
- Produces `AgentVersionEvaluationRecord`, `AgentVersionCyclePairRecord`, and `AgentVersionEvaluationDecisionRecord`.
- Produces tables `agent_version_evaluations`, `agent_version_cycle_pairs`, and `agent_version_evaluation_decisions`.
- No service, CLI, cycle, Job, or model behavior changes in this task.

- [ ] **Step 1: Write failing migration-chain and ORM tests**

```python
def test_0062_declares_agent_version_authority() -> None:
    assert CURRENT_MIGRATION_HEAD == "0062_agent_version_evaluation"
    assert {
        AgentVersionEvaluationRecord.__tablename__,
        AgentVersionCyclePairRecord.__tablename__,
        AgentVersionEvaluationDecisionRecord.__tablename__,
    } == {
        "agent_version_evaluations",
        "agent_version_cycle_pairs",
        "agent_version_evaluation_decisions",
    }
```

Add clean `0061 -> 0062 -> 0061` round-trip coverage and assert downgrade raises SQLSTATE `P3D01` when any new table contains a row.

- [ ] **Step 2: Run focused tests and verify RED**

```bash
uv run --project engine pytest engine/tests/integration/test_agent_version_evaluation_migration.py engine/tests/unit/ops/test_backup_restore.py -q
```

Expected: failure because revision 0062, ORM records, and backup order do not exist.

- [ ] **Step 3: Add exact ORM records**

```python
class AgentVersionEvaluationRecord(ImmutableCreatedAt, Base):
    __tablename__ = "agent_version_evaluations"
    id: Mapped[UUID] = mapped_column(primary_key=True, default=new_uuid7)
    incumbent_account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    challenger_account_id: Mapped[UUID] = mapped_column(ForeignKey("paper_accounts.id"))
    instrument_id: Mapped[UUID] = mapped_column(ForeignKey("instruments.id"))
    incumbent_version_source_run_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_research_runs.id")
    )
    challenger_prompt_version_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_prompt_versions.id")
    )
    challenger_model_config_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_model_configs.id")
    )
    calendar_authority_id: Mapped[UUID] = mapped_column(
        ForeignKey("exchange_calendar_authorities.id")
    )
    incumbent_prompt_hash: Mapped[str] = mapped_column(String(64))
    incumbent_model_hash: Mapped[str] = mapped_column(String(64))
    challenger_prompt_hash: Mapped[str] = mapped_column(String(64))
    challenger_model_hash: Mapped[str] = mapped_column(String(64))
    train_slots: Mapped[list[datetime]] = mapped_column(ARRAY(UTCDateTime()))
    oos_slots: Mapped[list[datetime]] = mapped_column(ARRAY(UTCDateTime()))
    policy_version: Mapped[str] = mapped_column(String(64))
    policy_hash: Mapped[str] = mapped_column(String(64))
    authority_hash: Mapped[str] = mapped_column(String(64), unique=True)
```

Add pair and decision records with the spec fields. Enforce `phase IN ('train','oos')`, `status IN ('PASS','FAIL','UNKNOWN')`, canonical hashes, distinct accounts/runs/cycles, array cardinalities 4/20, and `policy_version='agent_version_evaluation_v1'`.

- [ ] **Step 4: Implement database triggers and fences**

The migration must:

1. advisory-lock registration and reject a second evaluation lacking a final decision;
2. require two distinct internal paper accounts with identical initial capital and permanently deny external-ID or broker-intent attachment;
3. verify AAPL through point-in-time alias authority;
4. freeze one complete XNYS calendar authority and verify every train/OOS slot is an in-session timestamp;
5. reject evaluation-account cycles outside the frozen slots or after finalization;
6. verify pair cycles/runs match registered accounts, slot, phase, instrument, frozen hashes, and `execution_locked=true`;
7. verify PASS selects the earliest OOS challenger run and FAIL/UNKNOWN selects the registered incumbent run;
8. make all three tables immutable;
9. refuse downgrade unless all three are empty.

- [ ] **Step 5: Update backup order and run GREEN**

```bash
uv run --project engine pytest engine/tests/integration/test_agent_version_evaluation_migration.py engine/tests/unit/ops/test_backup_restore.py -q
cd engine
uv run ruff check src/nexus/persistence/models/agent_versions.py alembic/versions/0062_agent_version_evaluation.py tests/integration/test_agent_version_evaluation_migration.py
uv run mypy src/nexus/persistence/models/agent_versions.py
```

- [ ] **Step 6: Commit Task 1**

```bash
git add engine/alembic/versions/0062_agent_version_evaluation.py engine/src/nexus/persistence/models/agent_versions.py engine/src/nexus/persistence/models/__init__.py engine/src/nexus/persistence/migrations.py engine/src/nexus/ops/backup_restore.py engine/tests/integration/test_agent_version_evaluation_migration.py engine/tests/unit/ops/test_backup_restore.py
git commit -m "feat(paper): add agent version evaluation authority"
```

---

### Task 2: Frozen v1 policy, registration, and isolated accounts

**Files:**
- Create: `engine/src/nexus/paper/agent_version_models.py`
- Create: `engine/src/nexus/paper/agent_version_evaluation.py`
- Create: `engine/tests/unit/paper/test_agent_version_policy.py`
- Create: `engine/tests/integration/test_agent_version_evaluation_pg.py`

**Interfaces:**
- Produces `AgentVersionEvaluationRequest`, `AgentVersionEvaluationRegistration`, `AgentVersionEvaluationProgress`, and `FrozenAgentVersionSelection(source_run_id, prompt_hash, model_hash)`.
- Produces `register_agent_version_evaluation(session, *, request, actor_id, idempotency_key, now) -> AgentVersionEvaluationRegistration`.
- Consumes existing `load_complete_prompt_authority`, `load_frozen_prompts`, `claim_or_lock`, `Account`, `Portfolio`, and `PaperAccountRecord`.

- [ ] **Step 1: Write policy and registration RED tests**

```python
def test_v1_policy_rejects_overlapping_windows() -> None:
    with pytest.raises(ValueError, match="AGENT_VERSION_WINDOW_INVALID"):
        AgentVersionEvaluationRequest(
            incumbent_version_source_run_id=INCUMBENT_RUN_ID,
            challenger_prompt_version_id=CHALLENGER_PROMPT_ID,
            challenger_model_config_id=MODEL_ID,
            train_slots=TRAIN_SLOTS,
            oos_slots=TRAIN_SLOTS + OOS_SLOTS[4:],
        )

async def test_registration_creates_equal_cash_accounts_and_no_job(
    session: AsyncSession,
    valid_request: AgentVersionEvaluationRequest,
) -> None:
    before_jobs = int(await session.scalar(select(func.count()).select_from(Job)) or 0)
    result = await register_agent_version_evaluation(
        session,
        request=valid_request,
        actor_id="test-agent-version-evaluation",
        idempotency_key="register-v1",
        now=NOW,
    )
    assert result.execution_locked is True
    assert result.model_jobs_created == 0
    assert int(await session.scalar(select(func.count()).select_from(Job)) or 0) == before_jobs
```

Also test non-AAPL, same identity, second open evaluation, invalid pricing, exact replay, idempotency conflict, and absence of external IDs/broker facts.

- [ ] **Step 2: Run tests and verify RED**

```bash
cd engine
uv run pytest tests/unit/paper/test_agent_version_policy.py tests/integration/test_agent_version_evaluation_pg.py -q
```

- [ ] **Step 3: Implement frozen policy objects and hashes**

```python
POLICY_VERSION = "agent_version_evaluation_v1"
TRAIN_SLOT_COUNT = 4
OOS_SLOT_COUNT = 20
MIN_TRAIN_SESSIONS = 2
MIN_OOS_SESSIONS = 10
MAX_OOS_SLOTS_PER_SESSION = 2
MIN_ACTION_DIVERGENCES = 3
MIN_CHALLENGER_FILLS = 1
MIN_EXCESS_RETURN = Decimal("0.005")
MIN_POSITIVE_EXCESS_SESSIONS = 7
MAX_DRAWDOWN_DEGRADATION = Decimal("0.0025")

class AgentVersionEvaluationRegistration(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    evaluation_id: UUID
    incumbent_account_id: UUID
    challenger_account_id: UUID
    authority_hash: str
    replayed: bool
    model_jobs_created: Literal[0] = 0
    execution_locked: Literal[True] = True
```

Hashes use compact sorted JSON, UTC `Z` timestamps, decimal strings, and exact ordered slots.

- [ ] **Step 4: Implement atomic registration**

In one transaction and idempotency claim: validate the incumbent's frozen prompts/model; load the challenger complete prompt authority/model; require a changed hash; resolve AAPL; validate XNYS slots; create two `provider='internal_paper_evaluation'` accounts, portfolios, and equal-capital cash-only paper accounts; then insert the evaluation. Do not create external IDs, cycles, runs, Jobs, outputs, orders, fills, or ledger facts.

- [ ] **Step 5: Run GREEN and commit Task 2**

```bash
cd engine
uv run pytest tests/unit/paper/test_agent_version_policy.py tests/integration/test_agent_version_evaluation_pg.py -q
uv run ruff check src/nexus/paper/agent_version_models.py src/nexus/paper/agent_version_evaluation.py tests/unit/paper/test_agent_version_policy.py tests/integration/test_agent_version_evaluation_pg.py
uv run mypy src/nexus/paper/agent_version_models.py src/nexus/paper/agent_version_evaluation.py
git add engine/src/nexus/paper/agent_version_models.py engine/src/nexus/paper/agent_version_evaluation.py engine/tests/unit/paper/test_agent_version_policy.py engine/tests/integration/test_agent_version_evaluation_pg.py
git commit -m "feat(paper): register isolated agent version evaluation"
```

---

### Task 3: Explicit version pin and recoverable dual-lane pairing

**Files:**
- Create: `engine/src/nexus/paper/agent_version_runner.py`
- Modify: `engine/src/nexus/agents/persistence.py`
- Modify: `engine/src/nexus/jobs/daily_research.py`
- Modify: `engine/src/nexus/paper/agent_cycle.py`
- Modify: `engine/tests/integration/test_agent_version_evaluation_pg.py`
- Modify: `engine/tests/integration/test_agent_paper_cycle_pg.py`
- Modify: `engine/tests/integration/test_agent_paper_cycle_e2e.py`

**Interfaces:**
- Produces `advance_agent_version_slot(session, *, evaluation_id, decision_slot_at, daily_limit_usd, now) -> AgentVersionSlotResult`.
- Produces `resume_agent_version_slot(session, *, evaluation_id, decision_slot_at, now) -> AgentVersionSlotResult`.
- `AgentVersionSlotResult` contains `evaluation_id`, `decision_slot_at`, `phase`, both cycle IDs, both run IDs, `status` (`prepared`, `running`, `paired`, or `incomparable`), `reason_code`, `replayed`, and literal `execution_locked=True`.
- Adds optional `version_source_run_id: UUID | None` to `admit_research_run`; it is mutually exclusive with latest-actor reuse and explicit prompt/model IDs.
- Produces `resolve_internal_agent_version_source(session, *, account_id, decision_slot_at) -> FrozenAgentVersionSelection`.

- [ ] **Step 1: Write RED tests for pinning and replay**

Cover: unrelated newer actor run cannot change the registered version; both cycles have equal slot/policies/sizing and start content; queued/running replay returns the same IDs; succeeded Job resumes original output; mismatched evidence/projection/version/slot/account prevents pair insertion; normal Stage 2 behavior remains compatible; no broker fact appears.

- [ ] **Step 2: Run tests and verify RED**

```bash
cd engine
uv run pytest tests/integration/test_agent_version_evaluation_pg.py tests/integration/test_agent_paper_cycle_pg.py tests/integration/test_agent_paper_cycle_e2e.py -q
```

- [ ] **Step 3: Add exact frozen-version loading**

```python
if sum((
    version_source_run_id is not None,
    reuse_latest_actor_agent_version,
    prompt_version_id is not None or model_config_id is not None,
)) != 1:
    raise ValueError("Agent version authority is ambiguous")

if version_source_run_id is not None:
    source_run = await session.get(AgentResearchRunRecord, version_source_run_id)
    prompts = None if source_run is None else await load_frozen_prompts(session, source_run)
    model = None if source_run is None else await load_frozen_model_config(session, source_run)
```

Do not mutate or copy historical runs. Keep the existing normal admission as compatibility fallback.

- [ ] **Step 4: Implement lane-aware native admission**

Use actors `local-agent-version-incumbent` and `local-agent-version-challenger`. Derive idempotency from evaluation, lane, slot, and instrument; keep `JobType.AGENT_PAPER_RESEARCH`. Each lane receives its own equal-start paper context while non-account evidence content hashes must match.

- [ ] **Step 5: Implement atomic prepare/resume and pair insertion**

Inspect both persisted cycle/job/output chains first. Create only missing exact-slot work; roll back both new admissions on conflict. Resume existing Jobs/outputs without enqueueing replacements. Insert the pair only after frozen versions, account-neutral starting projection, and non-account evidence compare equal.

- [ ] **Step 6: Run GREEN and commit Task 3**

```bash
cd engine
uv run pytest tests/integration/test_agent_version_evaluation_pg.py tests/integration/test_agent_paper_cycle_pg.py tests/integration/test_agent_paper_cycle_e2e.py -q
uv run ruff check src/nexus/agents/persistence.py src/nexus/jobs/daily_research.py src/nexus/paper/agent_cycle.py src/nexus/paper/agent_version_runner.py
uv run mypy src/nexus/agents/persistence.py src/nexus/jobs/daily_research.py src/nexus/paper/agent_cycle.py src/nexus/paper/agent_version_runner.py
git add engine/src/nexus/agents/persistence.py engine/src/nexus/jobs/daily_research.py engine/src/nexus/paper/agent_cycle.py engine/src/nexus/paper/agent_version_runner.py engine/tests/integration/test_agent_version_evaluation_pg.py engine/tests/integration/test_agent_paper_cycle_pg.py engine/tests/integration/test_agent_paper_cycle_e2e.py
git commit -m "feat(paper): run paired frozen agent versions"
```

---

### Task 4: Deterministic OOS finalization and future internal selection

**Files:**
- Modify: `engine/src/nexus/paper/agent_version_models.py`
- Modify: `engine/src/nexus/paper/agent_version_evaluation.py`
- Modify: `engine/src/nexus/paper/agent_cycle.py`
- Modify: `engine/tests/unit/paper/test_agent_version_policy.py`
- Modify: `engine/tests/integration/test_agent_version_evaluation_pg.py`

**Interfaces:**
- Produces `AgentVersionMetricFacts` with `oos_pair_count`, `action_divergence_count`, `challenger_fill_count`, `cumulative_excess_return`, `positive_excess_session_count`, `drawdown_degradation`, `hard_risk_violation_count`, `ledger_conserved`, `duplicate_execution_fact_count`, `cost_authority_complete`, `comparison_authority_complete`, and `authority_complete`.
- Produces `evaluate_agent_version_metrics(facts: AgentVersionMetricFacts) -> AgentVersionGateReport`.
- Produces `finalize_agent_version_evaluation(session, *, evaluation_id, now) -> AgentVersionEvaluationDecision`.
- Produces `load_selected_internal_agent_version(session, *, now) -> FrozenAgentVersionSelection | None`.
- `AgentVersionEvaluationDecision` contains `evaluation_id`, `status`, `selected_version_source_run_id`, `reason_code`, `metric_hash`, `decision_hash`, `replayed`, and literal `execution_locked=True`.

- [ ] **Step 1: Write the complete policy truth table as RED tests**

```python
PASS_FACTS = AgentVersionMetricFacts(
    oos_pair_count=20,
    action_divergence_count=3,
    challenger_fill_count=1,
    cumulative_excess_return=Decimal("0.005"),
    positive_excess_session_count=7,
    drawdown_degradation=Decimal("0.0025"),
    hard_risk_violation_count=0,
    ledger_conserved=True,
    duplicate_execution_fact_count=0,
    cost_authority_complete=True,
    comparison_authority_complete=True,
    authority_complete=True,
)

def test_agent_version_v1_passes_only_complete_gate() -> None:
    report = evaluate_agent_version_metrics(PASS_FACTS)
    assert (report.status, report.reason_code) == ("PASS", "OOS_IMPROVEMENT_PROVEN")

def test_agent_version_v1_missing_pair_is_unknown() -> None:
    report = evaluate_agent_version_metrics(replace(PASS_FACTS, oos_pair_count=19))
    assert (report.status, report.reason_code) == ("UNKNOWN", "OOS_PAIR_INCOMPLETE")

def test_agent_version_v1_ledger_imbalance_fails() -> None:
    report = evaluate_agent_version_metrics(replace(PASS_FACTS, ledger_conserved=False))
    assert (report.status, report.reason_code) == ("FAIL", "LEDGER_CONSERVATION_FAILED")
```

Add equally explicit tests for insufficient divergence, no challenger fill, duplicate execution facts, return below `0.005`, fewer than seven positive sessions, excessive drawdown degradation, incomplete authority, incomplete cost authority, and missing `hold_current`/`baseline_v1`/SPY comparison authority.

Also prove train facts never enter OOS metrics, cost-policy mismatch is UNKNOWN, early finalization raises `AGENT_VERSION_WINDOW_OPEN`, and repeated finalization returns the original row.

- [ ] **Step 2: Run tests and verify RED**

```bash
cd engine
uv run pytest tests/unit/paper/test_agent_version_policy.py tests/integration/test_agent_version_evaluation_pg.py -q
```

- [ ] **Step 3: Implement Decimal-only metrics and fixed gates**

Load only persisted performance snapshots, fills, fees, risk evaluations, terminal cycle facts, pair hashes, and the existing `hold_current`/`baseline_v1`/SPY comparison authorities. Reject binary floats. Calculate both lane metrics, after-cost per-session excess, cumulative time-weighted return, drawdown degradation, sizing divergence, challenger fills, duplicate execution facts, ledger conservation, and all three external comparison rows in a fixed field order. A missing or dimensionally incompatible comparison is `UNKNOWN`, not a zero or omitted baseline.

- [ ] **Step 4: Persist one final decision and wire selection**

PASS selects the earliest-slot OOS challenger run; FAIL/UNKNOWN selects the registered incumbent run. Normal future internal Agent Paper admission may read only the latest valid PASS. The selected run must still pass frozen prompt and model validation; invalid authority fails closed and never falls back to a mutable latest prompt.

- [ ] **Step 5: Run GREEN and commit Task 4**

```bash
cd engine
uv run pytest tests/unit/paper/test_agent_version_policy.py tests/integration/test_agent_version_evaluation_pg.py tests/integration/test_agent_paper_cycle_pg.py -q
uv run ruff check src/nexus/paper/agent_version_models.py src/nexus/paper/agent_version_evaluation.py src/nexus/paper/agent_cycle.py
uv run mypy src/nexus/paper/agent_version_models.py src/nexus/paper/agent_version_evaluation.py src/nexus/paper/agent_cycle.py
git add engine/src/nexus/paper/agent_version_models.py engine/src/nexus/paper/agent_version_evaluation.py engine/src/nexus/paper/agent_cycle.py engine/tests/unit/paper/test_agent_version_policy.py engine/tests/integration/test_agent_version_evaluation_pg.py
git commit -m "feat(paper): finalize agent version evidence"
```

---

### Task 5: Local CLI and read-only API/BFF/page projection

**Files:**
- Create: `engine/src/nexus/ops/agent_version_evaluation.py`
- Create: `engine/tests/unit/ops/test_agent_version_evaluation.py`
- Create: `app/components/agent-version-evaluation.tsx`
- Modify: `engine/pyproject.toml`
- Modify: `engine/src/nexus/api/schemas/paper.py`
- Modify: `engine/src/nexus/paper/read_service.py`
- Modify: `engine/src/nexus/api/routes/paper.py`
- Modify: `engine/tests/unit/api/test_paper_contracts.py`
- Modify: `engine/tests/integration/test_paper_api.py`
- Modify: `engine/tests/contract/test_agent_paper_no_broker.py`
- Modify: `app/lib/engine/errors.ts`
- Modify: `app/(protected)/paper/page.tsx`
- Modify: `app/components/agent-paper-cycle.tsx`
- Modify: `tests/unit/paper/runtime-boundary.test.ts`
- Modify: `tests/unit/paper/page.test.tsx`
- Modify: `tests/e2e/paper.spec.ts`
- Modify: generated files under `app/lib/generated/`

**Interfaces:**
- CLI: `nexus-engine-agent-version-evaluation {register,advance,resume,finalize,status}` with local PostgreSQL validation and JSON-only stdout.
- API: add `version_evaluation: AgentVersionEvaluationResponse | null` to `AgentPaperCyclePageResponse`; no write route.
- Page: one read-only evaluation card with `execution_locked=true`, `broker_write=false`, and `live_write=false`.

- [ ] **Step 1: Write CLI, schema, validator, and component RED tests**

Assert `status` is read-only; `register` creates no Job; `advance` rejects unregistered slots; `resume` cannot create replacement Jobs; the API exposes no prompt text/account ID/create/promote/broker/live action; malformed counts/lifecycle/lock/PASS facts fail closed; and the page no longer says challenger unavailable after registration.

- [ ] **Step 2: Run tests and verify RED**

```bash
cd engine
uv run pytest tests/unit/ops/test_agent_version_evaluation.py tests/unit/api/test_paper_contracts.py tests/integration/test_paper_api.py tests/contract/test_agent_paper_no_broker.py -q
cd ..
npm run test:unit -- tests/unit/paper/runtime-boundary.test.ts tests/unit/paper/page.test.tsx
```

- [ ] **Step 3: Implement the local-only CLI**

Reuse `_validate_local_database` semantics from `nexus.ops.agent_paper_once`. Read secrets only from protected local configuration, never arguments. The CLI delegates to Tasks 2–4 and contains no model transport or provider client.

- [ ] **Step 4: Add bounded read schema and projection**

```python
class AgentVersionEvaluationResponse(_StrictSchema):
    evaluation_id: UUID
    status: Literal["observing", "PASS", "FAIL", "UNKNOWN"]
    incumbent_prompt_hash: str
    incumbent_model_hash: str
    challenger_prompt_hash: str
    challenger_model_hash: str
    train_completed: int = Field(ge=0, le=4)
    oos_completed: int = Field(ge=0, le=20)
    oos_missing: int = Field(ge=0, le=20)
    oos_incomparable: int = Field(ge=0, le=20)
    incumbent_metrics: AgentVersionLaneMetricsResponse | None
    challenger_metrics: AgentVersionLaneMetricsResponse | None
    comparisons: tuple[AgentVersionComparisonResponse, ...] = Field(max_length=3)
    gates: tuple[AgentVersionGateResponse, ...] = Field(max_length=8)
    reason_code: str
    execution_locked: Literal[True]
    broker_write: Literal[False]
    live_write: Literal[False]
```

`AgentVersionLaneMetricsResponse` contains decimal-string NAV, return, max drawdown, turnover, total cost, order count, fill count, and hard-risk count. `AgentVersionComparisonResponse.key` is limited to `hold_current`, `baseline_v1`, and `market_benchmark`; each row has an explicit READY/UNKNOWN state and reason code. `AgentVersionGateResponse` contains a fixed gate key, PASS/FAIL/UNKNOWN status, observed decimal strings, threshold decimal strings, and reason code. Project only the latest open evaluation or latest final decision. Do not expose account IDs, prompt content, model pricing, or actions.

- [ ] **Step 5: Regenerate client and implement fail-closed UI**

```bash
npm run generate:client
npm run check:generated
```

Render the card on `/paper`, retain version history, and remove `VERSION_CHALLENGER_UNAVAILABLE` only when the evaluation projection proves registration. Add no button or mutation route.

- [ ] **Step 6: Run GREEN and commit Task 5**

```bash
cd engine
uv run pytest tests/unit/ops/test_agent_version_evaluation.py tests/unit/api/test_paper_contracts.py tests/integration/test_paper_api.py tests/contract/test_agent_paper_no_broker.py -q
cd ..
npm run test:unit -- tests/unit/paper/runtime-boundary.test.ts tests/unit/paper/page.test.tsx
npm run typecheck
npm run check:generated
git add engine/pyproject.toml engine/src/nexus/ops/agent_version_evaluation.py engine/src/nexus/api/schemas/paper.py engine/src/nexus/paper/read_service.py engine/src/nexus/api/routes/paper.py engine/tests/unit/ops/test_agent_version_evaluation.py engine/tests/unit/api/test_paper_contracts.py engine/tests/integration/test_paper_api.py engine/tests/contract/test_agent_paper_no_broker.py app/lib/generated app/lib/engine/errors.ts 'app/(protected)/paper/page.tsx' app/components/agent-paper-cycle.tsx app/components/agent-version-evaluation.tsx tests/unit/paper/runtime-boundary.test.ts tests/unit/paper/page.test.tsx tests/e2e/paper.spec.ts
git commit -m "feat(paper): expose agent version evaluation"
```

---

### Task 6: Fixture acceptance, runtime readback, and real-observation handoff

**Files:**
- Modify: `tests/e2e/agent-paper-cycle-real.spec.ts`
- Modify: `docs/runbooks/nexus-paper-once.md`
- Modify: `docs/plans/2026-09-12-nexus-simulation-evolution-roadmap.md` only to record verified evidence, never to weaken gates.

**Interfaces:**
- Produces no new runtime interface.
- Produces fixture proof for no-trade, full fill, partial fill, risk rejection, unavailable, recovery, UNKNOWN, FAIL, and PASS.
- Produces one bounded scheduled real observation only after persisted-state preflight proves no prior matching work.

- [ ] **Step 1: Add end-to-end fixture acceptance**

Prove both lanes independently traverse:

```text
evaluation -> cycle -> run -> Job -> output -> decision -> batch
-> order -> fill -> ledger -> performance -> pair -> final decision
```

Assert account isolation, ledger conservation, idempotent replay, no duplicate facts, execution lock, zero broker-table writes, all three comparison rows, and DB -> API -> BFF -> `/paper` readback.

- [ ] **Step 2: Run the proportional full suite**

```bash
cd engine
uv run pytest tests/integration/test_agent_version_evaluation_migration.py tests/integration/test_agent_version_evaluation_pg.py tests/integration/test_agent_paper_cycle_pg.py tests/integration/test_agent_paper_cycle_e2e.py tests/integration/test_paper_api.py tests/unit/paper/test_agent_version_policy.py tests/unit/ops/test_agent_version_evaluation.py tests/unit/api/test_paper_contracts.py tests/contract/test_agent_paper_no_broker.py -q
uv run ruff check src tests/integration/test_agent_version_evaluation_migration.py tests/integration/test_agent_version_evaluation_pg.py tests/unit/paper/test_agent_version_policy.py tests/unit/ops/test_agent_version_evaluation.py
uv run mypy src/nexus
cd ..
npm run test:unit
npm run typecheck
npm run check:generated
npm run build
```

If full lint fails only under existing untracked `outputs/acceptance/**`, run changed-file ESLint and report that unrelated pre-existing failure; do not edit or delete those files.

- [ ] **Step 3: Perform committed-snapshot runtime readback**

Rebuild/restart only the API/frontend processes owned by this task. Verify authenticated Engine API, BFF, `/paper`, and browser console. Confirm evaluation empty/fixture state, version history, execution lock, and zero broker/live commands. Do not restart the existing worker just for readback.

- [ ] **Step 4: Write persisted-state-first runbook steps**

Document checks in this order: evaluation decision/open state; both cycles per slot; each run's native Job and terminal output; pair/decision facts; model budget; broker binding/command counts; execution lock. Only missing exact-slot work may be admitted. Timeout calls `status` or `resume`, never blind `register`/`advance`.

- [ ] **Step 5: Commit acceptance and runbook**

```bash
git add tests/e2e/agent-paper-cycle-real.spec.ts docs/runbooks/nexus-paper-once.md docs/plans/2026-09-12-nexus-simulation-evolution-roadmap.md
git commit -m "test(paper): verify agent version evaluation"
```

- [ ] **Step 6: Schedule real evidence without blocking Stages 4 and 5**

After code, migration, fixture suite, and readback are green, inspect the business database. If no matching evaluation exists, register one AAPL evaluation and schedule only its frozen 4 train and 20 OOS slots. The scheduled task stays quiet when unchanged, uses `status/resume` before creation, stops on final decision or safety failure, and never calls broker/live endpoints. Continue Stage 4 and Stage 5 readiness work while the 10-session OOS evidence accumulates.

---

## Plan Completion Check

Before claiming Stage 3 implementation complete, verify from current state:

- migration head is 0062 and authenticated API readiness is healthy;
- isolated evaluation accounts exist only for a registered evaluation;
- no evaluation work or model call was duplicated;
- fixture order/fill/ledger proof passes even if live AAPL remains no-trade;
- incomplete live evidence is observing/UNKNOWN, never PASS;
- only persisted PASS selects a future internal Agent version;
- `execution_locked=true`, broker command count is zero, live order count is zero;
- Stage 4 and Stage 5 work is not blocked on the 10-session OOS clock.
