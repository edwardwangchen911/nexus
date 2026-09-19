# Portfolio Management First Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the bounded multi-candidate portfolio-decision flow from explicit candidate scope through portfolio synthesis, deterministic target activation, existing internal-paper execution, read-only reconciliation, and the existing NEXUS pages.

**Architecture:** Preserve per-security Agent research and the current paper execution/ledger stack. Add an explicit candidate batch, one bounded portfolio-synthesis run, a deterministic `portfolio_allocation_v1` proposal, an immutable active-target version, and a target-aware order batch; all write coordination is account-locked and fail-closed. Reads project persisted facts into the existing decisions, paper, workspace, and portfolio routes without triggering providers, models, cancellation, or orders.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, SQLAlchemy 2 async, PostgreSQL/Alembic, pytest, Ruff, Mypy, React/TypeScript, vinext, Vitest, Playwright.

**Spec:** `docs/superpowers/specs/2026-09-18-portfolio-management-first-slice-design.md`

## Global Constraints

- Keep `execution_locked=true`; do not add broker-simulation writes, live writes, credential access, deployment, or `.openai/hosting.json` changes.
- Normal portfolio-management cycles require an explicit immutable candidate batch of 1–5 approved USD common stocks/ETFs; no AAPL fallback, watchlist promotion, or market scan.
- `portfolio_allocation_v1`: core 10%, tactical 5%, single-name target cap 15%, target cash floor 20%, at most two additions/increases per cycle, 5 percentage-point change bands, 10% one-way turnover cap, 2 percentage-point minimum target difference, USD 100 minimum notional, and 3 percentage-point same-target rebalance drift.
- Existing hard risk remains independent and wins when stricter, including its 10% minimum cash, 15% single-name limit, 30% sector limit, and USD 100 minimum buy notional.
- Run once per XNYS session at 10:00 `America/New_York`; derive UTC from the frozen calendar. Security research is valid for seven calendar days; a portfolio proposal expires at the session close in which it is formed.
- Initial target creation requires one separately persisted, user-approved internal-paper projection. Policy approval does not approve an unseen projection.
- Agent outputs investment choice and strict priority only; deterministic code owns target weights, quantities, risk, and execution.
- At most one active target and one resource-occupying adjustment plan per account. A waiting proposal never activates automatically after the preceding plan clears.
- Browser/BFF reads never call providers, models, cancel, order, target activation, or retry logic.
- Preserve every legacy AAPL cycle/decision/batch/order/fill/ledger record without inferred backfill.
- Each task uses TDD, runs its focused tests, and commits only its own files. Do not modify user-owned dirty files in the original `main` worktree.

## File and Interface Map

### New engine modules

- `engine/src/nexus/paper/portfolio_policy.py`: immutable policy contract and canonical hash.
- `engine/src/nexus/paper/candidate_batch.py`: create/read explicit candidate batches.
- `engine/src/nexus/agents/portfolio_synthesis_contracts.py`: strict model input/output schemas and canonical hashes.
- `engine/src/nexus/agents/portfolio_synthesis_persistence.py`: synthesis admission, budget reservation, terminal persistence, and replay.
- `engine/src/nexus/agents/portfolio_synthesis_worker.py`: one-job worker using existing Codex runtime plumbing.
- `engine/src/nexus/agents/prompts/portfolio-management-synthesis-v1.md`: bounded choice-only prompt.
- `engine/src/nexus/paper/portfolio_proposal.py`: deterministic allocation from active target + admitted synthesis.
- `engine/src/nexus/paper/target_service.py`: initial baseline approval, target activation, occupancy projection, and target-aware batch coordination.
- `engine/alembic/versions/0065_portfolio_management_first_slice.py`: additive schema and guards.

### Primary modified engine modules

- `engine/src/nexus/persistence/models/paper.py`, `engine/src/nexus/persistence/models/agents.py`
- `engine/src/nexus/persistence/migrations.py`
- `engine/src/nexus/jobs/models.py`, `engine/src/nexus/worker.py`
- `engine/src/nexus/ops/schwab_market_stream.py`, `engine/src/nexus/ops/agent_risk_authority.py`
- `engine/src/nexus/ops/agent_paper_once.py`, `engine/src/nexus/ops/agent_paper_loop.py`
- `engine/src/nexus/paper/agent_cycle_models.py`, `agent_cycle_authority.py`, `agent_cycle.py`, `agent_decision_store.py`, `agent_version_runner.py`, `service.py`, `read_service.py`
- `engine/src/nexus/api/schemas/paper.py`, `engine/src/nexus/api/routes/paper.py`

### Frontend files

- `app/lib/paper/bff.ts`
- `app/api/v1/paper/cycles/[cycleId]/route.ts`
- `app/api/v1/paper/portfolio/route.ts`
- Create `app/api/v1/paper/plans/[planId]/route.ts`
- Create `app/api/v1/paper/orders/[orderId]/cancel-result/route.ts`
- `app/(protected)/decisions/page.tsx`, `app/(protected)/decisions/[cycleId]/page.tsx`
- `app/(protected)/paper/page.tsx`, `app/(protected)/workspace/page.tsx`, `app/(protected)/portfolio/page.tsx`
- Create focused components under `app/components/decisions/` and `app/components/paper/` rather than expanding page files with business calculations.

---

### Task 1: Freeze `portfolio_allocation_v1`

**Files:**
- Create: `engine/src/nexus/paper/portfolio_policy.py`
- Create: `engine/tests/unit/paper/test_portfolio_policy.py`

**Interfaces:**
- Produces: `PortfolioAllocationPolicyV1`, `PORTFOLIO_ALLOCATION_V1`, `PORTFOLIO_ALLOCATION_POLICY_HASH`, `canonical_portfolio_allocation_policy_hash(policy) -> str`.
- Consumes: exact-decimal validation pattern from `engine/src/nexus/risk/policy.py`.

- [ ] **Step 1: Write failing exact-value and float-rejection tests**

```python
def test_portfolio_allocation_v1_is_exact_and_content_addressed() -> None:
    policy = PORTFOLIO_ALLOCATION_V1
    assert policy.core_target == Decimal("0.10")
    assert policy.tactical_target == Decimal("0.05")
    assert policy.target_cash_floor == Decimal("0.20")
    assert policy.single_name_target_cap == Decimal("0.15")
    assert policy.max_increases == 2
    assert policy.adjustment_step == Decimal("0.05")
    assert policy.turnover_cap == Decimal("0.10")
    assert policy.minimum_target_delta == Decimal("0.02")
    assert policy.minimum_notional_usd == Decimal("100")
    assert policy.rebalance_drift == Decimal("0.03")
    assert canonical_portfolio_allocation_policy_hash(policy) == PORTFOLIO_ALLOCATION_POLICY_HASH

def test_portfolio_allocation_policy_rejects_binary_float() -> None:
    with pytest.raises(TypeError):
        PortfolioAllocationPolicyV1(core_target=0.10)
```

- [ ] **Step 2: Run the focused test and verify the import failure**

Run: `uv run pytest engine/tests/unit/paper/test_portfolio_policy.py -q --import-mode=importlib`
Expected: FAIL because `nexus.paper.portfolio_policy` does not exist.

- [ ] **Step 3: Implement the frozen policy and canonical hash**

```python
class PortfolioAllocationPolicyV1(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    version: Literal["portfolio_allocation_v1"] = "portfolio_allocation_v1"
    core_target: Decimal = Decimal("0.10")
    tactical_target: Decimal = Decimal("0.05")
    target_cash_floor: Decimal = Decimal("0.20")
    single_name_target_cap: Decimal = Decimal("0.15")
    max_increases: int = 2
    adjustment_step: Decimal = Decimal("0.05")
    turnover_cap: Decimal = Decimal("0.10")
    minimum_target_delta: Decimal = Decimal("0.02")
    minimum_notional_usd: Decimal = Decimal("100")
    rebalance_drift: Decimal = Decimal("0.03")
    research_valid_days: int = 7
    decision_local_time: time = time(10, 0)
```

Canonicalize every decimal as 12-place text, include the schema/version and local decision time, SHA-256 the sorted compact JSON, and reject binary floats before Pydantic coercion.

- [ ] **Step 4: Run focused tests, Ruff, and Mypy for the module**

Run: `uv run pytest engine/tests/unit/paper/test_portfolio_policy.py -q --import-mode=importlib && uv run ruff check engine/src/nexus/paper/portfolio_policy.py engine/tests/unit/paper/test_portfolio_policy.py && uv run mypy engine/src/nexus/paper/portfolio_policy.py`
Expected: PASS with zero failures.

- [ ] **Step 5: Commit**

```bash
git add engine/src/nexus/paper/portfolio_policy.py engine/tests/unit/paper/test_portfolio_policy.py
git commit -m "feat: freeze portfolio allocation policy v1"
```

### Task 2: Add additive persistence and database guards

**Files:**
- Create: `engine/alembic/versions/0065_portfolio_management_first_slice.py`
- Modify: `engine/src/nexus/persistence/models/paper.py`
- Modify: `engine/src/nexus/persistence/models/agents.py`
- Modify: `engine/src/nexus/persistence/migrations.py`
- Modify: `engine/tests/helpers/migration_schema_expectations.py`
- Create: `engine/tests/integration/test_portfolio_management_migration.py`
- Modify: `engine/tests/integration/test_migration.py`

**Interfaces:**
- Produces ORM records: `AgentPaperCandidateBatchRecord`, `AgentPortfolioSynthesisRunRecord`, `AgentPortfolioSynthesisOutputRecord`, `AgentPortfolioSynthesisBudgetReservationRecord`, `PaperInitialTargetApprovalRecord`, `PaperTargetVersionRecord`.
- Adds nullable FKs: `AgentPaperCycleRecord.candidate_batch_id`, `StrategyDecisionRecord.portfolio_synthesis_output_id`, `PaperOrderBatchRecord.target_version_id`.
- Migration head becomes `0065_portfolio_management_first_slice`.

- [ ] **Step 1: Write migration tests before the migration**

```python
async def test_0065_adds_portfolio_management_facts(
    migrated_database_url: str,
) -> None:
    engine = create_async_engine(migrated_database_url)
    async with engine.connect() as connection:
        tables, columns = await connection.run_sync(
            lambda sync_connection: (
                set(inspect(sync_connection).get_table_names()),
                {
                    table: {
                        column["name"]: column
                        for column in inspect(sync_connection).get_columns(table)
                    }
                    for table in (
                        "agent_paper_cycles",
                        "strategy_decisions",
                        "paper_order_batches",
                    )
                },
            )
        )
    await engine.dispose()
    assert {
        "agent_paper_candidate_batches",
        "agent_portfolio_synthesis_runs",
        "agent_portfolio_synthesis_outputs",
        "agent_portfolio_synthesis_budget_reservations",
        "paper_initial_target_approvals",
        "paper_target_versions",
    } <= tables
    assert columns["agent_paper_cycles"]["candidate_batch_id"]["nullable"] is True
    assert columns["strategy_decisions"]["portfolio_synthesis_output_id"]["nullable"] is True
    assert columns["paper_order_batches"]["target_version_id"]["nullable"] is True
```

Add guard cases proving: candidate arrays are 1–5 unique UUIDs; one initial approval per account; target sequence is unique per account; one decision activates once; previous target cannot cross accounts; completed synthesis is required by a new portfolio proposal; and legacy rows with all new FKs null still insert/read.

- [ ] **Step 2: Run migration tests and verify missing revision failure**

Run: `uv run pytest engine/tests/integration/test_portfolio_management_migration.py engine/tests/integration/test_migration.py -q --import-mode=importlib`
Expected: FAIL because revision `0065_portfolio_management_first_slice` and the ORM models do not exist.

- [ ] **Step 3: Implement the additive migration and ORM models**

Use PostgreSQL `ARRAY(UUID(as_uuid=True))` for canonical candidate IDs, JSONB only for frozen synthesis input/output documents, composite account/portfolio FKs where existing tables expose them, canonical 64-hex checks for all hashes, UTC timestamps, and unique constraints matching the design. Add append-only database triggers using the same helper pattern as the existing immutable Agent/Paper tables.

The target table must not make `(account_id, target_content_hash)` unique: a later legitimate return to an older allocation is a new version. Instead enforce `(account_id, version_seq)`, unique `strategy_decision_id`, and unique non-null `previous_target_version_id`.

- [ ] **Step 4: Verify upgrade, model parity, and isolated downgrade**

Run: `uv run pytest engine/tests/integration/test_portfolio_management_migration.py engine/tests/integration/test_migration.py engine/tests/integration/test_paper_db_guards.py -q --import-mode=importlib`
Expected: PASS, including downgrade only while new tables are empty.

- [ ] **Step 5: Run static checks and commit**

Run: `uv run ruff check engine/alembic/versions/0065_portfolio_management_first_slice.py engine/src/nexus/persistence engine/tests/integration/test_portfolio_management_migration.py && uv run mypy engine/src/nexus/persistence`
Expected: PASS.

```bash
git add engine/alembic/versions/0065_portfolio_management_first_slice.py engine/src/nexus/persistence engine/tests/helpers/migration_schema_expectations.py engine/tests/integration/test_portfolio_management_migration.py engine/tests/integration/test_migration.py
git commit -m "feat: add portfolio management persistence"
```

### Task 3: Create explicit candidate batches

**Files:**
- Create: `engine/src/nexus/paper/candidate_batch.py`
- Modify: `engine/src/nexus/ops/agent_paper_once.py`
- Create: `engine/tests/unit/paper/test_candidate_batch.py`
- Modify: `engine/tests/unit/ops/test_agent_paper_once.py`
- Create: `engine/tests/integration/test_agent_paper_candidate_batch_pg.py`

**Interfaces:**
- Produces: `CandidateBatchInput`, `CandidateBatchResult`, `create_agent_paper_candidate_batch(session, *, account_id, universe_snapshot_id, instrument_ids, idempotency_key, created_at)`, `load_agent_paper_candidate_batch(...)`.
- CLI: `nexus-engine-agent-paper-once prepare-candidates --account-id ... --universe-snapshot-id ... --instrument-id ... --idempotency-key ...`.

- [ ] **Step 1: Write unit tests for canonicalization and bounds**

```python
def test_candidate_batch_input_sorts_and_deduplicates_only_by_rejection() -> None:
    with pytest.raises(ValueError, match="CANDIDATE_BATCH_INVALID"):
        CandidateBatchInput(instrument_ids=(AAPL_ID, AAPL_ID))
    value = CandidateBatchInput(instrument_ids=(MSFT_ID, AAPL_ID))
    assert value.instrument_ids == tuple(sorted((MSFT_ID, AAPL_ID), key=lambda item: item.int))

@pytest.mark.parametrize("count", [0, 6])
def test_candidate_batch_input_is_bounded(count: int) -> None:
    with pytest.raises(ValueError, match="CANDIDATE_BATCH_INVALID"):
        CandidateBatchInput(instrument_ids=tuple(uuid4() for _ in range(count)))
```

- [ ] **Step 2: Write PostgreSQL tests for membership and idempotency**

Assert exact replay for the same actor/idempotency/content; conflict for the same key with different candidates; rejection for wrong account capability, non-USD, unsupported asset type, missing/expired alias, or instrument outside the bound universe snapshot. Assert no provider Job or model run is created.

- [ ] **Step 3: Run tests and verify failure**

Run: `uv run pytest engine/tests/unit/paper/test_candidate_batch.py engine/tests/integration/test_agent_paper_candidate_batch_pg.py -q --import-mode=importlib`
Expected: FAIL because the service is absent.

- [ ] **Step 4: Implement the service and CLI command**

Implement the exact interface `create_agent_paper_candidate_batch(session, *, account_id: UUID, universe_snapshot_id: UUID, instrument_ids: tuple[UUID, ...], idempotency_key: str, created_at: datetime) -> CandidateBatchResult`. Validate and canonicalize through `CandidateBatchInput`, acquire the account advisory lock, replay or reject the existing `IdempotencyRecord`, persist the immutable batch, flush, then persist the terminal idempotency response in the same transaction.

Use the route namespace `agent-paper-candidate-batch-v1`; hash normalized IDs, account, universe snapshot, source, and schema. Do not accept ticker text in the domain service; CLI callers must resolve and display instrument identities before creation.

- [ ] **Step 5: Run focused tests and commit**

Run: `uv run pytest engine/tests/unit/paper/test_candidate_batch.py engine/tests/unit/ops/test_agent_paper_once.py engine/tests/integration/test_agent_paper_candidate_batch_pg.py -q --import-mode=importlib && uv run ruff check engine/src/nexus/paper/candidate_batch.py engine/src/nexus/ops/agent_paper_once.py`
Expected: PASS.

```bash
git add engine/src/nexus/paper/candidate_batch.py engine/src/nexus/ops/agent_paper_once.py engine/tests/unit/paper/test_candidate_batch.py engine/tests/unit/ops/test_agent_paper_once.py engine/tests/integration/test_agent_paper_candidate_batch_pg.py
git commit -m "feat: add explicit agent paper candidate batches"
```

### Task 4: Generalize the bounded provider and manifest chain

**Files:**
- Modify: `engine/src/nexus/ops/schwab_market_stream.py`
- Modify: `engine/src/nexus/ops/agent_risk_authority.py`
- Modify: `engine/src/nexus/paper/agent_cycle_models.py`
- Modify: `engine/src/nexus/paper/agent_cycle_authority.py`
- Modify: `engine/tests/unit/ops/test_agent_risk_authority.py`
- Modify: `engine/tests/unit/paper/test_agent_cycle.py`
- Modify: `engine/tests/integration/test_agent_paper_trigger_reservation_pg.py`
- Modify: `engine/tests/integration/test_agent_paper_cycle_pg.py`

**Interfaces:**
- Produces: `capture_and_import_schwab_level_one_batch(..., bindings: Mapping[str, UUID])` and `collect_source_proofs_batch(..., candidates: tuple[RiskCandidate, ...])`.
- Changes: `prepare_candidates(session, *, authority, requested_instrument_ids)` and manifest validation from exactly one AAPL to 1–5 exact candidates.

- [ ] **Step 1: Add failing multi-symbol capture tests**

```python
result = await capture_and_import_schwab_level_one_batch(
    session,
    store,
    repo_root=repo_root,
    client=client,
    bindings={"AAPL": AAPL_ID, "MSFT": MSFT_ID},
    capture=fake_complete_capture,
)
assert set(result.quote_ids) == {"AAPL", "MSFT"}
```

Add a second test where MSFT has no qualified quote and assert `PROVIDER_MARKET_STREAM_INCOMPLETE` with zero imported decision authority for the batch.

- [ ] **Step 2: Add failing shared-risk-source tests**

Record HTTP calls and assert one Nasdaq listing fetch, one Nasdaq halt fetch, and one bounded corporate-action collection per instrument. Assert the returned risk authorities each include the correct instrument in `required_instrument_ids`.

- [ ] **Step 3: Add failing manifest tests**

Assert two canonical entries validate; duplicate IDs/symbols, a symbol/alias mismatch, a candidate absent from the requested batch, stale quote, missing research snapshot, or missing one of three risk authorities fails the whole batch without silently dropping a candidate.

- [ ] **Step 4: Run the focused tests and verify single-AAPL assumptions fail**

Run: `uv run pytest engine/tests/unit/ops/test_agent_risk_authority.py engine/tests/unit/paper/test_agent_cycle.py engine/tests/integration/test_agent_paper_cycle_pg.py -q --import-mode=importlib`
Expected: FAIL on the current hard-coded AAPL wrapper and `len(manifest) != 1` validation.

- [ ] **Step 5: Implement batch capture, shared-source parsing, and exact manifest validation**

Implement the exact interface `prepare_candidates(session, *, authority: CycleAuthority, requested_instrument_ids: tuple[UUID, ...]) -> tuple[CandidateManifestEntry, ...]`. Load aliases and universe memberships for the exact requested IDs in one bounded query, then load each candidate's research/quote/risk facts, reject the entire batch on any missing or mismatched fact, and return entries sorted by instrument UUID.

Require `1 <= len <= 5`, exact set equality with the requested batch, a current valid ticker alias, approved universe membership, research snapshot, prior daily authority, fresh Schwab quote/artifact, and three exact risk authorities per candidate. Keep legacy manifest parsing unchanged.

- [ ] **Step 6: Run focused tests, static checks, and commit**

Run: `uv run pytest engine/tests/unit/ops/test_agent_risk_authority.py engine/tests/unit/paper/test_agent_cycle.py engine/tests/integration/test_agent_paper_trigger_reservation_pg.py engine/tests/integration/test_agent_paper_cycle_pg.py -q --import-mode=importlib && uv run ruff check engine/src/nexus/ops engine/src/nexus/paper/agent_cycle_models.py engine/src/nexus/paper/agent_cycle_authority.py`
Expected: PASS.

```bash
git add engine/src/nexus/ops/schwab_market_stream.py engine/src/nexus/ops/agent_risk_authority.py engine/src/nexus/paper/agent_cycle_models.py engine/src/nexus/paper/agent_cycle_authority.py engine/tests/unit/ops/test_agent_risk_authority.py engine/tests/unit/paper/test_agent_cycle.py engine/tests/integration/test_agent_paper_trigger_reservation_pg.py engine/tests/integration/test_agent_paper_cycle_pg.py
git commit -m "feat: generalize agent paper candidate evidence"
```

### Task 5: Wire explicit candidates into the runner without changing AAPL evaluation

**Files:**
- Modify: `engine/src/nexus/ops/agent_paper_once.py`
- Modify: `engine/src/nexus/ops/agent_paper_loop.py`
- Modify: `engine/src/nexus/paper/agent_cycle.py`
- Modify: `engine/src/nexus/paper/agent_version_runner.py`
- Modify: `engine/tests/unit/ops/test_agent_paper_once.py`
- Modify: `engine/tests/unit/ops/test_agent_paper_loop.py`
- Modify: `engine/tests/integration/test_agent_paper_cycle_e2e.py`
- Modify: `engine/tests/integration/test_agent_version_evaluation_migration.py`

**Interfaces:**
- `AgentPaperTriggerRequest.candidate_batch_id: UUID` for normal cycles.
- `start_schwab_agent_paper_cycle(..., requested_instrument_ids, candidate_batch_id=None)`; version-evaluation callers pass the frozen evaluation instrument explicitly.

- [ ] **Step 1: Add failing request-hash and no-fallback tests**

Assert the trigger reservation hash changes when `candidate_batch_id` changes; missing batch returns `CANDIDATE_BATCH_REQUIRED` before provider calls; account/universe mismatch returns `CANDIDATE_BATCH_INVALID`; and the loop does not manufacture AAPL.

- [ ] **Step 2: Add failing version-evaluation compatibility test**

Create an AAPL evaluation pair without a normal candidate batch and assert both lanes still use exactly the evaluation's frozen AAPL `instrument_id`. Add a negative case for a non-AAPL evaluation instrument and expect the existing evaluation registration rejection.

- [ ] **Step 3: Run focused tests and verify failure**

Run: `uv run pytest engine/tests/unit/ops/test_agent_paper_once.py engine/tests/unit/ops/test_agent_paper_loop.py engine/tests/integration/test_agent_paper_cycle_e2e.py -q --import-mode=importlib`
Expected: FAIL because the trigger and loop do not accept candidate batches.

- [ ] **Step 4: Implement the normal and evaluation paths**

Load and validate the candidate batch before any source collection. Add its ID/hash to the trigger claim request hash and cycle row. Pass all symbols to the Task 4 collectors. Remove the `_start_cycle` open-order early stop so research can proceed; do not remove resource blocking from target activation later.

For the loop, add a required protected setting `NEXUS_AGENT_PAPER_CANDIDATE_BATCH_ID`. Schedule only the 10:00 America/New_York slot obtained from the frozen service calendar; do not compare against a fixed UTC hour.

- [ ] **Step 5: Verify runner, evaluation, and no-broker boundaries**

Run: `uv run pytest engine/tests/unit/ops/test_agent_paper_once.py engine/tests/unit/ops/test_agent_paper_loop.py engine/tests/integration/test_agent_paper_cycle_e2e.py engine/tests/contract/test_agent_paper_no_broker.py engine/tests/static/test_agent_paper_schwab_boundary.py -q --import-mode=importlib`
Expected: PASS with no broker import or external order capability.

- [ ] **Step 6: Commit**

```bash
git add engine/src/nexus/ops/agent_paper_once.py engine/src/nexus/ops/agent_paper_loop.py engine/src/nexus/paper/agent_cycle.py engine/src/nexus/paper/agent_version_runner.py engine/tests/unit/ops/test_agent_paper_once.py engine/tests/unit/ops/test_agent_paper_loop.py engine/tests/integration/test_agent_paper_cycle_e2e.py engine/tests/integration/test_agent_version_evaluation_migration.py
git commit -m "feat: run explicit multi-candidate paper cycles"
```

### Task 6: Add strict portfolio-synthesis contracts and persistence

**Files:**
- Create: `engine/src/nexus/agents/portfolio_synthesis_contracts.py`
- Create: `engine/src/nexus/agents/portfolio_synthesis_persistence.py`
- Modify: `engine/src/nexus/jobs/models.py`
- Create: `engine/tests/unit/agents/test_portfolio_synthesis_contracts.py`
- Create: `engine/tests/integration/test_portfolio_synthesis_persistence_pg.py`

**Interfaces:**
- Produces: `PortfolioSynthesisInputV1`, `PortfolioSynthesisIntentV1`, `PortfolioChoiceV1`, `admit_portfolio_synthesis(...)`, `persist_portfolio_synthesis_terminal(...)`.
- Adds `JobType.AGENT_PORTFOLIO_SYNTHESIS = "agent.portfolio_synthesis"` at priority 180.

- [ ] **Step 1: Write failing schema tests**

```python
def test_synthesis_intent_contains_choice_not_sizing() -> None:
    intent = PortfolioSynthesisIntentV1.model_validate(valid_document())
    assert [choice.rank for choice in intent.choices] == [1, 2]
    forbidden = valid_document() | {"target_weight": "0.10"}
    with pytest.raises(ValidationError):
        PortfolioSynthesisIntentV1.model_validate(forbidden)
```

Require each compared instrument exactly once, actions from `include/increase/reduce/exit/maintain/defer`, role `core/tactical/none`, strict unique ranks for competing exposure increases, cash rationale, theses/counter-theses/invalidation references, and no numeric allocation/order fields.

- [ ] **Step 2: Write failing persistence/budget tests**

Assert one run per cycle, one output per run, exact replay, idempotency conflict, account-wide daily budget including existing security research reservations, settlement from known token usage, release on pre-call failure, and no second model admission after a terminal output.

- [ ] **Step 3: Run tests and verify missing contracts**

Run: `uv run pytest engine/tests/unit/agents/test_portfolio_synthesis_contracts.py engine/tests/integration/test_portfolio_synthesis_persistence_pg.py -q --import-mode=importlib`
Expected: FAIL because the modules and JobType are absent.

- [ ] **Step 4: Implement contracts, hashes, and persistence**

Implement the exact interface `admit_portfolio_synthesis(session, *, cycle_id: UUID, research_output_ids: tuple[UUID, ...], active_target_version_id: UUID | None, projection_hash: str, daily_limit_usd: Decimal, now: datetime) -> PortfolioSynthesisAdmission`. Under the account lock, load and validate the cycle, candidate batch, every terminal security output, active target, and projection; construct the canonical input; reserve the remaining account-day model budget; create one run and one Job; then flush all records atomically. A repeated call returns the persisted run and never creates another Job.

Canonical input must include ordered research output IDs/hashes, candidate batch hash, active target content hash or explicit absence, projection hash, policy hash, prompt/model/tool hashes, and session-close expiry. Persist no raw credentials or personal account facts outside the existing protected database.

- [ ] **Step 5: Run focused tests and commit**

Run: `uv run pytest engine/tests/unit/agents/test_portfolio_synthesis_contracts.py engine/tests/integration/test_portfolio_synthesis_persistence_pg.py -q --import-mode=importlib && uv run ruff check engine/src/nexus/agents/portfolio_synthesis_contracts.py engine/src/nexus/agents/portfolio_synthesis_persistence.py engine/src/nexus/jobs/models.py && uv run mypy engine/src/nexus/agents/portfolio_synthesis_contracts.py engine/src/nexus/agents/portfolio_synthesis_persistence.py`
Expected: PASS.

```bash
git add engine/src/nexus/agents/portfolio_synthesis_contracts.py engine/src/nexus/agents/portfolio_synthesis_persistence.py engine/src/nexus/jobs/models.py engine/tests/unit/agents/test_portfolio_synthesis_contracts.py engine/tests/integration/test_portfolio_synthesis_persistence_pg.py
git commit -m "feat: add portfolio synthesis authority"
```

### Task 7: Run and resume one synthesis Job per cycle

**Files:**
- Create: `engine/src/nexus/agents/prompts/portfolio-management-synthesis-v1.md`
- Create: `engine/src/nexus/agents/portfolio_synthesis_worker.py`
- Modify: `engine/src/nexus/worker.py`
- Modify: `engine/src/nexus/paper/agent_cycle.py`
- Create: `engine/tests/unit/agents/test_portfolio_synthesis_worker.py`
- Modify: `engine/tests/integration/test_agent_paper_cycle_pg.py`

**Interfaces:**
- Produces: `run_portfolio_synthesis_worker_once(settings, *, job_ids=()) -> bool`.
- Cycle transition: security outputs complete → synthesis admitted/queued → terminal synthesis read → deterministic proposal step.

- [ ] **Step 1: Write a failing prompt-boundary test**

Assert the prompt explicitly forbids weights, amounts, quantities, orders, risk overrides, and unbound instruments; requires strict priority when funds cannot cover all increases; and permits abstention when evidence cannot support a comparison.

- [ ] **Step 2: Write failing recovery tests**

Cover queued, running, succeeded-with-output, succeeded-without-output, failed, abstained, budget exhausted, and stream-disconnected-after-persist cases. In the persisted-output case assert the model adapter is called zero times on resume.

- [ ] **Step 3: Run tests and verify missing worker**

Run: `uv run pytest engine/tests/unit/agents/test_portfolio_synthesis_worker.py engine/tests/integration/test_agent_paper_cycle_pg.py -q --import-mode=importlib`
Expected: FAIL because synthesis admission/worker routing is absent.

- [ ] **Step 4: Implement the worker and cycle resume branch**

Reuse the Codex adapter, pricing, safe-output persistence, and job-claim patterns from `portfolio_decision_worker.py`, but parse only `PortfolioSynthesisIntentV1`. Add worker dispatch for `AGENT_PORTFOLIO_SYNTHESIS`. Keep cycle status `researching` while the synthesis Job is pending; identify it by the unique run `cycle_id`, not a new mutable cycle status.

Map failed, invalid, over-budget, or incomplete terminal synthesis to cycle result code `PORTFOLIO_SYNTHESIS_UNAVAILABLE`; do not continue to deterministic proposal construction.

- [ ] **Step 5: Verify exactly-once behavior and commit**

Run: `uv run pytest engine/tests/unit/agents/test_portfolio_synthesis_worker.py engine/tests/integration/test_portfolio_synthesis_persistence_pg.py engine/tests/integration/test_agent_paper_cycle_pg.py -q --import-mode=importlib`
Expected: PASS and model-call counter remains one across repeated resume.

```bash
git add engine/src/nexus/agents/prompts/portfolio-management-synthesis-v1.md engine/src/nexus/agents/portfolio_synthesis_worker.py engine/src/nexus/worker.py engine/src/nexus/paper/agent_cycle.py engine/tests/unit/agents/test_portfolio_synthesis_worker.py engine/tests/integration/test_agent_paper_cycle_pg.py
git commit -m "feat: synthesize one portfolio choice per cycle"
```

### Task 8: Build deterministic proposals and approve one initial baseline

**Files:**
- Create: `engine/src/nexus/paper/portfolio_proposal.py`
- Create: `engine/src/nexus/paper/target_service.py`
- Modify: `engine/src/nexus/paper/agent_decision_store.py`
- Modify: `engine/src/nexus/paper/agent_cycle.py`
- Create: `engine/tests/unit/paper/test_portfolio_proposal.py`
- Create: `engine/tests/integration/test_initial_target_approval_pg.py`
- Create: `engine/tests/integration/test_portfolio_proposal_pg.py`

**Interfaces:**
- Produces: `PortfolioProposalV1`, `build_portfolio_proposal(...)`, `approve_initial_target_projection(...)`, `persist_portfolio_proposal(...)`.
- New strategy version: `agent_portfolio_proposal_v1`; parameters hash must equal `PORTFOLIO_ALLOCATION_POLICY_HASH`.

- [ ] **Step 1: Write failing allocation tests for every action**

```python
proposal = build_portfolio_proposal(
    active_target=target({MSFT_ID: "0.10", AAPL_ID: "0.05"}, cash="0.85"),
    synthesis=intent(
        include=(choice(NVDA_ID, role="tactical", rank=1),),
        maintain=(MSFT_ID,),
    ),
    policy=PORTFOLIO_ALLOCATION_V1,
)
assert proposal.weights == {MSFT_ID: Decimal("0.10"), AAPL_ID: Decimal("0.05"), NVDA_ID: Decimal("0.05")}
assert proposal.cash_weight == Decimal("0.80")
```

Add cases for core 10%, tactical 5%, explicit increase/reduce in 5-point bands, exit to zero, unchanged omissions inherited exactly, max two increases, 15% cap, 20% cash floor, 10% turnover cap, ambiguous rank rejection, and repeated identical intent producing the same target rather than another increment.

- [ ] **Step 2: Write failing initial-approval tests**

Assert approval reloads the exact `PaperPortfolioProjection` at `projection_as_of`, verifies `projection_hash`, requires no open orders/reservations/unknown outcome, writes one immutable approval, and creates no target/batch/order. Assert a second approval, stale hash, wrong account, or unseen as-of is rejected.

- [ ] **Step 3: Run tests and verify missing implementation**

Run: `uv run pytest engine/tests/unit/paper/test_portfolio_proposal.py engine/tests/integration/test_initial_target_approval_pg.py engine/tests/integration/test_portfolio_proposal_pg.py -q --import-mode=importlib`
Expected: FAIL because proposal and approval services are absent.

- [ ] **Step 4: Implement the proposal and initial approval services**

Implement the exact interface `approve_initial_target_projection(session, *, account_id: UUID, projection_as_of: datetime, expected_projection_hash: str, approved_by: str, approved_at: datetime, idempotency_key: str) -> InitialTargetApprovalResult`. Under the account advisory lock, rebuild the stored projection at the exact timestamp, compare its canonical hash, reject any occupied/unknown state, persist the one-time approval, and persist the idempotency response in the same transaction. It must not call target activation or order-batch services.

For the first proposal, require the approval and exact projection hash. For later proposals, require the active target and ignore the approval as an allocation source. Store `portfolio_synthesis_output_id`, complete targets/cash, proposal expiry, and a decision hash covering synthesis + policy + target content.

- [ ] **Step 5: Verify legacy decision loading and commit**

Run: `uv run pytest engine/tests/unit/paper/test_portfolio_proposal.py engine/tests/integration/test_initial_target_approval_pg.py engine/tests/integration/test_portfolio_proposal_pg.py engine/tests/integration/test_agent_paper_decision_pg.py -q --import-mode=importlib`
Expected: PASS; legacy `agent_paper_decision_v1` still round-trips unchanged.

```bash
git add engine/src/nexus/paper/portfolio_proposal.py engine/src/nexus/paper/target_service.py engine/src/nexus/paper/agent_decision_store.py engine/src/nexus/paper/agent_cycle.py engine/tests/unit/paper/test_portfolio_proposal.py engine/tests/integration/test_initial_target_approval_pg.py engine/tests/integration/test_portfolio_proposal_pg.py
git commit -m "feat: build deterministic portfolio proposals"
```

### Task 9: Activate immutable targets and create target-aware plans

**Files:**
- Modify: `engine/src/nexus/paper/target_service.py`
- Modify: `engine/src/nexus/paper/service.py`
- Modify: `engine/src/nexus/paper/agent_cycle.py`
- Modify: `engine/tests/unit/paper/test_service.py`
- Create: `engine/tests/integration/test_paper_target_activation_pg.py`
- Modify: `engine/tests/integration/test_paper_concurrency_pg.py`
- Modify: `engine/tests/integration/test_paper_atomicity_pg.py`

**Interfaces:**
- Produces: `activate_portfolio_target(session, *, account_id, strategy_decision_id, request_time, idempotency_key) -> TargetActivationResult`.
- Extends: `create_paper_order_batch(..., target_version_id: UUID | None = None)`; new portfolio proposals require non-null target, legacy callers remain null.

- [ ] **Step 1: Write failing target-version tests**

Cover first activation, changed target with previous link, same hash reusing the current version, expired proposal, missing policy/approval, and proposal formed against an older active target. Assert max `version_seq` is authoritative and no mutable active flag exists.

- [ ] **Step 2: Write failing occupancy and same-target plan tests**

Assert open orders, cash reservation, position reservation, or unknown cancellation blocks target replacement and new plan but does not delete the proposal. Assert a fully settled prior plan permits only a fresh proposal. For an unchanged target, assert drift below 3 points creates no plan; drift at/above 3 points creates one plan only when not covered by existing fills/orders and not merely a previously risk-rejected remainder.

- [ ] **Step 3: Write failing concurrency/rollback tests**

Run two activation requests concurrently and assert one target version and at most one batch. Inject failure after target insert but before batch persistence and assert the whole transaction rolls back.

- [ ] **Step 4: Run tests and verify failure**

Run: `uv run pytest engine/tests/integration/test_paper_target_activation_pg.py engine/tests/integration/test_paper_concurrency_pg.py engine/tests/integration/test_paper_atomicity_pg.py -q --import-mode=importlib`
Expected: FAIL because target activation and target-aware batches are absent.

- [ ] **Step 5: Implement account-locked activation and occupancy projection**

Implement the exact interface `activate_portfolio_target(session, *, account_id: UUID, strategy_decision_id: UUID, request_time: datetime, idempotency_key: str) -> TargetActivationResult`. Under the account advisory lock, reload the proposal, policy, approval/current target, current projection, orders, fills, reservations, and cancellation certainty; reject stale or occupied input; append or reuse the immutable target version; calculate uncovered deltas; invoke `create_paper_order_batch` at most once; and persist the idempotency result in one transaction.

Move open-order rejection out of research/proposal creation and into this coordinator. Compute occupation from order state, reservation events, fills, ledger/projection consistency, and cancellation certainty. Append cycle result codes `TARGET_ACTIVATION_BLOCKED`, `POLICY_UNCONFIRMED`, or the existing `NO_ORDER_REQUIRED` without adding a parallel mutable state table.

- [ ] **Step 6: Run target/service regressions and commit**

Run: `uv run pytest engine/tests/unit/paper/test_service.py engine/tests/integration/test_paper_target_activation_pg.py engine/tests/integration/test_paper_order_batch_pg.py engine/tests/integration/test_paper_concurrency_pg.py engine/tests/integration/test_paper_atomicity_pg.py -q --import-mode=importlib`
Expected: PASS.

```bash
git add engine/src/nexus/paper/target_service.py engine/src/nexus/paper/service.py engine/src/nexus/paper/agent_cycle.py engine/tests/unit/paper/test_service.py engine/tests/integration/test_paper_target_activation_pg.py engine/tests/integration/test_paper_concurrency_pg.py engine/tests/integration/test_paper_atomicity_pg.py
git commit -m "feat: activate targets and coordinate paper plans"
```

### Task 10: Add pure-read cancellation result and plan projections

**Files:**
- Modify: `engine/src/nexus/paper/read_service.py`
- Modify: `engine/src/nexus/api/schemas/paper.py`
- Modify: `engine/src/nexus/api/routes/paper.py`
- Modify: `engine/tests/unit/api/test_paper_contracts.py`
- Modify: `engine/tests/integration/test_paper_api.py`
- Create: `engine/tests/integration/test_paper_cancel_result_read_pg.py`

**Interfaces:**
- Produces: `PaperReadService.cancel_result(order_id, idempotency_key)`, `PaperReadService.plan(batch_id)`.
- Adds GET `/internal/v1/paper/orders/{order_id}/cancel-result` and GET `/internal/v1/paper/plans/{batch_id}`.

- [ ] **Step 1: Write failing four-state cancellation tests**

```python
response = await client.get(
    f"/internal/v1/paper/orders/{order_id}/cancel-result",
    headers={**auth_headers, "Idempotency-Key": cancel_key},
)
assert response.json()["data"]["status"] == "NOT_RECORDED"
assert response.json()["data"]["reservation_released"] is False
```

Add `COMMITTED`, `CONFLICT`, and `UNKNOWN` fixtures. Snapshot counts/hashes of order events, idempotency rows, reservations, ledger entries, targets, and batches before/after first and repeated GET; assert exact equality.

- [ ] **Step 2: Write failing plan projection tests**

Assert plan response includes target version, proposal decision, risk approved/reduced/rejected amounts, confirmed fills, open-order coverage, cash/position reservation, remaining target gap, occupation state, and independent as-of timestamps. Legacy batch returns `target_version_id=null` and `association_state="legacy_unassociated"`.

- [ ] **Step 3: Run tests and verify route absence**

Run: `uv run pytest engine/tests/integration/test_paper_cancel_result_read_pg.py engine/tests/integration/test_paper_api.py -q --import-mode=importlib`
Expected: FAIL with 404/missing schema.

- [ ] **Step 4: Implement pure projections and routes**

Do not import or call `cancel_internal_paper_order` from the GET handler. Use the `Idempotency-Key` header, reject query strings, verify exact account/order/request-hash binding, and return the latest authoritative order/reservation facts even for `NOT_RECORDED`.

- [ ] **Step 5: Run API tests, OpenAPI generation smoke test, and commit**

Run: `uv run pytest engine/tests/unit/api/test_paper_contracts.py engine/tests/integration/test_paper_cancel_result_read_pg.py engine/tests/integration/test_paper_api.py -q --import-mode=importlib && uv run ruff check engine/src/nexus/paper/read_service.py engine/src/nexus/api/schemas/paper.py engine/src/nexus/api/routes/paper.py`
Expected: PASS.

```bash
git add engine/src/nexus/paper/read_service.py engine/src/nexus/api/schemas/paper.py engine/src/nexus/api/routes/paper.py engine/tests/unit/api/test_paper_contracts.py engine/tests/integration/test_paper_api.py engine/tests/integration/test_paper_cancel_result_read_pg.py
git commit -m "feat: expose read-only paper reconciliation"
```

### Task 11: Extend cycle/portfolio reads and regenerate the client

**Files:**
- Modify: `engine/src/nexus/paper/read_service.py`
- Modify: `engine/src/nexus/api/schemas/paper.py`
- Modify: `engine/tests/integration/test_paper_api.py`
- Modify: `app/lib/generated/schema.d.ts`
- Modify: `app/lib/generated/runtime-contract.ts`
- Modify: `app/lib/paper/bff.ts`
- Modify: `app/api/v1/paper/cycles/[cycleId]/route.ts`
- Modify: `app/api/v1/paper/portfolio/route.ts`
- Create: `app/api/v1/paper/plans/[planId]/route.ts`
- Create: `app/api/v1/paper/orders/[orderId]/cancel-result/route.ts`
- Modify: `tests/unit/paper/routes.test.ts`
- Modify: `tests/unit/paper/runtime-boundary.test.ts`

**Interfaces:**
- Cycle detail adds candidate batch, security research, synthesis judgment, proposal, active target, activation result, blocking plan, and plan summary.
- Portfolio adds `management_summary` with current target, occupying plan, latest proposal, exact identities, and as-of.

- [ ] **Step 1: Add failing engine response tests**

Assert DB IDs/hashes round-trip unchanged; `proposal`, `active_target`, `plan`, and `actual` use separate objects and timestamps; `POLICY_UNCONFIRMED` does not expose invented weights; legacy cycles remain readable.

- [ ] **Step 2: Add failing BFF route tests**

Mock the generated client and assert GET routes only call their exact read operation. For cancel-result, forward the existing attempt key as `Idempotency-Key`; assert the BFF never calls `cancelInternalPaperOrder`.

- [ ] **Step 3: Run focused backend/frontend tests and verify failure**

Run: `uv run pytest engine/tests/integration/test_paper_api.py -q --import-mode=importlib && npm run test:unit -- tests/unit/paper/routes.test.ts tests/unit/paper/runtime-boundary.test.ts`
Expected: FAIL because schemas and generated operations are absent.

- [ ] **Step 4: Implement projections, routes, and regenerate**

Run after backend schema changes: `npm run generate:client`. Commit both generated files. BFF functions return server facts unchanged and do not calculate weights, available cash, occupation, or activation status.

- [ ] **Step 5: Verify generated-client parity and commit**

Run: `npm run check:generated && npm run test:unit -- tests/unit/paper/routes.test.ts tests/unit/paper/runtime-boundary.test.ts && npm run typecheck`
Expected: PASS.

```bash
git add engine/src/nexus/paper/read_service.py engine/src/nexus/api/schemas/paper.py engine/tests/integration/test_paper_api.py app/lib/generated app/lib/paper/bff.ts app/api/v1/paper tests/unit/paper/routes.test.ts tests/unit/paper/runtime-boundary.test.ts
git commit -m "feat: expose portfolio target read models"
```

### Task 12: Implement decisions and paper product readback

**Files:**
- Create: `app/components/decisions/portfolio-decision-detail.tsx`
- Create: `app/components/decisions/target-comparison.tsx`
- Create: `app/components/paper/adjustment-plan.tsx`
- Create: `app/components/paper/cancel-result-check.tsx`
- Modify: `app/(protected)/decisions/page.tsx`
- Modify: `app/(protected)/decisions/[cycleId]/page.tsx`
- Modify: `app/(protected)/paper/page.tsx`
- Modify: `app/(protected)/workspace/page.tsx`
- Modify: `app/(protected)/portfolio/page.tsx`
- Modify: `tests/unit/decisions/pages.test.tsx`
- Modify: `tests/unit/paper/page.test.tsx`
- Modify: `tests/unit/workbench/page.test.tsx`
- Modify: `tests/unit/portfolio/page.test.tsx`
- Modify: `tests/e2e/decisions.spec.ts`
- Modify: `tests/e2e/paper.spec.ts`
- Modify: `tests/e2e/workbench.spec.ts`
- Modify: `tests/e2e/portfolio.spec.ts`

**Interfaces:**
- Consumes only Task 11 BFF read models and Task 10 pure-read cancel result.
- Produces no business mutation except the pre-existing explicit cancel action, which remains visually and programmatically separate from result checking.

- [ ] **Step 1: Add failing decisions rendering tests**

Render fixtures for: active V7 + blocked PROP-008, no active target, unchanged proposal, policy unconfirmed, and legacy cycle. Assert the first visible conclusion identifies proposal versus active target; Agent judgment and deterministic weights have separate headings; inherited rows say research validity was not renewed; and no button text implies approval or immediate activation.

- [ ] **Step 2: Add failing paper rendering/interaction tests**

Render partial fill + unknown cancellation + reserved cash. Assert plan/target IDs, risk reduction, fills, open coverage, available versus reserved cash, remaining gap, and occupation appear independently. Click “核对原操作结果” twice and assert two GET reads, zero cancel POSTs, no target/plan mutation, and stale response isolation after selecting another order.

- [ ] **Step 3: Add failing summary/mobile/accessibility tests**

Assert workspace/portfolio show only target/plan summaries and links. In Playwright mobile viewport, require the blocking conclusion before target cards, labelled table/card equivalents, keyboard-accessible disclosures, status text independent of color, and no horizontal clipping of IDs or amounts.

- [ ] **Step 4: Run focused tests and verify missing components**

Run: `npm run test:unit -- tests/unit/decisions/pages.test.tsx tests/unit/paper/page.test.tsx tests/unit/workbench/page.test.tsx tests/unit/portfolio/page.test.tsx`
Expected: FAIL because portfolio-management components and states are absent.

- [ ] **Step 5: Implement the components using server-projected facts**

Keep layout aligned with the approved handoff: blocking conclusion first; Agent choice before deterministic target comparison; plan timeline before historical evaluation content; compact summaries only on workspace/portfolio. Do not add frontend arithmetic for cash, weights, gap, or occupation.

- [ ] **Step 6: Run unit, E2E, lint, typecheck, and build**

Run: `npm run test:unit -- tests/unit/decisions/pages.test.tsx tests/unit/paper/page.test.tsx tests/unit/workbench/page.test.tsx tests/unit/portfolio/page.test.tsx && npm run test:e2e -- tests/e2e/decisions.spec.ts tests/e2e/paper.spec.ts tests/e2e/workbench.spec.ts tests/e2e/portfolio.spec.ts && npm run lint && npm run typecheck && npm run build`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add app tests/unit/decisions/pages.test.tsx tests/unit/paper/page.test.tsx tests/unit/workbench/page.test.tsx tests/unit/portfolio/page.test.tsx tests/e2e/decisions.spec.ts tests/e2e/paper.spec.ts tests/e2e/workbench.spec.ts tests/e2e/portfolio.spec.ts
git commit -m "feat: present portfolio targets and plans"
```

### Task 13: Cross-layer acceptance, compatibility, and operator documentation

**Files:**
- Create: `engine/tests/integration/test_portfolio_management_e2e.py`
- Modify: `engine/tests/contract/test_agent_paper_no_broker.py`
- Modify: `engine/tests/static/test_agent_paper_schwab_boundary.py`
- Modify: `README.md`
- Create: `docs/runbooks/portfolio-management-first-slice.md`
- Modify: `docs/design/nexus-portfolio-management-first-slice-design-handoff.md`

**Interfaces:**
- Final acceptance uses normal product services/CLI only; fixture tests do not authorize natural runtime success.

- [ ] **Step 1: Write the controlled end-to-end scenarios**

Implement deterministic fixtures for:

1. approved initial snapshot → two qualified candidates → one synthesis → proposal → target → target-aware batch → full fill → balanced ledger;
2. no target change → no new target version;
3. old plan partially filled/unknown cancel → new proposal retained but activation blocked;
4. old plan settled → old proposal still not activated → fresh cycle revalidates;
5. same-target drift with already-covered demand → no duplicate order;
6. policy ambiguity/insufficient evidence → no target or order;
7. legacy AAPL cycle readback unchanged.

For every scenario assert proposal→target→plan→order→fill→ledger identities and exact cash/position conservation.

- [ ] **Step 2: Run the end-to-end test and fix only first-slice defects**

Run: `uv run pytest engine/tests/integration/test_portfolio_management_e2e.py -q --import-mode=importlib`
Expected: PASS. Do not loosen policy, time windows, evidence, or hard risk to make it green.

- [ ] **Step 3: Update operator documentation with exact safe commands**

Document: inspect account/projection; create candidate batch; show the exact snapshot for user approval; create initial approval only after explicit confirmation; configure candidate batch ID; run one bounded cycle; read cycle/plan/cancel-result; verify `execution_locked=true`. Mark external broker simulation and live trading as unavailable.

Correct the design handoff wording so a client attempt key is not called a server-confirmed cancellation request unless `COMMITTED` is persisted.

- [ ] **Step 4: Run the complete proportional verification suite**

Run:

```bash
uv run pytest engine/tests/unit/paper engine/tests/unit/agents/test_portfolio_synthesis_contracts.py engine/tests/unit/agents/test_portfolio_synthesis_worker.py engine/tests/unit/ops/test_agent_paper_once.py engine/tests/unit/ops/test_agent_paper_loop.py engine/tests/integration/test_agent_paper_cycle_pg.py engine/tests/integration/test_portfolio_management_migration.py engine/tests/integration/test_agent_paper_candidate_batch_pg.py engine/tests/integration/test_portfolio_synthesis_persistence_pg.py engine/tests/integration/test_initial_target_approval_pg.py engine/tests/integration/test_portfolio_proposal_pg.py engine/tests/integration/test_paper_target_activation_pg.py engine/tests/integration/test_paper_cancel_result_read_pg.py engine/tests/integration/test_paper_api.py engine/tests/integration/test_portfolio_management_e2e.py engine/tests/contract/test_agent_paper_no_broker.py -q --import-mode=importlib
uv run ruff check engine
uv run mypy engine/src
npm run check:generated
npm run test:unit
npm run lint
npm run typecheck
npm run build
```

Expected: every command exits 0. If any command fails, retain all failures from the same run before changing code.

- [ ] **Step 5: Run a bounded local acceptance without external trading**

Use an isolated test database and fixture/object-store endpoints. Verify the persisted chain and UI for all controlled cases. Do not call Schwab, Alpaca, Fosun, Bark, broker simulation write, or live trading during this step.

- [ ] **Step 6: Commit documentation and acceptance coverage**

```bash
git add engine/tests/integration/test_portfolio_management_e2e.py engine/tests/contract/test_agent_paper_no_broker.py engine/tests/static/test_agent_paper_schwab_boundary.py README.md docs/runbooks/portfolio-management-first-slice.md docs/design/nexus-portfolio-management-first-slice-design-handoff.md
git commit -m "test: accept portfolio management first slice"
```

## Post-Implementation Runtime Gates

The implementation is merge-ready after Task 13 verification and review, but the feature is not naturally accepted until all of the following occur separately:

1. The user is shown one real internal-paper projection and explicitly approves that exact `projection_hash/as_of`; no assistant may infer this approval from the policy confirmation.
2. A real 1–5 candidate batch is explicitly selected from the approved universe.
3. A future XNYS 10:00 slot runs through the normal unattended runner with qualified provider evidence and bounded model budget.
4. Persisted proposal, target, plan, order, fill, ledger, and performance facts are read back consistently. A legitimate no-trade result remains valid but does not by itself prove the natural build/fill path.
5. `execution_locked=true`, real-account read-only status, and absence of broker/live write authority are reverified from the running version.

## Specification Coverage Check

- Candidate scope, provider evidence, research validity, and XNYS scheduling: Tasks 3–5.
- One bounded portfolio judgment with account-day budget and replay safety: Tasks 6–7.
- User-approved initial baseline, deterministic sizing, inheritance, target versioning, and no-change reuse: Tasks 8–9.
- One resource-occupying plan, unknown-result blocking, pure-read reconciliation, and no duplicate demand: Tasks 9–10.
- Proposal/target/plan/actual separation across decisions, paper, workspace, and portfolio: Tasks 11–12.
- Legacy AAPL evaluation, no-broker boundaries, migration compatibility, controlled acceptance, and natural-runtime gates: Tasks 2, 5, and 13 plus the post-implementation gates.
