# Agent Version Evaluation Runtime Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Repair the existing AAPL 4+20 evaluation so its unattended runner can publish legitimate session-close performance facts and reach a meaningful deterministic terminal result.

**Architecture:** Keep the existing evaluation tables, dual-lane cycle pipeline, policy thresholds, and single host runner. Change performance comparison from one snapshot per OOS slot to one official XNYS close per OOS session, let the runner publish missing evaluation closes through the existing performance service without Bark summaries, and reject schedules whose same-session slots are less than 15 minutes apart. Safely invalidate only an untouched legacy unsafe evaluation before registering its future replacement.

**Tech Stack:** Python 3.12, SQLAlchemy asyncio, PostgreSQL, Pydantic, pytest, existing local Agent Paper service CLI.

**Spec:** `docs/superpowers/specs/2026-09-17-agent-version-parallel-evaluation-design.md` section 12

## Global Constraints

- Preserve 4 train slots, 20 OOS slots over 10 sessions, existing PASS/FAIL/UNKNOWN thresholds, AAPL-only scope, and `execution_locked=true`.
- Do not add a table, migration, worker, Universe, data provider, broker binding, retry loop, or new notification type.
- A performance snapshot remains valid only at the exact `market_close_at` from the frozen complete XNYS calendar authority.
- Ordinary Stage 2 performance publication must continue to enqueue its Bark session summary; only evaluation-account publication suppresses it explicitly.
- Never replay an existing cycle, Job, output, order, fill, or model call. Inspect persisted state before invalidation, replacement registration, and service restart.
- Preserve the user's dirty `README.md`, `docs/images/`, `docs/user-guide.md`, and untracked 2026-09-13 plan/spec. Do not push or deploy without separate authorization.

---

### Task 1: Freeze the schedule-safety contract and close only untouched unsafe evaluations

**Files:**
- Modify: `engine/src/nexus/paper/agent_version_models.py`
- Modify: `engine/src/nexus/paper/agent_version_evaluation.py`
- Test: `engine/tests/unit/paper/test_agent_version_policy.py`
- Test: `engine/tests/integration/test_agent_version_evaluation_pg.py`

**Interfaces:**
- Produces `MIN_SAME_SESSION_SLOT_GAP = timedelta(minutes=15)` and includes its seconds in `evaluation_policy_payload()`.
- Produces `_has_unsafe_same_session_gap(slots: Sequence[datetime]) -> bool` shared by request validation and legacy invalidation.
- Extends `invalidate_agent_version_evaluation(...)` with terminal reason `EVALUATION_SCHEDULE_UNSAFE` only when no evaluation work facts exist.

- [x] **Step 1: Add failing policy tests**

Add one train and one OOS request whose same-session slots are five minutes apart and assert construction raises `ValueError("AGENT_VERSION_WINDOW_INVALID")`. Add a 15-minute boundary case and assert it is accepted. Assert the canonical policy payload contains `"min_same_session_slot_gap_seconds": 900` and its policy hash differs from the pre-repair payload.

- [x] **Step 2: Run the focused unit tests and verify RED**

```bash
cd engine
uv run pytest tests/unit/paper/test_agent_version_policy.py -q
```

Expected: the five-minute schedules are still accepted and the new policy field is absent.

- [x] **Step 3: Implement the minimal schedule validator**

Group slots by UTC session date after existing calendar validation, sort each group, and reject any adjacent delta smaller than `MIN_SAME_SESSION_SLOT_GAP`. Apply it independently to train and OOS slots; keep all existing cardinality, ordering, session-count, overlap, and market-hours checks.

- [x] **Step 4: Add failing invalidation integration tests**

Create an open five-minute-gap evaluation fixture using direct persistence to represent the legacy record. Assert invalidation writes an immutable `UNKNOWN` decision with `reason_code == "EVALUATION_SCHEDULE_UNSAFE"`, selects the incumbent, and creates no Job or model output. Parameterize one cycle, one research run/Job, one order, and one fill; each must raise `AGENT_VERSION_INVALIDATION_UNSAFE`. Assert a safe 15-minute evaluation raises `AGENT_VERSION_EVALUATION_NOT_INVALID`.

- [x] **Step 5: Run the exact integration nodes and verify RED**

```bash
cd engine
uv run pytest tests/integration/test_agent_version_evaluation_pg.py -k 'schedule_unsafe or invalidation_unsafe' -q
```

- [x] **Step 6: Implement the guarded invalidation branch and run GREEN**

Before persisting the new reason, count pairs and both evaluation accounts' cycles, research runs, Jobs, outputs, orders, and fills. Require every count to be zero. Retain the existing `CALENDAR_PRIOR_SESSION_MISSING` branch and replay behavior unchanged.

```bash
cd engine
uv run pytest tests/unit/paper/test_agent_version_policy.py tests/integration/test_agent_version_evaluation_pg.py -q
uv run ruff check src/nexus/paper/agent_version_models.py src/nexus/paper/agent_version_evaluation.py tests/unit/paper/test_agent_version_policy.py tests/integration/test_agent_version_evaluation_pg.py
uv run mypy src/nexus/paper/agent_version_models.py src/nexus/paper/agent_version_evaluation.py
```

### Task 2: Compare exactly one official close per OOS session

**Files:**
- Modify: `engine/src/nexus/paper/agent_version_evaluation.py`
- Test: `engine/tests/integration/test_agent_version_evaluation_pg.py`

**Interfaces:**
- Produces `_oos_session_closes(session, evaluation) -> tuple[datetime, ...]`, derived only from `evaluation.calendar_authority_id` and the 10 distinct OOS session dates.
- `_load_agent_version_metric_facts(...)` requires one exact close snapshot per account per returned close and uses those 10 ordered rows for return, positive-session, and drawdown metrics.

- [x] **Step 1: Add a failing complete-metrics test**

Persist all 20 comparable OOS pairs and two completed cycles per slot, then publish or insert one legitimate close snapshot per account for each of the 10 OOS sessions. Assert `authority_complete is True`, the final NAVs determine `cumulative_excess_return`, and the 10 close-to-close comparisons determine `positive_excess_session_count`. The old code must report incomplete because it expects 20 snapshots.

- [x] **Step 2: Add missing, duplicate, and wrong-time tests**

Remove one required account close and assert `authority_complete is False`. Add a snapshot outside the exact frozen close set and assert it is ignored rather than substituted. Preserve the existing database uniqueness rule for duplicate `(account_id, as_of)` facts.

- [x] **Step 3: Run the new tests and verify RED**

```bash
cd engine
uv run pytest tests/integration/test_agent_version_evaluation_pg.py -k 'session_close_metrics' -q
```

- [x] **Step 4: Replace slot partitioning with exact session-close loading**

Load the 10 `ExchangeCalendarSessionRecord.market_close_at` values for the evaluation's distinct OOS dates and exact frozen calendar. Select snapshots only where `as_of IN closes`, index by `(account_id, as_of)`, and require the exact 20-key Cartesian set. Compute cumulative excess from the last close, positive days by sequential close return, and drawdown degradation across those rows. Leave pair/cycle/decision/binding counts at 20 per lane.

- [x] **Step 5: Run GREEN and static checks**

```bash
cd engine
uv run pytest tests/integration/test_agent_version_evaluation_pg.py -q
uv run ruff check src/nexus/paper/agent_version_evaluation.py tests/integration/test_agent_version_evaluation_pg.py
uv run mypy src/nexus/paper/agent_version_evaluation.py
```

### Task 3: Publish evaluation closes in the existing runner without Bark summaries

**Files:**
- Modify: `engine/src/nexus/paper/performance.py`
- Modify: `engine/src/nexus/ops/agent_paper_loop.py`
- Test: `engine/tests/integration/test_paper_performance_pg.py`
- Test: `engine/tests/unit/ops/test_agent_paper_loop.py`

**Interfaces:**
- Extends `publish_paper_performance_snapshot(..., enqueue_session_summary: bool = True)`; false skips only `enqueue_paper_session_summary` after the same persistence/readback checks.
- Extends `AgentPaperLoopSnapshot` with `evaluation_performance_account_id: UUID | None` and `evaluation_performance_due_at: datetime | None`.
- Adds `AgentPaperLoopAction.PUBLISH_EVALUATION_PERFORMANCE` and executor support that calls the existing performance publisher with `enqueue_session_summary=False`.

- [x] **Step 1: Add failing summary-suppression tests**

Use the existing performance integration fixture twice. The default call must create the existing summary outbox row. A second evaluation-account call with `enqueue_session_summary=False` must persist performance, positions, and attributions but create no session-summary outbox row. Replaying the same `(account_id, as_of)` must remain read-only.

- [x] **Step 2: Run the performance tests and verify RED**

```bash
cd engine
uv run pytest tests/integration/test_paper_performance_pg.py -k 'session_summary or suppress' -q
```

- [x] **Step 3: Implement the narrow publication flag**

Keep the default true and wrap only the final `enqueue_paper_session_summary(...)` call. Do not change calendar, ledger, mark, cost, hash, snapshot, position, attribution, transaction, or idempotency behavior.

- [x] **Step 4: Add failing loop-selection tests**

Cover these exact states: a due incumbent close is selected before a later evaluation slot; after it exists, the challenger close is selected; after both exist, the next slot is selected; before both deadlines, `WAIT_FOR_EVALUATION` targets the earlier one; after all 10 closes and 20 pairs, finalization is selected. Assert evaluation waiting still suppresses ordinary Stage 2 work.

- [x] **Step 5: Run loop tests and verify RED**

```bash
cd engine
uv run pytest tests/unit/ops/test_agent_paper_loop.py -k 'evaluation and performance' -q
```

- [x] **Step 6: Implement persisted due-close inspection and execution**

For the first OOS close at or before database `clock_timestamp()` that lacks either account snapshot, return one account action in incumbent-then-challenger order. When none is due, retain current next-unpaired-slot behavior. Set the wait target to `min(next_slot, next_unpublished_close)` and use the existing heartbeat wait loop. Execute one publication transaction and return the new action result only after exact readback succeeds.

- [x] **Step 7: Run GREEN and focused regression**

```bash
cd engine
uv run pytest tests/integration/test_paper_performance_pg.py tests/unit/ops/test_agent_paper_loop.py -q
uv run ruff check src/nexus/paper/performance.py src/nexus/ops/agent_paper_loop.py tests/integration/test_paper_performance_pg.py tests/unit/ops/test_agent_paper_loop.py
uv run mypy src/nexus/paper/performance.py src/nexus/ops/agent_paper_loop.py
```

### Task 4: Product-path acceptance and safe runtime replacement

**Files:**
- Modify: `docs/runbooks/nexus-paper-once.md`
- Modify: `docs/plans/2026-09-12-nexus-simulation-evolution-roadmap.md`

**Interfaces:**
- Consumes the existing local `invalidate`, `register`, `status`, service `start`, service `status`, and persisted evaluation readback commands.
- Produces one terminal legacy evaluation and one future open evaluation with 15-minute same-session gaps; no historical slot is replayed.

- [x] **Step 1: Run the complete focused regression before touching runtime state**

```bash
cd engine
uv run pytest tests/unit/paper/test_agent_version_policy.py tests/integration/test_agent_version_evaluation_pg.py tests/integration/test_paper_performance_pg.py tests/unit/ops/test_agent_paper_loop.py -q
uv run ruff check src/nexus/paper/agent_version_models.py src/nexus/paper/agent_version_evaluation.py src/nexus/paper/performance.py src/nexus/ops/agent_paper_loop.py
uv run mypy src/nexus/paper/agent_version_models.py src/nexus/paper/agent_version_evaluation.py src/nexus/paper/performance.py src/nexus/ops/agent_paper_loop.py
```

- [x] **Step 2: Inspect the current evaluation before invalidation**

Read `evaluation -> pairs -> evaluation-account cycles -> runs -> jobs -> outputs -> orders -> fills`. Continue only if every work-fact count is zero and the service remains inactive. If any fact exists, stop without invalidation or replay and report the exact persisted state.

- [x] **Step 3: Invalidate the untouched unsafe evaluation through the normal CLI**

Call the existing local management entry once with its current evaluation ID. Read back one immutable decision with `status=UNKNOWN`, `reason_code=EVALUATION_SCHEDULE_UNSAFE`, and the incumbent selected. Do not retry an uncertain result; resolve it from persistence.

- [x] **Step 4: Register one replacement with future XNYS slots**

Reuse the exact frozen instrument, incumbent source run, challenger prompt/model, initial capital, and complete calendar authority. Select the next two future XNYS sessions for four train slots and the following ten sessions for 20 OOS slots; on every session use two in-market UTC slots separated by at least 15 minutes. Use a new content-derived idempotency key. Registration must create zero Jobs, cycles, orders, fills, or model outputs.

- [x] **Step 5: Restart and verify the host service**

Start the existing Agent Paper service once. Verify fresh heartbeat, the new evaluation ID, `evaluation_waiting`, correct next deadline, zero queued/running Jobs before the slot, and unchanged `execution_locked=true`. Verify API, general worker, Bark worker, and Web remain healthy.

- [x] **Step 6: Update the existing monitor and documentation**

Retarget the existing `NEXUS AAPL 4+20评价` monitor to the replacement evaluation ID without creating a duplicate. Document the 10-close contract, 15-minute gap, old terminal reason, new future window, service state, rollback command, and the rule that natural market-time evidence remains pending.

- [x] **Step 7: Final repository verification**

```bash
git diff --check
git status --short
git diff -- docs/superpowers/specs/2026-09-17-agent-version-parallel-evaluation-design.md docs/superpowers/plans/2026-09-18-agent-version-evaluation-runtime-repair.md engine/src/nexus/paper/agent_version_models.py engine/src/nexus/paper/agent_version_evaluation.py engine/src/nexus/paper/performance.py engine/src/nexus/ops/agent_paper_loop.py
```

Confirm the pre-existing dirty files remain present and unchanged by this repair. A local commit is allowed by the standing task authorization; pushing or deployment remains a separate external-write gate.
