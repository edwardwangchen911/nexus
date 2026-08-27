# NEXUS Phase 2 Independent Review Remediation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 Phase 2 从“组件和测试存在”修复为可由仓库内受限 worker 执行、可按候选重放、OOS 次数不可重置、注册表可完整重建的内部模拟闭环。

**Architecture:** 使用前向迁移 `0023_evolution_authority` 增加候选级投影、验证结果、lineage OOS 暴露账本和 registry activation，不回改 `0022`。一个数据库受限的 `nexus.evolution.executor` 组合四种既有 Job operation；所有比较、Top-3、试验次数、Shadow 和内部模拟冠军切换从 PostgreSQL 权威派生，读模型与 Web 对 identity/hash/ETag 失败关闭。

**Tech Stack:** Python 3.12、Pydantic 2、SQLAlchemy 2 async、Alembic、PostgreSQL 16、pytest/pytest-asyncio/Hypothesis、FastAPI；React 19、TypeScript、vinext、Vitest、Playwright。

**Spec:** `docs/superpowers/specs/2026-08-27-nexus-phase-2-independent-review-remediation-design.md`

## Global Constraints

- Agent 不能生成或执行任意代码、修改硬风控、访问 Broker/账户/交易密钥、创建订单、决定 Top-3、覆盖门禁或启用实盘。
- 实盘默认关闭；真实账户、券商写操作、真实订单、部署、密钥/权限、付费调用和破坏性操作不在本计划授权范围内。
- 不编辑已提交的 `0022_strategy_evolution`；新迁移必须是单头 `0023_evolution_authority`。
- PostgreSQL 是候选状态、OOS 次数、registry head、审批与内部模拟冠军的最终权威；前端状态不能授权写操作。
- 所有新业务状态先写可复现红测并观察预期失败，再写最小生产实现。
- 保留 `run_backtest()` 的 baseline-only、Agent-free、`promotable=false` 语义；新评价不能改变已有历史运行。
- Strategy R&D 生产组合根不得导入 broker、Fosun、真实订单、账户或 live-write 模块。
- 保留用户现有改动；每个任务只提交自身文件，提交前运行 `git diff --check`。

---

### Task 1: Add candidate, OOS, and registry activation authorities

**Files:**
- Create: `engine/alembic/versions/0023_strategy_evolution_authority.py`
- Modify: `engine/src/nexus/persistence/models/evolution.py`
- Modify: `engine/src/nexus/persistence/migrations.py`
- Modify: `engine/tests/helpers/migration_schema_expectations.py`
- Modify: `engine/tests/integration/test_evolution_migration.py`
- Modify: `engine/tests/integration/test_evolution_db_guards.py`

**Interfaces:**
- Consumes: current Alembic head `0022_strategy_evolution` and its immutable Phase 2 roots.
- Produces: ORM records `StrategyResearchLineageProjectionRecord`, `StrategyExperimentCandidateProjectionRecord`, `StrategyCandidateValidationResultRecord`, `StrategyOOSExposureRecord`, `StrategyExperimentOOSExposureLinkRecord`, `StrategyOOSEvidenceLinkRecord`, `StrategyRegistryActivationRecord`; nullable `StrategyExperimentEventRecord.experiment_id`; nullable `StrategyExperimentFamilyRecord.baseline_backtest_run_id`; migration head `0023_evolution_authority`.

- [x] **Step 1: Write migration red tests**

Add exact schema expectations and round-trip tests:

```python
def test_0023_adds_candidate_oos_and_activation_authorities(
    migrated_database_url: str,
) -> None:
    tables = asyncio.run(_table_names(migrated_database_url))
    assert {
        "strategy_research_lineage_projections",
        "strategy_experiment_candidate_projections",
        "strategy_candidate_validation_results",
        "strategy_oos_exposures",
        "strategy_experiment_oos_exposure_links",
        "strategy_oos_evidence_links",
        "strategy_registry_activations",
    } <= tables
    event_columns = asyncio.run(
        _columns(migrated_database_url, "strategy_experiment_events")
    )
    family_columns = asyncio.run(
        _columns(migrated_database_url, "strategy_experiment_families")
    )
    assert event_columns["experiment_id"] == "YES"
    assert family_columns["baseline_backtest_run_id"] == "YES"
    constraints = asyncio.run(
        _rows(
            migrated_database_url,
            "SELECT conname FROM pg_constraint WHERE conrelid IN "
            "('strategy_oos_exposures'::regclass,"
            "'strategy_registry_activations'::regclass)",
        )
    )
    names = {str(row["conname"]) for row in constraints}
    assert {
        "uq_strategy_oos_exposures_lineage_expression",
        "uq_strategy_oos_exposures_lineage_ordinal",
        "uq_strategy_registry_activations_event",
        "uq_strategy_registry_activations_hash",
    } <= names
```

Add a downgrade test that inserts one OOS exposure and expects SQLSTATE `23514`, plus an allowed empty `0023 -> 0022 -> 0023` round trip.

- [x] **Step 2: Run migration tests and observe RED**

Run:

```bash
uv run --package nexus-engine pytest \
  engine/tests/integration/test_evolution_migration.py \
  engine/tests/integration/test_evolution_db_guards.py -q
```

Expected: FAIL because revision `0023_evolution_authority` and the new tables/columns do not exist.

- [x] **Step 3: Implement the forward migration and ORM records**

The revision declaration is exact:

```python
revision = "0023_evolution_authority"
down_revision = "0022_strategy_evolution"
branch_labels = None
depends_on = None
```

Create all seven tables from the spec, add `experiment_id` and `baseline_backtest_run_id`, install immutable UPDATE/DELETE guards for authority facts, and install monotonic projection guards. Upgrade must abort if a pre-0023 family has comparison/Shadow/promotion facts that cannot be assigned to one candidate. Registry backfill must abort if a non-GENESIS transition is already effective without activation evidence.

Set:

```python
CURRENT_MIGRATION_HEAD = "0023_evolution_authority"
```

Do not add a second Alembic head and do not modify `0022_strategy_evolution.py`.

- [x] **Step 4: Verify migration GREEN and guards**

Run:

```bash
uv run --package nexus-engine pytest \
  engine/tests/integration/test_evolution_migration.py \
  engine/tests/integration/test_evolution_db_guards.py -q
uv run --package nexus-engine ruff check \
  engine/alembic/versions/0023_strategy_evolution_authority.py \
  engine/src/nexus/persistence/models/evolution.py \
  engine/tests/integration/test_evolution_migration.py
uv run --package nexus-engine mypy engine/src/nexus/persistence/models/evolution.py
```

Expected: migration and guard tests PASS; Alembic reports one head, exactly `0023_evolution_authority`.

- [x] **Step 5: Commit Task 1**

```bash
git add engine/alembic/versions/0023_strategy_evolution_authority.py \
  engine/src/nexus/persistence/models/evolution.py \
  engine/src/nexus/persistence/migrations.py \
  engine/tests/helpers/migration_schema_expectations.py \
  engine/tests/integration/test_evolution_migration.py \
  engine/tests/integration/test_evolution_db_guards.py
git commit -m "feat(engine): add strategy evolution authority facts"
```

### Task 2: Make candidate and family projections fully replayable

**Files:**
- Modify: `engine/src/nexus/evolution/repository.py`
- Modify: `engine/src/nexus/evolution/service.py`
- Modify: `engine/src/nexus/evolution/evaluation.py`
- Modify: `engine/src/nexus/evolution/shadow/service.py`
- Modify: `engine/src/nexus/evolution/registry.py`
- Create: `engine/tests/unit/evolution/test_candidate_state.py`
- Modify: `engine/tests/integration/test_evolution_admission_pg.py`
- Modify: `engine/tests/integration/test_evolution_projection_integrity_pg.py`

**Interfaces:**
- Produces: `CandidateState`, `aggregate_family_state(states)`, `append_candidate_event(...)`, `load_verified_experiment_authority(session, family_id, lock=False)`, `transition_candidate_state(...)`.
- Consumes: Task 1 candidate projection and event `experiment_id` columns.

- [x] **Step 1: Write pure state-machine red tests**

Cover every legal edge and these illegal mutations:

```python
def test_family_remains_active_while_one_of_three_candidates_finishes() -> None:
    assert aggregate_family_state(
        ("VALIDATION_FAILED", "ADMITTED", "ADMITTED")
    ) == "BACKTEST_RUNNING"

def test_one_promoted_candidate_dominates_family_terminal_state() -> None:
    assert aggregate_family_state(
        ("PROMOTED", "SHADOW_FAILED", "OOS_NOT_SELECTED")
    ) == "PROMOTED"

@pytest.mark.parametrize(
    ("before", "after"),
    (("ADMITTED", "OOS_RESERVED"), ("OOS_NOT_SELECTED", "OOS_SELECTED")),
)
def test_candidate_state_rejects_skipped_or_terminal_transitions(before, after) -> None:
    with pytest.raises(ValueError, match="CANDIDATE_STATE_TRANSITION_INVALID"):
        require_candidate_transition(before, after)
```

- [x] **Step 2: Run state tests and observe RED**

```bash
uv run --package nexus-engine pytest engine/tests/unit/evolution/test_candidate_state.py -q
```

Expected: FAIL because the candidate replay/state functions do not exist.

- [x] **Step 3: Implement candidate event replay and verified loader**

`load_verified_experiment_authority()` must perform one family/event/candidate-projection load, verify every event content hash and sequence, verify each candidate projection points to its latest candidate event, replay legal transitions, compute family aggregate, and compare the stored family projection including lineage cumulative count. Its return type must expose immutable records plus the verified projections, so callers do not issue a second unverified load.

Admission must append one candidate-bound `ADMITTED` event per experiment and create one candidate projection per experiment. Replace direct family projection reads in admission, comparison, Shadow start/finalize, veto, stale marking and promotion with the verified loader before any mutation.

- [x] **Step 4: Add PostgreSQL corruption and multi-candidate red tests**

Use controlled SQL to create:

1. an event for candidate A with candidate B's projection sequence;
2. a family projection state that disagrees with candidate aggregate;
3. 24 admitted candidates where one fails and the other 23 remain independently writable.

Each service write must raise `STRATEGY_EXPERIMENT_CHAIN_INVALID`, and the database projection trigger must reject a projection update without the matching event.

- [x] **Step 5: Run focused integration tests GREEN**

```bash
uv run --package nexus-engine pytest \
  engine/tests/unit/evolution/test_candidate_state.py \
  engine/tests/integration/test_evolution_admission_pg.py \
  engine/tests/integration/test_evolution_projection_integrity_pg.py \
  engine/tests/integration/test_evolution_promotion_gate_pg.py \
  engine/tests/integration/test_evolution_shadow_pg.py \
  engine/tests/integration/test_evolution_registry_pg.py -q
```

Expected: PASS; no test directly changes a family projection without first appending a legal event.

- [x] **Step 6: Commit Task 2**

```bash
git add engine/src/nexus/evolution/repository.py \
  engine/src/nexus/evolution/service.py \
  engine/src/nexus/evolution/evaluation.py \
  engine/src/nexus/evolution/shadow/service.py \
  engine/src/nexus/evolution/registry.py \
  engine/tests/unit/evolution/test_candidate_state.py \
  engine/tests/integration/test_evolution_admission_pg.py \
  engine/tests/integration/test_evolution_projection_integrity_pg.py
git commit -m "fix(engine): replay candidate strategy state independently"
```

### Task 3: Enforce validation Top-3 and database-owned OOS accounting

**Files:**
- Modify: `engine/src/nexus/evolution/evaluation.py`
- Modify: `engine/src/nexus/evolution/repository.py`
- Modify: `engine/src/nexus/evolution/jobs.py`
- Modify: `engine/src/nexus/evolution/service.py`
- Modify: `engine/tests/unit/evolution/test_evaluation.py`
- Create: `engine/tests/integration/test_evolution_shortlist_pg.py`
- Create: `engine/tests/integration/test_evolution_oos_exposure_pg.py`
- Modify: `engine/tests/integration/test_evolution_backtest_pg.py`
- Modify: `engine/tests/integration/test_evolution_promotion_gate_pg.py`
- Modify: `engine/tests/integration/test_evolution_jobs_pg.py`

**Interfaces:**
- Produces: `persist_candidate_validation_result(...)`, `finalize_validation_shortlist(session, family_id, now)`, `reserve_oos_exposure(session, experiment_id, now)`, `load_lineage_oos_trial_count(...)`, database-derived `persist_champion_comparison(session, experiment_id, champion_backtest_run_id, challenger_backtest_run_id, now)`.
- Changes: `StrategyRNDJobPayloadV1` adds nullable, hash-bound `candidate_state` and `candidate_version` for `evaluate_candidate` only.

- [x] **Step 1: Write 24-candidate shortlist red test**

Seed 24 literal validation results whose hand-computed ordering has tie cases. Assert:

```python
assert selected_ordinals == (3, 7, 11)
assert count_state("OOS_SELECTED") == 3
assert count_state("OOS_NOT_SELECTED") == 21
assert family_projection.current_state == "BACKTEST_RUNNING"
```

Repeat the final result concurrently from two sessions and assert one immutable result and the same shortlist.

- [x] **Step 2: Run shortlist test and observe RED**

```bash
uv run --package nexus-engine pytest \
  engine/tests/integration/test_evolution_shortlist_pg.py -q
```

Expected: FAIL because validation facts and production shortlist finalization do not exist.

- [x] **Step 3: Implement immutable validation and shortlist finalization**

Persist terminal strategy attempt and validation result in one transaction, transition only that candidate, then lock the family. Run Top-3 only when all 24 candidate projections are `VALIDATION_PASSED|VALIDATION_FAILED`; load metrics from the database and never accept a caller-supplied shortlist.

Update `_due_candidate_jobs()` to target candidate projections, not `StrategyExperimentRecord.status`. A queued/running Job for the same experiment, candidate state and version is a duplicate; a terminal Job from an earlier state is not.

- [x] **Step 4: Write OOS reservation, dedup, crash, replay and concurrency red tests**

Cover:

```python
first = await reserve_oos_exposure(session, experiment_a, now=NOW)
second = await reserve_oos_exposure(session, experiment_b_same_expression, now=LATER)
assert first.exposure_id == second.exposure_id
assert first.ordinal == second.ordinal == 1
assert await load_lineage_oos_trial_count(session, lineage_id) == 1
```

Commit reservation, simulate an exception before backtest persistence, retry and assert the count remains one. Run two first-time reservations concurrently and assert one exposure, one ordinal and two experiment links.

- [x] **Step 5: Implement OOS reservation and remove caller-owned trial count**

`persist_champion_comparison()` must load the experiment exposure link and locked lineage projection, then require:

```python
protocol_attempts == lineage_projection.cumulative_oos_trial_count
comparison_report.cumulative_oos_trial_count == protocol_attempts
```

Remove `champion_generation` and `cumulative_oos_trial_count` parameters from every call site. Insert `strategy_oos_evidence_links` with the final comparison and both backtest runs. Reject a comparison when no committed exposure exists.

- [x] **Step 6: Run Task 3 focused suites GREEN**

```bash
uv run --package nexus-engine pytest \
  engine/tests/unit/evolution/test_evaluation.py \
  engine/tests/integration/test_evolution_shortlist_pg.py \
  engine/tests/integration/test_evolution_oos_exposure_pg.py \
  engine/tests/integration/test_evolution_backtest_pg.py \
  engine/tests/integration/test_evolution_promotion_gate_pg.py \
  engine/tests/integration/test_evolution_jobs_pg.py -q
uv run --package nexus-engine ruff check engine/src/nexus/evolution engine/tests/unit/evolution
uv run --package nexus-engine mypy engine/src/nexus/evolution
```

Expected: PASS; `rg "cumulative_oos_trial_count=" engine/src/nexus/evolution` finds no comparison call that supplies a caller count.

- [x] **Step 7: Commit Task 3**

```bash
git add engine/src/nexus/evolution/evaluation.py \
  engine/src/nexus/evolution/repository.py \
  engine/src/nexus/evolution/jobs.py \
  engine/src/nexus/evolution/service.py \
  engine/tests/unit/evolution/test_evaluation.py \
  engine/tests/integration/test_evolution_shortlist_pg.py \
  engine/tests/integration/test_evolution_oos_exposure_pg.py \
  engine/tests/integration/test_evolution_backtest_pg.py \
  engine/tests/integration/test_evolution_promotion_gate_pg.py \
  engine/tests/integration/test_evolution_jobs_pg.py
git commit -m "fix(engine): make OOS selection and trials authoritative"
```

### Task 4: Rebuild registry head from events and activations

**Files:**
- Modify: `engine/src/nexus/evolution/registry.py`
- Modify: `engine/src/nexus/evolution/read_service.py`
- Modify: `engine/tests/unit/evolution/test_registry.py`
- Modify: `engine/tests/integration/test_evolution_registry_pg.py`
- Modify: `engine/tests/integration/test_evolution_registry_concurrency_pg.py`
- Modify: `engine/tests/integration/test_evolution_db_guards.py`

**Interfaces:**
- Produces: `RegistryReplayStateV1`, `rebuild_registry_head(events, activations)`, `registry_head_is_valid(head, events, activations)`.
- Changes: `activate_pending_transition()` appends `StrategyRegistryActivationRecord` before updating the mutable head.

- [x] **Step 1: Write pure replay counterexample tests**

Build literal GENESIS, PAUSE, RESUME, PROMOTE and activation facts. Assert the rebuilt state has exact active definition/generation, pause, pending, last event, sequence and `version=len(events)+len(activations)`. Mutate only one head field at a time and assert validation fails.

```python
for field, forged in (
    ("active_definition_id", unrelated_id),
    ("auto_promotion_paused", True),
    ("pending_event_id", promotion_event.id),
    ("version", expected.version + 1),
):
    assert not registry_head_is_valid(replace(head, **{field: forged}), events, activations)
```

- [x] **Step 2: Run registry unit tests and observe RED**

```bash
uv run --package nexus-engine pytest engine/tests/unit/evolution/test_registry.py -q
```

Expected: FAIL because current chain verification checks hashes/last event but does not replay the head.

- [x] **Step 3: Implement full replay and activation persistence**

Replace `registry_chain_is_valid()` internals with full replay. Requesting PROMOTE/ROLLBACK leaves a pending transition; PAUSE/RESUME/VETO advance event sequence without making the transition effective; activation makes exactly the referenced transition effective and clears pending. Reject an activation whose event is not the current pending transition, whose decision session precedes the effective session, or whose from definition/generation no longer matches.

The PostgreSQL head guard from Task 1 must compare new head fields with facts visible in the same transaction. Add direct SQL tests for same-generation active swaps, pause flips and forged pending bindings; all must return SQLSTATE `23514`.

- [x] **Step 4: Verify promotion, rollback, open-order and concurrency flows GREEN**

```bash
uv run --package nexus-engine pytest \
  engine/tests/unit/evolution/test_registry.py \
  engine/tests/integration/test_evolution_registry_pg.py \
  engine/tests/integration/test_evolution_registry_concurrency_pg.py \
  engine/tests/integration/test_evolution_db_guards.py -q
```

Assert pending orders create no activation and do not change the active champion. Replay the same boundary after successful activation and assert one activation row and one effective head.

- [x] **Step 5: Commit Task 4**

```bash
git add engine/src/nexus/evolution/registry.py \
  engine/src/nexus/evolution/read_service.py \
  engine/tests/unit/evolution/test_registry.py \
  engine/tests/integration/test_evolution_registry_pg.py \
  engine/tests/integration/test_evolution_registry_concurrency_pg.py \
  engine/tests/integration/test_evolution_db_guards.py
git commit -m "fix(engine): rebuild strategy registry from immutable facts"
```

### Task 5: Build the restricted production operation executor and CLI

**Files:**
- Create: `engine/src/nexus/backtest/authority_loader.py`
- Create: `engine/src/nexus/evolution/validation.py`
- Create: `engine/src/nexus/evolution/authority.py`
- Create: `engine/src/nexus/evolution/executor.py`
- Modify: `engine/src/nexus/evolution/worker.py`
- Modify: `engine/src/nexus/evolution/jobs.py`
- Modify: `engine/src/nexus/evolution/service.py`
- Modify: `engine/pyproject.toml`
- Create: `engine/tests/integration/test_backtest_authority_loader_pg.py`
- Create: `engine/tests/integration/test_evolution_executor_pg.py`
- Modify: `engine/tests/integration/test_evolution_worker_pg.py`
- Create: `engine/tests/unit/evolution/test_executor_capabilities.py`

**Interfaces:**
- Produces: `load_point_in_time_dataset(session, baseline_run_id, as_of)`, `run_candidate_validation_v1(strategy, data, protocol)`, `DatabaseStrategyRNDAuthorityResolver`, `StrategyRNDExecutor`, `build_strategy_rnd_executor(settings, responses_client=None)`, CLI `nexus-engine-strategy-rnd-worker`.
- Consumes: Tasks 2–4 verified state, shortlist/OOS functions, existing backtest/Shadow/handover/registry services.

- [ ] **Step 1: Write authority-loader round-trip red test**

Seed one real test authority bundle with existing helpers, persist a baseline run, then load it through the new production loader. Assert the reconstructed `PointInTimeDataset` and `BacktestProtocol` reproduce the stored baseline `result_hash` when run with the stored code version. Tamper one exact-set member and assert `BACKTEST_AUTHORITY_INCOMPLETE`.

- [ ] **Step 2: Run loader test and observe RED**

```bash
uv run --package nexus-engine pytest \
  engine/tests/integration/test_backtest_authority_loader_pg.py -q
```

Expected: FAIL because no production authority loader exists.

- [ ] **Step 3: Implement the point-in-time authority loader and validation evaluator**

Load only IDs enumerated by `BacktestAuthorityBundleRecord` and its member tables. Reconstruct calendar, raw/all price authorities and bars, universe snapshots, sector classifications, corporate actions/delistings, instrument metadata and benchmarks. Recompute every collection and bundle content hash and require every `available_at <= as_of`.

`run_candidate_validation_v1()` must call the deterministic strategy interpreter with no Agent in its session loop and return a frozen report with:

```python
validation_sharpe = mean(valid_window_sharpes)
validation_max_drawdown = min(valid_window_drawdowns)
validation_cost = sum(valid_window_total_costs)
authority_complete = bool(valid_windows) and not incomplete_reasons
```

No session after the frozen validation cutoff may be present in the evaluator input.

- [ ] **Step 4: Write real executor red tests**

Use a fixed `StrategyRNDRuntime`, but do not inject a fake operation executor. Invoke `build_strategy_rnd_executor()` and assert operation transitions:

```text
generate_family -> 24 candidate projections
24 validation Jobs -> exactly 3 OOS_SELECTED
3 reservation Jobs -> committed exposure ordinals
3 OOS Jobs -> immutable comparisons/decisions
historical PASS -> Strategy Shadow group
63 advances -> finalization
finalize -> handover + pending internal-paper promotion
activation boundary -> effective internal-paper champion
rollback request + activation -> previous champion
```

Use a small deterministic fixture authority that still passes the real loader and deterministic services. Assert paper orders, broker snapshots and live execution facts remain unchanged.

- [ ] **Step 5: Implement the four-operation executor**

`StrategyRNDExecutor.__call__(context, payload)` dispatches only the four enum values. It resolves all IDs from the current transaction, verifies candidate state/version and maps only stable domain outcomes to `JobHandlerResult`. Unknown exceptions are not serialized into Job/audit output.

Production runtime rules:

```python
if settings.environment is Environment.PRODUCTION:
    require settings.agent_runtime_mode is AgentRuntimeMode.RESPONSES
    require explicit model authority and API key
else:
    allow fixed fixture runtime without network
```

Add:

```toml
nexus-engine-strategy-rnd-worker = "nexus.evolution.worker:main"
```

`worker.main()` constructs `EngineSettings`, calls `build_strategy_rnd_executor(settings)`, and runs forever. No CLI argument may accept strategy DSL, policy thresholds, account, Broker, order or live flag.

- [ ] **Step 6: Add capability static test and run executor suites GREEN**

The static test parses imports in `nexus/evolution/executor.py`, `authority.py`, `validation.py`, and `worker.py` and rejects module names containing `broker`, `fosun`, `account`, `order_create`, `live_write`. It also instantiates the registry and asserts its only queue is `strategy_rnd`.

Run:

```bash
uv run --package nexus-engine pytest \
  engine/tests/unit/evolution/test_executor_capabilities.py \
  engine/tests/integration/test_backtest_authority_loader_pg.py \
  engine/tests/integration/test_evolution_executor_pg.py \
  engine/tests/integration/test_evolution_worker_pg.py -q
uv run --package nexus-engine ruff check \
  engine/src/nexus/backtest/authority_loader.py \
  engine/src/nexus/evolution/validation.py \
  engine/src/nexus/evolution/authority.py \
  engine/src/nexus/evolution/executor.py \
  engine/src/nexus/evolution/worker.py
uv run --package nexus-engine mypy \
  engine/src/nexus/backtest/authority_loader.py \
  engine/src/nexus/evolution
```

Expected: PASS; the worker test calls the real executor and no longer defines a fake `execute()` callback.

- [ ] **Step 7: Commit Task 5**

```bash
git add engine/src/nexus/backtest/authority_loader.py \
  engine/src/nexus/evolution/validation.py \
  engine/src/nexus/evolution/authority.py \
  engine/src/nexus/evolution/executor.py \
  engine/src/nexus/evolution/worker.py \
  engine/src/nexus/evolution/jobs.py \
  engine/src/nexus/evolution/service.py \
  engine/pyproject.toml \
  engine/tests/unit/evolution/test_executor_capabilities.py \
  engine/tests/integration/test_backtest_authority_loader_pg.py \
  engine/tests/integration/test_evolution_executor_pg.py \
  engine/tests/integration/test_evolution_worker_pg.py
git commit -m "feat(engine): execute the restricted strategy evolution loop"
```

### Task 6: Strictly verify and batch the read model

**Files:**
- Modify: `engine/src/nexus/evolution/evaluation.py`
- Modify: `engine/src/nexus/evolution/shadow/service.py`
- Modify: `engine/src/nexus/evolution/registry.py`
- Modify: `engine/src/nexus/evolution/read_service.py`
- Modify: `engine/src/nexus/api/schemas/evolution.py`
- Modify: `engine/tests/integration/test_evolution_read_api_pg.py`
- Create: `engine/tests/integration/test_evolution_read_scale_pg.py`

**Interfaces:**
- Produces public validators `validate_champion_comparison_record`, `validate_promotion_decision_record`, `validate_shadow_finalization_record`, `validate_handover_decision_record`.
- Changes `ShadowProgressResponse` to include `evaluation_sessions`, `grace_sessions`, `grace_elapsed_sessions`.

- [ ] **Step 1: Write identity/hash mismatch red tests**

Create schema-valid JSON with one mismatched field at a time: comparison ID, experiment ID, policy ID, comparison hash, decision hash, finalization hash and Shadow group ID. The detail endpoint must return partial/stale, `write_enabled=false`, and no fabricated metrics.

- [ ] **Step 2: Run read API test and observe RED**

```bash
uv run --package nexus-engine pytest \
  engine/tests/integration/test_evolution_read_api_pg.py -q
```

Expected: at least one mismatch is incorrectly reported as verified by the current schema-only reader.

- [ ] **Step 3: Expose and reuse strict validators**

Move the existing private comparison and decision checks behind public functions; add equivalent Shadow/handover checks. The read service must call these functions and verify candidate projection/family aggregate plus registry activations. Derive Shadow status only from finalization status; `group.status` is creation metadata.

- [ ] **Step 4: Batch integrity reads and add fixed-query scale test**

Seed 500 terminal families with 24 candidate projections and 2,000 registry facts. Attach a SQLAlchemy query counter. One overview call must execute no more than 12 SQL statements independent of family count; assert no query is issued inside a per-family loop. Keep stable `(created_at,id)` pagination and only materialize the requested 20 experiment/history responses.

- [ ] **Step 5: Run read tests GREEN**

```bash
uv run --package nexus-engine pytest \
  engine/tests/integration/test_evolution_read_api_pg.py \
  engine/tests/integration/test_evolution_read_scale_pg.py \
  engine/tests/integration/test_evolution_api_pg.py -q
uv run --package nexus-engine ruff check \
  engine/src/nexus/evolution/read_service.py \
  engine/src/nexus/api/schemas/evolution.py
uv run --package nexus-engine mypy \
  engine/src/nexus/evolution/read_service.py \
  engine/src/nexus/api/schemas/evolution.py
```

- [ ] **Step 6: Commit Task 6**

```bash
git add engine/src/nexus/evolution/evaluation.py \
  engine/src/nexus/evolution/shadow/service.py \
  engine/src/nexus/evolution/registry.py \
  engine/src/nexus/evolution/read_service.py \
  engine/src/nexus/api/schemas/evolution.py \
  engine/tests/integration/test_evolution_read_api_pg.py \
  engine/tests/integration/test_evolution_read_scale_pg.py
git commit -m "fix(api): verify strategy evolution evidence identities"
```

### Task 7: Close Web ETag, Shadow progress, and modal accessibility gaps

**Files:**
- Modify: `contracts/openapi.yaml`
- Regenerate: `app/lib/generated/schema.d.ts`
- Regenerate: `app/lib/generated/runtime-contract.ts`
- Modify: `app/(protected)/strategies/page.tsx`
- Modify: `tests/unit/strategy-evolution/fixtures.ts`
- Modify: `tests/unit/strategies/page.test.tsx`
- Modify: `tests/unit/strategies/runtime-boundary.test.ts`
- Modify: `tests/e2e/strategy-evolution.spec.ts`

**Interfaces:**
- Consumes Task 6 Shadow progress fields and ETag semantics.
- Produces strict overview/detail authority binding and accessible rollback modal behavior.

- [ ] **Step 1: Write Web red tests**

Add Vitest cases where detail ETag is missing or differs from overview; both must render partial, clear detail evidence and disable all mutations. Add manual experiment-selection mismatch. Assert Shadow copy is `21 / 63 sessions` and shows `宽限期 5 sessions` separately.

Add Playwright keyboard assertions:

```ts
await rollback.click();
await expect(confirm).toBeFocused();
await page.keyboard.press("Tab");
await expect(cancel).toBeFocused();
await page.keyboard.press("Tab");
await expect(confirm).toBeFocused();
await confirm.click();
await expect(rollback).toBeFocused();
```

- [ ] **Step 2: Run Web tests and observe RED**

```bash
npm test -- tests/unit/strategies/page.test.tsx \
  tests/unit/strategies/runtime-boundary.test.ts
npx playwright test tests/e2e/strategy-evolution.spec.ts --project=chromium
```

Expected: missing detail ETag is accepted, progress shows 68, and Tab can escape or confirm fails to restore focus.

- [ ] **Step 3: Implement strict ETag and modal behavior**

Require `nextEtag !== null && detailEtag !== null && detailEtag === nextEtag` on initial refresh and selection. On mismatch set detail null, keep the last verified overview read-only, enter partial and update the evidence ref accordingly.

Use a dialog ref and `showModal()` after mount. Trap Tab between the two action buttons, and route Escape, cancel, confirm and action completion through one `closeRollback()` that restores `rollbackTriggerRef`.

Use `shadow.evaluation_sessions` for the progress denominator; show `grace_sessions` and `grace_elapsed_sessions` as separate text.

- [ ] **Step 4: Regenerate contracts and run Web GREEN**

```bash
npm run generate:contracts
npm run check:generated
npm test -- tests/unit/strategies/page.test.tsx \
  tests/unit/strategies/runtime-boundary.test.ts \
  tests/unit/strategies/routes.test.ts
npm run lint
npm run build
npx playwright test tests/e2e/strategy-evolution.spec.ts --project=chromium
```

Expected: all PASS; Axe critical/serious findings remain zero and no real-trading control appears.

- [ ] **Step 5: Commit Task 7**

```bash
git add contracts/openapi.yaml \
  app/lib/generated/schema.d.ts \
  app/lib/generated/runtime-contract.ts \
  'app/(protected)/strategies/page.tsx' \
  tests/unit/strategy-evolution/fixtures.ts \
  tests/unit/strategies/page.test.tsx \
  tests/unit/strategies/runtime-boundary.test.ts \
  tests/e2e/strategy-evolution.spec.ts
git commit -m "fix(web): bind strategy evidence and modal authority"
```

### Task 8: Final closed-loop acceptance and independent review

**Files:**
- Modify: `docs/superpowers/plans/2026-08-27-nexus-phase-2-independent-review-remediation.md`
- Modify: `docs/superpowers/reviews/2026-08-27-nexus-phase-2-strategy-evolution-review.md`

**Interfaces:**
- Consumes all prior tasks.
- Produces final exact-tree verification record, independent review evidence, and a clean committed branch.

- [ ] **Step 1: Run migration round trip on a fresh database**

Run `0022 -> 0023 -> 0022 -> 0023`, verify one head, then insert each new fact class and prove downgrade refuses with SQLSTATE `23514`. Record the temporary Compose project name; never print credentials.

- [ ] **Step 2: Run exact final-tree Engine verification**

```bash
uv run --package nexus-engine ruff check engine/src engine/tests integrations
uv run --package nexus-engine mypy engine/src
uv run --package nexus-engine pytest \
  engine/tests/unit engine/tests/property engine/tests/evals engine/tests/contract \
  engine/tests/test_health.py -q
uv run --package nexus-engine pytest engine/tests/integration -q
```

If the integration suite exceeds one worker lease/window, partition only by mutually exclusive file lists and record the exact total. Zero unexpected `nexus_test_%` databases may remain.

- [ ] **Step 3: Run exact final-tree Web verification**

```bash
npm run check:generated
npm test
npm run lint
npm run build
npx playwright test
```

- [ ] **Step 4: Run safety and fact assertions**

Verify:

```text
API key/private key header matches: 0
evolution Broker/Fosun/account/order-create/live-write imports: 0
Phase 2 .openai/hosting.json changes: 0
paper order and broker snapshot delta in closed-loop test: 0
registry replay valid: true
candidate/family projections valid: true
orphan test databases: 0
git diff --check: clean
```

- [ ] **Step 5: Request a fresh read-only independent review**

Provide the reviewer the remediation spec, this plan, the complete Phase 2 diff and exact test evidence. Require findings classified Critical/Important/Minor and explicitly inspect: production executor, 24→3 shortlist, OOS crash/replay/concurrency, projection replay, registry activation, read identity, ETag/modal, Broker/live boundary.

Every reproduced Critical or Important finding gets a red regression test and fix before repeating Steps 2–5. Do not mark completion based only on test counts or the existence of a review task.

- [ ] **Step 6: Update acceptance artifacts and commit**

Mark plan checkboxes from actual evidence only. Update the review record with exact commands, counts, commit hash, residual Minor findings and the independent review result.

```bash
git add docs/superpowers/plans/2026-08-27-nexus-phase-2-independent-review-remediation.md \
  docs/superpowers/reviews/2026-08-27-nexus-phase-2-strategy-evolution-review.md
git commit -m "docs: record phase 2 remediation acceptance"
```

- [ ] **Step 7: Clean only task-created temporary resources**

Stop the exact temporary Compose project with its explicit project name and remove only its task-created volumes. Confirm the worktree is clean. Do not merge, push, deploy or delete the worktree as part of this task.
