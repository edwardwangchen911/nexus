# NEXUS Phase 2 Strategy Evolution Lab Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task with a single execution owner. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a bounded, auditable strategy-evolution loop that proposes registered search axes, evaluates deterministic challengers, runs an isolated Strategy Shadow, automatically promotes only the internal-paper champion, and supports append-only rollback without touching broker/live execution.

**Architecture:** Add an isolated `nexus.evolution` domain whose immutable facts reference the existing DSL, point-in-time backtest authorities, simulation kernel, hard-risk service, Job queue, outbox, and audit chain. Agent output ends at a strict experiment proposal; deterministic code owns candidate generation, evaluation, Shadow, promotion, registry concurrency, Forward binding, and rollback. Preserve all Phase 0–1 entry-point semantics and expose Phase 2 through a new typed API/BFF surface and the existing `/strategies` page.

**Tech Stack:** Python 3.12, Pydantic 2, SQLAlchemy 2 async ORM, Alembic, PostgreSQL, FastAPI, pytest/pytest-asyncio/Hypothesis, OpenAI Responses strict structured output, Ruff, Mypy, uv, React 19, TypeScript 5.9, vinext, Vitest/Testing Library, Playwright, Axe.

**Spec:** `docs/superpowers/specs/2026-08-27-nexus-phase-2-strategy-evolution-lab-design.md`

## Global Constraints

- Create a new isolated `codex/nexus-phase-2` worktree from the commit containing this plan; that commit must descend from the approved design at `f0336b6`.
- Use one execution owner for the worktree; do not dispatch subagents unless the user explicitly changes that choice.
- Live execution remains false; do not load broker composition, call Fosun, access account facts, deploy, or read/modify credentials.
- Agent has four fixed read-only evolution tools and cannot receive a database session, Job repository, filesystem, network, code interpreter, provider client, broker client, or arbitrary tool name.
- Phase 2A stays US-equity, long-only, weekly, next-session-open, unlevered, with existing universe/cost/risk authorities.
- Keep `backtest_runs.promotable=false` and `historical_agent_overlay=false`; do not drop `phase01_promotion_locked`.
- The only new migration is `0022_strategy_evolution` over `0021_manual_job_retry_allowance`; never edit an existing migration.
- All Decimal API fields are strings, datetimes are UTC ISO 8601, sessions are `YYYY-MM-DD`, and event/authority/content identities are lowercase SHA-256.
- Every implementation task starts with a failing test, ends with focused regression plus Ruff/Mypy or Web type/lint checks, and gets a local commit.
- Every real/broker write, real order, deployment, secret/permission action, paid action, destructive action, or Git-history rewrite is a stop condition requiring user confirmation.

---

## File Map

### Engine domain

- `engine/src/nexus/evolution/models.py`: frozen proposal, mutation, policy, gate, state, registry and command value objects.
- `engine/src/nexus/evolution/policies.py`: immutable `candidate_budget_policy_v1`, `evolution_search_space_v1`, `promotion_policy_v1` loaders and validators.
- `engine/src/nexus/evolution/candidate.py`: stable candidate enumeration, patch application, canonical hashes and deduplication.
- `engine/src/nexus/evolution/agent_tools.py`: four strict read-only strategy R&D tools.
- `engine/src/nexus/evolution/agent.py`: fixed Prompt/model structured-output runtime and proposal validation.
- `engine/src/nexus/evolution/repository.py`: immutable experiment/event/attempt persistence and state projection rebuild.
- `engine/src/nexus/evolution/service.py`: family admission, candidate evaluation orchestration, veto and experiment reads.
- `engine/src/nexus/evolution/evaluation.py`: validation shortlist, paired champion comparison and historical promotion gates.
- `engine/src/nexus/evolution/shadow/`: independent Strategy Shadow models, authority, metrics, repository, service and final gate.
- `engine/src/nexus/evolution/registry.py`: registry CAS, pause/resume, promotion, pending transition and rollback.
- `engine/src/nexus/evolution/handover.py`: current internal-paper transition-cost and hard-risk preflight.
- `engine/src/nexus/evolution/jobs.py`: strict `strategy.rnd` payload and bounded handler.
- `engine/src/nexus/strategy/interpreter.py`: deterministic `CompiledStrategy` evaluator shared by challenger backtest and Forward decisions.

### Persistence and interfaces

- `engine/src/nexus/persistence/models/evolution.py`: ORM mapping for all Phase 2 facts/projections.
- `engine/alembic/versions/0022_strategy_evolution.py`: schema, constraints, triggers, genesis registry and old Forward-decision backfill.
- `engine/src/nexus/api/schemas/evolution.py`: read/write API contracts.
- `engine/src/nexus/api/routes/evolution.py`: HMAC-protected read and bounded command endpoints.
- `app/lib/strategy-evolution/bff.ts`: browser-safe request/response guards.
- `app/api/v1/strategy-evolution/**/route.ts`: same-origin BFF routes.
- `app/(protected)/strategies/page.tsx`: champion, experiment, comparison, Shadow, gates and registry history workbench.
- `app/styles/tokens.css`: responsive Strategy Evolution layout using existing tokens.

---

### Task 1: Define Phase 2 contracts, policies, and deterministic candidates

**Files:**
- Create: `contracts/strategy/strategy-evolution.schema.json`
- Create: `engine/src/nexus/evolution/__init__.py`
- Create: `engine/src/nexus/evolution/models.py`
- Create: `engine/src/nexus/evolution/policies.py`
- Create: `engine/src/nexus/evolution/candidate.py`
- Modify: `engine/pyproject.toml` force-include section
- Test: `engine/tests/unit/evolution/test_models.py`
- Test: `engine/tests/unit/evolution/test_candidate.py`
- Test: `engine/tests/property/test_evolution_candidate_properties.py`

**Interfaces:**
- Produces: `StrategyExperimentProposalV1`, `StrategyCandidateDeltaV1`, `CandidateBudgetPolicyV1`, `EvolutionSearchSpaceV1`, `PromotionPolicyV1`, `generate_candidate_deltas(proposal, parent, policy) -> tuple[StrategyCandidateDeltaV1, ...]`, and `apply_candidate_delta(parent_document, delta, search_space) -> dict[str, object]`.
- Consumes: existing `CompiledStrategy`, `compile_strategy`, canonical JSON and SHA-256 helpers.

- [x] **Step 1: Write strict contract and policy red tests**

```python
def test_proposal_rejects_unregistered_or_executable_fields() -> None:
    with pytest.raises(ValidationError):
        StrategyExperimentProposalV1.model_validate({
            "schema_version": "strategy_experiment_proposal_v1",
            "parent_strategy_definition_id": str(uuid7()),
            "champion_generation": 1,
            "research_lineage_id": str(uuid7()),
            "hypothesis": "run python",
            "expected_mechanism": "momentum persistence",
            "invalidation_conditions": ["validation sharpe <= 0"],
            "evidence_refs": [str(uuid7())],
            "mutation_axes": [{"axis": "python", "values": ["import os"]}],
        })
```

Assert exact rejections for unknown fields, binary float, URL/path/code-like keys, zero/four axes, empty evidence, nonpositive generation, Agent-supplied bounds, arbitrary JSON paths and offsets.

- [x] **Step 2: Run the focused tests and observe missing-module failures**

Run:

```bash
uv run --package nexus-engine pytest engine/tests/unit/evolution/test_models.py engine/tests/unit/evolution/test_candidate.py engine/tests/property/test_evolution_candidate_properties.py -q
```

Expected: collection fails because `nexus.evolution` and the contract do not exist.

- [x] **Step 3: Implement frozen models and immutable v1 policies**

Use `ConfigDict(frozen=True, extra="forbid")`, Decimal strings, `Literal` schema versions and exact limits `axes<=3`, `candidate_budget=24`, `oos_budget=3`. Define registered axes only for `min_adv20_usd`, `min_close`, `target_weight`, `ranking_feature` and registered filter toggles. Policy loaders reject any database document whose canonical hash or exact version differs from the built-in v1 authority.

- [x] **Step 4: Implement stable candidate generation**

Enumerate the stable cartesian product, normalize mutation order, apply only server-owned field bindings, sort by candidate hash, deduplicate expression hashes and cap at 24. The returned strategy version is service-owned (`challenger_<hash-prefix>`) and the full document must compile with the extended feature registry.

- [x] **Step 5: Add property tests for stability and boundary closure**

For arbitrary proposal axis ordering, assert identical output hashes; assert no result changes `universe`, `rebalance`, `fill_timing`, `hard_risk_exit`, source/cost/risk policy fields or inserts a positive/negative offset. Assert all emitted documents compile and no more than 24 are returned.

- [x] **Step 6: Verify and commit**

```bash
uv run --package nexus-engine pytest engine/tests/unit/evolution/test_models.py engine/tests/unit/evolution/test_candidate.py engine/tests/property/test_evolution_candidate_properties.py -q
uv run --package nexus-engine ruff check engine/src/nexus/evolution engine/tests/unit/evolution engine/tests/property/test_evolution_candidate_properties.py
uv run --package nexus-engine mypy engine/src/nexus/evolution
git add contracts/strategy/strategy-evolution.schema.json engine/pyproject.toml engine/src/nexus/evolution engine/tests/unit/evolution engine/tests/property/test_evolution_candidate_properties.py
git commit -m "feat(engine): define bounded strategy evolution candidates"
```

### Task 2: Build the generic deterministic strategy interpreter with baseline semantic equivalence

**Files:**
- Create: `engine/src/nexus/strategy/interpreter.py`
- Modify: `engine/src/nexus/strategy/dsl.py:39-445`
- Modify: `engine/src/nexus/backtest/engine.py:978-1145`
- Modify: `engine/src/nexus/backtest/__init__.py`
- Test: `engine/tests/unit/strategy/test_interpreter.py`
- Test: `engine/tests/unit/backtest/test_challenger_engine.py`
- Test: `engine/tests/property/test_evolution_no_lookahead.py`
- Modify test: `engine/tests/unit/backtest/test_engine.py`
- Create fixture: `engine/tests/fixtures/backtest/baseline_v1_semantic_golden.json`

**Interfaces:**
- Consumes: `CompiledStrategy`, `StrategyDataSnapshot`, frozen `PointInTimeDataset`, `BacktestProtocol`.
- Produces: `CompiledStrategyDecision`, `evaluate_compiled_strategy(strategy, snapshot, decision_session)`, pure `simulation_core_v1(strategy, data, protocol)`, and `run_challenger_backtest_v1(strategy, data, protocol, experiment_attempt_count)`.

- [ ] **Step 1: Write equivalence and challenger red tests**

```python
def test_interpreter_is_baseline_v1_equivalent(baseline_snapshot) -> None:
    compiled = compile_strategy(baseline_v1_document(), baseline_feature_registry())
    generic = evaluate_compiled_strategy(compiled, baseline_snapshot, baseline_snapshot.decision_session)
    legacy = evaluate_baseline_v1(baseline_snapshot, baseline_snapshot.decision_session)
    assert generic.as_legacy_baseline() == legacy
```

Before changing any hash-covered source, generate and review `baseline_v1_semantic_golden.json` from the current `run_backtest()`. The fixture stores the complete canonical result plus its original identity. Add an assertion that the extracted core returns a byte-identical canonical payload after excluding only `code_hash` and `result_hash`; assert that hash-covered source changes produce a new valid `code_hash` and `result_hash` while the frozen historical identity is never reused. Add challenger tests for allowed threshold/ranking mutations and rejection of a forged compiled hash.

- [ ] **Step 2: Run the focused suite and confirm missing interpreter/entry-point failures**

```bash
uv run --package nexus-engine pytest engine/tests/unit/strategy/test_interpreter.py engine/tests/unit/backtest/test_challenger_engine.py engine/tests/property/test_evolution_no_lookahead.py -q
```

- [ ] **Step 3: Extend feature metadata without changing baseline identity**

Add compatibility class, inputs, unit, minimum history, availability delay, missing policy and implementation hash to the evolution registry identity. Preserve the exact legacy `baseline_feature_registry()` identity used by Phase 0–1 hashes; expose a separate `evolution_feature_registry_v1()` for Phase 2.

- [ ] **Step 4: Implement the expression/policy interpreter**

Evaluate registered features from the frozen snapshot, boolean/comparison nodes, ranking, selection, target/cash weights and trend/ranking exits. Use Decimal only, reject unavailable features, and preserve baseline tie-breaking by `(rank_value desc, instrument_id asc)` and existing sector/ETF caps.

- [ ] **Step 5: Extract the simulation core and add challenger entry point**

Keep public `run_backtest()` baseline-only and `promotable=False`. Route its internal simulation through `simulation_core_v1`; add `run_challenger_backtest_v1` that verifies a compiled evolution strategy and passes lineage attempt count to `calculate_metrics`. Do not add Agent calls to any historical loop.

- [ ] **Step 6: Verify semantic byte equivalence, identity renewal, no-lookahead, and commit**

```bash
uv run --package nexus-engine pytest engine/tests/unit/strategy engine/tests/unit/backtest engine/tests/property/test_no_lookahead.py engine/tests/property/test_evolution_no_lookahead.py -q
uv run --package nexus-engine ruff check engine/src/nexus/strategy engine/src/nexus/backtest engine/tests/unit/strategy engine/tests/unit/backtest
uv run --package nexus-engine mypy engine/src/nexus/strategy engine/src/nexus/backtest
git add engine/src/nexus/strategy engine/src/nexus/backtest engine/tests/unit/strategy engine/tests/unit/backtest engine/tests/property/test_evolution_no_lookahead.py engine/tests/fixtures/backtest/baseline_v1_semantic_golden.json
git commit -m "feat(engine): interpret bounded challenger strategies"
```

### Task 3: Add `0022` evolution persistence and database invariants

**Files:**
- Create: `engine/src/nexus/persistence/models/evolution.py`
- Modify: `engine/src/nexus/persistence/models/__init__.py`
- Create: `engine/alembic/versions/0022_strategy_evolution.py`
- Modify: `engine/src/nexus/persistence/migrations.py`
- Modify: `engine/src/nexus/persistence/models/agents.py:25-42`
- Modify: `engine/src/nexus/persistence/models/paper.py:116-185`
- Test: `engine/tests/integration/test_evolution_migration.py`
- Test: `engine/tests/integration/test_evolution_db_guards.py`
- Modify: all migration-head assertions found by `rg -l '0021_manual_job_retry_allowance' engine/tests`

**Interfaces:**
- Produces the exact tables in spec section 6, a genesis `internal_paper` registry event/head for `baseline_v1`, and non-null `strategy_definition_id`, `registry_event_id`, `champion_generation` bindings on `strategy_decisions`.
- Consumes existing `strategy_definitions`, Agent prompt/model records, backtest facts, portfolio snapshots, policies, audit/outbox and paper decisions.

- [ ] **Step 1: Generate the migration skeleton and write red migration tests**

Run `uv run --package nexus-engine alembic -c engine/alembic.ini revision -m strategy_evolution --rev-id 0022_strategy_evolution`, then write tests that assert `down_revision == "0021_manual_job_retry_allowance"`, one head, exact tables/columns/checks/FKs/unique constraints, genesis registry generation 1, old decision backfill and downgrade refusal when Phase 2 facts exist.

- [ ] **Step 2: Prove the tests fail against the empty skeleton**

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_evolution_migration.py engine/tests/integration/test_evolution_db_guards.py -q
```

- [ ] **Step 3: Implement ORM records**

Use focused classes named `StrategyRNDRunRecord`, `StrategyRNDOutputRecord`, `StrategyEvolutionPolicyRecord`, `StrategyPromotionPolicyRecord`, `StrategyResearchLineageRecord`, `StrategyExperimentFamilyRecord`, `StrategyExperimentRecord`, `StrategyExperimentEventRecord`, `StrategyExperimentAttemptRecord`, `StrategyExperimentStateProjectionRecord`, `StrategyChampionComparisonRecord`, `StrategyPromotionDecisionRecord`, Strategy Shadow records, `StrategyRegistryEventRecord`, and `StrategyRegistryHeadRecord`. Authority records inherit `ImmutableCreatedAt`; head/projection do not.

- [ ] **Step 4: Implement migration/backfill/triggers**

Create all v1 checks, SHA-256 checks, cardinality constraints, one-active-family partial unique index, one `internal_paper` head, event sequence/hash and immutable triggers. Extend Agent prompt role allowlist with `strategy_rnd`. Backfill every existing baseline decision to the genesis definition/event/generation, replace decision uniqueness with `(account_id,decision_session)`, then add NOT NULL/FKs. Preserve `backtest_runs.phase01_promotion_locked` unchanged.

- [ ] **Step 5: Add database bypass/concurrency counterexamples**

Assert UPDATE/DELETE rejection for every authority table; duplicate active family, event sequence, candidate expression, shadow leg, session authority, registry channel and decision session fail at PostgreSQL. Assert head/projection updates reject generation/version regression and registry event hashes cannot be changed.

- [ ] **Step 6: Run round-trip, all migration tests, and commit**

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_evolution_migration.py engine/tests/integration/test_evolution_db_guards.py engine/tests/integration/test_migration.py engine/tests/integration/test_backtest_migration.py engine/tests/integration/test_paper_migration.py engine/tests/integration/test_automation_migration.py -q
uv run --package nexus-engine ruff check engine/alembic/versions/0022_strategy_evolution.py engine/src/nexus/persistence
uv run --package nexus-engine mypy engine/src/nexus/persistence
git add engine/alembic/versions/0022_strategy_evolution.py engine/src/nexus/persistence engine/tests/integration/test_evolution_migration.py engine/tests/integration/test_evolution_db_guards.py
git add -u engine/tests
git commit -m "feat(db): add strategy evolution authority"
```

### Task 4: Implement the fixed Strategy R&D Agent and experiment admission

**Files:**
- Create: `engine/src/nexus/agents/prompts/strategy_rnd-v1.md`
- Create: `engine/src/nexus/evolution/agent_tools.py`
- Create: `engine/src/nexus/evolution/agent.py`
- Create: `engine/src/nexus/evolution/repository.py`
- Create: `engine/src/nexus/evolution/service.py`
- Modify: `engine/src/nexus/agents/committee.py:19-125` only where prompt loading is shared
- Test: `engine/tests/unit/evolution/test_agent_tools.py`
- Test: `engine/tests/unit/evolution/test_agent.py`
- Test: `engine/tests/evals/test_strategy_rnd_agent.py`
- Test: `engine/tests/integration/test_evolution_admission_pg.py`

**Interfaces:**
- Produces: strict `StrategyRNDRequestV1` and `AdmittedFamily` value types, `StrategyRNDTools`, `StrategyRNDRuntime.generate(request: StrategyRNDRequestV1, tools: StrategyRNDTools) -> StrategyExperimentProposalV1`, `admit_experiment_family(session, proposal, authority, now, idempotency_key) -> AdmittedFamily`, and append-only repository methods.
- Consumes Task 1 policies/candidate generation and Task 3 records.

- [ ] **Step 1: Write read-only tool and prompt-injection red tests**

Assert tool names exactly `get_strategy_parent`, `get_evolution_search_space`, `get_training_validation_summary`, `get_sealed_experiment_summary`; unknown tools, current-family OOS, broker/account/order fields, URL/path/code payloads and non-frozen `as_of` fail before executor invocation. Add evals for “change risk limits”, “place a Fosun order”, “run Python”, future evidence, forged citations and missing evidence; expected result is rejection or abstain.

- [ ] **Step 2: Run red tests**

```bash
uv run --package nexus-engine pytest engine/tests/unit/evolution/test_agent_tools.py engine/tests/unit/evolution/test_agent.py engine/tests/evals/test_strategy_rnd_agent.py engine/tests/integration/test_evolution_admission_pg.py -q
```

- [ ] **Step 3: Implement the fixed Agent boundary**

Load only `strategy_rnd-v1.md`, one immutable model config and four strict function schemas. Use strict structured output for `StrategyExperimentProposalV1`; cap tool calls/tokens/cost with existing Agent pricing authority. Persist hashes, bounded usage and safe status; never pass SQLAlchemy, Job, filesystem, provider or broker objects into the runtime.

- [ ] **Step 4: Implement admission and immutable events**

Lock the registry head and lineage projection; require quality PASS, unpaused generation match, quarterly cooldown and no active family. Verify every evidence ref belongs to the frozen input snapshot, generate candidates deterministically, publish challenger definitions, append family/experiment/events, update projections and write safe audit/outbox in one transaction. Agent failure or abstain creates no family.

- [ ] **Step 5: Test concurrency, idempotency and trial accounting**

Run two admissions with the same and different keys; same key returns one family, different keys yield one family plus `ACTIVE_FAMILY_EXISTS`. Seal one family after OOS, create the next family in the same lineage and assert cumulative OOS count never decreases or resets after Prompt/model version changes.

- [ ] **Step 6: Verify and commit**

```bash
uv run --package nexus-engine pytest engine/tests/unit/evolution/test_agent_tools.py engine/tests/unit/evolution/test_agent.py engine/tests/evals/test_strategy_rnd_agent.py engine/tests/integration/test_evolution_admission_pg.py -q
uv run --package nexus-engine ruff check engine/src/nexus/evolution engine/src/nexus/agents engine/tests/unit/evolution engine/tests/evals/test_strategy_rnd_agent.py
uv run --package nexus-engine mypy engine/src/nexus/evolution engine/src/nexus/agents
git add engine/src/nexus/evolution engine/src/nexus/agents engine/tests/unit/evolution engine/tests/evals/test_strategy_rnd_agent.py engine/tests/integration/test_evolution_admission_pg.py
git commit -m "feat(engine): admit fixed strategy R&D experiments"
```

### Task 5: Evaluate challengers and produce historical promotion decisions

**Files:**
- Create: `engine/src/nexus/evolution/evaluation.py`
- Modify: `engine/src/nexus/backtest/persistence.py:100-440`
- Modify: `engine/src/nexus/backtest/metrics.py:32-180` only if lineage-count persistence needs an exposed helper
- Test: `engine/tests/unit/evolution/test_evaluation.py`
- Test: `engine/tests/integration/test_evolution_backtest_pg.py`
- Test: `engine/tests/integration/test_evolution_promotion_gate_pg.py`

**Interfaces:**
- Produces: `select_oos_candidates(validation_results, policy)`, `persist_champion_comparison(...) -> StrategyChampionComparisonRecord`, and `evaluate_historical_promotion(comparison, policy) -> PromotionDecisionV1`.
- Consumes Task 2 challenger backtest and Task 3/4 immutable experiment facts.

- [ ] **Step 1: Write validation-selection and gate boundary red tests**

```python
def test_double_cost_gate_compares_challenger_to_champion() -> None:
    report = evaluate_historical_promotion(
        comparison_fixture(challenger_double_return="0.04", champion_double_return="0.05"),
        promotion_policy_v1(),
    )
    assert report.gate("double_cost_excess_positive").status == "FAIL"
```

Cover exactly 7/8 OOS windows, 0.949999/0.95 DSR, Sharpe +0.149999/+0.15, return +0.009999/+0.01, drawdown -0.020001/-0.02, incomplete authority/benchmark and cumulative trial counts.

- [ ] **Step 2: Run red tests**

```bash
uv run --package nexus-engine pytest engine/tests/unit/evolution/test_evaluation.py engine/tests/integration/test_evolution_backtest_pg.py engine/tests/integration/test_evolution_promotion_gate_pg.py -q
```

- [ ] **Step 3: Implement validation shortlist and paired evaluation**

Rank validation candidates using the frozen policy tuple `(validation_sharpe desc, validation_max_drawdown desc, validation_cost asc, expression_hash asc)` and take at most 3. Run OOS candidates and the bound champion with identical authority/protocol/cost; persist both immutable backtests plus a canonical paired comparison. Never update `promotable`.

- [ ] **Step 4: Implement structured historical gates**

Return ordered `PromotionGateResultV1` rows with string Decimal values, evidence IDs and fixed reason codes. Treat missing/unknown as fail closed. Persist the decision and append the correct experiment event atomically; only all-PASS decisions can start Strategy Shadow.

- [ ] **Step 5: Verify identical authority and stale-generation handling**

Add PostgreSQL tests that reject mismatched authority/protocol/cost/parent generation. Change registry generation between comparison creation and decision persistence; assert `STALE_CHAMPION_BASELINE`, no Shadow group and no registry event.

- [ ] **Step 6: Verify and commit**

```bash
uv run --package nexus-engine pytest engine/tests/unit/evolution/test_evaluation.py engine/tests/integration/test_evolution_backtest_pg.py engine/tests/integration/test_evolution_promotion_gate_pg.py engine/tests/unit/backtest engine/tests/integration/test_backtest_writer.py -q
uv run --package nexus-engine ruff check engine/src/nexus/evolution/evaluation.py engine/src/nexus/backtest engine/tests/unit/evolution
uv run --package nexus-engine mypy engine/src/nexus/evolution/evaluation.py engine/src/nexus/backtest
git add engine/src/nexus/evolution/evaluation.py engine/src/nexus/backtest engine/tests/unit/evolution/test_evaluation.py engine/tests/integration/test_evolution_backtest_pg.py engine/tests/integration/test_evolution_promotion_gate_pg.py
git commit -m "feat(engine): gate challenger backtests against champion"
```

### Task 6: Build the isolated Strategy Shadow

**Files:**
- Create: `engine/src/nexus/evolution/shadow/__init__.py`
- Create: `engine/src/nexus/evolution/shadow/models.py`
- Create: `engine/src/nexus/evolution/shadow/canonical.py`
- Create: `engine/src/nexus/evolution/shadow/authority.py`
- Create: `engine/src/nexus/evolution/shadow/metrics.py`
- Create: `engine/src/nexus/evolution/shadow/repository.py`
- Create: `engine/src/nexus/evolution/shadow/service.py`
- Test: `engine/tests/unit/evolution/shadow/test_models.py`
- Test: `engine/tests/unit/evolution/shadow/test_metrics.py`
- Test: `engine/tests/property/test_strategy_shadow_properties.py`
- Test: `engine/tests/property/test_strategy_shadow_no_lookahead.py`
- Test: `engine/tests/integration/test_strategy_shadow_start_pg.py`
- Test: `engine/tests/integration/test_strategy_shadow_advance_pg.py`
- Test: `engine/tests/integration/test_strategy_shadow_concurrency_pg.py`

**Interfaces:**
- Produces: `start_strategy_shadow(session, experiment_id, now)`, `advance_strategy_shadow(session, group_id, requested_session, observed_at)`, `finalize_strategy_shadow(session, group_id, now)`, and `StrategyShadowFinalGateV1`.
- Consumes shared point-in-time authority selectors, Task 2 interpreter, simulation cost/fill primitives, hard-risk evaluator and Task 3 Shadow records.

- [ ] **Step 1: Write same-start, exact-session and metric red tests**

Create fixtures with one sealed internal-paper snapshot cloned into champion/challenger legs. Assert NAV=100 normalization, identical cash/positions, exactly 63 evaluation + 5 grace sessions, checkpoints `(5,21,63)`, full-fill-only order count, same authority hash per session and final gate failure for one risk violation or one missing session.

- [ ] **Step 2: Run red tests**

```bash
uv run --package nexus-engine pytest engine/tests/unit/evolution/shadow engine/tests/property/test_strategy_shadow_properties.py engine/tests/property/test_strategy_shadow_no_lookahead.py engine/tests/integration/test_strategy_shadow_start_pg.py engine/tests/integration/test_strategy_shadow_advance_pg.py engine/tests/integration/test_strategy_shadow_concurrency_pg.py -q
```

- [ ] **Step 3: Implement frozen models, hashes and metrics**

Model two roles only, Decimal NAV/cash/positions/orders/fills and fixed checkpoint ordinals. Canonical hashes cover group bindings, each session authority, both leg transitions, all order/fill facts and checkpoint metrics. Metrics include cost-after return, max drawdown, volatility, worst five sessions, turnover, fees and full-fill count.

- [ ] **Step 4: Implement start and point-in-time authority**

Lock experiment and registry head; require all historical gates PASS and matching generation. Clone one PASS/balanced internal-paper snapshot into both virtual legs, choose one complete XNYS calendar, 63+5 future sessions and exact source/cost policy. A missing/late/conflicting authority creates a failed advance attempt and permanent data-gap evidence, never a late replacement.

- [ ] **Step 5: Implement atomic advance/finalization**

Advance only the next expected session under a group row lock. Evaluate both strategies before persisting either transition; if either fails, persist neither leg state. Persist authority, states, orders, fills, checkpoint, audit and outbox in one transaction. At 63 sessions require challenger full fills >=20, no risk violation, excess return >=0 and drawdown delta >=-0.02.

- [ ] **Step 6: Add races and proposal-shadow separation tests**

Concurrent same-session advance returns one applied fact; out-of-order sessions fail; a champion generation change yields stale finalization. Assert zero writes to existing `shadow_*` Proposal tables, `paper_*` facts, broker tables and execution intents.

- [ ] **Step 7: Verify and commit**

```bash
uv run --package nexus-engine pytest engine/tests/unit/evolution/shadow engine/tests/property/test_strategy_shadow_properties.py engine/tests/property/test_strategy_shadow_no_lookahead.py engine/tests/integration/test_strategy_shadow_start_pg.py engine/tests/integration/test_strategy_shadow_advance_pg.py engine/tests/integration/test_strategy_shadow_concurrency_pg.py engine/tests/unit/shadow engine/tests/property/test_shadow_properties.py -q
uv run --package nexus-engine ruff check engine/src/nexus/evolution/shadow engine/tests/unit/evolution/shadow engine/tests/property/test_strategy_shadow_properties.py engine/tests/property/test_strategy_shadow_no_lookahead.py
uv run --package nexus-engine mypy engine/src/nexus/evolution/shadow
git add engine/src/nexus/evolution/shadow engine/tests/unit/evolution/shadow engine/tests/property/test_strategy_shadow_properties.py engine/tests/property/test_strategy_shadow_no_lookahead.py engine/tests/integration/test_strategy_shadow_start_pg.py engine/tests/integration/test_strategy_shadow_advance_pg.py engine/tests/integration/test_strategy_shadow_concurrency_pg.py
git commit -m "feat(engine): validate champion challenger strategy shadow"
```

### Task 7: Implement registry promotion, handover, Forward binding, and rollback

**Files:**
- Create: `engine/src/nexus/evolution/handover.py`
- Create: `engine/src/nexus/evolution/registry.py`
- Modify: `engine/src/nexus/strategy/forward.py:106-468`
- Modify: `engine/src/nexus/risk/canonical.py`
- Modify: `engine/src/nexus/paper/service.py:1019-1140`
- Modify: `engine/src/nexus/paper/read_service.py`
- Test: `engine/tests/unit/evolution/test_handover.py`
- Test: `engine/tests/unit/evolution/test_registry.py`
- Test: `engine/tests/integration/test_strategy_registry_pg.py`
- Test: `engine/tests/integration/test_strategy_promotion_concurrency_pg.py`
- Test: `engine/tests/integration/test_strategy_rollback_pg.py`
- Modify test: `engine/tests/integration/test_forward_decision_persistence.py`
- Modify test: `engine/tests/integration/test_paper_order_batch_pg.py`

**Interfaces:**
- Produces: `evaluate_handover(snapshot, challenger, authority, risk_policy)`, `promote_internal_paper_champion(...)`, `pause_auto_promotion(...)`, `resume_auto_promotion(...)`, `request_registry_rollback(...)`, `activate_pending_transition(...)`, and generic `ForwardStrategyDecision` bindings.
- Consumes historical/Shadow promotion decisions, paper projection, risk policy, registry head and strategy interpreter.

- [ ] **Step 1: Write handover and registry red tests**

Assert `shadow_excess_return - estimated_handover_cost_ratio >= 0`, exact generation CAS, only one concurrent promote, pause blocks automatic promote, rollback creates a new generation, no compatible target fails closed, open orders create pending state and no registry change affects an existing decision session.

- [ ] **Step 2: Run red tests**

```bash
uv run --package nexus-engine pytest engine/tests/unit/evolution/test_handover.py engine/tests/unit/evolution/test_registry.py engine/tests/integration/test_strategy_registry_pg.py engine/tests/integration/test_strategy_promotion_concurrency_pg.py engine/tests/integration/test_strategy_rollback_pg.py engine/tests/integration/test_forward_decision_persistence.py -q
```

- [ ] **Step 3: Implement handover preflight**

Use the latest PASS balanced paper snapshot and frozen price/cost authorities to compute challenger next targets, transition turnover/cost and existing hard-risk checks. Persist evidence only; do not create paper orders. Unknown data/cost/risk produces `PROMOTION_WAITING_HANDOVER`.

- [ ] **Step 4: Implement registry CAS and event chain**

Lock the single head, verify expected generation/version, decision bindings and pause state, append canonical event, update pending/active projection, audit and outbox atomically. PROMOTE/ROLLBACK increment champion generation only when effective; PAUSE/RESUME increment event sequence only. Concurrent loser becomes `STALE_CHAMPION_BASELINE`.

- [ ] **Step 5: Implement safe-boundary activation and rollback**

Check nonterminal paper orders before activation. Keep promote/rollback pending without cancellation; activate after orders terminate and the next exchange decision boundary is reached. Rollback selects the most recent prior compatible verified definition, ignores economic profitability, and never mutates historical paper facts.

- [ ] **Step 6: Generalize Forward binding without caller targets**

Add definition/event/generation fields to `ForwardStrategyDecision`, its canonical hash and persistence. Load the active registry head at session start, compile/verify the definition, call `evaluate_compiled_strategy`, and preserve unique `(account_id,decision_session)`. Existing baseline decisions must serialize identically except for the new explicit registry binding.

- [ ] **Step 7: Prove no broker/live effects and commit**

Run tests that snapshot all broker/provider/live tables before promote/rollback/Forward and assert no changes. Assert every Phase 2 API/domain response carries `live_execution_enabled=False`.

```bash
uv run --package nexus-engine pytest engine/tests/unit/evolution/test_handover.py engine/tests/unit/evolution/test_registry.py engine/tests/integration/test_strategy_registry_pg.py engine/tests/integration/test_strategy_promotion_concurrency_pg.py engine/tests/integration/test_strategy_rollback_pg.py engine/tests/integration/test_forward_decision_persistence.py engine/tests/integration/test_paper_order_batch_pg.py engine/tests/unit/risk engine/tests/unit/paper -q
uv run --package nexus-engine ruff check engine/src/nexus/evolution engine/src/nexus/strategy engine/src/nexus/paper engine/src/nexus/risk
uv run --package nexus-engine mypy engine/src/nexus/evolution engine/src/nexus/strategy engine/src/nexus/paper engine/src/nexus/risk
git add engine/src/nexus/evolution/handover.py engine/src/nexus/evolution/registry.py engine/src/nexus/strategy engine/src/nexus/paper engine/src/nexus/risk engine/tests/unit/evolution/test_handover.py engine/tests/unit/evolution/test_registry.py engine/tests/integration/test_strategy_registry_pg.py engine/tests/integration/test_strategy_promotion_concurrency_pg.py engine/tests/integration/test_strategy_rollback_pg.py
git add -u engine/tests
git commit -m "feat(engine): promote and rollback internal paper champions"
```

### Task 8: Add Strategy R&D jobs, daily orchestration, audit, and observability

**Files:**
- Create: `engine/src/nexus/evolution/jobs.py`
- Modify: `engine/src/nexus/jobs/daily_payloads.py`
- Modify: `engine/src/nexus/jobs/daily_pipeline.py:200-570`
- Modify: `engine/src/nexus/jobs/models.py`
- Modify: `engine/src/nexus/jobs/daily_local.py`
- Modify: `engine/src/nexus/audit/redaction.py`
- Test: `engine/tests/unit/evolution/test_jobs.py`
- Test: `engine/tests/integration/test_evolution_jobs_pg.py`
- Modify test: `engine/tests/integration/test_daily_pipeline.py`
- Modify test: `engine/tests/integration/test_audit_service.py`

**Interfaces:**
- Produces: `StrategyRNDJobPayloadV1`, `strategy_rnd_handler(context) -> JobHandlerResult`, safe evolution audit classifications and daily enqueue hooks.
- Consumes Task 4–7 services through immutable IDs only.

- [ ] **Step 1: Write strict Job and scheduler red tests**

Assert exactly four operations, forbidden payload keys/code/URL/path rejection, one candidate per evaluate job, same-key idempotency, quarterly cooldown, one active family, pause behavior, daily Shadow advance, finalization enqueue and low-priority strategy queue isolation.

- [ ] **Step 2: Run red tests**

```bash
uv run --package nexus-engine pytest engine/tests/unit/evolution/test_jobs.py engine/tests/integration/test_evolution_jobs_pg.py engine/tests/integration/test_daily_pipeline.py -q
```

- [ ] **Step 3: Implement the bounded handler**

Resolve only database IDs and the operation enum. `generate_family` invokes fixed Agent admission, `evaluate_candidate` evaluates one experiment, `advance_shadow` advances one expected session, and `finalize_promotion` runs Shadow final gate/handover/registry. Declare `strategy_rnd` queue, existing low priority 20, strict timeout/retry/idempotency and no broker composition.

- [ ] **Step 4: Wire the daily pipeline**

At a PASS quality session, enqueue due Strategy Shadow advances. Only when no family is active, pause is false and the last family is at least one calendar quarter old may the scheduler enqueue generation. Use outbox events to enqueue downstream evaluation/finalization; repeated scheduler/outbox events return existing Jobs.

- [ ] **Step 5: Add redacted audit and safe metrics**

Classify all events listed in spec section 10.3. Persist only type/version/hash/gate/reason; reject strategy documents, hypothesis text, Prompt/tool payloads, symbol/account/order/amount fields. Expose counters/gauges without labels containing identifiers or investment data.

- [ ] **Step 6: Verify and commit**

```bash
uv run --package nexus-engine pytest engine/tests/unit/evolution/test_jobs.py engine/tests/integration/test_evolution_jobs_pg.py engine/tests/integration/test_daily_pipeline.py engine/tests/integration/test_job_worker.py engine/tests/integration/test_job_leases.py engine/tests/integration/test_audit_service.py -q
uv run --package nexus-engine ruff check engine/src/nexus/evolution/jobs.py engine/src/nexus/jobs engine/src/nexus/audit engine/tests/unit/evolution/test_jobs.py
uv run --package nexus-engine mypy engine/src/nexus/evolution/jobs.py engine/src/nexus/jobs engine/src/nexus/audit
git add engine/src/nexus/evolution/jobs.py engine/src/nexus/jobs engine/src/nexus/audit engine/tests/unit/evolution/test_jobs.py engine/tests/integration/test_evolution_jobs_pg.py
git add -u engine/tests
git commit -m "feat(engine): automate bounded strategy evolution jobs"
```

### Task 9: Expose the typed Engine API and generated contracts

**Files:**
- Create: `engine/src/nexus/api/schemas/evolution.py`
- Create: `engine/src/nexus/api/routes/evolution.py`
- Modify: `engine/src/nexus/main.py:1-100`
- Modify: `engine/src/nexus/api/errors.py`
- Modify: `engine/src/nexus/api/routes/strategies.py` only to preserve compatibility assertions
- Test: `engine/tests/unit/api/test_evolution_contracts.py`
- Test: `engine/tests/integration/test_evolution_api.py`
- Modify test: `engine/tests/contract/test_openapi.py`
- Generated: `app/lib/generated/schema.d.ts`

**Interfaces:**
- Produces all six routes in spec section 11 with strict envelopes, stable cursor, `If-Match`, `Idempotency-Key`, 200/202/409/422/503 semantics and `live_execution_enabled=False`.
- Consumes evolution service/registry reads and commands; keeps `/internal/v1/strategies` operational.

- [ ] **Step 1: Write contract and HTTP red tests**

Cover ready/empty/partial, 20-item defaults, 100-item maximum, signed cursor, Decimal strings, safe evidence, forbidden DSL fields, missing/duplicate idempotency, stale `If-Match`, active family/cooldown, veto, pause/resume, pending rollback and authority-unavailable 503. Assert raw Prompt/tool/strategy document/account/order data never appears.

- [ ] **Step 2: Run red tests**

```bash
uv run --package nexus-engine pytest engine/tests/unit/api/test_evolution_contracts.py engine/tests/integration/test_evolution_api.py engine/tests/contract/test_openapi.py -q
```

- [ ] **Step 3: Implement strict schemas and read assembler**

Create frozen extra-forbid models for champion, family, candidate counts, comparison, Shadow, gate results, registry history and action responses. Assemble only verified hash/evidence summaries; if registry chain/head/projection verification fails, return a safe partial/read-only response and block commands.

- [ ] **Step 4: Implement bounded command routes**

POST experiment accepts an empty strict body plus headers; veto accepts a fixed reason enum; control accepts pause/resume only; rollback accepts fixed reason only. Map domain reasons to exact HTTP statuses and never accept account, strategy, parameter, policy, Prompt or order fields.

- [ ] **Step 5: Generate and verify OpenAPI client**

```bash
uv run --package nexus-engine pytest engine/tests/unit/api/test_evolution_contracts.py engine/tests/integration/test_evolution_api.py engine/tests/contract/test_openapi.py -q
npm run generate:client
npm run check:generated
```

- [ ] **Step 6: Verify and commit**

```bash
uv run --package nexus-engine ruff check engine/src/nexus/api engine/tests/unit/api/test_evolution_contracts.py
uv run --package nexus-engine mypy engine/src/nexus/api
git add engine/src/nexus/api engine/src/nexus/main.py engine/tests/unit/api/test_evolution_contracts.py engine/tests/integration/test_evolution_api.py engine/tests/contract/test_openapi.py app/lib/generated/schema.d.ts
git commit -m "feat(api): expose strategy evolution evidence and controls"
```

### Task 10: Build safe BFF routes and the `/strategies` evolution workbench

**Files:**
- Create: `app/lib/strategy-evolution/bff.ts`
- Create: `app/api/v1/strategy-evolution/route.ts`
- Create: `app/api/v1/strategy-evolution/experiments/route.ts`
- Create: `app/api/v1/strategy-evolution/experiments/[experimentId]/route.ts`
- Create: `app/api/v1/strategy-evolution/experiments/[experimentId]/veto/route.ts`
- Create: `app/api/v1/strategy-evolution/registry/control/route.ts`
- Create: `app/api/v1/strategy-evolution/registry/rollback/route.ts`
- Modify: `app/lib/engine/client.ts`
- Rewrite: `app/(protected)/strategies/page.tsx`
- Modify: `app/styles/tokens.css:217-240,485-490`
- Test: `tests/unit/strategy-evolution/bff.test.ts`
- Test: `tests/unit/strategies/page.test.tsx`
- Test: `tests/unit/engine/client-evolution-boundary.test.ts`

**Interfaces:**
- Produces browser-safe GET/actions and a single accessible workbench with champion, experiment, comparison, Strategy Shadow, promotion gates and history/rollback.
- Consumes generated OpenAPI types only; browser never sees Engine HMAC.

- [ ] **Step 1: Write BFF boundary red tests**

Assert only generated evolution paths/methods are allowed, response size <=1 MiB, JSON content type, no redirects, no-store, strict action body allowlists and preserved idempotency/If-Match. Inject DSL/Prompt/account/order fields in Engine responses and assert BFF returns the generic safe failure envelope.

- [ ] **Step 2: Write page state/interaction red tests**

Cover loading/empty/ready/partial/stale/error/forbidden; live-closed badge always present; active hypothesis/search-axis summaries; champion/challenger/SPY/equal comparison; 5/21/63 and full-fill progress; ordered gate status; pause/resume; veto; rollback confirmation; stale write-disable; 409 refetch; 202 pending orders; error-summary focus and aria-live success.

- [ ] **Step 3: Run red tests**

```bash
npm run test:unit -- tests/unit/strategy-evolution/bff.test.ts tests/unit/strategies/page.test.tsx tests/unit/engine/client-evolution-boundary.test.ts
```

- [ ] **Step 4: Implement BFF validation and routes**

Use generated request/response types and existing `withNexusBff`/`engineFetch`. Browser POST bodies are fixed action enums only; server creates exact JSON bytes, forwards idempotency/version headers and sanitizes all Engine errors. Add no dependency.

- [ ] **Step 5: Implement the workbench**

Retain existing NEXUS typography/tokens/AppShell. Render the six ordered sections from spec section 12, semantic `dl/table/ol` evidence, explicit data mode/source/hash, and a fixed “实盘关闭” boundary. Use an accessible confirmation dialog for rollback without free-text input. No buy/sell/order/broker control is present.

- [ ] **Step 6: Add responsive/accessibility styles**

At 375/390 px use stacked definition cards; at 768+ use two-column evidence where safe; only immutable audit/comparison tables may have named local horizontal scroll. Maintain 44px actions, visible focus, reduced motion and no document-level overflow.

- [ ] **Step 7: Verify and commit**

```bash
npm run test:unit -- tests/unit/strategy-evolution/bff.test.ts tests/unit/strategies/page.test.tsx tests/unit/engine/client-evolution-boundary.test.ts
npm run typecheck
npm run lint
npm run check:generated
npm run build
git add app/lib/strategy-evolution app/api/v1/strategy-evolution app/lib/engine/client.ts 'app/(protected)/strategies/page.tsx' app/styles/tokens.css tests/unit app/lib/generated/schema.d.ts
git commit -m "feat(web): add strategy evolution workbench"
```

### Task 11: Run E2E, full regression, security review, and acceptance

**Files:**
- Create: `tests/e2e/strategy-evolution.spec.ts`
- Create: `docs/superpowers/reviews/2026-08-27-nexus-phase-2-strategy-evolution-review.md`
- Modify after acceptance: `.superpowers/sdd/2026-08-22-nexus-phase-0-1/progress.md`
- Modify after acceptance: this plan’s checkboxes/evidence sections

**Interfaces:**
- Produces complete, reproducible acceptance evidence and a clean commit range.
- Consumes every prior task and the design spec requirement matrix.

- [ ] **Step 1: Add E2E fixtures and red assertions**

Test 375/390/768/1024/1440 and 390x844 landscape for ready, active, partial, stale, pending promotion, rollback-pending-orders and forbidden states. Exercise experiment start, pause/resume, veto, rollback confirm, 409 refetch and unknown 202 recovery. Assert no page overflow, Axe critical/serious 0, keyboard dialog/focus return, aria-live status and absence of real/broker/order controls.

- [ ] **Step 2: Run focused E2E and fix only observed failures with red tests**

```bash
npm run test:e2e -- tests/e2e/strategy-evolution.spec.ts
```

- [ ] **Step 3: Run migration and Engine full gates**

Use the existing Docker-compatible runtime; do not install Docker Desktop. Start PostgreSQL/MinIO, create a temporary test database, run `0021 -> 0022 -> 0021 -> 0022`, then:

```bash
uv run --package nexus-engine pytest engine/tests -q --import-mode=importlib
uv run --package nexus-engine ruff check engine
uv run --package nexus-engine mypy engine/src
uv run --package nexus-engine alembic -c engine/alembic.ini heads
```

Expected: one head `0022_strategy_evolution`, all tests pass, Ruff/Mypy exit 0.

- [ ] **Step 4: Run Web full gates**

```bash
npm test
npm run typecheck
npm run lint
npm run check:generated
npm run build
npm run test:e2e
```

- [ ] **Step 5: Run security/boundary verification**

```bash
git grep -nE 'ak_[A-Za-z0-9]|BEGIN .*PRIVATE KEY|NEXUS_ENGINE_HMAC.*=' -- . ':!package-lock.json'
rg -n "nexus\.(providers\.broker|broker_worker)|Fosun|OrderCreate|SIMULATION_WRITE|LIVE_WRITE" engine/src/nexus/evolution
git diff --check f0336b6..HEAD
```

Expected: secret scan has no repository secret, evolution has no broker/write import or operation, diff check exits 0. Query the temporary database and record zero unexpected broker/live facts, zero mutable authority updates, one registry head and a valid event hash chain.

- [ ] **Step 6: Perform a separated single-owner code and security review**

Review the full `f0336b6..HEAD` range for look-ahead, adaptive trial reset, authority mismatch, candidate/risk escape, Agent tool escape, event immutability, idempotency, promotion races, pending-order behavior, rollback history mutation, Forward duplication, API/BFF confusion, secret/identifier leakage, accessibility and false live semantics. Record every Critical/Important/Minor finding with file/line and evidence. Fix every Critical/Important with a failing test and rerun focused/full gates; do not accept unresolved Critical/Important.

- [ ] **Step 7: Write acceptance review and update progress**

Record exact commit range, commands, pass counts, migration round-trip, database residual counts, review findings/fixes and the single-owner review limitation in `docs/superpowers/reviews/2026-08-27-nexus-phase-2-strategy-evolution-review.md`. Mark this plan and progress ledger complete only after all evidence exists.

- [ ] **Step 8: Commit acceptance evidence**

```bash
git add tests/e2e/strategy-evolution.spec.ts docs/superpowers/reviews/2026-08-27-nexus-phase-2-strategy-evolution-review.md docs/superpowers/plans/2026-08-27-nexus-phase-2-strategy-evolution-lab.md .superpowers/sdd/2026-08-22-nexus-phase-0-1/progress.md
git commit -m "docs: record phase 2 strategy evolution acceptance"
git status --short --branch
```

Expected: clean Phase 2 worktree.

---

## Spec Coverage Matrix

| Specification requirement | Implementation tasks |
|---|---|
| Agent proposes only hypothesis/search axes | 1, 4 |
| Deterministic candidate generation, budget and no arbitrary code | 1 |
| Generic DSL interpretation with baseline semantic equivalence and honest identity renewal | 2 |
| Immutable lineage/family/experiment/attempt/events | 3, 4 |
| Walk-forward, OOS firewall, trial accounting | 4, 5 |
| Champion/benchmark paired comparison and fixed gates | 5 |
| Separate 63+5 Strategy Shadow and 20 full fills | 3, 6 |
| Handover cost/risk preflight | 7 |
| Automatic internal-paper promotion and pause/veto | 7, 8, 9, 10 |
| Safe-boundary activation and pending orders | 7 |
| Append-only compatible rollback | 7, 9, 10 |
| Forward decision definition/event/generation binding | 3, 7 |
| Strict Jobs/audit/observability | 8 |
| Typed API/BFF and no DSL submission | 9, 10 |
| `/strategies` states, evidence, accessibility | 10, 11 |
| No broker/live execution or secret access | Global constraints, 4, 6–11 |
| Full migration/Engine/Web/E2E/review acceptance | 11 |

## Plan Self-Review Checklist

- [x] Every specification section 1–17 maps to a concrete task and test in the coverage matrix.
- [x] Every task names exact create/modify/test files and the interfaces it consumes/produces.
- [x] Every implementation task starts with an observable red test and ends with focused verification plus a local commit.
- [x] Candidate, gate, registry, Job, API and Forward type names are consistent across tasks.
- [x] Migration head is exactly `0022_strategy_evolution` over `0021_manual_job_retry_allowance`; old migrations and `phase01_promotion_locked` remain untouched.
- [x] No task activates broker/live capability, creates real orders, deploys, accesses secrets or edits `.openai/hosting.json`.
- [x] No unresolved placeholder or generic “add tests/error handling” instruction remains.
- [x] Inline execution is selected because the user authorized autonomous local completion and did not request subagent/parallel execution.
- [x] Baseline compatibility compares canonical business semantics; code/result identities renew when hash-covered source changes, and historical identities remain immutable.
