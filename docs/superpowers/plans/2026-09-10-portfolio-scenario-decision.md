# Portfolio Scenario Decision Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a deterministic, current-holdings-only concentration-reduction scenario that a portfolio-scoped Agent can evaluate without supplying weights, then expose one frozen candidate for human review while execution remains locked.

**Architecture:** Keep the legacy three-proposal path unchanged. Add a parallel `PortfolioMandateV1 → pure scenario engine → read-only Agent tool → deterministic recomputation and persistence → single-candidate read model` path. The model sees only redacted scenario metrics and a result hash; application code owns weights, constraints, persistence, readiness, and all Shadow gates.

**Tech Stack:** Python 3.12, Pydantic v2, SQLAlchemy 2, Alembic, PostgreSQL 17, FastAPI, React 19, vinext, TypeScript 5.9, Vitest, Pytest.

**Spec:** `docs/superpowers/specs/2026-09-10-portfolio-scenario-decision-design.md`

## Global Constraints

- Base implementation work on current `main`; do not implement on the existing Fosun push worktree.
- `execution_locked` remains literal `true` in domain, API, persistence, and UI contracts.
- V1 supports only `reduce_security_concentration`, `current_holdings_only`, reduce/maintain actions, and no new positions.
- Agent requests and outputs never contain target weights, quantities, amounts, or orders.
- System risk policy cannot be weakened by the Mandate.
- Concentration is security-only; cash is reported separately.
- One-way turnover includes the cash leg and uses `Decimal` with a one-basis-point step.
- Reads never trigger provider, model, or recomputation side effects.
- Legacy three-proposal records and APIs remain readable and unchanged.
- No new third-party dependency is permitted.

---

### Task 1: Freeze scenario contracts and financial semantics

**Files:**
- Create: `engine/src/nexus/optimizer/scenario_contracts.py`
- Create: `engine/tests/unit/optimizer/test_scenario_contracts.py`
- Modify: `app/components/proposal-comparison.tsx`
- Modify: `tests/unit/optimize/proposal-comparison.test.tsx`

**Interfaces:**
- Consumes: `TypedMetric`, `EvidenceRef`, `DecimalString`, existing UTC validators.
- Produces: `PortfolioMandateV1`, `PortfolioScenarioRequest`, `ScenarioPosition`, `ScenarioMetricSet`, `PortfolioScenarioEvaluation`, `FrozenPortfolioScenario`, `one_way_turnover(...)`.

- [ ] **Step 1: Write contract tests for forbidden widening and exact turnover**

```python
def test_mandate_cannot_allow_new_positions() -> None:
    with pytest.raises(ValidationError):
        mandate(allow_new_positions=True)

def test_one_way_turnover_includes_cash_leg() -> None:
    assert one_way_turnover(
        current={UUID(int=1): Decimal("0.60")},
        target={UUID(int=1): Decimal("0.55")},
        current_cash=Decimal("0.40"),
        target_cash=Decimal("0.45"),
    ) == Decimal("0.05")

def test_scenario_rejects_an_increase() -> None:
    with pytest.raises(ValidationError):
        scenario_position(current_weight="0.20", target_weight="0.21")
```

- [ ] **Step 2: Run the contract tests and confirm they fail because the module does not exist**

Run:

```bash
uv run pytest engine/tests/unit/optimizer/test_scenario_contracts.py -q --import-mode=importlib
```

Expected: collection fails for missing `nexus.optimizer.scenario_contracts`.

- [ ] **Step 3: Implement the closed Pydantic models and turnover function exactly as specified**

Use `ConfigDict(frozen=True, extra="forbid")`, reject binary floats recursively, require UTC datetimes, and validate SHA-256 strings with `^[0-9a-f]{64}$`.

- [ ] **Step 4: Correct legacy UI copy without changing legacy calculations**

Replace claims that Top1/Top3 use total assets with wording that they use normalized security holdings and show cash separately. Add a component assertion that the old incorrect phrase is absent and the security-only phrase is present.

- [ ] **Step 5: Run focused verification**

```bash
uv run pytest engine/tests/unit/optimizer/test_scenario_contracts.py -q --import-mode=importlib
npm test -- tests/unit/optimize/proposal-comparison.test.tsx
uv run ruff check engine/src/nexus/optimizer/scenario_contracts.py engine/tests/unit/optimizer/test_scenario_contracts.py
uv run mypy engine/src/nexus/optimizer/scenario_contracts.py
npm run typecheck
```

Expected: all commands pass.

- [ ] **Step 6: Commit the semantic contract**

```bash
git add engine/src/nexus/optimizer/scenario_contracts.py engine/tests/unit/optimizer/test_scenario_contracts.py app/components/proposal-comparison.tsx tests/unit/optimize/proposal-comparison.test.tsx
git commit -m "feat: define portfolio scenario contracts"
```

### Task 2: Implement the deterministic concentration scenario engine

**Files:**
- Create: `engine/src/nexus/optimizer/scenario_engine.py`
- Create: `engine/tests/unit/optimizer/test_scenario_engine.py`
- Modify: `engine/src/nexus/optimizer/minimum_change.py`
- Test: `engine/tests/unit/optimizer/test_minimum_change.py`

**Interfaces:**
- Consumes: `PortfolioSnapshot`, `PortfolioDiagnosticReport`, `PortfolioMandateV1`, `RealAdvisoryPolicy`, `ScenarioEvidence`, `TypedMetric`.
- Produces: `evaluate_adjustment_scenario(...) -> PortfolioScenarioEvaluation` and a private deterministic one-basis-point reducer.

- [ ] **Step 1: Write parameterized tests for the four objective outcomes**

```python
@pytest.mark.parametrize(
    ("weights", "max_turnover", "expected"),
    [
        (("0.50", "0.20", "0.10", "0.10"), "0.30", "met"),
        (("0.50", "0.20", "0.10", "0.10"), "0.02", "improved_not_met"),
        (("0.25", "0.25", "0.25", "0.25"), "0.10", "unchanged"),
        (("1.00",), "0.20", "infeasible"),
    ],
)
def test_objective_outcomes(weights, max_turnover, expected) -> None:
    result = evaluate_adjustment_scenario(
        snapshot=snapshot_with_weights(weights),
        report=bound_report(),
        mandate=mandate(max_turnover=max_turnover),
        policy=policy(),
        evidence=None,
        cost=None,
    )
    assert result.objective_status == expected
    assert result.execution_locked is True
```

- [ ] **Step 2: Add invariant tests**

Assert deterministic output under reversed input order, exact weight conservation, no target increase, current-universe equality, max-turnover enforcement, system minimum-cash precedence, one-basis-point termination, and strict objective improvement.

- [ ] **Step 3: Run tests and confirm failure for missing engine**

```bash
uv run pytest engine/tests/unit/optimizer/test_scenario_engine.py -q --import-mode=importlib
```

Expected: collection fails for missing `scenario_engine`.

- [ ] **Step 4: Implement the pure engine**

Do not call SQLAlchemy, network transports, clocks, UUID generators, or model APIs inside the pure calculation. The evaluation has no persistent `scenario_id`; the persistence service creates it only after hash validation. Hash canonical JSON containing all frozen input identities and exact decimal strings.

- [ ] **Step 5: Reuse risk evidence without weakening unknowns**

Call `scenario_metrics(...)` only when `ScenarioEvidence` matches the snapshot and valuation time. Preserve its unknown reasons otherwise. Use `cost=None` to return `COST_NOT_PROVEN`; keep tax `TAX_DATA_UNAVAILABLE`.

- [ ] **Step 6: Keep legacy minimum-change behavior isolated**

Extract only shared stable ordering helpers if useful. Do not change `reduce_to_concentration_limits(...)` return values or legacy proposal tests in this task.

- [ ] **Step 7: Run focused verification**

```bash
uv run pytest engine/tests/unit/optimizer/test_scenario_engine.py engine/tests/unit/optimizer/test_minimum_change.py engine/tests/unit/optimizer/test_scenario_risk.py -q --import-mode=importlib
uv run ruff check engine/src/nexus/optimizer/scenario_engine.py engine/src/nexus/optimizer/minimum_change.py engine/tests/unit/optimizer/test_scenario_engine.py
uv run mypy engine/src/nexus/optimizer/scenario_engine.py
```

Expected: all commands pass.

- [ ] **Step 8: Commit the pure engine**

```bash
git add engine/src/nexus/optimizer/scenario_engine.py engine/src/nexus/optimizer/minimum_change.py engine/tests/unit/optimizer/test_scenario_engine.py engine/tests/unit/optimizer/test_minimum_change.py
git commit -m "feat: generate deterministic concentration scenarios"
```

### Task 3: Add immutable Mandate and scenario persistence

**Files:**
- Create: `engine/alembic/versions/0048_portfolio_scenario_decisions.py`
- Modify: `engine/src/nexus/persistence/models/optimization.py`
- Modify: `engine/src/nexus/persistence/models/agents.py`
- Create: `engine/tests/integration/test_portfolio_scenario_migration.py`

**Interfaces:**
- Consumes: domain contracts from Task 1 and existing `Base`, `ImmutableCreatedAt`, `UTCDateTime`.
- Produces: `PortfolioMandateRecord`, `PortfolioScenarioRecord`, `PortfolioScenarioPositionRecord`, `PortfolioDecisionRunRecord`, `PortfolioScenarioSelectionRecord`.

- [ ] **Step 1: Write migration tests before the migration**

The test must upgrade from `0047` to `0048`, assert all constraints and unique indexes, insert one valid record graph, reject `allow_new_positions=true`, reject a target increase, reject `execution_locked=false`, reject a second selection for the same scenario, and downgrade to `0047` without touching legacy proposal rows.

- [ ] **Step 2: Run migration tests and confirm missing revision failure**

```bash
uv run pytest engine/tests/integration/test_portfolio_scenario_migration.py -q --import-mode=importlib
```

Expected: failure because Alembic revision `0048` is absent.

- [ ] **Step 3: Implement the additive migration and ORM records**

Use `NUMERIC(38,12)` for weights, PostgreSQL JSONB for metric/evidence payloads, UTC timestamps, explicit check-constraint names, and foreign keys to the frozen portfolio/diagnostic/Mandate identities. Create tables in dependency order and drop them in reverse order.

- [ ] **Step 4: Add immutable-row enforcement consistent with existing published records**

Reuse the immutable trigger style in `engine/alembic/versions/0009_optimization.py` and the terminal-state guard style in `engine/alembic/versions/0020_resilient_automation_audit.py`. Verify updates and deletes are rejected for Mandate, scenario, positions, selections, and completed decision runs. Add a PostgreSQL partial unique index for one `prepared/running` decision run per `(portfolio_snapshot_id, mandate_id)`.

- [ ] **Step 5: Run migration verification**

```bash
uv run alembic -c engine/alembic.ini upgrade head
uv run pytest engine/tests/integration/test_portfolio_scenario_migration.py -q --import-mode=importlib
uv run ruff check engine/alembic/versions/0048_portfolio_scenario_decisions.py engine/src/nexus/persistence/models
uv run mypy engine/src/nexus/persistence/models
```

Expected: upgrade, test, Ruff, and Mypy pass.

- [ ] **Step 6: Commit persistence**

```bash
git add engine/alembic/versions/0048_portfolio_scenario_decisions.py engine/src/nexus/persistence/models/optimization.py engine/src/nexus/persistence/models/agents.py engine/tests/integration/test_portfolio_scenario_migration.py
git commit -m "feat: persist portfolio mandates and scenarios"
```

### Task 4: Implement Mandate and scenario application services

**Files:**
- Create: `engine/src/nexus/optimizer/scenario_repository.py`
- Create: `engine/src/nexus/optimizer/scenario_service.py`
- Create: `engine/tests/integration/test_portfolio_scenario_service.py`

**Interfaces:**
- Consumes: Task 1 contracts, Task 2 engine, Task 3 records, `load_frozen_snapshot`, diagnostic repository, `load_real_advisory_policy_v1`, `load_scenario_evidence`.
- Produces: `create_mandate(...)`, `load_current_mandate(...)`, `evaluate_scenario(...)`, `persist_verified_scenario(...)`, `load_scenario(...)`.

- [ ] **Step 1: Write service tests for versioning and point-in-time binding**

```python
async def test_create_mandate_appends_version_without_mutating_v1(session) -> None:
    first = await create_mandate(session, request(...))
    second = await create_mandate(session, request(max_turnover="0.08"))
    assert (first.version, second.version) == (1, 2)
    assert first.max_turnover != second.max_turnover

async def test_evaluation_rejects_a_report_from_another_snapshot(session) -> None:
    with pytest.raises(ScenarioBindingError, match="SCENARIO_DIAGNOSTIC_MISMATCH"):
        await evaluate_scenario(session, mismatched_request())
```

- [ ] **Step 2: Add tests for deterministic recomputation and atomic persistence**

Prove that a mismatched `result_hash` creates zero scenario rows; a matching hash creates exactly one scenario plus all positions; replay returns the same row; and an exception between parent and child inserts rolls back the transaction.

- [ ] **Step 3: Run tests and confirm missing service failure**

```bash
uv run pytest engine/tests/integration/test_portfolio_scenario_service.py -q --import-mode=importlib
```

- [ ] **Step 4: Implement repository functions with explicit locks**

Use a row/advisory lock for the next Mandate version and unique constraints as the final race guard. Scenario evaluation performs reads only. Persistence accepts the expected hash, recomputes the pure result, compares hashes using `hmac.compare_digest`, and inserts atomically.

- [ ] **Step 5: Implement freshness checks**

Mark a scenario stale when a newer READY portfolio snapshot exists, the Mandate is no longer current, or the frozen evidence authority is no longer valid for the recorded valuation time. Reads return stale reasons without recalculation.

- [ ] **Step 6: Run focused verification**

```bash
uv run pytest engine/tests/integration/test_portfolio_scenario_service.py engine/tests/unit/optimizer/test_scenario_engine.py -q --import-mode=importlib
uv run ruff check engine/src/nexus/optimizer/scenario_repository.py engine/src/nexus/optimizer/scenario_service.py engine/tests/integration/test_portfolio_scenario_service.py
uv run mypy engine/src/nexus/optimizer/scenario_repository.py engine/src/nexus/optimizer/scenario_service.py
```

- [ ] **Step 7: Commit services**

```bash
git add engine/src/nexus/optimizer/scenario_repository.py engine/src/nexus/optimizer/scenario_service.py engine/tests/integration/test_portfolio_scenario_service.py
git commit -m "feat: add portfolio scenario services"
```

### Task 5: Expose bounded internal APIs

**Files:**
- Modify: `engine/src/nexus/api/schemas/optimization.py`
- Modify: `engine/src/nexus/api/routes/optimization.py`
- Create: `engine/tests/integration/test_portfolio_scenario_api.py`
- Create: `app/api/v1/portfolio-mandates/route.ts`
- Create: `app/api/v1/portfolio-mandates/current/route.ts`
- Create: `app/api/v1/portfolio-scenarios/evaluate/route.ts`
- Create: `app/api/v1/portfolio-scenarios/[scenarioId]/route.ts`
- Create: `app/api/v1/portfolio-scenarios/[scenarioId]/select/route.ts`
- Modify: `app/lib/engine/client.ts`
- Modify: `app/lib/engine/errors.ts`
- Create: `tests/unit/engine/portfolio-scenario-routes.test.ts`
- Modify: `contracts/openapi.json`
- Modify: `app/lib/generated/schema.d.ts`
- Modify: `app/lib/generated/runtime-contract.ts`

**Interfaces:**
- Consumes: Task 4 application services and existing auth/idempotency helpers.
- Produces: Engine plus BFF Mandate create/read, scenario evaluate/read, and selection endpoints from the spec.

- [ ] **Step 1: Write API contract tests**

Cover 64 KiB request bounds, JSON media type validation, exact decimal strings, forbidden extra fields, idempotency replay/conflict, stable error codes, no-weight request schema, response `execution_locked=true`, and GET side-effect absence. BFF tests must also reject invalid UUID paths, non-canonical query strings, oversized bodies, mismatched upstream identities, and responses that fail the generated runtime schema.

- [ ] **Step 2: Run the API tests and confirm 404 or schema absence**

```bash
uv run pytest engine/tests/integration/test_portfolio_scenario_api.py -q --import-mode=importlib
npm test -- tests/unit/engine/portfolio-scenario-routes.test.ts
```

- [ ] **Step 3: Implement API schemas and routes**

Reuse `_bounded_what_if_request`’s streaming size guard as a shared bounded JSON parser rather than duplicating it. Keep the legacy `/internal/v1/what-if/evaluate` endpoint unchanged. Implement each Web route through `withNexusBff` and `engineFetch`; add exact path templates and response validation before any page consumes the endpoint.

- [ ] **Step 4: Generate and verify clients**

```bash
npm run generate:client
npm run check:generated
```

Expected: generated OpenAPI, TypeScript schema, and runtime contract agree.

- [ ] **Step 5: Run focused verification**

```bash
uv run pytest engine/tests/integration/test_portfolio_scenario_api.py -q --import-mode=importlib
uv run ruff check engine/src/nexus/api/schemas/optimization.py engine/src/nexus/api/routes/optimization.py
uv run mypy engine/src/nexus/api
npm test -- tests/unit/engine/portfolio-scenario-routes.test.ts tests/unit/engine/client-security.test.ts tests/unit/engine/bff-boundary.test.ts
npm run typecheck
```

- [ ] **Step 6: Commit API contracts**

```bash
git add engine/src/nexus/api/schemas/optimization.py engine/src/nexus/api/routes/optimization.py engine/tests/integration/test_portfolio_scenario_api.py app/api/v1/portfolio-mandates app/api/v1/portfolio-scenarios app/lib/engine/client.ts app/lib/engine/errors.ts tests/unit/engine/portfolio-scenario-routes.test.ts contracts/openapi.json app/lib/generated/schema.d.ts app/lib/generated/runtime-contract.ts
git commit -m "feat: expose portfolio scenario APIs"
```

### Task 6: Add the Agent read-only scenario tool

**Files:**
- Modify: `engine/src/nexus/agents/tools.py`
- Modify: `engine/src/nexus/agents/database_tools.py`
- Create: `engine/tests/unit/agents/test_adjustment_scenario_tool.py`
- Create: `engine/tests/integration/test_adjustment_scenario_tool.py`

**Interfaces:**
- Consumes: `EvaluateAdjustmentScenarioInput`, `scenario_service.evaluate_scenario`.
- Produces: registered tool `evaluate_adjustment_scenario` returning a redacted `agent_read_only_tool_result_v1` payload.

- [ ] **Step 1: Write schema and redaction tests**

```python
def test_tool_input_has_no_weight_fields() -> None:
    schema = tool_schema("evaluate_adjustment_scenario")
    serialized = json.dumps(schema).lower()
    assert "weight" not in serialized
    assert "quantity" not in serialized
    assert "order" not in serialized

async def test_tool_result_redacts_positions(registry) -> None:
    result = await registry.execute("evaluate_adjustment_scenario", valid_input())
    serialized = json.dumps(result).lower()
    assert "target_weight" not in serialized
    assert "positions" not in serialized
    assert result["execution_locked"] is True
```

- [ ] **Step 2: Add a no-write integration test**

Count Mandate, scenario, position, decision-run, proposal, Shadow, order, and fill rows before and after the tool call; all counts must remain identical.

- [ ] **Step 3: Run tests and confirm the tool is not registered**

```bash
uv run pytest engine/tests/unit/agents/test_adjustment_scenario_tool.py engine/tests/integration/test_adjustment_scenario_tool.py -q --import-mode=importlib
```

- [ ] **Step 4: Register and implement the tool projection**

Return only `analysis_status`, `objective_status`, baseline/candidate aggregate metrics, cash, one-way turnover, unknown metrics, hard reasons, evidence references, and `result_hash`. Decision readiness is derived only after the validated Agent intent exists. Do not return exact account values, quantities, per-position weights, provider clients, or SQL errors.

- [ ] **Step 5: Run focused verification**

```bash
uv run pytest engine/tests/unit/agents/test_adjustment_scenario_tool.py engine/tests/integration/test_adjustment_scenario_tool.py -q --import-mode=importlib
uv run ruff check engine/src/nexus/agents/tools.py engine/src/nexus/agents/database_tools.py engine/tests/unit/agents/test_adjustment_scenario_tool.py
uv run mypy engine/src/nexus/agents
```

- [ ] **Step 6: Commit the tool**

```bash
git add engine/src/nexus/agents/tools.py engine/src/nexus/agents/database_tools.py engine/tests/unit/agents/test_adjustment_scenario_tool.py engine/tests/integration/test_adjustment_scenario_tool.py
git commit -m "feat: let agents evaluate adjustment scenarios"
```

### Task 7: Add portfolio-scoped Agent decision runs

**Files:**
- Create: `engine/src/nexus/agents/portfolio_decision.py`
- Create: `engine/src/nexus/agents/prompts/portfolio-decision-v1.md`
- Modify: `engine/src/nexus/strategy/intents.py`
- Create: `engine/src/nexus/jobs/portfolio_decision.py`
- Modify: `engine/src/nexus/jobs/models.py`
- Modify: `engine/src/nexus/jobs/registry.py`
- Modify: `engine/src/nexus/worker.py`
- Create: `engine/src/nexus/api/routes/portfolio_decisions.py`
- Modify: `engine/src/nexus/main.py`
- Modify: `engine/src/nexus/api/schemas/optimization.py`
- Create: `app/api/v1/portfolio-decisions/run/route.ts`
- Create: `app/api/v1/portfolio-decisions/[runId]/route.ts`
- Modify: `app/lib/engine/client.ts`
- Modify: `app/lib/engine/errors.ts`
- Create: `tests/unit/engine/portfolio-decision-routes.test.ts`
- Create: `engine/tests/unit/agents/test_portfolio_scenario_intent.py`
- Create: `engine/tests/integration/test_portfolio_decision_run.py`

**Interfaces:**
- Consumes: Task 6 tool, existing Responses/Codex staged runners, Task 3 decision-run record, Task 4 deterministic persistence.
- Produces: `PortfolioScenarioIntentV1`, `run_portfolio_decision(...)`, `JobType.PORTFOLIO_DECISION`, a closed Job handler, and durable POST/GET Engine plus BFF endpoints.

- [ ] **Step 1: Write closed intent tests**

Reject any extra field, weight-like key, unsafe text, missing evidence, result-hash mismatch, expired validity, or `execution_locked=false`. Accept only the three assessment values defined in the spec.

- [ ] **Step 2: Write lifecycle and recovery tests**

Cover `prepared → running → completed`, `failed`, and `abstained`; one active run per snapshot/Mandate; `RetryPolicy.NEVER`; `max_attempts=1`; reconnect reads existing run/output; manual termination is retained as failure/termination cause rather than completion.

- [ ] **Step 3: Write the hash-binding acceptance test**

Stub the model to call the tool and return the observed hash. Assert the application recomputes the result, persists one exact scenario, and saves the validated intent. Return a different hash and assert zero scenario rows.

- [ ] **Step 4: Run tests and confirm missing portfolio decision module**

```bash
uv run pytest engine/tests/unit/agents/test_portfolio_scenario_intent.py engine/tests/integration/test_portfolio_decision_run.py -q --import-mode=importlib
```

- [ ] **Step 5: Implement the intent and prompt**

The prompt must instruct the model to identify the most important supported portfolio issue, call exactly the declared read-only tool, discuss known and unknown consequences, and choose `advance_candidate_for_review`, `observe`, or `do_not_advance`. It must explicitly prohibit weights, quantities, orders, execution claims, and treating `partial` as recommendation-ready.

- [ ] **Step 6: Implement the coordinator**

Reuse existing bounded staged-response handling, usage accounting, evidence validation, and durable status patterns. Do not make `AgentResearchRunRecord.instrument_id` nullable. Persist only after deterministic recomputation and validation succeed.

- [ ] **Step 7: Register the durable Job and POST/GET recovery APIs**

Add `JobType.PORTFOLIO_DECISION = "portfolio.decision"` at Agent priority on the general queue. Permit its handler the same bounded 120-second ceiling as `AGENT_RESEARCH`, but require `RetryPolicy.NEVER` and `max_attempts=1`. POST creates/reuses the durable run and Job and returns 202 with a Location header; GET returns persisted status and the scenario identity only after completion. Add the Engine router to `create_engine_app`, add exact BFF path templates and response validation, and prove an interrupted client can recover without a second model invocation.

- [ ] **Step 8: Regenerate contracts and run focused verification**

```bash
npm run generate:client
npm run check:generated
uv run pytest engine/tests/unit/agents/test_portfolio_scenario_intent.py engine/tests/integration/test_portfolio_decision_run.py engine/tests/unit/agents/test_runtime.py -q --import-mode=importlib
uv run ruff check engine/src/nexus/agents/portfolio_decision.py engine/src/nexus/strategy/intents.py engine/src/nexus/jobs/portfolio_decision.py engine/src/nexus/jobs/models.py engine/src/nexus/jobs/registry.py engine/src/nexus/api/routes/portfolio_decisions.py
uv run mypy engine/src/nexus/agents engine/src/nexus/strategy engine/src/nexus/jobs engine/src/nexus/api
npm test -- tests/unit/engine/portfolio-decision-routes.test.ts tests/unit/engine/client-security.test.ts
npm run typecheck
```

- [ ] **Step 9: Commit portfolio decision runs**

```bash
git add engine/src/nexus/agents/portfolio_decision.py engine/src/nexus/agents/prompts/portfolio-decision-v1.md engine/src/nexus/strategy/intents.py engine/src/nexus/jobs/portfolio_decision.py engine/src/nexus/jobs/models.py engine/src/nexus/jobs/registry.py engine/src/nexus/worker.py engine/src/nexus/api/routes/portfolio_decisions.py engine/src/nexus/main.py engine/src/nexus/api/schemas/optimization.py engine/tests/unit/agents/test_portfolio_scenario_intent.py engine/tests/integration/test_portfolio_decision_run.py app/api/v1/portfolio-decisions app/lib/engine/client.ts app/lib/engine/errors.ts tests/unit/engine/portfolio-decision-routes.test.ts contracts/openapi.json app/lib/generated/schema.d.ts app/lib/generated/runtime-contract.ts
git commit -m "feat: add portfolio decision agent runs"
```

### Task 8: Implement readiness and human-selection semantics

**Files:**
- Create: `engine/src/nexus/optimizer/scenario_readiness.py`
- Modify: `engine/src/nexus/optimizer/scenario_service.py`
- Modify: `engine/src/nexus/api/schemas/optimization.py`
- Modify: `engine/src/nexus/api/routes/optimization.py`
- Create: `engine/tests/unit/optimizer/test_scenario_readiness.py`
- Modify: `engine/tests/integration/test_portfolio_scenario_api.py`

**Interfaces:**
- Consumes: frozen scenario, current DB authority, Agent intent.
- Produces: `derive_scenario_readiness(...)`, stale reasons, and versioned selection records/status.

- [ ] **Step 1: Write the readiness truth table as parameterized tests**

Cases must prove that partial analysis is displayable; missing cost, tax, FX, relevant equity-sector evidence, history, liquidity, stale input, unmet objective, hard violation, or non-advancing Agent assessment prevents `decision_ready`; no case changes `execution_locked`.

- [ ] **Step 2: Write selection concurrency tests**

The correct version can transition `unselected → selected_for_validation` or `unselected → rejected`; stale versions return `SCENARIO_VERSION_CONFLICT`; selection does not create Shadow, order, or fill rows.

- [ ] **Step 3: Run tests and confirm missing readiness module**

```bash
uv run pytest engine/tests/unit/optimizer/test_scenario_readiness.py engine/tests/integration/test_portfolio_scenario_api.py -q --import-mode=importlib
```

- [ ] **Step 4: Implement readiness as a pure derivation**

Return a status and stable reason-code tuple. Never accept readiness from request parameters or persisted frontend state. Keep Shadow eligibility as a separate existing computation.

- [ ] **Step 5: Implement local human selection**

Store selection as an append-only event or versioned state following the repository’s lifecycle convention. The API response must state `shadow_started=false` and `execution_locked=true`.

- [ ] **Step 6: Regenerate clients and verify**

```bash
npm run generate:client
npm run check:generated
uv run pytest engine/tests/unit/optimizer/test_scenario_readiness.py engine/tests/integration/test_portfolio_scenario_api.py -q --import-mode=importlib
uv run ruff check engine/src/nexus/optimizer/scenario_readiness.py engine/src/nexus/optimizer/scenario_service.py engine/src/nexus/api
uv run mypy engine/src/nexus/optimizer engine/src/nexus/api
```

- [ ] **Step 7: Commit readiness and selection**

```bash
git add engine/src/nexus/optimizer/scenario_readiness.py engine/src/nexus/optimizer/scenario_service.py engine/src/nexus/api/schemas/optimization.py engine/src/nexus/api/routes/optimization.py engine/tests/unit/optimizer/test_scenario_readiness.py engine/tests/integration/test_portfolio_scenario_api.py contracts/openapi.json app/lib/generated/schema.d.ts app/lib/generated/runtime-contract.ts
git commit -m "feat: derive scenario readiness and selection"
```

### Task 9: Build the single-candidate portfolio UI

**Files:**
- Create: `app/components/portfolio-scenario.tsx`
- Create: `tests/unit/portfolio/portfolio-scenario.test.tsx`
- Modify: `app/(protected)/portfolio/page.tsx`
- Modify: `app/lib/display-labels.ts`
- Modify: `app/styles/ui.css`
- Create: `tests/e2e/portfolio-scenario.spec.ts`

**Interfaces:**
- Consumes: generated scenario response and selection endpoint.
- Produces: current-vs-candidate view with Mandate, readiness, unknowns, evidence, and human-selection controls.

- [ ] **Step 1: Write component tests for semantic copy and actions**

Assert that `candidate_only` renders “候选情景” and never “推荐方案”; security-only denominator is visible; unknowns show their reason; stale data disables actions; selection requires confirmation; and no “执行” action exists.

- [ ] **Step 2: Write page-state tests**

Cover loading, no Mandate, no scenario, partial, stale, forbidden, error, selected, and rejected. Refresh must only GET persisted state.

- [ ] **Step 3: Run tests and confirm missing component**

```bash
npm test -- tests/unit/portfolio/portfolio-scenario.test.tsx
```

- [ ] **Step 4: Implement the component and portfolio-page integration**

Use existing design tokens and shared formatting. Display at most three decimals without converting tiny nonzero values to zero. Keep legacy proposal pages reachable as historical/read-only views, but do not render their fixed three columns in the new scenario section.

- [ ] **Step 5: Add browser acceptance coverage**

The E2E route must use a sanitized fixture and assert that page load/refresh makes no POST request to model, provider, Shadow, order, or fill endpoints.

- [ ] **Step 6: Run frontend verification**

```bash
npm run lint
npm run typecheck
npm test -- tests/unit/portfolio/portfolio-scenario.test.tsx
npm run test:e2e -- tests/e2e/portfolio-scenario.spec.ts --workers=1
npm run build
```

Expected: all commands pass and the built page contains no execution affordance.

- [ ] **Step 7: Commit the UI**

```bash
git add app/components/portfolio-scenario.tsx tests/unit/portfolio/portfolio-scenario.test.tsx app/'(protected)'/portfolio/page.tsx app/lib/display-labels.ts app/styles/ui.css tests/e2e/portfolio-scenario.spec.ts
git commit -m "feat: show one portfolio adjustment candidate"
```

### Task 10: Verify the complete local read-only loop

**Files:**
- Create: `engine/tests/integration/test_portfolio_scenario_e2e.py`
- Modify: `docs/plans/2026-09-02-core-evidence-checkpoint.md`

**Interfaces:**
- Consumes: Tasks 1-9.
- Produces: one repeatable fixture acceptance and one real-local read-only acceptance record when existing data is sufficient.

- [ ] **Step 1: Write a cross-layer fixture test**

The test must create a READY frozen portfolio, matching diagnostics, Mandate, qualified scenario evidence and a stubbed Agent response; call the normal API entry; assert exact persisted hashes and weights; GET the scenario; select it; and prove zero Shadow/order/fill records.

- [ ] **Step 2: Run the cross-layer test**

```bash
uv run pytest engine/tests/integration/test_portfolio_scenario_e2e.py -q --import-mode=importlib
```

Expected: pass against PostgreSQL.

- [ ] **Step 3: Run the focused regression suite**

```bash
uv run pytest \
  engine/tests/unit/optimizer/test_scenario_contracts.py \
  engine/tests/unit/optimizer/test_scenario_engine.py \
  engine/tests/unit/optimizer/test_scenario_readiness.py \
  engine/tests/unit/agents/test_adjustment_scenario_tool.py \
  engine/tests/unit/agents/test_portfolio_scenario_intent.py \
  engine/tests/integration/test_portfolio_scenario_migration.py \
  engine/tests/integration/test_portfolio_scenario_service.py \
  engine/tests/integration/test_portfolio_scenario_api.py \
  engine/tests/integration/test_portfolio_decision_run.py \
  engine/tests/integration/test_portfolio_scenario_e2e.py \
  -q --import-mode=importlib
uv run ruff check engine
uv run mypy engine/src
npm run lint
npm run typecheck
npm test
npm run check:generated
npm run build
```

Expected: every command passes.

- [ ] **Step 4: Run one bounded real-local read-only acceptance**

Use the normal product configuration and existing persisted data. Do not refresh the broker, call new external providers, or replay a model run when a matching durable decision run already exists. Record:

- snapshot/diagnostic/Mandate/scenario identities in protected local evidence;
- objective and analysis status;
- unknown reasons;
- `execution_locked=true`;
- zero Shadow/order/fill deltas;
- page result for the same scenario ID.

If cost, tax, sector, history, or liquidity remains missing, the expected accepted result is `partial + candidate_only`, not a recommendation.

- [ ] **Step 5: Update the existing evidence checkpoint**

Append only the verified user-visible result, code baseline, exact verification commands, remaining external/data blockers, and next allowed slice. Do not add private holdings, amounts, credentials, or provider raw responses.

- [ ] **Step 6: Commit acceptance evidence**

```bash
git add engine/tests/integration/test_portfolio_scenario_e2e.py docs/plans/2026-09-02-core-evidence-checkpoint.md
git commit -m "test: verify portfolio scenario decision loop"
```

## Implementation stop point

Stop after Task 10. Do not add a generic optimizer, new-position support, automatic Shadow, broker simulation, or execution. The next architecture decision is the explicit adapter from one selected scenario to the existing Shadow contract, and it requires a separate reviewed design because the current Shadow model assumes exactly three variants.
