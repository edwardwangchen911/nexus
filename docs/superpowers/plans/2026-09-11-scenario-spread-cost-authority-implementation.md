# Scenario Spread Cost Authority Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Use already-persisted, valuation-time-matched Alpaca IEX bid/ask evidence to calculate a reproducible sell-side half-spread estimate for one read-only portfolio adjustment scenario, while all other cost gaps and `execution_locked=true` remain unchanged.

**Architecture:** Publish one immutable purpose-specific source policy, then reuse `latest_quotes`, `data_artifacts`, and `idempotency_records`; do not create a scenario, quote-bundle, or candidate table. The scenario API first creates its existing deterministic no-cost candidate, selects policy and quote rows under one PostgreSQL statement-level MVCC snapshot, verifies canonical row bytes and the stored raw artifact, freezes a v3 manifest, and applies a pure Decimal calculation. Agent runs freeze an internal V2 input pointing to the exact scenario claim so admission, worker recovery, and tool execution reproduce the same result without selecting newer evidence.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, SQLAlchemy async, PostgreSQL, Alembic, `exchange-calendars==4.13.2`, pytest, React 19, TypeScript, Vitest, Playwright.

**Spec:** `docs/superpowers/specs/2026-09-11-scenario-spread-cost-authority-design.md`

## Global Constraints

- The only accepted market source is `source=alpaca`, `feed=iex`, `delivery_mode=real_time`, `transport=websocket`, `event_kind=quote`, `artifact_schema=alpaca_stock_stream_v1`.
- The policy key/version/ID are exactly `portfolio_scenario_cost_analysis`, `scenario_spread_cost_policy_v1`, and `7733d99e-1594-5f1e-87ea-e5cc23575159`.
- The quote freshness limit is exactly 45 seconds and applies to `valuation_time - quote.observed_at`; exactly 45 seconds is allowed.
- The calendar authority is exactly `exchange_calendars==4.13.2`, calendar `XNYS`, contract `xnys_regular_open_closed_v1`; pre-market, post-market, and overnight remain unsupported.
- `created_at <= valuation_time` proves only that the insert transaction started by the cutoff; never claim that the fact was committed at valuation time.
- First selection uses one SQL statement and its statement-level MVCC snapshot; replay uses only exact IDs and hashes from the manifest.
- Do not contact Alpaca, Fosun, or any other Provider from API, page, Agent, or tests.
- Commission, slippage, broker regulatory pass-through, tax, and total estimated cost stay unknown; `analysis_status=partial`, `execution_eligible=false`, and `execution_locked=true` stay unchanged.
- Existing v2 scenario claims, result hashes, and `PortfolioDecisionInputV1` runs are never migrated, backfilled, or recomputed with spread evidence.
- Do not add a business table, public request field, public response schema version, optimization abstraction, Shadow action, order, or execution path.

---

## File Map

- Create `engine/alembic/versions/0050_scenario_spread_cost_policy.py`: publish and guardedly remove the immutable cost-analysis policy.
- Modify `engine/src/nexus/persistence/migrations.py`: advance the single expected head to `0050_scenario_spread_cost_policy`.
- Modify `engine/pyproject.toml` and `uv.lock`: pin `exchange-calendars==4.13.2` without adding another dependency.
- Create `engine/src/nexus/optimizer/scenario_spread_cost.py`: own manifest contracts, canonical hashes, SQL selection, raw-artifact verification, exact replay, and pure spread application.
- Modify `engine/src/nexus/ops/alpaca_market_stream.py`: expose a bounded parser for an already-frozen `alpaca_stock_stream_v1` raw payload; no network behavior changes.
- Modify `engine/src/nexus/api/routes/optimization.py`, `engine/src/nexus/api/errors.py`, and `engine/src/nexus/main.py`: wire v3 idempotency, object-store injection, evidence application, and stable replay failure.
- Modify `engine/src/nexus/agents/portfolio_decision_contracts.py`: add internal `PortfolioDecisionInputV2` and one discriminated parser while retaining V1.
- Modify `engine/src/nexus/agents/portfolio_decision_persistence.py`: resolve and freeze the first fully valid v3 scenario claim for Agent admission.
- Modify `engine/src/nexus/agents/portfolio_decision_tools.py`, `engine/src/nexus/agents/portfolio_decision_execution.py`, `engine/src/nexus/agents/portfolio_decision_worker.py`, `engine/src/nexus/worker.py`, and `engine/src/nexus/api/routes/portfolio_decisions.py`: exact V2 replay through API read, worker recovery, tool call, and terminal validation.
- Modify `app/components/portfolio-scenario.tsx`: render regulatory reference, spread bundle, and unknown evidence versions with different copy.
- Add or modify the focused tests named in each task; do not reorganize unrelated fixtures.
- Modify `docs/plans/2026-09-02-core-evidence-checkpoint.md` only after implementation verification, appending one timestamped result section.

## Task 1: Publish the immutable policy and pin the calendar runtime

**Files:**
- Create: `engine/alembic/versions/0050_scenario_spread_cost_policy.py`
- Modify: `engine/src/nexus/persistence/migrations.py`
- Modify: `engine/pyproject.toml`
- Modify: `uv.lock`
- Create: `engine/tests/integration/test_scenario_spread_cost_migration.py`

**Interfaces:**
- Produces: constants in the migration matching the fixed policy ID, key, version, schema `1`, and payload in the spec.
- Produces: `CURRENT_MIGRATION_HEAD = "0050_scenario_spread_cost_policy"`.
- Preserves: all existing tables and v1/v2 idempotency records.

- [ ] **Step 1: Write the failing migration tests**

Add tests that assert the head relationship, exact JSONB policy row, no schema additions, a clean downgrade, and downgrade refusal after a v3 manifest references the policy:

```python
def test_0050_is_single_head_and_publishes_exact_policy(temporary_database_url, monkeypatch):
    config = Config("engine/alembic.ini")
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(config, "0050_scenario_spread_cost_policy")
    scripts = ScriptDirectory.from_config(config)
    revision = scripts.get_revision("0050_scenario_spread_cost_policy")
    assert revision is not None
    assert revision.down_revision == "0049_pd_terminal_phases"
    assert scripts.get_heads() == [CURRENT_MIGRATION_HEAD]
    assert asyncio.run(_policy_row(temporary_database_url)) == EXPECTED_POLICY


def test_0050_downgrade_refuses_referenced_v3_manifest(temporary_database_url, monkeypatch):
    config = Config("engine/alembic.ini")
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(config, "0050_scenario_spread_cost_policy")
    asyncio.run(_insert_v3_claim(temporary_database_url))
    with pytest.raises(RuntimeError, match="scenario spread cost policy is referenced"):
        command.downgrade(config, "0049_pd_terminal_phases")
```

- [ ] **Step 2: Run the tests and confirm the missing revision fails**

Run:

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_scenario_spread_cost_migration.py -q --import-mode=importlib
```

Expected: FAIL because revision `0050_scenario_spread_cost_policy` and the new migration head do not exist.

- [ ] **Step 3: Implement the exact data migration and dependency pin**

Use an exact policy payload and guarded downgrade. The downgrade must acquire both locks before its reference check:

```python
revision = "0050_scenario_spread_cost_policy"
down_revision = "0049_pd_terminal_phases"

POLICY_ID = "7733d99e-1594-5f1e-87ea-e5cc23575159"
POLICY_PAYLOAD = {
    "purpose": "portfolio_scenario_cost_analysis",
    "selection": "latest_qualified_before_valuation_cutoff",
    "fallback": "disabled",
    "session_scope": "xnys_regular",
    "max_quote_age_seconds": 45,
    "calculation": "sell_half_spread_from_snapshot_mark_v1",
    "calendar_authority": {
        "library": "exchange_calendars",
        "library_version": "4.13.2",
        "calendar": "XNYS",
        "session_window_contract": "xnys_regular_open_closed_v1",
    },
    "require_verified_evidence": True,
    "require_empty_quality_flags": True,
    "alpaca": {
        "feed": "iex",
        "delivery_mode": "real_time",
        "transport": "websocket",
        "artifact_schema": "alpaca_stock_stream_v1",
        "market_scope": "US",
        "event_kind": "quote",
    },
}

def downgrade() -> None:
    op.execute(
        "LOCK TABLE public.idempotency_records, public.source_policies "
        "IN ACCESS EXCLUSIVE MODE"
    )
    referenced = op.get_bind().scalar(text(
        "SELECT EXISTS (SELECT 1 FROM idempotency_records "
        "WHERE route='/internal/v1/portfolio-scenarios/evaluate:portfolio_scenario_evaluation_v3' "
        "AND schema_version='portfolio_scenario_v3' "
        "AND response_payload->>'source_policy_id'=:policy_id)"
    ), {"policy_id": POLICY_ID})
    if referenced:
        raise RuntimeError("scenario spread cost policy is referenced")
    op.execute("ALTER TABLE source_policies DISABLE TRIGGER trg_source_policies_immutable")
    deleted = op.get_bind().execute(text(
        "DELETE FROM source_policies WHERE id=CAST(:id AS uuid) "
        "AND key='portfolio_scenario_cost_analysis' "
        "AND version='scenario_spread_cost_policy_v1' "
        "AND schema_version='1' AND payload=CAST(:payload AS jsonb)"
    ), {
        "id": POLICY_ID,
        "payload": json.dumps(POLICY_PAYLOAD, sort_keys=True, separators=(",", ":")),
    })
    if deleted.rowcount != 1:
        raise RuntimeError("scenario spread cost policy downgrade mismatch")
    op.execute("ALTER TABLE source_policies ENABLE TRIGGER trg_source_policies_immutable")
```

Change the dependency entry to `"exchange-calendars==4.13.2"`, run `uv lock`, and change `CURRENT_MIGRATION_HEAD` to the new revision. Do not hand-edit resolved package hashes in `uv.lock`.

- [ ] **Step 4: Run focused migration and dependency verification**

Run:

```bash
uv lock --check
uv run --package nexus-engine python -c 'import importlib.metadata as m; assert m.version("exchange-calendars") == "4.13.2"'
uv run --package nexus-engine pytest engine/tests/integration/test_scenario_spread_cost_migration.py engine/tests/integration/test_portfolio_decision_migration.py -q --import-mode=importlib
```

Expected: all commands exit 0; 0050 is the only head; referenced downgrade is rejected without deleting the policy.

- [ ] **Step 5: Commit Task 1**

```bash
git add engine/alembic/versions/0050_scenario_spread_cost_policy.py engine/src/nexus/persistence/migrations.py engine/pyproject.toml uv.lock engine/tests/integration/test_scenario_spread_cost_migration.py
git commit -m "feat: publish scenario spread cost policy"
```

## Task 2: Implement manifest contracts and pure Decimal spread calculation

**Files:**
- Create: `engine/src/nexus/optimizer/scenario_spread_cost.py`
- Create: `engine/tests/unit/optimizer/test_scenario_spread_cost.py`
- Modify: `engine/tests/unit/optimizer/test_scenario_engine.py`

**Interfaces:**
- Produces: flat `ScenarioSpreadEvidenceInputV1` (all manifest fields except `expected_result_hash`), final `ScenarioSpreadEvidenceManifestV1` (the same fields plus `expected_result_hash`), `VerifiedSpreadQuoteV1`, and runtime `ScenarioSpreadEvidenceV1(input, quotes, bundle_hash, available_at)`.
- Produces: `canonical_scenario_spread_manifest_hash(manifest) -> str`.
- Produces: `finalize_scenario_spread_manifest(input, *, expected_result_hash) -> ScenarioSpreadEvidenceManifestV1`.
- Produces: `scenario_cost_basis_hash(snapshot, base_result) -> str`.
- Produces: `apply_scenario_spread_cost(snapshot, base_result, evidence) -> PortfolioScenarioEvaluationV2`.
- Consumes: the unchanged no-cost `PortfolioScenarioEvaluationV2` returned by `evaluate_adjustment_scenario()`.

- [ ] **Step 1: Write failing contract and hand-calculation tests**

Cover canonical ordering, binary-float rejection, full cost-basis sensitivity, the snapshot-mark formula, partial quote coverage, and no-reduction semantics:

```python
def test_apply_spread_uses_snapshot_mark_to_convert_sold_weight_to_quantity():
    base, snapshot = _base_result_and_snapshot(current_price="40", sold_weight="0.10")
    evidence = _available_evidence(bid="39.90", ask="40.10")
    result = apply_scenario_spread_cost(snapshot=snapshot, base_result=base, evidence=evidence)
    assert result.cost_breakdown.spread.status == "known"
    assert result.cost_breakdown.spread.value == Decimal("0.00025")
    assert result.candidate.estimated_cost.status == "unknown"
    assert result.cost_breakdown.total_estimated_cost_ex_tax.status == "unknown"
    assert result.execution_locked is True


def test_cost_basis_hash_changes_when_snapshot_current_price_changes():
    first = scenario_cost_basis_hash(*_basis(current_price="40"))
    second = scenario_cost_basis_hash(*_basis(current_price="41"))
    assert first != second
```

- [ ] **Step 2: Run tests and confirm the missing module fails**

Run:

```bash
uv run --package nexus-engine pytest engine/tests/unit/optimizer/test_scenario_spread_cost.py -q
```

Expected: collection FAIL because `nexus.optimizer.scenario_spread_cost` does not exist.

- [ ] **Step 3: Add strict internal models and pure functions**

Use frozen Pydantic models with `extra="forbid"`; render UUID, UTC datetime, and Decimal using the existing canonical scenario rules. The core calculation must be exactly:

```python
def _instrument_spread_ratio(
    *, sold_weight: Decimal, snapshot_current_price: Decimal,
    bid: Decimal, ask: Decimal,
) -> Decimal:
    midpoint = (bid + ask) / Decimal("2")
    return sold_weight * (midpoint - bid) / snapshot_current_price


def apply_scenario_spread_cost(
    *, snapshot: PortfolioSnapshot,
    base_result: PortfolioScenarioEvaluationV2,
    evidence: ScenarioSpreadEvidenceV1,
) -> PortfolioScenarioEvaluationV2:
    if snapshot.id != base_result.portfolio_snapshot_id:
        raise ValueError("SCENARIO_SPREAD_COST_BASIS_INVALID")
    if scenario_cost_basis_hash(snapshot, base_result) != evidence.input.scenario_cost_basis_hash:
        raise ValueError("SCENARIO_SPREAD_COST_BASIS_INVALID")
    reduced = tuple(p for p in base_result.positions if p.target_weight < p.current_weight)
    quotes = {quote.instrument_id: quote for quote in evidence.quotes}
    spread = TypedMetric.unknown("BID_ASK_EVIDENCE_UNAVAILABLE")
    if evidence.input.policy_status == "available" and reduced and set(quotes) == {
        position.instrument_id for position in reduced
    }:
        snapshot_prices = {
            position.instrument_id: position.current_price
            for position in snapshot.positions
            if position.current_price is not None
        }
        spread = TypedMetric.known(sum(
            (
                _instrument_spread_ratio(
                    sold_weight=position.current_weight - position.target_weight,
                    snapshot_current_price=snapshot_prices[position.instrument_id],
                    bid=quotes[position.instrument_id].bid,
                    ask=quotes[position.instrument_id].ask,
                )
                for position in reduced
            ),
            Decimal("0"),
        ))
    breakdown = base_result.cost_breakdown.model_copy(update={
        "evidence_version": (
            "scenario_cost_evidence_bundle_v1" if spread.status == "known"
            else base_result.cost_breakdown.evidence_version
        ),
        "evidence_hash": (
            evidence.bundle_hash if spread.status == "known"
            else base_result.cost_breakdown.evidence_hash
        ),
        "available_at": (
            evidence.available_at if spread.status == "known"
            else base_result.cost_breakdown.available_at
        ),
        "spread": spread,
    })
    candidate = base_result.candidate.model_copy(update={
        "estimated_cost": TypedMetric.unknown("COST_EVIDENCE_INCOMPLETE")
    })
    updated = base_result.model_copy(update={"cost_breakdown": breakdown, "candidate": candidate})
    return updated.model_copy(update={"result_hash": canonical_scenario_hash(updated)})
```

The bundle hash input must include `scenario_cost_basis_hash_v1`, no-cost candidate hash, regulatory reference hash or `None`, policy payload hash, calculation version, calendar authority/session window, `evidence_selected_at`, and sorted quote/artifact identities. Do not include `expected_result_hash` or the final manifest hash in the bundle hash.

- [ ] **Step 4: Run the pure tests and existing scenario regressions**

Run:

```bash
uv run --package nexus-engine pytest engine/tests/unit/optimizer/test_scenario_spread_cost.py engine/tests/unit/optimizer/test_scenario_engine.py engine/tests/unit/optimizer/test_scenario_contracts.py -q
uv run ruff check engine/src/nexus/optimizer/scenario_spread_cost.py engine/tests/unit/optimizer/test_scenario_spread_cost.py
```

Expected: all tests and Ruff pass; existing no-cost evaluation still returns the regulatory reference and unknown spread until evidence is explicitly applied.

- [ ] **Step 5: Commit Task 2**

```bash
git add engine/src/nexus/optimizer/scenario_spread_cost.py engine/tests/unit/optimizer/test_scenario_spread_cost.py engine/tests/unit/optimizer/test_scenario_engine.py
git commit -m "feat: calculate frozen scenario spread cost"
```

## Task 3: Select and verify frozen quote evidence under one MVCC snapshot

**Files:**
- Modify: `engine/src/nexus/optimizer/scenario_spread_cost.py`
- Modify: `engine/src/nexus/ops/alpaca_market_stream.py`
- Modify: `engine/tests/unit/ops/test_alpaca_market_stream.py`
- Create: `engine/tests/integration/test_scenario_spread_cost_authority_pg.py`

**Interfaces:**
- Produces: `parse_market_stream_raw_payload(raw: bytes) -> MarketStreamArchive`.
- Produces: `select_scenario_spread_evidence(session, store, snapshot, base_result) -> ScenarioSpreadEvidenceV1` with a provisional `input` that has no final-result field.
- Produces: `replay_scenario_spread_evidence(session, store, snapshot, base_result, manifest) -> ScenarioSpreadEvidenceV1`.
- Consumes: a `MarketObjectStore` implementing `read_raw(key, *, uri) -> bytes`.
- Raises: `ScenarioSpreadEvidenceReplayInvalid` for a structurally or cryptographically inconsistent selected/replayed authority.

- [ ] **Step 1: Write failing parser, SQL-selection, tamper, and concurrency tests**

Add tests for 45-second boundaries, XNYS regular session only, latest deterministic ordering, missing one reduced quote, policy unavailable, row-field tampering with unchanged stored hash, actual artifact-byte tampering, exact replay, and an uncommitted concurrent insert:

```python
@pytest.mark.asyncio
async def test_selection_cannot_see_quote_committed_after_statement_snapshot(database_url):
    writer = await _begin_quote_insert_without_commit(database_url)
    selection = await _select_in_separate_transaction(database_url)
    await writer.commit()
    assert selection.input.selected_quotes == ()
    assert selection.spread_status == "unknown"


@pytest.mark.asyncio
async def test_replay_rejects_changed_bid_with_unchanged_stored_hash(database_url):
    manifest = await _select_apply_and_finalize_known_evidence(database_url)
    await _tamper_quote_under_replica_role(database_url, bid="1.01")
    with pytest.raises(ScenarioSpreadEvidenceReplayInvalid):
        await _replay(database_url, manifest)
```

- [ ] **Step 2: Run focused tests and confirm missing interfaces fail**

Run:

```bash
uv run --package nexus-engine pytest engine/tests/unit/ops/test_alpaca_market_stream.py engine/tests/integration/test_scenario_spread_cost_authority_pg.py -q --import-mode=importlib
```

Expected: FAIL because raw-payload parsing and spread authority selection/replay are not implemented.

- [ ] **Step 3: Expose the bounded raw parser and implement exact verification**

Decode only the existing frozen envelope shape and reuse `_parse_archive`; reject duplicate JSON keys, invalid base64, excess frames, and mismatched schema:

```python
def parse_market_stream_raw_payload(raw: bytes) -> MarketStreamArchive:
    envelope = json.loads(raw, object_pairs_hook=_unique)
    if set(envelope) != {"schema", "receipt_base64", "frames_base64"}:
        raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID")
    if envelope["schema"] != ARCHIVE_SCHEMA:
        raise MarketStreamError("ALPACA_MARKET_STREAM_ARCHIVE_INVALID")
    receipt_raw = base64.b64decode(envelope["receipt_base64"], validate=True)
    frames = [base64.b64decode(value, validate=True) for value in envelope["frames_base64"]]
    return _parse_archive(json.loads(receipt_raw, object_pairs_hook=_unique), frames, receipt_raw=receipt_raw)
```

For every selected row, call `canonical_quote_observation()` with `instrument_id`, `source`, `feed`, `delivery_mode`, `event_kind`, `bid`, `ask`, `last`, `observed_at`, `available_at`, `received_at`, `revision`, and `tuple(quality_flags)`; recompute its SHA-256 and compare it with the row and manifest hashes. Derive the content-addressed raw key from artifact receipt date and SHA-256, read actual bytes, compare byte size and SHA-256, parse them, and match symbol, bid, ask, and quote event time to the normalized row.

- [ ] **Step 4: Implement one-statement selection and exact replay**

Compute the pinned XNYS session window before SQL, verify the installed package version, then issue one statement shaped as follows:

```sql
WITH requested AS (
  SELECT unnest(CAST(:instrument_ids AS uuid[])) AS instrument_id
), policy AS (
  SELECT id,key,version,schema_version,payload,created_at
  FROM source_policies
  WHERE id=CAST(:policy_id AS uuid) AND created_at <= :valuation_time
), ranked AS (
  SELECT q.*, a.uri, a.sha256 AS artifact_hash, a.byte_size,
         a.mime_type, a.schema_version AS artifact_schema, a.source AS artifact_source,
         a.observed_at AS artifact_observed_at,
         a.available_at AS artifact_available_at,
         a.received_at AS artifact_received_at,
         a.created_at AS artifact_created_at,
         row_number() OVER (
           PARTITION BY q.instrument_id
           ORDER BY q.observed_at DESC, q.available_at DESC,
                    q.received_at DESC, q.id DESC
         ) AS rank
  FROM latest_quotes q
  JOIN requested r ON r.instrument_id=q.instrument_id
  JOIN data_artifacts a ON a.id=q.data_artifact_id
  WHERE q.source='alpaca' AND q.feed='iex'
    AND q.delivery_mode='real_time' AND q.event_kind='quote'
    AND q.evidence_status='verified' AND q.quality_flags='[]'::jsonb
    AND q.observed_at BETWEEN :session_open AND :valuation_time
    AND q.available_at <= :valuation_time AND q.received_at <= :valuation_time
    AND q.created_at <= :valuation_time AND a.created_at <= :valuation_time
    AND :valuation_time - q.observed_at <= interval '45 seconds'
)
SELECT statement_timestamp() AS evidence_selected_at, r.instrument_id,
       p.id AS policy_id, p.payload AS policy_payload, ranked.*
FROM requested r
LEFT JOIN policy p ON true
LEFT JOIN ranked ON ranked.instrument_id=r.instrument_id AND ranked.rank=1
ORDER BY r.instrument_id
```

Do not perform a second “latest quote” query after this statement. Replay loads policy, quote, and artifact rows by exact IDs from the manifest, recomputes every hash, reuses the frozen session window, and compares the final result with `expected_result_hash`; it never calls `exchange_calendars` to choose a new window.

- [ ] **Step 5: Run DB, parser, and pure calculation verification**

Run:

```bash
uv run --package nexus-engine pytest engine/tests/unit/ops/test_alpaca_market_stream.py engine/tests/unit/optimizer/test_scenario_spread_cost.py engine/tests/integration/test_scenario_spread_cost_authority_pg.py -q --import-mode=importlib
uv run ruff check engine/src/nexus/ops/alpaca_market_stream.py engine/src/nexus/optimizer/scenario_spread_cost.py engine/tests/integration/test_scenario_spread_cost_authority_pg.py
uv run mypy engine/src/nexus/optimizer/scenario_spread_cost.py engine/src/nexus/ops/alpaca_market_stream.py
```

Expected: all commands pass; the concurrency test proves the first statement does not see a later commit; exact replay remains stable after newer quotes are inserted.

- [ ] **Step 6: Commit Task 3**

```bash
git add engine/src/nexus/optimizer/scenario_spread_cost.py engine/src/nexus/ops/alpaca_market_stream.py engine/tests/unit/ops/test_alpaca_market_stream.py engine/tests/integration/test_scenario_spread_cost_authority_pg.py
git commit -m "feat: freeze scenario spread evidence"
```

## Task 4: Wire v3 scenario idempotency without changing the public contract

**Files:**
- Modify: `engine/src/nexus/api/routes/optimization.py`
- Modify: `engine/src/nexus/api/errors.py`
- Modify: `engine/src/nexus/main.py`
- Modify: `engine/tests/helpers/portfolio_api.py`
- Modify: `engine/tests/integration/test_portfolio_scenario_api.py`
- Modify: `engine/tests/unit/api/test_optimization_economic_contract.py`

**Interfaces:**
- Produces: route namespace `/internal/v1/portfolio-scenarios/evaluate:portfolio_scenario_evaluation_v3` and claim schema `portfolio_scenario_v3`.
- Produces: `ErrorCode.SCENARIO_SPREAD_EVIDENCE_REPLAY_INVALID` mapped to HTTP 409.
- Preserves: request `portfolio_scenario_request_v1`, response `portfolio_scenario_evaluation_v2`, and BFF payload.
- Consumes: `select_scenario_spread_evidence()`, `replay_scenario_spread_evidence()`, and `apply_scenario_spread_cost()`.

- [ ] **Step 1: Extend API tests with known, unknown, replay, and corruption cases**

Inject a fake `MarketObjectStore` through `create_engine_app`, seed a qualifying future snapshot/policy/quote/artifact, and assert:

```python
assert first.status_code == 200
assert first.json()["data"]["cost_breakdown"]["spread"] == {
    "status": "known", "value": "0.00025", "reason": None,
}
assert first.json()["data"]["cost_breakdown"]["evidence_version"] == (
    "scenario_cost_evidence_bundle_v1"
)
assert first.json()["data"]["candidate"]["estimated_cost"]["status"] == "unknown"
assert first.json()["data"]["execution_eligible"] is False
assert first.json()["data"]["execution_locked"] is True
assert stored_claim.route.endswith("portfolio_scenario_evaluation_v3")
assert stored_claim.response_payload["expected_result_hash"] == first.json()["data"]["result_hash"]
```

Also assert an owner with unavailable policy persists an unavailable manifest, replay remains unknown after a policy/quote appears, changed request bytes conflict, newer quotes do not change replay, and broken exact evidence returns the new 409 without a second audit event.

- [ ] **Step 2: Run the API test and confirm v2 behavior fails the new assertions**

Run:

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_portfolio_scenario_api.py -q --import-mode=importlib
```

Expected: FAIL because the route still uses v2 claims, does not persist a manifest, and never applies known spread evidence.

- [ ] **Step 3: Add object-store injection and v3 owner/replay branches**

Add an optional `scenario_evidence_store: MarketObjectStore | None` argument to `create_engine_app`; production defaults to `S3ProviderEvidenceStore(settings)`, while tests pass a bounded in-memory fake. In the route:

```python
claim, owner = await claim_or_lock(
    session, actor.actor_id,
    f"{route}:portfolio_scenario_evaluation_v3",
    idempotency_key, idempotency_request_hash, "portfolio_scenario_v3",
)
base_result = evaluate_adjustment_scenario(
    snapshot=snapshot,
    report=report,
    identities=identities,
    constraints=constraints,
    policy=policy,
    diagnostic_report_id=diagnostic_record.id,
    diagnostic_input_hash=diagnostic_record.diagnostic_input_hash,
    request_hash=canonical_scenario_hash(scenario_request),
)
if owner:
    evidence = await select_scenario_spread_evidence(
        session, request.app.state.scenario_evidence_store,
        snapshot=snapshot, base_result=base_result,
    )
    result = apply_scenario_spread_cost(
        snapshot=snapshot, base_result=base_result, evidence=evidence,
    )
    claim.response_payload = finalize_scenario_spread_manifest(
        evidence.input, expected_result_hash=result.result_hash,
    ).model_dump(mode="json")
else:
    manifest = ScenarioSpreadEvidenceManifestV1.model_validate(claim.response_payload)
    evidence = await replay_scenario_spread_evidence(
        session,
        request.app.state.scenario_evidence_store,
        snapshot=snapshot,
        base_result=base_result,
        manifest=manifest,
    )
    result = apply_scenario_spread_cost(
        snapshot=snapshot,
        base_result=base_result,
        evidence=evidence,
    )
    if result.result_hash != manifest.expected_result_hash:
        raise ScenarioSpreadEvidenceReplayInvalid()
```

Translate only `ScenarioSpreadEvidenceReplayInvalid` to the stable 409. Ordinary missing policy/quote evidence returns a successful partial analysis with unknown spread. Audit only the owner and add `spread_status` plus policy version; never add quote IDs, instrument IDs, weights, or amounts.

- [ ] **Step 4: Run API, error-contract, and no-write regressions**

Run:

```bash
uv run --package nexus-engine pytest engine/tests/integration/test_portfolio_scenario_api.py engine/tests/unit/api/test_optimization_economic_contract.py -q --import-mode=importlib
uv run ruff check engine/src/nexus/api/routes/optimization.py engine/src/nexus/api/errors.py engine/src/nexus/main.py
uv run mypy engine/src/nexus/api/routes/optimization.py engine/src/nexus/main.py
```

Expected: all pass; proposal, Shadow, paper order, and fill counts stay unchanged; no Provider mock is called.

- [ ] **Step 5: Commit Task 4**

```bash
git add engine/src/nexus/api/routes/optimization.py engine/src/nexus/api/errors.py engine/src/nexus/main.py engine/tests/helpers/portfolio_api.py engine/tests/integration/test_portfolio_scenario_api.py engine/tests/unit/api/test_optimization_economic_contract.py
git commit -m "feat: bind scenario spread evidence to v3 replay"
```

## Task 5: Preserve exact spread authority through Agent V2

**Files:**
- Modify: `engine/src/nexus/agents/portfolio_decision_contracts.py`
- Modify: `engine/src/nexus/agents/portfolio_decision_persistence.py`
- Modify: `engine/src/nexus/agents/portfolio_decision_tools.py`
- Modify: `engine/src/nexus/agents/portfolio_decision_execution.py`
- Modify: `engine/src/nexus/agents/portfolio_decision_worker.py`
- Modify: `engine/src/nexus/worker.py`
- Modify: `engine/src/nexus/api/routes/portfolio_decisions.py`
- Modify: `engine/tests/unit/agents/test_portfolio_decision_contracts.py`
- Modify: `engine/tests/unit/agents/test_portfolio_decision_persistence.py`
- Modify: `engine/tests/unit/agents/test_portfolio_decision_tools.py`
- Modify: `engine/tests/unit/agents/test_portfolio_decision_execution.py`
- Modify: `engine/tests/integration/test_portfolio_decision_admission_pg.py`
- Modify: `engine/tests/integration/test_portfolio_decision_worker_pg.py`
- Modify: `engine/tests/integration/test_portfolio_decision_api.py`
- Modify: `engine/tests/integration/test_portfolio_decision_e2e.py`

**Interfaces:**
- Produces: `PortfolioDecisionInputV2` with `scenario_claim_id: UUID` and `scenario_evidence_manifest_hash: Hash64` in addition to all V1 fields.
- Produces: `parse_portfolio_decision_input(payload) -> PortfolioDecisionInputV1 | PortfolioDecisionInputV2`.
- Produces: `load_exact_scenario_evaluation(session, store, frozen_v2) -> PortfolioScenarioEvaluationV2`.
- Consumes: the same `MarketObjectStore` passed by the worker; no tool performs Provider I/O or latest-quote selection.
- Preserves: V1 read/recovery/terminal behavior and the public Agent create/read/tool projection contracts.

- [ ] **Step 1: Write the V1/V2 contract and route-read matrix tests**

Add strict parsing tests and update every fixture that intentionally exercises V1 to keep `schema_version="portfolio_decision_input_v1"`. Add V2 tests that reject missing/extra claim fields and altered manifest hashes:

```python
def test_parse_portfolio_decision_input_keeps_v1_and_accepts_strict_v2():
    assert isinstance(parse_portfolio_decision_input(_v1_payload()), PortfolioDecisionInputV1)
    parsed = parse_portfolio_decision_input(_v2_payload())
    assert isinstance(parsed, PortfolioDecisionInputV2)
    assert parsed.scenario_claim_id == SCENARIO_CLAIM_ID


def test_v2_hash_changes_with_scenario_claim_identity():
    first = PortfolioDecisionInputV2.model_validate(_v2_payload())
    second = first.model_copy(update={"scenario_claim_id": OTHER_CLAIM_ID})
    assert canonical_portfolio_decision_hash(first) != canonical_portfolio_decision_hash(second)
```

- [ ] **Step 2: Run contract and admission tests and confirm V2 is absent**

Run:

```bash
uv run --package nexus-engine pytest engine/tests/unit/agents/test_portfolio_decision_contracts.py engine/tests/integration/test_portfolio_decision_admission_pg.py -q --import-mode=importlib
```

Expected: FAIL because only `PortfolioDecisionInputV1` exists and admission recomputes without a scenario claim.

- [ ] **Step 3: Add the discriminated frozen-input parser and claim resolver**

Extract the existing shared V1 fields into a private base model, define V1 and V2 as sibling strict contracts, and replace direct `PortfolioDecisionInputV1.model_validate(run.frozen_input)` calls with one parser. This preserves V1 serialization while avoiding an incompatible `schema_version` type override:

```python
class _PortfolioDecisionInputBase(_StrictPortfolioDecisionModel):
    portfolio_id: UUID
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    decision_time: datetime
    objective: ScenarioObjective
    max_turnover: RatioDecimalString
    minimum_cash: RatioDecimalString
    universe_policy: Literal["current_holdings_only"]
    allow_new_positions: Literal[False]
    scenario_request_hash: Hash64
    expected_scenario_result_hash: Hash64
    diagnostic_input_hash: Hash64
    policy_version: str = Field(min_length=1, max_length=100)
    evidence_hash: Hash64

class PortfolioDecisionInputV1(_PortfolioDecisionInputBase):
    schema_version: Literal["portfolio_decision_input_v1"]

class PortfolioDecisionInputV2(_PortfolioDecisionInputBase):
    schema_version: Literal["portfolio_decision_input_v2"]
    scenario_claim_id: UUID
    scenario_evidence_manifest_hash: Hash64


PortfolioDecisionInput = Annotated[
    PortfolioDecisionInputV1 | PortfolioDecisionInputV2,
    Field(discriminator="schema_version"),
]
_PORTFOLIO_DECISION_INPUT_ADAPTER = TypeAdapter(PortfolioDecisionInput)

def parse_portfolio_decision_input(value: object) -> PortfolioDecisionInput:
    return _PORTFOLIO_DECISION_INPUT_ADAPTER.validate_python(value)
```

At new Agent admission, query v3 claims for the same actor and `response_payload.expected_result_hash`, order by `(created_at, id)`, validate each manifest and its exact dependencies, and choose the first fully valid one. Recompute the no-cost candidate from the parsed Agent request and compare its canonical `scenario_request_hash`, `no_cost_candidate_hash`, and `scenario_cost_basis_hash` chain with the manifest. Do **not** try to reconstruct `IdempotencyRecord.request_hash`, because that field hashes the original request bytes; only require that the selected row retains a canonical 64-character hash, then bind future recovery to the exact row ID and manifest hash. Different valid manifests with the same result hash are allowed. Freeze its ID and full manifest hash into V2. If no fully valid claim exists, return the existing portfolio-decision authority mismatch without reserving budget or enqueuing work. Pass `request.app.state.scenario_evidence_store` from `create_portfolio_decision_run()` into admission for this verification.

- [ ] **Step 4: Write failing exact-replay worker/tool tests**

Cover these cases explicitly:

```python
@pytest.mark.asyncio
async def test_v2_tool_replays_frozen_claim_after_new_quote_is_inserted(
    migrated_database_url: str,
):
    session, store, run = await _seed_admitted_v2_run(migrated_database_url)
    await _insert_newer_quote(migrated_database_url, observed_at=VALUATION_TIME)
    tools = await build_portfolio_decision_tools(session, run, scenario_evidence_store=store)
    projection = await tools.execute("evaluate_adjustment_scenario", {
        "objective": "reduce_security_concentration",
    })
    assert projection["result_hash"] == run.frozen_input["expected_scenario_result_hash"]


@pytest.mark.asyncio
async def test_same_result_different_manifests_selects_first_fully_valid_claim(
    migrated_database_url: str,
):
    first, second = await _seed_two_unknown_claims_with_different_manifests(
        migrated_database_url
    )
    run = await _admit_v2_run_for_result(
        migrated_database_url,
        expected_result_hash=first.response_payload["expected_result_hash"],
    )
    assert run.frozen_input["scenario_claim_id"] == str(first.id)
    assert first.id != second.id
```

Also test missing exact claim, changed stored manifest, changed quote bytes, worker restart, tool result recording, terminal persistence, API read of V1, and API read of V2.

- [ ] **Step 5: Thread the evidence store through execution and worker recovery**

Change signatures explicitly rather than using global state:

```python
async def build_portfolio_decision_tools(
    session: AsyncSession,
    run: PortfolioDecisionRunRecord,
    *,
    scenario_evidence_store: MarketObjectStore,
) -> PortfolioDecisionToolRegistry:
    frozen = parse_portfolio_decision_input(run.frozen_input)
    evaluation = await load_exact_scenario_evaluation(
        session, scenario_evidence_store, run=run, frozen=frozen
    )
    return PortfolioDecisionTools(
        objective=frozen.objective,
        expected_result_hash=frozen.expected_scenario_result_hash,
        citation_ids=citations.citation_ids,
        executor=lambda: evaluation,
    )

async def load_portfolio_decision_execution_authority(
    session: AsyncSession,
    *,
    run: PortfolioDecisionRunRecord,
    scenario_evidence_store: MarketObjectStore,
) -> PortfolioDecisionExecutionAuthority:
    frozen = parse_portfolio_decision_input(run.frozen_input)
    tools = await build_portfolio_decision_tools(
        session, run, scenario_evidence_store=scenario_evidence_store
    )
    return PortfolioDecisionExecutionAuthority(
        frozen=frozen,
        context=context,
        prompt=prompt.content,
        model=model,
        model_config=model_config,
        tools=tools,
    )

# Add `scenario_evidence_store: MarketObjectStore` to the existing worker signature,
# then change only the authority call inside its existing `start` callback:
authority = await load_portfolio_decision_execution_authority(
    target,
    run=run,
    scenario_evidence_store=scenario_evidence_store,
)
```

For V1, retain the existing `evaluate_adjustment_scenario()` path. For V2, load the exact claim by ID, require the same actor, v3 route/schema, canonical row hash shape, manifest hash, canonical scenario/no-cost input chain, and expected result hash; do not equate the parsed request hash with the idempotency raw-body hash. Call `replay_scenario_spread_evidence()` and compare the reproduced result hash. Update API `_response`, worker authority load, tool-result recording, terminal validation, and `validate_portfolio_intent` to accept the union through `parse_portfolio_decision_input()`.

- [ ] **Step 6: Run the full focused Agent matrix**

Run:

```bash
uv run --package nexus-engine pytest \
  engine/tests/unit/agents/test_portfolio_decision_contracts.py \
  engine/tests/unit/agents/test_portfolio_decision_persistence.py \
  engine/tests/unit/agents/test_portfolio_decision_tools.py \
  engine/tests/unit/agents/test_portfolio_decision_execution.py \
  engine/tests/integration/test_portfolio_decision_admission_pg.py \
  engine/tests/integration/test_portfolio_decision_worker_pg.py \
  engine/tests/integration/test_portfolio_decision_api.py \
  engine/tests/integration/test_portfolio_decision_e2e.py \
  -q --import-mode=importlib
uv run ruff check engine/src/nexus/agents engine/src/nexus/api/routes/portfolio_decisions.py engine/src/nexus/worker.py
uv run mypy engine/src/nexus/agents engine/src/nexus/api/routes/portfolio_decisions.py engine/src/nexus/worker.py
```

Expected: all pass; both V1 and V2 complete the same public lifecycle, while V2 uses its exact claim and no latest quote selection.

- [ ] **Step 7: Commit Task 5**

```bash
git add engine/src/nexus/agents/portfolio_decision_contracts.py engine/src/nexus/agents/portfolio_decision_persistence.py engine/src/nexus/agents/portfolio_decision_tools.py engine/src/nexus/agents/portfolio_decision_execution.py engine/src/nexus/agents/portfolio_decision_worker.py engine/src/nexus/worker.py engine/src/nexus/api/routes/portfolio_decisions.py engine/tests/unit/agents engine/tests/integration/test_portfolio_decision_admission_pg.py engine/tests/integration/test_portfolio_decision_worker_pg.py engine/tests/integration/test_portfolio_decision_api.py engine/tests/integration/test_portfolio_decision_e2e.py
git commit -m "feat: replay scenario cost authority in portfolio agent"
```

## Task 6: Distinguish cost evidence authority in the page

**Files:**
- Modify: `app/components/portfolio-scenario.tsx`
- Modify: `tests/unit/portfolio/portfolio-scenario.test.tsx`
- Modify: `tests/e2e/portfolio-scenario.spec.ts`

**Interfaces:**
- Consumes: existing `cost_breakdown.evidence_version` and `available_at`; no BFF or generated schema change.
- Produces: three fail-closed presentation branches: regulatory reference, `scenario_cost_evidence_bundle_v1`, and unrecognized version.

- [ ] **Step 1: Write failing component tests for all three evidence versions**

```tsx
it("labels a spread bundle without claiming regulatory pass-through evidence", async () => {
  renderScenario({
    evidence_version: "scenario_cost_evidence_bundle_v1",
    spread: { status: "known", value: "0.00025", reason: null },
  });
  expect(screen.getByText(/情景成本证据束已记录/)).toBeVisible();
  expect(screen.getByText(/点差依据已冻结/)).toBeVisible();
  expect(screen.getByText(/监管费转嫁仍未证明/)).toBeVisible();
  expect(screen.queryByText(/公开监管费率参考已记录/)).not.toBeInTheDocument();
});

it("fails closed for an unrecognized cost evidence version", async () => {
  renderScenario({ evidence_version: "unexpected_cost_authority_v9" });
  expect(screen.getByText(/成本证据版本未识别/)).toBeVisible();
  expect(screen.queryByText(/依据已冻结/)).not.toBeInTheDocument();
});
```

Keep the existing regulatory-reference assertion as the third branch.

- [ ] **Step 2: Run the component tests and confirm the generic regulatory copy fails**

Run:

```bash
npm run test:unit -- tests/unit/portfolio/portfolio-scenario.test.tsx
```

Expected: FAIL because the component currently labels every non-null evidence version as a public regulatory-rate reference.

- [ ] **Step 3: Add one small evidence-copy helper and render its result**

```tsx
function costEvidenceCopy(version: string): string {
  if (version.startsWith("us_regulatory_fee_reference_")) {
    return "公开监管费率参考已记录；不等于复星账户实际收费依据。";
  }
  if (version === "scenario_cost_evidence_bundle_v1") {
    return "情景成本证据束已记录；点差依据已冻结，监管费转嫁仍未证明。";
  }
  return "成本证据版本未识别；当前不能据此确认任何成本权威。";
}
```

Retain the evidence version and formatted `available_at` beside this copy. Do not add controls or change number formatting.

- [ ] **Step 4: Run component, route, type, and lint checks**

Run:

```bash
npm run test:unit -- tests/unit/portfolio/portfolio-scenario.test.tsx tests/unit/engine/portfolio-scenario-routes.test.ts
npm run typecheck
npm run lint -- app/components/portfolio-scenario.tsx tests/unit/portfolio/portfolio-scenario.test.tsx
```

Expected: all pass; existing regulatory copy remains, spread bundle copy is distinct, unknown versions make no authority claim.

- [ ] **Step 5: Run the bounded browser contract**

Run:

```bash
npm run test:e2e -- tests/e2e/portfolio-scenario.spec.ts
```

Expected: pass with one POST to the scenario endpoint, no page-triggered market refresh, no Shadow action, and the locked badge still visible.

- [ ] **Step 6: Commit Task 6**

```bash
git add app/components/portfolio-scenario.tsx tests/unit/portfolio/portfolio-scenario.test.tsx tests/e2e/portfolio-scenario.spec.ts
git commit -m "fix: label scenario cost evidence precisely"
```

## Task 7: Run cross-layer verification and record the real read-only result

**Files:**
- Modify: `docs/plans/2026-09-02-core-evidence-checkpoint.md`

**Interfaces:**
- Consumes: completed Tasks 1-6 on one integrated branch.
- Produces: a timestamped evidence checkpoint distinguishing code, integration, runtime activation, and current real-data coverage.
- Preserves: current real broker/account data, Shadow records, paper orders, fills, and Provider request counts.

- [ ] **Step 1: Run the complete focused backend suite**

```bash
uv run --package nexus-engine pytest \
  engine/tests/unit/optimizer/test_scenario_contracts.py \
  engine/tests/unit/optimizer/test_scenario_engine.py \
  engine/tests/unit/optimizer/test_scenario_cost_evidence.py \
  engine/tests/unit/optimizer/test_scenario_spread_cost.py \
  engine/tests/unit/ops/test_alpaca_market_stream.py \
  engine/tests/integration/test_scenario_spread_cost_migration.py \
  engine/tests/integration/test_scenario_spread_cost_authority_pg.py \
  engine/tests/integration/test_portfolio_scenario_api.py \
  engine/tests/unit/agents/test_portfolio_decision_contracts.py \
  engine/tests/unit/agents/test_portfolio_decision_persistence.py \
  engine/tests/unit/agents/test_portfolio_decision_tools.py \
  engine/tests/unit/agents/test_portfolio_decision_execution.py \
  engine/tests/integration/test_portfolio_decision_admission_pg.py \
  engine/tests/integration/test_portfolio_decision_worker_pg.py \
  engine/tests/integration/test_portfolio_decision_api.py \
  engine/tests/integration/test_portfolio_decision_e2e.py \
  -q --import-mode=importlib
```

Expected: exit 0 with no Provider network access.

- [ ] **Step 2: Run static, generated-contract, and frontend verification**

```bash
uv run ruff check engine
uv run mypy engine/src
npm run test:unit -- tests/unit/portfolio/portfolio-scenario.test.tsx tests/unit/engine/portfolio-scenario-routes.test.ts tests/unit/engine/portfolio-decision-routes.test.ts
npm run typecheck
npm run check:generated
npm run lint
```

Expected: all commands exit 0; `check:generated` reports no public API drift.

- [ ] **Step 3: Verify safety counters before and after the normal local read path**

Use the existing local configuration and normal API/page entry; do not start an Alpaca or Fosun collection. Capture aggregate counts only:

```sql
SELECT
  (SELECT count(*) FROM shadow_groups) AS shadow_groups,
  (SELECT count(*) FROM paper_orders) AS paper_orders,
  (SELECT count(*) FROM paper_fills) AS paper_fills,
  (SELECT count(*) FROM latest_quotes
    WHERE source='alpaca' AND feed='iex' AND event_kind='quote'
      AND evidence_status='verified') AS verified_iex_quotes;
```

Then open the existing portfolio page, generate one read-only adjustment scenario through the normal product endpoint, and record:

- whether the current snapshot has a post-0050 policy and full 45-second quote coverage for every reduced instrument;
- the returned `spread.status`, evidence version, and `result_hash` without recording holdings, prices, weights, amounts, or account identifiers;
- that commission, slippage, regulatory pass-through, tax, and total remain unknown;
- that `analysis_status=partial`, `execution_eligible=false`, and `execution_locked=true`;
- that Shadow/order/fill counts and Provider collection counts did not change.

Expected for the currently known real snapshot: spread remains unknown because it predates the new policy and has 0/12 artifact-bound bid/ask coverage. If the persisted state has changed, report the newly measured aggregate instead of copying this expectation.

- [ ] **Step 4: Append one evidence checkpoint section**

Append a section titled `情景点差成本权威（2026-09-11）` containing:

```markdown
- 可用结果：已持久化且合格的 Alpaca IEX 盘口可形成只读点差估算；其他成本与执行锁不变。
- 代码与集成：列出集成提交和正常 API/Agent V2 路径是否验证。
- 实际验证：列出执行过的命令及退出状态；记录真实数据覆盖聚合，不记录账户事实明细。
- 当前缺口：如真实快照无合格盘口，保留 `BID_ASK_EVIDENCE_UNAVAILABLE`，不触发采集。
- 安全边界：Provider 调用、Shadow、订单、成交均未新增；`execution_locked=true`。
```

Use actual results and timestamps; do not paste the example as an unverified claim.

- [ ] **Step 5: Review the integrated diff for scope and secrets**

```bash
git diff --check HEAD~6
git diff --stat HEAD~6
git diff HEAD~6 -- . ':!uv.lock' | rg -n 'TO[D]O|TB[D]|API_KEY|SECRET|TOKEN|BEGIN PRIVATE KEY' && exit 1 || true
git status --short
```

Expected: no whitespace errors, no placeholders or credential material, only the planned files, and no unrelated user changes included.

- [ ] **Step 6: Commit the verified checkpoint**

```bash
git add docs/plans/2026-09-02-core-evidence-checkpoint.md
git commit -m "docs: record scenario spread cost verification"
```

## Stop Conditions

- Stop and report rather than weakening policy if the real snapshot has no eligible quote, the installed calendar version differs, the object artifact cannot be read, or any exact replay hash conflicts.
- Stop before any Provider collection, credential change, Shadow creation, paper order, real order, deployment, or paid entitlement change; none is required by this plan.
- A successful page render, HTTP 200, or known spread does not authorize execution and does not make total cost known.
- If the integrated change requires a new public field, new business table, different quote source, or second valuation time, return to design review instead of extending this plan in place.
