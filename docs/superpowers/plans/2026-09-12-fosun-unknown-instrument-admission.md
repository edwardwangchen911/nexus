# Fosun Unknown Instrument Admission Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Preserve raw discovery and known-instrument market updates when a normal Fosun refresh encounters a new US holding, while keeping the portfolio non-decision-ready until authoritative identity onboarding completes.

**Architecture:** Add an immutable provider-code discovery document on the generic artifact/snapshot path, split quote acquisition from known-instrument bar acquisition, and terminate the broker branch as a persisted PARTIAL attempt when unresolved identities exist. Existing READY behavior remains unchanged; no unresolved code receives an Instrument or Agent evidence owner.

**Tech Stack:** Python 3.12, Pydantic v2, SQLAlchemy async/PostgreSQL 17, FastAPI, pytest, Ruff, Mypy, vinext/React/TypeScript.

**Spec:** `docs/superpowers/specs/2026-09-12-fosun-unknown-instrument-admission-design.md`

## Global Constraints

- Fosun classification is routing evidence only; never infer canonical exchange or ETF versus ETN.
- Unresolved holdings never create `Instrument`, `InstrumentAlias`, position, order, research evidence, or execution authority.
- Full refresh performs one quote batch; bars are requested only for resolved codes.
- Pending refresh records `BrokerSyncRun.PARTIAL` and creates no `PortfolioSnapshotRecord`.
- Page/API/status reads use persisted data and perform zero provider or model calls.
- `execution_locked=true`; latest complete display snapshot and latest attempted refresh remain distinct.
- Broker-only refresh retains its existing fail-closed behavior for an unknown US identity.
- No new physical table or Alembic migration; migration head remains `0052_fosun_classification`.
- Do not modify or absorb unrelated intraday Alpaca changes in the main checkout.

---

### Task 1: Strict provider-code discovery evidence

**Files:**
- Create: `engine/src/nexus/market/fosun_instrument_discovery.py`
- Create: `engine/src/nexus/market/fosun_instrument_discovery_storage.py`
- Create: `engine/tests/unit/market/test_fosun_instrument_discovery.py`
- Create: `engine/tests/integration/test_fosun_instrument_discovery_storage.py`

**Interfaces:**
- Consumes: `FosunClassificationLevels`, `FosunFundAssetType`, and `FosunRawQuoteBinding` from `nexus.research.fosun_security_classification`; `FosunSecurityClassification` and `classify_fosun_security_family` from the existing gateway contract.
- Produces: `FosunInstrumentDiscoveryDocument`, `recommended_identity_route(family)`, `save_discovery_artifact(session, store, document)`, `persist_instrument_discovery(session, document, location)`, and `read_pending_instrument_discoveries(session, as_of)`.

- [ ] **Step 1: Write strict document tests**

Create literal fixtures proving:

```python
document = FosunInstrumentDiscoveryDocument(
    provider_code="usNEW",
    provider_name="New Company",
    market_scope="US",
    currency="USD",
    provider_product_type=6,
    levels=FosunClassificationLevels(first=1, second=101, third=10105),
    equity_profile_fields_complete=True,
    net_asset_profile_fields_complete=False,
    fund_asset_type=FosunFundAssetType(chs="", cht="", eng=""),
    routing_family="company_equity_like",
    recommended_identity_route="current_held_common_stock",
    holding_observed_at=NOW,
    received_at=NOW,
    available_at=NOW,
    raw_quote=RAW_BINDING,
)
assert document.identity_authoritative is False
assert document.canonical_instrument_id is None
assert document.execution_locked is True
assert document.provider_object_id == "discovery:usNEW"
```

Also assert fund/structured routes to `current_held_fund_or_structured`, unknown routes
to `manual_review`, mismatched family/route fails, malformed code fails, future or
misordered clocks fail, and ETF/ETN is never emitted as a canonical type.

- [ ] **Step 2: Run document tests and observe RED**

Run:

```bash
PYTHONPATH=engine/src /Users/wangchen/Documents/ChatGPT/投资/.venv/bin/python \
  -m pytest -q engine/tests/unit/market/test_fosun_instrument_discovery.py
```

Expected: collection fails because `nexus.market.fosun_instrument_discovery` does not exist.

- [ ] **Step 3: Implement the minimal strict document**

Use frozen strict Pydantic models, existing validators, canonical compact JSON, a
16-KiB normalized-document limit, exact route recomputation, and these constants:

```python
SOURCE = "fosun_instrument_discovery"
SCHEMA = "fosun_instrument_discovery_v1"
RAW_SCHEMA = "fosun_gateway_market_snapshot_v1"
```

- [ ] **Step 4: Run document tests and observe GREEN**

Run the Step 2 command. Expected: all tests pass.

- [ ] **Step 5: Write PostgreSQL storage tests**

Use the existing migrated-database fixture and in-memory object-store double. Assert:

```python
first = await persist_instrument_discovery(session, document, location=location)
replay = await persist_instrument_discovery(session, document, location=location)
assert replay.id == first.id
pending = await read_pending_instrument_discoveries(session, as_of=NOW)
assert pending[first.id] == document
assert await session.scalar(select(ResearchEvidenceRef).where(
    ResearchEvidenceRef.source == SOURCE
)) is None
```

Add literal conflict cases for canonical/raw URI/hash/length/request ID, equal-clock
different facts, post-`as_of` evidence, and an alias that becomes valid on `as_of`
causing the item to disappear from the current pending query without mutation.

- [ ] **Step 6: Run storage tests and observe RED**

Run:

```bash
NEXUS_TEST_DATABASE_URL="$NEXUS_TEST_DATABASE_URL" PYTHONPATH=engine/src \
  /Users/wangchen/Documents/ChatGPT/投资/.venv/bin/python -m pytest -q \
  engine/tests/integration/test_fosun_instrument_discovery_storage.py
```

Expected: fail because the storage functions are missing.

- [ ] **Step 7: Implement content-addressed storage and exact readback**

Mirror the savepoint/upsert/readback shape in
`fosun_security_classification_storage.py`, but insert only `DataArtifact` and
`DataSnapshot`. `read_pending_instrument_discoveries` must parse `provider_code`, map
it to the ticker symbol only after validation, and exclude a row when a unique valid
current US/USD ticker alias exists. Ambiguous aliases fail closed.

- [ ] **Step 8: Run Task 1 verification and commit**

Run both Task 1 test files plus Ruff and Mypy for the two modules. Commit:

```bash
git add engine/src/nexus/market/fosun_instrument_discovery.py \
  engine/src/nexus/market/fosun_instrument_discovery_storage.py \
  engine/tests/unit/market/test_fosun_instrument_discovery.py \
  engine/tests/integration/test_fosun_instrument_discovery_storage.py
git commit -m "feat(engine): persist pending Fosun identity evidence"
```

### Task 2: Partial identity resolution and refresh ordering

**Files:**
- Modify: `engine/src/nexus/ops/fosun_local_refresh.py`
- Modify: `engine/tests/unit/ops/test_fosun_refresh_order.py`
- Modify: `engine/tests/integration/test_fosun_identity_selection.py`
- Modify: `engine/tests/integration/test_fosun_refresh_order_pg.py`

**Interfaces:**
- Consumes: Task 1 discovery document/storage API.
- Produces: `_LocalBrokerIdentity.unresolved_codes`, split quote/bar read helpers, filtered known-instrument market persistence, and the `identity_pending` refresh result.

- [ ] **Step 1: Add the failing full-refresh ordering test**

Replace the old unit expectation that `identity_preflight` fails before market reading.
The new test uses one known and one unknown holding and asserts the consumer-visible
contract:

```python
result = await refresh_local_fosun(settings)
assert result["status"] == "identity_pending"
assert result["snapshot_status"] == "PARTIAL"
assert result["execution_locked"] is True
assert result["pending_instrument_count"] == 1
assert result["portfolio_snapshot_updated"] is False
assert state["quote_read"] is True
assert state["known_market_committed"] is True
assert state["broker_read"] is False
```

Assert literal request counts: accounts 1, holdings 1, quotes 1, bars 1, cash/orders/
cash_flows 0, total 4. Do not assert on mock existence; state changes represent real
flow boundaries.

- [ ] **Step 2: Run the unit test and observe RED**

Run:

```bash
PYTHONPATH=engine/src /Users/wangchen/Documents/ChatGPT/投资/.venv/bin/python \
  -m pytest -q engine/tests/unit/ops/test_fosun_refresh_order.py
```

Expected: the existing `LOCAL_BROKER_INSTRUMENT_SOURCE_REQUIRED` branch raises and
the pending assertions fail.

- [ ] **Step 3: Return unresolved codes without inventing IDs**

Extend `_LocalBrokerIdentity` with `unresolved_codes: tuple[str, ...] = ()`.
Add an `allow_unresolved: bool = False` parameter to `_load_local_broker_identity`.
Only the full refresh passes `True`; broker-only refresh uses the default. For a valid
unknown US/USD holding under full refresh, append the code and omit it from
`instrument_ids`. Preserve every existing ambiguity/invalid-source rejection.

- [ ] **Step 4: Split quote and bar acquisition**

Introduce private immutable return types and helpers:

```python
async def _read_quote_snapshot(market, *, codes) -> RawGatewayResponse: ...
async def _read_bar_snapshots(market, *, codes, request_start_at,
                              request_end_at, expected_sessions
                              ) -> tuple[tuple[str, RawGatewayResponse], ...]: ...
```

`refresh_local_fosun` reads the quote once for all US holding codes, resolves identity,
then reads bars only for codes present in `identity.instrument_ids`. Preserve the
existing maximum provider request budget.

- [ ] **Step 5: Persist known market and unresolved discovery atomically**

Update `_persist_market_snapshot` to parse the complete quote response once and archive
it once. For each quote item:

- when `instrument_code in identity.instrument_ids`, use existing classification and
  latest-quote paths;
- when in `identity.unresolved_codes`, construct/persist/read back the Task 1 document;
- otherwise raise a fixed scope mismatch.

Universe membership and bars use only resolved codes. Return known-symbol and pending
route counts separately. If there are zero resolved codes, skip existing producers
that require a non-empty instrument set while still committing raw/discovery evidence.

- [ ] **Step 6: Persist a PARTIAL sync attempt and stop broker reads**

Add `_persist_identity_pending_sync(session, identity, completed_at)` that inserts one
`BrokerSyncRun` using the existing local route, deterministic request-hash/idempotency
conventions, `status="PARTIAL"`, `attempt_count=1`, `failure_reason="IDENTITY_PENDING"`,
and `cash_flow_quality="NOT_RUN"`. It creates no `PortfolioSnapshotRecord`.
Before inserting, apply the same Account/Portfolio create-or-exactly-validate logic as
`_persist_ready_broker_snapshot`, covering first-ever local refresh without creating
an unresolved Instrument.

After the market transaction commits, call this helper and return the locked pending
result before `_read_broker_facts`. The READY branch remains byte-for-byte compatible
at its public result boundary.

- [ ] **Step 7: Run the unit test and observe GREEN**

Run the Step 2 command. Expected: pass for READY, pending, market failure, broker
failure, and broker-persistence identity-source cases.

- [ ] **Step 8: Add and run PostgreSQL cross-layer tests**

Add cases proving exact raw/discovery binding, known-market commit, PARTIAL sync, no
new PortfolioSnapshot, no Instrument/alias for unknown, and READY recovery after an
authoritative alias is seeded. Run:

```bash
NEXUS_TEST_DATABASE_URL="$NEXUS_TEST_DATABASE_URL" PYTHONPATH=engine/src \
  /Users/wangchen/Documents/ChatGPT/投资/.venv/bin/python -m pytest -q \
  engine/tests/integration/test_fosun_identity_selection.py \
  engine/tests/integration/test_fosun_refresh_order_pg.py
```

- [ ] **Step 9: Run Task 2 verification and commit**

Run the four affected test files, Ruff on changed Python, and Mypy on Engine. Commit:

```bash
git add engine/src/nexus/ops/fosun_local_refresh.py \
  engine/tests/unit/ops/test_fosun_refresh_order.py \
  engine/tests/integration/test_fosun_identity_selection.py \
  engine/tests/integration/test_fosun_refresh_order_pg.py
git commit -m "fix(engine): preserve refresh on pending Fosun identity"
```

### Task 3: Persisted status and protected API projection

**Files:**
- Modify: `engine/src/nexus/ops/fosun_local.py`
- Modify: `engine/src/nexus/api/schemas/portfolio.py`
- Modify: `engine/src/nexus/portfolio/service.py`
- Modify: `engine/src/nexus/api/routes/portfolios.py`
- Modify: `engine/tests/unit/ops/test_fosun_local.py`
- Modify: `engine/tests/integration/test_portfolio_api.py`

**Interfaces:**
- Consumes: Task 1 pending reader and Task 2 PARTIAL sync record.
- Produces: distinct latest-attempt fields and a protected persisted pending list in portfolio responses.

- [ ] **Step 1: Write failing status/API tests**

Seed a READY snapshot, then a newer PARTIAL `IDENTITY_PENDING` run and one unresolved
discovery. Assert:

```python
assert status["snapshot_status"] == "READY"
assert status["latest_sync_run_status"] == "PARTIAL"
assert status["latest_sync_failure_reason"] == "IDENTITY_PENDING"
assert status["decision_readiness"] == "NOT_READY"
assert status["pending_instrument_count"] == 1
```

The protected portfolio response exposes only provider code/name, routing family,
recommended route, available time, and `identity_status="pending"`. Assert forbidden
account, quantity, cost, raw URI, and raw payload fields are absent.

- [ ] **Step 2: Run tests and observe RED**

Run:

```bash
PYTHONPATH=engine/src /Users/wangchen/Documents/ChatGPT/投资/.venv/bin/python \
  -m pytest -q engine/tests/unit/ops/test_fosun_local.py \
  engine/tests/integration/test_portfolio_api.py
```

Expected: latest run is incorrectly taken from the latest snapshot and pending fields
are absent.

- [ ] **Step 3: Separate latest complete snapshot from latest attempt**

In `read_local_status`, query the newest `BrokerSyncRun` independently from the newest
snapshot. Derive decision readiness from the latest attempt: any newer PARTIAL pending
attempt forces `NOT_READY`, while display readiness and counts can still use the latest
complete snapshot. Add safe pending count and evidence time from Task 1.

- [ ] **Step 4: Add strict API schemas and service projection**

Add `PendingInstrumentIdentityResponse` and additive fields on the real-read-only
portfolio response. Query pending discovery via the Task 1 reader. Keep pagination
bounded by the current holding maximum and preserve authentication/audit behavior.

- [ ] **Step 5: Run status/API tests and observe GREEN**

Run Step 2. Expected: all pass and no provider double is constructed.

- [ ] **Step 6: Regenerate API clients and commit**

Run the repository's existing OpenAPI generation/check command from `package.json`,
then the API schema contract tests. Commit Engine/API and generated client changes:

```bash
git add engine/src/nexus/ops/fosun_local.py engine/src/nexus/api/schemas/portfolio.py \
  engine/src/nexus/portfolio/service.py engine/src/nexus/api/routes/portfolios.py \
  engine/tests/unit/ops/test_fosun_local.py engine/tests/integration/test_portfolio_api.py \
  app/lib/generated
git commit -m "feat(api): expose pending instrument identity state"
```

### Task 4: Portfolio pending-identity presentation

**Files:**
- Modify: `app/(protected)/portfolio/page.tsx`
- Modify: `app/components/portfolio-summary.tsx` only if the existing page cannot keep the banner local
- Modify: `tests/unit/portfolio/page.test.tsx`
- Modify: `tests/e2e/portfolio.spec.ts`

**Interfaces:**
- Consumes: Task 3 generated `PendingInstrumentIdentityResponse` fields.
- Produces: persisted-only pending banner and rows; no action endpoint.

- [ ] **Step 1: Write a failing component/page test**

Supply a portfolio response with one pending item and assert visible Chinese semantics:

```text
新增证券身份待确认
已保留复星分类证据；确认前不参与决策或交易
普通股身份资料待核验
```

Assert the page still renders the latest complete snapshot with an explicit stale/
previous-snapshot label and never renders “决策就绪” for the latest attempt.

- [ ] **Step 2: Run the focused frontend test and observe RED**

Run:

```bash
npx vitest run tests/unit/portfolio/page.test.tsx
```

Expected failure is the missing pending presentation.

- [ ] **Step 3: Implement the compact persisted-only UI**

Render one warning banner and bounded rows. Map the three route values to fixed Chinese
copy. Do not add refresh/onboarding/trading buttons. Preserve units, timestamps, and
existing snapshot cards.

- [ ] **Step 4: Run frontend verification and commit**

Run the focused test, frontend lint, typecheck, and build. Commit only the page/
component/test and generated contract changes belonging to this feature:

```bash
git add 'app/(protected)/portfolio/page.tsx' app/components/portfolio-summary.tsx \
  tests/unit/portfolio/page.test.tsx tests/e2e/portfolio.spec.ts
git commit -m "feat(web): show pending instrument identity"
```

### Task 5: Cross-layer verification, review, and integration

**Files:**
- Modify: `docs/superpowers/reviews/2026-09-12-fosun-unknown-instrument-admission-review.md`
- Modify: CI merge-gate list only if newly created tests are not already collected.

**Interfaces:**
- Consumes: Tasks 1-4.
- Produces: merge-ready isolated branch and exact rollout instructions.

- [ ] **Step 1: Run focused Engine verification**

Run all new/changed unit and PostgreSQL tests against an isolated test database. Verify
the test suite observes both the pre-fix RED history and current GREEN behavior.

- [ ] **Step 2: Run static and contract verification**

Run Ruff, Mypy, OpenAPI generation/check, frontend lint, typecheck, focused tests, and
build using commands from `.github/workflows/ci.yml` and `package.json`.

- [ ] **Step 3: Run the Engine merge gate**

Run the exact Engine merge-gate command from CI. Do not repeat an unrelated historical
full-suite command if the merge gate already covers the changed paths.

- [ ] **Step 4: Perform final code review**

Review specifically for invented identity, partial-portfolio leakage, clock/hash/raw
binding errors, extra provider calls, request-count drift, API privacy, and page-triggered
external work. Fix findings with a new failing regression first.

- [ ] **Step 5: Update review evidence and commit**

Record exact verification results, residual risks, and rollout sequence in the review
document. Commit the review/CI-only changes.

- [ ] **Step 6: Integrate without touching concurrent main changes**

Confirm the main checkout owner has finished or that its unrelated changes are safely
committed elsewhere. Merge the isolated branch into local `main` without resetting,
stashing, or overwriting user changes. If main is still dirty, stop before merge and
report the exact commits ready for integration.

- [ ] **Step 7: Define the separate runtime acceptance**

After merge and only under explicit runtime authorization: create a protected complete
backup, rebuild API, confirm migration head unchanged at `0052_fosun_classification`,
replace affected services, invoke one normal full Fosun refresh, then verify raw
discovery, known market updates, PARTIAL sync/no new portfolio snapshot, persisted API,
and page. Never replay an uncertain provider result.
