# NEXUS Phase 3A Offline Broker Simulation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a complete, deterministic, offline broker-simulation execution workbench while keeping every real Fosun write capability disabled and externally blocked.

**Architecture:** A new immutable broker-execution authority is separate from optimization proposals and internal paper orders. The Engine prepares server-owned intents and approvals; a broker-only worker executes typed commands through a narrow simulation gateway protocol. Fixture transport covers every write/reconciliation outcome without sockets. Database constraints, leases, hashes, expiry, account locks, and reconciliation own safety; Agent and browser state own none of it.

**Tech Stack:** Python 3.12, FastAPI, Pydantic 2, SQLAlchemy 2, Alembic, PostgreSQL 17, httpx ASGI transport, pytest/Hypothesis, React 19, TypeScript 5.9, vinext, Vitest, Playwright, Axe.

**Spec:** `docs/superpowers/specs/2026-08-28-nexus-phase-3-broker-simulation-design.md`

## Global Constraints

- `LIVE_WRITE` is absent from every Phase 3A enum, route, settings model, Compose profile, fixture, database row, and generated browser contract.
- `SIMULATION_WRITE` is a future externally admitted capability. Ordinary app/fixture mode stays `OFFLINE_FIXTURE`, returns `external_activation_blocked`, and exposes no enabled dispatch/modify/cancel browser control.
- Offline tests may execute commands only through an injected in-process `FixtureSimulationGateway`; they may not create an `httpx` network transport or read provider credentials.
- The Agent worker, Agent tools, research API, and strategy-R&D worker must not import `nexus.broker_simulation`, simulation gateway clients, account bindings, approvals, or commands.
- Browser write bodies contain identifiers, expected version/event sequence, and a fixed reason enum only. They never contain account IDs, symbols, prices, quantities, order types, Provider IDs, URLs, methods, or headers.
- An uncertain create/modify/cancel attempt is never retried. It creates `result_unknown`, starts reconciliation, and locks the account binding.
- Every task is implemented red-green with focused commits. Migration is generated as a new revision and historical revisions are never edited.

## Canonical State and Value Contracts

The implementation uses these exact closed values:

```text
CapabilityStatus: OFFLINE_FIXTURE | READ_ONLY_ADMITTED | READ_ONLY_ACTIVATED |
                  SIMULATION_CONTRACT_ADMITTED | SIMULATION_WRITE_ACTIVATED
ActivationDisplay: offline_ready | read_only_activated | simulation_write_activated
ExternalActivation: external_activation_blocked | externally_activated
IntentStatus: draft | prepared | approved | rejected | expired | dispatching |
              completed | partially_completed | failed | submission_unknown |
              reconciling | reconciled
CommandKind: create | modify | cancel
CommandStatus: prepared | sending | acknowledged | rejected | transport_failed |
               result_unknown | reconciled | reconciliation_required
OrderStatus: not_sent | acknowledged | working | partially_filled | filled |
             cancelled | rejected | expired | reconciling
ReconciliationStatus: pending | running | matched | conflict | acknowledged
```

Supported order facts are fixed to `USD`, `buy|sell`, positive whole shares, `limit`, `DAY`, regular session, long-only, and an instrument already admitted by the deterministic US-equity/ETF whitelist. All money and quantity values are canonical decimal strings in API/hash documents and `Decimal` in Python.

## File Map

- `engine/src/nexus/broker_simulation/domain.py`: enums, typed immutable documents, canonical hashes, transition guards, expiry.
- `engine/src/nexus/broker_simulation/risk.py`: deterministic account/order/batch revalidation.
- `engine/src/nexus/broker_simulation/intents.py`: prepare, approve, reject, expire, and query services.
- `engine/src/nexus/broker_simulation/commands.py`: dispatch lease, attempts, unknown-result policy, modify/cancel admission.
- `engine/src/nexus/broker_simulation/reconciliation.py`: polling facts, conflict lock, and acknowledgement.
- `engine/src/nexus/broker_simulation/webhooks.py`: bounded receipt/authentication adapter/idempotent candidate events.
- `engine/src/nexus/broker_simulation/jobs.py`: broker-only dispatch/reconcile handlers.
- `engine/src/nexus/persistence/models/broker_simulation.py`: ten Phase 3 table groups.
- `engine/alembic/versions/0024_broker_simulation.py`: forward migration and guarded empty-only downgrade.
- `engine/src/nexus/providers/broker/simulation_contracts.py`: exact Gateway request/response models.
- `engine/src/nexus/providers/broker/simulation_gateway.py`: injected `SimulationGateway` protocol and authenticated client.
- `integrations/fosun_gateway/src/nexus_fosun_gateway/adapter/simulation_fixture.py`: no-socket scripted fixture.
- `integrations/fosun_gateway/src/nexus_fosun_gateway/policy/simulation_operations.py`: exact seven-operation allowlist.
- `integrations/fosun_gateway/src/nexus_fosun_gateway/routes/simulation.py`: simulation-only routes.
- `integrations/fosun_gateway/src/nexus_fosun_gateway/simulation_main.py`: isolated simulation composition root.
- `engine/src/nexus/api/schemas/broker_simulation.py`: closed read/write envelopes.
- `engine/src/nexus/api/routes/broker_simulation.py`: authenticated internal routes.
- `app/lib/broker-simulation/bff.ts`: strict body/path/header validators.
- `app/api/v1/broker-simulation/**/route.ts`: fixed BFF route family.
- `app/(protected)/broker-simulation/page.tsx`: offline review workbench.
- `app/components/broker-simulation/*`: status, evidence, batch, timeline, and reconciliation views.
- `contracts/openapi.json`, `app/lib/generated/schema.d.ts`, `app/lib/generated/runtime-contract.ts`: generated outputs only.

---

### Task 1: Freeze pure domain contracts and transition laws

**Files:**
- Create: `engine/src/nexus/broker_simulation/__init__.py`
- Create: `engine/src/nexus/broker_simulation/domain.py`
- Create: `engine/tests/unit/broker_simulation/test_domain.py`
- Create: `engine/tests/property/test_broker_simulation_state_machines.py`

**Interfaces:**
- Produces: the exact enums above, `BrokerOrderDocument`, `BrokerIntentDocument`, `ApprovalDocument`, `canonical_document_hash`, `require_intent_transition`, `require_command_transition`, `require_order_transition`, and `effective_approval_expiry`.
- Consumes: `nexus.audit.canonical.canonical_json`, `nexus.domain.time.require_utc`, Decimal and UUID.

- [x] **Step 1: Write failing closed-enum, canonical-hash, transition, and expiry tests**

```python
def test_unknown_result_has_no_edge_back_to_sending() -> None:
    with pytest.raises(BrokerSimulationTransitionError, match="COMMAND_TRANSITION_INVALID"):
        require_command_transition(CommandStatus.RESULT_UNKNOWN, CommandStatus.SENDING)


def test_approval_expiry_uses_earliest_authority() -> None:
    assert effective_approval_expiry(
        prepared_at=utc("2026-08-28T14:00:00Z"),
        quote_expires_at=utc("2026-08-28T14:02:00Z"),
        capability_expires_at=utc("2026-08-28T14:04:00Z"),
        session_close_at=utc("2026-08-28T20:00:00Z"),
    ) == utc("2026-08-28T14:02:00Z")
```

- [x] **Step 2: Verify RED**

Run: `uv run --project engine pytest engine/tests/unit/broker_simulation/test_domain.py engine/tests/property/test_broker_simulation_state_machines.py -q`

Expected: collection fails because `nexus.broker_simulation.domain` is absent.

- [x] **Step 3: Implement only closed Pydantic/dataclass contracts and explicit edge sets**

`BrokerOrderDocument` uses `extra="forbid"`, is frozen, accepts no binary floats, validates whole shares and tick alignment, and includes `instrument_id`, `side`, `quantity`, `limit_price`, `currency="USD"`, `order_type="limit"`, `time_in_force="DAY"`, `session="regular"`, `sort_key`. `BrokerIntentDocument` binds proposal group, strategy decision, portfolio snapshot, evidence hash set, risk-policy hash, execution-model hash, account-binding ID, tuple of orders, and total notional. Hashes are SHA-256 of canonical JSON only.

- [x] **Step 4: Run focused tests and static checks**

Run: `uv run --project engine pytest engine/tests/unit/broker_simulation/test_domain.py engine/tests/property/test_broker_simulation_state_machines.py -q`

Run: `uv run --project engine ruff check engine/src/nexus/broker_simulation/domain.py engine/tests/unit/broker_simulation/test_domain.py engine/tests/property/test_broker_simulation_state_machines.py`

Expected: all pass; Hypothesis proves no invalid or backward edge is accepted.

- [x] **Step 5: Commit**

```bash
git add engine/src/nexus/broker_simulation engine/tests/unit/broker_simulation/test_domain.py engine/tests/property/test_broker_simulation_state_machines.py
git commit -m "feat(broker): define simulation execution authority"
```

---

### Task 2: Add the Phase 3 schema and ORM invariants

**Files:**
- Create: `engine/src/nexus/persistence/models/broker_simulation.py`
- Modify: `engine/src/nexus/persistence/models/__init__.py`
- Generate then modify: `engine/alembic/versions/0024_broker_simulation.py`
- Create: `engine/tests/integration/test_broker_simulation_schema.py`
- Create: `engine/tests/integration/test_broker_simulation_constraints.py`

**Interfaces:**
- Produces: ORM records for the ten table groups in design section 12 and revision `0024_broker_simulation` with `down_revision="0023_strategy_evolution_authority"`.
- Consumes: existing UUIDv7, immutable-created-at mixins, UTC type, deferred PostgreSQL constraints and Alembic test helpers.

- [x] **Step 1: Write failing metadata and database-constraint tests**

Tests assert exact table names, unique intent/order hashes, one approval per intent version/body hash, one active send attempt per command, monotonic event sequences, simulation-only binding checks, no `live` capability string, conflict-locked accounts rejecting new commands, and terminal event immutability.

- [x] **Step 2: Verify RED and generate the revision**

Run: `uv run --project engine pytest engine/tests/integration/test_broker_simulation_schema.py engine/tests/integration/test_broker_simulation_constraints.py -q`

Expected: missing model/table failures.

Generate: `uv run --project engine alembic -c engine/alembic.ini revision -m broker_simulation --rev-id 0024_broker_simulation`

- [x] **Step 3: Implement models, forward DDL, and guarded downgrade**

The revision creates: `broker_account_bindings`, `broker_execution_intents`, `broker_execution_intent_orders`, `broker_batch_approvals`, `broker_commands`, `broker_command_attempts`, `broker_order_events`, `broker_webhook_receipts`, `broker_reconciliation_runs`, `broker_reconciliation_diffs`, and `broker_simulation_admissions`. Provider/account/order external identifiers are stored only as keyed digest or encrypted envelope using the existing broker external-ID crypto boundary. Downgrade checks all eleven tables; if any row exists it raises SQLSTATE `P3D01`, otherwise drops in reverse dependency order.

- [x] **Step 4: Run round-trip and concurrency tests**

Run: `uv run --project engine pytest engine/tests/integration/test_broker_simulation_schema.py engine/tests/integration/test_broker_simulation_constraints.py engine/tests/integration/test_persistence_invariants.py -q`

Expected: empty `0023 -> 0024 -> 0023 -> 0024` succeeds; populated downgrade and every illegal invariant fail without residue.

- [x] **Step 5: Commit**

```bash
git add engine/src/nexus/persistence/models engine/alembic/versions/0024_broker_simulation.py engine/tests/integration/test_broker_simulation_schema.py engine/tests/integration/test_broker_simulation_constraints.py
git commit -m "feat(db): persist broker simulation authority"
```

---

### Task 3: Build the isolated fixture simulation Gateway

**Files:**
- Create: `engine/src/nexus/providers/broker/simulation_contracts.py`
- Create: `integrations/fosun_gateway/src/nexus_fosun_gateway/policy/simulation_operations.py`
- Create: `integrations/fosun_gateway/src/nexus_fosun_gateway/adapter/simulation_fixture.py`
- Create: `integrations/fosun_gateway/src/nexus_fosun_gateway/routes/simulation.py`
- Create: `integrations/fosun_gateway/src/nexus_fosun_gateway/simulation_main.py`
- Create: `integrations/fosun_gateway/tests/contract/test_simulation_fixture_contract.py`
- Create: `integrations/fosun_gateway/tests/security/test_simulation_surface.py`
- Modify: `integrations/fosun_gateway/pyproject.toml`

**Interfaces:**
- Produces exactly seven operations: `capabilities`, `create-order`, `modify-order`, `cancel-order`, `orders`, `portfolio`, `cash-flows` under `/internal/v1/fosun-simulation/`.
- Consumes existing HMAC/mTLS middleware; uses no SDK and no provider secret in fixture mode.

- [x] **Step 1: Write failing route-set and scripted-outcome tests**

Tests enumerate the FastAPI route table, reject extra keys and generic URL/method/header inputs, enforce 64 KiB request and 1 MiB response limits, and replay fixed scripts for acknowledged, rejected, partial batch, pre-send timeout, post-send unknown, pagination, duplicate event, and reconciliation conflict.

- [x] **Step 2: Verify RED**

Run: `uv run --package nexus-fosun-gateway pytest integrations/fosun_gateway/tests/contract/test_simulation_fixture_contract.py integrations/fosun_gateway/tests/security/test_simulation_surface.py -q`

- [x] **Step 3: Implement a separate composition root**

`simulation_main.create_simulation_gateway_app` accepts only `GatewaySettings.for_test()` plus injected `FixtureSimulationGatewayScript`. It does not import `SdkFosunTransport` or `read_only_router`. The existing `main.create_gateway_app` remains exact read-only and its route-set tests must prove no simulation route appeared.

- [x] **Step 4: Run Gateway regression with socket guard**

Run: `NEXUS_OFFLINE_SOCKET_GUARD=1 PYTHONPATH="$PWD/scripts/offline_guard" uv run --package nexus-fosun-gateway pytest integrations/fosun_gateway/tests -q`

Expected: all pass with zero external socket attempts; read-only and simulation operation sets are disjoint.

- [x] **Step 5: Commit**

```bash
git add engine/src/nexus/providers/broker/simulation_contracts.py integrations/fosun_gateway
git commit -m "feat(gateway): add isolated simulation fixture surface"
```

---

### Task 4: Implement account binding, intent preparation, and deterministic risk

**Files:**
- Create: `engine/src/nexus/broker_simulation/risk.py`
- Create: `engine/src/nexus/broker_simulation/intents.py`
- Create: `engine/tests/unit/broker_simulation/test_risk.py`
- Create: `engine/tests/integration/test_broker_simulation_intents.py`

**Interfaces:**
- Produces: `prepare_execution_intent`, `approve_execution_intent`, `reject_execution_intent`, `expire_execution_intents`, `BrokerRiskDecision`.
- Consumes existing proposal-group, strategy-decision, portfolio snapshot, market evidence, source policy, drawdown lock, whitelist and sector/cash constraints.

- [x] **Step 1: Write failing server-authority and drift tests**

Tests prove the service generates the order set from database facts, canonical sorting is stable, the browser cannot override facts, all order restrictions are enforced, and any evidence/account/risk/hash/version drift causes `INTENT_AUTHORITY_CHANGED` before approval.

- [x] **Step 2: Verify RED**

Run: `uv run --project engine pytest engine/tests/unit/broker_simulation/test_risk.py engine/tests/integration/test_broker_simulation_intents.py -q`

- [x] **Step 3: Implement prepare/approve/reject under row locks**

`prepare_execution_intent` requires a simulation binding with `reconciliation_status="matched"`, builds only `DAY` limit whole-share orders, creates an immutable intent/order set plus prepared event, and returns its canonical hash, version and expiry. `approve_execution_intent` accepts only `intent_id`, `actor_id`, `expected_version`, `if_match`, `request_body_hash`, `idempotency_key`, and `approved_at`; it reloads every authority and writes one approval or returns the original idempotent result. The actor is the verified service actor, never a body field.

- [x] **Step 4: Run focused and conflicting-approval tests**

Run: `uv run --project engine pytest engine/tests/unit/broker_simulation/test_risk.py engine/tests/integration/test_broker_simulation_intents.py -q`

Expected: all pass; two concurrent approvals yield one approval fact and one stable response.

- [x] **Step 5: Commit**

```bash
git add engine/src/nexus/broker_simulation/risk.py engine/src/nexus/broker_simulation/intents.py engine/tests/unit/broker_simulation/test_risk.py engine/tests/integration/test_broker_simulation_intents.py
git commit -m "feat(broker): prepare and approve simulation intents"
```

---

### Task 5: Implement command dispatch and no-resend uncertainty

**Files:**
- Create: `engine/src/nexus/providers/broker/simulation_gateway.py`
- Create: `engine/src/nexus/broker_simulation/commands.py`
- Create: `engine/tests/unit/providers/test_simulation_gateway_client.py`
- Create: `engine/tests/integration/test_broker_simulation_dispatch.py`

**Interfaces:**
- Produces: `SimulationGateway` protocol, `FixtureSimulationGatewayClient`, default-disabled `HttpSimulationGatewayClient`, `dispatch_approved_intent`, `prepare_modify_command`, `prepare_cancel_command`.
- Consumes Gateway contracts, HMAC client primitives, database attempt uniqueness, and Task 4 risk revalidation.

- [x] **Step 1: Write failing serial-dispatch and unknown-result tests**

Tests assert one external attempt maximum, canonical order sorting, stop-after-first-non-success, remaining `not_sent`, transport exception after possible send becomes `result_unknown`, worker restart starts reconciliation instead of resend, modify creates a new approval-bound command, and cancel requires explicit actor/idempotency input.

- [x] **Step 2: Verify RED**

Run: `uv run --project engine pytest engine/tests/unit/providers/test_simulation_gateway_client.py engine/tests/integration/test_broker_simulation_dispatch.py -q`

- [x] **Step 3: Implement the attempt transaction boundary**

Before each call, atomically acquire a command lease, persist `sending` and attempt number, revalidate account/capability/approval/snapshot/risk, then call the injected gateway outside the row-lock transaction. A definite response appends an outcome event. Any exception after attempt persistence writes only `result_unknown` plus account lock/reconciliation job. No code path transitions `result_unknown` to `sending`.

`HttpSimulationGatewayClient` can be constructed only when `SIMULATION_WRITE_ACTIVATED` and a valid admission record are injected. Phase 3A tests assert its constructor fails before creating `httpx.AsyncClient`.

- [x] **Step 4: Run failure-injection and restart tests**

Run: `uv run --project engine pytest engine/tests/unit/providers/test_simulation_gateway_client.py engine/tests/integration/test_broker_simulation_dispatch.py engine/tests/integration/test_outbox.py -q`

- [x] **Step 5: Commit**

```bash
git add engine/src/nexus/providers/broker/simulation_gateway.py engine/src/nexus/broker_simulation/commands.py engine/tests/unit/providers/test_simulation_gateway_client.py engine/tests/integration/test_broker_simulation_dispatch.py
git commit -m "feat(broker): dispatch simulation commands fail closed"
```

---

### Task 6: Add webhook receipts and polling reconciliation

**Files:**
- Create: `engine/src/nexus/broker_simulation/webhooks.py`
- Create: `engine/src/nexus/broker_simulation/reconciliation.py`
- Create: `engine/tests/unit/broker_simulation/test_webhooks.py`
- Create: `engine/tests/integration/test_broker_simulation_reconciliation.py`

**Interfaces:**
- Produces: `receive_webhook`, `run_simulation_reconciliation`, `acknowledge_reconciliation`; `WebhookAuthenticator` protocol whose Phase 3A implementation always returns `not_admitted` unless a fixture test injects a verifier.
- Consumes exact raw bytes, content hash, event identity, Gateway orders/portfolio/cash-flow snapshots, and account locks.

- [x] **Step 1: Write failing duplicate, disorder, conflict, and unlock tests**

Tests cover body limit, schema closure, repeated event ID/hash, event-ID collision with different bytes, old timestamps, out-of-order candidates, unauthenticated hints, polling confirmation, webhook/poll conflict, ambiguous unknown result, explicit acknowledgement, and inability to unlock without a complete matched snapshot.

- [x] **Step 2: Verify RED**

Run: `uv run --project engine pytest engine/tests/unit/broker_simulation/test_webhooks.py engine/tests/integration/test_broker_simulation_reconciliation.py -q`

- [x] **Step 3: Implement non-authoritative receipts and authoritative polling**

Webhook receipt stores event ID, body SHA-256, byte length, received time, auth status and safe code only; raw body stays in the injected immutable evidence store and never in logs/database JSON. It may append a candidate event but cannot unlock an account or finalize a fill. Polling archives normalized snapshot hashes, reconciles exact provider-order mappings, and unlocks only after orders, positions, cash and cash flows all match under one reconciliation run.

- [x] **Step 4: Run focused tests**

Run: `uv run --project engine pytest engine/tests/unit/broker_simulation/test_webhooks.py engine/tests/integration/test_broker_simulation_reconciliation.py -q`

- [x] **Step 5: Commit**

```bash
git add engine/src/nexus/broker_simulation/webhooks.py engine/src/nexus/broker_simulation/reconciliation.py engine/tests/unit/broker_simulation/test_webhooks.py engine/tests/integration/test_broker_simulation_reconciliation.py
git commit -m "feat(broker): reconcile simulation orders authoritatively"
```

---

### Task 7: Wire the broker-only worker and default-disabled capability

**Files:**
- Create: `engine/src/nexus/broker_simulation/jobs.py`
- Modify: `engine/src/nexus/broker_worker.py`
- Modify: `engine/src/nexus/providers/broker/settings.py`
- Modify: `engine/src/nexus/jobs/models.py`
- Create: `engine/tests/unit/broker_simulation/test_jobs.py`
- Create: `engine/tests/integration/test_broker_simulation_worker.py`

**Interfaces:**
- Produces broker-queue handlers for `broker.simulation.dispatch` and `broker.simulation.reconcile`; updates `JobType` and `JOB_PRIORITIES` together.
- Consumes the injected fixture gateway for tests and no Agent/research registry.

- [ ] **Step 1: Write failing queue, composition, capability-expiry, and lease tests**

Tests assert the broker registry has exactly the two Phase 3A handlers plus existing read-only sync/reconcile handlers, all use `QueueName.BROKER`, task payloads are strict and location-free, app/general workers cannot claim them, and `OFFLINE_FIXTURE` never constructs the HTTP Gateway client.

- [ ] **Step 2: Verify RED**

Run: `uv run --project engine pytest engine/tests/unit/broker_simulation/test_jobs.py engine/tests/integration/test_broker_simulation_worker.py -q`

- [ ] **Step 3: Implement one-shot and polling broker worker composition**

`BrokerWorkerSettings` adds only non-secret `NEXUS_BROKER_SIMULATION_MODE=offline_fixture|external` and requires an injected fixture gateway in offline mode. `external` requires a current admitted capability and the existing isolated gateway identity; Phase 3A repository defaults contain neither. The loop revalidates capability before claim, before Gateway call, after wait and before commit.

- [ ] **Step 4: Run broker and existing read-only worker regression**

Run: `uv run --project engine pytest engine/tests/unit/broker_simulation/test_jobs.py engine/tests/integration/test_broker_simulation_worker.py engine/tests/integration/test_broker_sync_admission.py engine/tests/integration/test_job_worker.py -q`

- [ ] **Step 5: Commit**

```bash
git add engine/src/nexus/broker_simulation/jobs.py engine/src/nexus/broker_worker.py engine/src/nexus/providers/broker/settings.py engine/src/nexus/jobs/models.py engine/tests/unit/broker_simulation/test_jobs.py engine/tests/integration/test_broker_simulation_worker.py
git commit -m "feat(broker): run isolated simulation job handlers"
```

---

### Task 8: Expose strict Engine API and generated contracts

**Files:**
- Create: `engine/src/nexus/api/schemas/broker_simulation.py`
- Create: `engine/src/nexus/api/routes/broker_simulation.py`
- Modify: `engine/src/nexus/main.py`
- Create: `engine/tests/integration/test_broker_simulation_api.py`
- Modify by generator: `contracts/openapi.json`
- Modify by generator: `app/lib/generated/schema.d.ts`
- Modify: `scripts/generate-client.mjs`
- Modify by generator: `app/lib/generated/runtime-contract.ts`

**Interfaces:**
- Produces the seven internal routes in design section 16 and closed `BrokerSimulationResponse`, `BrokerIntentResponse`, `BrokerActionResponse` roots.
- Consumes verified service actor, raw-body hash, Idempotency-Key, If-Match, expected version/event sequence, and 64 KiB body middleware.

- [ ] **Step 1: Write failing API contract and authority-boundary tests**

Tests assert auth on every route, `GET` returns `offline_ready/external_activation_blocked`, external action attempts return `409 SIMULATION_WRITE_DISABLED`, prepare/reject remain local-authority operations, approve cannot dispatch when external activation is blocked, body/query extras return 422, replay returns the same resource, stale ETag/version returns 409, and errors never include account/Provider IDs.

- [ ] **Step 2: Verify RED**

Run: `uv run --project engine pytest engine/tests/integration/test_broker_simulation_api.py -q`

- [ ] **Step 3: Implement routes and register the router**

The GET response contains only internal IDs and redacted account label. The server derives actor, request hash, all order facts and account binding. `approve`, `modify`, `cancel`, and reconciliation acknowledgement use their exact database service; external dispatch admission is independently checked and returns `SIMULATION_WRITE_DISABLED` in ordinary Phase 3A.

- [ ] **Step 4: Regenerate and verify contracts**

Run: `npm run generate:client`

Run: `npm run check:generated && uv run --project engine pytest engine/tests/integration/test_broker_simulation_api.py -q`

Expected: generator adds the three new root schemas; generated files have no manual edits.

- [ ] **Step 5: Commit**

```bash
git add engine/src/nexus/api engine/src/nexus/main.py engine/tests/integration/test_broker_simulation_api.py contracts/openapi.json app/lib/generated scripts/generate-client.mjs
git commit -m "feat(api): expose offline broker simulation workbench"
```

---

### Task 9: Build the offline review Web workbench and strict BFF

**Files:**
- Create: `app/lib/broker-simulation/bff.ts`
- Create: `app/api/v1/broker-simulation/route.ts`
- Create route handlers matching the six fixed action paths under `app/api/v1/broker-simulation/`.
- Modify: `app/(protected)/broker-simulation/page.tsx`
- Create: `app/components/broker-simulation/capability-banner.tsx`
- Create: `app/components/broker-simulation/intent-summary.tsx`
- Create: `app/components/broker-simulation/order-batch.tsx`
- Create: `app/components/broker-simulation/order-timeline.tsx`
- Create: `app/components/broker-simulation/reconciliation-panel.tsx`
- Create: `tests/unit/broker-simulation/bff.test.ts`
- Create: `tests/unit/broker-simulation/page.test.tsx`
- Create: `tests/e2e/broker-simulation.spec.ts`

**Interfaces:**
- Produces a server-rendered `offline_ready` workbench and BFF validators for UUID path segments, empty/fixed bodies, If-Match and Idempotency-Key.
- Consumes generated runtime contract and existing `engineFetch`/`withNexusBff` HMAC boundary.

- [ ] **Step 1: Write failing rendering, BFF, and accessibility tests**

Tests cover loading, empty, ready, partial, stale, error, forbidden, reconciling and unknown states; 200% reflow; keyboard/focus order; `aria-live`; and Axe critical/serious zero. Boundary tests send every forbidden order/account/Provider field and prove rejection before `engineFetch`.

- [ ] **Step 2: Verify RED**

Run: `npm test -- --run tests/unit/broker-simulation/bff.test.ts tests/unit/broker-simulation/page.test.tsx`

- [ ] **Step 3: Implement the page information hierarchy**

Render capability/account summary, proposal-vs-simulation difference, risk/evidence, server order preview, approval state, event timeline, internal-vs-broker comparison, reconciliation and audit. The banner always shows `实盘关闭`. Under `external_activation_blocked`, dispatch/modify/cancel buttons are not rendered as enabled controls; the page explains the external gate and still exposes inspectable fixture evidence.

- [ ] **Step 4: Run Web gates**

Run: `npm test -- --run tests/unit/broker-simulation`

Run: `npm run typecheck && npm run lint && npm run build`

Run: `npm run test:e2e -- tests/e2e/broker-simulation.spec.ts tests/e2e/accessibility-gates.spec.ts`

- [ ] **Step 5: Commit**

```bash
git add app/'(protected)'/broker-simulation app/api/v1/broker-simulation app/components/broker-simulation app/lib/broker-simulation tests/unit/broker-simulation tests/e2e/broker-simulation.spec.ts
git commit -m "feat(web): add offline broker simulation workbench"
```

---

### Task 10: Prove Phase 3A acceptance and preserve the Phase 3B gate

**Files:**
- Modify: `infra/compose.yaml`
- Modify: `infra/env.example`
- Create: `docs/runbooks/broker-simulation-activation.md`
- Create: `docs/superpowers/reviews/2026-08-28-nexus-phase-3a-review.md`
- Modify: `docs/superpowers/plans/2026-08-28-nexus-phase-3a-broker-simulation.md`
- Create: `engine/tests/unit/ops/test_phase3_compose_policy.py`

**Interfaces:**
- Produces a statically valid, non-default `broker-simulation` profile whose external adapter is disabled without a future admission package; produces the Phase 3B checklist, not an activation.
- Consumes Tasks 1–9 and the design's external gates.

- [ ] **Step 1: Write failing Compose and secret-surface tests**

Tests prove the app profile has no simulation service/credential mapping, read-only Gateway has no simulation route, simulation profile uses distinct service name/HMAC/mTLS variable names, `SIMULATION_WRITE` defaults false/absent, `LIVE_WRITE` is absent, and no tracked file contains Provider credential-shaped fixture values.

- [ ] **Step 2: Add the static profile and activation runbook**

The profile may define `fosun-simulation-gateway` and `broker-worker` wiring, but no ordinary local command starts it. The runbook lists exact Phase 1 five-session evidence, frozen SDK/contract/test-vector/authentication requirements, bounded first-batch approval contents, stop conditions, rollback, and `external_activation_blocked` as the current state. It contains no secret value and performs no real request.

- [ ] **Step 3: Run offline Engine/Gateway/PostgreSQL suites**

```bash
NEXUS_OFFLINE_SOCKET_GUARD=1 PYTHONPATH="$PWD/scripts/offline_guard" \
  uv run --project engine pytest \
  engine/tests/unit/broker_simulation \
  engine/tests/property/test_broker_simulation_state_machines.py \
  engine/tests/unit/providers/test_simulation_gateway_client.py \
  engine/tests/integration/test_broker_simulation_schema.py \
  engine/tests/integration/test_broker_simulation_constraints.py \
  engine/tests/integration/test_broker_simulation_intents.py \
  engine/tests/integration/test_broker_simulation_dispatch.py \
  engine/tests/integration/test_broker_simulation_reconciliation.py \
  engine/tests/integration/test_broker_simulation_worker.py \
  engine/tests/integration/test_broker_simulation_api.py -q
```

Run: `NEXUS_OFFLINE_SOCKET_GUARD=1 PYTHONPATH="$PWD/scripts/offline_guard" uv run --package nexus-fosun-gateway pytest integrations/fosun_gateway/tests -q`

Expected: all pass, socket attempts 0, external HTTP client constructions 0.

- [ ] **Step 4: Run full repository and UI acceptance**

```bash
uv run --project engine ruff check engine integrations/fosun_gateway
uv run --project engine mypy engine/src integrations/fosun_gateway/src
uv run --project engine pytest --import-mode=importlib \
  engine/tests/unit engine/tests/property engine/tests/evals engine/tests/contract engine/tests/test_health.py -q
npm run check:generated
npm test
npm run typecheck
npm run lint
npm run build
npm run test:e2e
docker compose --env-file infra/env.example -f infra/compose.yaml --profile app config --quiet
docker compose --env-file infra/env.example -f infra/compose.yaml --profile broker-simulation config --quiet
git diff --check
```

Expected: all exit 0; one Alembic head; `nexus_test_%` residue 0; task Compose residue 0; secret scan 0; Agent-to-broker imports 0; Critical/Important findings 0.

- [ ] **Step 5: Record independent review and final commit**

The review records exact test counts/durations, migration round trip, concurrency/failure injection, socket guard, route sets, secret scan, Web accessibility, unresolved Minor findings and rollback. It explicitly states `Phase 3A offline_ready` and `Phase 3B external_activation_blocked`.

```bash
git add infra/compose.yaml infra/env.example docs/runbooks/broker-simulation-activation.md docs/superpowers/reviews/2026-08-28-nexus-phase-3a-review.md docs/superpowers/plans/2026-08-28-nexus-phase-3a-broker-simulation.md engine/tests/unit/ops/test_phase3_compose_policy.py
git commit -m "docs: record phase 3a broker simulation acceptance"
```

## Phase 3B Stop Condition

After Task 10, stop all Provider work. Do not run Phase 1 real probes, start a simulation Gateway against Fosun, load/rotate credentials, register a Webhook, or submit/cancel/modify any external order. Those actions require the external evidence listed in the runbook plus a new explicit user approval naming the bounded operation window. Phase 3A completion is valid and reportable without Phase 3B.

## Plan Self-Review

- **Design coverage:** Tasks 1–2 cover immutable authority and persistence; Tasks 3 and 5 cover the isolated Gateway boundary; Tasks 4–7 cover approval, hard risk, attempts, unknown outcomes, reconciliation and broker-only jobs; Tasks 8–9 cover API/BFF/Web; Task 10 covers security, migration, accessibility and external-gate acceptance.
- **Authority separation:** Optimization, internal paper, broker simulation and external Provider facts remain distinct. Agent and browser inputs cannot become order facts or capabilities.
- **No false activation:** Phase 3A exercises the complete loop only through an injected no-socket fixture. Repository defaults and the shipped page remain `offline_ready / external_activation_blocked`; Phase 3B is an explicit stop boundary.
- **Failure safety:** Serial dispatch, one persisted attempt, no resend after uncertainty, account locking, authenticated-or-hint-only Webhooks and full polling reconciliation are testable invariants rather than UI convention.
- **Execution readiness:** All tasks have concrete files, commands, test seams, commit boundaries and acceptance outputs; external unknowns are admission blockers rather than implementation placeholders.
