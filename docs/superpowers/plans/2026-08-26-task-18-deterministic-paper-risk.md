# Task 18 Deterministic Risk and Paper Ledger Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a fail-closed USD 10,000 internal paper account that turns one immutable baseline_v1 forward decision into deterministic risk-approved, reduced, or rejected orders, with append-only order/reservation events and balanced double-entry accounting.

**Architecture:** Persist the forward strategy decision and its point-in-time authorities first, evaluate risk in a pure Decimal-only domain layer, then atomically append paper batch, order, risk, reservation, ledger, audit, and outbox facts. Broker snapshots, Shadow facts, Agent output, live quotes, and all Fosun write surfaces stay outside this domain.

**Tech Stack:** Python 3.12, Pydantic 2, SQLAlchemy 2 async, PostgreSQL 16, Alembic, pytest, pytest-asyncio, Hypothesis, Ruff, mypy, uv.

**Spec:** docs/superpowers/specs/2026-08-26-task-18-deterministic-paper-risk-design.md

## Global Constraints

- Work only in /Users/wangchen/Documents/ChatGPT/投资/.worktrees/nexus-phase-0-1 on branch codex/nexus-phase-0-1.
- Follow strict TDD: add the named failing test, run it and observe the intended failure, implement only enough production code to pass, rerun, then commit.
- Use Decimal throughout; reject binary float and non-finite values at every public domain boundary.
- No broker SDK, Fosun API, account credential, external network, deployment, permission, live quote, or real-order path may be added or called.
- Do not edit migrations 0001 through 0013. New schema belongs only to 0014_paper.py.
- All business facts are append-only. ORM validation is not sufficient; migration tests must prove database constraints and triggers.
- Keep Task 19-owned fills, position adjustments, position snapshots, and performance snapshots empty.
- Preserve Task 16 baseline and Task 17 Shadow semantic outputs.
- Use existing UUIDv7 for runtime facts and fixed UUIDv5 constants for migration seeds.
- Keep commits small and scoped. Run git diff --check before every commit.

## Task 1: Add canonical paper and risk domain models

**Files:**

- Create: engine/src/nexus/risk/__init__.py
- Create: engine/src/nexus/risk/models.py
- Create: engine/src/nexus/risk/canonical.py
- Create: engine/src/nexus/strategy/forward.py
- Create: engine/src/nexus/paper/__init__.py
- Create: engine/src/nexus/paper/models.py
- Create: engine/tests/unit/risk/test_models.py
- Create: engine/tests/unit/risk/test_canonical.py
- Create: engine/tests/unit/strategy/test_forward_models.py
- Create: engine/tests/unit/paper/test_models.py

### Step 1: Write failing model-boundary tests

Add focused tests for:

- Money, weight, price, quantity, and ratio fields reject float, NaN, infinity, over-precision, negative values where forbidden, duplicate instrument IDs, and non-canonical hashes.
- PaperPortfolioProjection requires USD, cash account, long-only, unique positions/orders, non-negative balances, positive NAV, peak NAV >= NAV, and a valid event-chain projection hash.
- RiskMarketSnapshot requires one price and complete sector exposure for every current or target instrument, matching decision_session/policy/quality authorities, and available_at <= decision_time.
- ForwardStrategyDecision and ForwardStrategyTarget live in nexus.strategy.forward, reject duplicate/overlapping targets and exits, and do not depend on risk or paper.
- OrderCandidate accepts only buy/sell, positive whole-share desired quantity, priority 0..2, and a strictly positive reference close.
- PaperOrder, OrderEventInput, Reservation, LedgerAccount, LedgerEntry, and LedgerTransaction are frozen, extra-forbid domain models.

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/unit/risk/test_models.py engine/tests/unit/strategy/test_forward_models.py engine/tests/unit/paper/test_models.py -q
~~~

Expected: FAIL because nexus.risk and nexus.paper do not exist.

### Step 2: Implement the minimum immutable models

Use Pydantic frozen models and shared exact-Decimal validators. Define explicit Literal unions for:

- RiskOutcome = approved | reduced | rejected
- OrderSide = buy | sell
- InternalOrderStatus including all master-spec statuses
- ReservationType = cash | position
- DrawdownState = unlocked | locked | recovery_eligible
- LedgerSide = debit | credit

Do not put SQLAlchemy, sessions, current time, random IDs, or policy constants in these models.

### Step 3: Write failing canonical-hash tests

Test all six v1 hash functions against fixed golden vectors, including:

- reordered mappings and arrays where order is non-semantic;
- UUID lowercase;
- UTC with six fractional digits and Z;
- Decimal with exactly twelve fractional digits;
- exclusion of row IDs and created_at;
- rejection of naive datetime, float, and unknown schema version.

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/unit/risk/test_canonical.py -q
~~~

Expected: FAIL because the v1 canonicalizers are absent.

### Step 4: Implement canonical.py and rerun

Expose named functions for:

- canonical_forward_strategy_decision_hash
- canonical_paper_projection_hash
- canonical_risk_batch_input_hash
- canonical_risk_evaluation_result_hash
- canonical_paper_ledger_transaction_hash
- canonical_paper_order_batch_hash

Each function must build its own versioned payload; do not expose a generic caller-controlled schema string.

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/unit/risk/test_models.py engine/tests/unit/risk/test_canonical.py engine/tests/unit/strategy/test_forward_models.py engine/tests/unit/paper/test_models.py -q
uv run --package nexus-engine ruff check engine/src/nexus/risk engine/src/nexus/strategy/forward.py engine/src/nexus/paper engine/tests/unit/risk engine/tests/unit/strategy/test_forward_models.py engine/tests/unit/paper
git diff --check
~~~

Commit:

~~~bash
git add engine/src/nexus/risk engine/src/nexus/strategy/forward.py engine/src/nexus/paper engine/tests/unit/risk engine/tests/unit/strategy/test_forward_models.py engine/tests/unit/paper
git commit -m "feat(engine): add paper risk domain models"
~~~

## Task 2: Implement risk_policy_v1 and drawdown state transitions

**Files:**

- Create: engine/src/nexus/risk/policy.py
- Create: engine/tests/unit/risk/test_policy.py
- Create: engine/tests/unit/risk/test_drawdown.py

### Step 1: Write failing policy tests

Cover exact v1 values:

- single name 0.15;
- sector 0.30;
- minimum cash 0.10;
- lock at drawdown >= 0.08;
- recovery eligibility strictly below 0.06;
- buy minimum USD 100;
- reserve markup 0.01;
- commission 0;
- US, USD, common_stock and ETF only.

Test content-hash recomputation, immutable version, no float, invalid threshold ordering, unsupported currency/market, and exactly one active record semantics at the adapter boundary.

### Step 2: Write failing drawdown transition tests

Cover:

- unlocked stays unlocked below 8%;
- unlocked -> locked at exactly 8%;
- locked remains locked at exactly 6%;
- locked -> recovery_eligible below 6%;
- recovery_eligible -> locked on rebound to 6%;
- recovery_eligible cannot unlock without user acknowledgment;
- acknowledgment rechecks the supplied current drawdown;
- locked states block buys but do not block a valid reduce-only candidate.

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/unit/risk/test_policy.py engine/tests/unit/risk/test_drawdown.py -q
~~~

Expected: FAIL on missing policy/state functions.

### Step 3: Implement and verify

Expose immutable RiskPolicy.v1(), verify_policy_content_hash(), transition_drawdown(), and drawdown_allows_candidate(). Keep user identity and persistence out of the pure function; Task 20 owns the acknowledgment API.

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/unit/risk/test_policy.py engine/tests/unit/risk/test_drawdown.py -q
uv run --package nexus-engine ruff check engine/src/nexus/risk engine/tests/unit/risk
git diff --check
~~~

Commit:

~~~bash
git add engine/src/nexus/risk/policy.py engine/tests/unit/risk/test_policy.py engine/tests/unit/risk/test_drawdown.py
git commit -m "feat(engine): add immutable paper risk policy"
~~~

## Task 3: Build the pure deterministic risk evaluator

**Files:**

- Create: engine/src/nexus/risk/evaluate.py
- Create: engine/tests/unit/risk/test_evaluate.py
- Create: engine/tests/property/test_risk_invariants.py

### Step 1: Write table-driven failing tests

Build fixtures entirely from domain models. Cover:

- target quantity floor(target_weight * NAV / close);
- exits target zero;
- no-change produces evidence and no order intent;
- stable sort by priority ascending, target weight descending, UUID ascending;
- input permutation produces the same ordered output and hashes;
- 15% single-name and 30% look-through sector caps;
- 10% worst-price cash floor;
- USD 100 minimum buy using reference close;
- whole shares;
- reduce/sell minimum-order exemption;
- insufficient available position rejects rather than shorting;
- pending buys consume worst cash and exposure;
- pending sells do not release cash or exposure;
- drawdown lock rejects buys and allows valid reductions;
- the first binding constraint yields reduced or rejected with all rule evidence retained.

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/unit/risk/test_evaluate.py -q
~~~

Expected: FAIL because evaluate_order_batch is absent.

### Step 2: Implement candidate derivation and evaluation

Keep two pure entry points:

~~~python
def derive_order_candidates(
    decision: ForwardStrategyDecision,
    portfolio: PaperPortfolioProjection,
    market: RiskMarketSnapshot,
) -> tuple[OrderCandidate, ...]: ...

def evaluate_order_batch(
    candidates: Sequence[OrderCandidate],
    portfolio: PaperPortfolioProjection,
    market: RiskMarketSnapshot,
    policy: RiskPolicy,
) -> tuple[RiskEvaluation, ...]: ...
~~~

Reject the whole call before candidate evaluation when authority completeness, policy, account, projection, session, whitelist, price, sector, suspension, or corporate-action proofs are invalid.

### Step 3: Add Hypothesis invariants

Generate bounded Decimal NAV, prices, target weights, sector weights, current holdings, cash, and pending orders. Assert:

- any approved/reduced result leaves available cash >= 0 and cash ratio >= 0.10;
- name and sector ratios never exceed limits;
- approved quantity is a non-negative integer not above desired quantity;
- sell quantity never exceeds unreserved position;
- input permutation preserves results and result hash;
- rejected candidates never reserve resources.

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/unit/risk/test_evaluate.py engine/tests/property/test_risk_invariants.py -q
uv run --package nexus-engine ruff check engine/src/nexus/risk/evaluate.py engine/tests/unit/risk/test_evaluate.py engine/tests/property/test_risk_invariants.py
git diff --check
~~~

Commit:

~~~bash
git add engine/src/nexus/risk/evaluate.py engine/tests/unit/risk/test_evaluate.py engine/tests/property/test_risk_invariants.py
git commit -m "feat(engine): add deterministic paper risk evaluation"
~~~

## Task 4: Implement append-only order, reservation, and ledger folds

**Files:**

- Create: engine/src/nexus/paper/orders.py
- Create: engine/src/nexus/paper/ledger.py
- Create: engine/tests/unit/paper/test_order_state.py
- Create: engine/tests/unit/paper/test_reservations.py
- Create: engine/tests/unit/paper/test_ledger.py
- Create: engine/tests/property/test_paper_ledger_invariants.py

### Step 1: Write failing order-state tests

Cover the complete generic transition map, then prove the internal Task 18 writer only permits:

- initial -> proposed;
- proposed -> risk_rejected;
- proposed -> risk_approved -> submitted.

Reject skipped risk, awaiting_approval for internal paper, Task 19 states from the Task 18 actor, non-contiguous sequence, mismatched from_status, duplicate event ID, terminal append, and payload hash mismatch.

### Step 2: Write failing reservation tests

Cover one reservation root per order, cash/position field exclusivity, reserved as first event, positive amount, continuous sequence, consumed/released bounded by outstanding amount, no negative remainder, and idempotent fold detection.

### Step 3: Write failing ledger tests

Cover:

- initial funding Dr available cash / Cr contributed capital;
- buy reserve Dr reserved cash / Cr available cash;
- debit equals credit and total is positive;
- one and only one side per entry;
- known account/currency;
- continuous entry sequence;
- duplicate IDs and transaction hashes rejected;
- input object is not mutated.

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/unit/paper/test_order_state.py engine/tests/unit/paper/test_reservations.py engine/tests/unit/paper/test_ledger.py -q
~~~

Expected: FAIL on missing folds.

### Step 4: Implement folds and property tests

Expose transition_order(), fold_reservation_events(), post_ledger_transaction(), balance_ledger(), and make_buy_reservation_transaction(). Property tests generate valid balanced entries then mutate one dimension to prove fail-closed behavior.

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/unit/paper engine/tests/property/test_paper_ledger_invariants.py -q
uv run --package nexus-engine ruff check engine/src/nexus/paper engine/tests/unit/paper engine/tests/property/test_paper_ledger_invariants.py
git diff --check
~~~

Commit:

~~~bash
git add engine/src/nexus/paper engine/tests/unit/paper engine/tests/property/test_paper_ledger_invariants.py
git commit -m "feat(engine): add append-only paper order and ledger folds"
~~~

## Task 5: Add the 0014 paper schema, seed, and database guards

**Files:**

- Create: engine/src/nexus/persistence/models/paper.py
- Modify: engine/src/nexus/persistence/models/__init__.py
- Modify: engine/alembic/env.py
- Create: engine/alembic/versions/0014_paper.py
- Modify: engine/tests/helpers/migration_schema_expectations.py
- Create: engine/tests/integration/test_paper_migration.py
- Create: engine/tests/integration/test_paper_db_guards.py

### Step 1: Write failing migration inventory and seed tests

Assert:

- Alembic head becomes 0014_paper with down_revision 0013_shadow;
- every table in design section 15 exists;
- paper_accounts joins exactly one Account and Portfolio seed;
- seed values are USD, cash, long_only, initial capital 10000;
- the seven ledger accounts exist once;
- funding has exactly two entries and recomputes balanced;
- the first drawdown event is unlocked with seq=1;
- Task 19 tables exist and contain zero rows;
- upgrade from 0013 and downgrade back to 0013 are reversible.

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/integration/test_paper_migration.py -q
~~~

Expected: FAIL because 0014 and the ORM mappings do not exist.

### Step 2: Add ORM mappings and migration tables

Use explicit named constraints and ForeignKeyConstraint bindings for root identity. Include:

- one-to-one account_id and portfolio_id on paper_accounts;
- composite parent bindings where a child repeats account_id/order_id/decision_id;
- NUMERIC(38,12), UTC timestamptz, canonical hash checks, currency checks, integer quantity checks;
- all unique constraints from the design;
- fixed UUIDv5 constants calculated outside the migration and written as literals;
- idempotent insert-on-conflict seed statements for Account, Portfolio, paper account, policy, ledger accounts, funding, and genesis drawdown event.

Do not infer paper kind from Account.provider in application code; the provider value used by the identity seed is only descriptive.

### Step 3: Write failing database-bypass tests

Using raw SQL in a temporary PostgreSQL database, prove failure for:

- UPDATE or DELETE on each paper fact/event/policy/decision table;
- order event seq gaps, duplicate seq, from_status mismatch, skipped risk, awaiting_approval, Task 19 status from Task 18 actor, and terminal append;
- reservation seq gaps, wrong instrument/currency binding, negative or excessive amount;
- ledger entry with both sides, neither side, negative amount, wrong currency, unknown account, duplicate seq;
- transaction with unequal totals or zero total at commit;
- duplicate strategy decision, batch, client_order_id, evaluation, and reservation;
- Task 18 集成服务和验收流程不会向 paper_fills 或其他 Task 19 表写行。

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/integration/test_paper_db_guards.py -q
~~~

Expected: FAIL until triggers and constraints are installed.

### Step 4: Add trigger functions and verify

Create narrowly scoped PostgreSQL functions:

- nexus_reject_paper_fact_mutation;
- nexus_paper_order_event_chain_valid;
- nexus_paper_reservation_event_chain_valid;
- nexus_paper_ledger_transaction_balanced;

Use deferred constraint triggers for checks that require the complete transaction. The chain trigger must derive prior state from events and cannot trust a mutable current_status column.
Do not add database roles, GRANT, REVOKE, or custom session variables. Task 18/19
write ownership is enforced by dedicated application services and acceptance
tests; database constraints independently enforce append-only, transition,
foreign-key, reservation, and accounting invariants.

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/integration/test_paper_migration.py engine/tests/integration/test_paper_db_guards.py -q
uv run --package nexus-engine alembic heads
uv run --package nexus-engine ruff check engine/src/nexus/persistence/models/paper.py engine/alembic/env.py engine/tests/integration/test_paper_migration.py engine/tests/integration/test_paper_db_guards.py
git diff --check
~~~

Commit:

~~~bash
git add engine/src/nexus/persistence/models engine/alembic engine/tests/helpers/migration_schema_expectations.py engine/tests/integration/test_paper_migration.py engine/tests/integration/test_paper_db_guards.py
git commit -m "feat(engine): add append-only paper schema"
~~~

## Task 6: Persist forward baseline decisions and exact risk authorities

**Files:**

- Modify: engine/src/nexus/strategy/forward.py
- Create: engine/src/nexus/risk/authority.py
- Create: engine/src/nexus/paper/projection.py
- Create: engine/tests/unit/strategy/test_forward_decision.py
- Create: engine/tests/unit/risk/test_authority.py
- Create: engine/tests/unit/paper/test_projection.py
- Create: engine/tests/integration/test_forward_decision_persistence.py
- Create: engine/tests/integration/test_risk_authority_pg.py

### Step 1: Write failing forward-decision tests

Start from the existing StrategyDecision returned by evaluate_baseline_v1. Cover:

- WEEKLY_REBALANCE maps targets and exits to immutable target rows;
- exit target weight is zero and priority is 0;
- existing target adjustment priority is 1 and new target priority is 2;
- NOT_WEEK_END cannot become a consumable decision;
- duplicate targets/exits, overlap errors, negative weights, total over one, non-whitelist instruments, missing execution_session, and mismatched sessions fail;
- canonical decision hash is independent of row IDs and insertion order.

### Step 2: Implement the pure forward adapter

Expose build_forward_strategy_decision(). It accepts the pure baseline result, current positions, frozen authority IDs/hashes, decision_time, and execution_session. It does not call the database or Agent and never accepts caller-supplied target weights.

### Step 3: Write failing persistence tests

Using a temporary database, persist one valid decision and assert:

- header and target rows reconstruct the exact domain decision;
- same account/version/session/input returns the existing decision;
- same key with different content conflicts;
- no update/delete is possible;
- NOT_WEEK_END creates no strategy_decision row;
- available_at later than decision_time is rejected.

### Step 4: Write failing authority/projection tests

Cover exact-set loading for:

- ExchangeCalendarAuthorityRecord;
- UniverseSnapshot and UniverseMember;
- PriceBarSetAuthority selected raw closes;
- SectorExposure sums;
- QualityReport coverage;
- no pending corporate action, suspension, or unresolved delisting;
- current ledger/order/reservation event scans.

Test missing, duplicate, extra, cross-session, cross-policy, stale, revised-after-decision, adjusted-price, and incomplete query-proof failures. Projection tests must detect chain gaps, illegal transitions, unbalanced ledger, negative resources, and hash mismatch.

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/unit/strategy/test_forward_decision.py engine/tests/unit/risk/test_authority.py engine/tests/unit/paper/test_projection.py engine/tests/integration/test_forward_decision_persistence.py engine/tests/integration/test_risk_authority_pg.py -q
~~~

Expected: FAIL until the adapter, authority loader, projection builder, and repository functions exist.

### Step 5: Implement persistence and authority loading

Keep database reads in authority.py/projection.py and return only validated domain objects. The authority loader must use one AsyncSession transaction snapshot and return stable failure codes; it must not query a fallback provider.

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/unit/strategy/test_forward_decision.py engine/tests/unit/risk/test_authority.py engine/tests/unit/paper/test_projection.py engine/tests/integration/test_forward_decision_persistence.py engine/tests/integration/test_risk_authority_pg.py -q
uv run --package nexus-engine pytest engine/tests/unit/strategy/test_baseline.py engine/tests/unit/backtest engine/tests/property/test_no_lookahead.py -q
uv run --package nexus-engine ruff check engine/src/nexus/strategy/forward.py engine/src/nexus/risk/authority.py engine/src/nexus/paper/projection.py engine/tests/unit/strategy/test_forward_decision.py engine/tests/unit/risk/test_authority.py engine/tests/unit/paper/test_projection.py engine/tests/integration/test_forward_decision_persistence.py engine/tests/integration/test_risk_authority_pg.py
git diff --check
~~~

Commit:

~~~bash
git add engine/src/nexus/strategy/forward.py engine/src/nexus/risk/authority.py engine/src/nexus/paper/projection.py engine/tests/unit/strategy/test_forward_decision.py engine/tests/unit/risk/test_authority.py engine/tests/unit/paper/test_projection.py engine/tests/integration/test_forward_decision_persistence.py engine/tests/integration/test_risk_authority_pg.py
git commit -m "feat(engine): persist point-in-time paper decisions"
~~~

## Task 7: Implement the atomic order-batch application service

**Files:**

- Create: engine/src/nexus/paper/service.py
- Create: engine/src/nexus/paper/errors.py
- Create: engine/tests/unit/paper/test_service.py
- Create: engine/tests/integration/test_paper_order_batch_pg.py
- Create: engine/tests/integration/test_paper_idempotency_pg.py
- Create: engine/tests/integration/test_paper_concurrency_pg.py
- Create: engine/tests/integration/test_paper_atomicity_pg.py

### Step 1: Write failing service orchestration tests

With fake repositories, assert exact call order:

1. account advisory transaction lock;
2. drawdown lock;
3. order roots sorted by UUID;
4. reservations sorted by UUID;
5. ledger accounts sorted by code;
6. idempotency;
7. decision/authority/projection reload;
8. pure risk evaluation;
9. persistence of one atomic fact set.

Assert request input contains only account_id, strategy_decision_id, idempotency_key, and trusted request_time. Candidate fields in a fake request are rejected rather than ignored.

### Step 2: Implement service shell and stable errors

Expose create_paper_order_batch() with dependency-injected AsyncSession and trusted clock adapter. Map only documented stable codes. Do not catch BaseException or turn constraint violations into approvals.

### Step 3: Write failing happy-path and fail-closed integration tests

The complete fixture must prove:

- all evaluated candidates have immutable rule evidence;
- approved/reduced candidates have proposed -> risk_approved -> submitted;
- rejected candidates have proposed -> risk_rejected;
- buys have cash reservation and balanced reserve transaction;
- sells have position reservation and no premature cash entry;
- audit and outbox hashes reference the same batch;
- raw-row hash recomputation matches all six canonical hashes.

For each missing authority/projection/policy component, assert zero orders, zero reservations, zero new ledger transactions, and a stable batch-level failure. Also assert Task 17 validated facts never call this service.

### Step 4: Write failing idempotency, concurrency, and fault tests

Cover:

- same key/same hash returns byte-equivalent result and unchanged row counts;
- same key/different hash returns IDEMPOTENCY_CONFLICT;
- different keys/same decision converge to the same batch;
- 8 concurrent sessions on one decision create one batch;
- different accounts can progress without deadlock;
- injected exceptions after every persistence phase roll back every Task 18 row;
- outbox publish failure after commit does not roll back business facts and dispatcher replay is idempotent.

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/unit/paper/test_service.py engine/tests/integration/test_paper_order_batch_pg.py engine/tests/integration/test_paper_idempotency_pg.py engine/tests/integration/test_paper_concurrency_pg.py engine/tests/integration/test_paper_atomicity_pg.py -q
~~~

Expected: FAIL before the complete transactional service.

### Step 5: Implement, rerun, and commit

Use PostgreSQL pg_advisory_xact_lock on a stable account-derived signed bigint. Sort all row locks. Catch the unique-race only to reload and compare canonical hashes; mismatched content remains a conflict.

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/unit/paper/test_service.py engine/tests/integration/test_paper_order_batch_pg.py engine/tests/integration/test_paper_idempotency_pg.py engine/tests/integration/test_paper_concurrency_pg.py engine/tests/integration/test_paper_atomicity_pg.py -q
uv run --package nexus-engine ruff check engine/src/nexus/paper/service.py engine/src/nexus/paper/errors.py engine/tests/unit/paper/test_service.py engine/tests/integration/test_paper_order_batch_pg.py engine/tests/integration/test_paper_idempotency_pg.py engine/tests/integration/test_paper_concurrency_pg.py engine/tests/integration/test_paper_atomicity_pg.py
git diff --check
~~~

Commit:

~~~bash
git add engine/src/nexus/paper/service.py engine/src/nexus/paper/errors.py engine/tests/unit/paper/test_service.py engine/tests/integration/test_paper_order_batch_pg.py engine/tests/integration/test_paper_idempotency_pg.py engine/tests/integration/test_paper_concurrency_pg.py engine/tests/integration/test_paper_atomicity_pg.py
git commit -m "feat(engine): create atomic internal paper order batches"
~~~

## Task 8: Expose the internal paper portfolio identity without mixing broker assets

**Files:**

- Modify: engine/src/nexus/portfolio/service.py
- Modify: engine/tests/unit/api/test_portfolio_contracts.py
- Modify: engine/tests/integration/test_portfolio_api.py

### Step 1: Write failing portfolio-list tests

Assert:

- kind=internal_paper returns the one seeded paper portfolio;
- kind=real_read_only still returns only broker portfolios;
- kind=shadow behavior is unchanged;
- an unfiltered list labels each row by its subtype relation;
- USD paper values and broker currencies stay separate and no combined total is emitted;
- the paper row reports source=internal_ledger, account model cash, long_only, and data state derived from ledger/projection validity;
- no paper order-create action or broker capability is advertised.

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/unit/api/test_portfolio_contracts.py engine/tests/integration/test_portfolio_api.py -q
~~~

Expected: FAIL because PortfolioService currently returns an empty internal_paper result.

### Step 2: Implement the subtype-aware read projection

Join paper_accounts instead of checking Account.provider. Keep broker snapshot projection unchanged. If paper projection validation fails, return partial/error metadata and no invented balance.

Run:

~~~bash
uv run --package nexus-engine pytest engine/tests/unit/api/test_portfolio_contracts.py engine/tests/integration/test_portfolio_api.py -q
uv run --package nexus-engine ruff check engine/src/nexus/portfolio/service.py engine/tests/unit/api/test_portfolio_contracts.py engine/tests/integration/test_portfolio_api.py
git diff --check
~~~

Commit:

~~~bash
git add engine/src/nexus/portfolio/service.py engine/tests/unit/api/test_portfolio_contracts.py engine/tests/integration/test_portfolio_api.py
git commit -m "feat(engine): list isolated internal paper portfolio"
~~~

## Task 9: Run full acceptance, separated review, and update the ledger

**Files:**

- Create only after implementation gates: docs/superpowers/reviews/2026-08-26-task-18-deterministic-paper-risk-review.md
- Modify only after acceptance: .superpowers/sdd/2026-08-22-nexus-phase-0-1/progress.md
- Modify only after acceptance: docs/superpowers/plans/2026-08-22-nexus-phase-0-1.md
- Modify during execution: this plan, checking completed steps and appending exact evidence

### Step 1: Freeze the review range and inspect repository state

Record:

~~~bash
git status --short
git diff --check
git log --oneline --decorate -15
git diff --stat d04bcca..HEAD
~~~

Do not accept from green focused tests alone. Acceptance requires committed code, migration round-trip, full regression, raw database residual checks, hash recomputation, and review evidence.

### Step 2: Verify the database runtime without installing Docker Desktop

Use the existing Docker-compatible runtime:

~~~bash
docker compose -f infra/compose.yaml up -d postgres minio
docker compose -f infra/compose.yaml ps
~~~

Docker Desktop is not required if Docker CLI, Compose, PostgreSQL, and MinIO are already healthy. Do not install or alter a runtime as part of Task 18.

### Step 3: Run migration round-trip and schema acceptance

Against a disposable test database, run:

~~~bash
uv run --package nexus-engine alembic -c engine/alembic.ini downgrade 0013_shadow
uv run --package nexus-engine alembic -c engine/alembic.ini upgrade head
uv run --package nexus-engine pytest engine/tests/integration/test_migration.py engine/tests/integration/test_paper_migration.py engine/tests/integration/test_paper_db_guards.py -q
~~~

The main developer database must not be destructively downgraded. Prefer the project temporary-database fixture or create a specifically named disposable database and remove it after verification.

### Step 4: Run complete Engine gates

Run:

~~~bash
uv run --package nexus-engine pytest --import-mode=importlib engine/tests -q
uv run --package nexus-engine ruff check engine
uv run --package nexus-engine mypy engine/src
~~~

Record exit status, exact test counts, elapsed time, and any skips. A rerun after a failure must record the failure cause and the final green command.

### Step 5: Run affected Web gates

The portfolio API contract is consumed by Web, so run:

~~~bash
npm test
npm run typecheck
npm run lint
npm run check:generated
npm run build
~~~

Run npm run test:e2e only if the internal paper row changes a rendered route or generated fixture. If no UI route changes in Task 18, record why E2E is deferred to Task 19 instead of claiming it ran.

### Step 6: Run one-shot database and hash verifier

In a rollback-only transaction:

1. seed one complete weekly baseline fixture;
2. persist the forward decision;
3. create one order batch;
4. recompute decision, projection, risk input/result, ledger transaction, and batch hashes from raw rows;
5. assert each digest exactly matches persistence;
6. assert submitted orders have continuous event chains and exact reservations;
7. assert every ledger transaction balances;
8. assert Task 19 tables stay zero rows;
9. assert broker/live/approval tables or routes receive no new facts/calls;
10. roll back and prove zero residual Task 18 business rows beyond immutable migration seeds.

Do not print account payloads, idempotency keys, environment variables, credentials, or provider data.

### Step 7: Perform a separated architecture/security review

Start a fresh review pass after implementation and tests, using only the committed diff and design spec as inputs. Review:

- look-ahead and available_at boundaries;
- exact authority set and source-switch prevention;
- Decimal precision, units, floor direction, 15/30/10 boundary semantics;
- pending buy/sell worst-case projections;
- drawdown lock and reduce-only behavior;
- duplicate decisions, idempotency races, advisory/row lock order and deadlocks;
- order/reservation event gaps, illegal transitions and terminal immutability;
- ledger balance, orphan facts, rollback atomicity and database bypass;
- Task 17/18/19 ownership and automatic-order boundary;
- Agent, broker, live, approval, credential and error-leakage boundaries;
- seed stability, downgrade safety, audit/outbox replay and hash recomputation.

Classify findings as Critical, Important, or Minor. Every Critical or Important finding gets a red regression test and fix before acceptance. Record that the separated pass is a single-executor review, not a second-person/model review.

### Step 8: Record acceptance and commit documentation

Create the review document with:

- commit range from d04bcca;
- exact commands, counts, exit codes, and elapsed times;
- migration round-trip and database residual evidence;
- canonical-hash recomputation evidence;
- finding counts and fix commits;
- explicit statement that Task 18 includes no simulated fills, paper performance/page, scheduler, broker simulation, live trading, deployment, credential, or full MVP completion.

Only after every gate passes:

- mark the original Task 18 checklist complete and add concise evidence;
- update progress.md with commits and verification;
- check completed steps in this plan;
- commit with docs: record Task 18 paper risk acceptance;
- confirm git status --short is clean.

## Spec Coverage Matrix

| Design requirement | Plan coverage |
| --- | --- |
| Isolated USD 10,000 cash/long-only account | Tasks 5, 8 |
| Immutable forward baseline decision | Tasks 1, 6 |
| Exact PIT prices, calendar, universe, sectors, quality and negative proof | Tasks 1, 6, 7 |
| 15% name, 30% sector, 10% cash, whole shares, USD 100 minimum | Tasks 2, 3 |
| Drawdown lock/recovery state | Tasks 2, 5, 6 |
| Deterministic ordering and approved/reduced/rejected | Task 3 |
| Append-only internal order state machine | Tasks 4, 5, 7 |
| Cash/position reservations | Tasks 4, 5, 7 |
| Balanced double-entry ledger | Tasks 4, 5, 7 |
| Canonical hashes and replay | Tasks 1, 6, 7, 9 |
| Idempotency, concurrency, atomic rollback | Task 7 |
| DB bypass and Task service ownership | Tasks 5, 7, 9 |
| Broker/Shadow/paper separation | Global constraints, Tasks 6–9 |
| Task 19 schema contract with zero Task 19 facts | Tasks 5, 7, 9 |
| Portfolio list without currency mixing | Task 8 |
| Full regression and review | Task 9 |

## Plan Self-Review Checklist

- [x] Every design-spec section maps to at least one explicit test or acceptance step.
- [x] Every implementation task names exact create/modify files, a red command, a green command, and a commit.
- [x] Public domain interfaces, state transitions, policy limits, sort order, hashes, seed identity, migration revision, and stable failure codes are fixed.
- [x] The plan does not ask the implementer to invent broker, live, approval, Agent, API, UI, fill, performance, scheduler, or self-evolution behavior.
- [x] Missing or inconsistent authority fails the whole batch before any order or reservation.
- [x] Approved/reduced candidates are still constrained under worst-case pending-order projection.
- [x] ORM checks are backed by migration constraints/triggers and direct SQL bypass tests.
- [x] Task 19 owns fills and performance; Task 18 only creates their guarded empty schema contract.
- [x] Task 17 validated cannot create a paper decision or order.
- [x] No unresolved TBD, placeholder command, pseudo-path, or approval checkpoint remains.
- [x] Completion evidence is distinct from the overall NEXUS goal and Tasks 19–21.
