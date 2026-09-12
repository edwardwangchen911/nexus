# Portfolio Scenario Research Shadow Design

Status: reviewed 97/100; awaiting user approval for implementation planning
Decision date: 2026-09-12

## 1. Outcome

Add one research-only forward observation for the current deterministic portfolio scenario. It
compares the selected target weights with `hold_current` over the next five complete XNYS
sessions, using one frozen scenario and the same point-in-time market evidence for both legs.

The result is a gross NAV comparison, not an order simulation. It never creates paper orders,
paper fills, broker requests, execution authority, or a strategy-promotion decision. The root and
every response keep `execution_locked=true`, `execution_eligible=false`, and
`execution_authorized=false`.

## 2. Scope boundary

This slice includes:

- starting one research Shadow from the current actor's exact
  `accepted_for_analysis` scenario selection;
- freezing the verified scenario result, initial portfolio facts, calendar, and five sessions;
- manually advancing only the next due session from already-persisted market evidence;
- calculating deterministic gross NAV for `hold_current` and `selected_candidate`;
- showing progress, evidence freshness, missing-data reasons, and the original cost-unknown state;
- durable recovery, idempotency, audit, API/BFF validation, and focused UI acceptance.

This slice excludes:

- automatic scheduling or hidden market collection;
- historical backtesting or adjusted-price policy changes;
- daily rebalancing, order sizing, simulated fills, tax lots, or cash settlement;
- changes to Proposal Shadow, Strategy Evolution Shadow, internal paper, risk, or broker tables;
- strategy ranking, promotion, profitability claims, or broker paper/live trading.

The five-session window proves operational continuity only. It is not evidence of alpha or a
reason to unlock execution.

## 3. Chosen architecture

Create a small `nexus.scenario_shadow` aggregate and reuse existing platform primitives:

- scenario claim replay and canonical scenario hashing;
- frozen portfolio snapshot and identity checks;
- XNYS calendar authority;
- persisted point-in-time price and corporate-action evidence;
- one new immutable, purpose-specific research-mark source policy;
- idempotency claims, audit events, outbox events, decimal serialization, and BFF validation.

Do not reuse Proposal Shadow as storage. Its database constraints require exactly three proposal
variants, 63 evaluation sessions, five grace sessions, executable proposal authority, and a formal
proposal group. Do not reuse Strategy Evolution Shadow as storage. It requires a passed historical
experiment, champion/challenger definitions, and a sealed internal-paper baseline. Creating fake
records for either subsystem would weaken their authority contracts.

Do not generalize either old subsystem in this slice. A two-table research aggregate is smaller
than changing their roots, foreign keys, query views, API schemas, and fixed UI semantics.

## 4. Frozen research contract

Each Shadow has exactly two legs:

- `hold_current`: `ScenarioPositionV1.current_weight` plus `cash_current_weight`;
- `selected_candidate`: `ScenarioPositionV1.target_weight` plus `cash_target_weight`.

Both weight vectors must sum exactly to `1`. The candidate remains
`current_holdings_only`, `allow_new_positions=false`, and reduce-only because those properties are
revalidated from `PortfolioScenarioEvaluationV2`; no Shadow request accepts positions or weights.

Both legs start at gross NAV `100`. The candidate is notionally reweighted once at the scenario's
`valuation_time` and then held. There is no daily rebalance. This answers the bounded question:
"What would the selected reduction have done from that frozen decision point?"

For leg `L` and session `t`:

```text
gross_nav_L(t) = 100 * (
  frozen_cash_weight_L
  + sum(frozen_security_weight_L[i] * total_return_factor[i, t])
)
```

`total_return_factor` is computed from the frozen anchor price and the session's qualified close,
including only qualified split and cash-distribution evidence available by the session cutoff. A
missing anchor, close, currency match, security identity, or required corporate-action fact rejects
the whole session. It never becomes zero return, zero price, or cash.

The calculation stays in the scenario's existing `currency:USD` scope. Cross-currency conversion
is not introduced.

The leg state is initialized directly from the frozen scenario weights and snapshot anchor prices:
`quantity = 100 * security_weight / anchor_price`, with remaining weight held as cash. This is a
research normalization, not a simulated fill. Later sessions reuse `apply_session_events` and
`mark_to_market`; they never call `execute_target_weights`.

## 5. Purpose-specific market authority

Existing intraday policies are `display_only_intraday_monitor`; the diagnostic adjusted-bar policy
is `portfolio_diagnostics_adjusted_daily_bars`; the legacy Proposal Shadow selector explicitly
rejects new roots. None of those labels is silently reused for research performance.

Migration `0056` publishes one immutable source policy with key
`portfolio_scenario_research_shadow`, version `scenario_shadow_price_policy_v1`, and purpose
`portfolio_scenario_research_shadow_gross_nav`. It selects the existing persisted Fosun US raw
daily-bar contract with fallback disabled and requires a complete corporate-action authority over
the anchor-to-session interval. Fixture evidence is admitted only in isolated tests.

The root freezes the policy ID, version, and canonical payload hash. Every advance verifies the
same policy and rejects a changed, missing, future-published, or wrong-purpose record. This policy
authorizes research marking only; it does not authorize display claims, scenario cost estimates,
paper orders, paper fills, Proposal Shadow, or execution.

## 6. Persistence

Migration `0056_portfolio_scenario_research_shadow.py` adds two tables.

### 6.1 `portfolio_scenario_shadows`

One immutable root per accepted selection version:

| Column | Contract |
| --- | --- |
| `id` | UUIDv7 primary key |
| `actor_id` | verified local actor identity |
| `selection_id` | FK to `portfolio_scenario_selections.id` |
| `selection_version` | integer, at least 1 |
| `scenario_claim_id` | FK to the exact scenario idempotency claim |
| `scenario_result_hash` | lowercase SHA-256 |
| `portfolio_snapshot_id` | FK to the frozen snapshot |
| `diagnostic_report_id` | FK to the bound diagnostic report |
| `scenario_document` | complete validated `PortfolioScenarioEvaluationV2` JSON |
| `scenario_document_hash` | canonical hash, equal to `scenario_result_hash` |
| `anchor_document` | validated USD anchor prices, weights, and source facts |
| `anchor_document_hash` | lowercase SHA-256 of the canonical anchor document |
| `source_policy_id` | FK to the research-mark policy |
| `source_policy_version` | exactly `scenario_shadow_price_policy_v1` |
| `source_policy_hash` | canonical policy payload hash |
| `calendar_authority_id` | FK to the exact calendar authority |
| `calendar_version` | frozen calendar version |
| `calendar_hash` | frozen calendar hash |
| `started_at` | database transaction time in UTC |
| `evaluation_sessions` | exactly five strictly increasing XNYS dates |
| `initial_nav` | exactly `100.000000000000` |
| `schema_version` | exactly `portfolio_scenario_shadow_v1` |
| `execution_locked` | exactly `true` |

Constraints include unique `(selection_id, selection_version)`, exact five-session cardinality,
canonical hashes, exact initial NAV, and immutable execution lock. The first evaluation session is
the first XNYS session whose regular open occurs after `started_at`; this avoids using a partial
session or data that predates the user's selection.

The root freezes the complete scenario document because a later advance must not depend on mutable
browser state or reinterpret a changed scenario claim. `scenario_claim_id` remains the provenance
link; the stored document is checked against both the claim and canonical hash at start.

### 6.2 `portfolio_scenario_shadow_attempts`

One immutable fact per advance attempt:

| Column | Contract |
| --- | --- |
| `id` | UUIDv7 primary key |
| `shadow_id` | FK to the research Shadow root |
| `claim_id` | FK to the advance idempotency claim |
| `session` | attempted XNYS session |
| `ordinal` | 1 through 5 |
| `attempt_number` | positive and monotonic within the session |
| `status` | `applied` or `rejected` |
| `reason_code` | null only for `applied` |
| `observed_at` | explicit UTC evaluation cutoff |
| `input_document` | selected source facts and explicit gaps; complete evidence for `applied` |
| `input_hash` | lowercase SHA-256 |
| `result_document` | two leg states and metrics for `applied`; empty for rejected |
| `result_hash` | canonical result hash for `applied`; null for rejected |
| `schema_version` | exactly `portfolio_scenario_shadow_attempt_v1` |

A unique constraint covers `(shadow_id, session, attempt_number)`. A partial unique index covers
`(shadow_id, session) WHERE status = 'applied'`. Rejected attempts remain auditable and do not
advance progress. A later retry uses a new idempotency key after evidence changes.

No mutable root status is stored. The query view derives:

- `not_started`: no root;
- `running`: root with fewer than five applied sessions;
- `complete`: root with five applied sessions;
- `next_expected_session`: evaluation session after the latest contiguous applied ordinal.

Only contiguous ordinals count. A database transaction and row/advisory lock prevent two writers
from applying the same session concurrently.

## 7. Start service

Add the following `start_scenario_research_shadow` interface:

```python
async def start_scenario_research_shadow(
    session: AsyncSession,
    store: MarketObjectStore,
    *,
    actor_id: str,
    scenario_result_hash: str,
    expected_selection_version: int,
    idempotency_key: str,
    request_hash: str,
) -> ScenarioShadowStartResult:
```

In one transaction it:

1. claims route `portfolio-scenario-research-shadow:{scenario_result_hash}`;
2. locks the actor's exact scenario selection;
3. requires `choice == accepted_for_analysis` and matching selection version;
4. replays the persisted scenario claim through the existing verification path;
5. requires scenario, selection, snapshot, report, actor, and result hashes to match;
6. reconstructs and validates both weight vectors and USD anchor facts;
7. loads one published XNYS authority and freezes the next five full sessions;
8. creates the immutable root, audit event, and outbox event;
9. stores the complete response in the idempotency claim.

The service does not call `has_executable_shadow_authority` and does not change the selection. Its
response always returns the three negative execution flags. An existing root for the same
selection/version is returned only when its hashes and identities match; otherwise startup fails
closed.

## 8. Advance service and evidence

Add the following `advance_scenario_research_shadow` interface:

```python
async def advance_scenario_research_shadow(
    session: AsyncSession,
    *,
    actor_id: str,
    scenario_result_hash: str,
    expected_session: date,
    observed_at: datetime,
    idempotency_key: str,
    request_hash: str,
) -> ScenarioShadowAdvanceResult:
```

The API supplies `observed_at` from trusted server/database time, not the browser body. The service
accepts only the derived `next_expected_session`, requires that session's regular close to be at or
before `observed_at`, and reads only already-persisted evidence available by `observed_at`.

Evidence authority produces one `ScenarioShadowSessionInputV1` containing:

- root and session identity;
- one qualified USD price anchor and session close per non-zero-weight instrument;
- source, feed, adjustment, revision, observed/available/received times, and evidence row IDs;
- all qualified corporate actions between the anchor and session;
- a complete symbol/instrument identity match;
- an input content hash.

Both legs use the same evidence bundle. The loader rejects source conflicts, future availability,
wrong sessions/currencies, missing instruments, duplicate conflicting revisions, and unqualified
corporate actions. It does not collect from Schwab, Fosun, Alpaca, or another provider.

For an applied session, the service writes the attempt, idempotency response, audit event, and
outbox event atomically. For a deterministic missing/incomplete-evidence rejection, it returns a
normal result envelope with `status=rejected` and writes the rejected attempt and fixed reason in
the same transaction. It does not raise an HTTP error that would roll back the rejection fact. An
uncertain transaction outcome is reported as unknown; callers first read persisted state with the
same identity and key rather than using a new key.

## 9. Metrics

The response and UI expose only:

- gross NAV for each leg by applied session;
- gross cumulative return for each leg;
- gross return delta `selected_candidate - hold_current`;
- maximum drawdown to date for each leg;
- applied, due, and total session counts;
- next expected and missing/rejected session details;
- source summaries and evidence freshness;
- the scenario's original cost component statuses and reasons.

Do not expose net return, annualized return, annualized volatility, Sharpe ratio, win rate,
promotion status, or an execution recommendation. Unknown costs are copied as unknown and never
subtracted as zero.

## 10. API and BFF

Engine routes:

```text
POST /internal/v1/portfolio-scenarios/{scenario_result_hash}/research-shadow
GET  /internal/v1/portfolio-scenarios/{scenario_result_hash}/research-shadow
POST /internal/v1/portfolio-scenarios/{scenario_result_hash}/research-shadow/advance
```

Start body:

```json
{
  "schema_version": "portfolio_scenario_shadow_start_v1",
  "selection_version": 1
}
```

Advance body:

```json
{
  "schema_version": "portfolio_scenario_shadow_advance_v1",
  "expected_session": "2026-09-14"
}
```

POST routes require existing verified actor, `Idempotency-Key`, bounded exact JSON, and normal
request/body hash validation. GET is read-only. Matching BFF routes proxy the same contracts.
Runtime validators require exact schema versions, exact scenario hash, both fixed leg names, exact
execution flags, canonical decimal strings, chronological sessions, and consistent counts.

Stable errors are:

| Reason | HTTP/code behavior |
| --- | --- |
| no accepted selection | 409 `SCENARIO_SHADOW_SELECTION_REQUIRED` |
| selection version changed | 409 `SCENARIO_SHADOW_SELECTION_STALE` |
| frozen identity/hash conflict | 409 `SCENARIO_SHADOW_AUTHORITY_INVALID` |
| requested session is not next | 409 `SCENARIO_SHADOW_SESSION_OUT_OF_ORDER` |
| close has not occurred | 409 `SCENARIO_SHADOW_SESSION_NOT_DUE` |
| missing/invalid price | 200 rejected result with `SCENARIO_SHADOW_PRICE_INCOMPLETE` |
| missing/invalid action evidence | 200 rejected result with `SCENARIO_SHADOW_CORPORATE_ACTION_INCOMPLETE` |
| five sessions already applied | 409 `SCENARIO_SHADOW_ALREADY_COMPLETE` |
| same key with different request | 409 existing `IDEMPOTENCY_CONFLICT` |

Error messages remain fixed and do not expose provider payloads or account data.

## 11. Web behavior

Add `app/components/portfolio-scenario-shadow.tsx` and mount it under the existing scenario result
and selection controls.

- Before acceptance, show no start control.
- With current `accepted_for_analysis`, show `开始研究观察`.
- After start, show two gross-NAV curves, progress `n / 5`, next session, evidence time, cost status,
  and the fixed notice `研究模拟，不生成订单，执行保持锁定`.
- Enable `推进下一交易日` only when the server view says a session is due. A click is an explicit
  local state change and reads persisted evidence only.
- A rejected advance keeps prior applied results visible and displays the stable reason.
- Page load and refresh issue GET requests only. They never advance, collect data, or call a model.

Do not reuse `ShadowPerformance` because it labels net metrics and assumes three proposal variants.
Reuse decimal/time formatters, chart tokens, and accessible table/legend patterns where their
semantics match.

## 12. File boundary

Create:

```text
engine/alembic/versions/0056_portfolio_scenario_research_shadow.py
engine/src/nexus/persistence/models/scenario_shadow.py
engine/src/nexus/scenario_shadow/__init__.py
engine/src/nexus/scenario_shadow/contracts.py
engine/src/nexus/scenario_shadow/authority.py
engine/src/nexus/scenario_shadow/service.py
engine/src/nexus/scenario_shadow/query.py
app/api/v1/portfolio-scenarios/[scenarioResultHash]/research-shadow/route.ts
app/api/v1/portfolio-scenarios/[scenarioResultHash]/research-shadow/advance/route.ts
app/components/portfolio-scenario-shadow.tsx
```

Modify only the focused integration points:

```text
engine/src/nexus/api/routes/optimization.py
engine/src/nexus/api/schemas/optimization.py
engine/src/nexus/api/errors.py
engine/src/nexus/persistence/models/__init__.py
engine/src/nexus/agents/portfolio_scenario_selection.py
engine/src/nexus/quality/source_policy.py
app/components/portfolio-scenario.tsx
app/lib/engine/client.ts
app/lib/engine/errors.ts
app/lib/generated/schema.d.ts
```

The selection module exposes one verified scenario replay function instead of duplicating its
private authority logic. No unrelated refactor is included.

## 13. Verification and acceptance

Development uses TDD. Focused unit tests cover:

1. exact weight balance and fixed two-leg names;
2. deterministic gross NAV and drawdown using Decimal only;
3. cash preservation and no daily rebalance;
4. split and distribution total-return handling;
5. missing/late/conflicting evidence rejection;
6. contiguous session ordering and derived status;
7. schema and canonical hash validation.
8. initialization calls no target-weight execution or fill calculation.
9. the research policy cannot authorize paper, Proposal Shadow, or execution purposes.

Isolated PostgreSQL tests cover:

1. start from a real-shaped accepted scenario claim and selection;
2. rejection of watch, rejected, stale-version, wrong-actor, and changed-hash inputs;
3. exact start replay and conflicting-key rejection;
4. one applied attempt per session under concurrent calls;
5. rejected attempt followed by a successful new-evidence retry;
6. restart/query recovery from persisted roots and attempts;
7. audit/outbox identities and absence of paper/broker writes;
8. migration upgrade/downgrade and schema constraints.

API/BFF/UI tests cover exact payload validation, status/error mapping, GET-only reload, explicit
advance, two-curve labels, unknown-cost display, and the execution-lock notice. Regenerate the API
client from the engine schema rather than editing generated types by hand.

Run focused Pytest, isolated PostgreSQL tests, Ruff, Mypy, frontend unit tests, typecheck, lint, and
the migration round trip. A browser acceptance uses the current persisted scenario to start the
research Shadow and proves reload recovery with zero new paper or broker records.

The final market-session acceptance waits for one eligible session. It follows the exact persisted
`root -> advance attempt -> evidence -> gross NAV -> API/BFF -> page` chain. That external-time gate
does not block code completion or the browser start/recovery acceptance.

## 14. Rollback

Code rollback removes the new routes and UI entry point. The new tables are isolated and no old
consumer depends on them. If schema downgrade is explicitly requested, migration `0056` drops only
the two new tables and removes only the exact unreferenced research policy after normal dependency
checks. Persisted research facts are not silently deleted during ordinary application rollback.

## 15. Design review

The second-pass review score is `97/100`:

| Dimension | Score | Review result |
| --- | ---: | --- |
| Domain and metric correctness | 20/20 | two frozen weight legs, gross-only labels, no daily rebalance |
| Permission and trading isolation | 20/20 | no orders/fills/broker writes; fixed negative execution flags |
| Existing-system reuse | 18/20 | reuses primitives without weakening either old Shadow contract |
| Persistence, idempotency, and recovery | 20/20 | immutable roots/attempts, atomic outcomes, durable rejection facts |
| Direct implementation and testability | 19/20 | exact files, interfaces, errors, migrations, and acceptance cases |

The review found and fixed two material ambiguities: display-only/diagnostic price policies cannot
authorize research marking, so the design now publishes one narrow immutable policy; and a
persisted rejected attempt cannot be raised as a transactional HTTP error, so deterministic data
rejections now return a committed `status=rejected` envelope. The remaining three points reflect
the unavoidable migration/API surface and the pending live-session evidence, not an unresolved
design decision.
