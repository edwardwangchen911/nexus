# Fosun Unknown Instrument Admission Design

Status: reviewed and implementation-ready
Decision date: 2026-09-12
Supersedes: the unknown-security acceptance claim in the Fosun classification review

## 1. Problem and verified root cause

The production `nexus-fosun-local refresh` flow reads accounts and holdings, then
calls `_load_local_broker_identity()` before it reads the quote surface. An active US
holding without a current ticker alias therefore raises
`LOCAL_BROKER_INSTRUMENT_SOURCE_REQUIRED` before the quote classification fields can
be read or archived.

The existing classification feature is correct for known instruments, but its prior
review incorrectly claimed that an unknown security would not fail refresh. The real
run on 2026-09-12 disproved that claim: schema `0052_fosun_classification` was live,
yet the refresh stopped with that exact error and persisted no new market or portfolio
snapshot.

Fosun does return useful deterministic fields:

- holdings: `marketCode`, `stockCode`, `symbol`, `name`, `productType`, currency;
- quote: `mkt`, `rawSymbol`, name, currency, `first/second/third`, equity-profile
  field presence, net-asset-profile field presence, and `fundAssetType*`;
- observed common-stock rows route as `(1, 101, 10105)` plus complete equity profile;
- observed ETF and ETN rows both route as `(2, 202, 20202)` plus complete net-asset
  profile.

These facts distinguish `company_equity_like` from `fund_or_structured_like`. They do
not establish the exact listing exchange, and the observed ETF and ETN values do not
distinguish those two canonical asset types. Holdings `productType=6` is shared by
observed common stocks, ETFs, and ETNs. The fields are therefore admissible discovery
and routing evidence, but not canonical instrument identity authority.

## 2. Outcome

A newly observed US holding must not discard useful data for every already-known
holding. One normal full refresh must:

1. read one holdings page and one quote batch for the active US holdings;
2. archive the exact raw quote before any unknown identity is reported;
3. persist provider-scoped discovery evidence for each unresolved code;
4. update quote/classification/bar evidence only for already resolved instruments;
5. record that the broker refresh is partial and leave the previous complete portfolio
   snapshot as the current decision snapshot;
6. return `status="identity_pending"` with bounded, non-account output;
7. keep `DECISION_READY=false` for the attempted refresh and
   `execution_locked=true`.

On a later refresh, after an existing authoritative identity importer has created the
instrument and current ticker alias, the same holding follows the unchanged full
READY path. No historical discovery or snapshot is rewritten.

## 3. Non-goals and safety invariants

- Do not infer `Instrument.exchange` from `marketCode="us"`.
- Do not infer ETF versus ETN from `(2, 202, 20202)`, `productType=6`, a provider
  display name, or the presence of NAV fields.
- Do not create an `Instrument`, `InstrumentAlias`, portfolio position, order, or
  execution authority for an unresolved provider code.
- Do not call SEC, FMP, Alpaca, Schwab, a model, or another source automatically from
  portfolio page reads or this refresh.
- Do not continue broker cash/order/cash-flow reconciliation once the current holding
  set is known to be unresolved; a partial subset is not a truthful portfolio.
- Do not expose account identifiers, quantities, costs, raw payloads, or credentials
  in CLI/API pending output.
- Do not retry a provider call inside this change.

## 4. Selected architecture

Reuse the generic immutable evidence tables. No source-specific SQL table is added.

```text
accounts + holdings
  -> resolve current aliases into known and unresolved provider codes
  -> one quote batch for all active US provider codes
  -> archive exact raw quote DataArtifact
       -> known code: existing classification + quote path
       -> unresolved code: FosunInstrumentDiscoveryDocument + DataSnapshot
  -> bars only for known codes
  -> known market evidence commits
  -> if unresolved exists:
       persist PARTIAL BrokerSyncRun, no PortfolioSnapshot
       return identity_pending; stop before cash/orders/cash-flows
     else:
       existing broker reconciliation and READY snapshot
```

Discovery evidence is not a `ResearchEvidenceRef`: there is no canonical instrument
to own it, and an Agent run must not silently treat provider-code evidence as security
identity. The pending read model derives current pending items from immutable discovery
snapshots whose provider code has no valid current ticker alias.

## 5. Discovery document contract

Add `engine/src/nexus/market/fosun_instrument_discovery.py`:

```python
SOURCE = "fosun_instrument_discovery"
SCHEMA = "fosun_instrument_discovery_v1"

class FosunInstrumentDiscoveryDocument(BaseModel):
    schema_version: Literal["fosun_instrument_discovery_v1"]
    source: Literal["fosun_instrument_discovery"]
    provider_code: str
    provider_name: str | None
    market_scope: Literal["US"]
    currency: Literal["USD"]
    provider_product_type: int | None
    levels: FosunClassificationLevels
    equity_profile_fields_complete: bool
    net_asset_profile_fields_complete: bool
    fund_asset_type: FosunFundAssetType
    routing_family: FosunSecurityFamily
    recommended_identity_route: Literal[
        "current_held_common_stock",
        "current_held_fund_or_structured",
        "manual_review",
    ]
    holding_observed_at: AwareDatetime
    received_at: AwareDatetime
    available_at: AwareDatetime
    raw_quote: FosunRawQuoteBinding
    canonical_instrument_id: None
    canonical_asset_type: None
    identity_authoritative: Literal[False]
    decision_eligible: Literal[False]
    execution_locked: Literal[True]
```

Models are frozen, strict, `extra="forbid"`, hide inputs in validation errors, and
reuse the existing provider-code and UTC validators. `received_at == available_at`;
`holding_observed_at <= received_at`. The routing family must recompute exactly from
the stored classification fields.

The recommended route is deterministic:

- `company_equity_like` -> `current_held_common_stock`;
- `fund_or_structured_like` -> `current_held_fund_or_structured`;
- `unknown` -> `manual_review`.

It is a route name, not proof that the target canonical type has been determined.

`provider_object_id` is `discovery:{provider_code}`. The revision is the SHA-256 of
canonical UTF-8 JSON with sorted keys and compact separators. The normalized document
is saved as a content-addressed `DataArtifact`, then a `DataSnapshot` with matching
source/object/revision/clocks/schema/payload is inserted or exactly reused. The
document binds the existing raw quote artifact by URI, SHA-256, byte length, gateway
request ID, and request-body SHA-256.

Any same-key mismatch in payload, hashes, locations, clocks, or schemas fails closed
with a stable `FOSUN_DISCOVERY_*_CONFLICT` code. A content-addressed object left by a
rolled-back database transaction is safe and reusable.

## 6. Identity resolution contract

Replace the current all-or-error preflight result with:

```python
class _LocalBrokerIdentity:
    ...existing fields...
    unresolved_codes: tuple[str, ...]
```

`_load_local_broker_identity()` must still reject ambiguous aliases, invalid current
aliases, non-US currency conflicts, and malformed provider codes. For a syntactically
valid `us...`/USD holding with no current alias, it adds the code to the sorted unique
`unresolved_codes` tuple instead of inventing an ID. `instrument_ids` contains only
resolved holdings.

Broker-only refresh retains the existing fail-closed behavior because it deliberately
does not construct a market client and cannot collect the discovery evidence required
by this contract.

## 7. Acquisition and transaction sequence

Split the current combined market read into two bounded operations:

1. `_read_quote_snapshot()` performs exactly one quote request for all active US
   holdings and returns the raw response plus parsed items.
2. Resolve known/unresolved identities from the database.
3. `_read_bar_snapshots()` requests bars only for resolved codes, preserving the
   existing per-code bounds and total request budget.
4. In one market transaction:
   - archive and insert the raw quote artifact once;
   - persist discovery snapshots for unresolved quote items;
   - run the existing classification, latest-quote, universe, calendar, and bar paths
     only for resolved codes;
   - read back every new discovery/classification record before commit.
5. If unresolved codes exist, persist one terminal `BrokerSyncRun(status="PARTIAL")`
   with `cash_flow_quality="NOT_RUN"`, `failure_reason="IDENTITY_PENDING"`, completed
   time, attempt count one, and the existing safe local route/idempotency conventions.
   Do not create a `PortfolioSnapshotRecord` for this run.
6. Stop before cash, orders, and cash-flow reads. Return the pending result.
7. If no unresolved code exists, continue the current broker freeze/persistence and
   diagnosis path unchanged.

If the quote response omits a requested unresolved code, persist a discovery document
with `routing_family="unknown"` only when the normalized quote item exists with the
existing missing-price representation. A malformed or extra code remains a provider
contract error. If raw archiving or discovery persistence fails, no partial sync run
is reported as successful.

## 8. Result and read model

The local CLI result for the pending branch is:

```json
{
  "status": "identity_pending",
  "snapshot_status": "PARTIAL",
  "execution_locked": true,
  "pending_instrument_count": 1,
  "pending_route_counts": {
    "current_held_common_stock": 1,
    "current_held_fund_or_structured": 0,
    "manual_review": 0
  },
  "known_market_symbols": 14,
  "portfolio_snapshot_updated": false,
  "provider_requests": {
    "accounts": 1,
    "holdings": 1,
    "quotes": 1,
    "bars": 14,
    "cash": 0,
    "orders": 0,
    "cash_flows": 0,
    "total": 17
  }
}
```

Counts are illustrative; production derives them. The public result contains counts
only, not provider codes or account facts. A protected pending-instrument read endpoint
may return provider code, provider name, routing family, evidence time, route, and
status. It must not return holdings quantity/cost or raw payload.

The portfolio status view must distinguish:

- latest complete portfolio snapshot (unchanged and still viewable);
- latest refresh attempt (`PARTIAL`, `IDENTITY_PENDING`);
- decision readiness for the latest attempt (`NOT_READY`);
- pending identity count and evidence time.

Page refresh remains persisted-read-only and never starts identity collection.

## 9. Compatibility and migration

No physical table or Alembic migration is required. `DataArtifact`, `DataSnapshot`,
and `BrokerSyncRun.PARTIAL` already support the selected records. Migration head stays
`0052_fosun_classification`.

Existing classification snapshots, READY portfolio snapshots, Runs, and API payloads
remain readable. The new pending fields are additive. Old code ignores the discovery
source; rollback is code rollback, retaining immutable unreferenced discovery rows.
No downgrade deletes evidence.

## 10. Exact implementation files

- `engine/src/nexus/market/fosun_instrument_discovery.py`: strict document and route.
- `engine/src/nexus/market/fosun_instrument_discovery_storage.py`: artifact,
  snapshot, replay/conflict, exact readback, pending query.
- `engine/src/nexus/ops/fosun_local_refresh.py`: partial identity result, split quote/
  bars, filtered known persistence, PARTIAL result and request counts.
- `engine/src/nexus/ops/fosun_local.py`: persisted status projection.
- Engine API schema/service files only if needed to expose the protected pending read
  model; no page-triggered producer.
- Web portfolio/overview component: one compact pending-identity banner and rows from
  the persisted read endpoint; no controls that imply execution eligibility.

Implementation must not modify unrelated intraday Alpaca work currently present in
the main checkout.

## 11. Test-first acceptance matrix

Every production behavior starts with a failing test whose failure is observed.

1. Unknown common-stock-like holding:
   - one quote request occurs before pending outcome;
   - exact raw quote and discovery snapshot persist;
   - known instruments' market evidence persists;
   - no cash/order/cash-flow read and no portfolio snapshot;
   - PARTIAL sync and locked pending result are returned.
2. Unknown fund/structured-like holding routes only to
   `current_held_fund_or_structured`; it does not claim ETF or ETN.
3. Unknown/missing/conflicting classification routes to manual review and remains
   locked.
4. Existing known holdings preserve the current request counts, READY snapshot,
   diagnostics, and classification behavior.
5. Broker-only refresh still rejects an unknown US identity before any persistence.
6. Discovery replay is idempotent; same-rank conflict, raw hash/length/URI/request
   mismatch, future clock, malformed code, and post-resolution stale selection fail
   closed.
7. A resolved alias makes the pending query omit the discovery without rewriting it;
   the next full refresh becomes READY.
8. API/page tests prove latest complete snapshot and latest PARTIAL attempt are not
   conflated, and page reads make zero provider/model calls.
9. PostgreSQL integration proves transaction rollback leaves no database half-state
   and no PortfolioSnapshot is linked to a partial unresolved run.

Targeted verification runs the affected unit and PostgreSQL integration tests, Ruff,
Mypy, Engine merge gate, frontend lint/typecheck/tests/build if Web changes, and a
focused migration-head check. A second real provider refresh is a separate explicit
acceptance step after code integration and runtime upgrade; it must not be replayed on
an ambiguous result.

## 12. Completion criteria

The implementation is complete only when:

- a synthetic unknown holding reproduces the old failure before the change and reaches
  the exact pending outcome after it;
- known market facts commit while the unresolved holding never gains an invented
  `Instrument` or portfolio position;
- the latest complete portfolio remains visible but cannot be mistaken for a fresh
  decision-ready snapshot;
- immutable discovery evidence is raw-bound and later resolvable through existing
  identity importers;
- all affected verification passes and the isolated branch is integrated without
  overwriting concurrent main-checkout changes.
