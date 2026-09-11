# Fosun Classification Agent Evidence Design

## Goal

Expose Fosun security-classification evidence through the existing
`get_security_snapshot` Agent tool while preserving point-in-time authority,
raw-source traceability, and fail-closed identity semantics.

The feature helps an Agent decide which research route to inspect. It does not
assign a canonical asset type, decide ETF versus ETN, or change execution
eligibility.

## Current state and problem

The Fosun gateway now preserves source fields from two different fact classes:

- Holdings `name`, `symbol`, and `productType` fields.
- Quote `first`, `second`, and `third` classification levels.
- Quote company-fundamental and net-asset field-presence profiles.
- Quote fund-type text, provider name, and delay flag.

These fields must not share one persistence or freshness policy merely because
they arrive in the same API response.

## Acquisition and persistence policy

NEXUS separates source acquisition from durable evidence. A fact that must be
fresh is still persisted after acquisition so the decision can be reproduced;
"real time" never means "memory only".

| Fact class | Examples | Acquisition rule | Persistence and consumer rule |
| --- | --- | --- | --- |
| Slow-changing provider identity hints | Holdings `name`, `symbol`, `productType`; Quote `first/second/third`, fund-type text, company/net-asset profile shape | Fetch on first discovery of a holding, each normal full Fosun refresh, and an explicit identity refresh | Persist immutable observations. Use only for research routing. Unknown or stale evidence never becomes a canonical asset type. |
| Current portfolio authority | quantity, available/frozen quantity, cost, cash | Read from the broker at the start of a portfolio refresh or decision workflow | Persist the reconciled portfolio snapshot. A stale snapshot may be displayed as stale but cannot satisfy a current decision gate. |
| Live market state | last, bid, ask, quote/trade timestamps, `tradeStatus`, session state, provider delay | Fetch for the requested decision bucket or an explicit market refresh job | Persist every admitted market snapshot with its receipt and source times. Page browsing only reads persisted results. Consumers apply the existing age, session, delivery-mode, and coverage gates. Never copy these fields into identity observations. |
| Runtime product/account capability | `buyable`, `sellable`, margin, fractional, DCA eligibility | Re-read when preparing a future executable action and again at final order validation | Persist the checked result with the proposal/order audit. Never infer security identity from these flags and never reuse an old value as current permission. |
| Periodic research facts | NAV, asset size, market cap, shares, PE/PB and filings | Refresh according to the source's publication cadence or when a research Run requests a missing/current period | Persist with source period, availability time, revision, and evidence reference. The classification record stores only profile-field presence, not these changing values. |
| Derived facts | routing family, features, diagnostics, scenario outputs | Compute deterministically from frozen inputs | Persist the result and input hashes. Recompute for a new Run; never silently rewrite an old Run. |
| Provider session and entitlement | login/subscription state, delayed/real-time entitlement | Verify on connection and for the active collection session | Persist a receipt for audit, but an old receipt cannot authorize a new session. |

For this feature, only the first row is added to the Agent security tool. Live
market state remains in the existing price/snapshot tools and portfolio facts
remain in portfolio tools. This prevents the Agent from treating a persisted
classification observation as a current quote or trading permission.

The Engine can deterministically derive one of three routing families from a
Quote item:

- `company_equity_like`
- `fund_or_structured_like`
- `unknown`

However, this evidence currently ends at the archived market response. The
database-backed Agent tools only expose evidence frozen into the research Run.
Reading the newest Quote at tool-call time would introduce a time-of-check
change and would bypass the Run's evidence manifest.

## Selected architecture

Use an immutable normalized identity-hint observation backed by the
content-addressed Fosun Quote artifact produced by a normal full Fosun refresh.
Freeze its `ResearchEvidenceRef` into the research snapshot, then project the
observation through `get_security_snapshot`.

The data flow is:

```text
Fosun Quote response
  -> content-addressed DataArtifact
  -> ResearchEvidenceRef + FosunSecurityClassificationObservation
  -> AgentDataSnapshotEvidenceRecord
  -> frozen Agent Run evidence manifest
  -> get_security_snapshot
```

This reuses the existing tool name and authorization scope. No Agent receives a
gateway client, broker credentials, object-store access, or a caller-supplied
classification.

## Persistence contract

Add an append-only `fosun_security_classification_observations` table with:

- Immutable row identity and `instrument_id`.
- A composite ownership relationship to one `ResearchEvidenceRef`.
- `source`, `provider_object_id`, `revision`, availability time, and receipt time.
- Provider name and optional `first`, `second`, `third` integer levels.
- Boolean field-presence facts for the company-equity and net-asset profiles.
- Optional Simplified Chinese, Traditional Chinese, and English fund-type text.
- The deterministic routing family.

The routing family is stored so the persisted projection is inspectable, but it
must equal a database check-compatible combination:

- `company_equity_like` requires exact levels `1/101/10105`, complete company
  profile fields, and an incomplete net-asset profile.
- `fund_or_structured_like` requires exact levels `2/202/20202`, complete
  net-asset profile fields, and an incomplete company profile.
- Every partial, conflicting, or unrecognized combination is `unknown`.

The row never contains a canonical `asset_type` column. Holdings `productType`
is retained in the broker reconciliation archive but is not copied into this
Quote-backed research observation because the live sample showed one constant
value across multiple security kinds and the broker archive is not the same
source artifact.

One full-refresh Quote batch may back multiple instrument observations. The
evidence reference owns the exact raw batch bytes; an observation is unique by
`instrument_id + evidence_ref_id`. Replaying identical bytes is idempotent.
Conflicting normalized content for the same owner is rejected.

The 15-second intraday collector does not create classification observations.
Its Quote artifacts remain market-state evidence governed by market freshness.
This avoids duplicating unchanged identity hints at quote frequency.

The migration downgrade removes only the new normalized observation table. It
does not delete shared artifacts or evidence references.

## Ingestion behavior

During the existing full Fosun local refresh transaction:

1. Archive the exact Quote evidence as today.
2. Create or reuse one content-addressed `ResearchEvidenceRef` for that artifact.
3. For every returned Quote item carrying provider classification evidence,
   persist one normalized observation.
4. Validate the stored row by reading it back through the production reader.
5. Commit it atomically with the existing Quote observations.

The full refresh already performs the required external read, so classification
capture adds no provider request. A newly discovered holding is included in
that bounded Quote request and can acquire its first observation immediately.
If this read is absent or malformed, the holding remains visible with unknown
classification; the system does not replay the request from an Agent tool.

A requested symbol with no provider row, or a provider row with all three
classification levels absent, creates no classification observation. This is
reported later as unavailable; absence is not rewritten as an affirmative
`unknown` observation.

Malformed field types still reject the provider response at the gateway
contract. Valid but partial fields persist and classify as `unknown`, so a new
security cannot fail the whole refresh merely because its enum is unfamiliar.

## Snapshot freezing

`prepare_research_snapshot` selects at most one Fosun classification observation
for the requested instrument where all source and database clocks are no later
than `as_of`.

Selection order is deterministic: newest availability, newest receipt, then
stable evidence identity. If equally ranked rows disagree on normalized
classification facts, no classification evidence is admitted.

The selected evidence reference is included in the same bounded evidence set as
other research observations. Existing maximum-evidence and decision-time rules
still apply. The research snapshot revision therefore changes when the selected
classification authority changes.

Older snapshots and Runs are never backfilled. They continue to return no Fosun
classification evidence.

Classification is supplemental rather than a decision-readiness gate. Its age
is returned to the consumer through `available_at`; the normal full refresh is
the refresh trigger. The Agent tool never performs an external read. If a
workflow later requires a stricter maximum age, that policy must be introduced
as an explicit versioned rule rather than hidden in the tool.

## Agent tool projection

For a frozen Fosun classification evidence item, `get_security_snapshot`
includes:

```json
{
  "provider_security_classification": {
    "provider": "fosun",
    "provider_name": "source value or null",
    "levels": {"first": 2, "second": 202, "third": 20202},
    "equity_profile_fields_complete": false,
    "net_asset_profile_fields_complete": true,
    "fund_asset_type": {"chs": "", "cht": "", "eng": ""},
    "routing_family": "fund_or_structured_like",
    "canonical_asset_type": null,
    "identity_authoritative": false
  }
}
```

The wrapper retains the existing `evidence_ref_id`, source, revision,
availability time, and content hash. Citation extraction therefore continues to
bind the claim to the Run's frozen evidence index.

If a frozen reference claims to be Fosun classification evidence but its owner
row is missing, duplicated, outside the instrument/time scope, or inconsistent
with the deterministic classifier, `get_security_snapshot` returns the existing
invalid-frozen-authority outcome. It does not silently omit or recompute the
record from current market state.

## Safety and non-goals

- No live provider request occurs during snapshot preparation or Agent tool use.
- Realtime price, delay, session, and tradability facts stay outside this tool
  projection and keep their existing market-snapshot freshness rules.
- No new Agent tool name or permission is added.
- Provider names are supporting routing evidence, not legal identity.
- `fund_or_structured_like` does not mean ETF and may include ETF, ETN, or an
  unresolved product.
- The feature does not create instruments, rewrite existing identity records,
  alter risk policy, or change `execution_locked`.
- Existing quote, price-bar, and broker reconciliation authorities remain
  separate.

## Verification

Tests must prove:

1. Ingestion stores complete, partial, and conflicting-valid provider evidence
   with the correct fail-closed routing family.
2. Identical replay is idempotent and an owner conflict is rejected.
3. Snapshot preparation freezes only an eligible point-in-time observation and
   changes the snapshot revision when authority changes.
4. The Agent tool returns the exact frozen projection with its evidence ID.
5. Mutating or deleting the owner row after freezing produces invalid frozen
   authority, not fallback to a newer Quote.
6. Old Runs without this evidence remain compatible.
7. Migration upgrade and downgrade preserve existing shared data.

Run focused unit and PostgreSQL integration tests, Ruff, Mypy, and migration
upgrade/downgrade checks for the affected Engine paths.
