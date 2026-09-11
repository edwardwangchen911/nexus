# Fosun Classification Agent Evidence Design

Status: reviewed and implementation-ready
Decision date: 2026-09-12

## 1. Outcome and boundaries

Persist the classification fields already returned by a normal full Fosun quote
refresh, freeze one point-in-time observation into a research snapshot, and expose
it through the existing `get_security_snapshot` Agent tool.

This is routing evidence only. It must never assign canonical
`Instrument.asset_type`, claim ETF versus ETN, satisfy price/spread/tradability/
entitlement/execution authority, trigger a provider call from a page or Agent tool,
or backfill an existing Run. `execution_locked=true` remains invariant.

The Engine classifier continues to return only `company_equity_like`,
`fund_or_structured_like`, or `unknown`. Valid partial/unrecognized evidence maps to
`unknown`; malformed source types still fail at the existing provider boundary.

## 2. Persistence and freshness policy

Acquisition freshness and durable storage are separate: current facts are persisted
after acquisition so decisions can be reproduced.

| Fact class | Acquisition | Persistence and consumer |
| --- | --- | --- |
| Provider identity hints | first discovery, normal full refresh, explicit future identity refresh | Immutable normalized evidence; research routing only |
| Portfolio facts | portfolio refresh/decision workflow | Existing portfolio snapshot and current-decision gates |
| Market state | explicit market collection | Existing quote/bar authority and age/session/feed gates |
| Runtime capability | future executable preparation and final validation | Proposal/order audit; old value cannot authorize |
| Research facts | source cadence or missing period | Existing versioned evidence and availability clocks |

Only the first row changes. Price, bid/ask, `provider_reported_delay`, session state,
and tradability remain outside the classification document.

## 3. Selected architecture

No new SQL table is added. Reuse the established immutable evidence chain:

```text
normal full Fosun Quote response
  -> existing raw DataArtifact (fosun_gateway_market_snapshot_v1)
  -> normalized per-instrument DataArtifact (fosun_security_classification_v1)
  -> DataSnapshot + ResearchEvidenceRef
  -> AgentDataSnapshotEvidenceRecord
  -> frozen Run manifest
  -> get_security_snapshot
```

This matches current research observations and leaves `LatestQuote`,
`PositionSnapshot`, the tool catalogue, risk, and execution policy unchanged.

## 4. Canonical document contract

Create `engine/src/nexus/research/fosun_security_classification.py` with:

```python
SOURCE = "fosun_security_classification"
SCHEMA = "fosun_security_classification_v1"
RAW_SCHEMA = "fosun_gateway_market_snapshot_v1"
MAX_CANONICAL_BYTES = 16_384

class FosunClassificationLevels(BaseModel):
    first: int | None
    second: int | None
    third: int | None

class FosunFundAssetType(BaseModel):
    chs: str | None  # max 128; retain empty source strings
    cht: str | None
    eng: str | None

class FosunRawQuoteBinding(BaseModel):
    artifact_sha256: str       # 64 lowercase hex
    artifact_byte_length: int  # 1..1 MiB
    artifact_uri: str
    gateway_request_id: UUID
    request_body_sha256: str   # 64 lowercase hex

class FosunSecurityClassificationDocument(BaseModel):
    schema_version: Literal["fosun_security_classification_v1"]
    source: Literal["fosun_security_classification"]
    instrument_id: UUID
    provider_code: str
    provider_name: str | None
    levels: FosunClassificationLevels
    equity_profile_fields_complete: bool
    net_asset_profile_fields_complete: bool
    fund_asset_type: FosunFundAssetType
    routing_family: FosunSecurityFamily
    received_at: AwareDatetime
    available_at: AwareDatetime
    raw_quote: FosunRawQuoteBinding
    revision_semantics: Literal["local_content_hash"]
    canonical_asset_type: None
    identity_authoritative: Literal[False]
    price_authority_eligible: Literal[False]
    historical_backtest_eligible: Literal[False]
    execution_locked: Literal[True]
```

All models are frozen, strict, `extra="forbid"`, and hide input in errors.
`received_at == available_at` because the provider classification has no separately
qualified source timestamp. `routing_family` must equal
`classify_fosun_security_family()` over stored fields. `provider_code` uses the
existing Engine Fosun validator. `provider_object_id` is exactly
`classification:{instrument_id}:{provider_code}`.

`canonical_bytes()` is deterministic UTF-8 JSON with sorted keys, compact separators,
and `ensure_ascii=False`. Its SHA-256 is the normalized artifact, snapshot, and
evidence revision/content hash. The payload binds the already archived raw quote
artifact; raw bytes are not copied into JSONB.

## 5. Storage API and transaction

Create `engine/src/nexus/research/fosun_security_classification_storage.py`:

```python
async def save_classification_artifact(
    session: AsyncSession,
    store: S3ProviderEvidenceStore,
    document: FosunSecurityClassificationDocument,
) -> str: ...

async def persist_security_classification(
    session: AsyncSession,
    document: FosunSecurityClassificationDocument,
    *, location: str,
) -> ResearchEvidenceRef: ...

async def read_security_classifications(
    session: AsyncSession,
    *, instrument_id: UUID, as_of: datetime,
    evidence_ids: tuple[UUID, ...] | None = None,
) -> dict[UUID, FosunSecurityClassificationDocument]: ...
```

`save_classification_artifact` writes
`evidence/fosun/security-classification/{sha256}.json` with `put_if_absent`, reads
it back byte-for-byte, and inserts/reuses `DataArtifact`. Persistence uses
`begin_nested()` and PostgreSQL `ON CONFLICT DO NOTHING`, then strictly reads back
`ResearchEvidenceRef` and `DataSnapshot`. It never commits; the full refresh owns
commit/rollback. A database rollback may leave an unreferenced content-addressed
object, which is safe and reusable.

Unique keys are `DataArtifact.sha256` and each
`(source, provider_object_id, revision)`. Identical replay reuses evidence. A match
with different URI, hash, size, clocks, schema, or payload raises a stable
`FOSUN_CLASSIFICATION_*_CONFLICT` error.

The reader joins snapshot and evidence on full source/object/revision identity and
validates normalized and raw artifacts, instrument/code binding, hashes, sizes,
locations, clocks, flags, and recomputed routing family. All relevant `created_at`,
`received_at`, and `available_at` values must be `<= as_of`.

Without `evidence_ids`, choose the newest rank `(available_at, received_at)`. If
same-rank rows differ in canonical classification facts, return empty. If equivalent,
choose the smallest evidence UUID. With `evidence_ids`, return only validated frozen
members.

## 6. Full-refresh producer

Modify `_persist_market_reads()` in `engine/src/nexus/ops/fosun_local_refresh.py`
after the existing raw quote artifact is flushed:

1. Reuse the already parsed quote page.
2. For each item with `provider_classification is not None`, construct the canonical
   document from the validated item and raw request/artifact binding.
3. Save the canonical artifact, then persist snapshot/evidence.
4. Re-read through the production reader and require exact ID/document equality.
5. Continue the existing transaction.

There are no extra provider calls. The push/15-second intraday collectors are not
producers. No classification object means no evidence and the refresh continues.
Price remains owned by existing quote records.

## 7. Snapshot freezing

Modify `prepare_research_snapshot()`:

- Keep the existing early `if not evidence: return None`; classification cannot
  create the base research snapshot.
- Read at most one eligible classification at `as_of`.
- Reserve one existing research evidence slot by evicting only the oldest SEC
  reference if required; never evict another supplemental observation.
- Append the reference and use `persisted_research_observations` scope.
- Let the existing payload hash change naturally with the evidence ID.

Old snapshots and Runs remain untouched.

## 8. Agent projection and fail-closed rule

`security_snapshot()` reads only frozen evidence IDs. The set of frozen IDs whose
source is `fosun_security_classification` must equal the validated reader keys.
Otherwise return existing invalid-frozen-authority output; never omit, recompute, or
fall back to latest data.

The matching evidence entry gains:

```json
{"provider_security_classification": {
  "provider": "fosun", "provider_name": null, "provider_code": "usXYZ",
  "levels": {"first": 2, "second": 202, "third": 20202},
  "equity_profile_fields_complete": false,
  "net_asset_profile_fields_complete": true,
  "fund_asset_type": {"chs": "", "cht": "", "eng": ""},
  "routing_family": "fund_or_structured_like",
  "canonical_asset_type": null, "identity_authoritative": false
}}
```

The outer evidence/citation fields remain unchanged.

## 9. Migration

Add `0052_fosun_security_classification.py` with revision ID
`0052_fosun_classification` (bounded by the existing 32-character version column),
revising `0051`. It creates no table;
it adds one branch to `nexus_validate_agent_snapshot_evidence()` requiring:

- canonical snapshot and normalized artifact ownership;
- bound raw quote artifact hash/URI/size/schema/source;
- exact source/schema/instrument/object/hash/size/location/clocks;
- exact JSON safety flags and one of the three routing literals;
- existence of all rows by the research snapshot time.

Python validates classifier semantics; SQL prevents malformed/unowned evidence from
being frozen. Downgrade refuses while any frozen classification owner exists,
otherwise removes only this branch. Generic persisted artifacts/snapshots/evidence
are not deleted and remain reusable after re-upgrade.

## 10. Failure semantics

| Condition | Result |
| --- | --- |
| No classification object | No evidence; refresh continues |
| Valid partial/unrecognized | Persist `unknown` |
| Malformed source type | Existing provider rejection |
| Artifact/owner mismatch | Roll back refresh with stable classification error |
| No base research evidence | No snapshot, unchanged |
| Equal-time conflict | No classification admitted |
| Frozen owner missing/corrupt | Invalid frozen authority |
| Newer evidence after Run cutoff | Ignored; no fallback |

## 11. Files, acceptance, and rollback

Create the two research modules, migration `0052`, and focused tests. Modify only
`fosun_local_refresh.py`, `agents/snapshots.py`, `agents/database_tools.py`, and
necessary focused fixtures. No Web/BFF/client/Gateway/dependency/table change.

Acceptance requires: full refresh persists raw-bound evidence without increasing
request count; routing variants and replays behave exactly; corruption fails closed;
one PIT observation freezes; later data cannot change an old Run; exact Agent
projection/citation is returned; old Runs work; migration guards pass; focused tests,
Ruff, and Mypy pass without real external calls.

Roll out migration before Engine. Until a full refresh, behavior is unchanged. Roll
back Engine before migration. If frozen evidence exists, keep migration installed;
the older Engine safely ignores the supplemental source. Never delete evidence merely
to force downgrade.
