# Schwab Intraday Automatic Trigger Design

Status: approved in chat; awaiting written-spec review
Decision date: 2026-09-12

## 1. Outcome and boundaries

Allow the existing Provider worker to schedule one bounded Schwab Level One capture for each
eligible 15-second intraday bucket, freeze its raw transcript, and publish verified trade and
two-sided quote observations through the existing `DataArtifact -> LatestQuote` path.

This remains display-only. It does not add a resident streaming service, automatic reconnect,
cross-provider retry/fallback, account access, minute bars, research authority, execution-cost
authority, or trading permission. Every Job remains one-shot and `execution_locked=true` remains
unchanged.

## 2. Purpose-specific provider configuration

Keep `NEXUS_MARKET_PROVIDER` as the daily/general market source. Add an optional
`NEXUS_INTRADAY_PROVIDER` with values `fosun`, `alpaca`, or `schwab`; when absent it inherits the
current market provider so existing configurations keep their behavior.

Schwab intraday mode requires the existing static App configuration and the existing protected
local token directory. It reuses the configured Provider database and object store identity; no
Schwab-specific PostgreSQL role, MinIO user, bucket, or object prefix is added. The normal Mac
local worker entry loads `infra/.env.local`, localizes Compose endpoints to loopback, and points at
the existing `.secrets/nexus-schwab-local` directory. Secrets never enter Job payloads, logs, or
command-line arguments.

## 3. Immutable Job contract

Publish `intraday_snapshot_job_v3` rather than widening v2 in place. The v3 payload retains the
existing calendar authority, bucket, canonical instrument bindings, source/feed/delivery fields,
and contract hash.

For Schwab it requires exactly:

- `source=schwab`, `feed=l1`, `delivery_mode=real_time`;
- no Fosun capability bindings;
- `include_minute_bars=false` in every bucket;
- at most the existing global 30-symbol ceiling;
- a Schwab-specific contract hash over the complete payload contract.

Migration `0055` admits only v3 Jobs after confirming there are no actionable v2 intraday Jobs.
The new worker rejects or refuses to start against older schema heads, preserving the existing
queue-drain and migration fencing behavior.

## 4. Provider adapter and data flow

Add a Schwab intraday adapter behind the existing snapshot-provider boundary:

```text
15-second scheduler
  -> provider Job v3
  -> revalidate calendar, watchlist, exact source and bucket
  -> load/refresh existing Schwab token once if required
  -> userPreference once
  -> one bounded LEVELONE_EQUITIES WebSocket connection
  -> freeze receipt plus original frames
  -> existing Provider object store raw archive
  -> validate bounded_complete and delayed=false
  -> normalize latest trade and two-sided quote per observed symbol
  -> existing idempotent LatestQuote ingestion
```

The adapter uses a private temporary directory only while collecting because the proven collector
already enforces file permissions, byte/frame limits, protocol order, and secret-reflection checks.
It packs the transcript into the same `schwab_level_one_stream_v1` in-memory envelope, then removes
only its own temporary directory. The existing handler archives those returned raw bytes before
normalization. Persisted evidence stays solely in the configured object store and database.

The connection duration is capped by the Job deadline and never reconnects. A complete transcript
may contain only a subset of requested symbols; those symbols are published and the Job reports a
partial result. Unknown delay, `delayed=true`, missing timestamps, one-sided quotes, malformed
frames, or incomplete authentication/subscription never become real-time observations.

## 5. Scheduling, selection, and errors

The scheduler chooses `NEXUS_INTRADAY_PROVIDER`, not `NEXUS_MARKET_PROVIDER`, for Job source and
scope. Fosun and Alpaca behavior remains unchanged. Only one source owns a bucket, so the change
does not introduce fallback or parallel provider requests.

Stable outcomes are:

| Condition | Result |
| --- | --- |
| Calendar covers today but market is closed | no Job |
| Missing current calendar/watchlist authority | existing fail-closed result |
| Token/config invalid | `INTRADAY_PROVIDER_AUTH_REQUIRED` |
| Schwab unavailable/disconnected/incomplete | `INTRADAY_PROVIDER_UNAVAILABLE` |
| Delayed or contradictory stream evidence | `INTRADAY_PROVIDER_POLICY_REJECTED` |
| Some requested symbols absent | persist valid observed symbols; `INTRADAY_SNAPSHOT_PARTIAL` |
| Complete valid observations | `INTRADAY_SNAPSHOT_COLLECTED` |
| Exact replay | `INTRADAY_SNAPSHOT_REPLAYED` |

Provider failure cooldown continues to operate by `schwab-intraday` scope across later 15-second
buckets. There is no in-Job retry and no retry of uncertain external results.

## 6. Files and verification

Expected changes are limited to Provider settings/composition, intraday schedule/collection,
Schwab stream normalization, the Provider worker's Mac-local entry, migration `0055`, Compose/env
documentation only where configuration inventory must remain accurate, and focused tests.

TDD acceptance covers:

1. Existing configs inherit their current intraday provider.
2. Schwab v3 payload rejects bars, wrong feed/delivery, credentials, and invalid identities.
3. A scheduled Job with a complete fake Schwab transcript persists raw evidence plus trade/quote
   and reaches `succeeded/INTRADAY_SNAPSHOT_COLLECTED`.
4. Replaying the exact transcript creates no duplicate observations.
5. Delayed, partial, expired, wrong-source, and authority-change cases fail closed without an
   external retry.
6. A real local automatic-trigger acceptance checks persisted `Job -> attempt -> artifact ->
   LatestQuote -> intraday view`. During a closed market, acceptance stops after proving the
   scheduler correctly creates no Job; a real provider call waits for an eligible market bucket.

Focused Pytest, PostgreSQL integration tests, Ruff, Mypy, migration round-trip, and Compose config
parsing must pass. Rollback stops the exact local Provider worker and restores
`NEXUS_INTRADAY_MONITOR_MODE=disabled`; persisted evidence is not deleted or relabeled.
