# Fosun read-only activation runbook

## Boundary and current result

Real-provider probing is not part of autonomous local implementation. This runbook may be used
only after the user explicitly approves one bounded probe and the six pilot prerequisites below are
evidenced. It never authorizes an order, simulation-account mutation, subscription, Webhook, or
generic provider request.

The one-shot probe does not start `provider-worker`, PostgreSQL writes, object storage, Compose, or
a long-running Gateway. Those components become part of the trusted boundary only in the later
continuous-ingestion stage.

The local decision command reads only the provider manifest and optional sanitized artifact,
detached receipt, and explicitly supplied non-secret publisher pin. It has no provider transport,
does not import the SDK, and does not read gateway credentials:

```bash
uv run --package nexus-fosun-gateway nexus-fosun-activation-gate
```

The committed manifest is deliberately `probe_ready`, not production `admitted`. Therefore the
continuous-runtime activation command remains
`fixture_with_next_action / COMPLETE_SDK_ADMISSION`; it cannot activate Provider access.

Fresh public-contract verification on 2026-08-31 is now reflected in the manifest, including
Trade OpenAPI SHA-256
`87e4e6601b8da122a504239a1922d8742ecd4072e2d10c30c0d6a97f896fb808`.
The production REST base URL is bound to `https://openapi.fosunxcz.com`.
`fsopenapi` is locked to the exact reviewed SDK commit for the bounded probe.

The first activation is REST pull-only. The separately confirmed push endpoint
`wss://openapi-push.fosunxcz.com/msg/v1/push` is deferred and is not a prerequisite for account,
cash, holdings, quote, or bar reads.

## Minimum preconditions before requesting probe confirmation

Do not ask for or execute a real probe until all of these are independently evidenced:

1. The API key previously exposed in conversation has been revoked. No key value may be used as
   proof or copied into a command, document, log, test, shell history, or artifact.
2. A rotated API key, client private key, server public key, and provider TLS trust material exist
   only in the gateway's secret manager and are scoped read-only.
3. The fixed production egress IP has been added to the provider allowlist and the intended target
   environment is recorded.
4. `docs/runbooks/fosun-sdk-admission.md` records the private-use basis, probe vector,
   security/TLS review and exact provider base URL for this one-shot SDK/OpenAPI subject.
5. The manifest is reviewed to `admission.status: probe_ready`, and the probe admission tests,
   offline gateway suite, route-surface policy and secret scan all pass.
6. A probe owner, UTC window, stop conditions and rollback owner are recorded. Only then request one
   explicit user confirmation for that exact network probe.

## Deferred continuous-runtime gates

The following controls remain relevant before sustained Provider collection or deployment, but
they do not block the first one-shot REST probe:

7. The reviewed `provider-worker` source/image identity and the exact Provider runtime grant template
   are recorded. The database credential must authenticate directly as the Provider login role:
   `SET ROLE`, outbound memberships and any role that can inherit or assume the Provider role are
   forbidden. Any unknown image, role-membership edge, writable shared schema, or unexplained
   runtime drift is a stop because this worker is part of the market-data TCB. Both the grant
   template preflight/postflight and every worker startup require a direct session on a `LOGIN`
   Provider role with no effective `CREATE` privilege on the current database.
8. The database startup probe confirms the Provider role remains read-only for authority inputs,
   cannot read backtest-member tables, and has no direct access to the publication-mark table or
   either minimal trigger-only `SECURITY DEFINER`. The probe must match the exact two-function set,
   source, owner, ACL, search path, two non-deferred publication `AFTER INSERT FOR EACH ROW`
   triggers, and one frozen-bar `BEFORE INSERT FOR EACH ROW` trigger. It also rejects effective
   privileges in any non-system schema, any non-allowlisted table/view/materialized-view/foreign
   table privilege or grant option, any Provider or `PUBLIC` column ACL, any Provider or `PUBLIC`
   sequence privilege, and any extra non-system `SECURITY DEFINER`. It also matches all 63
   non-internal application triggers on Provider-insertable tables plus five mutation-side
   triggers on `integration_status` and `provider_capabilities` (68 total), including enabled
   state, timing/events, function owner/configuration and function-body digest. The latter set
   freezes the status lock, capability-scope lock, capability append-only guard and both
   created-at guards; disabling, renaming or changing any of them is a startup stop. RLS helpers use a
   `pg_catalog`-first fixed search path, and bypass policies are bound to the catalog
   `pg_has_role` OID. Provider sessions must report `READ COMMITTED` at startup and before every
   schedule/preclaim/claim cycle. Advisory locks, not table `UPDATE`, serialize authority
   validation against status/artifact/manifest mutation. The fixed application order is sorted
   capability scope, sorted integration-status UUID, then sorted raw-artifact UUID; a post-lock
   `populate_existing` reload must not be replaced by an ORM-cached value. Every Provider-writable
   relation must remain a plain table
   (`relkind='r'`); every frozen trigger must have an empty `tgattr` (`UPDATE OF` is forbidden) and
   must not be a partition clone or inherited topology expansion.
   The role can only `SELECT` `public.alembic_version`; each worker startup and every polling loop
   require the one exact current head. The `jobs` admission trigger and its frozen function digest
   must also prove that new intraday Jobs have matching v2 column and payload schemas.
9. Object-store admission records a Provider-only IAM principal distinct from the app/general
   worker principal, a pre-provisioned bucket whose versioning status is exactly `Enabled`, and the
   fixed `provider/market` prefix. Missing versioning state or `Suspended` is an activation stop.
   Effective-policy evidence must deny bucket-root/list access and every other prefix. Below
   `provider/market`, allow PutObject, key-scoped GetObject only for conditional-create conflict
   verification, and GetObjectVersion for frozen evidence replay; ListBucket and DeleteObject are
   not required and must be denied. Missing, broader, inherited, or unreviewed access is a stop.
   For every raw or normalized create, the returned locator must bind the complete expected
   content-addressed key. `memory://` must equal the key exactly. `s3://` requires a non-empty
   bucket, no userinfo or fragment delimiter, an unencoded path ending in the exact full key with
   no empty/`.`/`..` segment, and exactly one non-empty, validly percent-encoded `versionId` query.
   Its decoded value must not equal the S3 sentinel `null`; literal and percent-encoded forms are
   rejected at PUT, conflict verification, frozen-URI admission and exact-version readback. A key
   appearing only in query text or another substring is invalid. `DataArtifact.sha256`
   remains the byte digest; `raw_artifact_identity_sha256` separately covers id, URI, byte size,
   mime, schema, source, observed/available/received and created-at. Publisher/ingestion must lock,
   reload and recompute that identity before exposing facts. A daily publisher must read the raw
   artifact through that exact locator: the S3 client binds the configured bucket/prefix/full key,
   sends the decoded `versionId` to GetObjectVersion, and verifies the returned version. A key-only
   current-version read is permitted only after a conditional create returns 409/412: it must read
   the exact content-addressed key, compare the complete bytes, obtain a non-`null` VersionId, and
   immediately freeze that locator. It is not admissible as ordinary evidence replay.
10. Pre-provision the external `nexus-fosun-gateway-replay-v1` volume on the one supported gateway
    host. It must be mounted at `/var/lib/nexus-fosun-gateway`, writable only by the gateway user,
    durable across container replacement, and support cross-process POSIX advisory locks. Never
    mount a different replay volume into an overlapping replacement. Loss, recreation, or snapshot
    rollback of this volume is a security event: stop both workers and investigate before resuming.
    Every Gateway process start independently enforces a new 61-second drain/readiness gate; do not
    delete the volume during ordinary Compose cleanup.
11. Issue broker and market client certificates from two independently controlled client CAs.
    Mount each trust file as exactly one PEM certificate with a critical or non-critical
    `BasicConstraints CA=true` extension; bundles, leaf certificates, unreadable files, identical
    certificates under different paths, reissued certificates sharing one CA public key, and a CA
    trusted by both listeners are stops. Record only the public certificate and SPKI SHA-256
    fingerprints in the activation evidence. The broker base URL
    must be `https://fosun-gateway:8081` and the market base URL must be
    `https://fosun-gateway:8082`; both clients separately verify the gateway server certificate.

Missing evidence is a stop, not permission to reduce checks or fall back to unreviewed data.
The configured provider base URL must be an absolute HTTPS URL without embedded credentials,
query, fragment, or redirect dependence; the admitted adapter forces certificate verification and
rejects redirects on both frozen-SDK HTTP surfaces.
Provider fixed-origin HTTP calls suppress `httpx` and `httpcore` dependency records at the client
and request boundary because those libraries may render full query URLs or headers. Use only the
sanitized `provider_http_result` event for routine diagnostics. Enabling root or dependency
DEBUG/INFO must not be used as a troubleshooting workaround; tests require FRED keys, queries,
hosts, paths, headers and bodies to remain absent.

## Schema cutover order

`0031_intraday_schema_admission` is an admission fence, not a legacy-job converter. Before applying
it, gracefully stop the pre-cutover Provider Worker and query the authoritative `jobs` table. Every
actionable `collect.intraday_snapshot` row whose column or payload schema is not
`intraday_snapshot_job_v2` must already be absent. If any such row remains, do not migrate or start
the new worker; return to the pre-cutover release, drain it there, stop it again, and repeat the
check. Never use the new worker to dead-letter incompatible work as a migration mechanism.

After migration, apply the exact Provider grant template, then start the new worker. PostgreSQL
rejects any newly inserted non-v2 intraday Job, any tuple other than `retry_policy=never` /
`max_attempts=1`, any payload bucket that does not exactly match the aligned `effective_at`, and any
legacy terminal requeue. A partial unique index permits only one v2 intraday Job globally for each
15-second bucket. Under the matching transaction advisory lock, the scheduler reuses that first
durable Job and its frozen payload even if the current watchlist, source, feed, or authority has
changed; neither an automatic/manual retry nor an `attempt_no > 1` may issue a Provider call. The
worker also checks the exact Alembic head, index definition, trigger body, and complete incompatible
column/payload queue before scheduling or claiming. Malformed legacy bucket text yields the stable
drain error `INTRADAY_V1_QUEUE_DRAIN_REQUIRED`; a running malformed legacy row may still be moved to
`dead_letter`, but it can never be requeued. Every scheduling transaction holds the shared
schema-admission advisory lock before
checking the head, and the same guard runs at the start of the heartbeat/reclaim/claim transaction;
downgrade takes the matching exclusive advisory lock, then takes `ACCESS EXCLUSIVE` on
`worker_heartbeats` followed by `jobs` before inspecting runtime state. Those table locks close the
gap against direct SQL writers that do not participate in the advisory protocol, while the advisory
lock keeps normal scheduling and claim paths serialized. With monitoring disabled, the worker still
registers a no-network
v2 drain handler: already queued v2 snapshots end as `INTRADAY_MONITOR_DISABLED`, while no new
snapshot is scheduled. A synchronized downgrade first stops
the Provider Worker, drains exact v2 actionable Jobs (`queued`, `running`, or retryable `failed`),
and waits until its latest 0031 heartbeat is older than two minutes. An immutable `dead_letter` is
terminal and does not permanently block rollback. The migration rejects an online or non-drained
downgrade with `P3D01` and rolls back without dropping the fence.

The preceding `0030` downgrade first takes `ACCESS EXCLUSIVE` on
`fosun_daily_bar_staging`, then checks for rows and runtime grants. This keeps the refusal check and
`DROP TABLE` in one serialized transaction; never remove or weaken that lock to force rollback.

`0032_ticker_alias_authority` is the current head. Its upgrade takes `ACCESS EXCLUSIVE` on
`instrument_aliases`, rejects any overlapping ticker period where one instrument has two symbols or
one case-insensitive symbol has two instruments, then installs two immediate GiST exclusion
constraints using `btree_gist`. Before applying it, query and repair ambiguity under an approved
market-master change; never normalize or delete rows inside the migration. After upgrade, Provider
startup must attest the exact two constraints. Daily materialization also requires canonical
uppercase US symbols and records invalid input as `FROZEN_INSTRUMENT_ALIAS_INVALID` rather than
calling the Gateway.

To downgrade 0032, stop the general, Provider, and broker workers; drain all actionable daily
root/market/shard/publisher, intraday, and `broker.simulation.dispatch` jobs; then wait until their
current-head heartbeats are older than two minutes. The broker claim transaction, like the general
and Provider claim paths, pins READ COMMITTED, holds the shared schema-admission lock, and verifies
the exact migration head before heartbeat/reclaim/claim. The downgrade takes the exclusive
schema-admission advisory lock followed
by `ACCESS EXCLUSIVE` locks on `worker_heartbeats`, `jobs`, and `instrument_aliases`; current daily
scheduling/preclaim and Provider scheduling/preclaim take the matching shared lock and exact-head
check. Any current general/Provider/broker heartbeat or actionable dependent job returns `P3D01`
without dropping either constraint.
The downgrade intentionally leaves `btree_gist` installed; removing a shared extension is a
separate reviewed database operation.

## Runtime topology after admission

Admission does not change the committed defaults or start any process. A later separately approved
activation keeps `NEXUS_MARKET_PROVIDER=fosun`, `NEXUS_ALPACA_MARKET_MODE=disabled`, and uses two
non-interchangeable gateway principals:

- `provider-worker` receives only `FOSUN_MARKET_GATEWAY_*` client mTLS/HMAC settings and may call
  `fosun_get_quotes` plus `fosun_get_bars`; it has no broker identity or account-ID encryption key.
  Its `PROVIDER_OBJECT_STORE_*` identity is separate from `OBJECT_STORE_*` and all runtime keys are
  forced below `provider/market`;
- `broker-worker` receives only the existing `FOSUN_GATEWAY_*` broker client identity and may call
  the fixed portfolio/order/cash-flow read operations; it has no Provider queue or market identity;
- `fosun-gateway` is available only in the explicit `provider` and `broker` Compose profiles and is
  the sole process that receives `FSOPENAPI_*`, activation evidence, and the TLS server identity.
  One process serves broker port `8081` with only `FOSUN_GATEWAY_SERVER_BROKER_CLIENT_CA` and market
  port `8082` with only `FOSUN_GATEWAY_SERVER_MARKET_CLIENT_CA`. The local listener port binds the
  expected HMAC principal; no forwarded or caller-supplied header participates in that decision.
  Cross-listener HMAC identities fail before nonce reservation and transport execution.
  Its deployment and runtime setting are both fixed to exactly one replica. A durable SQLite nonce
  journal survives normal restart; an exclusive lease on the same named volume rejects a second
  process. Every production process start drains authentication for 61 seconds, including ordinary
  restart and replacement, so a restored older volume snapshot cannot become immediately usable.
  A persisted clock high-water mark keeps authentication and readiness closed while local wall time
  is behind observed time. Compose health additionally requires the lifespan owner PID and actual
  Gateway TCP listeners, so both workers wait until both `8081` and `8082` are accepting. Future
  timestamps are rejected. Replay capacity is counted independently for every globally unique
  HMAC key ID, preventing a market-key flood from consuming broker-key capacity. Separate replay
  admission lanes also keep all but the currently admitted market reservation out of Broker's
  shared SQLite transaction queue.
  The readiness marker contains only its version, owner PID, and the admitted capability-expiry
  epoch. Before evaluating admission, startup samples a monotonic anchor; the transport then latches
  expiry permanently when either UTC wall time or that monotonic deadline is reached. A watchdog
  applies the latch even when no request or health probe arrives. The persistent replay clock floor
  is advanced to at least `ceil(expiry)`, and an atomically replaced, file-and-directory-fsynced
  expiry tombstone preserves the fail-closed decision across restart and wall-clock rollback.
  HTTP readiness checks the same boundary. The secret-free CLI holds the marker guard from its first
  read through expiry latching, PID/singleton-lease verification and any fail-stop action, so owner
  shutdown cannot race it and erase the last evidence. If both tombstone and ready-marker persistence
  fail after expiry, the CLI uses `SIGKILL` only after confirming the recorded PID is live and the
  singleton lease is still held; the retained ready marker then forces review on the next startup.
  Request handlers continue to return `ACTIVATION_EXPIRED` and never reach the SDK. Refreshing
  activation requires a reviewed whole-process restart with new evidence; an in-place marker rewrite
  is not a supported recovery path.
  External orchestration is unsupported unless every candidate mounts the same POSIX-lock-capable
  journal volume; rolling overlap then fails closed because only one process can hold the lease;
- Web, API, the general worker and Agent receive neither client identity. Gateway route
  authorization derives the principal from the signed key ID; an unsigned role claim cannot expand
  operations.

The SDK adapter admits at most one waiting/executing request per domain before the shared SDK
execution slot. Broker and market use separate rate limiters capped at 6/min and 24/min respectively
(aggregate 30/min and 2 QPS); the absolute request deadline covers the domain lane, rate wait,
shared slot, and actual Provider call. Only the actual Provider call holds the single SDK execution
slot, implemented as one daemon thread at a time. The thread performs the final readiness/capability
checks and any logging before atomically consuming the token; a successful consume is immediately
followed by the blocking SDK method in that same thread. Therefore market method starts are at least
one second apart and every rolling `(t-60s, t]` window contains at most 24 starts. A rate wait returns
to the event loop, releases the shared slot, and sleeps there rather than blocking Broker work.
Provider rate waits, backlog, and retry backoff therefore cannot fill a shared queue ahead of broker
work. The currently executing Provider call remains serial and is never overlapped. Each
`fosun_get_bars` gateway POST is exactly one SDK `kline` attempt: an SDK exception or timeout is
terminal and must not schedule hidden retry or backoff. If a live synchronous SDK call crosses the
deadline, or its waiter is cancelled, the process permanently poisons the transport. The first
timeout reports `PROVIDER_TRANSPORT_POISONED / RESTART_GATEWAY_AFTER_REVIEW`; later broker and market
requests fail at entry. Poison removes the shared-volume readiness marker, so HTTP readiness and
the CLI/Compose healthcheck both become unhealthy. Do not retry in the same process, widen the
deadline, or recreate the transport in place. Stop and inspect the Gateway; only after the cause is
understood may an explicitly approved whole-process restart restore service. A stuck daemon cannot
be killed safely inside the living process, but it cannot block process exit. Late daemon exceptions
are consumed without formatting or logging Provider error text. Both ordinary ASGI lifespan and the
production dual-listener shutdown close new admission without waiting for a stuck daemon. Quotes and
broker reads retain their bounded transport retries only while the transport remains healthy; every
actual retry consumes the same per-domain QPS/RPM budget. Those retries belong to one signed Gateway
operation and do not authorize a second intraday Job, bucket, or Job attempt.

An existing non-expired ready marker at startup is not cleaned up automatically: it means the prior
owner did not complete the guarded shutdown path. Keep the service stopped, preserve the journal,
marker, tombstone and lease metadata, and verify the recorded PID, volume identity, activation
window, and orchestrator history. Archive the evidence before any reviewed marker recovery. Never
delete or rewrite marker state merely to make a healthcheck pass; a fresh admitted process must
re-enter the 61-second authentication drain and create its own marker.

Provider object I/O is bounded to 32 daemon calls with a fixed wall-clock deadline and no hidden
retry inside one storage request. `OBJECT_IO_SATURATED` or `OBJECT_IO_DEADLINE_EXCEEDED` is a stop
signal, not an
invitation to widen the pool or timeout: stop the Provider Worker, inspect the object-store health
and network path, then restart the worker only after the cause is understood. Calls abandoned by a
cancelled handler can create content-addressed orphans, but cannot publish database facts; the
bounded daemon model keeps those calls from blocking process exit. The automatic-safe aggregate
publisher remains queued/retryable with the exact transient error code and may reread the already
archived raw evidence after recovery. Never re-run a max-attempt-one Fosun external-read child to
repair an object-store failure.

Raw and normalized objects are written directly to their immutable, content-addressed final keys.
There is no temporary/promote/delete path and the Provider principal must not have DeleteObject.
The aggregate publisher uses a fixed worker pool: the first completed failure stops new queued I/O,
cancels and reaps in-flight siblings, and preserves the exact typed stop code rather than waiting for
input-order completion. Daily-child raw writes and intraday raw/normalized writes map typed object
failures to terminal `never/1` evidence; aggregate raw reads/normalized writes, SEC/FRED evidence,
and explicitly selected Alpaca daily ingestion keep the exact code as automatic-safe retry evidence.
All object keys use the ASCII `[A-Za-z0-9._/-]+` allowlist in addition to structural path checks.
SEC accession numbers must exactly match `10 digits-2 digits-6 digits`; the collector validates every
submission accession and its company-facts content before the first raw/evidence object write. An
invalid accession must leave zero objects and zero financial facts rather than constructing an
ambiguous query/fragment locator.

The daily aggregate prepares every normalized object before it captures one batch-wide publication
time from PostgreSQL `clock_timestamp()` (floored by the worker clock and latest staging time).
Every manifest in the aggregate must carry that same `provider_authorized_at`; a historical as-of
strictly before it must return none of the batch. Different per-child authorization times or a
publication time captured before object preparation are fail-closed acceptance failures.

The Fosun daily root has a hard admission cap of 49 instruments, matching the tested 45-second
publisher budget. A frozen universe of 50 or more instruments terminates as
`FROZEN_UNIVERSE_TOO_LARGE` before any external-read child or publisher Job is created. Do not raise
the cap as an availability workaround; a larger universe requires a new measured budget and reviewed
schema/queue contract.

Each 45-second max-attempt-one daily child owns an internal deadline no later than second 44. It
reserves the last 3 seconds for staging finalization and the preceding 6 seconds for an independent
raw archive; the Gateway fetch itself is capped at 10 seconds and therefore must start no later than
the derived acquisition call-start deadline. Preflight, pacing, authority revalidation and durable
reservation all consume that same acquisition budget. If a complete response exists when acquisition
or permit-exit fails, archive it first through the independent session, then terminalize with the
stable acquisition code. Exhausted acquisition, raw archive and finalization phases respectively use
`FOSUN_DAILY_ACQUISITION_DEADLINE_EXCEEDED`,
`FOSUN_DAILY_RAW_ARCHIVE_DEADLINE_EXCEEDED`, and
`FOSUN_DAILY_FINALIZE_DEADLINE_EXCEEDED`; no response at the derived boundary uses
`FOSUN_DAILY_PHASE_BUDGET_EXHAUSTED`. Operator cancellation still propagates.

Every normalized intraday minute bar, including an explicitly admitted Alpaca bar, must use the
exact raw transport SHA-256 as its revision. A different raw response with identical OHLCV must
produce a distinct normalized authority; the persistence boundary rejects any raw/revision
mismatch. Reusing `snapshot-v1` or another provider-generic revision is an admission failure.
Normalized intraday bars are create-only writes to their final digest key; database publication
starts only after every required object write succeeds. Raw archive and database-ingest phases are
also bounded by the handler-owned overall deadline and terminalize as
`INTRADAY_RAW_ARCHIVE_DEADLINE_EXCEEDED` or `INTRADAY_INGEST_DEADLINE_EXCEEDED` without relabeling
operator cancellation.

The 30-second intraday Job gives Fosun acquisition at most 24 seconds measured from handler entry,
including the initial database revalidation, and retains a five-second partial-evidence reserve plus
a one-second worker safety margin. If that acquisition deadline cancels the provider, every fully
received bars/quote response is encoded into one deterministic bounded partial envelope and committed
through an independent database session/transaction before the internal deadline. The Job then ends
as `skipped / INTRADAY_SNAPSHOT_READ_CANCELLED`; it does not normalize, publish, retry, or create a
second attempt. No completed response means no fabricated artifact. A guard/database failure after a
completed response follows the same archive path with
`INTRADAY_AUTHORITY_REVALIDATION_FAILED`. Archive failure or timeout remains fail closed. An operator
shutdown or worker-owned outer cancellation is propagated as cancellation and is not mislabeled as
the internally owned acquisition deadline.

The tracked environment template contains names and safe-disabled defaults only. Populating those
names, mounting certificates, changing permissions, starting the gateway against the real provider,
or executing a probe remains approval-gated. There is no automatic fallback to Alpaca or fixture.

## Closed operation set and request budget

The probe must cover exactly these eight metadata labels and no others:

| Artifact label | Provider capability | Maximum requests |
| --- | --- | ---: |
| `Session` | Create/query the bounded read-only session | 3 |
| `Accounts` | List readable accounts | 3 |
| `CashSummary` | Read cash summary contract | 3 |
| `Holdings` | Read holdings contract | 3 |
| `OrderList` | List existing orders, never mutate | 3 |
| `CashFlows` | List cash flows | 3 |
| `quote` | Minimal quote read | 3 |
| `kline_minimal` | One minimal K-line read | 3 |

The total hard cap is 24 requests and the capability artifact may be valid for at most 24 hours.
The six broker-domain labels from `Session` through `CashFlows` also have a shared hard cap of six
physical requests; `quote` plus `kline_minimal` have a shared rolling cap of 24/minute. Per-label,
domain and total caps are independently rechecked from the signed artifact during activation.
Any additional or duplicate operation fails the gate. Names containing order creation/submission/
cancellation/replacement, trade/simulation, subscription, or Webhook semantics reject the entire
artifact. On unexpected permission, contract, history, adjustment, pagination, rate-limit, TLS,
authentication, or response behavior, stop immediately and do not expand the request budget.

The admitted Engine/Gateway contract treats a daily `kline` `end_time` as inclusive. The
`kline_minimal` evidence must prove that a request ending on a known target trading date actually
contains that completed date. An exclusive or ambiguous SDK boundary fails admission; do not
silently add a following calendar day. Supporting an exclusive SDK later requires a versioned
adapter contract, explicit requested/returned-window lineage, and new boundary tests before another
probe can be admitted.

## Sanitized artifact

The probe runner serializes only canonical `nexus.fosun.sanitized-probe.v2` metadata:

- provider, capture/expiry timestamps, exact manifest SHA-256 and pinned SDK tag/commit;
- one record per allowed operation: label, request count, pass/fail, and normalized response-contract
  SHA-256;
- six booleans for read-only permission, history window, adjustment semantics, pagination,
  rate-limit behavior and response contract. This pilot can directly observe only the requested
  history-window match and response shape, so only those two are `true`; credential write denial,
  multi-page traversal, adjustment semantics and Provider rate-limit behavior remain `false`.
- the publication signer's public identity. This declaration is never trusted by itself;
  activation compares it with an independently change-controlled public-key pin.

The schema forbids extra fields. It has no place for API keys, key IDs, account IDs, holdings,
amounts, prices, symbols, raw/normalized response bodies, request bodies, headers, endpoints,
session IDs, error text, correlation values, or personal data. Store the artifact in the approved
audit object domain; store only its URI/hash/length and activation contract hash in authoritative
metadata. Do not commit a real artifact to this repository.

## Future one-shot probe procedure

This procedure is dormant until every precondition above is evidenced and the exact UTC window,
symbol and dates are attached to the approved probe record. The three secret-bearing
`FSOPENAPI_*` credential values must be injected by the approved secret manager into the one-shot
process; never type them into a command, store them in an env file, or place them in shell history.
The non-secret base URL comes from the admission record. The publisher seed stays in a separate
current-user `0600` regular, single-link file under a protected current-user `0700` directory and
trusted ancestors. It is exactly 43 bytes with no newline: canonical unpadded base64url that
decodes to a 32-byte Ed25519 seed. Its derived public key must match the independently approved
43-byte public pin. All paths below are absolute, and the probe-ready base URL must byte-for-byte
match the manifest.

Set only the non-secret, approval-record values locally (the sample dates are deliberately
placeholders and must not be reused blindly):

```bash
set -eu
probe_run_id='fosun-probe-yyyymmddthhmmssz'
probe_manifest='/approved/config/fosun-provider-manifest.json'
probe_output_dir="/approved/staging/${probe_run_id}"
probe_operator_dir="/approved/operator/${probe_run_id}"
probe_frame_basename="${probe_run_id}.frame.json"
probe_frame="${probe_operator_dir}/${probe_frame_basename}"
probe_symbol='usAAPL'
probe_session_date='YYYY-MM-DD'
probe_cash_from='YYYY-MM-DD'
probe_cash_to='YYYY-MM-DD'
probe_output_basename="${probe_run_id}.json"
probe_runtime='/approved/runtime/nexus-fosun-gateway/bin'
probe_capture_bin="${probe_runtime}/nexus-fosun-capture-read-only-probe"
probe_key_preflight_bin="${probe_runtime}/nexus-fosun-probe-publisher-key-preflight"
probe_receipt_bin="${probe_runtime}/nexus-fosun-publish-probe-receipt"

umask 077
mkdir -m 700 -- "$probe_output_dir" "$probe_operator_dir"
test -x "$probe_capture_bin"
test -x "$probe_key_preflight_bin"
test -x "$probe_receipt_bin"
export FOSUN_PROVIDER_MANIFEST="$probe_manifest"
export FOSUN_SDK_ADMITTED=true
export FOSUN_PROBE_OUTPUT_DIR="$probe_output_dir"
export FOSUN_PROBE_PUBLISHER_PRIVATE_KEY_FILE='/approved/secrets/probe-publisher.seed'
export FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U='<approved-non-secret-pin>'
export FOSUN_PROBE_HTTPS_PROXY_URL='http://127.0.0.1:9090'
export FSOPENAPI_BASE_URL='https://openapi.fosunxcz.com'
```

The local pilot accepts only that exact loopback HTTP proxy for HTTPS CONNECT traffic. Both SDK
sessions keep `trust_env=false`, override caller-supplied proxy values, retain end-to-end TLS
verification, and cannot use a remote or credential-bearing proxy URL. The proxy's observed public
egress must match the Provider allowlist immediately before the approved probe.

`probe_session_date` must be a completed Provider session strictly before the UTC start date. The
cash-flow range is inclusive and may span at most seven dates (`to - from <= 6 days`). The child
probe command has exactly eight named arguments and no generic method, URL, account or
output-directory input.

Immediately before the approved network window, validate the signer in a separate no-network
process. A failure consumes no Provider request at this step and is a stop; do not weaken the file
checks or replace the approved pin:

```bash
env -u PYTHONHOME -u PYTHONPATH -u SSLKEYLOGFILE "$probe_key_preflight_bin"
```

The capture wrapper opens the result frame create-only with `O_NOFOLLOW` in the protected operator
directory, then passes that held descriptor to the fixed eight-argument probe in the same process.
There is no child probe that can outlive the wrapper. It never
truncates an existing path and emits nothing to its own stdout or stderr:

```bash
set +e
env -u PYTHONHOME -u PYTHONPATH -u SSLKEYLOGFILE "$probe_capture_bin" \
  --frame-output-dir "$probe_operator_dir" \
  --frame-basename "$probe_frame_basename" \
  --manifest "$probe_manifest" \
  --symbol "$probe_symbol" \
  --session-date "$probe_session_date" \
  --cash-flow-date-from "$probe_cash_from" \
  --cash-flow-date-to "$probe_cash_to" \
  --limit 1 \
  --capability-ttl-seconds 3600 \
  --output-basename "$probe_output_basename"
probe_exit=$?
set -e
```

All three binaries must come from the already reviewed and preinstalled immutable runtime. Do not use a
package manager, dependency resolver, environment sync, download, or source checkout during the
approved network window. `mkdir` is intentionally create-only: if either run directory already
exists or either runtime binary is missing, stop and obtain a new run ID before any Provider call.
The scrubbed interpreter environment is part of the fixed launcher contract: `PYTHONHOME`,
`PYTHONPATH`, and `SSLKEYLOGFILE` must be absent before Python starts so an injected module cannot
shadow the admitted SDK and TLS session secrets cannot be exported.

The probe writes no routine stderr and exactly one compact result frame. Do not parse or
promote anything unless `probe_exit` is zero, the frame is one newline-terminated JSON object, its
status is `probe_published`, and the artifact hash and byte length match the create-only file.
Any partial or malformed frame, wrapper failure, or pre-existing frame name requires quarantine;
never repair or overwrite it. Publish the
detached receipt create-only into the same protected staging directory with the repository's
durable publisher rather than a shell redirection:

```bash
unset FSOPENAPI_API_KEY FSOPENAPI_CLIENT_PRIVATE_KEY FSOPENAPI_SERVER_PUBLIC_KEY
unset FOSUN_PROBE_PUBLISHER_PRIVATE_KEY_FILE FSOPENAPI_BASE_URL

if [ "$probe_exit" -ne 0 ]; then
  printf '%s\n' 'probe capture failed; quarantine this run and stop' >&2
  exit "$probe_exit"
fi

set +e
env -u PYTHONHOME -u PYTHONPATH -u SSLKEYLOGFILE "$probe_receipt_bin" \
  --capture-exit "$probe_exit" \
  --frame-dir "$probe_operator_dir" \
  --frame-basename "$probe_frame_basename" \
  --artifact-dir "$probe_output_dir" \
  --artifact-basename "$probe_output_basename"
receipt_exit=$?
set -e

if [ "$receipt_exit" -ne 0 ]; then
  printf '%s\n' 'receipt publication uncertain; quarantine this run and stop' >&2
  exit "$receipt_exit"
fi
```

The fixed receipt publisher independently requires the exact capture outcome `0`, a protected
single-link `0600` frame and artifact, their stable exact bytes, the fixed success-frame field
order, and matching digest/length/receipt subject. A nonzero capture result cannot be converted
into a receipt even if a complete-looking success frame or artifact exists. It emits only the two
recomputed hashes and publishes `publication-receipt.json` create-only.
Any nonzero receipt-publisher exit means the receipt may already be durable while its summary was
not delivered. Quarantine both run directories, do not retry the create-only publisher, do not
reconstruct a summary, and do not run activation for that run.

Never automatically retry after any physical request or reuse an output directory/name. Apply the
following fixed disposition:

| Result | Required disposition |
| --- | --- |
| `COMPLETE_SDK_ADMISSION`, `INVALID_PROBE_PLAN`, `PROBE_SETTINGS_INVALID`, `SDK_NOT_AVAILABLE` | No Provider request is expected; correct the admission/runtime/operator record before seeking a new window. The independent signer preflight must precede the probe because a signer race after requests maps conservatively to `PROBE_FAILED`, never this group. |
| `SDK_CONTRACT_MISMATCH`, `PROBE_ACCOUNT_UNAVAILABLE`, `PROBE_BUDGET_EXCEEDED`, `PROBE_RESPONSE_INVALID`, `PROBE_WINDOW_MISMATCH`, `PROBE_CAPABILITY_EXPIRED`, `PROBE_FAILED` | Treat the approved window as consumed regardless of observed counts. Stop, retain only sanitized audit metadata, review request counts, and obtain a new explicit window before any new probe. |
| `PROBE_INTERRUPTED` / exit 130 | Treat the run as consumed; verify both sessions closed and do not retry automatically. |
| `PROBE_OUTPUT_EXISTS` | Treat the window as consumed because artifact publication follows Provider reads. Preserve the existing file, investigate the run ID collision, and use a newly approved run ID. Never overwrite. |
| `PROBE_OUTPUT_INVALID`, `PROBE_OUTPUT_QUARANTINE_REQUIRED`, malformed/partial/absent frame plus exit 2 (operationally `PROBE_RECEIPT_DELIVERY_UNCERTAIN`), or capture-wrapper failure | Treat the window as consumed and isolate both staging and operator run directories. Do not reconstruct a frame or receipt, rerun the signer, activate, or promote any file. |

The one-shot runner verifies the exact manifest, artifact, detached receipt and publisher pin before
it can emit `probe_published`; its returned contract hash binds those bytes. Do not feed a
`probe_ready` artifact into the production activation command: continuous runtime deliberately
remains closed until a later, separately reviewed `admitted` manifest exists. The probe result
cannot change transport mode, permissions, deployment state, or secrets.

This pilot artifact is review evidence only and must never be promoted into the continuous Gateway.
It binds the exact `probe_ready` manifest SHA and deliberately records four unobserved checks as
`false`; changing the manifest to `admitted` also changes that SHA. Future sustained collection
therefore requires a separately designed evidence runner that actually exercises the missing
semantics, a fresh `admitted` manifest-bound artifact/receipt/hash, independent review, and explicit
deployment approval. None of that is part of this first probe.

## Activation and rollback

An `activated_read_only` result is necessary but not sufficient for deployment. Deployment and
secret/permission changes remain separately approval-gated. Keep live trading disabled, keep the
seven internal gateway routes POST-only and authenticated with mTLS plus HMAC, and retain the
deterministic risk/approval boundary. Never add a generic proxy or write route.

On any outage, stale capability, reconciliation mismatch, unexpected field, permission expansion,
trigger-topology drift, hash mismatch, either listener failing readiness, a client-CA fingerprint
change, or suspected `provider-worker` compromise: stop both the provider worker and gateway
workloads, disable/invalidate the integration, revoke provider credentials
through the secret manager, retain only sanitized audit metadata, and return the platform to
fixture research/paper-trading state. Do not substitute fixture facts into a live-provider view and
do not send a compensating broker operation.
