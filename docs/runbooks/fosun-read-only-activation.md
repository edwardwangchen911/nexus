# Fosun read-only activation runbook

## Boundary and current result

Real-provider probing is not part of autonomous local implementation. This runbook may be used
only after the user explicitly approves one bounded probe and every prerequisite below is already
evidenced. It never authorizes an order, simulation-account mutation, subscription, Webhook, or
generic provider request.

This boundary treats `provider-worker` as the trusted market normalization/writer TCB. The gateway
isolates provider credentials, egress, and the fixed read-only operation surface; it does not attest
responses against a hostile provider worker. Immutable evidence means append-only after acceptance,
not that an already-compromised writer cannot fabricate an internally consistent fact graph.

The local decision command reads only the committed provider manifest and an optional sanitized
probe artifact. It has no provider transport, does not import the SDK, and does not read gateway
credentials:

```bash
uv run --package nexus-fosun-gateway nexus-fosun-activation-gate
```

With the current blocked manifest the only valid result is
`fixture_with_next_action / COMPLETE_SDK_ADMISSION`. `--require-activated` exits 2 for that result;
the ordinary command exits 0 after emitting the decision contract so Phase 0 offline verification
can remain independent from Phase 1 provider activation.

## Preconditions before requesting probe confirmation

Do not ask for or execute a real probe until all of these are independently evidenced:

1. The API key previously exposed in conversation has been revoked. No key value may be used as
   proof or copied into a command, document, log, test, shell history, or artifact.
2. A rotated API key, client private key, server public key, and provider TLS trust material exist
   only in the gateway's secret manager and are scoped read-only.
3. The fixed production egress IP has been added to the provider allowlist and the intended target
   environment is recorded.
4. `docs/runbooks/fosun-sdk-admission.md` has attached written permission, provider test vector,
   security review, TLS-verification remediation, and exact provider base URL for the pinned
   SDK/OpenAPI subject.
5. The manifest is reviewed to `admission.status: admitted`, and the runtime admission tests,
   offline gateway suite, route-surface policy and secret scan all pass.
6. A probe owner, UTC window, stop conditions and rollback owner are recorded. Only then request one
   explicit user confirmation for that exact network probe.
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
   sequence privilege, and any extra non-system `SECURITY DEFINER`. It also matches all 60
   non-internal application triggers on Provider-insertable tables, including enabled state,
   timing/events, function owner/configuration and function-body digest. RLS helpers use a
   `pg_catalog`-first fixed search path, and bypass policies are bound to the catalog
   `pg_has_role` OID. Advisory locks, not table `UPDATE`, serialize authority validation against
   status/artifact/manifest mutation. Every Provider-writable relation must remain a plain table
   (`relkind='r'`); every frozen trigger must have an empty `tgattr` (`UPDATE OF` is forbidden) and
   must not be a partition clone or inherited topology expansion.
9. Object-store admission records a Provider-only IAM principal distinct from the app/general
   worker principal, a pre-provisioned versioned bucket, and the fixed `provider/market` prefix.
   Effective-policy evidence must deny bucket-root/list access and every other prefix; allow
   Put/Get only below `provider/market`; and allow Delete only below
   `provider/market/temporary/market`. Missing, broader, inherited, or unreviewed access is a stop.
10. Pre-provision the external `nexus-fosun-gateway-replay-v1` volume on the one supported gateway
    host. It must be mounted at `/var/lib/nexus-fosun-gateway`, writable only by the gateway user,
    durable across container replacement, and support cross-process POSIX advisory locks. Never
    mount a different replay volume into an overlapping replacement. Loss, recreation, or snapshot
    rollback of this volume is a security event: stop both workers and investigate before resuming.
    Every Gateway process start independently enforces a new 61-second drain/readiness gate; do not
    delete the volume during ordinary Compose cleanup.

Missing evidence is a stop, not permission to reduce checks or fall back to unreviewed data.
The configured provider base URL must be an absolute HTTPS URL without embedded credentials,
query, fragment, or redirect dependence; the admitted adapter forces certificate verification and
rejects redirects on both frozen-SDK HTTP surfaces.

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
  Its deployment and runtime setting are both fixed to exactly one replica. A durable SQLite nonce
  journal survives normal restart; an exclusive lease on the same named volume rejects a second
  process. Every production process start drains authentication for 61 seconds, including ordinary
  restart and replacement, so a restored older volume snapshot cannot become immediately usable.
  A persisted clock high-water mark keeps authentication and readiness closed while local wall time
  is behind observed time. Compose health additionally requires the lifespan owner PID and actual
  Gateway TCP listener, so both workers wait. Future timestamps are rejected.
  External orchestration is unsupported unless every candidate mounts the same POSIX-lock-capable
  journal volume; rolling overlap then fails closed because only one process can hold the lease;
- Web, API, the general worker and Agent receive neither client identity. Gateway route
  authorization derives the principal from the signed key ID; an unsigned role claim cannot expand
  operations.

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
Any additional or duplicate operation fails the gate. Names containing order creation/submission/
cancellation/replacement, trade/simulation, subscription, or Webhook semantics reject the entire
artifact. On unexpected permission, contract, history, adjustment, pagination, rate-limit, TLS,
authentication, or response behavior, stop immediately and do not expand the request budget.

## Sanitized artifact

The probe runner must serialize only `nexus.fosun.sanitized-probe.v1` metadata:

- provider, capture/expiry timestamps, exact manifest SHA-256 and pinned SDK tag/commit;
- one record per allowed operation: label, request count, pass/fail, and normalized response-contract
  SHA-256;
- six booleans for read-only permission, history window, adjustment semantics, pagination,
  rate-limit behavior and response contract.

The schema forbids extra fields. It has no place for API keys, key IDs, account IDs, holdings,
amounts, prices, symbols, raw/normalized response bodies, request bodies, headers, endpoints,
session IDs, error text, correlation values, or personal data. Store the artifact in the approved
audit object domain; store only its URI/hash/length and activation contract hash in authoritative
metadata. Do not commit a real artifact to this repository.

After a separately approved probe, evaluate the artifact locally:

```bash
uv run --package nexus-fosun-gateway nexus-fosun-activation-gate \
  --probe-artifact /approved/sanitized/fosun-probe.json \
  --require-activated
```

`contract_hash` binds the result schema version, exact provider-manifest bytes, SDK subject and,
when present, exact sanitized-artifact bytes. Only an admitted manifest plus an unexpired,
subject-matched, exact closed operation set with every check passing returns
`activated_read_only`. Every other path returns `fixture_with_next_action` with a stable action.
The result itself cannot change transport mode, permissions, deployment state, or secrets.

For a later separately approved deployment, mount the exact artifact read-only into the gateway
secret/config domain, set its in-container path as `FOSUN_ACTIVATION_PROBE_ARTIFACT`, and set the
separately recorded 64-character result as `FOSUN_ACTIVATION_CONTRACT_HASH`. Neither variable is
available to the API, research worker, or broker worker. Gateway startup recomputes and compares the
contract before SDK import; the SDK adapter checks the artifact expiry at operation entry and again
after queue/rate-limit waits before every Provider attempt, so a long-running process fails closed
when the capability window ends. Re-probing, mounting, changing these environment values, or
restarting/deploying the gateway remains approval-gated.

## Activation and rollback

An `activated_read_only` result is necessary but not sufficient for deployment. Deployment and
secret/permission changes remain separately approval-gated. Keep live trading disabled, keep the
seven internal gateway routes POST-only and authenticated with mTLS plus HMAC, and retain the
deterministic risk/approval boundary. Never add a generic proxy or write route.

On any outage, stale capability, reconciliation mismatch, unexpected field, permission expansion,
trigger-topology drift, hash mismatch, or suspected `provider-worker` compromise: stop both the provider worker and gateway
workloads, disable/invalidate the integration, revoke provider credentials
through the secret manager, retain only sanitized audit metadata, and return the platform to
fixture research/paper-trading state. Do not substitute fixture facts into a live-provider view and
do not send a compensating broker operation.
