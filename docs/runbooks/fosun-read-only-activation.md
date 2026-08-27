# Fosun read-only activation runbook

## Boundary and current result

Real-provider probing is not part of autonomous local implementation. This runbook may be used
only after the user explicitly approves one bounded probe and every prerequisite below is already
evidenced. It never authorizes an order, simulation-account mutation, subscription, Webhook, or
generic provider request.

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

Missing evidence is a stop, not permission to reduce checks or fall back to unreviewed data.
The configured provider base URL must be an absolute HTTPS URL without embedded credentials,
query, fragment, or redirect dependence; the admitted adapter forces certificate verification and
rejects redirects on both frozen-SDK HTTP surfaces.

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
or hash mismatch: stop the gateway workload, disable the integration, revoke provider credentials
through the secret manager, retain only sanitized audit metadata, and return the platform to
fixture research/paper-trading state. Do not substitute fixture facts into a live-provider view and
do not send a compensating broker operation.
