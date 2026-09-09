# Fosun SDK admission runbook

## Current ruling

The SDK and REST contract are **`probe_ready` for one private, local, read-only probe**. This is not
a production admission. It does
not activate credentials, call Fosun, start continuous collection, enable push, deploy a service,
or authorize an order. The runtime boolean alone cannot override the manifest and probe evidence.
Long-running Provider Worker and production Gateway activation remain separate later gates.
The reviewed production REST base URL is now bound as
`https://openapi.fosunxcz.com`; runtime configuration must match it byte-for-byte.
The SDK source also identifies the production push endpoint as
`wss://openapi-push.fosunxcz.com/msg/v1/push`, but push is outside the initial
pull-only activation and does not block it.

NEXUS now also contains a Gateway-owned offline-tested bounded collector for one explicit `qt`
subscription. It fixes the reviewed endpoint and 15-second application heartbeat, accepts one
strict US or HK code, never reconnects, and writes a private size/time/frame-bounded raw archive
with a consistency-checked receipt. This is reusable implementation evidence only: it has no
credential-loading CLI or service route, is not wired to Provider Worker, database, API, or Web,
and no real WebSocket connection may be started under the current REST-only admission. A real
probe requires separate evidence that the provider-approved private use covers market push; a
successful connection would prove only that bounded probe, not continuous coverage or runtime
admission.

The reviewed candidate is tag [`v1.2.0`](https://github.com/fosunwealth/openapi-python-sdk/tree/891d95fb4967efaa682387c1b35966ceea38fb21)
at commit `891d95fb4967efaa682387c1b35966ceea38fb21`. A separate public-source refresh on
2026-08-27 found `main` still at
[`7f0fb48f3e1ce04329b990cf16f391cf59078137`](https://github.com/fosunwealth/openapi-python-sdk/tree/7f0fb48f3e1ce04329b990cf16f391cf59078137).
`main` is comparison evidence only; it is neither the admitted subject nor an auto-upgrade target.

## Private-use basis

Both reviewed SDK trees contain `setup.py` but no standard `LICENSE`, `COPYING`, or `NOTICE` file.
The user confirmed on 2026-08-31 that Fosun OpenAPI access was approved, and Fosun's official
[`fw-trading`](https://github.com/fosunwealth/fw-trading/tree/157c52fda8387c2ae658bb2010060df75d269f07)
repository at commit `157c52fda8387c2ae658bb2010060df75d269f07`
instructs approved users to install `fsopenapi` and use the production REST base URL. Together these
form the recorded basis for private, single-user REST read-only use in NEXUS. The manifest therefore
records `PROVIDER-AUTHORIZED-PRIVATE-USE`; this is not an open-source license, does not authorize
redistribution, and does not cover push or order execution. A conflicting provider term stops use.

The frozen candidate still calls `requests.Session.request(..., verify=False)` in
[`fsopenapi/client.py`](https://github.com/fosunwealth/openapi-python-sdk/blob/891d95fb4967efaa682387c1b35966ceea38fb21/fsopenapi/client.py#L154-L158).
Its session handshake separately calls module-level `requests.post(..., verify=False)` in
[`fsopenapi/auth.py`](https://github.com/fosunwealth/openapi-python-sdk/blob/891d95fb4967efaa682387c1b35966ceea38fb21/fsopenapi/auth.py#L163-L169).
The refreshed `main` has the same behavior in
[`fsopenapi/client.py`](https://github.com/fosunwealth/openapi-python-sdk/blob/7f0fb48f3e1ce04329b990cf16f391cf59078137/fsopenapi/client.py#L161-L165).
No current-main evidence resolves the TLS gate. NEXUS now has a gateway-owned candidate remediation
that wraps both exact v1.2.0 HTTP surfaces and unconditionally replaces `verify=False` with
`verify=True` while disabling redirects so provider headers cannot cross to a redirected host.
Both wrappers also force streamed reads and materialize at most 4 MiB before the frozen SDK can
call `response.json()`; an oversized declared or chunked response is closed and rejected without
JSON parsing.
`FSOPENAPI_BASE_URL` must be exactly `https://openapi.fosunxcz.com`; host, case, port, path, query,
fragment or trailing-slash drift blocks SDK construction before a network call. Repository policy prevents either
wrapper from being removed. The two-surface tests and the independent local operational review are
the recorded TLS/security evidence for the bounded probe.

The three public OpenAPI documents matched the SHA-256 values in
`contracts/providers/fosun/manifest.json` on 2026-08-31 after refreshing the changed Trade
contract. A future mismatch blocks admission
and requires a reviewed manifest and fixture update; it never auto-upgrades this adapter.

## Recorded probe admission evidence

- Private-use basis: the user-confirmed approval and provider-published setup instructions above.
- Reviewed source provenance: exact SDK tag and commit above.
- Reconfirm that the runtime REST base URL exactly matches the reviewed
  `https://openapi.fosunxcz.com` value; it must have no query, fragment, redirect dependency,
  ambiguous port, or trailing slash.
- Security/TLS evidence: the reviewed gateway wrapper forces verification, disables redirects,
  bounds time and response size, and has dedicated tests for both SDK HTTP surfaces.
- Probe vector: official REST schemas/examples plus the synthetic success, empty, pagination,
  permission, rate-limit and malformed-response fixtures. No real account response becomes a
  fixture, and the first probe must not be described as proving unexercised Provider edge cases.
- Rotate any previously exposed API credential, provision only read-only permissions, confirm
  the fixed production egress allowlist, and store all provider and TLS material in the deployment
  secret manager. Never place values in this repository, logs, commands, or tickets.
- Re-run the public OpenAPI hashes and compare them byte-for-byte with the provider manifest.
- Lock only
  `fsopenapi @ git+https://github.com/fosunwealth/openapi-python-sdk.git@891d95fb4967efaa682387c1b35966ceea38fb21`
  in the gateway package. A branch or tag name alone is not sufficient.
- Run the gateway unit/contract/security tests, static analysis, dependency policy and secret scan
  before requesting the single real probe. Compose and long-running runtime gates are deferred.

## Runtime policy after a future continuous-runtime approval

Production must use `FOSUN_GATEWAY_TRANSPORT_MODE=read_only`, `READ_ONLY` capability, the
gateway-local current/previous HMAC identity, and a server certificate/key plus the engine-client
CA. Uvicorn uses `CERT_REQUIRED`. The engine-side client certificate/key/CA are separate variables
and files; neither TLS identity nor HMAC keys may be reused from the browser-to-engine BFF domain.

The composition root imports `fsopenapi` only after a future production admission and configuration
checks. The committed `probe_ready` manifest cannot start that runtime. A missing or unapproved SDK
returns `SDK_NOT_ADMITTED`; it never falls back to fixtures. Production `admitted` also requires the
same exact private-use basis rather than an arbitrary string. Runtime startup additionally reads the bounded sanitized artifact and detached
publication receipt at `FOSUN_ACTIVATION_PROBE_ARTIFACT` and
`FOSUN_ACTIVATION_PROBE_RECEIPT`, verifies the receipt only against the independently managed
canonical 43-byte file at `FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U_FILE`, recomputes the
activation decision, and compares it in constant time with the exact 64-byte lowercase hex file at
`FOSUN_ACTIVATION_CONTRACT_HASH_FILE` before importing the SDK.
Manifest, artifact, and receipt are descriptor-first, no-follow, protected regular files. A
missing, changed, expired, noncanonical, signer-mismatched, or subject-mismatched bundle blocks
startup, and the adapter rechecks expiry after any queue,
rate-limit or retry wait immediately before every Provider attempt.
Compose fixes the four container targets: artifact and receipt are independent read-only binds with
`create_host_path: false`; publisher key and contract hash are separate file-backed configs. Raw
pin/hash environment values are not part of the Gateway settings contract. The probe process never
receives the approved hash config or promoted evidence paths.
SDK construction
forces `logging_enable=false` and `log_body=false`. Gateway logs allow only operation name, attempt,
outcome, and safe timing/correlation fields—never raw bodies, headers, endpoint strings, external
account IDs, or secrets.

The compiled surface remains seven authenticated POST operations below
`/internal/v1/read-only/`: capabilities, account sync, portfolio sync, order listing, cash-flow
listing, quotes, and bars. There is no generic URL/method proxy, order mutation, simulation-account
mutation, subscription CRUD, or Webhook handler.

## Rollback

Disable the integration and stop the gateway workload. Do not switch a production deployment to
fixture data and do not continue with stale provider facts. Preserve sanitized audit metadata,
revoke the gateway/provider credentials through the secret manager, and investigate before a new
admission decision. No rollback step writes to a broker account.
