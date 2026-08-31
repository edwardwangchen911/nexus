# Fosun SDK admission runbook

## Current ruling

Admission is **BLOCKED**. The gateway supports offline `fixture` mode and contains a
production-code `read_only` seam, but `fsopenapi` is intentionally absent from `uv.lock`.
Do not call a Fosun endpoint, enable provider credentials, or set `FOSUN_SDK_ADMITTED=true`
until every gate below has signed evidence.
The committed manifest has machine-readable `status: blocked` and null evidence references;
the runtime boolean alone cannot override it. The gateway image carries that exact record and
admits only when its SDK/OpenAPI subject matches the frozen constants and all written-permission,
provider-test-vector, security-review, and TLS-remediation references are populated.
`admission.provider_base_url` is also `null`; admission must bind the exact reviewed HTTPS base URL
before a credential can be loaded, and runtime configuration must match it byte-for-byte.

The reviewed candidate is tag [`v1.2.0`](https://github.com/fosunwealth/openapi-python-sdk/tree/891d95fb4967efaa682387c1b35966ceea38fb21)
at commit `891d95fb4967efaa682387c1b35966ceea38fb21`. A separate public-source refresh on
2026-08-27 found `main` still at
[`7f0fb48f3e1ce04329b990cf16f391cf59078137`](https://github.com/fosunwealth/openapi-python-sdk/tree/7f0fb48f3e1ce04329b990cf16f391cf59078137).
`main` is comparison evidence only; it is neither the admitted subject nor an auto-upgrade target.

Both reviewed trees contain `setup.py` but no `LICENSE`, `COPYING`, or `NOTICE` file, and
`setup.py` declares no license metadata/classifier. Therefore the committed manifest must retain
`license: UNRESOLVED`; written permission for the intended private use and build/distribution model
remains a separate required artifact.

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
`FSOPENAPI_BASE_URL` must be an absolute credential-free HTTPS URL with no query/fragment; any
surface drift blocks SDK construction before a network call, and repository policy prevents either
wrapper from being removed. This code and its two-surface tests still need
to be cited by the independent security-review evidence before the manifest's
`tls_verification_remediation` field may be populated. The SDK stays out of `uv.lock` until that
review and every other admission gate pass.

The three public OpenAPI documents matched the SHA-256 values in
`contracts/providers/fosun/manifest.json` on 2026-08-22. A future mismatch blocks admission
and requires a reviewed manifest and fixture update; it never auto-upgrades this adapter.

## Required evidence before admission

- Attach written permission or an applicable license determination approved for this private
  deployment and its build/distribution process.
- Archive the reviewed source provenance and independently reproduce the candidate commit hash.
- Record the exact credential-free HTTPS provider base URL in `admission.provider_base_url`; it
  must have no query, fragment, redirect dependency, ambiguous port, or trailing slash.
- Complete a security review of the SDK, dependencies, session lifecycle, cryptography, error
  handling, and network behavior. The frozen candidate disables TLS verification in both session
  creation and ordinary requests; admission stays blocked until the review approves the tested
  gateway-owned two-surface remediation or an exact upstream replacement.
- Approve a sanitized, provider-confirmed test vector for authentication/session creation,
  account and portfolio reads, pagination, price `power`, permission errors, rate limiting, and
  malformed responses. No real account response may become a fixture.
- Rotate any previously exposed API credential, provision only read-only permissions, confirm
  the fixed production egress allowlist, and store all provider and TLS material in the deployment
  secret manager. Never place values in this repository, logs, commands, or tickets.
- Re-run the public OpenAPI hashes and compare them byte-for-byte with the provider manifest.
- Review the exact dependency diff, then add only
  `fsopenapi @ git+https://github.com/fosunwealth/openapi-python-sdk.git@891d95fb4967efaa682387c1b35966ceea38fb21`
  to the gateway package and regenerate `uv.lock`. A branch or tag name alone is not sufficient.
- Run the full gateway unit, contract, security, mTLS, static-analysis, lock, Compose, route-surface,
  and secret-material gates before recording admission approval.

## Runtime policy after a future approval

Production must use `FOSUN_GATEWAY_TRANSPORT_MODE=read_only`, `READ_ONLY` capability, the
gateway-local current/previous HMAC identity, and a server certificate/key plus the engine-client
CA. Uvicorn uses `CERT_REQUIRED`. The engine-side client certificate/key/CA are separate variables
and files; neither TLS identity nor HMAC keys may be reused from the browser-to-engine BFF domain.

The composition root imports `fsopenapi` only after admission and configuration checks. A missing
or unapproved SDK returns `SDK_NOT_ADMITTED`; it never falls back to fixtures. `admitted` also
requires `sdk.license` to contain the reviewed license/permission determination rather than
`UNRESOLVED`. Runtime startup additionally reads the bounded sanitized artifact and detached
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
