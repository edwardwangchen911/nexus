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

The reviewed candidate is tag `v1.2.0` at commit
`891d95fb4967efaa682387c1b35966ceea38fb21`. The repository has no standard license file,
so written permission for the intended use and distribution model remains unresolved.
The three public OpenAPI documents matched the SHA-256 values in
`contracts/providers/fosun/manifest.json` on 2026-08-22. A future mismatch blocks admission
and requires a reviewed manifest and fixture update; it never auto-upgrades this adapter.

## Required evidence before admission

- Attach written permission or an applicable license determination approved for this private
  deployment and its build/distribution process.
- Archive the reviewed source provenance and independently reproduce the candidate commit hash.
- Complete a security review of the SDK, dependencies, session lifecycle, cryptography, error
  handling, and network behavior. The frozen candidate currently calls its HTTP session with
  TLS verification disabled (`verify=False`); admission stays blocked until this is corrected
  upstream or an approved, tested remediation exists.
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
or unapproved SDK returns `SDK_NOT_ADMITTED`; it never falls back to fixtures. SDK construction
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
