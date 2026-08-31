# NEXUS Fosun Sanitized Probe Runner Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILLS: use `superpowers:test-driven-development` for every
> behavior change and `superpowers:verification-before-completion` before any completion claim.

**Goal:** Add an offline-verifiable, one-shot Fosun read-only probe runner that can later produce a
strictly bounded, value-free activation artifact and detached publication receipt, while preserving
all current SDK, contract, credential, deployment, and trading stops.

**Architecture:** Admission is evaluated from one immutable manifest byte string before settings,
secrets, or SDK import. A probe-only factory installs zero-retry HTTPS adapters plus hooks at the two
reviewed HTTP surfaces, so authentication and every business attempt are counted before dispatch.
The fixed executor discards normalized values and emits only static contract hashes. A trusted-dirfd
publisher writes a v2 artifact create-only; only after complete fsync does an explicitly provisioned,
public-pin-bound Ed25519 key sign a detached receipt required by both activation evaluation and
production composition. The artifact-declared key is never a trust root.

**Tech Stack:** Python 3.12, Pydantic 2, cryptography Ed25519, asyncio/thread-safe budgets, pytest,
Ruff, Mypy, existing Fosun Gateway adapters and offline socket guard.

**Spec:** `docs/superpowers/specs/2026-08-31-nexus-fosun-sanitized-probe-runner-design.md`

## Global constraints

- Do not modify `contracts/providers/fosun/manifest.json`, frozen OpenAPI hashes, `uv.lock`, or add
  `fsopenapi`; current admission remains blocked.
- Do not read or print credential values, old key files, account data, provider responses, or full
  environment variables. Tests use synthetic canaries only.
- Do not make a real network/provider/account call, start the production Gateway, create runtime
  infrastructure, deploy, change permissions, enable Alpaca, or submit any order.
- The root agent is the sole writer in
  `.worktrees/nexus-fosun-probe-runner`; review agents are read-only.
- Each behavior starts with a focused failing test, the expected failure is observed, then the
  minimum implementation is added. Existing tests must remain green after every task.
- Commit each coherent task locally. No push or external activation.

---

## Task 1: Version and bind activation to a detached publication receipt

**Files:**

- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/activation.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/settings.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/main.py`
- Modify: `integrations/fosun_gateway/tests/unit/test_activation.py`
- Modify: `integrations/fosun_gateway/tests/unit/test_composition.py`
- Modify: `integrations/fosun_gateway/tests/test_health.py`

1. RED: update activation fixtures to v2 and require a 43-character unpadded base64url Ed25519
   public key; prove old v1, malformed key bytes, and extra fields are rejected.
2. RED: add `ProbePublicationReceipt` tests for the exact v1 schema, 64-byte signature, domain-
   separated signing payload, missing receipt, wrong artifact digest/key/signature, cross-artifact
   replay, and expired/future evidence.
3. RED: prove the activation contract hash changes with receipt bytes and the independently supplied
   publisher public-key pin, and that
   `evaluate_activation_paths` reads the receipt with a 16-KiB cap.
4. GREEN: implement the v2 artifact and strict receipt models, base64url decoding, Ed25519 verify,
   receipt/pin-bound contract hash, `PROVIDE_PROBE_PUBLICATION_RECEIPT`,
   `PROVIDE_TRUSTED_PROBE_PUBLISHER`, and the corresponding review actions.
5. RED/GREEN: add absolute `FOSUN_ACTIVATION_PROBE_RECEIPT`,
   `FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U_FILE`, and
   `FOSUN_ACTIVATION_CONTRACT_HASH_FILE` to production settings. Require exact protected file bytes
   with the artifact, bind all values to one immutable manifest read, and pass the independent tuple
   into composition before SDK import. Missing/mismatched receipt, pin, or hash must remain
   `SDK_NOT_ADMITTED` with no import.
6. Run focused activation/composition/health tests, Ruff, and Mypy.
7. Commit: `feat(fosun): bind activation to publication receipt`.

## Task 2: Instrument the actual reviewed HTTP surfaces

**Files:**

- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/security/provider_tls.py`
- Modify: `integrations/fosun_gateway/tests/security/test_provider_tls.py`
- Modify: composition fakes only where the default API contract requires it

1. RED: add a probe-mode observer fake and prove both auth `post` and business `request` call
   `before_http_attempt` before the delegate and the bounded 2xx result hook after complete read.
2. RED: install retrying fake HTTPS adapters and prove probe mode replaces them with adapters whose
   total/connect/read/redirect/status/other retry dimensions are all zero before any delegate call.
3. RED: prove a hook refusal yields zero delegate calls; hook exceptions and status/body canaries do
   not reach stdout, stderr, logs, or exceptions rendered by the CLI layer.
4. GREEN: add optional probe-only zero-retry enforcement and pre/post hooks while preserving the
   existing default production wrapper behavior. Return closable controls only when probe mode
   verifies both sessions.
5. RED/GREEN: prove `close()` closes client and isolated auth sessions idempotently and that missing
   adapter/hook/close surfaces fail before network.
6. Run the complete provider TLS and composition regression tests, Ruff, and Mypy.
7. Commit: `fix(fosun): count probe requests at http edge`.

## Task 3: Build settings, physical budgets, and the fixed SDK executor

**Files:**

- Create: `integrations/fosun_gateway/src/nexus_fosun_gateway/probe_settings.py`
- Create: `integrations/fosun_gateway/src/nexus_fosun_gateway/adapter/probe_sdk.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/admission.py`
- Create: `integrations/fosun_gateway/tests/unit/test_probe_settings.py`
- Create: `integrations/fosun_gateway/tests/unit/test_probe_sdk.py`
- Create: `integrations/fosun_gateway/tests/security/test_probe_sdk_secrecy.py`

1. RED: prove a blocked/stale/malformed manifest exits before settings construction, secret lookup,
   SDK import, adapter construction, or network spy.
2. GREEN: expose a read-only admitted-subject parser from the same in-memory document and add strict
   `ProbeProviderSettings`; require `FOSUN_SDK_ADMITTED=true`, exact path spelling/base URL, all
   provider materials, and trusted absolute output directory configuration.
3. RED: test `PhysicalHttpBudget` with a deterministic clock/sleeper: one QPS, rolling total 24,
   auth+broker 6, market 24, per-label 3, auth only during Accounts, no overlapping labels, no
   request after refusal, and thread-safe attribution.
4. GREEN: implement the budget and its assertion-only SDK call-edge limiter. Count Session only at
   the auth proxy; record one bounded 2xx success and every failed physical attempt.
5. RED: with a fake lazily imported SDK, prove `build_probe_transport` fixes SDK construction flags,
   installs both TLS hooks before methods are reachable, has no fallback, and cannot be imported by
   production `main.py`.
6. RED: test the exact sequential Accounts/CashSummary/Holdings/OrderList/CashFlows/quote/
   kline_minimal calls, in-memory-only account ID, first-error stop, zero accounts, auth outside
   Accounts, multiple/no 2xx auth, malformed pages, wrong provider day identity, retries, timeout,
   poison, and cancellation/close.
7. GREEN: implement `SdkProbeExecutor`; return only value-free counts/check booleans, never a
   `GatewayPage`, item, request ID, account ID, symbol, price, or exception text.
8. Run focused unit/security tests, existing SDK adapter regressions, Ruff, and Mypy.
9. Commit: `feat(fosun): add bounded read-only probe executor`.

## Task 4: Generate the static v2 artifact and detached receipt

**Files:**

- Create: `integrations/fosun_gateway/src/nexus_fosun_gateway/probe.py`
- Create: `integrations/fosun_gateway/tests/unit/test_probe.py`
- Create or extend: `integrations/fosun_gateway/tests/security/test_probe_secrecy.py`

1. RED: add strict `ProbePlan` tests for canonical US symbol, provider session date before start,
   cash-flow date range <=7 days, limit 1-10, TTL 60-86400, and safe output basename. Reject URLs,
   keys, account IDs, methods, arbitrary paths, and unknown fields.
2. RED: freeze one hand-written canonical response-contract declaration and expected SHA-256 per
   operation. Prove Pydantic metadata/defaults and live cardinality/value mutations cannot alter it.
3. GREEN: build `SanitizedProbeArtifact` only from manifest subject, static digests, counted attempts,
   six deterministic checks, completion clock, and the explicit publisher signer's public key.
4. RED/GREEN: implement the contract-only validator and receipt signer; prove signing material is
   exact, the private key never serializes, receipt verification uses an independently supplied pin,
   direct self-signing cannot activate, and signing never occurs before publisher fsync success.
5. RED: plant unique canaries in response values, account/symbol/URL/request ID, Pydantic metadata,
   exceptions, environment values, and output basename; assert none occur in artifact bytes, receipt,
   stdout/stderr/caplog/traceback.
6. Run focused tests, activation regressions, Ruff, and Mypy.
7. Commit: `feat(fosun): generate value-free probe evidence`.

## Task 5: Add the fail-closed CLI and trusted-dirfd publisher

**Files:**

- Create: `integrations/fosun_gateway/src/nexus_fosun_gateway/probe_cli.py`
- Create: `integrations/fosun_gateway/src/nexus_fosun_gateway/publication.py`
- Modify: `integrations/fosun_gateway/pyproject.toml`
- Create: `integrations/fosun_gateway/tests/unit/test_probe_cli.py`
- Create: `integrations/fosun_gateway/tests/security/test_probe_publication.py`

1. RED: prove the manifest path is length-bounded, opened once with terminal no-follow, read <=1 MiB,
   retained as immutable bytes, and never re-opened after admission. Invalid admission emits only the
   fixed exit-2 result and never constructs settings/imports SDK.
2. RED: test the custom parser never prints usage or operator values. All ordinary failures emit one
   fixed compact JSON frame; stderr is empty; interruption closes both sessions, starts no later
   operation, emits the fixed interruption frame, and exits 130.
3. RED: test parent traversal with held `O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC` fds, exact final owner/
   `0700`, temp `0600` regular current-UID inode, same-dirfd hard-link create-only publication, file
   and directory fsync order, and no absolute-path cleanup.
4. RED: adversarial tests cover existing final, symlink at each component/final, group/world-writable
   parent, parent rename, unsupported link flags, partial write, file fsync, link, both directory
   fsyncs, temp unlink, cleanup failure, and quarantine code. An unsigned residual must fail
   activation even when its artifact JSON is otherwise valid.
5. GREEN: implement publisher and CLI orchestration; add `nexus-fosun-read-only-probe` entry point.
   Never add the SDK dependency and never expose a generic request method.
6. RED/GREEN: use one EINTR-aware low-level stdout write loop with the receipt last. Inject short
   writes before/inside/after receipt and prove nonzero exit, no second frame, and no cross-artifact
   capability; a fully recoverable receipt can validate only the already-fsynced exact artifact.
7. Run CLI tests under the offline socket guard plus the complete Gateway suite, Ruff, and Mypy.
8. Commit: `feat(fosun): add sanitized probe command`.

## Task 6: Update runbooks, complete acceptance, review, and locally integrate

**Files:**

- Modify: `docs/runbooks/fosun-sdk-admission.md`
- Modify: `docs/runbooks/fosun-read-only-activation.md`
- Modify: `docs/adr/0002-fosun-gateway-boundary.md` only if the new receipt boundary is absent
- Modify: `scripts/ci/repository_policy.py` and policy tests only if needed to freeze the new
  probe-only exception/receipt requirement

1. Document v1 rejection, v2 artifact/receipt pairing, zero-retry physical counting, trusted output
   directory, fixed error codes, and the exact future operator sequence. Keep the current result
   `COMPLETE_SDK_ADMISSION` and state that a CLI framework is not external readiness.
2. Re-run the current public-contract drift check without modifying the manifest; record the current
   Trade mismatch as an activation blocker. Do not fetch or install the SDK dependency.
3. Run fresh acceptance:

   ```bash
   NEXUS_OFFLINE_SOCKET_GUARD=1 PYTHONPATH="$PWD/scripts/offline_guard" \
     uv run pytest integrations/fosun_gateway/tests -q
   uv run ruff check integrations/fosun_gateway/src integrations/fosun_gateway/tests
   uv run mypy integrations/fosun_gateway/src
   uv run pytest tests/policy -q
   uv run python scripts/ci/repository_policy.py \
     --root . --inventory-output /tmp/nexus-fosun-probe-inventory.json
   uv run --package nexus-fosun-gateway nexus-fosun-activation-gate --require-activated
   ```

   The last command must exit 2 with `COMPLETE_SDK_ADMISSION`; no real request is permitted.
4. Verify `fsopenapi` remains absent from `uv.lock`, the provider manifest is byte-identical to base,
   no credential signature exists in the branch diff, and the worktree is clean after commits.
5. Dispatch independent code and security reviewers over the exact branch diff; fix every P0-P2 or
   actionable in-scope finding with a new failing test, rerun affected/full gates, and re-review.
6. Commit documentation/acceptance changes. Fast-forward local `main` only if it still equals the
   recorded base `2d6face`; otherwise stop before merge and report the concurrent change. Never push,
   deploy, configure a real secret, or run the probe.

## Completion boundary

Local completion means the safe runner exists, all offline gates and independent reviews pass, the
blocked activation result is freshly demonstrated, and local `main` is integrated without changing
external state. Real Fosun activation remains incomplete until written SDK permission/license,
provider test vector, security/TLS evidence, exact production base URL, reviewed current OpenAPI
hashes, pinned SDK lock admission, rotated read-only credentials, fixed egress allowlist, and the
runtime infrastructure are all present. Only then may a separately recorded, timed <=24-request
read-only network probe be scheduled. No real order is authorized by this plan.
