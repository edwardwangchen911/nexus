# NEXUS Fosun Read-Only Scope Contract Design

**Status:** Approved architecture specification. It authorizes only local implementation and
offline verification. It does not authorize a Provider request, SDK admission, credential access,
deployment, account/order/subscription writes, or any live-trading change.

## Decision

Replace the current all-or-nothing `nexus.fosun.sanitized-probe.v2` evidence contract with two
non-substitutable, operation-scoped contracts:

| Scope | Contract version | Final authorized operations | User-facing purpose |
| --- | --- | --- | --- |
| `core` | `nexus.fosun.read-only-scope.v1.core` | `fosun_sync_accounts`, `fosun_sync_portfolio` (`cash`, `holdings`), `fosun_get_quotes`, `fosun_get_bars` | Account discovery, cash, holdings, quote, and one daily K-line identity |
| `extended` | `nexus.fosun.read-only-scope.v1.extended` | `fosun_list_orders`, `fosun_list_cash_flows` | Order-list and cash-flow read-only evidence |

`Session` and `Accounts` are physical/precondition operations, never implicitly claimed as
`extended` capability.  The runner records three distinct sets in every scope artifact:

1. `physical_operation_set`: every labelled dispatch actually attributed by the physical HTTP
   hooks, including `Session`.
2. `prerequisite_operation_set`: operations needed to authenticate or discover the exact
   in-memory account binding before a final operation.  `extended` is exactly
   `("Session", "Accounts")`.
3. `authorized_operation_set`: only the final Gateway operations granted to runtime.  It is
   exactly the closed row in the table above and excludes prerequisites.

The scope is not a UI convenience and is never inferred from a subset, superset, order, request
count, filename, or success frame.  A valid `core` artifact cannot authorize `OrderList` or
`CashFlows`; an `extended` artifact cannot authorize `core`; old eight-operation evidence cannot
silently become either one.  This is deliberately a contract split, not merely a reordered probe.

## Current facts and problem boundary

The existing runner has one closed order:
`Session -> Accounts -> CashSummary -> Holdings -> OrderList -> CashFlows -> quote -> kline_minimal`.
Its artifact schema is v2, its admission evaluator requires the exact complete sequence, its
operation allowlist has the seven Gateway operations, and the SDK executor retains the first
normalized account identifier only in memory.  The fourth one-shot run
`fosun-probe-20260901t03111788232261z` used `max_attempts=1`, stopped at `OrderList`, reported
five physical requests, and published no artifact or receipt.  It crossed Session, Accounts,
CashSummary, and Holdings; CashFlows/quote/kline did not run.  Its value-free error mapping means
the offline result cannot distinguish Provider permission, request parameters, account state, or
Provider business rejection.

The implementation must preserve that secrecy property: raw Provider code, message, response data,
exception text, account identifier, credential, endpoint details, and request bodies are neither
persisted nor printed.  No failure frame includes an artifact, receipt, or admission claim.

## Scope execution contracts

### Core

The fixed `core` physical sequence is:

`Session -> Accounts -> CashSummary -> Holdings -> quote -> kline_minimal`.

`Session` is observed only when the isolated authentication proxy dispatches while `Accounts` is
active. `Accounts` must normalize one readable account; its first account ID remains process-local
and is used only by CashSummary and Holdings.  A missing or invalid account stops with the existing
value-free failure semantics before any market call.  No cursor continuation occurs.  A core
artifact has final authorized operations:

```text
fosun_sync_accounts
fosun_sync_portfolio:cash
fosun_sync_portfolio:holdings
fosun_get_quotes
fosun_get_bars
```

### Extended

The fixed `extended` physical sequence is:

`Session -> Accounts -> OrderList -> CashFlows`.

The same authentication and account-discovery rules apply. `Accounts` does not grant
`fosun_sync_accounts` to an extended runtime, and its account ID must never be supplied from CLI,
environment, artifact, receipt, frame, or an unverified prior core run.  It is freshly discovered
in the same extended execution and used only in memory for OrderList/CashFlows.  A valid extended
artifact authorizes exactly:

```text
fosun_list_orders
fosun_list_cash_flows
```

The runner never begins extended after a core failure.  If an approved orchestration invokes core
then extended, core receipt publication is complete before extended starts.  Extended not started
or failed leaves the already verified core artifact/receipt unchanged, and admission treats it as
core-only; it does not contaminate or revoke valid core evidence.  Extended publication failure
after its artifact is durable follows the existing quarantine rule and cannot alter core files.

### Budgets, CLI, frames, and planning

Each invocation accepts a single explicit `--scope core|extended`, parses the same non-secret plan
arguments applicable to that scope, constructs one fresh executor, sets `max_attempts=1`, and
stops at the first error.  It has no retry flag and has no batch/default scope.

The hard aggregate cap remains 24 physical requests and the broker-domain cap remains 6; neither
cap is raised.  The per-label cap remains 3.  The implementation additionally enforces a
scope-local broker cap of 6 and uses the existing physical pre-dispatch hooks, so SDK-internal
authentication and retries cannot evade attribution.  Since retry is disabled, every second
physical dispatch is a contract violation or an already-counted attempted dispatch, never a retry
allowance.  The only legal labels are the selected scope's physical sequence; an unlabelled,
overlapping, wrong-proxy, out-of-sequence, or extra request fails before bytes leave the process.

The CLI result frame includes `scope`, `scope_contract_version`, and an artifact/receipt only for
`probe_published`; every failure keeps those fields value-free/null. Capture and receipt commands
take scope and reject a frame/artifact whose scope differs. Artifact basenames are scope-namespaced
and create-only. A failed run directory, frame, artifact, receipt, or quarantine directory is
never reused; a new run requires a new directory and fresh explicit external authorization.

## Evidence, receipt, and admission

### Artifact and receipt schema

Replace v2 with `nexus.fosun.sanitized-probe.v3`. Its required canonical fields retain the
manifest hash, SDK tag/commit, publisher key, capture/expiry timestamps, and value-free checks, and
add:

```json
{
  "schema_version": "nexus.fosun.sanitized-probe.v3",
  "scope": "core",
  "scope_contract_version": "nexus.fosun.read-only-scope.v1.core",
  "physical_operation_set": ["Session", "Accounts", "CashSummary", "Holdings", "quote", "kline_minimal"],
  "prerequisite_operation_set": ["Session", "Accounts"],
  "authorized_operation_set": ["fosun_sync_accounts", "fosun_sync_portfolio:cash", "fosun_sync_portfolio:holdings", "fosun_get_quotes", "fosun_get_bars"],
  "operations": []
}
```

The shown values are schema examples, not live values; lists must match the code-owned ordered
constants byte-for-byte. Each operation evidence remains value-free and includes its static
response-contract digest and physical request count. Checks remain scope-specific fixed constants:
the core contract checks only observations actually made by core; extended checks only its first
pages and never asserts cash/holding/market semantics. Neither scope claims pagination beyond its
first page, Provider write permission, or a live account authorization broader than its final set.

The detached receipt moves to `nexus.fosun.probe-publication.v2` and additionally binds
`artifact_sha256`, `scope`, and `scope_contract_version` in canonical compact sorted-key JSON.
The signed bytes are `NEXUS-FOSUN-PROBE-PUBLICATION-V2\\0 + artifact_sha256 + \\0 + scope + \\0 +
scope_contract_version`, with unambiguous UTF-8 encoding. Verification must parse, canonicalize,
and compare the artifact and receipt before signature verification. The activation contract hash
includes manifest digest, SDK subject, artifact digest, receipt digest, trusted-key digest, scope,
scope-contract-version, and ordered authorized set. This prevents scope swapping, downgrade, and
receipt replay across scopes.

Publication stays create-only and follows the existing held-dirfd/no-follow, owner/mode/link-count,
atomic durable write, and post-write verification rules. Sign only after durable artifact
publication; create neither artifact nor receipt on executor failure. A post-publication error is
quarantined, not admitted and not overwritten.

### Runtime admission and composition

`evaluate_activation` becomes scope-aware: it accepts one expected scope and returns a capability
whose `authorized_operation_set` is exact. Gateway construction must receive that set and install
an operation guard before SDK dispatch and route selection. The core Gateway exposes only its five
final operations; extended exposes only OrderList/CashFlows. A forbidden operation returns the
existing non-disclosing denial before transport construction. `fosun_capabilities` may describe
the active final set but is never evidence of an ungranted operation.

Version gate: production admission accepts only v3/v2 receipts and a matching v1 scope contract.
v1 artifacts, v1 receipts, v2 all-operation artifacts/receipts, missing scope fields, duplicate or
unordered sets, unknown scope versions, and any mismatched manifest/SDK/publisher/time binding are
`REVIEW_PROBE_ARTIFACT` or `REVIEW_PROBE_PUBLICATION_RECEIPT`, with no fallback. Existing runtime
must remain fixture-only until an exact scope capability validates.

First release does **not** combine core and extended artifacts in one runtime. This avoids deciding
cross-run account equivalence from value-free evidence and keeps the initial least-privilege
runtime one scope at a time. A later composition design may allow a union only if both artifacts
independently validate and all of these exact bindings match: provider, manifest hash, SDK tag and
commit, Provider base origin, trusted publisher key, non-expired capture window, same scope-contract
family, and an account-binding commitment that proves the same discovered account without exposing
its ID. Any absent/different value, clock ambiguity, duplicate scope, stale evidence, invalid
receipt, or account-binding mismatch denies the entire requested union; it must not silently select
one artifact or widen one scope.

## Compatibility, migration, and rollback

Land the schema/parser/activation gate as one atomic local change. Before it is enabled, committed
settings remain `fixture`, `FOSUN_SDK_ADMITTED=false`, and no legacy artifact is admitted. The
feature gate is an exact `FOSUN_READ_ONLY_SCOPE_CONTRACT_VERSION=v1` only in a separately admitted
production secret domain; absent or other values keep fixture mode. The gate may select one scope
only and does not choose a default.

Migration is fail-closed: do not transform, copy, reinterpret, or re-sign an old artifact. Archive
old evidence outside the activation path with its original digest and reason, then collect a newly
authorized run for the selected scope. Rollback is safe only by disabling the v1 feature gate and
returning to fixture mode after stopping the Gateway; it is not safe to restore the old all-full
admission logic because that would reinterpret deprecated evidence. Re-enabling old logic requires
a separate security review, a new schema/admission version, full tests, and a new explicit external
authorization.

## Safety invariants and non-goals

- Zero order create/modify/cancel; zero simulation account create/reset; zero subscriptions,
  webhooks, account writes, or configuration writes; live trading remains disabled.
- No Provider raw error code/message/data or credential in artifacts, receipts, frames, Git, logs,
  exceptions, fixtures, snapshots, command examples, or tests.
- No SDK install/sync, Provider call, network probe, UI, deployment, database/migration, automated
  scheduling, or unrelated refactor in this change.
- The Engine/Agent cannot use scope evidence to bypass deterministic risk, approval, audit, or the
  explicit human confirmation required for any future execution.

## Exact implementation surface

| Area | Expected files | Change |
| --- | --- | --- |
| Contract constants | `integrations/fosun_gateway/src/nexus_fosun_gateway/probe_contract.py` | Define immutable scope descriptors, ordered physical/prerequisite/final sets, scope-specific response digests, and frame fields. |
| Artifact/activation | `activation.py`, `probe.py` | Add v3 artifact/v2 receipt validation, canonical binding/signing, exact scope evaluator, legacy rejection. |
| Execution | `adapter/probe_sdk.py`, `probe_cli.py` | Select one descriptor, enforce sequence/one-shot scope budget, scope-safe account discovery, frames. |
| Publication pipeline | `probe_capture.py`, `probe_receipt.py`, `publication.py` | Bind and verify scope in protected create-only frame/artifact/receipt handling. |
| Runtime | `settings.py`, `main.py`, `policy/operations.py`, `routes/read_only.py`, relevant composition code | Add exact version/scope gate and runtime operation guard; fixture default unchanged. |
| Tests/policy/docs | existing `integrations/fosun_gateway/tests/{unit,security,contract}`, `tests/policy/test_repository_policy.py`, `scripts/ci/repository_policy.py`, activation runbook | Cover contracts and prohibit scope confusion/write surface; revise operator runbook without secrets. |

Do not modify provider manifests, dependency pins/lockfiles, `.openai/hosting.json`, Engine/database
schema, frontend, broker simulation, or real credentials. Existing `fosun_capabilities` does not
become a third scope and does not enlarge either authorized set.

## TDD matrix and acceptance

Implement in these independently reviewable phases, writing a failing focused test before each
minimal implementation change:

1. **Descriptor and artifact validation:** exact core/extended constants and order; reject missing,
   reordered, duplicate, extra, cross-scope, legacy v1/v2, and scope-version-mismatched evidence;
   verify canonical digest and receipt signature bind scope.
2. **Executor/CLI:** core reaches only its labels; extended visibly dispatches Session/Accounts as
   prerequisites but authorizes only two finals; no supplied account ID; first failure stops;
   max attempts remains one; physical total <=24 and broker <=6; no internal retry or unlabelled
   request; no raw error leaks.
3. **Capture/publication:** scope fields survive one protected success frame; failures have null
   evidence; artifact and receipt are create-only; scope-swapped frame/receipt is rejected;
   extended failure cannot alter a prior core artifact.
4. **Admission/composition/network:** core denies OrderList/CashFlows, extended denies account,
   cash, holdings, quote, and bar operations; fixture remains default; invalid/mixed evidence
   constructs no SDK client; offline socket guard confirms tests do not open a network connection;
   repository policy rejects writes and scope bypasses.
5. **Regression:** run the complete Gateway suite plus static/policy gates below. No Provider,
   capture CLI against secrets, or probe command is part of acceptance.

```bash
NEXUS_OFFLINE_SOCKET_GUARD=1 PYTHONPATH="$PWD/scripts/offline_guard" uv run --package nexus-fosun-gateway pytest integrations/fosun_gateway/tests -q
uv run pytest tests/policy -q
uv run python scripts/ci/repository_policy.py
uv run --package nexus-fosun-gateway ruff check integrations/fosun_gateway/src integrations/fosun_gateway/tests
uv run --package nexus-fosun-gateway mypy integrations/fosun_gateway/src
git diff --check
```

## Fifth core probe gate

The present approval covers this local semantic design only. A fifth external core probe is not
authorized by this document or by any prior failed full-scope run. It may be proposed only after
the implementation is merged, independently accepted with the matrix above, and its Gateway remains
fail-closed. The main session must then present the exact core final operation set, the full fixed
physical plan (`Session, Accounts, CashSummary, Holdings, quote, kline_minimal`), one-shot
`max_attempts=1`, first-error stop, the unchanged 24/6 caps, no write surface, and the expected
Provider/account impact. It must obtain or confirm authorization applicable to that exact run before
any external connection. A failed core run publishes no artifact/receipt and cannot be retried,
reused, or escalated without a new authorization.
