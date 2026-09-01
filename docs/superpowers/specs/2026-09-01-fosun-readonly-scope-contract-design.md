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

Each invocation also receives one non-secret, canonical run ID matching
`^[a-z0-9][a-z0-9-]{15,63}$`. It is an execution identity, not an account or Provider value. The
approved absolute output parent and its `runs/` child must already be protected; the runner then
creates `runs/<run-id>/<scope>/` through held directory descriptors before it can create a frame.
The parent, runs, new run, and new scope directories are no-follow, current-UID owned, mode `0700`.
A run ID is create-only: an existing run directory, including an incomplete one, is
`PROBE_OUTPUT_EXISTS`.

The CLI result frame includes `run_id`, `scope`, `scope_contract_version`, and an artifact/receipt
only for `probe_published`; every failure keeps evidence fields value-free/null. Within that scope
directory the protected names are fixed as `result.frame.json`, `artifact.json`,
`publication-receipt.json`, `staging/`, and `quarantine/`; all are created through the held dirfd
and never target a sibling scope. Artifact and receipt schemas bind the same run ID and scope, so a
same-parent core run and extended run cannot overwrite or be mistaken for one another. Capture and
receipt CLIs take `--run-id` and `--scope`, then reject unless their arguments, directory identity,
frame, artifact, and receipt have identical `run_id`, `scope`, and `scope_contract_version`.
A failed run directory, frame, artifact, receipt, staging, or quarantine directory is never reused;
a new run needs a new run ID and fresh explicit external authorization.

## Evidence, receipt, and admission

### Artifact and receipt schema

Replace v2 with `nexus.fosun.sanitized-probe.v3`. Its required canonical fields retain the
manifest hash, SDK tag/commit, publisher key, capture/expiry timestamps, and value-free checks, and
add:

```json
{
  "schema_version": "nexus.fosun.sanitized-probe.v3",
  "run_id": "example-readonly-run-0001",
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

The detached receipt is uniquely `nexus.fosun.probe-publication.v2` and additionally binds
`artifact_sha256`, `run_id`, `scope`, and `scope_contract_version` in canonical compact sorted-key
JSON. The signed bytes are `NEXUS-FOSUN-PROBE-PUBLICATION-V2\\0 + artifact_sha256 + \\0 + run_id +
\\0 + scope + \\0 + scope_contract_version`, with unambiguous UTF-8 encoding. Verification must
parse, canonicalize, and compare artifact and receipt before signature verification. The activation
contract hash includes manifest digest, SDK subject, artifact digest, receipt digest, trusted-key
digest, run ID, scope, scope-contract-version, and ordered authorized set. This prevents scope/run
swapping, downgrade, and receipt replay across scopes.

Publication stays create-only and follows the existing held-dirfd/no-follow, owner/mode/link-count,
atomic durable write, and post-write verification rules. Sign only after durable artifact
publication; create neither artifact nor receipt on executor failure. A post-publication error is
quarantined, not admitted and not overwritten.

### Runtime admission and composition

`evaluate_activation` becomes scope-aware: it accepts one expected scope and returns an immutable
`RuntimeAuthorization` containing exactly `scope`, `scope_contract_version`, `run_id`, expiry, and
the closed `authorized_operation_set`. Gateway construction receives this value; it never receives
an artifact-derived coarse `GatewayOperation` allowlist.

There are two non-interchangeable gates:

1. **Startup gate:** validate the one exact artifact/receipt/version/run-ID/scope pairing and build
   `RuntimeAuthorization` before constructing `SdkFosunTransport` or its SDK client. Invalid,
   missing, mixed, expired, or legacy evidence keeps the runtime fixture-only or rejects startup;
   it never constructs a Provider SDK client.
2. **Request gate:** a successfully started read-only Gateway can own one shared transport/client.
   Its per-request authorization is independent of that startup construction and must complete
   before every `transport.execute` call or Provider physical dispatch:

   1. Verify the listener's HMAC identity and its caller-specific allowed operation set.
   2. Perform route schema validation. For portfolio, validate `PortfolioReadRequest` including its
      `read_kind` enum before making any authorization decision from it.
   3. Derive the runtime authorization name. Portfolio derives exactly
      `fosun_sync_portfolio:cash` or `fosun_sync_portfolio:holdings` from the validated `read_kind`;
      every other route maps one-to-one to its final operation name.
   4. Require the derived name in both the HMAC caller's allowed operation set and
      `RuntimeAuthorization.authorized_operation_set`; otherwise return the existing non-disclosing
      denial without calling `transport.execute` or causing Provider physical dispatch.
   5. Only then invoke the route's fixed `transport.execute` operation with the already validated
      request.

Thus the guard cannot merely check the existing coarse `fosun_sync_portfolio` during route selection:
it must guard the derived cash/holdings name after enum validation. Core may permit both derived
names; an intentionally narrowed core authorization may permit only cash or only holdings and must
deny the other in both directions. Invalid `read_kind` fails schema validation before authorization;
it cannot be coerced to either scope or reach transport. `fosun_capabilities` may describe the
active final set but is never evidence of an ungranted operation.

The only production evidence pairing is:

```text
nexus.fosun.sanitized-probe.v3 artifact
  + nexus.fosun.probe-publication.v2 receipt
  + identical run_id/scope/scope_contract_version
  + nexus.fosun.read-only-scope.v1.{scope}
```

Artifact v1/v2, receipt v1, any missing run-ID/scope fields, unknown versions, duplicate or
unordered sets, mismatched manifest/SDK/publisher/time binding, and every other cross-pairing are
`REVIEW_PROBE_ARTIFACT` or `REVIEW_PROBE_PUBLICATION_RECEIPT`, with no fallback. In particular, a
legacy v2 all-operation artifact must never be interpreted as core or extended. Existing runtime
remains fixture-only until this exact scope capability validates.

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
| Artifact/activation | `activation.py`, `probe.py` | Add v3 artifact/v2 receipt validation, canonical run-ID/scope binding/signing, exact scope evaluator, legacy rejection. |
| Execution | `adapter/probe_sdk.py`, `probe_cli.py` | Select one descriptor, enforce sequence/one-shot scope budget, scope-safe account discovery, run-ID frames. |
| Publication pipeline | `probe_capture.py`, `probe_receipt.py`, `publication.py` | Create protected per-run/per-scope directories and verify frame/artifact/receipt run-ID/scope identity. |
| Runtime | `settings.py`, `main.py`, `policy/operations.py`, `routes/read_only.py`, relevant composition code | Add exact version/scope gate, `RuntimeAuthorization`, and post-validation derived-operation guard; fixture default unchanged. |
| Tests/policy/docs | existing `integrations/fosun_gateway/tests/{unit,security,contract}`, `tests/policy/test_repository_policy.py`, `scripts/ci/repository_policy.py`, activation runbook | Cover contracts and prohibit scope confusion/write surface; revise operator runbook without secrets. |

Do not modify provider manifests, dependency pins/lockfiles, `.openai/hosting.json`, Engine/database
schema, frontend, broker simulation, or real credentials. Existing `fosun_capabilities` does not
become a third scope and does not enlarge either authorized set.

## TDD matrix and acceptance

Implement in these independently reviewable phases, writing a failing focused test before each
minimal implementation change:

1. **Descriptor and artifact validation:** exact core/extended constants and order; reject missing,
   reordered, duplicate, extra, cross-scope, legacy artifact v1/v2, receipt v1, unknown versions,
   and scope/run-ID/version-mismatched evidence; verify canonical digest and receipt signature bind
   scope and run ID.
2. **Executor/CLI:** core reaches only its labels; extended visibly dispatches Session/Accounts as
   prerequisites but authorizes only two finals; no supplied account ID; first failure stops;
   max attempts remains one; physical total <=24 and broker <=6; no internal retry or unlabelled
   request; no raw error leaks.
3. **Capture/publication:** scope/run ID fields survive one protected success frame; failures have
   null evidence; artifact and receipt are create-only; same-parent core then extended use distinct
   directories; wrong scope/run-ID cross-references fail closed; after extended publication failure,
   the earlier core artifact/receipt bytes and SHA-256 are unchanged; failed directories cannot be
   reused.
4. **Admission/composition/network:** core denies OrderList/CashFlows, extended denies account,
   cash, holdings, quote, and bar operations. Test cash-only and holdings-only authorization deny
   the opposite `read_kind`, invalid `read_kind` fails before authorization, and all denied cases
   leave `transport.execute` uninvoked with zero Provider physical dispatch. Separately, invalid or
   mixed startup evidence constructs neither SDK client nor `SdkFosunTransport`. Fixture remains
   default; offline socket guard confirms tests do not open a network connection; repository policy
   rejects writes and scope bypasses.
5. **Regression:** run the complete Gateway suite plus static/policy gates below. No Provider,
   capture CLI against secrets, or probe command is part of acceptance.

```bash
set -euo pipefail
NEXUS_GATEWAY_TMP="$(mktemp -d "$PWD/.nexus-gateway-acceptance.XXXXXX")"
NEXUS_POLICY_TMP="$(mktemp -d "$PWD/.nexus-policy-acceptance.XXXXXX")"
chmod 700 "$NEXUS_GATEWAY_TMP" "$NEXUS_POLICY_TMP"
cleanup() {
  rm -f "$NEXUS_POLICY_TMP/nexus-dependency-inventory.json"
  rmdir "$NEXUS_POLICY_TMP" "$NEXUS_GATEWAY_TMP"
}
trap cleanup EXIT

TMPDIR="$NEXUS_GATEWAY_TMP" PYTHONDONTWRITEBYTECODE=1 \
  NEXUS_OFFLINE_SOCKET_GUARD=1 PYTHONPATH="$PWD/scripts/offline_guard" UV_OFFLINE=1 \
  uv run --package nexus-fosun-gateway pytest integrations/fosun_gateway/tests -q \
  -p no:cacheprovider
UV_OFFLINE=1 uv run pytest tests/policy -q
UV_OFFLINE=1 uv run python scripts/ci/repository_policy.py \
  --root . \
  --inventory-output "$NEXUS_POLICY_TMP/nexus-dependency-inventory.json"
UV_OFFLINE=1 uv run --package nexus-fosun-gateway ruff check \
  integrations/fosun_gateway/src integrations/fosun_gateway/tests
UV_OFFLINE=1 uv run --package nexus-fosun-gateway mypy integrations/fosun_gateway/src
git diff --check
```

Do not set `umask 077` inside the pytest process: it would turn a test-created `0755`
`unsafe_dir` into `0700` and mask the intended unsafe-directory assertion. The initial `644+1`
Gateway outcome was that harness-side umask contamination, not a product regression. The corrected
harness has been observed to report Gateway `645 passed` and Policy `128 passed`; those observations
validate only this acceptance harness and are not evidence that the future scope-contract
implementation is complete. The cleanup trap removes only the two explicit checkout-local temporary
directories (and the inventory file); it never writes a tracked path or uses system `/tmp`.

## Fifth core probe gate

The present approval covers this local semantic design only. A fifth external core probe is not
authorized by this document or by any prior failed full-scope run. It may be proposed only after
the implementation is merged, independently accepted with the matrix above, and its Gateway remains
fail-closed. The main session must then present the exact core final operation set, the full fixed
physical plan (`Session, Accounts, CashSummary, Holdings, quote, kline_minimal`), one-shot
`max_attempts=1`, first-error stop, the unchanged 24/6 caps, no write surface, the unique protected
core run ID/directory, and the expected Provider/account impact. It must obtain or confirm
authorization applicable to that exact run before any external connection. A failed core run
publishes no artifact/receipt and cannot be retried, reused, or escalated without a new
authorization.
