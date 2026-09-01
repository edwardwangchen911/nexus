# Fosun Read-Only Scope Contract Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the all-operation Fosun probe admission with one immutable, fail-closed `core` or `extended` read-only scope authorization, without adding a Provider write surface.

**Architecture:** Keep the existing Gateway route-to-SDK mapping (`GatewayOperation`) for the seven Provider-backed read routes, but introduce a separate `RuntimeOperation` namespace for the fine-grained final capability contract. `fosun_capabilities` is an HMAC-authenticated local introspection exception: it returns active final operations and never becomes a `RuntimeOperation` or calls transport. Scope descriptors own physical, prerequisite, and final ordered sets; artifact v3 and receipt v2 bind the selected descriptor plus a create-only run ID. Startup creates one immutable `RuntimeAuthorization` before SDK construction, while each Provider-backed read request authorizes a schema-derived runtime operation before `transport.execute`.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, cryptography Ed25519, pytest, Ruff, mypy, uv.

**Spec:** `docs/superpowers/specs/2026-09-01-fosun-readonly-scope-contract-design.md`

## Global Constraints

- All implementation and verification is offline: `UV_OFFLINE=1`, `NEXUS_OFFLINE_SOCKET_GUARD=1`; never run a Provider, capture, receipt, or probe CLI against credentials.
- Keep `transport_mode="fixture"` and `FOSUN_SDK_ADMITTED=false` as committed defaults; exact `FOSUN_READ_ONLY_SCOPE_CONTRACT_VERSION=v1` selects one explicitly configured scope only, with no default scope.
- Do not modify provider manifests, dependency pins or lockfiles, `.openai/hosting.json`, Engine/database schema, frontend, broker simulation, credentials, deployment, or scheduling.
- Add no order create/modify/cancel, simulation account create/reset, subscription, webhook, account write, configuration write, raw Provider error value, credential, endpoint detail, or request body to source, evidence, frames, logs, exceptions, fixtures, snapshots, or docs.
- `core` and `extended` are closed, non-substitutable descriptor rows; first release admits exactly one artifact/receipt scope and never unions them.
- Every probe execution has `max_attempts=1`, first-error stop, total physical cap `24`, broker cap `6`, per-label cap `3`, no retry flag, and no extra/unlabelled/wrong-proxy/out-of-sequence dispatch.
- Preserve held-dirfd/no-follow/current-UID/mode/link-count/durable-write checks. The approved protected parent and `runs/` child are `0700`; create-only paths are `runs/<run-id>/<scope>/result.frame.json`, `artifact.json`, `publication-receipt.json`, `staging/`, and `quarantine/`.
- Use small reviewable commits; run `git diff --check` before every commit. Do not claim external activation: a fifth core probe is outside this plan and needs post-merge independent acceptance plus fresh explicit authorization in the main session.

---

## File map and dependency order

| File | Responsibility | Task |
| --- | --- | --- |
| `integrations/fosun_gateway/src/nexus_fosun_gateway/probe_contract.py` | Defines and exports `RuntimeOperation`, immutable scope descriptors, run-ID validation, ordered physical/prerequisite/final operations, response digests, frame fields | 1 |
| `integrations/fosun_gateway/src/nexus_fosun_gateway/activation.py` | Artifact v3/receipt v2 parsing, canonical pair verification, immutable runtime authorization, exact activation result | 1 |
| `integrations/fosun_gateway/src/nexus_fosun_gateway/probe.py` | Scope-aware plan, artifact construction and local validation | 2 |
| `integrations/fosun_gateway/src/nexus_fosun_gateway/adapter/probe_sdk.py` | Scope-selected executor and physically attributed one-shot budget | 2 |
| `integrations/fosun_gateway/src/nexus_fosun_gateway/probe_cli.py` | Explicit `--run-id`/`--scope`, scope-specific arguments, frames, and the sole production publication entry | 2, 3 |
| `integrations/fosun_gateway/src/nexus_fosun_gateway/publication.py` | Held-dirfd creation and create-only scope output operations | 3 |
| `integrations/fosun_gateway/src/nexus_fosun_gateway/probe_capture.py` | Scope/run-bound protected frame creation and verification | 3 |
| `integrations/fosun_gateway/src/nexus_fosun_gateway/probe_receipt.py` | Scope/run-bound receipt extraction, cross-reference rejection | 3 |
| `integrations/fosun_gateway/src/nexus_fosun_gateway/policy/operations.py` | Coarse Gateway mapping plus fine-grained `RuntimeOperation` and derivation | 4 |
| `integrations/fosun_gateway/src/nexus_fosun_gateway/policy/runtime_authorization.py` | Pure post-schema authorization seam for caller/runtime intersection | 4 |
| `integrations/fosun_gateway/src/nexus_fosun_gateway/security/hmac.py` | HMAC identity/signature/replay/listener authentication with fine-grained caller operations only | 4 |
| `integrations/fosun_gateway/src/nexus_fosun_gateway/routes/read_only.py` | HMAC, schema, derive, caller intersection, runtime intersection, then transport | 4 |
| `integrations/fosun_gateway/src/nexus_fosun_gateway/settings.py` | Exact v1 feature/scope configuration and fixture-default validation | 4 |
| `integrations/fosun_gateway/src/nexus_fosun_gateway/main.py` | Evidence startup gate before `fsopenapi` import/client/`SdkFosunTransport`; bind authorization to app | 4 |
| `scripts/ci/repository_policy.py` | Review tripwires and policy checks for runtime scope guard and no write expansion | 5 |
| `tests/policy/test_repository_policy.py` | Policy unit cases | 5 |
| `docs/runbooks/fosun-read-only-activation.md` | Secret-free one-scope operator procedure, rollback, and external-probe boundary | 5 |

Recommended order is Task 1 → 2 → 3 → 4 → 5. Each task consumes only the interfaces named below and is independently reviewable after its own regression and commit. Do not run two tasks concurrently in one worktree.

### Task 1: Define immutable scope descriptors and v3/v2 evidence admission

**Files:**
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/probe_contract.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/activation.py`
- Modify: `integrations/fosun_gateway/tests/unit/test_activation.py`
- Modify: `integrations/fosun_gateway/tests/unit/test_probe.py`
- Modify: `integrations/fosun_gateway/tests/security/test_probe_receipt.py`

**Interfaces:**
- Produces `ProbeScope = Literal["core", "extended"]`, `ScopeDescriptor`, `SCOPE_DESCRIPTORS: Mapping[ProbeScope, ScopeDescriptor]`, and `scope_descriptor(scope: ProbeScope) -> ScopeDescriptor`.
- Produces `RuntimeOperation = Literal["fosun_sync_accounts", "fosun_sync_portfolio:cash", "fosun_sync_portfolio:holdings", "fosun_list_orders", "fosun_list_cash_flows", "fosun_get_quotes", "fosun_get_bars"]` in `probe_contract.py`; later Tasks 1 and 4 import it from that module.
- `ScopeDescriptor` is frozen and exposes `scope_contract_version: str`, `physical_operation_set: tuple[ProbeLabel, ...]`, `prerequisite_operation_set: tuple[ProbeLabel, ...]`, `authorized_operation_set: tuple[RuntimeOperation, ...]`, and scope-specific checks/response-digest lookup.
- Produces `RuntimeAuthorization(frozen=True)` with `scope: ProbeScope`, `scope_contract_version: str`, `run_id: str`, `capability_expires_at: datetime`, and `authorized_operation_set: frozenset[RuntimeOperation]`.
- Changes `evaluate_activation(manifest_bytes: bytes, probe_bytes: bytes | None, publication_receipt_bytes: bytes | None, *, expected_scope: ProbeScope, trusted_publication_public_key_bytes: bytes | None, checked_at: datetime | None) -> tuple[FosunActivationResult, RuntimeAuthorization | None]`; fixture outcomes return `(result, None)`.
- Changes `verify_probe_publication(manifest_bytes: bytes, probe_bytes: bytes, publication_receipt_bytes: bytes, *, expected_scope: ProbeScope, trusted_publication_public_key_bytes: bytes, checked_at: datetime | None) -> str | None` and accepts no legacy schema.

- [ ] **Step 1: Write failing descriptor and evidence tests**

```python
def test_scope_descriptors_are_immutable_closed_rows() -> None:
    assert scope_descriptor("core").physical_operation_set == (
        "Session", "Accounts", "CashSummary", "Holdings", "quote", "kline_minimal",
    )
    assert scope_descriptor("extended").prerequisite_operation_set == ("Session", "Accounts")
    assert scope_descriptor("extended").authorized_operation_set == (
        "fosun_list_orders", "fosun_list_cash_flows",
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        SCOPE_DESCRIPTORS["core"].authorized_operation_set += ("fosun_list_orders",)
    with pytest.raises(TypeError):
        SCOPE_DESCRIPTORS["core"] = scope_descriptor("extended")

def test_activation_rejects_legacy_or_cross_scope_pair() -> None:
    result, authorization = evaluate_activation(
        MANIFEST_PATH.read_bytes(), legacy_v2_bytes, receipt_v1_bytes,
        expected_scope="core", trusted_publication_public_key_bytes=TRUSTED_KEY,
        checked_at=CHECKED_AT,
    )
    assert result.next_action == "REVIEW_PROBE_ARTIFACT"
    assert authorization is None
```

- [ ] **Step 2: Run focused tests to verify failure**

Run: `UV_OFFLINE=1 uv run --package nexus-fosun-gateway pytest integrations/fosun_gateway/tests/unit/test_activation.py integrations/fosun_gateway/tests/unit/test_probe.py integrations/fosun_gateway/tests/security/test_probe_receipt.py -q`

Expected: FAIL because `RuntimeOperation`, `ScopeDescriptor`, `RuntimeAuthorization`, v3 artifact fields, v2 receipt fields, and `expected_scope` do not exist.

- [ ] **Step 3: Implement the minimal contract and parser changes**

```python
RuntimeOperation = Literal[
    "fosun_sync_accounts", "fosun_sync_portfolio:cash", "fosun_sync_portfolio:holdings",
    "fosun_list_orders", "fosun_list_cash_flows", "fosun_get_quotes", "fosun_get_bars",
]

@dataclass(frozen=True)
class ScopeDescriptor:
    scope: ProbeScope
    scope_contract_version: str
    physical_operation_set: tuple[ProbeLabel, ...]
    prerequisite_operation_set: tuple[ProbeLabel, ...]
    authorized_operation_set: tuple[RuntimeOperation, ...]

SCOPE_DESCRIPTORS = MappingProxyType({
    "core": ScopeDescriptor("core", "nexus.fosun.read-only-scope.v1.core", ("Session", "Accounts", "CashSummary", "Holdings", "quote", "kline_minimal"), ("Session", "Accounts"), ("fosun_sync_accounts", "fosun_sync_portfolio:cash", "fosun_sync_portfolio:holdings", "fosun_get_quotes", "fosun_get_bars")),
    "extended": ScopeDescriptor("extended", "nexus.fosun.read-only-scope.v1.extended", ("Session", "Accounts", "OrderList", "CashFlows"), ("Session", "Accounts"), ("fosun_list_orders", "fosun_list_cash_flows")),
})
```

Make `SanitizedProbeArtifact` v3 require canonical `run_id`, `scope`, `scope_contract_version`, all three ordered sets, operations, and scope-specific checks. Make receipt v2 require `artifact_sha256`, `run_id`, `scope`, `scope_contract_version`, and `signature_b64u`; verify canonical compact JSON and compare the artifact/receipt tuple before Ed25519 verification of `NEXUS-FOSUN-PROBE-PUBLICATION-V2\0 + artifact_sha256 + \0 + run_id + \0 + scope + \0 + scope_contract_version`. Validate ordered equality with the code-owned descriptor (not set inclusion), reject duplicate/unknown/reordered/extra fields and v1/v2 artifacts or v1 receipts with `REVIEW_PROBE_ARTIFACT` or `REVIEW_PROBE_PUBLICATION_RECEIPT`. Add run ID regex `^[a-z0-9][a-z0-9-]{15,63}$`; include run ID, scope, version, artifact/receipt digests, trusted key digest, manifest digest, SDK subject, and ordered finals in the activation contract hash. Return `RuntimeAuthorization` only after every exact pair/binding/expiry check passes.

- [ ] **Step 4: Run focused tests and contract regressions**

Run: `UV_OFFLINE=1 uv run --package nexus-fosun-gateway pytest integrations/fosun_gateway/tests/unit/test_activation.py integrations/fosun_gateway/tests/unit/test_probe.py integrations/fosun_gateway/tests/security/test_probe_receipt.py -q`

Expected: PASS; tests cover both descriptors, exact ordering, missing/duplicate/extra/cross-scope fields, run-ID mismatch, legacy rejection, signature material binding, and a frozen authorization.

- [ ] **Step 5: Commit the independently reviewable evidence contract**

```bash
git diff --check
git add integrations/fosun_gateway/src/nexus_fosun_gateway/probe_contract.py integrations/fosun_gateway/src/nexus_fosun_gateway/activation.py integrations/fosun_gateway/tests/unit/test_activation.py integrations/fosun_gateway/tests/unit/test_probe.py integrations/fosun_gateway/tests/security/test_probe_receipt.py
git commit -m "feat: add scoped Fosun evidence contract"
```

### Task 2: Make executor, artifact builder, and CLI one-shot scope-aware

**Files:**
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/adapter/probe_sdk.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/probe.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/probe_cli.py`
- Modify: `integrations/fosun_gateway/tests/unit/test_probe_sdk.py`
- Modify: `integrations/fosun_gateway/tests/unit/test_probe.py`
- Modify: `integrations/fosun_gateway/tests/unit/test_probe_cli.py`
- Modify: `integrations/fosun_gateway/tests/security/test_probe_sdk_secrecy.py`
- Modify: `integrations/fosun_gateway/tests/security/test_probe_secrecy.py`

**Interfaces:**
- Consumes `ScopeDescriptor`, `ProbeScope`, run-ID validator, and v3 artifact models from Task 1.
- Changes `ProbePlan` to a discriminated scope form: common `run_id`, `scope`, `limit`, and `capability_ttl_seconds`; core additionally requires `symbol` and `session_date`, while extended additionally requires `cash_flow_date_from` and `cash_flow_date_to`. `_parse_probe_plan()` accepts `--manifest --run-id --scope --limit --capability-ttl-seconds --symbol --session-date` for core, and `--manifest --run-id --scope --limit --capability-ttl-seconds --cash-flow-date-from --cash-flow-date-to` for extended; each rejects duplicate/unknown/retry/account-ID and the other scope’s fields before settings, executor, or transport construction.
- Changes `ProbeRequestPlan` to the same discriminated data shape and `SdkProbeExecutor.run(plan: ProbeRequestPlan, descriptor: ScopeDescriptor) -> ProbeExecutionEvidence`.
- Produces a `PreparedProbeArtifact` carrying `run_id`, `scope`, `scope_contract_version`, v3 canonical bytes, SHA-256, length, and expiry; `sign_publication_receipt()` emits v2 bytes.

- [ ] **Step 1: Write failing scope execution tests**

```python
@pytest.mark.asyncio
async def test_extended_records_prerequisites_but_authorizes_only_finals() -> None:
    evidence = await executor.run(request_plan, scope_descriptor("extended"))
    assert evidence.physical_operation_set == ("Session", "Accounts", "OrderList", "CashFlows")
    artifact = await run_sanitized_probe(manifest, extended_plan, executor, started_at=CHECKED_AT, private_key_factory=Ed25519PrivateKey.generate, utc_clock=lambda: CHECKED_AT)
    document = json.loads(artifact.artifact_bytes)
    assert document["authorized_operation_set"] == ["fosun_list_orders", "fosun_list_cash_flows"]
    assert "external_account_id" not in artifact.artifact_bytes.decode()

def test_first_dispatch_failure_stops_at_one_attempt_without_error_value() -> None:
    result = failure_result(ProbeSdkError("PROBE_FAILED", operation="Accounts", physical_request_count=2))
    assert result["physical_request_count"] == 2
    assert all(result[name] is None for name in ("artifact_sha256", "publication_receipt"))

@pytest.mark.parametrize("arguments", [
    CORE_ARGUMENTS + ("--cash-flow-date-from", "2026-08-01"),
    EXTENDED_ARGUMENTS + ("--symbol", "usAAPL"),
])
def test_scope_rejects_cross_scope_arguments_before_transport(arguments: tuple[str, ...]) -> None:
    with pytest.raises(ProbeSdkError, match="INVALID_PROBE_PLAN"):
        _parse_probe_plan(arguments, MANIFEST_SPELLING)
```

Define `CORE_ARGUMENTS` and `EXTENDED_ARGUMENTS` in `test_probe_cli.py` as complete alternating tuples, respectively `("--manifest", MANIFEST_SPELLING, "--run-id", "readonly-core-run-0001", "--scope", "core", "--symbol", "usAAPL", "--session-date", "2026-08-31", "--limit", "1", "--capability-ttl-seconds", "60")` and `("--manifest", MANIFEST_SPELLING, "--run-id", "readonly-extended-run-0001", "--scope", "extended", "--cash-flow-date-from", "2026-08-01", "--cash-flow-date-to", "2026-08-02", "--limit", "1", "--capability-ttl-seconds", "60")`; import `_parse_probe_plan` from `probe_cli` in that existing test module.

- [ ] **Step 2: Run focused tests to verify failure**

Run: `UV_OFFLINE=1 uv run --package nexus-fosun-gateway pytest integrations/fosun_gateway/tests/unit/test_probe_sdk.py integrations/fosun_gateway/tests/unit/test_probe.py integrations/fosun_gateway/tests/unit/test_probe_cli.py integrations/fosun_gateway/tests/security/test_probe_sdk_secrecy.py integrations/fosun_gateway/tests/security/test_probe_secrecy.py -q`

Expected: FAIL because executor has one fixed eight-label sequence and CLI has no run/scope fields.

- [ ] **Step 3: Implement descriptor-selected, one-shot execution**

Use the descriptor physical sequence exactly: core `Session, Accounts, CashSummary, Holdings, quote, kline_minimal`; extended `Session, Accounts, OrderList, CashFlows`. Keep account discovery freshly within each execution, pass the normalized account ID only to CashSummary/Holdings or OrderList/CashFlows in the same process, and do not add a plan/environment/CLI/artifact/frame field for it. Replace the current single `ProbePlan`/`ProbeRequestPlan` with `CoreProbePlan` and `ExtendedProbePlan` Pydantic models plus `ProbePlan = CoreProbePlan | ExtendedProbePlan`; core’s `to_request_plan()` supplies only symbol/session date/limit and extended’s supplies only cash-flow date range/limit. Reset `PhysicalHttpBudget` per executor, make pre-dispatch reject labels absent from the selected descriptor, enforce total `<=24`, broker `<=6`, scope broker `<=6`, per-label `<=3`, and `max_attempts=1`; a second attributed dispatch fails rather than retries. Build v3 operation evidence only from the descriptor and use descriptor checks. Add `run_id`, `scope`, and `scope_contract_version` to the fixed ordered frame fields; on every non-published path keep artifact/receipt fields null and preserve only safe failure code/category/label/count.

- [ ] **Step 4: Run focused tests and verify static typing**

Run: `UV_OFFLINE=1 uv run --package nexus-fosun-gateway pytest integrations/fosun_gateway/tests/unit/test_probe_sdk.py integrations/fosun_gateway/tests/unit/test_probe.py integrations/fosun_gateway/tests/unit/test_probe_cli.py integrations/fosun_gateway/tests/security/test_probe_sdk_secrecy.py integrations/fosun_gateway/tests/security/test_probe_secrecy.py -q && UV_OFFLINE=1 uv run --package nexus-fosun-gateway mypy integrations/fosun_gateway/src`

Expected: PASS; core never labels OrderList/CashFlows, extended never labels cash/holdings/market, supplied account ID and retry flags fail before transport, all failures are value-free, and mypy has no errors.

- [ ] **Step 5: Commit scope execution without publication changes**

```bash
git diff --check
git add integrations/fosun_gateway/src/nexus_fosun_gateway/adapter/probe_sdk.py integrations/fosun_gateway/src/nexus_fosun_gateway/probe.py integrations/fosun_gateway/src/nexus_fosun_gateway/probe_cli.py integrations/fosun_gateway/tests/unit/test_probe_sdk.py integrations/fosun_gateway/tests/unit/test_probe.py integrations/fosun_gateway/tests/unit/test_probe_cli.py integrations/fosun_gateway/tests/security/test_probe_sdk_secrecy.py integrations/fosun_gateway/tests/security/test_probe_secrecy.py
git commit -m "feat: run one scoped Fosun probe"
```

### Task 3: Publish scope/run-isolated frames, artifacts, and receipts

**Files:**
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/publication.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/probe_cli.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/probe_capture.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/probe_receipt.py`
- Modify: `integrations/fosun_gateway/tests/security/test_probe_publication.py`
- Modify: `integrations/fosun_gateway/tests/security/test_probe_capture.py`
- Modify: `integrations/fosun_gateway/tests/security/test_probe_receipt.py`
- Modify: `integrations/fosun_gateway/tests/unit/test_probe_cli.py`

**Interfaces:**
- Consumes Task 1 v3/v2 models and Task 2 `ProbePlan`/frame fields.
- Produces `create_scope_run_directory(output_parent: Path, run_id: str, scope: ProbeScope) -> ScopedProbeOutput` with held `dir_fd`; `ScopedProbeOutput` has fixed names and exposes no path to sibling scopes.
- Changes `_execute_once()` in `probe_cli.py` to call `create_scope_run_directory(settings.output_dir, plan.run_id, plan.scope)` once, then pass that returned held scope directory only to frame/artifact/receipt publication; it removes the baseline `settings.output_dir / plan.output_basename` path construction and has no alternate `publish_probe_artifact(settings.output_dir, ...)` call.
- Changes capture and receipt CLIs to require `--run-id` and `--scope`, use the fixed directory and fixed basenames, and reject all argument/frame/artifact/receipt identity disagreements.

- [ ] **Step 1: Write failing protected-output tests**

```python
def test_same_parent_core_and_extended_cannot_overwrite_each_other(tmp_path: Path) -> None:
    parent = protected_absolute_parent_with_runs(tmp_path)
    core = create_scope_run_directory(parent, "readonly-core-run-0001", "core")
    extended = create_scope_run_directory(parent, "readonly-extended-run-0001", "extended")
    assert core.directory != extended.directory
    assert core.directory.name == "core"
    assert extended.directory.name == "extended"

def test_receipt_rejects_frame_artifact_scope_or_run_swap() -> None:
    assert probe_receipt.main(swapped_arguments) == 2
    assert not (scope_dir / "publication-receipt.json").exists()

def test_existing_run_directory_is_exact_create_only_failure(tmp_path: Path) -> None:
    output_parent = protected_absolute_parent_with_runs(tmp_path)
    (output_parent / "runs" / "readonly-core-run-0001").mkdir(mode=0o700)
    with pytest.raises(ProbeSdkError, match="PROBE_OUTPUT_EXISTS"):
        create_scope_run_directory(output_parent, "readonly-core-run-0001", "core")
```

- [ ] **Step 2: Run focused tests to verify failure**

Run: `UV_OFFLINE=1 uv run --package nexus-fosun-gateway pytest integrations/fosun_gateway/tests/security/test_probe_publication.py integrations/fosun_gateway/tests/security/test_probe_capture.py integrations/fosun_gateway/tests/security/test_probe_receipt.py integrations/fosun_gateway/tests/unit/test_probe_cli.py -q`

Expected: FAIL because output uses arbitrary basenames/directories and identity is not scope/run-bound.

- [ ] **Step 3: Implement held-dirfd, create-only hierarchy and cross-checks**

Add `protected_absolute_parent_with_runs(tmp_path: Path) -> Path` to `test_probe_publication.py`: it creates an absolute `tmp_path / "operator"` and its `runs` child, chmods both to `0700`, and returns the parent. Open that already-approved absolute parent and `runs/` via no-follow dirfds, require current UID and `0700`, then create `runs/<run-id>` and its only selected `<scope>` child with `O_EXCL` semantics; any existing run directory returns `PROBE_OUTPUT_EXISTS`. Create the fixed frame/artifact/receipt/staging/quarantine names only below that held scope dirfd. Preserve existing durable atomic write and post-write verification; never reuse a failed frame/artifact/receipt/staging/quarantine. Capture writes `result.frame.json`; the sole `probe_cli._execute_once()` publication path writes `artifact.json`; receipt extraction writes `publication-receipt.json`. Parse and compare run ID/scope/version from CLI arguments, selected directory identity, success frame, artifact, and receipt before signing/publishing. Add a test that monkeypatches `publish_probe_artifact` and asserts its directory argument is the held selected scope directory, never `settings.output_dir`. On a post-artifact error, quarantine the selected scope only; prove an already published core artifact and receipt retain identical bytes and SHA-256 after a later extended publication failure.

- [ ] **Step 4: Run focused tests and failure-state regressions**

Run: `UV_OFFLINE=1 uv run --package nexus-fosun-gateway pytest integrations/fosun_gateway/tests/security/test_probe_publication.py integrations/fosun_gateway/tests/security/test_probe_capture.py integrations/fosun_gateway/tests/security/test_probe_receipt.py -q`

Expected: PASS; unsafe ownership/mode/symlink/link count, existing/incomplete run, frame/artifact/receipt swap, failed-directory reuse, and cross-scope overwrite all fail closed without evidence leakage.

- [ ] **Step 5: Commit publication isolation**

```bash
git diff --check
git add integrations/fosun_gateway/src/nexus_fosun_gateway/publication.py integrations/fosun_gateway/src/nexus_fosun_gateway/probe_capture.py integrations/fosun_gateway/src/nexus_fosun_gateway/probe_receipt.py integrations/fosun_gateway/src/nexus_fosun_gateway/probe_cli.py integrations/fosun_gateway/tests/security/test_probe_publication.py integrations/fosun_gateway/tests/security/test_probe_capture.py integrations/fosun_gateway/tests/security/test_probe_receipt.py integrations/fosun_gateway/tests/unit/test_probe_cli.py
git commit -m "feat: isolate Fosun scope publication"
```

### Task 4: Bind startup evidence and per-request derived authorization

**Files:**
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/policy/operations.py`
- Create: `integrations/fosun_gateway/src/nexus_fosun_gateway/policy/runtime_authorization.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/security/hmac.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/routes/read_only.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/settings.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/main.py`
- Modify: `integrations/fosun_gateway/tests/security/test_principal_authorization.py`
- Modify: `integrations/fosun_gateway/tests/security/read_only_helpers.py`
- Modify: `integrations/fosun_gateway/tests/security/test_read_only_surface.py`
- Modify: `integrations/fosun_gateway/tests/security/test_read_only_transport.py`
- Create: `integrations/fosun_gateway/tests/unit/test_runtime_authorization.py`
- Modify: `integrations/fosun_gateway/tests/unit/test_composition.py`
- Modify: `integrations/fosun_gateway/tests/test_health.py`

**Interfaces:**
- Consumes Task 1 `RuntimeAuthorization` and the scope-aware `evaluate_activation` signature.
- Consumes Task 1 `RuntimeOperation`; produces `derive_runtime_operation(operation: GatewayOperation, payload: RequestModel) -> RuntimeOperation` in `policy/operations.py`.
- Produces `authorize_runtime_operation(operation: RuntimeOperation, actor: VerifiedGatewayActor, runtime_authorization: RuntimeAuthorization | None, transport_mode: Literal["fixture", "read_only"]) -> None` in new `policy/runtime_authorization.py`; it returns for an allowed operation and otherwise raises existing `GatewayError("OPERATION_NOT_ALLOWED", ..., status_code=404, next_action="USE_AUTHORIZED_OPERATION")`.
- Changes `security/hmac.py` `_VerificationIdentity.allowed_operations` and `VerifiedGatewayActor.allowed_operations` to `frozenset[GatewayOperation | RuntimeOperation]`. In read-only mode `_verification_identity()` supplies fine runtime sets; fixture mode supplies the existing coarse `GatewayOperation` sets. `GatewayHmacVerifier.verify(request: Request, body: bytes, *, expected_actor_id: str | None) -> VerifiedGatewayActor` verifies identity/signature/replay/listener only; it removes the `operation: GatewayOperation | None` parameter and its pre-schema coarse allowlist rejection.
- `build_transport(settings: GatewaySettings, *, on_poison: Callable[[], None] | None = None, on_activation_expired: Callable[[datetime], None] | None = None, activation_checked_at: datetime | None = None, activation_monotonic_anchor: float | None = None) -> tuple[FosunTransport, RuntimeAuthorization | None]` returns a non-null authorization only for read-only mode; fixture returns `(FixtureFosunTransport(), None)`. `create_gateway_app` stores `app.state.runtime_authorization`.
- `fosun_capabilities` is not a `RuntimeOperation`: after HMAC identity verification it returns `GatewayPage` built locally from `RuntimeAuthorization.authorized_operation_set` in read-only mode, with no `transport.execute`; fixture mode retains the existing fixture/static local capabilities response and does not read `runtime_authorization` or construct SDK/Provider state.

- [ ] **Step 1: Write failing startup and request-gate tests**

```python
def test_read_only_scope_intersection_is_independent_of_fixture_settings() -> None:
    core = RuntimeAuthorization(scope="core", scope_contract_version="nexus.fosun.read-only-scope.v1.core", run_id="readonly-core-run-0001", capability_expires_at=EXPIRY, authorized_operation_set=frozenset(scope_descriptor("core").authorized_operation_set))
    extended = RuntimeAuthorization(scope="extended", scope_contract_version="nexus.fosun.read-only-scope.v1.extended", run_id="readonly-extended-run-0001", capability_expires_at=EXPIRY, authorized_operation_set=frozenset(scope_descriptor("extended").authorized_operation_set))
    broker = VerifiedGatewayActor("nexus-broker-worker", UUID(REQUEST_ID), "test-key", frozenset({"fosun_sync_accounts", "fosun_sync_portfolio:cash", "fosun_list_orders", "fosun_list_cash_flows"}))
    authorize_runtime_operation("fosun_sync_accounts", broker, core, "read_only")
    with pytest.raises(GatewayError, match="OPERATION_NOT_ALLOWED"):
        authorize_runtime_operation("fosun_sync_accounts", broker, extended, "read_only")
    with pytest.raises(GatewayError, match="OPERATION_NOT_ALLOWED"):
        authorize_runtime_operation("fosun_sync_portfolio:holdings", broker, core, "read_only")

def test_mixed_startup_evidence_constructs_no_sdk_client(monkeypatch) -> None:
    monkeypatch.setattr(main.importlib, "import_module", pytest.fail)
    with pytest.raises(GatewayError, match="SDK_NOT_ADMITTED"):
        build_transport(mixed_scope_settings)

def test_capabilities_is_local_and_scope_exact(
    core_runtime_client: tuple[TestClient, RecordingTransport],
    extended_runtime_client: tuple[TestClient, RecordingTransport],
    fixture_client: tuple[TestClient, RecordingTransport],
) -> None:
    core, core_transport = core_runtime_client
    extended, extended_transport = extended_runtime_client
    fixture, fixture_transport = fixture_client
    core_item = _post_signed(core, "fosun_capabilities", {}, nonce="0198f7a0-2222-7777-8222-000000000902").json()["items"][0]
    extended_item = _post_signed(extended, "fosun_capabilities", {}, nonce="0198f7a0-2222-7777-8222-000000000903").json()["items"][0]
    assert (core_item["kind"], core_item["provider"], core_item["mode"]) == ("capability", "fosun", "read_only")
    assert core_item["operations"] == [
        "fosun_sync_accounts", "fosun_sync_portfolio:cash", "fosun_sync_portfolio:holdings", "fosun_get_quotes", "fosun_get_bars",
    ]
    assert (extended_item["kind"], extended_item["provider"], extended_item["mode"]) == ("capability", "fosun", "read_only")
    assert extended_item["operations"] == ["fosun_list_orders", "fosun_list_cash_flows"]
    assert _post_signed(fixture, "fosun_capabilities", {}, nonce="0198f7a0-2222-7777-8222-000000000904").status_code == 200
    assert (core_transport.calls, extended_transport.calls, fixture_transport.calls) == (0, 0, 0)
```

Import `UUID` from `uuid`, `GatewayError`, `RuntimeAuthorization`, `scope_descriptor`, `VerifiedGatewayActor`, and `authorize_runtime_operation` in `test_runtime_authorization.py`; define `REQUEST_ID = "0198f7a0-2222-7777-8222-000000000901"` and an aware UTC `EXPIRY` in that file.

Put the pure helper cases in new `test_runtime_authorization.py`; they construct `VerifiedGatewayActor` and Task 1 `RuntimeAuthorization` directly, so `transport_mode="read_only"` is explicit and no `GatewaySettings.for_test()` fixture can mask a branch. Test: core accounts allows; extended accounts denies; a cash-only actor denies holdings; a runtime set missing cash denies cash; `runtime_authorization=None` denies in read-only; and fixture mode returns without runtime intersection. Keep `test_principal_authorization.py` for HTTP integration: define `core_runtime_client` and `extended_runtime_client` by `settings = GatewaySettings.for_test().model_copy(update={"transport_mode": "read_only"})`, then monkeypatch `nexus_fosun_gateway.main.build_transport` before `create_gateway_app(settings)` to return `(RecordingTransport(), authorization)` and record that `importlib.import_module` was never called. This enters the real read-only route branch without SDK construction. Define `fixture_client` from unmodified `GatewaySettings.for_test()` and leave `runtime_authorization` unset. Assert the core accounts request reaches recording transport once, extended accounts returns 404 with zero calls, capabilities calls zero transport in both modes, and a signed fixture quote remains 200.

- [ ] **Step 2: Run focused tests to verify failure**

Run: `UV_OFFLINE=1 uv run --package nexus-fosun-gateway pytest integrations/fosun_gateway/tests/security/test_principal_authorization.py integrations/fosun_gateway/tests/security/test_read_only_surface.py integrations/fosun_gateway/tests/security/test_read_only_transport.py integrations/fosun_gateway/tests/unit/test_composition.py integrations/fosun_gateway/tests/test_health.py -q`

Expected: FAIL because current HMAC authorizes coarse `fosun_sync_portfolio` before `PortfolioReadRequest` validation and startup has no selected scope/runtime authorization.

- [ ] **Step 3: Implement separate fine-grained authorization without widening SDK mapping**

Keep `GatewayOperation` and every Provider-backed route-to-`transport.execute` call unchanged. Import Task 1 `RuntimeOperation` into `policy/operations.py` and add an explicit mapping: accounts/orders/cash flows/quotes/bars map one-to-one; `fosun_sync_portfolio` derives only after `PortfolioReadRequest.model_validate` returns, mapping `cash` to `fosun_sync_portfolio:cash` and `holdings` to `fosun_sync_portfolio:holdings`. Implement `authorize_runtime_operation()` as a pure function: if `transport_mode == "fixture"`, return; if mode is `"read_only"` and `runtime_authorization is None`, raise `OPERATION_NOT_ALLOWED`; otherwise require its input runtime operation in both `actor.allowed_operations` and `runtime_authorization.authorized_operation_set`, raising the same denial on either absence. In `security/hmac.py`, migrate the two identity dataclasses and `_verification_identity()` principal rows to fine runtime sets only for read-only mode (broker: accounts, cash, holdings, orders, cash flows; market: quotes, bars); fixture mode retains coarse operation sets. Preserve signature/replay/listener behavior and remove only `if operation is not None and operation not in identity.allowed_operations`. In `_execute`, call `verify(request, body, expected_actor_id=_listener_actor(request))`, parse schema second, derive third, invoke `authorize_runtime_operation(derived, actor, app.state.runtime_authorization, settings.transport_mode)` fourth, then call `transport.execute` fifth. Invalid `read_kind` returns existing `VALIDATION_ERROR` before derivation; any helper denial returns existing non-disclosing `OPERATION_NOT_ALLOWED` and makes zero transport/provider dispatch. Make `fosun_capabilities` a separate HMAC-authenticated route: in read-only mode it returns a local `GatewayPage` containing one `CapabilityItem(kind="capability", provider="fosun", mode="read_only", operations=tuple(descriptor-final-order))` from the active authorization without calling `transport.execute`; in fixture mode it retains the existing fixture/static `CapabilityItem(mode="fixture")` behavior without runtime authorization. It is introspection only and cannot grant, derive, or widen any capability. Update `read_only_helpers._post_signed(client: TestClient, operation: str, payload: dict[str, object], **auth: object) -> Response` only for fine-runtime expected assertions; it remains the existing concrete signing helper, not a newly invented `signed_post` helper.

In settings require exact production `FOSUN_READ_ONLY_SCOPE_CONTRACT_VERSION=v1` and one explicit `FOSUN_READ_ONLY_SCOPE=core|extended` only when `transport_mode="read_only"`; absent/other values remain fixture validation. In `main.build_transport`, read exact evidence paths, invoke scope-aware activation, require non-null authorization and matching stored contract hash before importing `fsopenapi` or constructing SDK client/`SdkFosunTransport`, then return/bind that authorization. Preserve fixture mode and `FOSUN_SDK_ADMITTED=false` defaults. Do not implement core+extended composition.

- [ ] **Step 4: Run focused tests with offline socket guard**

Run:

```bash
set -euo pipefail
NEXUS_RUNTIME_TMP="$(mktemp -d "$PWD/.nexus-runtime-task4.XXXXXX")"
chmod 700 "$NEXUS_RUNTIME_TMP"
cleanup() { rmdir "$NEXUS_RUNTIME_TMP"; }
trap cleanup EXIT
TMPDIR="$NEXUS_RUNTIME_TMP" PYTHONDONTWRITEBYTECODE=1 \
  NEXUS_OFFLINE_SOCKET_GUARD=1 PYTHONPATH="$PWD/scripts/offline_guard" UV_OFFLINE=1 \
  uv run --package nexus-fosun-gateway pytest \
  integrations/fosun_gateway/tests/security/test_principal_authorization.py \
  integrations/fosun_gateway/tests/security/test_read_only_surface.py \
  integrations/fosun_gateway/tests/security/test_read_only_transport.py \
  integrations/fosun_gateway/tests/unit/test_composition.py \
  integrations/fosun_gateway/tests/test_health.py -q -p no:cacheprovider
```

Expected: PASS; cash-only and holdings-only both deny their opposite, core accounts succeeds while extended accounts is denied by runtime authorization, core denies orders/cash flows, extended denies accounts/cash/holdings/quotes/bars, invalid schema does not authorize, capabilities returns each scope’s exact final set without dispatch and grants none, fixture routes retain existing behavior without SDK/Provider access, mixed/legacy/missing startup evidence imports no SDK, and no socket opens.

- [ ] **Step 5: Remove only the explicit local test directory and commit runtime gates**

```bash
git diff --check
git add integrations/fosun_gateway/src/nexus_fosun_gateway/policy/operations.py integrations/fosun_gateway/src/nexus_fosun_gateway/policy/runtime_authorization.py integrations/fosun_gateway/src/nexus_fosun_gateway/security/hmac.py integrations/fosun_gateway/src/nexus_fosun_gateway/routes/read_only.py integrations/fosun_gateway/src/nexus_fosun_gateway/settings.py integrations/fosun_gateway/src/nexus_fosun_gateway/main.py integrations/fosun_gateway/tests/security/test_principal_authorization.py integrations/fosun_gateway/tests/security/read_only_helpers.py integrations/fosun_gateway/tests/security/test_read_only_surface.py integrations/fosun_gateway/tests/security/test_read_only_transport.py integrations/fosun_gateway/tests/unit/test_runtime_authorization.py integrations/fosun_gateway/tests/unit/test_composition.py integrations/fosun_gateway/tests/test_health.py
git commit -m "feat: gate Fosun requests by scoped runtime authorization"
```

### Task 5: Update policy/runbook and execute the complete offline acceptance harness

**Files:**
- Modify: `scripts/ci/repository_policy.py`
- Modify: `tests/policy/test_repository_policy.py`
- Modify: `docs/runbooks/fosun-read-only-activation.md`

**Interfaces:**
- Consumes the Task 4 production surface and exact `RuntimeOperation` derivation point.
- Produces `check_scoped_runtime_authorization(root: Path) -> list[PolicyFinding]`, called by repository-policy main, which rejects reintroduction of coarse pre-schema portfolio authorization, probe-only imports in runtime, writes, and changed production surface without reviewed hash updates.
- Produces an operator runbook that names one explicit scope, v3/v2/run-ID pairing and safe rollback to fixture mode, but contains no values, commands that invoke a probe, or activation claim.

- [ ] **Step 1: Write failing policy and runbook tests**

```python
def test_repository_policy_rejects_coarse_portfolio_authorization(tmp_path: Path) -> None:
    route = tmp_path / "integrations/fosun_gateway/src/nexus_fosun_gateway/routes/read_only.py"
    route.parent.mkdir(parents=True)
    route.write_text('if operation not in actor.allowed_operations: return await transport.execute(operation, {})')
    findings = repository_policy.check_scoped_runtime_authorization(tmp_path)
    assert [(item.rule, item.path) for item in findings] == [
        ("scoped_runtime_authorization_missing", "integrations/fosun_gateway/src/nexus_fosun_gateway/routes/read_only.py"),
    ]
```

- [ ] **Step 2: Run policy tests to verify failure**

Run: `UV_OFFLINE=1 uv run pytest tests/policy/test_repository_policy.py -q`

Expected: FAIL because there is no scoped-runtime authorization policy check.

- [ ] **Step 3: Implement minimal policy and documentation update**

Update `_EXPECTED_GATEWAY_SOURCE_SHA256` for exactly `policy/operations.py`, `policy/runtime_authorization.py`, `routes/read_only.py`, and `main.py` after their intended reviewed changes; add `security/hmac.py` to that mapping and make `check_gateway_read_allowlist()` hash all six named files, retaining `adapter/sdk.py`. Recompute `_EXPECTED_GATEWAY_INSTALLABLE_SRC_SHA256` after Tasks 1–4. Keep `_EXPECTED_FOSUN_GATEWAY_PYPROJECT_SHA256` unchanged because no Task edits `integrations/fosun_gateway/pyproject.toml`; add a test asserting an unchanged pyproject produces no pin finding and a changed pyproject still produces its existing finding. Preserve frozen SDK lock/manifest/write gates, then add the structural check requiring `verify(request, body, expected_actor_id=...)`, schema validation, `derive_runtime_operation`, `authorize_runtime_operation`, and `transport.execute` in order, plus a capabilities branch that cannot call transport. Add a test that `security/hmac.py` or `policy/runtime_authorization.py` absent from the reviewed mapping or altered after fixture hash setup yields the existing reviewed-source finding. Keep forbidden write scanning active over all Gateway source. Revise the runbook to require exactly one v3 artifact and v2 receipt with equal run ID/scope/version, describe legacy archive-only treatment, scope-specific least privilege, fixture-mode rollback after Gateway stop, and state that no fifth core probe is authorized by implementation acceptance.

- [ ] **Step 4: Run full required offline acceptance harness**

Run exactly:

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
TMPDIR="$NEXUS_POLICY_TMP" PYTHONDONTWRITEBYTECODE=1 UV_OFFLINE=1 \
  uv run pytest tests/policy -q -p no:cacheprovider
UV_OFFLINE=1 uv run python scripts/ci/repository_policy.py \
  --root . \
  --inventory-output "$NEXUS_POLICY_TMP/nexus-dependency-inventory.json"
UV_OFFLINE=1 uv run --package nexus-fosun-gateway ruff check \
  integrations/fosun_gateway/src integrations/fosun_gateway/tests
UV_OFFLINE=1 uv run --package nexus-fosun-gateway mypy integrations/fosun_gateway/src
git diff --check
```

Expected: Gateway, policy, repository policy, Ruff, mypy, and Git checks pass. The harness uses only checkout-local `0700` temporary directories, keeps the ambient umask unchanged, installs the offline socket guard, uses `UV_OFFLINE=1`, supplies required policy `--root`/`--inventory-output`, and removes only those two known temporary directories.

- [ ] **Step 5: Commit policy and operational instructions**

```bash
git add scripts/ci/repository_policy.py tests/policy/test_repository_policy.py docs/runbooks/fosun-read-only-activation.md
git commit -m "docs: define scoped Fosun activation procedure"
git show --check --stat HEAD
git status --short --branch
```

Expected: `git show --check` has no whitespace error and status is clean after the commit.

## Plan self-review

- [x] **Spec coverage:** Task 1 covers immutable core/extended descriptors, three closed sets, v3/v2 canonical run/scope binding, legacy/cross-scope rejection, signature/hash binding, and no scope composition. Task 2 covers selected physical sequences, process-local account binding, one-shot/budget/label behavior, and value-free frames. Task 3 covers protected create-only run/scope hierarchy, staging/quarantine, matching frame/artifact/receipt, and cross-scope preservation. Task 4 covers startup-before-SDK construction, immutable runtime authorization, exact feature gate, HMAC→schema→derived authorization→two intersections→transport order, and cash/holdings bidirectional denial. Task 5 covers repository policy, runbook, no writes/raw errors, and required offline harness.
- [x] **Review findings closure:** Task 1 defines `RuntimeOperation` in `probe_contract.py` before `ScopeDescriptor` and `RuntimeAuthorization`, and uses `FrozenInstanceError` plus a separate `MappingProxyType` write test. Task 2 gives mutually exclusive core/extended CLI tuples and no cross-scope `ProbePlan` field. Task 3 initializes its protected parent before first use, changes the real `probe_cli._execute_once()` publication path, and tests the existing run directory’s exact `PROBE_OUTPUT_EXISTS`. Task 4 adds the pure `policy/runtime_authorization.py` seam so read-only core/extended intersection tests do not use `GatewaySettings.for_test()`, includes `security/hmac.py`, existing `_post_signed`, a broker identity set containing `fosun_sync_accounts`, scope-exact `CapabilityItem` JSON assertions, and fixture regression without runtime authorization or Provider access. Task 5 uses `PolicyFinding`, enumerates the six reviewed source hashes/tree hash/unchanged pyproject hash, and gives every pytest checkout-local `TMPDIR`.
- [x] **Placeholder and helper scan:** No `TBD`, `TODO`, “implement later”, “appropriate error handling”, “similar to Task”, ellipsis, undefined `signed_post`, or unspecified test helper remains; `protected_absolute_parent_with_runs`, `CORE_ARGUMENTS`, `EXTENDED_ARGUMENTS`, and `_post_signed` are defined or identified in their exact test file.
- [x] **Type consistency:** `RuntimeOperation` is defined only in Task 1 `probe_contract.py`; `activation.py`, `policy/operations.py`, `policy/runtime_authorization.py`, `security/hmac.py`, routes, and tests import it. The pure helper has all four inputs (`RuntimeOperation`, `VerifiedGatewayActor`, `RuntimeAuthorization | None`, and transport mode) and returns or raises the existing denial before any transport call. `fosun_capabilities` is deliberately outside that seven-member final type, is not passed to transport in read-only mode, and is not an authorization source. `ProbeScope`, `ScopeDescriptor`, `RuntimeAuthorization`, `expected_scope`, `run_id`, and `scope_contract_version` are produced before consumption; the coarse `GatewayOperation` remains the Provider transport type for non-capabilities routes and fixture compatibility.
- [x] **Path and command check:** Every production/test/runbook/policy path listed exists in the baseline; the acceptance commands use existing `uv`, pytest, repository-policy, Ruff, mypy, and Git entry points. All pytest commands use checkout-local `mktemp` directories with `0700`, traps clean only those exact directories without changing umask, and no command targets system `/tmp` or runs a Provider/probe/capture command.

## Execution handoff

Plan complete and saved to `docs/superpowers/plans/2026-09-01-fosun-readonly-scope-contract.md`. Two execution options:

1. Subagent-Driven (recommended) - dispatch a fresh subagent per task, review between tasks, fast iteration.

2. Inline Execution - execute tasks in this session using executing-plans, batch execution with checkpoints.

The fifth core probe is not an execution task. It may be considered only after code merge and independent acceptance, by the main session with a new explicit external authorization.
