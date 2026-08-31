# NEXUS Fosun Sanitized Read-Only Probe Runner Design

**Status:** Approved by the standing NEXUS autonomous-design authorization and the explicit
read-only activation approval on 2026-08-31. This specification authorizes local implementation
and offline verification only. It does not admit the SDK, change the provider manifest, install an
unlicensed dependency, call a Fosun business endpoint, deploy a workload, or authorize any order.

## Problem

NEXUS started with a transport-free activation evaluator for
`nexus.fosun.sanitized-probe.v1`, but it had no executable component that could produce an artifact;
this design cuts the accepted contract over to v2 plus a detached publication receipt.
The production Gateway is a long-running service with seven internal read-only routes; it is not a
bounded probe orchestrator. Starting it manually would not enforce the artifact's eight-label
closed set, per-label request count, total request budget, immediate-stop behavior, or value-free
serialization contract.

The activation audit also found independent external blockers:

- the committed provider manifest remains `blocked`, with an unresolved SDK license, no exact
  production base URL, and four empty evidence references;
- `fsopenapi` is intentionally absent from the workspace lock;
- the official Trade OpenAPI bytes have changed since the frozen review, so their manifest digest
  cannot be refreshed without a new contract/security review;
- rotated credentials, fixed-egress allowlist evidence, and the Provider runtime infrastructure are
  not present in the approved secret domain.

The runner must close only the missing local orchestration gap. It must preserve all of those stops.

## Chosen approach

Add a separate one-shot CLI inside `nexus-fosun-gateway`. It reuses the reviewed SDK client shape,
TLS-remediation, dependency-log suppression, normalization, timeout, and operation boundaries, but
never opens an HTTP listener. A dedicated `build_probe_transport` is the only construction path
allowed to omit an activation artifact: it accepts the already-read admitted manifest bytes,
installs the physical-HTTP budget before any SDK method can run, and returns a one-shot transport
that cannot be mounted in the production Gateway. The CLI evaluates immutable in-memory manifest
bytes before constructing provider settings, reading secrets, or importing `fsopenapi`. A blocked
or stale manifest therefore remains a transport-free exit.

Rejected alternatives:

1. **Use the production Gateway as the probe.** Rejected because it cannot prove the closed
   operation sequence or bounded artifact and requires deployment identities unrelated to a
   one-shot provider test.
2. **Keep an operator script outside the repository.** Rejected because its request surface,
   secret handling, and output cannot be tested, reviewed, or reproduced.
3. **Call the OpenAPI directly without the official SDK.** Rejected because independently
   reimplementing ECDH, signing, session renewal, and encryption would create a second unreviewed
   security implementation.
4. **Count only SDK method dispatches.** Rejected because the frozen SDK's authentication request
   and any internal HTTP retry occur below that edge. A real request bound must be enforced at both
   reviewed HTTP proxies immediately before socket dispatch.

## Scope and non-goals

The implementation adds the offline-safe runner framework and its production adapter seam. It does
not:

- modify `contracts/providers/fosun/manifest.json` or its frozen hashes;
- add `fsopenapi` to `pyproject.toml` dependencies or `uv.lock`;
- infer a license or production URL from public source examples;
- place secrets in command-line arguments, files in the repository, logs, exceptions, or output;
- probe order mutation, simulation, subscription, Webhook, wealth, or generic SDK methods;
- create the replay volume, mTLS identities, database roles, object-store policies, or deployment;
- change Alpaca mode, live-trading state, risk rules, or order approval.

After external admission evidence exists, a separate reviewed change may refresh the OpenAPI
manifest, add the exact pinned SDK dependency, and execute this runner during one explicitly timed
probe window.

## Components

### `probe_settings.py`

`ProbeProviderSettings` is an environment-only Pydantic settings model for the one-shot process. It
contains only:

- `FOSUN_PROVIDER_MANIFEST`;
- `FOSUN_SDK_ADMITTED`;
- `FOSUN_PROBE_OUTPUT_DIR`;
- `FOSUN_PROBE_PUBLISHER_PRIVATE_KEY_FILE`;
- `FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U`;
- `FSOPENAPI_BASE_URL`;
- `FSOPENAPI_API_KEY`;
- `FSOPENAPI_CLIENT_PRIVATE_KEY`;
- `FSOPENAPI_SERVER_PUBLIC_KEY`.

Secret values use `SecretStr`, input values are hidden from validation errors, and `repr` exposes
only the manifest path and boolean admission state. The CLI constructs this model only after the
single bounded manifest read passes `sdk_admission_document_is_valid`. Before settings construction,
the CLI reads only the explicit non-secret, absolute, length-bounded `--manifest` path, opens it
once, and retains the immutable bytes. The
settings value must match that captured spelling; the resolved base URL must match the admitted
value parsed from those exact bytes byte-for-byte. Neither admission nor transport construction may
re-open the manifest path.

### `adapter/probe_sdk.py`

`build_probe_transport` is a probe-only factory and is not imported by `main.py`. It requires the
admitted in-memory document, exact `ProbeProviderSettings`, and a fresh `PhysicalHttpBudget`; only
then may it lazily import the pinned SDK, create a client with dependency/body logging disabled,
replace both HTTPS adapters with an explicitly zero-retry adapter, and install both reviewed TLS
proxies. Redirects remain disabled. The factory inspects the mounted adapters and fails unless every
transport retry dimension is zero, both proxies expose the physical pre/post-dispatch hooks, and
both sessions are closable. Therefore every permitted retry must re-enter a counted proxy; neither
`requests` nor urllib3 may retry below it. Tests and repository policy keep this exception out of
the long-running production composition root.

`SdkProbeExecutor` owns the real one-shot sequence. Its constructor accepts the factory result, its
`PhysicalHttpBudget`, and a UTC clock. It never accepts a generic callable, URL, path, SDK method
name, pre-built unbudgeted SDK client, or production Gateway transport from the operator.

The exact logical sequence is:

1. `Session` — the physical authentication attempts observed on the isolated auth proxy while the
   fresh client performs `Accounts`;
2. `Accounts` — `fosun_sync_accounts`;
3. `CashSummary` — `fosun_sync_portfolio` with `read_kind=cash`;
4. `Holdings` — `fosun_sync_portfolio` with `read_kind=holdings`;
5. `OrderList` — `fosun_list_orders`;
6. `CashFlows` — `fosun_list_cash_flows`;
7. `quote` — `fosun_get_quotes` for one approved public test symbol;
8. `kline_minimal` — `fosun_get_bars` for exactly one requested provider day-bar identity.

The first normalized `AccountItem.external_account_id` is retained only in memory and supplied to
the next four calls. It is never returned by the executor. Zero readable accounts is a fixed
`PROBE_ACCOUNT_UNAVAILABLE` stop and creates no artifact.

`PhysicalHttpBudget` is installed into both exact proxies created by
`enforce_sdk_tls_verification`: the isolated authentication `post` surface and the SDK client's
business `request` surface. The hook executes synchronously immediately before each delegate call,
including authentication, SDK-internal retry, and Gateway transport retry. It atomically attributes
the attempt to the executor's one current fixed label, waits for one-QPS pacing, then consumes the
following hard caps before any bytes can be sent:

- at most three physical HTTP attempts for any artifact label;
- at most 24 physical HTTP attempts total and within any rolling minute;
- at most six authentication plus broker-domain attempts in a rolling minute;
- at most 24 market-domain attempts in a rolling minute.

`Session` is counted only when the authentication proxy is actually about to dispatch. A fresh
client must produce at least one and no more than three authentication attempts during `Accounts`.
The auth proxy records only whether a fully bounded response had an integer 2xx HTTP status; it
never parses, retains, or exposes the body. `Session.passed=true` requires exactly one 2xx auth
result and a successfully normalized `Accounts` page. Zero/multiple 2xx results, authentication
outside the `Accounts` operation, or successful Accounts without an observed auth attempt is
`SDK_CONTRACT_MISMATCH`; failed attempts still consume the Session/broker/total budgets and no
artifact is written. A retry consumes the same three-per-label and broker/total budgets.
Every business request is attributed to the active artifact label. The ordinary SDK call-edge
limiter is a fail-closed assertion that a physical hook is installed and a label is active; it does
not claim to count network attempts. Missing/overlapping labels, a request on the wrong proxy, or a
budget overrun is rejected before delegate invocation. The provider-confirmed test vector remains
required evidence about session semantics and the absence of write behavior, but never substitutes
for the runtime physical cap.

The executor is strictly sequential and never follows pagination. The `pagination` check means
only “provider-confirmed cursor semantics plus a live normalized first-page structural check”; it
does not claim that a live next cursor was traversed. Any future multi-page probe is a separate
review because it changes broker budgets.

### `probe.py`

`ProbePlan` accepts only non-secret operational inputs:

- one canonical symbol matching `us[A-Z][A-Z0-9.]{0,14}`;
- one provider day-bar session date strictly before probe start; the request derives the canonical
  inclusive pair `<date>T00:00:00Z` / `<date>T00:00:00Z` and `limit=1`;
- one UTC cash-flow date window no longer than seven calendar days; order listing has no date-window
  claim because the frozen route exposes only cursor and limit;
- list/page limit from 1 through 10;
- requested capability TTL from 60 through 86,400 seconds;
- an output basename matching `^[a-z0-9][a-z0-9._-]{0,62}\.json$`.

No API key, account ID, endpoint, certificate, or provider response can be supplied on the command
line. The provider base URL and cryptographic material come only from the Gateway secret domain.
The output parent is the explicitly configured absolute `FOSUN_PROBE_OUTPUT_DIR` secret-domain
directory; it is not accepted as a CLI argument and has no implicit home-directory default. It must already
exist, be an absolute non-symlink path, be owned by the effective UID, and have exact mode `0700`.

The session date is a provider day-bar identity check, not an XNYS-calendar attestation. A successful
page must contain exactly one `BarItem` whose UTC date equals the requested identity. The Engine's
independent point-in-time market calendar remains responsible for deciding whether that date is a
completed US exchange session before facts can support Agent, backtest, paper, or order authority.

`run_sanitized_probe` executes the fixed sequence once. Each successful normalized page produces a
contract digest from a hand-written, versioned, canonical JSON allow-list constant containing only
the probe schema version, logical label, Gateway page field/type contract, and expected item
field/type contract. Runtime Pydantic schema generation, model metadata/default/example values,
reflection, observed cardinality, and live objects are forbidden digest inputs. The digest does not
depend on item count, values, account state, holdings, symbols returned by the provider, or raw
response bytes.
Live data is used only for deterministic assertions before being discarded:

- normalization succeeds for every response;
- pagination invariants are accepted by existing normalizers;
- the daily bar page contains exactly one bar with the requested provider session identity under
  the admitted inclusive-end contract;
- no rate-limit/permission/contract Gateway error occurs;
- the full operation sequence remains inside the counted budgets.

The six artifact checks are derived as follows:

- `permission_read_only`: admitted written-permission/test-vector evidence plus the compile-time
  closed runner and Gateway operation sets;
- `history_window`: the returned daily bar contains exactly the requested provider session identity;
- `adjustment_semantics`: the admitted provider test vector is present and live bars pass the
  frozen power/scaling normalizer;
- `pagination`: admitted provider-test-vector evidence exists and every live first page has
  internally consistent cursor state; no live continuation is implied;
- `rate_limit`: the attempt counter stays within all hard caps and no rate-limit error occurs;
- `response_contract`: all eight logical outcomes pass their fixed normalized contract.

The capability clock begins before SDK construction. `captured_at` is the completion time and
`capability_expires_at` is start plus the requested TTL. If the probe finishes at or after expiry,
it fails without an artifact, so runtime authorization can never exceed the requested window.

Only after the bounded SDK executor and value-free evidence validation succeed, immediately before
artifact construction, the runner loads one pre-provisioned Ed25519 private seed from the probe-only
secret domain. A failed probe never reads it. The file must be an absolute no-follow regular file, owned by the effective
UID, mode `0600`, one link, exactly 43 canonical base64url bytes, under an effective-UID `0700`
directory. Its derived public key must match the independently change-controlled
`FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U` trust anchor. There is no generated or artifact-
selected trust root. This is a deliberate probe schema cutover from `nexus.fosun.sanitized-probe.v1` to
`nexus.fosun.sanitized-probe.v2`; no real v1 artifact has ever been admitted, and all v1 artifacts
are now rejected as `REVIEW_PROBE_ARTIFACT`. The v2 artifact adds exactly one field,
`publication_public_key_b64u`, whose value must match `^[A-Za-z0-9_-]{43}$` and decode without
padding to exactly 32 raw Ed25519 public-key bytes. This field declares the signer identity but is
never itself trusted by activation. The private key is never serialized or passed to the Gateway.

After every artifact and directory publication step has returned success, the runner signs exactly:

`b"NEXUS-FOSUN-PROBE-PUBLICATION-V1\\0" + bytes.fromhex(artifact_sha256)`

The detached receipt is canonical compact sorted-key JSON with no trailing newline and exactly these
fields (`extra=forbid`):

- `schema_version = "nexus.fosun.probe-publication.v1"`;
- `artifact_sha256`: lowercase 64-hex digest of the exact v2 artifact bytes;
- `signature_b64u`: unpadded URL-safe base64 matching `^[A-Za-z0-9_-]{86}$` and decoding to exactly
  64 Ed25519 signature bytes.

The receipt exists only inside the final success result. A failure or interruption before signing
destroys the process-local private key without producing a receipt. Once signature bytes have begun
writing to stdout they cannot be revoked; a later stdout failure is therefore reported as
`PROBE_RECEIPT_DELIVERY_UNCERTAIN`, and the already durable artifact is not automatically configured
or activated. Any complete receipt recoverable from that single frame can validate only that exact
durable artifact, never a quarantined or different artifact.

### CLI and output

The new console entry point is `nexus-fosun-read-only-probe`. Its control flow is fixed:

1. open the manifest once without following a terminal symlink, read at most 1 MiB, retain its
   descriptor identity and immutable bytes, and never resolve it again during this run;
2. validate full SDK admission and parse the exact base URL/SDK subject from those bytes;
3. parse only the non-secret plan arguments;
4. construct `ProbeProviderSettings`, require the settings path spelling to match the captured
   manifest input,
   and require exact base-URL equality;
5. lazily import `fsopenapi`, construct `SDKClient` with logging/body logging disabled, and apply
   both reviewed TLS wrappers with the physical pre-dispatch budget hook;
6. execute the fixed sequence;
7. validate the generated bytes through the contract-only v2 validator;
8. durably publish the output exactly once inside the controlled directory using only held dirfds;
   only this publisher can return the process-local durable-publication attestation accepted by the
   signer;
9. only after all publication fsyncs succeed, use that attestation once to sign the exact artifact
   digest and evaluate the manifest/artifact/receipt/trusted-public-key tuple in memory;
10. print a fixed result object containing status, safe code, output SHA-256, byte length, capability
    expiry, activation contract hash, and detached publication receipt.

Before admission, steps 4 through 9 are unreachable. `fsopenapi` absence after admission is a fixed
`SDK_NOT_AVAILABLE` error. Provider, validation, timeout, filesystem, and unexpected exceptions are
mapped to fixed codes without formatting or logging the exception. The CLI disables argparse's
default usage/error rendering and never echoes an operator string. It builds exactly one canonical
compact fixed-field-order JSON frame in memory, with `publication_receipt` physically last, and sends
it through a low-level EINTR-aware write loop. No later stdout byte is permitted.
Exit 0 occurs only after the complete frame is written; a short/error write exits 2 as
`PROBE_RECEIPT_DELIVERY_UNCERTAIN` through the process status (without attempting a second stdout
frame), and stderr stays empty. `KeyboardInterrupt`/cancellation is the sole ordinary error
contract exception: after refusing all new operations and closing both SDK HTTP sessions, the CLI
emits fixed code `PROBE_INTERRUPTED` and exits 130. The console process exit is the final abort
boundary for a poisoned daemon request; in-process callers must wait for the existing ten-second
socket bound and may not start another operation.

The output publisher opens every absolute parent component with `O_DIRECTORY|O_NOFOLLOW|O_CLOEXEC`,
verifies the final held directory is owned by the effective UID with exact mode `0700`, and keeps its
dirfd for the full transaction. It creates an unpredictable temporary basename with
`openat(O_WRONLY|O_CREAT|O_EXCL|O_NOFOLLOW|O_CLOEXEC, 0600)`, verifies by `fstat` that the fd is one
regular current-UID file with exact mode `0600`, writes canonical bytes, and fsyncs the fd. It then
publishes create-only with a same-dirfd hard link to the requested basename, fsyncs the directory,
unlinks the temporary name, and fsyncs the directory again. No cleanup path re-resolves an absolute
pathname. A pre-existing final name, symlink component, untrusted parent, or unsupported atomic
primitive fails closed.

Failure before final publication leaves no final artifact. A failure after publication performs
best-effort unlink through the held dirfd and a second directory fsync. If removal/durability cannot
be proved, the fixed code is `PROBE_OUTPUT_QUARANTINE_REQUIRED`; the possible final file is not
claimed absent. Crucially, the failure path never signs or emits the detached publication receipt,
so the residual bytes are machine-ineligible for activation. This is an explicit filesystem-failure
stop, not a successful probe.

The artifact is canonical compact JSON with no trailing newline and validates as
`SanitizedProbeArtifact`. An output file is written only when all eight records and six checks pass.
Failed provider/contract probes may produce fixed stdout status but never create an output file.
Filesystem publication failures follow the explicit quarantine rule above; the system never labels
an uncertain file durable or automatically consumes it.

`evaluate_activation(manifest_bytes, probe_bytes, publication_receipt_bytes, *,
trusted_publication_public_key_bytes=..., checked_at=...)` and
`evaluate_activation_paths(manifest_path, probe_path, publication_receipt_path, *,
trusted_publication_public_key_bytes=..., checked_at=...)`
require the detached receipt before they can return `activated_read_only`. A missing receipt returns
`PROVIDE_PROBE_PUBLICATION_RECEIPT`; malformed, unsigned, digest-mismatched, or cross-artifact replay
returns `REVIEW_PROBE_PUBLICATION_RECEIPT`. A missing/malformed external trust anchor returns
`PROVIDE_TRUSTED_PROBE_PUBLISHER`/`REVIEW_TRUSTED_PROBE_PUBLISHER`; an artifact declaring a different
key returns `REVIEW_PROBE_PUBLISHER_BINDING`. The activation contract hash binds the raw receipt and
trusted-anchor SHA-256 values in addition to the manifest, SDK subject, result schema, and artifact SHA-256. The pure
contract-only validator used before publication can validate v2 artifact structure/subject/checks
but cannot return an activation result.

The activation CLI adds required-for-activation `--publication-receipt` and the explicitly supplied
non-secret `--trusted-publisher-public-key-b64u`; its result is a local evaluation, not publisher
provenance or deployment approval. Production settings add absolute
`FOSUN_ACTIVATION_PROBE_RECEIPT`, bounded to 16 KiB at read time,
`FOSUN_ACTIVATION_PROBE_PUBLISHER_PUBLIC_KEY_B64U_FILE`, and
`FOSUN_ACTIVATION_CONTRACT_HASH_FILE`. The last two files contain exactly 43 canonical base64url
bytes and 64 lowercase hexadecimal bytes, with no newline. Artifact/receipt paths and the
independently managed control files are required in read-only production mode before SDK import. The
Gateway never derives, scans for, or accepts a receipt from the artifact directory. The receipt is
separately provisioned from a verified exit-0 runner result; it contains no account/market values but
is treated as capability evidence.

The checked-in Compose baseline mounts artifact and receipt as independent read-only file binds with
`create_host_path: false`. It provides the publisher pin and contract hash through separate
file-backed configs under `/run/nexus/fosun-control`; no raw pin/hash value is accepted by the
Gateway environment. The one-shot runner remains outside the long-running Gateway composition and
never receives the approved contract-hash config, replay volume, or promoted evidence paths.

## Security invariants

- Manifest admission precedes secret settings construction and SDK import.
- The publisher private key and Gateway public trust anchor are separate permission domains. The
  deployment/configuration principal controlling the public anchor is an explicit trust root;
  anchor rotation requires a new artifact, receipt, contract hash, review, and restart.
- Artifact, receipt, and manifest inputs are descriptor-first, terminal/intermediate no-follow,
  regular, root/current-UID owned, non-group/world-writable, bounded, and stable across one read.
- Admission, base-URL extraction, SDK subject binding, and artifact binding use the same immutable
  manifest byte string; no post-admission path re-read is allowed.
- The CLI has no verbose/debug mode and never enables dependency logging.
- The runner never serializes `GatewayPage`, SDK objects, exception text, URLs, request IDs, account
  IDs, symbols returned by the provider, prices, quantities, balances, holdings, order data, or raw
  response bodies.
- The structural digest input is static code-owned schema, not observed values or cardinality.
- Every physical request, including authentication and SDK-internal retries, is pre-counted at one
  of the two reviewed TLS proxies before delegate invocation.
- Both underlying HTTPS adapters have all automatic retry dimensions fixed to zero, so no socket
  attempt can occur below the counted proxy edge.
- Only the seven existing Gateway operations are callable; `Session` is an evidence label, not a
  generic SDK route.
- All operations are sequential. A failed/expired/poisoned transport stops immediately.
- Output is create-only beneath a held trusted `0700` dirfd and cannot overwrite a symlink or
  previous probe artifact; uncertain post-publication durability has no detached signature and is
  rejected by activation code even if complete artifact bytes remain.
- Repository policy continues to reject write-operation tokens in production Gateway surfaces and
  continues to require an exact admitted SDK/lock subject before production use.
- The runner does not prove production DB, object-store, mTLS, HMAC, replay-volume, or deployment
  readiness; those remain separate post-probe gates.

## Error contract

The CLI returns `0` only after a validated artifact is durably created. All blocked or failed paths
return `2` with one of these safe codes:

- `COMPLETE_SDK_ADMISSION`
- `INVALID_PROBE_PLAN`
- `PROBE_SETTINGS_INVALID`
- `SDK_NOT_AVAILABLE`
- `SDK_CONTRACT_MISMATCH`
- `PROBE_ACCOUNT_UNAVAILABLE`
- `PROBE_BUDGET_EXCEEDED`
- `PROBE_RESPONSE_INVALID`
- `PROBE_WINDOW_MISMATCH`
- `PROBE_CAPABILITY_EXPIRED`
- `PROBE_OUTPUT_EXISTS`
- `PROBE_OUTPUT_INVALID`
- `PROBE_OUTPUT_QUARANTINE_REQUIRED`
- `PROBE_RECEIPT_DELIVERY_UNCERTAIN`
- `PROBE_FAILED`

The status payload has no free-form message or exception field. `PROBE_INTERRUPTED` exits 130 and is
not one of the ordinary exit-2 failures.

## Verification

TDD must cover:

- blocked manifest exits before provider settings construction, SDK import, or transport factory;
- exact sequence and one-shot stopping behavior;
- authentication, hidden SDK retries, and Gateway retries counted at the two physical HTTP proxies,
  with zero-retry underlying HTTPS adapters, per-label/total/domain caps, one-QPS pacing, and zero
  delegate calls after refusal;
- no sensitive canary appears in artifact bytes, stdout, stderr, caplog, traceback, or filename;
- empty accounts, malformed normalized response, wrong K-line session, expiration, rate limit,
  permission error, timeout, transport poison, and operator cancellation;
- exact artifact schema, constant contract digests, activation-gate acceptance, held-dirfd `0600`
  create-only output, adversarial parent/final symlink and rename refusal, staged-failure cleanup,
  and post-publication failure rejection without a detached Ed25519 receipt;
- publication receipt signature/digest verification, cross-artifact replay rejection, missing-receipt
  rejection in both activation CLI and production composition, and no signature before every file
  and directory fsync succeeds;
- subprocess and injected low-level short writes before, within, and after the receipt prove nonzero
  exit/no second frame; a recoverable complete receipt remains bound only to the already-fsynced
  exact artifact;
- lazy pinned-SDK import, logging disabled, both TLS wrappers applied, and no fallback transport;
- interruption during authentication/business HTTP closes both sessions, starts no later operation,
  leaves no ordinary output, and exits with fixed code 130;
- complete Gateway regression, Ruff, Mypy, repository policy, secret scan, and independent code and
  security review.

No test may use a real credential, account response, network socket, or provider request.

## Rollback

Before external activation, rollback is deletion of the runner commit or disabling its console
entry point; no provider or account state exists. After a future probe, stop the one-shot process,
preserve a successfully published sanitized artifact and fixed status metadata, quarantine any
uncertain unsigned output by inode within the controlled directory, preserve the matching detached
receipt only for a successful run, revoke/rotate provider credentials via the secret manager if
exposure is suspected, and do not start the production Gateway. No rollback operation calls a
broker write API.
