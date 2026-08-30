# NEXUS Fosun-first Market Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:test-driven-development` for every behavior change and `superpowers:verification-before-completion` before any completion claim.

**Goal:** Make the already-approved Fosun-first US-market policy executable through an isolated market-only gateway identity, immutable/PIT provider authority, daily and intraday evidence, Agent price authority v2, and explicit UI quality, while keeping Alpaca installed but disabled by default.

**Architecture:** In Fosun mode, the Provider Worker calls only `fosun_get_quotes` and `fosun_get_bars` through a fixed-origin mTLS/HMAC-v2 client. The gateway derives principal and operation grants from configured key IDs, never from an unsigned header. Every Fosun call freezes and revalidates database authority at actual pre/post times; normalized facts carry the exact capability/status binding and are admitted by new immutable v2 policies. Alpaca remains a separately gated adapter that can run only when explicitly approved and enabled, with no automatic fallback.

**Tech Stack:** Python 3.12, FastAPI, Pydantic 2, httpx, SQLAlchemy/Alembic, PostgreSQL 17, pytest/Hypothesis, React/vinext, TypeScript, Vitest, Playwright, Docker Compose static validation.

**Spec:** `docs/superpowers/specs/2026-08-29-nexus-fosun-market-primary-design.md`

## Global Constraints

- Never read, print, reuse, or request the revoked key or any real credential value.
- No real Fosun/Alpaca/account call, permission grant, paid activation, deployment, broker write, simulation write, or order.
- Live trading remains off and Agent/LLM code never receives provider/gateway credentials or order capability.
- Root agent owns integration for `.worktrees/nexus-fosun-market-primary`; scoped agents may write only explicitly assigned, non-overlapping files and never commit.
- Every production behavior begins with one focused failing test, the expected failure is observed, then the minimum implementation is added.
- Pre-0027 historical migrations and v1 policy rows/functions are never edited; 0027 adds v2 behavior and preserves v1 replay, while forward-only 0028/0029 harden authority execution and transport lineage without rewriting v1 facts.
- Existing user work and sibling worktrees are untouched.
- Each task ends with focused tests and a local commit. No push or deployment.

---

## Task 1: Freeze the architectural correction and traceability

**Files:**
- Create: `docs/superpowers/specs/2026-08-29-nexus-fosun-market-primary-design.md`
- Create: `docs/superpowers/plans/2026-08-29-nexus-fosun-market-primary.md`
- Modify: `docs/adr/0002-fosun-gateway-boundary.md`
- Modify: `docs/superpowers/plans/2026-08-28-nexus-runtime-real-data.md`

- [x] Update ADR 0002 from “broker-worker only” to two independent caller identities and HMAC v2; keep SDK admission blocked.
- [x] Correct the real-data plan’s wrong spec pointer and state that it implemented Alpaca as an adapter, not the final source selection.
- [x] Run `git diff --check` and a link/path scan.
- [x] Commit: `docs(architecture): freeze fosun-first market runtime`

## Task 2: Enforce gateway principals, HMAC v2, and strict market routes

**Files:**
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/settings.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/security/hmac.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/policy/operations.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/routes/read_only.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/adapter/normalization.py`
- Modify: `integrations/fosun_gateway/src/nexus_fosun_gateway/adapter/sdk.py`
- Modify: `engine/src/nexus/providers/broker/gateway_client.py`
- Modify: gateway and broker gateway tests/helpers under `integrations/fosun_gateway/tests/**` and `engine/tests/**/test_gateway_client.py`

- [x] RED: add the complete broker/provider current/previous × operation authorization matrix; assert actor/request-id tampering, v1, duplicate cross-role identities, and non-production read_only all fail before transport.
- [x] GREEN: add `canonical_hmac_v2`, signed version/actor/request-id, configured principal registry, exact actor match, and operation authorization immediately after verification.
- [x] RED: add invalid quote code/set and bars pair/window/order/OHLC/volume/limit tests plus activation-expiry-after-call test.
- [x] GREEN: enforce strict request models and post-response normalizer invariants; recheck activation after SDK return.
- [x] Update the existing broker client to HMAC v2 without adding any market path.
- [x] Run the full gateway suite and focused engine gateway-client tests.
- [x] Commit: `fix(gateway): isolate signed market principals`

## Task 3: Add immutable Fosun provider binding and authority v2

**Files:**
- Create: `engine/alembic/versions/0027_fosun_market_authority.py`
- Create: `engine/alembic/versions/0028_fosun_authority_hardening.py`
- Create: `engine/alembic/versions/0029_fosun_bar_transport_freshness.py`
- Modify: `engine/src/nexus/persistence/models/data.py`
- Modify: `engine/src/nexus/persistence/models/market.py`
- Modify: `engine/src/nexus/persistence/migrations.py`
- Modify: `engine/src/nexus/quality/source_policy.py`
- Modify: `engine/src/nexus/market/price_bar_authority.py`
- Modify: `engine/src/nexus/market/intraday_authority.py`
- Modify: migration helpers and integration tests under `engine/tests/integration/**`

- [x] RED: migration tests require the five binding columns on manifests/quotes, immutable v2 policy rows, complete tuple constraints, Provider binding triggers, and unchanged v1 replay.
- [x] RED: direct SQL tests reject missing/mismatched capability, status, operation, hash, version, scope, future-created/expired authority and post-invalidation binding.
- [x] GREEN: add 0027 for the v2 contract; create `source_policy_v2`, `quote_source_policy_v2`, `intraday_monitor_policy_v2`, price/intraday canonical/prepare/validate v2 functions and fail-closed downgrade guards. Add forward-only 0028/0029 remediation for pinned authority schemas, restricted-role publication marking, advisory-lock symmetry, gateway transport lineage and freshness; do not rewrite pre-0027 migrations.
- [x] GREEN: update ORM and finalizers to select v1 for existing Alpaca/fixture and v2 for Fosun only.
- [x] Update Agent snapshot validation to accept exact v2 Fosun price authority while retaining every existing research and v1 guard.
- [x] Run migration/schema/authority/PIT integration tests with PostgreSQL.
- [x] Commit: `feat(db): add fosun market authority v2`

## Task 4: Build the fixed Fosun market client and normalizers

**Files:**
- Create: `engine/src/nexus/providers/market/fosun_gateway.py`
- Create: `engine/src/nexus/providers/market/fosun.py`
- Create: `engine/tests/unit/providers/test_fosun_market_gateway.py`
- Create: `engine/tests/unit/providers/test_fosun_market.py`
- Modify: `engine/src/nexus/providers/base.py`
- Modify: `engine/src/nexus/providers/market/snapshot_contracts.py`
- Modify: `engine/src/nexus/providers/market/alpaca_snapshot.py`
- Modify: shared provider test fakes as needed

- [x] RED: fixed-origin HTTPS, mTLS, HMAC v2, `trust_env=False`, no redirects, exact two market paths, body/response caps, strict media type, safe errors and secret-redaction tests.
- [x] GREEN: implement a market-only client that cannot express account/order operations and returns exact gateway response bytes plus strict typed pages.
- [x] RED: test `AAPL -> usAAPL` PIT mapping, quote set equality, Decimal preservation, receipt-only timestamp flag, unknown delivery, complete minute bars, daily bars and future/window rejection.
- [x] GREEN: implement Fosun daily `MarketDataProvider` and raw-first intraday provider/normalizer; preserve bid/ask as null and never synthesize spread.
- [x] Make the intraday handler call provider-owned normalization rather than importing Alpaca normalization directly.
- [x] Run the new provider tests, Alpaca regression tests, Ruff and Mypy for touched modules.
- [x] Commit: `feat(provider): add fosun market gateway adapter`

## Task 5: Freeze authority in daily and intraday jobs

**Files:**
- Create: `engine/alembic/versions/0030_fosun_daily_staging.py`
- Create: `engine/src/nexus/market/artifact_authority.py`
- Modify: `engine/src/nexus/jobs/provider_collection.py`
- Modify: `engine/src/nexus/jobs/intraday_schedule.py`
- Modify: `engine/src/nexus/jobs/intraday_collection.py`
- Modify: `engine/src/nexus/market/ingestion.py`
- Modify: `engine/src/nexus/market/intraday_ingestion.py`
- Modify: `engine/src/nexus/market/object_store.py`
- Modify: `engine/src/nexus/persistence/models/data.py`
- Modify: unit/integration job and ingestion tests
- Modify: artifact-authority and object-store tests

- [x] RED: capability `created_at > check_time`, expiry, wrong status/hash/version, and wrong operation prevent scheduling/network.
- [x] RED: actual pre/post clock drift archives exact transport evidence but creates zero normalized facts; claim effective time cannot mask a mid-call change.
- [x] GREEN: add frozen provider authority payload fields and v2 schema; compare exact authority snapshots before and after fetch.
- [x] GREEN: persist the five binding fields on Fosun facts/manifests and use source-specific raw schema versions.
- [x] RED/GREEN: batch quotes every 15 seconds, minute bars only on minute boundary, Fosun max 10 symbols, completed bars only, bounded deterministic envelope and idempotent replay.
- [x] RED/GREEN: validate but strip Alpaca `minuteBar` on non-minute buckets so the single Snapshot still publishes trade/quote without a bar authority.
- [x] RED/GREEN: bind every admitted intraday bar revision to the exact raw SHA-256; prove distinct Alpaca raw snapshots with identical minute OHLCV do not collapse while exact raw replay remains idempotent.
- [x] RED/GREEN: fan out Fosun daily bars into one max-attempt-one external-read Job per instrument, stagger at no more than 10/min, persist raw-only immutable staging, and publish all expected instruments in one aggregate transaction only after exact-set/hash/authority revalidation.
- [x] RED/GREEN: pace the actual Gateway call, not an earlier reservation; perform slow alias/authority revalidation first, then durably reserve immediately before the one-shot external read.
- [x] RED/GREEN: reject lowercase/invalid US ticker aliases as `FROZEN_INSTRUMENT_ALIAS_INVALID` terminal evidence before constructing strict Provider payloads.
- [x] RED/GREEN: after all normalized objects are prepared, bind every manifest in the aggregate to one PostgreSQL-clock-floored publication time; prove a historical as-of immediately before that time sees none of the batch.
- [x] RED/GREEN: freeze the full raw DataArtifact identity separately from its payload digest, lock/reload all artifacts in sorted order, and reject every identity/URI drift with zero publication.
- [x] RED/GREEN: enforce one strict URI matcher for raw/normalized keys, including query-only, substring and empty-fragment-delimiter attacks.
- [x] RED/GREEN: prove 49 symbols × about 275 sessions (13,475 bars) completes within the 45-second handler budget using measured PostgreSQL/CPU time plus conservative object-I/O budget; any missing/failed/duplicate/tampered child produces zero normalized market publication.
- [x] Run focused unit tests plus PostgreSQL provider/intraday ingestion and collection tests.
- [x] Commit: `feat(jobs): collect authority-bound fosun evidence`

## Task 6: Make Fosun the production market source without requiring Alpaca

**Files:**
- Create: `engine/alembic/versions/0031_intraday_schema_admission.py`
- Create: `engine/alembic/versions/0032_ticker_alias_authority.py`
- Modify: `engine/src/nexus/providers/settings.py`
- Modify: `engine/src/nexus/providers/composition.py`
- Modify: `engine/src/nexus/provider_worker.py`
- Modify: `engine/src/nexus/persistence/session.py`
- Modify: `engine/src/nexus/market/database_series.py`
- Modify: `engine/src/nexus/market/service.py`
- Modify: `engine/src/nexus/market/intraday_metrics.py`
- Modify: relevant unit/integration tests

- [x] RED: Fosun production settings construct no Alpaca client and never access Alpaca secrets; explicit Alpaca mode still enforces all existing authorities.
- [x] GREEN: add explicit market provider/Alpaca mode, Fosun gateway settings, source-specific adjustment and provider factories; SEC/FRED stay independent.
- [x] RED/GREEN: keep explicit Alpaca intraday as a source-specific payload/handler path with max 30 symbols and no Fosun read guard; never select it automatically.
- [x] RED/GREEN: refuse Provider Worker startup/claim while nonterminal legacy `intraday_snapshot_job_v1` rows remain, so an upgrade cannot silently dead-letter approved work.
- [x] RED/GREEN: install forward-only 0031 database admission for exact matching v2 column/payload schemas and explicit valid source/feed pairs, block legacy terminal requeue, attest its topology/body, and directly verify the unique Alembic head before every Provider polling cycle.
- [x] RED/GREEN: install 0032 case-insensitive PIT ticker bijection constraints, reject ambiguous upgrades/concurrent claims, attest the exact constraints at Provider startup, and fence downgrade against general, Provider, and broker-dependent schedule/claim transactions.
- [x] RED/GREEN: pin Provider sessions to READ COMMITTED and attest it before schedule/preclaim/claim; freeze the complete 68-trigger Provider-writable and authority-input contract, including status/capability locks and capability append-only.
- [x] RED/GREEN: database loaders accept only exact Fosun v2 bindings, never retroactively admit old facts, preserve v1 Alpaca/fixture, and select whole-source Fosun first.
- [x] RED/GREEN: receipt-only trade flags, unknown delivery and unavailable bid/ask produce partial/null metrics and stable quality reasons.
- [x] RED/GREEN: for raw Fosun daily authority expose only `close`; reject SMA/RSI/volatility with `adjusted_price_authority_unavailable` until an audited corporate-action/adjusted-price authority exists.
- [x] Update Provider Worker shutdown to close owned Fosun clients and keep fixture/no-network composition isolated.
- [x] Run composition, worker, source policy, market API, Agent price and paper price regressions.
- [x] Commit: `feat(runtime): select fosun as primary market source`

## Task 7: Wire the least-privilege topology and UI quality

**Files:**
- Modify: `infra/compose.yaml`
- Modify: `infra/env.example`
- Modify: `docs/runbooks/fosun-read-only-activation.md`
- Modify: `docs/runbooks/local-development.md`
- Modify: `app/components/intraday-monitor.tsx`
- Modify: `app/(protected)/market/page.tsx`
- Modify: Web unit/E2E tests and generated API contract only if API schema changes

- [x] RED: static Compose tests prove provider-worker only sees market identity, broker only sees broker identity, app/general sees neither, and gateway joins provider/broker profiles without enabling writes.
- [x] GREEN: add market-only env names/mount paths and safe defaults; do not populate values or start containers.
- [x] RED/GREEN: bind HTTP and marker/CLI readiness to a permanent wall/monotonic expiry latch; persist the replay floor and expiry tombstone, retain fail-stop evidence, and reject unclean-marker restart.
- [x] RED/GREEN: show “复星 L1（只读）”, “实时性未获证明”, “盘口未提供”, and human-readable quality reasons; no numeric spread when bid/ask absent.
- [x] RED/GREEN: retain `adjustment` through the chart view, label the history evidence, and keep the raw-price cross-period warning visible.
- [x] Run Compose `config` with synthetic fixture values only, Web unit, lint, typecheck, build and targeted Playwright market tests.
- [x] Commit: `feat(web): expose fosun market evidence quality`

## Task 8: Full acceptance, independent review, and local integration

- [x] Run offline socket-guarded Python suite using `--import-mode=importlib` where required.
- [x] Run the complete PostgreSQL integration suite against an isolated temporary database.
- [x] Run the complete Fosun gateway suite.
- [x] Run Web unit, lint, typecheck, build and complete E2E; rerun any isolated parallel timeout, then repeat the complete failed layer.
- [x] Run secret scan over the exact branch diff and tracked files; report names only, never values.
- [x] Dispatch independent code and security reviewers with the full branch diff; fix every P0–P2 or otherwise actionable in-scope finding using TDD and re-review.
- [x] Run `git diff --check`, verify exact commit ancestry and a clean worktree.
- [ ] Locally fast-forward `main` only if it still equals the recorded base; otherwise stop before merge and report the concurrent change.
- [ ] Remove only this task's linked worktree after verified local integration; do not touch siblings.
- [ ] Mark the Codex goal complete only after all required evidence above exists.

**Local acceptance evidence (2026-08-31, final-review remediation snapshot):**

- Engine offline socket guard: `1399 passed, 1006 skipped` in an exclusive run; an earlier run under parallel Web/Gateway load tripped only the unchanged backtest performance gate, while all exclusive full runs passed without weakening the 25-second threshold.
- PostgreSQL integration: `1096 passed, 6 warnings` from empty isolated databases in `3437.86s`; all temporary databases were reaped after the run. The complete Fosun migration file passed `58/58`, including deterministic 0027/0028/0029 schema-owner write races that require `P3D01`, not deadlock, after lock contention and an explicit database-default `REPEATABLE READ` regression that proves Alembic executes at `READ COMMITTED`.
- Engine Ruff, Mypy over 302 source files, and diff-format validation passed for the final migration remediation.
- Fosun Gateway under the offline socket guard: `284 passed`; the real `requests`/`urllib3` no-socket canary path and construction/call-edge logger reconfiguration tests passed. Ruff and Mypy passed for 26 source files.
- Web: lint, typecheck, generated-contract check and all build targets passed; two complete Vitest reruns passed `482/482`, the previously slow pagination case passed in isolation, and Playwright passed `85/85`.
- Compose validation passed for the explicit Provider, Alpaca-Provider and Broker profiles using only the committed empty fixture template. Repository secret-signature scan covered 993 tracked/untracked text files with zero findings.
- Initial independent review of `ac96b2a` found one P1 downgrade race and one P2 SDK dependency-log gap; the first remediation commit closed both. Code re-review of that commit found the equivalent 0028 downgrade race and an implicit Alembic isolation-level assumption. Both additional P1 findings were reproduced as three failing tests and fixed with the complete 0028 lock set plus a fail-closed `READ COMMITTED` migration boundary.
- Independent code and security re-reviews of exact SHA `f569584` covered the complete `ed2666d..f569584` branch diff, closed every prior P1/P2, found no remaining P0–P2 or other merge blocker, and both returned `Ready to locally merge: Yes`.
- No real Provider, account, broker, order, deployment, permission, credential or paid-data action was performed. Fosun SDK/external activation remains blocked and Alpaca remains disabled.

## Rulings Ledger

- Ruling: gateway normalized HTTP bytes are transport evidence, not Provider raw — preserves the secret/privacy boundary; if later true Provider raw is required, the gateway contract must version separately.
- Ruling: HMAC v2 replaces unactivated v1 — internal fixtures and clients must upgrade together; no deployed compatibility is assumed.
- Ruling: production replay state is a durable SQLite journal on one external named volume; a POSIX singleton lease rejects overlap, every process start drains 61 seconds, a persisted clock floor rejects rollback, readiness requires the owning process/listener, future timestamps fail, and an unclean ready marker blocks restart pending review.
- Ruling: a live synchronous SDK timeout/cancellation poisons the whole in-process transport, removes external readiness, and requires reviewed whole-process restart; daemon execution preserves shutdown and late Provider exceptions are silently retrieved.
- Ruling: intraday Provider Jobs run at priority 290 above 280 daily shards/publisher but below 300 broker safety work, preventing one-worker daily starvation; daily v2 Jobs freeze ticker alias identity and use the frozen symbol end to end.
- Ruling: one XNYS session/profile/source has one database-serialized daily root; current authority drift reuses the first frozen root, and no implicit rerun generation exists.
- Ruling: a daily publisher reads each locked raw artifact through its exact frozen locator; ordinary replay binds bucket/prefix/key and decoded non-`null` `versionId` to GetObjectVersion. Only a conditional-create 409/412 may use one current-version GetObject on the exact digest key to compare complete bytes and freeze its returned VersionId; missing/suspended versioning or `VersionId=null` fails closed.
- Ruling: ticker identity is a database-enforced PIT bijection and Provider symbols are canonical uppercase ASCII; 0032 downgrade shares the schema-admission fence, locks heartbeat/jobs/aliases before inspection, and includes broker simulation dispatch consumers.
- Ruling: durable daily reservations are committed after all slow revalidation and immediately before the real Gateway call, so reservation spacing is actual-call spacing.
- Ruling: every 15-second bucket has one globally unique durable intraday v2 Job with `never/1` retry authority; scheduler drift reuses its first frozen payload, and mismatched buckets or later attempts perform zero Provider calls.
- Ruling: the Fosun intraday handler uses one worker-owned monotonic deadline; acquisition includes preflight and ends by 24 seconds, reserving 5 seconds for independent partial-raw commit plus a 1-second outer-worker margin. Internal cancellation and guard failure archive completed responses then terminalize without normalization/publication/retry; operator cancellation propagates.
- Ruling: each max-attempt-one Fosun daily child uses one derived monotonic budget: a 44-second internal ceiling under the 45-second worker limit, 3 seconds reserved for final staging, 6 seconds for independent raw archive, and a 10-second fetch cap. A completed response is archived even when acquisition or permit exit fails; no phase may borrow from its successor or trigger a second external read.
- Ruling: Provider objects are direct create-only writes to immutable final digest keys; no production temporary/promote/delete seam or DeleteObject permission remains. Concurrent publisher I/O fails on the first completed error, stops queued work, cancels siblings, and preserves typed object stop codes according to each Job's retry authority.
- Ruling: object-store client construction and every actual PUT/GET worker call re-establish a closed boto3/botocore/s3transfer/urllib3 logging boundary; signed authorization, access-key identity, endpoint and raw payload diagnostics cannot propagate into common DEBUG handlers.
- Ruling: 0027, 0028 and 0029 downgrade acquire their complete fact/protection relation sets in one shared global `ACCESS EXCLUSIVE` order before inspection and hold them through DDL. The Alembic online runner explicitly requests and attests `READ COMMITTED`, overriding a stricter database default; an in-flight schema-owner fact therefore commits before a fresh guard snapshot and forces `P3D01` instead of losing authority or lineage.
- Ruling: the frozen Fosun SDK import, construction, adapter call and both requests wire surfaces re-establish a closed fsopenapi/requests/urllib3 logger boundary and reset `http.client` wire debugging; dependency reconfiguration cannot expose Provider URL/query/header/body/certificate/account material.
- Ruling: explicit Alpaca remains disabled and is not production-admitted until its intraday path has equivalent one-shot phase-budget, raw-reserve, exact-stop-code and cancellation evidence; this does not make Alpaca a Phase 1 prerequisite.
- Ruling: fixed-origin Provider HTTP disables unsanitized httpx/httpcore dependency records at construction and every request boundary; only NEXUS-owned bounded result logs remain, so a common root/HTTP DEBUG configuration cannot persist FRED query credentials.
- Ruling: provider object keys use a strict URI-safe ASCII allowlist, and SEC accession identifiers are validated in both normalization and collection before any object write; malformed identifiers publish neither objects nor facts.
- Ruling: Gateway rate tokens are consumed in the daemon SDK thread immediately beside method invocation; every bounded transport retry consumes a token, while rate waits release the shared cross-domain slot.
- Ruling: display-only intraday quotes cannot size or persist proposal-derived Broker intents; a new reviewed execution-price authority is required before that route can proceed.
- Ruling: daily `kline end_time` is inclusive in the admitted contract; ambiguous/exclusive SDK behavior blocks activation until a versioned window-lineage change and tests exist.
- Ruling: a Broker Worker process uses one unique worker ID and one fixed start timestamp for its entire polling lifetime; a restart receives a new identity instead of colliding with durable heartbeat history.
- Ruling: activation expiry is a permanent wall/monotonic readiness and request boundary; a watchdog, `ceil(expiry)` replay floor, atomic tombstone and guarded CLI fail-stop prevent rollback or cleanup races from reopening it.
- Ruling: unknown delivery may support partial display but not Agent/price authority — initial activation may abstain until entitlement evidence exists.
- Ruling: no automatic fallback — availability is lower during a Fosun outage, but unapproved Alpaca network/fees cannot be triggered silently.
