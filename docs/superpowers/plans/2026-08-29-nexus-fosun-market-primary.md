# NEXUS Fosun-first Market Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:test-driven-development` for every behavior change and `superpowers:verification-before-completion` before any completion claim.

**Goal:** Make the already-approved Fosun-first US-market policy executable through an isolated market-only gateway identity, immutable/PIT provider authority, daily and intraday evidence, Agent price authority v2, and explicit UI quality, while keeping Alpaca installed but disabled by default.

**Architecture:** The Provider Worker calls only `fosun_get_quotes` and `fosun_get_bars` through a fixed-origin mTLS/HMAC-v2 client. The gateway derives principal and operation grants from configured key IDs, never from an unsigned header. Every Fosun call freezes and revalidates database authority at actual pre/post times; normalized facts carry the exact capability/status binding and are admitted by new immutable v2 policies. Alpaca remains a separately gated adapter with no automatic fallback.

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

- [ ] Update ADR 0002 from “broker-worker only” to two independent caller identities and HMAC v2; keep SDK admission blocked.
- [ ] Correct the real-data plan’s wrong spec pointer and state that it implemented Alpaca as an adapter, not the final source selection.
- [ ] Run `git diff --check` and a link/path scan.
- [ ] Commit: `docs(architecture): freeze fosun-first market runtime`

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

- [ ] RED: add the complete broker/provider current/previous × operation authorization matrix; assert actor/request-id tampering, v1, duplicate cross-role identities, and non-production read_only all fail before transport.
- [ ] GREEN: add `canonical_hmac_v2`, signed version/actor/request-id, configured principal registry, exact actor match, and operation authorization immediately after verification.
- [ ] RED: add invalid quote code/set and bars pair/window/order/OHLC/volume/limit tests plus activation-expiry-after-call test.
- [ ] GREEN: enforce strict request models and post-response normalizer invariants; recheck activation after SDK return.
- [ ] Update the existing broker client to HMAC v2 without adding any market path.
- [ ] Run the full gateway suite and focused engine gateway-client tests.
- [ ] Commit: `fix(gateway): isolate signed market principals`

## Task 3: Add immutable Fosun provider binding and authority v2

**Files:**
- Create: `engine/alembic/versions/0027_fosun_market_authority.py`
- Modify: `engine/src/nexus/persistence/models/data.py`
- Modify: `engine/src/nexus/persistence/models/market.py`
- Modify: `engine/src/nexus/persistence/migrations.py`
- Modify: `engine/src/nexus/quality/source_policy.py`
- Modify: `engine/src/nexus/market/price_bar_authority.py`
- Modify: `engine/src/nexus/market/intraday_authority.py`
- Modify: migration helpers and integration tests under `engine/tests/integration/**`

- [ ] RED: migration tests require the five binding columns on manifests/quotes, immutable v2 policy rows, complete tuple constraints, Provider binding triggers, and unchanged v1 replay.
- [ ] RED: direct SQL tests reject missing/mismatched capability, status, operation, hash, version, scope, future-created/expired authority and post-invalidation binding.
- [ ] GREEN: add 0027 for the v2 contract; create `source_policy_v2`, `quote_source_policy_v2`, `intraday_monitor_policy_v2`, price/intraday canonical/prepare/validate v2 functions and fail-closed downgrade guards. Add forward-only 0028/0029 remediation for pinned authority schemas, restricted-role publication marking, advisory-lock symmetry, gateway transport lineage and freshness; do not rewrite pre-0027 migrations.
- [ ] GREEN: update ORM and finalizers to select v1 for existing Alpaca/fixture and v2 for Fosun only.
- [ ] Update Agent snapshot validation to accept exact v2 Fosun price authority while retaining every existing research and v1 guard.
- [ ] Run migration/schema/authority/PIT integration tests with PostgreSQL.
- [ ] Commit: `feat(db): add fosun market authority v2`

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

- [ ] RED: fixed-origin HTTPS, mTLS, HMAC v2, `trust_env=False`, no redirects, exact two market paths, body/response caps, strict media type, safe errors and secret-redaction tests.
- [ ] GREEN: implement a market-only client that cannot express account/order operations and returns exact gateway response bytes plus strict typed pages.
- [ ] RED: test `AAPL -> usAAPL` PIT mapping, quote set equality, Decimal preservation, receipt-only timestamp flag, unknown delivery, complete minute bars, daily bars and future/window rejection.
- [ ] GREEN: implement Fosun daily `MarketDataProvider` and raw-first intraday provider/normalizer; preserve bid/ask as null and never synthesize spread.
- [ ] Make the intraday handler call provider-owned normalization rather than importing Alpaca normalization directly.
- [ ] Run the new provider tests, Alpaca regression tests, Ruff and Mypy for touched modules.
- [ ] Commit: `feat(provider): add fosun market gateway adapter`

## Task 5: Freeze authority in daily and intraday jobs

**Files:**
- Modify: `engine/src/nexus/jobs/provider_collection.py`
- Modify: `engine/src/nexus/jobs/intraday_schedule.py`
- Modify: `engine/src/nexus/jobs/intraday_collection.py`
- Modify: `engine/src/nexus/market/ingestion.py`
- Modify: `engine/src/nexus/market/intraday_ingestion.py`
- Modify: unit/integration job and ingestion tests

- [ ] RED: capability `created_at > check_time`, expiry, wrong status/hash/version, and wrong operation prevent scheduling/network.
- [ ] RED: actual pre/post clock drift archives exact transport evidence but creates zero normalized facts; claim effective time cannot mask a mid-call change.
- [ ] GREEN: add frozen provider authority payload fields and v2 schema; compare exact authority snapshots before and after fetch.
- [ ] GREEN: persist the five binding fields on Fosun facts/manifests and use source-specific raw schema versions.
- [ ] RED/GREEN: batch quotes every 15 seconds, minute bars only on minute boundary, Fosun max 10 symbols, completed bars only, bounded deterministic envelope and idempotent replay.
- [ ] Run focused unit tests plus PostgreSQL provider/intraday ingestion and collection tests.
- [ ] Commit: `feat(jobs): collect authority-bound fosun evidence`

## Task 6: Make Fosun the production market source without requiring Alpaca

**Files:**
- Modify: `engine/src/nexus/providers/settings.py`
- Modify: `engine/src/nexus/providers/composition.py`
- Modify: `engine/src/nexus/provider_worker.py`
- Modify: `engine/src/nexus/market/database_series.py`
- Modify: `engine/src/nexus/market/service.py`
- Modify: `engine/src/nexus/market/intraday_metrics.py`
- Modify: relevant unit/integration tests

- [ ] RED: Fosun production settings construct no Alpaca client and never access Alpaca secrets; explicit Alpaca mode still enforces all existing authorities.
- [ ] GREEN: add explicit market provider/Alpaca mode, Fosun gateway settings, source-specific adjustment and provider factories; SEC/FRED stay independent.
- [ ] RED/GREEN: database loaders accept only exact Fosun v2 bindings, never retroactively admit old facts, preserve v1 Alpaca/fixture, and select whole-source Fosun first.
- [ ] RED/GREEN: receipt-only trade flags, unknown delivery and unavailable bid/ask produce partial/null metrics and stable quality reasons.
- [ ] Update Provider Worker shutdown to close owned Fosun clients and keep fixture/no-network composition isolated.
- [ ] Run composition, worker, source policy, market API, Agent price and paper price regressions.
- [ ] Commit: `feat(runtime): select fosun as primary market source`

## Task 7: Wire the least-privilege topology and UI quality

**Files:**
- Modify: `infra/compose.yaml`
- Modify: `infra/env.example`
- Modify: `docs/runbooks/fosun-read-only-activation.md`
- Modify: `docs/runbooks/local-development.md`
- Modify: `app/components/intraday-monitor.tsx`
- Modify: `app/(protected)/market/page.tsx`
- Modify: Web unit/E2E tests and generated API contract only if API schema changes

- [ ] RED: static Compose tests prove provider-worker only sees market identity, broker only sees broker identity, app/general sees neither, and gateway joins provider/broker profiles without enabling writes.
- [ ] GREEN: add market-only env names/mount paths and safe defaults; do not populate values or start containers.
- [ ] RED/GREEN: show “复星 L1（只读）”, “实时性未获证明”, “盘口未提供”, and human-readable quality reasons; no numeric spread when bid/ask absent.
- [ ] Run Compose `config` with synthetic fixture values only, Web unit, lint, typecheck, build and targeted Playwright market tests.
- [ ] Commit: `feat(web): expose fosun market evidence quality`

## Task 8: Full acceptance, independent review, and local integration

- [ ] Run offline socket-guarded Python suite using `--import-mode=importlib` where required.
- [ ] Run the complete PostgreSQL integration suite against an isolated temporary database.
- [ ] Run the complete Fosun gateway suite.
- [ ] Run Web unit, lint, typecheck, build and complete E2E; rerun any isolated parallel timeout, then repeat the complete failed layer.
- [ ] Run secret scan over the exact branch diff and tracked files; report names only, never values.
- [ ] Dispatch independent code and security reviewers with the full branch diff; fix every Critical/Important finding using TDD and re-review.
- [ ] Run `git diff --check`, verify exact commit ancestry and a clean worktree.
- [ ] Locally fast-forward `main` only if it still equals the recorded base; otherwise stop before merge and report the concurrent change.
- [ ] Remove only this task's linked worktree after verified local integration; do not touch siblings.
- [ ] Mark the Codex goal complete only after all required evidence above exists.

## Rulings Ledger

- Ruling: gateway normalized HTTP bytes are transport evidence, not Provider raw — preserves the secret/privacy boundary; if later true Provider raw is required, the gateway contract must version separately.
- Ruling: HMAC v2 replaces unactivated v1 — internal fixtures and clients must upgrade together; no deployed compatibility is assumed.
- Ruling: production replay state is a durable SQLite journal on one external named volume; a POSIX singleton lease rejects overlap, every process start drains 61 seconds, a persisted clock floor rejects rollback, readiness requires the owning process/listener, and future timestamps fail.
- Ruling: unknown delivery may support partial display but not Agent/price authority — initial activation may abstain until entitlement evidence exists.
- Ruling: no automatic fallback — availability is lower during a Fosun outage, but unapproved Alpaca network/fees cannot be triggered silently.
