# NEXUS real fixture E2E residual hardening implementation plan

> Authority: `docs/superpowers/specs/2026-08-31-nexus-real-fixture-e2e-design.md` remains binding. This follow-up exists only because the scoped re-review of `26b3d9b..2b122a7` reproduced four load-bearing residual findings.

## Goal

Close the four reproduced residual findings without changing the NEXUS product boundary: the fixture gate must remain local-only, credential-free, deterministic, mock-free, backed by a caller-provided dedicated empty PostgreSQL database, and unable to reach Fosun, an account, a Provider, production, deployment, or an order path.

## Global constraints

- Work only in `.worktrees/nexus-real-e2e-mvp` on `edward/nexus-real-e2e-mvp`; `main` remains unchanged until all review and verification gates pass.
- TDD for every behavior change. Every negative mutation must fail before implementation and pass afterward.
- No new runtime service, Docker Desktop requirement, database reset/delete logic, production/provider access, credential material, deployment, push, or Git-history rewrite.
- Database tests may create only exact disposable `nexus_e2e_*` databases in an explicitly named local test container and must record exact cleanup plus an empty residue query.
- One task implementer writes at a time. Reviewers are read-only. Controller does not fix implementation findings.

## Task 1: Make repository policy fail closed on effective artifacts

**Files:**

- Modify `scripts/ci/repository_policy.py`
- Modify `tests/policy/test_repository_policy.py`
- Modify documentation only if the enforcement contract changes wording

**Requirements:**

1. Add RED mutations proving the checker rejects all reproduced bypasses:
   - a comment retains `workers: 1` while the effective config is `workers: 8`;
   - the exact CI step is disabled with job- or step-level `if: false`;
   - the exact CI step uses `continue-on-error: true`;
   - bracket calls for `route`, `routeFromHAR`, `addInitScript`, `route.fulfill`, `navigator.serviceWorker.register`, and `window/globalThis.fetch` replacement;
   - side-effect imports and multiline/alias import forms that leave `tests/e2e-real`.
2. Do not keep extending fragile positive-string matching. Freeze the effective approved artifacts with a fail-closed mechanism that cannot be satisfied by comments or dead YAML. A narrow exact-content SHA-256 of the dedicated Playwright config, the extracted `real-e2e` job block, and the single approved real spec is acceptable and preferred when it avoids adding a parser dependency; retain readable semantic checks as defense in depth.
3. Reject duplicate `real-e2e` job keys, YAML aliases/merge indirection in the job, job/step `if`, `continue-on-error`, shell wrapping, extra real-E2E invocations, and any deviation from the exact `npm run test:e2e:real` step.
4. Keep the dedicated test boundary at exactly one tracked `integrations-real.spec.ts`. Continue scanning source, but an approved-content digest must ensure syntax variants cannot silently alter the mock-free test while the policy remains green.
5. Recompute any approved digest only from an independently reviewed current artifact. Tests must prove a changed artifact fails until the policy constant is deliberately updated.

**Verification:**

```bash
uv run pytest tests/policy/test_repository_policy.py -q
uv run python scripts/ci/repository_policy.py
uv run ruff check scripts/ci/repository_policy.py tests/policy/test_repository_policy.py
```

Dispatch an independent task reviewer over this task diff. No open Critical/Important finding may flow into Task 2.

## Task 2: Close abnormal process lifecycle and empty-database inventory gaps

**Files:**

- Modify `scripts/e2e/real-fixture.mjs`
- Modify `scripts/e2e/real_fixture_db_guard.py`
- Modify `tests/unit/e2e/real-fixture-runner.test.ts`
- Modify `engine/tests/unit/ops/test_real_fixture_db_guard.py`
- Modify `engine/tests/integration/test_real_fixture_db_guard.py`
- Update runbook/spec only where behavior must be stated precisely

**Requirements:**

1. RED: when guard failure wins during an active stage, assert the runner sends termination, awaits the stage `close`, keeps the guard stop/finalization ordered after stage shutdown, and escalates after a bounded grace period when the direct child ignores SIGTERM. The runner must not resolve or reject while that child is still active.
2. Implement one idempotent child-termination path used by external SIGINT/SIGTERM, abort, spawn failure, and guard loss. It must remove listeners/timers on every path, wait for close, use a bounded SIGKILL escalation where supported, and keep diagnostics fixed and credential-free. Do not shell-concatenate commands.
3. RED/GREEN database catalog checks for at least user-created public collations, operators/operator classes/families, text-search configurations/dictionaries/parsers/templates, conversions, languages, event triggers, publications/subscriptions where supported, foreign servers/wrappers/user mappings, transforms, casts, and large objects in addition to existing schemas/relations/routines/types/extensions. Version-dependent catalogs must be checked safely without weakening supported PostgreSQL 17 behavior.
4. The preflight remains read-only and holds the same session advisory lock until every child has stopped. It must never create/drop/truncate/delete/reset the target.
5. Add a real PostgreSQL integration proving representative non-table objects are rejected, concurrent lock is rejected, an empty database succeeds, and the lock is released after all completion/error paths.

**Verification:**

```bash
npm test -- --run tests/unit/e2e/real-fixture-runner.test.ts tests/unit/e2e/real-fixture-result.test.ts
uv run pytest engine/tests/unit/ops/test_real_fixture_db_guard.py -q --import-mode=importlib
uv run pytest engine/tests/integration/test_real_fixture_db_guard.py -q --import-mode=importlib
npm run lint
npm run typecheck
uv run ruff check engine scripts/ci
uv run mypy engine/src
```

Dispatch an independent task reviewer over this task diff. Resolve findings within the normal capped task loop.

## Task 3: Fresh acceptance, whole-branch review, and local integration

1. On a fresh exact `nexus_e2e_*` database, run `npm run test:e2e:real`; record migration, Playwright, and result-verifier exits plus read-only persistence evidence (`fosun fixture version=1`, exact idempotency record count 1, capability count 0, migration head).
2. Run the full relevant acceptance matrix:

```bash
npm run test:e2e -- --workers=3
npm test
npm run lint
npm run typecheck
npm run build
uv run pytest tests/policy -q
uv run pytest engine/tests/integration/test_integrations_api.py -q --import-mode=importlib
uv run pytest engine/tests/unit/ops/test_real_fixture_db_guard.py -q --import-mode=importlib
uv run pytest engine/tests/integration/test_real_fixture_db_guard.py -q --import-mode=importlib
uv run ruff check engine scripts/ci
uv run mypy engine/src
uv run python scripts/ci/repository_policy.py
git diff --check main...HEAD
```

3. Remove only the exact disposable databases/container and prove exact-name residue is zero.
4. Run one independent whole-branch code/security review from `main...HEAD`, explicitly replaying the four residual mutation cases. Follow the capped review protocol; do not merge with an open load-bearing P0/P1/P2.
5. If review and all fresh verification are clean, fast-forward local `main`, rerun merged-result smoke/acceptance, remove only this feature worktree/branch, and leave push/deploy/Fosun/account/order untouched. If not clean, preserve branch and evidence and report the blocker.
