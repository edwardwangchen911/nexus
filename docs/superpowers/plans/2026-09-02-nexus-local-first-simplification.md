# NEXUS Local-First Simplification Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the existing Mac-local Fosun read-only loop understandable and operable without weakening the no-write boundary or adding deployment machinery.

**Architecture:** Preserve the persisted `READY/PARTIAL/STALE/RECONCILIATION_REQUIRED` risk contract and derive separate display/decision readiness in the read API. Add one local CLI that performs offline configuration diagnosis, reads persisted local status, or explicitly runs the existing refresh. Keep Provider errors value-free while exposing stable local categories.

**Tech Stack:** Python 3.12, Pydantic, SQLAlchemy asyncio, FastAPI/OpenAPI, TypeScript/React, pytest/Vitest.

**Spec:** User-approved recommendations in the current task and `/Users/wangchen/.cursor/projects/Users-wangchen-Documents-ChatGPT/canvases/nexus-delivery-retrospective.canvas.tsx`.

## Global Constraints

- Mac-local single-user application only; no deployment or multi-tenant work.
- No Fosun network request during implementation or verification.
- Do not weaken credentials isolation, read-only operation allowlists, rate limits, redaction, fail-closed behavior, or order approval boundaries.
- Do not delete worktrees, historical evidence, plans, reviews, or probe runs.
- Root `main` has one writer for this implementation.

---

### Task 1: Derived portfolio readiness

**Files:**
- Modify: `engine/src/nexus/api/schemas/portfolio.py`
- Modify: `engine/src/nexus/portfolio/service.py`
- Test: `engine/tests/unit/api/test_portfolio_contracts.py`
- Test: `engine/tests/integration/test_portfolio_api.py`
- Regenerate: `contracts/openapi.json`, `app/lib/generated/schema.d.ts`, `app/lib/generated/runtime-contract.ts`
- Modify: `app/(protected)/portfolio/page.tsx`
- Test: `tests/unit/portfolio/page.test.tsx`

**Interfaces:**
- Produces `display_readiness=DISPLAY_READY|DISPLAY_UNAVAILABLE` and `decision_readiness=DECISION_READY|RECONCILIATION_PENDING|DATA_STALE|RECONCILIATION_REQUIRED` on `PortfolioSnapshotResponse`.

- [x] Write schema/service/UI tests for a populated `PARTIAL` snapshot being display-ready but decision-pending.
- [x] Run the tests and confirm they fail because the readiness fields do not exist.
- [x] Implement the smallest pure readiness mapper and wire it into the response/UI.
- [x] Regenerate the OpenAPI client and run the focused tests green.

### Task 2: Unified local Fosun CLI

**Files:**
- Create: `engine/src/nexus/ops/fosun_local.py`
- Modify: `engine/pyproject.toml`
- Test: `engine/tests/unit/ops/test_fosun_local.py`

**Interfaces:**
- Produces `nexus-fosun-local doctor|status|refresh`.
- `doctor` performs no network/database calls and emits only configured/missing field names and safe file-state categories.
- `status` reads only aggregate persisted refresh facts from PostgreSQL.
- `refresh` calls the existing bounded `refresh_local_fosun` function.

- [x] Write failing command/parser, redaction and dispatch tests.
- [x] Implement safe doctor/status/refresh functions.
- [x] Run focused CLI tests green.

### Task 3: Safe Provider diagnostics

**Files:**
- Modify only if a failing gap is found: `integrations/fosun_gateway/src/nexus_fosun_gateway/adapter/probe_sdk.py`
- Test: `integrations/fosun_gateway/tests/unit/test_probe_sdk.py`
- Modify: `engine/src/nexus/ops/fosun_local.py`

**Interfaces:**
- Preserve stable categories `auth|permission|rate_limit|provider|transport|contract` without serializing exception values.

- [x] Add a regression test proving local CLI output exposes a stable category but not exception text, request values, URLs, or keys.
- [x] Implement only the missing mapping; do not expand the probe artifact or receipt system.
- [x] Run focused Gateway and CLI tests green.

### Task 4: Project governance and operator docs

**Files:**
- Modify: `AGENTS.md`
- Modify: `README.md`

- [x] Document local fixture versus local real read-only terminology.
- [x] Document WIP=1 for the core path, one optional isolated UI writer, one review round, risk-tiered tests, and the two-day user-visible stop rule.
- [x] Replace the old refresh command with the unified CLI and explain display versus decision readiness.
- [x] Run available generated-contract, lint, build and diff checks.

### Task 5: Verification

- [x] Run focused Engine unit tests; integration tests collected but skipped because the DB fixture was unavailable.
- [x] Run focused Web unit tests, typecheck and generated-contract check.
- [x] Run focused Gateway tests and Ruff/Mypy for changed Python modules.
- [x] Run `git diff --check` and review the final diff against every global constraint.
