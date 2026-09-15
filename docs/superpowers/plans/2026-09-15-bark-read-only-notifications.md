# Bark Read-only Notifications Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver desensitized encrypted Bark notifications only after existing NEXUS Paper facts commit.

**Architecture:** A dedicated notification worker consumes an exact whitelist from the existing generic outbox and publishes through a narrow Bark adapter. Existing delivery attempts provide bounded retry/dead-letter behavior; Bark's stable notification ID closes duplicate delivery without a new table.

**Tech Stack:** Python 3.12, SQLAlchemy asyncio, httpx, cryptography, PostgreSQL outbox, Docker Compose, Bark Server v2.3.5.

**Spec:** `docs/superpowers/specs/2026-09-15-bark-read-only-notifications-design.md`

## Global Constraints

- No new dependency, migration, table, scheduler, broker, market-data or model call.
- Notification failure never rolls back or mutates a Paper business fact.
- Secrets are file-only and never logged, persisted, accepted as CLI values, or committed.
- `execution_locked=true` is unchanged and outside this worker's interface.
- Only existing event types are supported; no invented failure status.

---

### Task 1: Activated outbox boundary

**Files:**
- Modify: `engine/src/nexus/outbox/dispatcher.py`
- Test: `engine/tests/integration/test_outbox.py`

**Interfaces:**
- Consumes: existing `dispatch_outbox_once(...)` claim flow.
- Produces: optional `created_at_or_after: datetime | None`, applied inside the locked claim query.

- [ ] **Step 1: Write a failing integration test**

Seed one event before and one at the UTC activation boundary, dispatch with
`created_at_or_after=activation`, and assert only the latter event ID is published.

- [ ] **Step 2: Verify RED**

Run: `uv run --project engine pytest engine/tests/integration/test_outbox.py -k created_at_or_after -q`

Expected: FAIL because `dispatch_outbox_once` does not accept the boundary.

- [ ] **Step 3: Implement the minimal query filter**

Thread the optional UTC-validated timestamp into `_claim`; append
`OutboxEvent.created_at >= created_at_or_after` only when supplied. Do not mutate skipped rows.

- [ ] **Step 4: Verify GREEN**

Run the same pytest command and expect PASS.

### Task 2: Desensitized encrypted Bark adapter

**Files:**
- Create: `engine/src/nexus/notifications/__init__.py`
- Create: `engine/src/nexus/notifications/bark.py`
- Test: `engine/tests/unit/notifications/test_bark.py`

**Interfaces:**
- Consumes: `OutboxMessage` and injected `httpx.AsyncClient`.
- Produces: `BarkPublisher.publish(message) -> None`, `BarkNotification`, and a closed
  `BARK_EVENT_SCHEMAS` registry input.

- [ ] **Step 1: Write failing mapping tests**

Use payload fixtures containing account IDs, instrument IDs, amounts, evidence text and tokens.
Assert the mapped title/body do not contain any fixture values and unsupported event types reject.

- [ ] **Step 2: Verify RED**

Run: `uv run --project engine pytest engine/tests/unit/notifications/test_bark.py -q`

Expected: import failure because the Bark adapter does not exist.

- [ ] **Step 3: Implement the fixed event map**

Map only the seven real event/schema pairs to fixed Chinese title/body text and optional `/paper`
URL. Do not interpolate the outbox payload.

- [ ] **Step 4: Add failing encryption and transport tests**

Inject a fixed 16-byte IV, decrypt the emitted ciphertext with the literal test key, and assert the
inner JSON contains only `title`, `body`, `group`, stable `id`, and optional `url`. Assert the outer
JSON contains only `device_key`, `ciphertext`, and `iv`. Add timeout, non-2xx and Bark-code failure
cases whose exceptions contain fixed codes and no response body.

- [ ] **Step 5: Implement minimal encryption and POST transport**

Use `cryptography` AES-128-CBC plus PKCS7, canonical UTF-8 JSON, `POST <localhost>/push`, and
`httpx` with retries disabled. Accept only response HTTP 2xx with JSON `code == 200`.

- [ ] **Step 6: Verify GREEN**

Run the Bark unit test file and expect all cases to pass.

### Task 3: File-only settings and dedicated worker

**Files:**
- Create: `engine/src/nexus/notifications/worker.py`
- Create: `engine/src/nexus/ops/bark_notifications.py`
- Modify: `engine/pyproject.toml`
- Test: `engine/tests/unit/notifications/test_bark_worker.py`
- Test: `engine/tests/integration/test_bark_outbox_pg.py`

**Interfaces:**
- Consumes: database URL, localhost server URL, UTC activation timestamp, and device/encryption
  key file paths from environment aliases.
- Produces: `nexus-engine-bark-notifications` process; one-shot helper for deterministic tests and a
  bounded polling loop for Compose.

- [ ] **Step 1: Write failing settings tests**

Assert disabled-by-default, reject non-localhost server URLs, direct secret values, unsafe secret
file modes, non-16-byte encryption keys, non-UTC activation time, and non-`/paper` links. Assert
safe 0600 files load without exposing values in repr/errors.

- [ ] **Step 2: Verify RED**

Run: `uv run --project engine pytest engine/tests/unit/notifications/test_bark_worker.py -q`

Expected: import failure because worker settings do not exist.

- [ ] **Step 3: Implement settings and loop**

Build an exact registry from `BARK_EVENT_SCHEMAS`, call `dispatch_outbox_once` with
`claim_registered_only=True`, `created_at_or_after=activated_at`, and non-raising publisher errors.
Sleep only between empty polls; cancellation exits without relabeling events.

- [ ] **Step 4: Add failing PostgreSQL contract tests**

Persist representative Paper facts and outbox rows, commit, then make the fake Bark endpoint fail.
Assert the business rows remain, attempt error is the fixed `PUBLISHER_FAILED`, retries are bounded,
and a repeated event ID yields the same inner Bark `id`.

- [ ] **Step 5: Implement CLI composition**

Load settings from environment/file paths, construct `httpx.AsyncClient(timeout=...)`, and expose the
new project script. Do not accept secret-bearing CLI arguments.

- [ ] **Step 6: Verify GREEN**

Run both new test files and expect PASS.

### Task 4: Local-only Compose and Chinese runbook

**Files:**
- Modify: `infra/compose.yaml`
- Modify: `infra/env.example`
- Create: `infra/compose.bark-register.yaml`
- Create: `docs/runbooks/bark-read-only-notifications.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: `.secrets/bark/device-key`, `.secrets/bark/encryption-key`, and existing `DATABASE_URL`.
- Produces: default-off `notifications` profile with unexposed Bark server and notification worker;
  an explicit, temporary LAN registration override.

- [ ] **Step 1: Add pinned Bark service and worker composition**

Use `ghcr.io/finb/bark-server:v2.3.5@sha256:32d65b07fa835c99b31a396b77727a04ed058377fc2482da3e9dc7397167ffc4`, a named data volume,
internal healthcheck, no steady-state `ports`, and `network_mode: service:bark-server` for the worker.
Mount both secret files read-only at fixed `/run/secrets` paths.

- [ ] **Step 2: Document activation, registration, health and rollback**

Document 0600 secret creation without secret values in commands, Bark App AES-128-CBC setup,
temporary trusted-LAN registration, immediate return to no-port steady state, `/ping`, outbox attempt
readback, exact stop commands, and non-destructive rollback. State that real iPhone/APNs receipt is
unverified until the user completes local key/registration setup.

- [ ] **Step 3: Validate Compose**

Run: `docker compose --env-file infra/env.example -f infra/compose.yaml config --profiles`

Run: `docker compose --env-file infra/env.example -f infra/compose.yaml -f infra/compose.bark-register.yaml config`

Expected: both commands exit 0; steady-state Bark service has no published port.

### Task 5: Final verification

**Files:** all files above.

**Interfaces:** verifies the complete slice without real credentials or APNs calls.

- [ ] **Step 1: Run focused tests**

Run: `uv run --project engine pytest engine/tests/unit/notifications engine/tests/integration/test_outbox.py engine/tests/integration/test_bark_outbox_pg.py -q`

- [ ] **Step 2: Run static checks**

Run: `uv run --project engine ruff check engine/src/nexus/notifications engine/src/nexus/ops/bark_notifications.py engine/src/nexus/outbox/dispatcher.py engine/tests/unit/notifications engine/tests/integration/test_bark_outbox_pg.py engine/tests/integration/test_outbox.py`

Run: `uv run --project engine mypy engine/src/nexus/notifications engine/src/nexus/ops/bark_notifications.py engine/src/nexus/outbox/dispatcher.py`

- [ ] **Step 3: Recheck scope and secrets**

Inspect `git diff --check`, `git status --short`, and search the diff for device/encryption key-shaped
test values outside tests. Confirm no migration, dependency, Agent paper loop, execution lock, Sites,
PWA, Passkey, approval, broker or iOS application file changed.

- [ ] **Step 4: Report four independent statuses**

Report code, automated tests, local configuration, and real iPhone/APNs acceptance separately. Never
claim phone receipt without the user's protected local Bark setup and an observed notification.
