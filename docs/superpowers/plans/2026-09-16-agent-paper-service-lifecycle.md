# Agent Paper Independent Worker Service Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run the existing five-minute Agent Paper loop as one independent host-side Worker service through the normal NEXUS `up/status/down` commands, without adding a supervisor or weakening trading safety.

**Architecture:** The existing loop remains the only scheduler and freezes one persisted XNYS calendar authority for its process lifetime. A single `flock`, the existing database heartbeat, and cooperative signal handling provide the complete Worker lifecycle. The existing macOS service adapter is hidden behind `nexus-local-stack`; Web/API remain independent and the unusable Compose Agent Paper service is removed.

**Tech Stack:** Python 3.12, asyncio, SQLAlchemy/PostgreSQL, `fcntl.flock`, macOS launchd adapter, Docker Compose, pytest, Ruff, Mypy.

**Spec:** `docs/superpowers/specs/2026-09-16-agent-paper-service-lifecycle-design.md`

## Global Constraints

- Keep `execution_locked=true`; this work never enables broker paper or live writes.
- Do not add an HTTP service, database table, Redis/Kafka/Celery, supervisor, runtime descriptor, PID identity system, or parent pipe.
- Agent Paper failure must not make Web/API unavailable.
- Every cycle first reads persisted `cycle → job → outputs → order/window`; unknown results are never replayed.
- The Worker uses one calendar authority ID for its full process lifetime and never switches silently.
- Runtime and status output must not expose account IDs, securities, orders, prompts, raw exceptions, commands, credentials, or environment values.
- Real service startup and real two-slot acceptance are separate approval gates after offline implementation.

---

## File map

| File | Responsibility in this plan |
| --- | --- |
| `engine/src/nexus/paper/agent_cycle_authority.py` | Reload current/next sessions from one already-bound calendar authority. |
| `engine/src/nexus/ops/agent_paper_loop.py` | Long-lived five-minute policy, simple lock, heartbeat and cooperative shutdown. |
| `engine/src/nexus/ops/agent_paper_once.py` | Carry the bound calendar ID through trigger reads. |
| `scripts/macos/agent-paper-loop-service.py` | Existing host service adapter; exact repo/config ownership only. |
| `engine/src/nexus/ops/local_stack.py` | Add Agent Paper start/status/stop to existing `up/status/down`. |
| `infra/compose.yaml` | Remove the unusable container Agent Paper path. |
| `docs/runbooks/agent-paper-cycle.md` | Document only the unified normal entry and diagnostic foreground entry. |
| `README.md` | Keep the top-level local lifecycle commands aligned with the runbook. |

---

### Task 1: Keep one calendar authority while waiting across sessions

**Files:**
- Modify: `engine/src/nexus/paper/agent_cycle_authority.py`
- Modify: `engine/src/nexus/ops/agent_paper_loop.py`
- Modify: `engine/src/nexus/ops/agent_paper_once.py`
- Modify: `engine/tests/unit/ops/test_agent_paper_loop.py`
- Modify: `engine/tests/unit/ops/test_agent_paper_once.py`
- Modify: `engine/tests/integration/test_agent_paper_cycle_pg.py`

**Interfaces:**
- Produces: `load_agent_paper_service_calendar_authority(session, *, now, expected_calendar_authority_id) -> ServiceCalendarAuthority`.
- Produces: `WAIT_FOR_SESSION`, `STOP_CALENDAR_UNAVAILABLE`, and `STOP_CALENDAR_AUTHORITY_MISMATCH`.
- Changes: `inspect_agent_paper_loop(..., expected_calendar_authority_id: UUID)`, `_inspect_prior_daily_scope`, daily/macro/trend prerequisite preparation, performance-due lookup and trigger preparation all require the bound ID.
- Consumed by Task 2: one immutable `calendar_authority_id` selected before the first heartbeat.

- [ ] **Step 1: Add failing fixed-authority loader tests**

Add PostgreSQL tests proving the loader reads only the expected ID, derives actual half-day close and next session, rejects a missing/out-of-range authority, and does not switch when a newer complete authority appears.

```python
loaded = await load_agent_paper_service_calendar_authority(
    session,
    now=AFTER_CLOSE,
    expected_calendar_authority_id=BOUND_ID,
)
assert loaded.calendar_authority_id == BOUND_ID
assert loaded.next_session == date(2026, 11, 30)
```

- [ ] **Step 2: Run the new loader tests and record RED**

```bash
uv run --project engine pytest \
  engine/tests/integration/test_agent_paper_cycle_pg.py \
  -q --import-mode=importlib -k 'service_calendar and bound'
```

Expected: collection or assertion failure because the bound loader does not exist.

- [ ] **Step 3: Implement the bound loader**

Query `ExchangeCalendarAuthorityRecord.id == expected_calendar_authority_id`, require `complete`, `published_at <= now`, `created_at <= now`, and range coverage. Query current/next sessions with `created_at <= now`; return the existing `ServiceCalendarAuthority`. Map missing/inconsistent rows only to `AGENT_PAPER_CALENDAR_UNAVAILABLE`; an ID returned by any nested cycle loader that differs maps to `AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH`. Both initial selection and every reload use `clock_timestamp()` obtained in the same database transaction, never the host clock.

- [ ] **Step 4: Add failing loop policy tests**

Extend `AgentPaperLoopSnapshot` with:

```python
calendar_authority_id: UUID | None = None
decision_window_start_at: datetime | None = None
decision_window_end_at: datetime | None = None
next_decision_window_start_at: datetime | None = None
```

Test pre-open, post-close, weekend, half-day and DST snapshots. Pre-open/post-close must return `WAIT_FOR_SESSION`, not `STOP_SESSION_CLOSED`; no provider/model/trigger callable may run while waiting.

- [ ] **Step 5: Implement authoritative waiting and propagate the ID**

Remove every fixed `time(16)` gate from `choose_loop_action` and `agent_paper_once`; use the bound authority's `decision_window_start_at` and actual `market_close_at`. `WAIT_FOR_SESSION` sleeps in `min(remaining, 15)` chunks and writes heartbeat after each chunk. `run_local_agent_paper_loop` selects once from the database clock, stores the ID in memory, and supplies it to `inspect_agent_paper_loop`, `_inspect_prior_daily_scope`, daily/macro/trend preparation, performance-due lookup, `load_quasi_realtime_cycle_authority` and both trigger reads.

Remove the Agent Paper path's independent `exchange_calendars` decisions: `latest_visible_macro_vintage`, `_macro_attempt_session`, and the one-year trend start session must derive their relevant session/open facts from `ExchangeCalendarSessionRecord` under the bound authority. The library may remain for unrelated callers, but no Agent Paper decision-time consumer may select a session outside the persisted bound ID.

Add one end-to-end PostgreSQL regression: bind an older authority, insert a newer complete authority, then cross close/weekend into the next session. Inspection, performance lookup, all three prerequisites and trigger must keep the original ID. When its range no longer covers the required next session, stop with calendar unavailable rather than reselecting.

- [ ] **Step 6: Run focused calendar and existing trigger regressions**

```bash
uv run --project engine pytest \
  engine/tests/unit/ops/test_agent_paper_once.py \
  engine/tests/unit/ops/test_agent_paper_loop.py \
  engine/tests/integration/test_agent_paper_cycle_pg.py \
  -q --import-mode=importlib
```

Expected: all tests pass with PostgreSQL integration tests executed, not skipped.

- [ ] **Step 7: Commit the calendar/long-wait policy**

```bash
git add engine/src/nexus/paper/agent_cycle_authority.py \
  engine/src/nexus/ops/agent_paper_loop.py \
  engine/src/nexus/ops/agent_paper_once.py \
  engine/tests/unit/ops/test_agent_paper_once.py \
  engine/tests/unit/ops/test_agent_paper_loop.py \
  engine/tests/integration/test_agent_paper_cycle_pg.py
git commit -m "feat(paper): keep loop on one calendar authority"
```

---

### Task 2: Add the minimal Worker lifecycle

**Files:**
- Modify: `engine/src/nexus/ops/agent_paper_loop.py`
- Modify: `engine/tests/unit/ops/test_agent_paper_loop.py`
- Modify: `engine/tests/integration/test_agent_paper_cycle_pg.py`

**Interfaces:**
- Produces: `acquire_agent_paper_lock(*, runtime_root: Path | None = None) -> ContextManager[None]` in the existing loop module. Production derives one per-UID macOS runtime root; tests inject a temporary root.
- Changes: `run_foreground_loop(..., shutdown_requested, confirm_shutdown(action, before_snapshot, action_result), ...)`.
- Produces: `STOP_REQUESTED` with exit 0 and `STOP_UNCONFIRMED` with fixed process exit 75.
- Does not produce any descriptor, PID model, new table, new process role or parent-fd API.

- [ ] **Step 1: Add a failing cross-process lock test**

Use `multiprocessing.get_context("spawn")`. The parent represents the background service in one repo and holds the per-UID shared lock; a child represents a second worktree foreground command and must receive only `AGENT_PAPER_LOOP_ALREADY_RUNNING`. Assert the losing process writes no heartbeat and never calls inspection, Provider or model code.

- [ ] **Step 2: Implement the small lock helper**

Use one per-user host directory such as `~/Library/Application Support/NEXUS/runtime`, required to be current-uid, non-symlink and mode `0700`. Open its single `agent-paper.lock` with `O_RDWR | O_CREAT | O_NOFOLLOW` mode `0600`, validate regular file/current uid/mode/single link with `os.fstat`, take `fcntl.flock(fd, LOCK_EX | LOCK_NB)`, retain the fd for the context lifetime, then unlock and close. Do not create another runtime module or any descriptor file.

- [ ] **Step 3: Add failing cooperative-stop tests**

Cover idle stop, stop during a blocked action, heartbeat while draining, confirmation exactly once, confirmation failure, repeated signal callbacks and heartbeat failure during an active action. The confirmation freezes `(action, before_snapshot, action_result)` and checks the exact action's persisted postcondition. Cover at least trigger, research Job, order-window/advance and performance actions. Wrap blocked-action scenarios in `asyncio.timeout(1)` and assert the action never receives `CancelledError`.

```python
result = await run_foreground_loop(
    inspect=inspect,
    execute=execute,
    sleep=asyncio.sleep,
    heartbeat=heartbeat,
    shutdown_requested=shutdown.is_set,
    confirm_shutdown=confirm_shutdown,
)
assert result is AgentPaperLoopAction.STOP_REQUESTED
assert action_cancelled is False
assert confirmation_reads == 1
```

- [ ] **Step 4: Implement cooperative shutdown**

At an idle boundary, a stop request returns `STOP_REQUESTED`. During an active action, continue heartbeat and wait for the action's existing bound; never cancel the action task. After completion, call `confirm_shutdown(action, before_snapshot, action_result)` once. It accepts only: trigger created/read back the expected slot; research/resume left the exact Job/cycle in a known state; collect/advance left the exact order/window in a known state; performance created/read back the exact `(account_id, as_of)` snapshot. A mismatch, conflict or read failure returns `STOP_UNCONFIRMED`. Only the temporary heartbeat sleep task may be cancelled after the action finishes.

- [ ] **Step 5: Bind signals and lock in the concrete runner**

Acquire the shared per-UID lock before selecting the calendar, writing heartbeat or inspecting cycles. Install `SIGINT` and `SIGTERM` handlers that only set one `asyncio.Event`; remove both handlers in `finally`. The Worker CLI maps `STOP_UNCONFIRMED` to exit 75 and all other fixed loop reasons to the existing allowlisted exits. `confirm_shutdown` performs one exact persisted inspection with the bound authority and never invokes a producer.

- [ ] **Step 6: Run focused lifecycle verification**

```bash
uv run --project engine pytest \
  engine/tests/unit/ops/test_agent_paper_loop.py \
  engine/tests/integration/test_agent_paper_cycle_pg.py \
  -q --import-mode=importlib
uv run --project engine ruff check \
  engine/src/nexus/ops/agent_paper_loop.py \
  engine/tests/unit/ops/test_agent_paper_loop.py
uv run --project engine mypy engine/src
```

- [ ] **Step 7: Commit the minimal lifecycle**

```bash
git add engine/src/nexus/ops/agent_paper_loop.py \
  engine/tests/unit/ops/test_agent_paper_loop.py \
  engine/tests/integration/test_agent_paper_cycle_pg.py
git commit -m "feat(paper): run worker continuously and stop safely"
```

---

### Task 3: Make the existing host adapter exact and ownership-safe

**Files:**
- Modify: `scripts/macos/agent-paper-loop-service.py`
- Modify: `engine/src/nexus/ops/agent_paper_loop.py`
- Modify: `engine/tests/unit/ops/test_agent_paper_host_service.py`
- Modify: `engine/tests/unit/ops/test_agent_paper_loop.py`
- Modify: `engine/tests/integration/test_agent_paper_cycle_pg.py`

**Interfaces:**
- Host adapter actions remain exactly `start`, `status`, `stop`, and internal `run`.
- All actions require explicit `--repo-root`, `--env-file`, and `--project-name`.
- Loop CLI adds explicit `--repo-root`, `--env-file`, and read-only `--service-status`; service execution uses exactly the registered repo/config values.
- The launchd label remains one per macOS user; an existing registration owned by another repo/config/project returns `AGENT_PAPER_SERVICE_OWNER_MISMATCH` without signaling it.
- Exit 75 is reserved for stop-unconfirmed. The adapter retains the launchd registration/last-exit fact; ordinary `stop` and `local_stack down` cannot clear it.

- [ ] **Step 1: Add failing explicit-ownership tests**

Test that generated plist arguments contain the exact resolved repo, ignored env file and validated project name. Invoke status/stop from a second root and assert ownership mismatch, no `launchctl kill`, no `bootout`, and no plist replacement. Also test repeated start for the same owner is idempotent. For a legacy registration whose arguments end at `run`, allow status/stop only when its absolute script path and launchd WorkingDirectory prove the same repo; any other legacy registration remains mismatch. Test that exit 75 makes repeated ordinary stop fail without bootout, while explicit same-owner `stop --acknowledge-unconfirmed` on an inactive registration performs one bootout and no signal.

- [ ] **Step 2: Add failing explicit-config loop tests**

Pass a non-default ignored env file to `main`; assert preflight and runtime both read it. Set conflicting business values in `os.environ` and prove the registered service still uses the env file values. A missing, tracked, symlinked or out-of-repo env file must fail before Codex status, database, Provider or model access.

- [ ] **Step 3: Implement exact arguments and ownership comparison**

Validate root/env/project with the same rules as `local_stack.py`. Store the non-secret values in plist `ProgramArguments`; do not store environment variables. Before start/status/stop of an existing fixed label, compare its registered program arguments with the expected exact tuple. Parse launchctl output only internally and never emit it. A mismatch returns the fixed code and performs no mutation. A same-root legacy registration may remain running; its first confirmed stop removes it, and the next start writes the new exact arguments.

When stop observes Worker exit 75, return `AGENT_PAPER_SERVICE_STOP_UNCONFIRMED`, retain the registration and do not bootout. Repeated ordinary stop returns the same result. Add `stop --acknowledge-unconfirmed` only for an already-exited same-owner registration with last exit 75; it performs no signal and no business read/write, and removes only that launchd registration after the user has separately inspected persisted facts. `nexus-local-stack down` never supplies this flag.

- [ ] **Step 4: Extend the loop config loader without changing other callers**

Change:

```python
def load_agent_paper_service_environment(
    repo_root: Path,
    process_environment: Mapping[str, str],
    *,
    env_file: Path | None = None,
) -> dict[str, str]: ...
```

`None` preserves the existing `infra/.env.local` behavior for current diagnostic callers. The registered service path supplies the already-validated explicit file and passes an empty configuration override mapping, so `os.environ` cannot replace database, feature flag, account, runtime or Provider values. The child process may retain only ordinary process-runtime variables needed by Python/Codex (`HOME`, `PATH`, `TMPDIR`, locale and certificate paths); none become Engine settings. No config value is printed or copied into the plist.

`--service-status` performs one read of the existing `worker_heartbeats` fact and one zero-external-request preflight, returning separate allowlisted heartbeat/preflight fields. It never starts a producer or retries a request.

- [ ] **Step 5: Run host adapter and CLI regressions**

```bash
uv run --project engine pytest \
  engine/tests/unit/ops/test_agent_paper_host_service.py \
  engine/tests/unit/ops/test_agent_paper_loop.py \
  engine/tests/integration/test_agent_paper_cycle_pg.py \
  -q --import-mode=importlib
uv run --project engine ruff check \
  scripts/macos/agent-paper-loop-service.py \
  engine/src/nexus/ops/agent_paper_loop.py \
  engine/tests/unit/ops/test_agent_paper_host_service.py
```

- [ ] **Step 6: Commit the exact host service adapter**

```bash
git add scripts/macos/agent-paper-loop-service.py \
  engine/src/nexus/ops/agent_paper_loop.py \
  engine/tests/unit/ops/test_agent_paper_host_service.py \
  engine/tests/unit/ops/test_agent_paper_loop.py \
  engine/tests/integration/test_agent_paper_cycle_pg.py
git commit -m "fix(paper): bind host service to one local project"
```

---

### Task 4: Connect the Worker to normal NEXUS lifecycle commands

**Files:**
- Modify: `engine/src/nexus/ops/local_stack.py`
- Modify: `engine/tests/unit/ops/test_local_stack.py`
- Modify: `infra/compose.yaml`
- Delete: `engine/tests/unit/ops/test_agent_paper_compose_policy.py`
- Modify: `docs/runbooks/agent-paper-cycle.md`
- Modify: `README.md`

**Interfaces:**
- `up`: after the existing app components succeed, start Agent Paper only when the validated env file enables it.
- `status`: report Compose, exact host adapter, database heartbeat and zero-external-request preflight as four separate facts; do not synthesize a total state.
- `down`: request Agent Paper stop first; a nonzero or unconfirmed stop aborts before Compose stop/down.
- No Compose `agent-paper-loop` service or `agent-paper` profile remains.

- [ ] **Step 1: Add failing lifecycle ordering tests**

Assert exact call order:

```text
up:     compose app steps -> host service start (only when enabled)
status: compose ps -> host service status
down:   host service stop -> compose stop/down
```

When host stop returns `AGENT_PAPER_SERVICE_STOP_UNCONFIRMED`, assert no Compose stop command runs on both first and repeated `down`. When the flag is disabled, `up` performs only existing app steps while status/down still inspect or stop a leftover exact-owner service. If Compose status fails, host/heartbeat/preflight reads still run and remain separately reported.

- [ ] **Step 2: Implement narrow host-service orchestration**

Add a dedicated helper that invokes only the reviewed adapter with exact root/env/project arguments, parses only its allowlisted JSON managed/heartbeat/preflight fields or fixed `error_code`, and discards all other stdout/stderr. `status` keeps those facts separate from the Compose result and continues even when only the Compose status probe fails. Do not turn `LocalStackCommand` into a generic process-management framework. Preserve existing exact Compose project targeting and broker exclusions.

- [ ] **Step 3: Remove the Compose Agent Paper path**

Delete the `agent-paper-loop` service and remove `agent-paper` from `object-store-init` profiles. Delete the obsolete Compose policy test; do not replace it with another container-service abstraction. Run `docker compose ... config --quiet` with the protected local env to prove the remaining file parses.

- [ ] **Step 4: Update the runbook and README**

Document `nexus-local-stack up/status/down` as the normal workflow. Keep the foreground loop command under diagnostics only. Remove instructions that present direct launchd or Compose Agent Paper commands as normal alternatives. State that Agent Paper offline does not make Web/API offline and that page status remains database-derived. Document the exceptional recovery order: ordinary `down` must first report exit-75 unconfirmed; the user inspects persisted `cycle → job → outputs → order/window`; only then may they run the exact same-owner internal adapter `stop --acknowledge-unconfirmed`; afterward they retry normal `down`.

- [ ] **Step 5: Run lifecycle and static policy tests**

```bash
uv run --project engine pytest \
  engine/tests/unit/ops/test_local_stack.py \
  engine/tests/unit/ops/test_agent_paper_host_service.py \
  engine/tests/unit/ops/test_agent_paper_loop.py \
  -q --import-mode=importlib
uv run --project engine ruff check \
  engine/src/nexus/ops/local_stack.py \
  engine/src/nexus/ops/agent_paper_loop.py \
  scripts/macos/agent-paper-loop-service.py \
  engine/tests/unit/ops/test_local_stack.py \
  engine/tests/unit/ops/test_agent_paper_host_service.py \
  engine/tests/unit/ops/test_agent_paper_loop.py
uv run --project engine mypy engine/src
docker compose --env-file infra/.env.local -f infra/compose.yaml --profile app config --quiet
```

- [ ] **Step 6: Commit unified lifecycle wiring**

```bash
git add engine/src/nexus/ops/local_stack.py \
  engine/tests/unit/ops/test_local_stack.py \
  infra/compose.yaml \
  docs/runbooks/agent-paper-cycle.md README.md
git add -u engine/tests/unit/ops/test_agent_paper_compose_policy.py
git commit -m "feat(paper): manage worker with local stack"
```

---

### Task 5: Offline acceptance and runtime gates

**Files:**
- Modify only if verified acceptance facts require it: `docs/runbooks/agent-paper-cycle.md`
- No production code changes are allowed in this task without returning to the relevant implementation task and its tests.

- [ ] **Step 1: Run the complete affected offline suite**

```bash
uv run --project engine pytest \
  engine/tests/unit/ops/test_agent_paper_once.py \
  engine/tests/unit/ops/test_agent_paper_loop.py \
  engine/tests/unit/ops/test_agent_paper_host_service.py \
  engine/tests/unit/ops/test_local_stack.py \
  engine/tests/integration/test_agent_paper_cycle_pg.py \
  engine/tests/integration/test_agent_paper_loop_heartbeat_migration.py \
  engine/tests/integration/test_workbench_read_service.py \
  -q --import-mode=importlib
uv run --project engine ruff check engine/src scripts/macos engine/tests
uv run --project engine mypy engine/src
git diff --check
```

Run PostgreSQL tests against an isolated local test database; skipped integration tests do not satisfy this step.

- [ ] **Step 2: Verify the offline product contract**

Using fakes/frozen clocks only, prove disabled, preflight-blocked, waiting, duplicate-start, active-action stop, stop-unconfirmed and cross-worktree ownership cases. Confirm Web/API read tests remain green when Agent Paper is offline. Do not start launchd, Provider, model or broker processes.

- [ ] **Step 3: Stop and request Gate A approval**

Gate A authorizes only one bounded local service smoke test through `nexus-local-stack up/status/down`. Before starting, create an isolated test database whose database clock is covered by a complete authority but is outside every decision window, and verify it has no cycle, queued/running Job, open order, due execution window or due performance snapshot. The normal preflight must still pass; no fake runtime mode or relaxed requirement is allowed. With the service pointed only at that isolated database, verify one Worker, fresh heartbeat, repeat-up idempotency, zero Provider/model/business-action calls and confirmed stop. A failed/unknown command is inspected from persisted facts and never replayed automatically.

- [ ] **Step 4: Stop and request Gate B approval**

Gate B separately authorizes a real two-slot internal-paper acceptance using the normal entry, existing protected configuration and current market data. Require per-slot decision, hard-risk result, compliant no-trade or internal simulated order, and same-cycle `/paper` readback. Preserve `execution_locked=true`; do not perform broker paper or live writes.

- [ ] **Step 5: Record only verified acceptance facts**

Update the existing runbook with exact timestamps, calendar authority ID, safe terminal codes and persisted object IDs. Do not create a new phase record. Do not include credentials, prompts, raw model output or personal account facts. If either gate is not approved or does not complete, record it as pending rather than weakening the acceptance criteria.
