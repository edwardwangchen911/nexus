# Agent Paper Service Lifecycle Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Agent Paper a singleton host-side Worker managed by the normal NEXUS backend lifecycle, continuously waiting across XNYS sessions while preserving fail-closed decision, stop, and replay rules.

**Architecture:** `nexus-local-stack serve` remains the only complete backend entry and supervises the existing Compose app services plus one host-side Agent Paper Worker. A focused runtime module owns safe local locks, descriptors, process identity, parent-liveness, and cooperative shutdown; the existing paper authority and loop modules retain calendar, cycle, risk, order, and heartbeat business rules.

**Tech Stack:** Python 3.12, asyncio, SQLAlchemy/asyncpg, Pydantic, `fcntl`/POSIX signals on macOS, Docker Compose, pytest, Ruff, Mypy.

**Spec:** `docs/superpowers/specs/2026-09-16-agent-paper-service-lifecycle-design.md`

## Global Constraints

- Do not add an HTTP service, database table, migration, queue, scheduler platform, Redis, Kafka, Celery, or dependency.
- Keep Agent Paper on the host so Codex SDK continues to use the existing ChatGPT login without copying or mounting credentials.
- `execution_locked=true` remains independent and must never be relaxed by service readiness.
- Every cycle action reads `cycle → job → outputs → order/window` before acting; unknown results stop without replay.
- The Worker binds one `calendar_authority_id` for its lifetime and never silently switches authority.
- Signals request shutdown; they never cancel an active model, provider, order, or performance action.
- Do not expose account IDs, securities, orders, prompts, raw exceptions, credentials, environment values, or captured subprocess output in runtime descriptors or CLI status.
- Preserve all existing user changes. Do not edit `README.md`, `docs/user-guide.md`, `docs/images/`, the unfinished 2026-09-13 plan/spec, or Shadow code in this implementation.
- Keep the launchd implementation present but unused until the new path passes real acceptance; remove it only in a later cleanup commit.
- Stop for separate explicit authorization before any protected-config/service-state acceptance and again before any real Codex SDK, Provider, or internal paper-write acceptance; restore the exact original feature-flag value afterward.
- Use one implementation writer in one worktree because Tasks 1–5 share core files and are sequentially dependent.

## File Map

| Path | Responsibility |
| --- | --- |
| `engine/src/nexus/paper/agent_cycle_authority.py` | Select one service-level XNYS authority and enforce the Worker-bound authority during cycle creation. |
| `engine/src/nexus/ops/agent_paper_once.py` | Thread the expected authority through the trigger path without weakening existing time, account, Universe, quote, or risk checks. |
| `engine/src/nexus/ops/agent_paper_runtime.py` | New focused module for private paths, two locks, descriptor schema, atomic writes, process identity, and safe status classification. |
| `engine/src/nexus/ops/agent_paper_loop.py` | Wait across sessions, heartbeat, bind the calendar, observe shutdown/parent EOF, drain active work, and read back exact terminal facts. |
| `engine/src/nexus/ops/local_stack.py` | Expose `serve`, `infra-up`, `status`, and `down`; supervise the host Worker and existing Compose services. |
| `engine/tests/integration/test_agent_paper_cycle_pg.py` | PostgreSQL authority selection, fixed-ID enforcement, half-day, DST, and mismatch regressions. |
| `engine/tests/unit/ops/test_agent_paper_runtime.py` | New deterministic filesystem, descriptor, lock, owner/mode, stale PID, and parent-pipe tests. |
| `engine/tests/unit/ops/test_agent_paper_loop.py` | Cross-session waits, heartbeat, cooperative drain, no-cancel, and fixed-authority loop policy. |
| `engine/tests/unit/ops/test_local_stack.py` | Full lifecycle command/status contracts and failure-safe stop ordering. |
| `engine/tests/unit/ops/test_agent_paper_host_service.py` | Keep legacy launchd tests green while it remains a disabled rollback implementation. |
| `docs/runbooks/agent-paper-cycle.md` | Replace launchd as the normal path with `nexus-local-stack serve/status/down`; record rollback and acceptance commands. |

---

### Task 1: Pin one service-level XNYS calendar authority

**Files:**
- Modify: `engine/src/nexus/paper/agent_cycle_authority.py`
- Modify: `engine/src/nexus/ops/agent_paper_once.py`
- Modify: `engine/src/nexus/ops/agent_paper_loop.py`
- Test: `engine/tests/integration/test_agent_paper_cycle_pg.py`
- Test: `engine/tests/unit/ops/test_agent_paper_once.py`

**Interfaces:**
- Produces: `ServiceCalendarAuthority`, `select_agent_paper_service_calendar_authority(session, *, now)`, and mandatory `expected_calendar_authority_id` validation in `load_quasi_realtime_cycle_authority`.
- Produces: optional keyword `expected_calendar_authority_id: UUID | None` on `trigger_once`/`trigger_in_session`; the service Worker always supplies it, while the bounded one-shot path resolves exactly one ID before external collection.
- Consumes later: Tasks 3 and 4 pass the selected ID into every loop inspection and trigger.

- [ ] **Step 1: Write failing service-selector and mismatch integration tests**

Add cases beside the existing quasi-realtime authority tests. Use the existing calendar seeding helpers and fixed UTC values; assert the selector works outside the decision window and the cycle loader rejects a different ID.

```python
async def test_service_calendar_selector_works_before_window_and_pins_one_authority(
    migrated_database_url: str,
) -> None:
    await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions() as session:
            selected = await select_agent_paper_service_calendar_authority(
                session,
                now=datetime(2026, 8, 26, 9, 0, tzinfo=UTC),
            )
        assert selected.session == date(2026, 8, 26)
        assert selected.next_session > selected.session
    finally:
        await engine.dispose()


async def test_cycle_loader_rejects_authority_switch(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions() as session:
            with pytest.raises(
                ValueError,
                match="AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH",
            ):
                await load_quasi_realtime_cycle_authority(
                    session,
                    account_id=seed.account_id,
                    now=datetime(2026, 8, 26, 13, 0, tzinfo=UTC),
                    expected_calendar_authority_id=UUID(int=0),
                )
    finally:
        await engine.dispose()
```

- [ ] **Step 2: Run the focused tests and verify the new interfaces are absent**

Run:

```bash
uv run --project engine pytest engine/tests/integration/test_agent_paper_cycle_pg.py -q --import-mode=importlib
```

Expected: the new tests fail because the selector and required authority argument do not exist; existing tests remain collected.

- [ ] **Step 3: Implement the read-only selector and fixed-ID loader**

Add a frozen result model and query using the database-provided UTC clock already passed as `now`. Selection is deterministic and does not load account or Universe facts.

```python
class ServiceCalendarAuthority(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    calendar_authority_id: UUID
    as_of: datetime
    local_date: date
    session: date
    decision_window_start_at: datetime
    market_open_at: datetime
    market_close_at: datetime
    next_session: date
    next_decision_window_start_at: datetime
    next_market_close_at: datetime


async def select_agent_paper_service_calendar_authority(
    session: AsyncSession,
    *,
    now: datetime,
) -> ServiceCalendarAuthority:
    require_utc(now)
    local_date = now.astimezone(ZoneInfo("America/New_York")).date()
    authority = await session.scalar(
        select(ExchangeCalendarAuthorityRecord)
        .where(
            ExchangeCalendarAuthorityRecord.exchange == "XNYS",
            ExchangeCalendarAuthorityRecord.completeness_status == "complete",
            ExchangeCalendarAuthorityRecord.range_start <= local_date,
            ExchangeCalendarAuthorityRecord.range_end > local_date,
            ExchangeCalendarAuthorityRecord.published_at <= now,
            ExchangeCalendarAuthorityRecord.created_at <= now,
        )
        .order_by(
            ExchangeCalendarAuthorityRecord.published_at.desc(),
            ExchangeCalendarAuthorityRecord.id.desc(),
        )
        .limit(1)
    )
    if authority is None:
        raise ValueError("AGENT_PAPER_CALENDAR_UNAVAILABLE")
    rows = tuple(
        (
            await session.scalars(
                select(ExchangeCalendarSessionRecord)
                .where(
                    ExchangeCalendarSessionRecord.calendar_authority_id == authority.id,
                    ExchangeCalendarSessionRecord.session >= local_date,
                    ExchangeCalendarSessionRecord.created_at <= now,
                )
                .order_by(ExchangeCalendarSessionRecord.session.asc())
                .limit(3)
            )
        ).all()
    )
    if rows and rows[0].session == local_date and now >= rows[0].market_close_at:
        rows = rows[1:]
    if len(rows) < 2 or rows[1].session <= rows[0].session:
        raise ValueError("AGENT_PAPER_CALENDAR_UNAVAILABLE")
    calendar_session, next_session = rows[:2]
    ny = ZoneInfo("America/New_York")
    return ServiceCalendarAuthority(
        calendar_authority_id=authority.id,
        as_of=now,
        local_date=local_date,
        session=calendar_session.session,
        decision_window_start_at=datetime.combine(
            calendar_session.session,
            time(7),
            tzinfo=ny,
        ).astimezone(UTC),
        market_open_at=calendar_session.market_open_at,
        market_close_at=calendar_session.market_close_at,
        next_session=next_session.session,
        next_decision_window_start_at=datetime.combine(
            next_session.session,
            time(7),
            tzinfo=ny,
        ).astimezone(UTC),
        next_market_close_at=next_session.market_close_at,
    )
```

The two-session requirement is deliberate: before or during a session, the fixed authority must also resolve the strict following session; after close, weekend, or holiday, it must resolve the next two sessions. If it cannot, fail before starting the Worker rather than accepting an authority that will expire at the first boundary.
The newest published candidate is authoritative. If that candidate is marked `complete` but its session rows fail the two-session or time-point checks, return `AGENT_PAPER_CALENDAR_UNAVAILABLE`; do not fall back to an older candidate because that would silently replace a damaged current authority. Add a regression with a valid older authority plus a newer malformed authority and require this fail-closed result.

Change `load_quasi_realtime_cycle_authority` to accept `expected_calendar_authority_id: UUID`, add `ExchangeCalendarAuthorityRecord.id == expected_calendar_authority_id` to its query, and map a missing row to `AGENT_PAPER_CALENDAR_AUTHORITY_MISMATCH` when that ID exists but is not valid for the decision time.
Move the close-window check after the persisted session is loaded: require local time at or after 07:00 ET and UTC `now < calendar_session.market_close_at`. Remove the fixed 16:00 ET close assumption so half-days use the authority row without relaxing the opening boundary.

- [ ] **Step 4: Thread the expected ID through both trigger reads**

The service path supplies its bound ID. The one-shot path resolves a selector result before collecting providers and keeps that value for the initial and final authority reads.

```python
async def trigger_in_session(
    ...,
    expected_calendar_authority_id: UUID | None = None,
) -> AgentPaperCycleResult:
    initial_time = await session.scalar(select(func.clock_timestamp()))
    selected = (
        await select_agent_paper_service_calendar_authority(session, now=initial_time)
        if expected_calendar_authority_id is None
        else None
    )
    calendar_authority_id = (
        selected.calendar_authority_id
        if selected is not None
        else expected_calendar_authority_id
    )
    assert calendar_authority_id is not None
    initial_authority = await load_quasi_realtime_cycle_authority(
        session,
        account_id=request.account_id,
        now=initial_time,
        expected_calendar_authority_id=calendar_authority_id,
    )
```

Apply the same `calendar_authority_id` to the final read after Schwab capture. Update every internal call and existing test fixture explicitly; do not add a fallback that reselects after provider work starts.

- [ ] **Step 5: Add half-day, DST, weekend, and range-end tests**

Use frozen authority rows to assert:

```python
assert half_day.market_close_at == datetime(2026, 11, 27, 18, 0, tzinfo=UTC)
assert summer.decision_window_start_at == datetime(2026, 7, 6, 11, 0, tzinfo=UTC)
assert winter.decision_window_start_at == datetime(2026, 12, 1, 12, 0, tzinfo=UTC)
assert summer.market_open_at == datetime(2026, 7, 6, 13, 30, tzinfo=UTC)
assert winter.market_open_at == datetime(2026, 12, 1, 14, 30, tzinfo=UTC)
assert weekend.session == date(2026, 11, 30)
```

Also assert a fixed authority whose range/session set cannot produce the next session raises `AGENT_PAPER_CALENDAR_UNAVAILABLE`; a newer unrelated authority must not alter a loader called with the older expected ID.

- [ ] **Step 6: Run focused validation**

Run:

```bash
uv run --project engine pytest \
  engine/tests/integration/test_agent_paper_cycle_pg.py \
  engine/tests/unit/ops/test_agent_paper_once.py \
  -q --import-mode=importlib
uv run --project engine ruff check \
  engine/src/nexus/paper/agent_cycle_authority.py \
  engine/src/nexus/ops/agent_paper_once.py \
  engine/src/nexus/ops/agent_paper_loop.py \
  engine/tests/integration/test_agent_paper_cycle_pg.py \
  engine/tests/unit/ops/test_agent_paper_once.py
uv run --project engine mypy engine/src
```

Expected: all commands exit 0; no external request is made by the selector tests.

- [ ] **Step 7: Commit the calendar contract**

```bash
git add engine/src/nexus/paper/agent_cycle_authority.py \
  engine/src/nexus/ops/agent_paper_once.py \
  engine/src/nexus/ops/agent_paper_loop.py \
  engine/tests/integration/test_agent_paper_cycle_pg.py \
  engine/tests/unit/ops/test_agent_paper_once.py
git commit -m "fix(paper): pin service calendar authority"
```

---

### Task 2: Add secure supervisor and Worker runtime facts

**Files:**
- Create: `engine/src/nexus/ops/agent_paper_runtime.py`
- Create: `engine/tests/unit/ops/test_agent_paper_runtime.py`

**Interfaces:**
- Produces: `RuntimeRole`, `RuntimeEntrypoint`, `RuntimeState`, `RuntimeExitReason`, `AgentPaperRuntimeDescriptor`, `RuntimePaths`, `RuntimeLock`, `prepare_runtime_paths`, `acquire_runtime_lock`, `write_runtime_descriptor`, `read_runtime_descriptor`, and `classify_runtime`.
- Consumes later: Task 3 uses the Worker lock/descriptor; Tasks 4–5 use both roles and runtime classification.

- [ ] **Step 1: Write failing private-path and descriptor tests**

Cover mode `0700` directories, mode `0600` regular single-link files, no symlinks/hardlinks, current uid, atomic replacement, schema rejection, and secret-field rejection.

```python
def test_prepare_runtime_paths_rejects_symlink_before_creation(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir(mode=0o700)
    (tmp_path / ".secrets").symlink_to(outside, target_is_directory=True)
    with pytest.raises(RuntimeSafetyError, match="AGENT_PAPER_RUNTIME_UNSAFE"):
        prepare_runtime_paths(tmp_path)


def test_descriptor_schema_has_only_safe_lifecycle_fields() -> None:
    assert set(AgentPaperRuntimeDescriptor.model_fields) == {
        "schema",
        "role",
        "pid",
        "process_started_at",
        "entrypoint",
        "process_version",
        "state",
        "calendar_authority_id",
        "exit_reason",
        "updated_at",
    }
```

- [ ] **Step 2: Write failing lock, PID-reuse, and stale-runtime tests**

Use a second process, not a second file object in the same process, for `flock` exclusion. Inject `process_probe(pid) -> ProcessIdentity | None` so tests can model PID reuse without signaling real processes.

```python
def test_worker_lock_blocks_a_second_process(runtime_paths: RuntimePaths) -> None:
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    with acquire_runtime_lock(runtime_paths.worker_lock):
        child = context.Process(
            target=probe_lock_in_child,
            args=(runtime_paths.worker_lock, sender),
        )
        child.start()
        sender.close()
        if not receiver.poll(5):
            child.terminate()
            child.join(timeout=5)
            pytest.fail("child lock probe timed out")
        result = receiver.recv()
        child.join(timeout=5)
        if child.is_alive():
            child.terminate()
            child.join(timeout=5)
        assert child.exitcode == 0
    assert result == "AGENT_PAPER_RUNTIME_LOCKED"


def test_pid_reuse_is_unconfirmed(runtime_paths: RuntimePaths) -> None:
    descriptor = AgentPaperRuntimeDescriptor(
        schema="nexus_agent_paper_runtime_v1",
        role=RuntimeRole.WORKER,
        pid=321,
        process_started_at="token-a",
        entrypoint="nexus-engine-agent-paper-loop",
        process_version="agent-paper-loop-v1",
        state=RuntimeState.RUNNING,
        calendar_authority_id=UUID(int=1),
        exit_reason=None,
        updated_at=datetime(2026, 9, 16, tzinfo=UTC),
    )
    status = classify_runtime(
        descriptor,
        lock_held=True,
        heartbeat_fresh=True,
        process_identity=ProcessIdentity(
            pid=321,
            started_at="token-b",
            entrypoint=RuntimeEntrypoint.WORKER,
        ),
    )
    assert status.code == "unconfirmed"
```

Define the child helper immediately above the test; it must not call any application service:

```python
def probe_lock_in_child(path: Path, sender: Connection) -> None:
    try:
        with acquire_runtime_lock(path):
            payload = "acquired"
    except RuntimeSafetyError as error:
        payload = str(error)
    sender.send(payload)
    sender.close()
```

- [ ] **Step 3: Run the new unit file and verify it fails at import**

Run:

```bash
uv run --project engine pytest engine/tests/unit/ops/test_agent_paper_runtime.py -q --import-mode=importlib
```

Expected: collection fails because `nexus.ops.agent_paper_runtime` does not exist.

- [ ] **Step 4: Implement the focused runtime module**

Use explicit enums and a forbidden-extra Pydantic model:

```python
class RuntimeRole(StrEnum):
    SUPERVISOR = "supervisor"
    WORKER = "worker"


class RuntimeEntrypoint(StrEnum):
    SUPERVISOR = "nexus-local-stack serve"
    WORKER = "nexus-engine-agent-paper-loop"


class RuntimeState(StrEnum):
    STARTING = "starting"
    RUNNING = "running"
    STOPPED = "stopped"
    UNCONFIRMED = "unconfirmed"


class RuntimeExitReason(StrEnum):
    STOP_REQUESTED = "STOP_REQUESTED"
    STOP_UNCONFIRMED = "STOP_UNCONFIRMED"
    WORKER_EXITED_UNCLEANLY = "WORKER_EXITED_UNCLEANLY"
    WORKER_EXIT_REASON_UNKNOWN = "WORKER_EXIT_REASON_UNKNOWN"
    RUNTIME_ABSENT_UNCONFIRMED = "RUNTIME_ABSENT_UNCONFIRMED"


@dataclass(frozen=True)
class RuntimePaths:
    repo_root: Path
    private_directory: Path
    supervisor_lock: Path
    supervisor_descriptor: Path
    worker_lock: Path
    worker_descriptor: Path


class AgentPaperRuntimeDescriptor(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema: Literal["nexus_agent_paper_runtime_v1"]
    role: RuntimeRole
    pid: int = Field(ge=1)
    process_started_at: str = Field(min_length=1, max_length=128)
    entrypoint: RuntimeEntrypoint
    process_version: str = Field(min_length=1, max_length=100)
    state: RuntimeState
    calendar_authority_id: UUID | None = None
    exit_reason: RuntimeExitReason | None = None
    updated_at: datetime
```

`prepare_runtime_paths` validates `.secrets` and `.secrets/nexus-agent-paper` before opening runtime files. `acquire_runtime_lock` uses `os.open(path, O_RDWR | O_CREAT | O_NOFOLLOW, 0o600)`, `os.fstat`, and `fcntl.flock(fd, LOCK_EX | LOCK_NB)`; it retains the fd for the context lifetime. Descriptor writes use a same-directory `O_EXCL | O_NOFOLLOW` temporary file, `fsync(file)`, `os.replace`, then `fsync(directory)`. Reads reject files with wrong type, uid, mode, link count, schema, or size above 16 KiB.

- [ ] **Step 5: Implement process identity and classification without emitting command output**

Represent the process start token and normalized entrypoint separately from the descriptor and compare both before any signal:

```python
@dataclass(frozen=True)
class ProcessIdentity:
    pid: int
    started_at: str
    entrypoint: RuntimeEntrypoint


def process_identity(pid: int) -> ProcessIdentity | None:
    completed = subprocess.run(
        ("ps", "-ww", "-p", str(pid), "-o", "lstart=", "-o", "command="),
        check=False,
        capture_output=True,
        text=True,
        timeout=2,
    )
    raw = completed.stdout.strip()
    if completed.returncode != 0 or not raw:
        return None
    started_at, command = split_ps_identity(raw)
    entrypoint = normalize_runtime_entrypoint(command)
    if entrypoint is None:
        return None
    return ProcessIdentity(pid=pid, started_at=started_at, entrypoint=entrypoint)
```

`RuntimeEntrypoint` has only `SUPERVISOR` and `WORKER`. `normalize_runtime_entrypoint` accepts only a command containing the reviewed token sequence `nexus-local-stack serve` or the reviewed executable basename `nexus-engine-agent-paper-loop`; it returns no raw command. Never log, return, or print `ps` stderr/stdout. `classify_runtime` reports `running`, `stopped`, or `unconfirmed` only when lock, descriptor, PID, start token, normalized entrypoint, and Worker heartbeat agree.

Add a parameterized rejection matrix for process identity:

```python
@pytest.mark.parametrize(
    ("command", "expected"),
    [
        ("/repo/.venv/bin/nexus-local-stack serve", RuntimeEntrypoint.SUPERVISOR),
        ("uv run --project engine nexus-local-stack serve", RuntimeEntrypoint.SUPERVISOR),
        ("/repo/.venv/bin/nexus-engine-agent-paper-loop --parent-fd 7", RuntimeEntrypoint.WORKER),
        ("python harmless.py --label 'nexus-local-stack serve'", None),
        ("/tmp/nexus-engine-agent-paper-loop-evil", None),
        ("/repo/.venv/bin/nexus-local-stack status", None),
        ("", None),
    ],
)
def test_runtime_entrypoint_normalization_is_exact(
    command: str,
    expected: RuntimeEntrypoint | None,
) -> None:
    assert normalize_runtime_entrypoint(command) == expected
```

Add role-swap tests where a supervisor descriptor is paired with a Worker identity and conversely; both must classify `unconfirmed`. Inject `ps` timeout, nonzero exit, empty stdout, and secret-looking stderr; require `process_identity` to return `None` without any raw text in the returned object, captured logs, or raised message.

- [ ] **Step 6: Run focused security validation**

Run:

```bash
uv run --project engine pytest engine/tests/unit/ops/test_agent_paper_runtime.py -q --import-mode=importlib
uv run --project engine ruff check \
  engine/src/nexus/ops/agent_paper_runtime.py \
  engine/tests/unit/ops/test_agent_paper_runtime.py
uv run --project engine mypy engine/src
```

Expected: all commands exit 0; tests do not signal any process they did not spawn.

- [ ] **Step 7: Commit runtime primitives**

```bash
git add engine/src/nexus/ops/agent_paper_runtime.py \
  engine/tests/unit/ops/test_agent_paper_runtime.py
git commit -m "feat(paper): add safe runtime lifecycle facts"
```

---

### Task 3: Make the Agent Paper loop long-lived and cooperatively stoppable

**Files:**
- Modify: `engine/src/nexus/ops/agent_paper_loop.py`
- Modify: `engine/src/nexus/ops/agent_paper_once.py`
- Modify: `engine/tests/unit/ops/test_agent_paper_loop.py`
- Modify: `engine/tests/integration/test_agent_paper_cycle_pg.py`

**Interfaces:**
- Consumes: Task 1 fixed `calendar_authority_id`; Task 2 Worker lock/descriptor.
- Produces: `WAIT_FOR_SESSION`, `STOP_REQUESTED`, `STOP_UNCONFIRMED`, `STOP_CALENDAR_UNAVAILABLE`, and `STOP_CALENDAR_AUTHORITY_MISMATCH` loop reasons.
- Produces: `run_foreground_loop(..., shutdown_requested, confirm_shutdown, ...)` that never cancels an active action.
- Produces: CLI arguments `--calendar-authority-id` and `--parent-fd`, used only by the supervisor path.

- [ ] **Step 1: Replace the after-close stop expectation with a wait policy test**

Extend `AgentPaperLoopSnapshot` test fixtures with `decision_window_start_at`, `decision_window_end_at`, and `next_decision_window_start_at` from the fixed calendar authority.

```python
def test_after_close_waits_for_next_authoritative_session() -> None:
    state = snapshot(
        now=AFTER_CLOSE,
        cycle_status="completed",
        cycle_slot_at=LAST_SLOT,
        decision_window_end_at=datetime(2026, 9, 14, 20, 0, tzinfo=UTC),
        next_decision_window_start_at=datetime(2026, 9, 15, 11, 0, tzinfo=UTC),
    )
    assert loop.choose_loop_action(state) == A.WAIT_FOR_SESSION
```

- [ ] **Step 2: Write shutdown tests before changing the loop**

Cover idle shutdown, shutdown during a running action, heartbeat during drain, one exact confirmation read, and confirmation failure. The active coroutine must set a flag in its `finally`; assert it was not cancelled.

```python
def test_shutdown_during_action_waits_without_cancelling_then_reads_once() -> None:
    async def scenario() -> tuple[loop.AgentPaperLoopAction, bool, int]:
        shutdown = asyncio.Event()
        action_started = asyncio.Event()
        action_can_finish = asyncio.Event()
        cancelled = False
        confirmations = 0

        async def inspect() -> loop.AgentPaperLoopSnapshot:
            return snapshot(cycle_status="completed", cycle_slot_at=PREVIOUS_SLOT)

        async def execute(_action, _snapshot):
            nonlocal cancelled
            action_started.set()
            try:
                await action_can_finish.wait()
            except asyncio.CancelledError:
                cancelled = True
                raise

        async def confirm_shutdown() -> bool:
            nonlocal confirmations
            confirmations += 1
            return True

        task = asyncio.create_task(
            run_foreground_loop(
                inspect=inspect,
                execute=execute,
                sleep=asyncio.sleep,
                heartbeat=noop_heartbeat,
                shutdown_requested=shutdown.is_set,
                confirm_shutdown=confirm_shutdown,
            )
        )
        await action_started.wait()
        shutdown.set()
        action_can_finish.set()
        return await task, cancelled, confirmations

    result, cancelled, confirmations = asyncio.run(scenario())
    assert result == A.STOP_REQUESTED
    assert cancelled is False
    assert confirmations == 1
```

- [ ] **Step 3: Run the policy tests and verify the expected failures**

Run:

```bash
uv run --project engine pytest engine/tests/unit/ops/test_agent_paper_loop.py -q --import-mode=importlib
```

Expected: the new action/reason names and shutdown parameters are missing, and the old after-close assertion still returns `STOP_SESSION_CLOSED`.

- [ ] **Step 4: Implement authoritative session waiting**

Add the new action/reasons, remove fixed `time(16)` scheduling from `choose_loop_action`, and let inspection load current/next sessions only from the Worker-bound authority. Keep the 07:00 ET decision start, but derive close and next session from persisted rows.

```python
if snapshot.now < snapshot.decision_window_start_at:
    return AgentPaperLoopAction.WAIT_FOR_SESSION
if snapshot.now >= snapshot.decision_window_end_at:
    return AgentPaperLoopAction.WAIT_FOR_SESSION
```

`WAIT_FOR_SESSION` sleeps in `min(remaining, 15)` chunks and persists heartbeat after every chunk. A missing next session returns `STOP_CALENDAR_UNAVAILABLE`; an ID mismatch returns `STOP_CALENDAR_AUTHORITY_MISMATCH`. Do not call a provider, model, trigger, order, or performance publisher while waiting unless an exact persisted `performance_due_at` already exists.

- [ ] **Step 5: Implement cooperative shutdown without task cancellation**

Change the loop signature:

```python
async def run_foreground_loop(
    *,
    inspect: Callable[[], Awaitable[AgentPaperLoopSnapshot]],
    execute: Callable[[AgentPaperLoopAction, AgentPaperLoopSnapshot], Awaitable[AgentPaperLoopAction | None]],
    sleep: Callable[[float], Awaitable[None]],
    heartbeat: Callable[[datetime], Awaitable[None]],
    shutdown_requested: Callable[[], bool],
    confirm_shutdown: Callable[[], Awaitable[bool]],
    heartbeat_interval: timedelta = timedelta(seconds=15),
    clock: Callable[[], datetime] = _utc_now,
    monotonic: Callable[[], float] = monotonic_clock,
) -> AgentPaperLoopAction:
```

At the top of an idle iteration, return `STOP_REQUESTED` when requested. During an action, keep the heartbeat timer but do not return early and do not cancel `action_task`. After the action ends, call `confirm_shutdown` exactly once; map `True` to `STOP_REQUESTED`, `False` or an exception to `STOP_UNCONFIRMED`. Only the heartbeat sleep task may be cancelled after the action finishes.

- [ ] **Step 6: Bind Worker lock, descriptor, signals, and parent EOF in the concrete runner**

`run_local_agent_paper_loop` receives the fixed authority and shutdown event. Acquire `agent-paper.lock` before first heartbeat or inspection, then write the Worker descriptor. Install signal handlers that only call `shutdown_event.set`; repeated `SIGINT`/`SIGTERM` calls perform the same idempotent set and never cancel the action. When `--parent-fd` is present, register a reader whose EOF sets the event and immediately removes that reader so EOF cannot busy-loop; never treat bytes from the pipe as commands.

```python
loop = asyncio.get_running_loop()
for signal_name in (signal.SIGINT, signal.SIGTERM):
    loop.add_signal_handler(signal_name, shutdown_event.set)
if parent_fd is not None:
    loop.add_reader(parent_fd, _mark_shutdown_on_parent_eof, parent_fd, shutdown_event)
```

`_mark_shutdown_on_parent_eof` calls `os.read(parent_fd, 1)`; on `b""` it calls `loop.remove_reader(parent_fd)` before setting the event. In the runner `finally`, remove both signal handlers, remove any remaining fd reader, and close the fd exactly once. `confirm_shutdown` performs one `inspect_agent_paper_loop` call with the bound authority and returns true only when the exact current cycle, Job, outputs, order, and window are in known states. It does not trigger a producer or retry a request.

- [ ] **Step 7: Add Worker-lock-before-side-effect and parent-EOF tests**

Assert failure to acquire the Worker lock produces no heartbeat, inspection, or action. Use `os.pipe()` to close the supervisor end and assert the same cooperative shutdown path is used and the fd reader is removed. Invoke both installed signal callbacks twice while an action is blocked and assert the action is not cancelled. Add a regression where heartbeat persistence fails during an active action: the action still reaches its bounded result, then the loop returns `STOP_UNCONFIRMED`. Wrap every blocked-action scenario in `asyncio.timeout(1)` so a broken drain fails the test instead of hanging; after exit, assert signal handlers and the parent fd are removed/closed.

- [ ] **Step 8: Run loop and cross-layer validation**

Run:

```bash
uv run --project engine pytest \
  engine/tests/unit/ops/test_agent_paper_loop.py \
  engine/tests/integration/test_agent_paper_cycle_pg.py \
  -q --import-mode=importlib
uv run --project engine ruff check \
  engine/src/nexus/ops/agent_paper_loop.py \
  engine/src/nexus/ops/agent_paper_once.py \
  engine/tests/unit/ops/test_agent_paper_loop.py \
  engine/tests/integration/test_agent_paper_cycle_pg.py
uv run --project engine mypy engine/src
```

Expected: all commands exit 0; existing exact-Job isolation, no-trade, order-window, and heartbeat tests remain green.

- [ ] **Step 9: Commit the long-lived loop**

```bash
git add engine/src/nexus/ops/agent_paper_loop.py \
  engine/src/nexus/ops/agent_paper_once.py \
  engine/tests/unit/ops/test_agent_paper_loop.py \
  engine/tests/integration/test_agent_paper_cycle_pg.py
git commit -m "feat(paper): keep loop across sessions safely"
```

---

### Task 4: Add the unified `serve` supervisor and lifecycle commands

**Files:**
- Modify: `engine/src/nexus/ops/agent_paper_runtime.py`
- Modify: `engine/src/nexus/ops/local_stack.py`
- Modify: `engine/tests/unit/ops/test_agent_paper_runtime.py`
- Modify: `engine/tests/unit/ops/test_local_stack.py`
- Test: `engine/tests/unit/ops/test_agent_paper_host_service.py`

**Interfaces:**
- Consumes: Task 1 calendar selector; Task 2 supervisor/Worker runtime facts; Task 3 Worker CLI and cooperative stop.
- Produces: `LocalStackAction.SERVE`, `LocalStackAction.INFRA_UP`, compatibility `UP`, aggregated `LocalBackendStatus`, `WorkerSpawnSpec`, `LocalBackendController`, and safe functions `prepare_local_backend`, `serve_local_backend`, `status_local_backend`, `down_local_backend`.
- Produces: private `LocalBackendDependencies` injection seam so unit tests cannot touch Docker, real processes, the database, signals, or wall-clock sleeps.

Define the seam once and construct it explicitly in tests:

```python
class WorkerProcess(Protocol):
    pid: int

    def poll(self) -> int | None: ...
    def wait(self, timeout: float | None = None) -> int: ...


class SignalRegistration(Protocol):
    def close(self) -> None: ...


@dataclass(frozen=True)
class WorkerSpawnSpec:
    argv: tuple[str, ...]
    cwd: Path
    environment: Mapping[str, str] = field(repr=False, compare=False)
    pass_fds: tuple[int, ...]
    start_new_session: Literal[True]


@dataclass(frozen=True)
class LocalBackendDependencies:
    run_compose: Callable[[LocalStackAction], tuple[LocalStackStepResult, ...]]
    preflight: Callable[[], Mapping[str, object]]
    select_calendar: Callable[[], ServiceCalendarAuthority]
    spawn_worker: Callable[[WorkerSpawnSpec], WorkerProcess]
    read_status_facts: Callable[[], LocalBackendPersistedFacts]
    process_probe: Callable[[int], ProcessIdentity | None]
    signal_process: Callable[[int, signal.Signals], None]
    install_signal_handlers: Callable[[Callable[[], None]], SignalRegistration]
    clock: Callable[[], datetime]
    sleep: Callable[[float], None]
```

Production creates this object from the reviewed functions. Tests pass fakes that append fixed safe call names to a list and return typed fixture values; no module-global monkeypatch may fall through to a real side effect.

`serve_local_backend` creates a `threading.Event` shutdown latch and calls the injected `install_signal_handlers(latch.set)` before `prepare_local_backend`. Production installs `SIGINT`, `SIGTERM`, and `SIGHUP` handlers in the main thread; each handler only calls the latch. Tests inject a registrar that captures the callback and never calls Python `signal.signal` from a test thread.

`prepare_local_backend(..., shutdown_requested: Callable[[], bool])` performs bounded startup and returns a controller that still owns `serve.lock`; it never reports a live service by returning and releasing the lock. Immediately after acquiring the lock, the controller binds the same latch. It checks the latch before Compose, after Compose, before spawn, after spawn, and during heartbeat wait. A set latch enters the same owned drain path for resources acquired so far. The controller contract is:

```python
class LocalBackendController:
    @property
    def startup_status(self) -> LocalBackendStatus: ...

    @property
    def supervisor_lock_held(self) -> bool: ...

    def request_shutdown(self) -> None: ...

    def run_forever(self) -> LocalBackendStatus: ...

    def close(self) -> None: ...
```

`run_forever` blocks while disabled, blocked, or ready, updates descriptors from the supervised child, and returns only a confirmed `stopped` status. If shutdown is unconfirmed it remains in the `unconfirmed` supervision state, keeps the lock/dependencies, and does not return. `close() -> None` is idempotent and is legal only after `run_forever` returned confirmed stopped or startup never created a Worker; otherwise it raises the fixed internal guard without releasing ownership.

`serve_local_backend(..., status_sink: Callable[[LocalBackendStatus], None]) -> int` sends exactly one safe startup status to the sink, calls `run_forever`, sends its one confirmed terminal status, and then calls `close`. It always removes the signal registration in an outer `finally`, including configuration/lock failures before a controller exists. Production uses the redacted JSON emitter; tests use a closure that appends the status and sets a `threading.Event`. A defensive fake returning `unconfirmed` must produce exit code 1, emit no `stopped`, and skip `close`; the real controller never returns that state.

Resource ownership begins immediately after `serve.lock` is acquired: instantiate the controller with the lock and preinstalled shutdown latch before starting Compose, and register each pipe end/child as soon as it is created. Startup follows this failure contract:

| Failure point | Controller state and cleanup |
| --- | --- |
| Unsafe config/runtime path or lock unavailable | No controller and no component started; return the fixed configuration error. |
| Compose partial failure | Return controller with `degraded_startup_failed` / `LOCAL_INFRA_START_FAILED`; no Worker; retain lock until controlled shutdown of the exact Compose project. |
| Preflight or selector blocked/unavailable | Return disabled/blocked controller; no Worker; close any unused pipe; retain lock and infrastructure for status/down. |
| Pipe creation or Worker spawn fails | Close owned pipe ends; record `AGENT_PAPER_WORKER_SPAWN_FAILED`; return degraded controller with no Worker. |
| Worker spawned, then descriptor write or heartbeat confirmation fails | Attach child and pipe to controller before the fallible operation, set `unconfirmed`, request cooperative shutdown, and enter the same drain path; do not raise past the ownership guard. |
| Heartbeat absent after 30 seconds | Record `AGENT_PAPER_HEARTBEAT_START_TIMEOUT`; never restart; cooperatively drain the child. |
| Worker stop cannot be confirmed | Keep supervisor lock and required dependencies online; `run_forever` continues in `unconfirmed` rather than returning a false stopped result. |
| Shutdown latch set at any startup boundary | Stop acquiring new resources and drain only the exact resources already owned; after spawn, use cooperative Worker shutdown. |

`close` may close pipe fds, stop the exact Compose project, and release `serve.lock` only when the Worker was never spawned or its exit and released Worker lock are confirmed. Failure to write a descriptor never transfers ownership to garbage collection and never justifies killing the child.

- [ ] **Step 1: Write failing CLI action and compatibility tests**

Assert `infra-up` builds the exact former `up` Compose sequence, `up` emits a deprecation code without changing scope, and `serve` is the only action that starts the host Worker.

```python
def test_infra_up_retains_exact_app_only_compose_scope(repo_root: Path) -> None:
    commands = build_local_stack_commands(
        LocalStackAction.INFRA_UP,
        repo_root=repo_root,
        env_file=repo_root / "infra/.env.local",
        project_name="nexus-phase3-local",
    )
    assert commands[-1].argv[-5:] == ("up", "-d", "--no-build", "api", "worker")
    assert all("agent-paper-loop" not in item for command in commands for item in command.argv)


def test_serve_is_rejected_when_supervisor_lock_is_held(runtime_paths: RuntimePaths) -> None:
    calls: list[str] = []
    dependencies = local_backend_dependencies_fixture(calls=calls)
    with acquire_runtime_lock(runtime_paths.supervisor_lock):
        with pytest.raises(
            LocalStackConfigurationError,
            match="LOCAL_BACKEND_ALREADY_RUNNING",
        ):
            prepare_local_backend(
                repo_root=runtime_paths.repo_root,
                env_file=runtime_paths.repo_root / "infra/.env.local",
                project_name="nexus-test",
                dependencies=dependencies,
                shutdown_requested=lambda: False,
            )
    assert calls == []
```

- [ ] **Step 2: Write supervisor-state tests for disabled, blocked, ready, and crashed Worker**

Inject Compose runner, preflight, selector, child factory, heartbeat reader, and clock. Assert no real Docker, database, SDK, or signal call occurs.

```python
@pytest.mark.parametrize(
    ("enabled", "preflight", "expected"),
    [
        (False, "disabled", "ready_agent_disabled"),
        (True, "blocked", "degraded_preflight_blocked"),
        (True, "ready", "ready"),
    ],
)
def test_serve_state_matrix(enabled: bool, preflight: str, expected: str) -> None:
    calls: list[str] = []
    dependencies = local_backend_dependencies_fixture(
        calls=calls,
        enabled=enabled,
        preflight_status=preflight,
    )
    controller = prepare_local_backend(
        repo_root=TEST_REPO_ROOT,
        env_file=TEST_REPO_ROOT / "infra/.env.local",
        project_name="nexus-test",
        dependencies=dependencies,
        shutdown_requested=lambda: False,
    )
    try:
        assert controller.startup_status.code == expected
        assert controller.supervisor_lock_held is True
        controller.request_shutdown()
        assert controller.run_forever().code == "stopped"
    finally:
        controller.close()
```

Create `local_backend_dependencies_fixture` and `TEST_REPO_ROOT` as test-only fixtures: the root contains only a minimal `infra/compose.yaml`, a Git-ignored mode-protected env file, and a mode-protected runtime directory. Every dependency returns a fixed typed value and records its safe call name. The ready fixture returns a fake Worker whose `poll()` remains `None` until controller shutdown; disabled/blocked fixtures raise if `spawn_worker` is invoked.

Add cases for a nonzero Worker exit (`WORKER_EXITED_UNCLEANLY`), missing final Worker descriptor (`WORKER_EXIT_REASON_UNKNOWN`), both runtimes absent (`RUNTIME_ABSENT_UNCONFIRMED`), stale PID, wrong mode/owner, and current candidate authority differing from the bound ID (`calendar_update_pending`).
Capture `WorkerSpawnSpec` in the fake `spawn_worker` and assert `start_new_session is True`, no `--account-id` token exists, and `pass_fds` contains only the parent read fd. Separately test the production spawn adapter with a fake `subprocess.Popen` and assert every spec field maps to the exact Popen keyword. Simulate supervisor death by closing the pipe write end; the Worker fixture must enter cooperative drain rather than receiving a terminal `SIGHUP`.

Parameterize startup failure injection at `run_compose`, `select_calendar`, `os.pipe`, `spawn_worker`, Worker descriptor write, and heartbeat timeout. For every case assert the exact fixed status/error code, lock ownership, owned fd closure, child drain request, Compose preservation or cleanup, and absence of automatic restart according to the table above.

Inject the shutdown latch at three startup boundaries: before Compose, after Compose/before spawn, and after spawn/before heartbeat confirmation. Assert no later resource is acquired, any spawned Worker receives only cooperative shutdown, exact Compose dependencies remain until Worker exit is confirmed, and the controller lock is held throughout.

Add a direct wrapper test using a fake controller in a test thread and an injected fake signal registrar. The sink is a closure that appends each status and sets a `threading.Event`; it proves the startup status was published and `run_forever` was entered. Invoke the registrar's captured callback, then assert latch propagation, `request_shutdown`, registration removal, and `close` each occur once. A second defensive fake makes `run_forever` return `unconfirmed`; assert wrapper exit code 1, no `stopped` emission, `close` not called, and signal registration still removed.

- [ ] **Step 3: Run local-stack/runtime unit tests and verify they fail**

Run:

```bash
uv run --project engine pytest \
  engine/tests/unit/ops/test_local_stack.py \
  engine/tests/unit/ops/test_agent_paper_runtime.py \
  -q --import-mode=importlib
```

Expected: the new action enum, supervisor functions, and status model do not exist.

- [ ] **Step 4: Split Compose-only actions from complete backend actions**

Extend the enum without changing the exact existing command allow-list:

```python
class LocalStackAction(StrEnum):
    DOCTOR = "doctor"
    INFRA_UP = "infra-up"
    UP = "up"
    SERVE = "serve"
    STATUS = "status"
    DOWN = "down"
```

`build_local_stack_commands` accepts only Compose-only `INFRA_UP`, `STATUS`, and infrastructure `DOWN`; `UP` maps to the `INFRA_UP` sequence with a safe deprecation result. `SERVE` is handled by `serve_local_backend`, not represented as an invented shell command.

- [ ] **Step 5: Implement controller preparation and the foreground supervisor**

`prepare_local_backend` performs these operations in order: validate paths, acquire `serve.lock`, write `starting`, run `INFRA_UP`, evaluate the feature flag, run zero-external-request preflight, select the calendar, create a pipe, spawn the Worker, wait for a fresh compatible heartbeat, write `running`, and return a controller that still owns the lock and pipe. `serve_local_backend` immediately enters `controller.run_forever()`; it does not return the startup status as a completed service.

```python
spawn_spec = WorkerSpawnSpec(
    argv=(
        str(engine_entrypoint),
        "--calendar-authority-id",
        str(calendar.calendar_authority_id),
        "--parent-fd",
        str(read_fd),
    ),
    cwd=repo_root,
    environment={**environment, "AGENT_RUNTIME_MODE": "codex_sdk"},
    pass_fds=(read_fd,),
    start_new_session=True,
)
worker = dependencies.spawn_worker(spawn_spec)


def spawn_worker(spec: WorkerSpawnSpec) -> WorkerProcess:
    return subprocess.Popen(
        spec.argv,
        cwd=spec.cwd,
        env=dict(spec.environment),
        pass_fds=spec.pass_fds,
        start_new_session=spec.start_new_session,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
```

The Worker loads its account ID only from the already protected local environment; never place it in argv, descriptors, or status. `start_new_session=True` prevents terminal `SIGHUP` from bypassing Worker drain; the parent pipe EOF remains the supervisor-death signal. Keep the write end open only while the supervisor is alive. A disabled feature produces `ready_agent_disabled` without preflight/Worker. A blocked preflight produces `degraded_preflight_blocked` and keeps supervising Compose until a valid `down`/signal.

- [ ] **Step 6: Implement safe status aggregation**

Add a forbidden-extra result model with safe codes only:

```python
class LocalBackendStatus(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    code: Literal[
        "ready",
        "ready_agent_disabled",
        "degraded_preflight_blocked",
        "degraded_startup_failed",
        "degraded_worker_stopped",
        "calendar_update_pending",
        "stopped",
        "unconfirmed",
    ]
    supervisor: Literal["running", "stopped", "unconfirmed"]
    agent_paper: Literal["running", "disabled", "blocked", "stopped", "unconfirmed"]
    containers: Literal["running", "partial", "stopped", "unconfirmed"]
    heartbeat: Literal["fresh", "stale", "missing", "incompatible", "not_applicable"]
    heartbeat_seen_at: datetime | None = None
    bound_calendar_authority_id: UUID | None = None
    candidate_calendar_authority_id: UUID | None = None
    latest_cycle_status: Literal[
        "prepared",
        "researching",
        "decision_recorded",
        "risk_evaluated",
        "awaiting_market",
        "completed",
        "unavailable",
        "failed",
    ] | None = None
    latest_cycle_slot_at: datetime | None = None
    latest_cycle_result_code: str | None = Field(default=None, pattern=r"^[A-Z0-9_]{1,100}$")
    startup_error_code: str | None = Field(default=None, pattern=r"^[A-Z0-9_]{1,100}$")
    exit_reason: RuntimeExitReason | None = None
    execution_locked: Literal[True] = True
```

Status reads only Compose state, validated descriptors/locks/process identities, the existing Worker heartbeat, read-only preflight/selector facts, and persisted last-cycle facts. It exposes no cycle, Job, order, account, or instrument ID. `latest_cycle_result_code` must come from the existing fixed safe-code mapping; unknown text maps to `AGENT_PAPER_RESULT_UNKNOWN`. Status never starts a service, refreshes a token, requests market data, or calls a model. Container `Up` alone cannot yield `ready`.

- [ ] **Step 7: Implement two-phase `down`**

`down_local_backend` validates the supervisor identity before sending one `SIGTERM`. The supervisor requests Worker shutdown and waits for the Worker lock/descriptor to prove exit. Only then does it run the existing API/worker stop and Compose down sequence. If the Worker is unknown or the stop deadline expires, return `AGENT_PAPER_SERVICE_STOP_UNCONFIRMED`, keep dependencies online, and send no second signal.

Use injected wait functions in tests; production waits in chunks no longer than 0.2 seconds with a total 10-second control-plane confirmation deadline. This deadline only observes cooperative completion and never cancels the active business task, whose own bounded timeout remains authoritative.

- [ ] **Step 8: Run lifecycle and legacy fallback tests**

Run:

```bash
uv run --project engine pytest \
  engine/tests/unit/ops/test_local_stack.py \
  engine/tests/unit/ops/test_agent_paper_runtime.py \
  engine/tests/unit/ops/test_agent_paper_host_service.py \
  -q --import-mode=importlib
uv run --project engine ruff check \
  engine/src/nexus/ops/local_stack.py \
  engine/src/nexus/ops/agent_paper_runtime.py \
  engine/tests/unit/ops/test_local_stack.py \
  engine/tests/unit/ops/test_agent_paper_runtime.py
uv run --project engine mypy engine/src
```

Expected: all commands exit 0; legacy launchd tests still pass, but no new code path invokes that script.

- [ ] **Step 9: Commit the unified lifecycle**

```bash
git add engine/src/nexus/ops/local_stack.py \
  engine/src/nexus/ops/agent_paper_runtime.py \
  engine/tests/unit/ops/test_local_stack.py \
  engine/tests/unit/ops/test_agent_paper_runtime.py
git commit -m "feat(paper): supervise worker with local backend"
```

---

### Task 5: Update operator flow and prove the complete slice

**Files:**
- Modify: `docs/runbooks/agent-paper-cycle.md`
- Verify: `scripts/macos/agent-paper-loop-service.py`
- Verify: `engine/tests/unit/ops/test_agent_paper_host_service.py`
- Verify: `engine/tests/integration/test_workbench_read_service.py`

**Interfaces:**
- Consumes: Tasks 1–4 complete lifecycle.
- Produces: one documented normal start/status/stop flow, one disabled launchd fallback, and evidence for implementation review.

- [ ] **Step 1: Replace the normal launchd instructions in the runbook**

Document only these normal commands:

```bash
uv run --project engine nexus-local-stack serve \
  --repo-root "$PWD" \
  --env-file infra/.env.local \
  --project-name nexus-fosun-live

uv run --project engine nexus-local-stack status \
  --repo-root "$PWD" \
  --env-file infra/.env.local \
  --project-name nexus-fosun-live

uv run --project engine nexus-local-stack down \
  --repo-root "$PWD" \
  --env-file infra/.env.local \
  --project-name nexus-fosun-live
```

Explain that `serve` stays foreground, closing it means the complete backend is no longer ready, `infra-up` is maintenance-only, page refresh remains read-only, and disabled/blocked/unknown states do not create cycles. Mark `scripts/macos/agent-paper-loop-service.py` as disabled rollback code that must not be loaded or started.

- [ ] **Step 2: Run the focused regression suite**

Run:

```bash
uv run --project engine pytest \
  engine/tests/unit/ops/test_agent_paper_runtime.py \
  engine/tests/unit/ops/test_local_stack.py \
  engine/tests/unit/ops/test_agent_paper_loop.py \
  engine/tests/unit/ops/test_agent_paper_once.py \
  engine/tests/unit/ops/test_agent_paper_host_service.py \
  engine/tests/integration/test_agent_paper_cycle_pg.py \
  engine/tests/integration/test_agent_paper_loop_heartbeat_migration.py \
  engine/tests/integration/test_workbench_read_service.py \
  -q --import-mode=importlib
```

Expected: all selected tests pass; no test uses a real model, provider, account, or Docker project.

- [ ] **Step 3: Run static and repository checks**

Run:

```bash
uv run --project engine ruff check engine
uv run --project engine mypy engine/src
git diff --check
git status --short
```

Expected: Ruff, Mypy, and diff check exit 0. Status shows only the planned files plus the user's pre-existing uncommitted files; no credential, runtime descriptor, lock, log, or `.secrets` file is tracked.

- [ ] **Step 4: Commit runbook changes**

```bash
git add docs/runbooks/agent-paper-cycle.md
git commit -m "docs(paper): make serve the normal lifecycle"
```

- [ ] **Step 5: Stop at the service-change authorization gate**

After offline tests, static checks, commits, and code review pass, stop execution and report the exact local effects of the remaining acceptance:

```text
Gate A: temporarily read/change the protected feature flag and start/stop the existing local
        Compose project plus the new supervisor; no model, Provider, market, or paper-cycle call.
Gate B: start the real Worker for two eligible slots; this may call Codex SDK, FRED/Alpaca/Schwab
        under existing access and persist local cycle, Job, Output, simulated order/fill, ledger,
        performance, and heartbeat facts.
Rollback: restore the exact original feature-flag value, cooperatively stop only the processes
          started by this acceptance, and retain every business/audit fact.
```

Obtain explicit user confirmation for Gate A before Step 6. Obtain separate explicit confirmation for Gate B before Step 7. Approval to implement code does not authorize either runtime gate.

- [ ] **Step 6: After Gate A approval, run a zero-external-request local lifecycle acceptance**

Read and retain the exact original feature-flag text in protected local memory. Temporarily set the flag to false in the protected local config, run `serve` in one terminal and `status` from another. Confirm:

```text
complete_backend=ready_agent_disabled
agent_paper=disabled
execution_locked=true
```

Run a second `serve` and require `LOCAL_BACKEND_ALREADY_RUNNING`. Run `down` and verify Worker was never spawned, the supervisor is confirmed stopped, then Compose dependencies stop in order. Repeat with a deliberately blocked non-secret fixture configuration and require `degraded_preflight_blocked` with zero Worker heartbeat/cycle/Job changes. Restore the original feature-flag text in a `finally` path even when acceptance fails; verify the file remains ignored, mode-protected, and untracked.

- [ ] **Step 7: After Gate B approval, run one bounded real Worker acceptance when prerequisites are ready**

Read and retain the exact original feature-flag text, then inspect the current persisted `cycle → job → outputs → order/window` chain and record only IDs in protected local notes, not Git or chat. Temporarily enable the feature, start `serve`, then confirm:

1. One compatible Agent Paper heartbeat appears within 30 seconds.
2. A second `serve` cannot acquire either lifecycle path.
3. Outside the decision window, two 15-second observations add heartbeats but no cycle, Job, model, provider, order, or performance call.
4. Across two eligible five-minute slots, each slot creates at most one cycle and one exact Agent Job; no-trade is acceptable.
5. `status` reports the Worker-bound authority separately from the current candidate authority.
6. `down` during an idle wait yields `STOP_REQUESTED` and stops dependencies only after Worker exit.

If any command/connection result is unknown, stop acceptance and inspect the original persisted facts. Do not restart, change the idempotency key, loosen freshness, switch provider, or enable launchd. In all outcomes, restore the exact original feature-flag text and cooperatively stop only confirmed processes started by the acceptance; never delete the new business facts.

- [ ] **Step 8: Perform one final review against the design**

Review the final diff for forward-looking time leaks, authority switching, task cancellation, duplicate Worker/cycle creation, PID reuse, unsafe files, sensitive output, dependency shutdown before confirmed Worker exit, and accidental launchd invocation. Require no P1/P2 finding before considering the implementation slice complete.

- [ ] **Step 9: Record rollback proof without deleting facts**

Using the original feature-flag value restored by Steps 6–7, run `status`, then `down` only for processes confirmed to belong to this acceptance. Confirm cycle, Job, Output, order, fill, ledger, performance, and heartbeat history remain unchanged except for expected heartbeat aging. Do not force the flag to false and do not downgrade the database because this slice has no migration.

After the new lifecycle has passed the real acceptance above, create a separate cleanup plan/commit to remove `scripts/macos/agent-paper-loop-service.py`, its unit test, and obsolete launchd runbook text. That cleanup is not part of this implementation plan.

---

## Completion Gate

The implementation is complete only when all of the following are true:

- `serve` is the sole documented complete backend entry; `infra-up` cannot be mistaken for complete readiness.
- Feature-disabled and preflight-blocked modes retain a singleton supervisor without starting Agent Paper.
- The Worker acquires its lock before heartbeat, inspection, or any business action.
- The Worker uses one calendar authority across waiting, inspection, preparation, and trigger paths.
- Night, weekend, holiday, half-day, and DST cases are deterministic and do not depend on wall-clock sleeps.
- A shutdown request never cancels active work and performs exactly one terminal readback.
- Unknown runtime or business state prevents duplicate start and dependency shutdown.
- Status separates Compose, supervisor, Worker heartbeat, calendar binding, latest cycle, readiness, and execution lock.
- No database migration, new service endpoint, dependency, Shadow integration, broker write, or permission change was introduced.
- Focused tests, Ruff, Mypy, diff check, zero-external lifecycle acceptance, and one bounded real Worker acceptance all pass.
