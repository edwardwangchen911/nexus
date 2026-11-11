"""Pending-read cancellation identifies its phase without leaking payloads."""

import asyncio
import json
from contextlib import asynccontextmanager, nullcontext
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from nexus.ops import agent_paper_loop as loop
from nexus.ops import manual_paper_host as host
from nexus.paper import formal_intraday_intent
from nexus.settings import EngineSettings


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase",
    [
        "session_open",
        "read_setup",
        "manual_requests",
        "intraday_intents",
        "session_close",
        "engine_dispose",
    ],
)
async def test_pending_cancellation_retains_phase_through_timeout(monkeypatch, capsys, phase):
    entered = asyncio.Event()

    async def pause(at):
        if phase == at:
            entered.set()
            await asyncio.Event().wait()

    async def execute(*args):
        await pause("read_setup")

    session = SimpleNamespace(execute=execute, info={})

    @asynccontextmanager
    async def begin():
        await pause("session_open")
        yield session
        await pause("session_close")

    async def dispose():
        await pause("engine_dispose")

    factory = SimpleNamespace(begin=begin, kw={"bind": SimpleNamespace(dispose=dispose)})
    monkeypatch.setattr(host, "session_factory", lambda _: factory)

    monkeypatch.setattr(host, "artifact_read_scope", lambda _: nullcontext())
    monkeypatch.setattr(host, "evidence_read_scope", lambda _: nullcontext())

    async def peek(*args, **kwargs):
        await pause("manual_requests")

    async def due(*args, **kwargs):
        await pause("intraday_intents")

    monkeypatch.setattr(host, "peek_manual_request", peek)
    monkeypatch.setattr(formal_intraday_intent, "due_intent", due)
    account = uuid4()
    settings = EngineSettings.for_test().model_copy(
        update={"agent_paper_manual_enabled": True, "agent_paper_account_id": account}
    )

    async def probe():
        async with asyncio.timeout(None) as deadline:

            async def expire():
                await entered.wait()
                deadline.reschedule(asyncio.get_running_loop().time())

            timer = asyncio.create_task(expire())
            try:
                await host.manual_work_pending(settings, account_id=account)
            finally:
                timer.cancel()
                await asyncio.gather(timer, return_exceptions=True)

    with pytest.raises(TimeoutError) as raised:
        await probe()
    loop._emit_agent_paper_unknown_failure(
        "inspect", datetime.now(UTC), error=raised.value, inspection_site="pending_work"
    )
    payload = json.loads(capsys.readouterr().err)
    assert payload["pending_work_phase"] == phase
    assert payload["failure_kind"] == "timeout"


@pytest.mark.parametrize("phase", ["private-canary", ["private-canary"], None])
def test_pending_diagnostic_rejects_non_allowlisted_phase(capsys, phase):
    error = TimeoutError("private-canary")
    error._nexus_pending_work_phase = phase
    error.__context__ = error
    loop._emit_agent_paper_unknown_failure(
        "inspect", datetime.now(UTC), error=error, inspection_site="pending_work"
    )
    captured = capsys.readouterr().err
    assert "private-canary" not in captured
    assert "pending_work_phase" not in json.loads(captured)


@pytest.mark.asyncio
async def test_pending_cleanup_failure_records_cleanup_not_original_read(monkeypatch):
    original = ValueError("private-original")
    cleanup = RuntimeError("private-cleanup")

    @asynccontextmanager
    async def begin():
        try:
            yield SimpleNamespace(execute=AsyncMock(), info={})
        finally:
            raise cleanup

    factory = SimpleNamespace(begin=begin, kw={"bind": SimpleNamespace(dispose=AsyncMock())})
    monkeypatch.setattr(host, "session_factory", lambda _: factory)
    monkeypatch.setattr(host, "artifact_read_scope", lambda _: nullcontext())
    monkeypatch.setattr(host, "evidence_read_scope", lambda _: nullcontext())
    monkeypatch.setattr(host, "peek_manual_request", AsyncMock(side_effect=original))
    account = uuid4()
    settings = EngineSettings.for_test().model_copy(
        update={"agent_paper_manual_enabled": True, "agent_paper_account_id": account}
    )
    with pytest.raises(RuntimeError) as raised:
        await host.manual_work_pending(settings, account_id=account)
    assert raised.value is cleanup
    assert cleanup._nexus_pending_work_phase == "session_close"
    assert original._nexus_pending_work_phase == "manual_requests"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["own", "inner", "close", "dispose", "external", "object_reader"]
)
async def test_only_ended_owned_read_is_a_safe_deadline(monkeypatch, failure):
    from nexus.ops.pending_probe import ReadProbeDeadline, bounded_probe

    deadline = asyncio.timeout(None)
    import threading

    entered = asyncio.Event()
    release = threading.Event()
    drained = asyncio.Event()
    event_loop = asyncio.get_running_loop()

    @asynccontextmanager
    async def begin():
        try:
            yield SimpleNamespace(execute=AsyncMock(), info={})
        finally:
            if failure == "close":
                raise OSError("close")

    async def dispose():
        if failure == "dispose":
            raise OSError("dispose")

    async def peek(*args, **kwargs):
        if failure == "object_reader":
            from nexus.market.object_store import _run_object_io

            def block():
                event_loop.call_soon_threadsafe(entered.set)
                try:
                    release.wait(10)
                finally:
                    event_loop.call_soon_threadsafe(drained.set)

            return await _run_object_io(block)
        entered.set()
        if failure == "inner":
            raise TimeoutError("driver")
        await asyncio.Event().wait()

    monkeypatch.setattr(host, "peek_manual_request", peek)
    monkeypatch.setattr(
        host,
        "session_factory",
        lambda _: SimpleNamespace(begin=begin, kw={"bind": SimpleNamespace(dispose=dispose)}),
    )
    monkeypatch.setattr(host, "artifact_read_scope", lambda _: nullcontext())
    monkeypatch.setattr(host, "evidence_read_scope", lambda _: nullcontext())
    account = uuid4()
    settings = EngineSettings.for_test().model_copy(
        update={"agent_paper_manual_enabled": True, "agent_paper_account_id": account}
    )
    task = asyncio.create_task(
        bounded_probe(lambda: host.manual_work_pending(settings, account_id=account), deadline)
    )
    await entered.wait()
    if failure != "inner":
        deadline.reschedule(asyncio.get_running_loop().time())
    if failure == "external":
        task.cancel()
    if failure == "own":
        assert isinstance(await task, ReadProbeDeadline)
    else:
        with pytest.raises(
            {
                "inner": TimeoutError,
                "object_reader": TimeoutError,
                "external": asyncio.CancelledError,
                "close": OSError,
                "dispose": OSError,
            }[failure]
        ):
            await task

    if failure == "object_reader":
        release.set()
        await asyncio.wait_for(drained.wait(), 3)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "finish", ["queued", "unknown", "due_failed", "publish_failed", "monitor_unknown"]
)
async def test_recovery_reinspects_new_day_before_one_dispatch(monkeypatch, finish):
    from datetime import timedelta

    from unit.ops.test_agent_paper_loop import snapshot

    account, request = uuid4(), uuid4()
    settings = EngineSettings.for_test().model_copy(
        update={"agent_paper_manual_enabled": True, "agent_paper_account_id": account}
    )
    base = datetime(2026, 10, 12, 15, tzinfo=UTC)
    logs, heartbeats, actions = [], [], []
    elapsed = 0
    clock_shift = timedelta()
    cycles = 0
    attempts = 0
    busy = True
    timer = None
    real_bounded = loop.bounded_probe

    @asynccontextmanager
    async def begin():
        try:
            yield SimpleNamespace(execute=AsyncMock(), info={})
        finally:
            logs.append("closed")

    monkeypatch.setattr(
        host,
        "session_factory",
        lambda _: SimpleNamespace(begin=begin, kw={"bind": SimpleNamespace(dispose=AsyncMock())}),
    )
    monkeypatch.setattr(host, "artifact_read_scope", lambda _: nullcontext())
    monkeypatch.setattr(host, "evidence_read_scope", lambda _: nullcontext())

    async def owned_probe(read, owned):
        nonlocal timer
        timer = owned
        return await real_bounded(read, owned)

    monkeypatch.setattr(loop, "bounded_probe", owned_probe)

    async def peek(*args, **kwargs):
        nonlocal attempts, elapsed, clock_shift
        attempts += 1
        logs.append("peek")
        if attempts <= 2:
            elapsed += 15
            timer.reschedule(asyncio.get_running_loop().time())
            await asyncio.Event().wait()
        clock_shift = timedelta(days=1)
        return object()

    monkeypatch.setattr(host, "peek_manual_request", peek)

    async def due(*args):
        logs.append("due")
        if finish == "due_failed":
            raise ValueError("MANUAL_RESULT_UNKNOWN")

    monkeypatch.setattr(formal_intraday_intent, "due_intent", due)

    async def inspect():
        nonlocal cycles
        logs.append("inspect")
        cycles += 1
        now = base + timedelta(seconds=elapsed) + clock_shift
        if cycles == 1:
            return snapshot(now=now, cycle_status="completed", cycle_slot_at=base.replace(hour=14))
        return snapshot(
            now=now,
            manual_research_only=True,
            manual_pending_request_id=request,
            manual_pending_status="unknown" if finish == "unknown" else "queued",
        )

    async def sleep(seconds):
        nonlocal elapsed
        elapsed += seconds

    async def heartbeat(_):
        heartbeats.append(elapsed)

    async def record(notice):
        logs.append("pause" if notice else "ready")
        if notice and finish == "publish_failed":
            raise OSError("commit unconfirmed")

    async def execute(action, state):
        actions.append((action, state.manual_pending_request_id, state.now.date()))
        return loop.AgentPaperLoopAction.STOP_REQUESTED

    def monitor_busy():
        nonlocal busy
        if busy:
            logs.append("busy")
            busy = False
            return True
        return False

    result = await loop.run_foreground_loop(
        inspect=inspect,
        execute=execute,
        heartbeat=heartbeat,
        sleep=sleep,
        wake_for_pending_work=lambda: host.manual_work_pending(settings, account_id=account),
        kick_monitor=lambda _: logs.append("kick"),
        monitor_busy=monitor_busy,
        monitor_confirmed=lambda: finish != "monitor_unknown",
        record_probe=record,
        clock=lambda: base + timedelta(seconds=elapsed) + clock_shift,
        monotonic=lambda: elapsed,
    )
    assert logs.index("busy") < logs.index("peek")
    assert "kick" not in logs[logs.index("busy") + 1 : logs.index("peek")]
    assert max(b - a for a, b in zip(heartbeats, heartbeats[1:], strict=False)) <= 15
    if finish == "queued":
        assert logs.count("pause") == 2 and logs.index("ready") > logs.index("due")
        assert logs[logs.index("ready") - 1] == "inspect"
        assert actions == [
            (loop.AgentPaperLoopAction.TRIGGER_MANUAL, request, (base + timedelta(days=1)).date())
        ]
        assert result == loop.AgentPaperLoopAction.STOP_REQUESTED
    else:
        assert not actions and "ready" not in logs
        assert result == loop.AgentPaperLoopAction.STOP_RESULT_UNKNOWN
