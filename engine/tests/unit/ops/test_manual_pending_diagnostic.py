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
