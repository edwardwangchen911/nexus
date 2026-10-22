"""Unconfirmed SDK cleanup cannot assert a locally terminated host."""

import asyncio
from types import SimpleNamespace

import pytest
from nexus.agents import codex_sdk
from nexus.paper.manual_host_exit import host_drain_scope


@pytest.mark.asyncio
async def test_cancel_during_sdk_close_prevents_host_exit():
    closing = asyncio.Event()
    released = asyncio.Event()

    async def close():
        closing.set()
        await released.wait()

    with host_drain_scope() as state:
        task = asyncio.create_task(codex_sdk._close_sdk_client(SimpleNamespace(close=close)))
        await closing.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        released.set()
        assert not state.confirmed


@pytest.mark.asyncio
@pytest.mark.parametrize("business_failure", [False, True])
async def test_exit_write_failure_preserves_primary_business_outcome(monkeypatch, business_failure):
    from contextlib import asynccontextmanager, contextmanager
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from nexus.ops import manual_paper_host
    from nexus.settings import EngineSettings

    events = []
    primary = ValueError("SYNTHETIC_BUSINESS_FAILURE")
    view = SimpleNamespace(request_id=uuid4(), status="completed")

    @contextmanager
    def lock():
        events.append("locked")
        try:
            yield
        finally:
            events.append("unlocked")

    class Factory:
        kw = {"bind": SimpleNamespace(dispose=AsyncMock())}

        @asynccontextmanager
        async def begin(self):
            yield object()

    async def dispatch(*_, **__):
        events.append("business")
        if business_failure:
            raise primary

    async def failed_exit(*_, **__):
        assert events == ["locked", "business"]
        events.append("exit_failed")
        raise RuntimeError("synthetic exit failure")

    monkeypatch.setattr(manual_paper_host, "session_factory", lambda *_: Factory())
    monkeypatch.setattr(manual_paper_host, "dispatch_pure_research_request", dispatch)
    monkeypatch.setattr("nexus.ops.agent_paper_loop.acquire_agent_paper_lock", lock)
    monkeypatch.setattr("nexus.ops.agent_paper_loop.heartbeat_agent_paper_loop", AsyncMock())
    monkeypatch.setattr(
        "nexus.ops.agent_paper_loop.agent_paper_host_identity",
        lambda *_, **__: ("synthetic", "test"),
    )
    monkeypatch.setattr(
        "nexus.paper.manual_entry.start_configured_manual_request", AsyncMock(return_value=view)
    )
    monkeypatch.setattr(
        "nexus.paper.manual_entry.read_configured_manual_request", AsyncMock(return_value=view)
    )
    monkeypatch.setattr("nexus.paper.manual_host_exit.record_host_exit", failed_exit)
    with pytest.raises(ValueError) as caught:
        await manual_paper_host.run_pure_research_once(EngineSettings(), idempotency_key="one")
    if business_failure:
        assert caught.value is primary
        assert primary.__notes__ == ["MANUAL_HOST_EXIT_UNCONFIRMED"]
    else:
        assert str(caught.value) == "MANUAL_HOST_EXIT_UNCONFIRMED"
        assert view.status == "completed"
    assert events == ["locked", "business", "exit_failed", "unlocked"]


@pytest.mark.asyncio
@pytest.mark.parametrize("business_failure", [False, True])
async def test_cancel_during_keepalive_drain_preserves_outcome(monkeypatch, business_failure):
    from contextlib import asynccontextmanager, contextmanager
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from nexus.ops import manual_paper_host
    from nexus.settings import EngineSettings

    entered = asyncio.Event()
    cleaning = asyncio.Event()
    released = asyncio.Event()
    primary = ValueError("SYNTHETIC_BUSINESS_FAILURE")
    view = SimpleNamespace(request_id=uuid4(), status="completed")
    events = []

    async def keepalive_wait(_):
        entered.set()
        try:
            await asyncio.Event().wait()
        finally:
            cleaning.set()
            await released.wait()

    @contextmanager
    def lock():
        events.append("locked")
        try:
            yield
        finally:
            events.append("unlocked")

    class Factory:
        kw = {"bind": SimpleNamespace(dispose=AsyncMock())}

        @asynccontextmanager
        async def begin(self):
            yield object()

    async def dispatch(*_, **__):
        await entered.wait()
        if business_failure:
            raise primary

    exit_writer = AsyncMock()
    monkeypatch.setattr(asyncio, "sleep", keepalive_wait)
    monkeypatch.setattr(manual_paper_host, "session_factory", lambda *_: Factory())
    monkeypatch.setattr(manual_paper_host, "dispatch_pure_research_request", dispatch)
    monkeypatch.setattr("nexus.ops.agent_paper_loop.acquire_agent_paper_lock", lock)
    monkeypatch.setattr("nexus.ops.agent_paper_loop.heartbeat_agent_paper_loop", AsyncMock())
    monkeypatch.setattr(
        "nexus.paper.manual_entry.start_configured_manual_request", AsyncMock(return_value=view)
    )
    monkeypatch.setattr(
        "nexus.paper.manual_entry.read_configured_manual_request", AsyncMock(return_value=view)
    )
    monkeypatch.setattr("nexus.paper.manual_host_exit.record_host_exit", exit_writer)
    host = asyncio.create_task(
        manual_paper_host.run_pure_research_once(EngineSettings(), idempotency_key="one")
    )
    await cleaning.wait()
    host.cancel()
    expected = ValueError if business_failure else asyncio.CancelledError
    with pytest.raises(expected) as caught:
        await host
    if business_failure:
        assert caught.value is primary
    assert caught.value.__notes__ == ["MANUAL_HOST_EXIT_UNCONFIRMED"]
    assert view.status == "completed"
    exit_writer.assert_not_awaited()
    assert events == ["locked", "unlocked"]


@pytest.mark.asyncio
async def test_sdk_close_cancelled_task_cannot_confirm_drain():
    async def close():
        raise asyncio.CancelledError

    with host_drain_scope() as state:
        with pytest.raises(asyncio.CancelledError):
            await codex_sdk._close_sdk_client(SimpleNamespace(close=close))
        assert not state.confirmed


@pytest.mark.asyncio
async def test_sdk_initialization_without_process_evidence_cannot_confirm_exit(monkeypatch):
    from engine.tests.unit.agents.test_codex_sdk import _sdk

    client, _, _, _ = _sdk(monkeypatch)
    client._client._sync = SimpleNamespace(_proc=None)
    client.account.side_effect = RuntimeError("synthetic initialization failure")
    with host_drain_scope() as state:
        with pytest.raises(codex_sdk.CodexInvocationError):
            await codex_sdk.invoke_codex({})
        assert not state.confirmed
