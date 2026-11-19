from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.monitoring import durable_sec_permission as permission


@pytest.fixture
def session():
    return SimpleNamespace(
        scalar=AsyncMock(return_value=None),
        begin=AsyncMock(),
        rollback=AsyncMock(),
        close=AsyncMock(),
    )


@pytest.mark.asyncio
async def test_send_fence_releases_on_real_headers_before_response_verification(
    monkeypatch, session
):
    monkeypatch.setattr(permission, "require_new_send", AsyncMock(return_value=True))
    fence = permission.SecRequestFence(lambda: session, UUID(int=1))
    request = SimpleNamespace(extensions={})
    await fence.acquire(request)
    session.rollback.assert_not_awaited()
    await request.extensions["trace"]("http11.send_request_headers.complete", {})
    session.rollback.assert_awaited_once()
    session.close.assert_awaited_once()
    await fence.release()
    session.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_refused_permission_cleans_up_before_any_headers(monkeypatch, session):
    monkeypatch.setattr(
        permission,
        "require_new_send",
        AsyncMock(side_effect=ValueError("SEC_RESEARCH_PERMISSION_INVALID")),
    )
    request = SimpleNamespace(extensions={})
    with pytest.raises(ValueError, match="PERMISSION_INVALID"):
        await permission.SecRequestFence(lambda: session, UUID(int=1)).acquire(request)
    assert request.extensions == {}
    session.rollback.assert_awaited_once()
    session.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_no_durable_batch_never_holds_request_transaction(monkeypatch, session):
    monkeypatch.setattr(permission, "require_new_send", AsyncMock(return_value=False))
    request = SimpleNamespace(extensions={})
    await permission.SecRequestFence(lambda: session, UUID(int=1)).acquire(request)
    assert request.extensions == {}
    session.rollback.assert_awaited_once()


@pytest.mark.asyncio
async def test_legacy_sec_dependency_is_not_skipped_by_new_routing_probe(monkeypatch):
    monkeypatch.setattr(permission, "durable_batch", AsyncMock(return_value=None))
    monkeypatch.setattr(
        permission, "has_sec_dependencies", AsyncMock(return_value=True), raising=False
    )
    monkeypatch.setattr(permission, "verify_send_inputs", AsyncMock(), raising=False)
    session = SimpleNamespace(execute=AsyncMock())
    assert await permission.require_new_send(session, UUID(int=1)) is True
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_current_capture_raw_custody_is_required_after_sql_gate(monkeypatch):
    monkeypatch.setattr(permission, "has_sec_dependencies", AsyncMock(return_value=True))
    raw = AsyncMock(side_effect=ValueError("SEC_RESEARCH_PERMISSION_INVALID"))
    monkeypatch.setattr(permission, "verify_send_inputs", raw, raising=False)
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(ValueError, match="SEC_RESEARCH_PERMISSION_INVALID"):
        await permission.require_new_send(session, UUID(int=1))
    raw.assert_awaited_once_with(session, UUID(int=1))


@pytest.mark.asyncio
async def test_headers_start_rechecks_permission_after_pool_or_tls_wait(monkeypatch, session):
    gate = AsyncMock(side_effect=[True, ValueError("SEC_RESEARCH_PERMISSION_INVALID")])
    monkeypatch.setattr(permission, "require_new_send", gate)
    fence = permission.SecRequestFence(lambda: session, UUID(int=1))
    request = SimpleNamespace(extensions={})
    await fence.acquire(request)
    with pytest.raises(ValueError, match="PERMISSION_INVALID"):
        await request.extensions["trace"]("http11.send_request_headers.started", {})
    await fence.release()
    assert gate.await_count == 2


@pytest.mark.asyncio
async def test_expired_work_cannot_start_headers(monkeypatch, session):
    from asyncio import get_running_loop

    monkeypatch.setattr(permission, "require_new_send", AsyncMock(return_value=True))
    fence = permission.SecRequestFence(lambda: session, UUID(int=1))
    request = SimpleNamespace(extensions={})
    await fence.acquire(request)
    fence.work_deadline = get_running_loop().time() - 1
    with pytest.raises(TimeoutError):
        await request.extensions["trace"]("http2.send_request_headers.started", {})
    await fence.release()


@pytest.mark.asyncio
async def test_transport_cannot_start_second_headers_after_fence_release(monkeypatch, session):
    monkeypatch.setattr(permission, "require_new_send", AsyncMock(return_value=True))
    fence = permission.SecRequestFence(lambda: session, UUID(int=1))
    request = SimpleNamespace(extensions={})
    await fence.acquire(request)
    await request.extensions["trace"]("http11.send_request_headers.complete", {})
    with pytest.raises(ValueError, match="PERMISSION_INVALID"):
        await request.extensions["trace"]("http11.send_request_headers.started", {})


@pytest.mark.asyncio
async def test_prior_trace_callback_cannot_cross_final_work_deadline(monkeypatch, session):
    from asyncio import get_running_loop

    gate = AsyncMock(return_value=True)
    monkeypatch.setattr(permission, "require_new_send", gate)
    fence = permission.SecRequestFence(lambda: session, UUID(int=1))

    async def original(name, info):
        if name.endswith("send_request_headers.started"):
            fence.work_deadline = get_running_loop().time() - 1

    request = SimpleNamespace(extensions={"trace": original})
    await fence.acquire(request)
    with pytest.raises(TimeoutError, match="RESPONSES_WORK_DEADLINE"):
        await request.extensions["trace"]("http11.send_request_headers.started", {})
    assert gate.await_count == 1
    await fence.release()
