from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.monitoring import durable_sec_permission as permission


@pytest.mark.asyncio
async def test_send_fence_releases_on_real_headers_before_response_verification(monkeypatch):
    session = SimpleNamespace(begin=AsyncMock(), rollback=AsyncMock(), close=AsyncMock())
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
async def test_refused_permission_cleans_up_before_any_headers(monkeypatch):
    session = SimpleNamespace(begin=AsyncMock(), rollback=AsyncMock(), close=AsyncMock())
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
async def test_no_durable_batch_never_holds_request_transaction(monkeypatch):
    session = SimpleNamespace(begin=AsyncMock(), rollback=AsyncMock(), close=AsyncMock())
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
    session = SimpleNamespace(execute=AsyncMock())
    assert await permission.require_new_send(session, UUID(int=1)) is True
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_headers_start_rechecks_permission_after_pool_or_tls_wait(monkeypatch):
    session = SimpleNamespace(begin=AsyncMock(), rollback=AsyncMock(), close=AsyncMock())
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
async def test_expired_work_cannot_start_headers(monkeypatch):
    from asyncio import get_running_loop

    session = SimpleNamespace(begin=AsyncMock(), rollback=AsyncMock(), close=AsyncMock())
    monkeypatch.setattr(permission, "require_new_send", AsyncMock(return_value=True))
    fence = permission.SecRequestFence(lambda: session, UUID(int=1))
    request = SimpleNamespace(extensions={})
    await fence.acquire(request)
    fence.work_deadline = get_running_loop().time() - 1
    with pytest.raises(TimeoutError):
        await request.extensions["trace"]("http2.send_request_headers.started", {})
    await fence.release()


@pytest.mark.asyncio
async def test_transport_cannot_start_second_headers_after_fence_release(monkeypatch):
    session = SimpleNamespace(begin=AsyncMock(), rollback=AsyncMock(), close=AsyncMock())
    monkeypatch.setattr(permission, "require_new_send", AsyncMock(return_value=True))
    fence = permission.SecRequestFence(lambda: session, UUID(int=1))
    request = SimpleNamespace(extensions={})
    await fence.acquire(request)
    await request.extensions["trace"]("http11.send_request_headers.complete", {})
    with pytest.raises(ValueError, match="PERMISSION_INVALID"):
        await request.extensions["trace"]("http11.send_request_headers.started", {})
