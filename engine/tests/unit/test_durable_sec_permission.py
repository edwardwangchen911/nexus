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
