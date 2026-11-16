"""Legacy storage keeps the permanent once refusal without changing its state meaning."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.monitoring import storage


@pytest.mark.asyncio
async def test_legacy_once_refusal_persists_the_exact_reason(monkeypatch):
    row = SimpleNamespace(
        id=UUID(int=1),
        account_id=UUID(int=2),
        instrument_id=UUID(int=3),
        kind="price",
        status="pending",
        reason=None,
        research_run_id=None,
    )
    session = SimpleNamespace(
        get=AsyncMock(return_value=row), refresh=AsyncMock(), flush=AsyncMock()
    )
    monkeypatch.setattr(storage, "lock_security", AsyncMock())
    monkeypatch.setattr("nexus.paper.target_service.lock_portfolio_account", AsyncMock())
    audit = AsyncMock()
    monkeypatch.setattr("nexus.audit.service.append_audit_event", audit)
    reason = "SINGLE_MONITOR_INCREMENT_ONCE_CONSUMED"
    result = await storage.mark_trigger_blocked(
        session, trigger_id=row.id, reason=reason, now=datetime(2026, 11, 16, tzinfo=UTC)
    )
    assert result is row and row.status == "blocked" and row.reason == reason
    assert row.research_run_id is None
    assert audit.await_args.kwargs["payload"]["reason"] == reason


@pytest.mark.asyncio
async def test_arbitrary_block_reason_is_not_persisted():
    session = SimpleNamespace(get=AsyncMock())
    with pytest.raises(ValueError, match="MONITOR_BLOCK_REASON_INVALID"):
        await storage.mark_trigger_blocked(
            session,
            trigger_id=UUID(int=1),
            reason="untrusted detail",
            now=datetime(2026, 11, 16, tzinfo=UTC),
        )
    session.get.assert_not_awaited()
