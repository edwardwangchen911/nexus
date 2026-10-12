from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.paper import manual_cancel


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["missing", "unlocked", "manual-request", "bad-hash"])
async def test_terminal_cancel_rejects_invalid_authority_before_job_claim(monkeypatch, boundary):
    now = datetime(2026, 10, 12, 14, tzinfo=UTC)
    cycle = SimpleNamespace(execution_locked=boundary != "unlocked")
    event = SimpleNamespace(
        sequence=1,
        from_status=None,
        to_status="unavailable",
        result_code="TEST_FAILURE",
        occurred_at=now,
        fact_ids={},
        event_hash="0" * 64,
    )
    session = SimpleNamespace(
        scalar=AsyncMock(
            side_effect=[
                None if boundary == "missing" else cycle,
                UUID(int=3) if boundary == "manual-request" else None,
            ]
        ),
        scalars=AsyncMock(return_value=[event]),
    )
    monkeypatch.setattr(manual_cancel, "lock_portfolio_account", AsyncMock())
    cancel = AsyncMock()
    monkeypatch.setattr(manual_cancel, "cancel_unused_manual_research", cancel)
    with pytest.raises(ValueError, match="MANUAL_RESULT_UNKNOWN"):
        await manual_cancel.close_terminal_cycle_unused_research(
            session, account_id=UUID(int=1), cycle_id=UUID(int=2), now=now
        )
    cancel.assert_not_awaited()
