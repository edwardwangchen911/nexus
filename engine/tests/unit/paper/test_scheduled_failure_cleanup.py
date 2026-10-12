"""Scheduled recovery never consumes manual or economic research ownership."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.paper import agent_cycle, manual_cancel


@pytest.mark.asyncio
@pytest.mark.parametrize("owner", ["manual_fact", "manual_row", "economic_lane"])
async def test_other_owner_retains_original_failure_without_cancellation(monkeypatch, owner):
    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=[True] if owner == "manual_row" else [None, True])
    )
    cycle = SimpleNamespace(id=UUID(int=1))
    events = (
        SimpleNamespace(
            to_status="researching",
            fact_ids={"manual_request_id": "original"} if owner == "manual_fact" else {},
        ),
    )
    cancel = AsyncMock()
    append = AsyncMock()
    monkeypatch.setattr(manual_cancel, "cancel_unused_manual_research", cancel)
    monkeypatch.setattr(agent_cycle, "_append_event", append)
    monkeypatch.setattr(agent_cycle, "_events", AsyncMock(return_value=events))
    monkeypatch.setattr(agent_cycle, "_result", lambda *_args, **_kwargs: "original-failure")
    result = await agent_cycle._stop_unavailable(
        session, cycle, events, cleanup_at=datetime(2026, 10, 12, tzinfo=UTC)
    )
    assert result == "original-failure"
    cancel.assert_not_awaited()
    append.assert_awaited_once_with(
        session, cycle, "researching", "unavailable", "RESEARCH_OUTPUT_NOT_ACTIONABLE", {}
    )
