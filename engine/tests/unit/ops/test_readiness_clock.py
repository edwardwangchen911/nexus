"""Readiness observation-time contracts for a concurrently heartbeating worker."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from nexus.readiness import probe_general_worker

_HEARTBEAT_TIME = datetime(2026, 8, 27, 12, 0, 0, 500, tzinfo=UTC)


@pytest.mark.asyncio
async def test_worker_clock_is_sampled_after_the_selected_heartbeat() -> None:
    events: list[str] = []

    class _Session:
        async def scalar(self, _statement: object) -> object:
            events.append("heartbeat-selected")
            return SimpleNamespace(seen_at=_HEARTBEAT_TIME)

    def _clock() -> datetime:
        events.append("clock-sampled")
        return _HEARTBEAT_TIME

    status = await probe_general_worker(  # type: ignore[arg-type]
        _Session(),
        clock=_clock,
    )

    assert status == "ready"
    assert events == ["heartbeat-selected", "clock-sampled"]
