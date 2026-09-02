import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest
from nexus.ops.price_evidence_status import read_price_evidence_status
from sqlalchemy.dialects import postgresql

NOW = datetime(2026, 9, 2, 10, tzinfo=UTC)


async def _read(held, rows, authorities=0):
    result = Mock()
    result.mappings.return_value.all.return_value = rows
    session = SimpleNamespace(
        scalars=AsyncMock(return_value=held),
        execute=AsyncMock(return_value=result),
        scalar=AsyncMock(return_value=authorities),
    )
    summary = await read_price_evidence_status(
        session,
        snapshot_id=uuid4(),
        snapshot_as_of=NOW - timedelta(minutes=5),
        checked_at=NOW,
    )
    return summary, session


@pytest.mark.asyncio
async def test_display_history_does_not_become_research_authority_or_leak_holdings():
    identifier = uuid4()
    summary, session = await _read(
        [identifier, uuid4()],
        [
            {
                "instrument": str(identifier),
                "delivery_mode": "unknown",
                "adjustment": "raw",
                "row_count": 277,
                "visible_at": NOW - timedelta(minutes=1),
                "end_at": NOW - timedelta(days=1),
            }
        ],
    )
    assert summary["held_instruments"] == 2
    assert summary["missing_series"] == 1
    assert summary["delivery_unverified"] == summary["declared_raw_series"] == 1
    assert summary["latest_series_after_account_snapshot"] == 1
    assert summary["minimum_series_rows"] == 277
    assert summary["stored_authority_records"] == 0
    assert summary["authority_revalidated"] is False
    assert summary["adjustment_request_verified"] is False
    assert len(summary["reasons"]) == 5
    assert str(identifier) not in json.dumps(summary)
    statement = str(session.execute.call_args.args[0].compile(dialect=postgresql.dialect()))
    assert "DISTINCT ON (price_series_manifests.instrument)" in statement
    for field in ("created_at", "end_at", "provider_authorized_at"):
        assert f"price_series_manifests.{field} <=" in statement
    for field in ("created_at", "available_at", "received_at"):
        assert f"data_artifacts.{field} <=" in statement


@pytest.mark.asyncio
async def test_stored_authority_count_is_not_reported_as_admission():
    summary, _ = await _read(
        [uuid4()],
        [
            {
                "delivery_mode": "delayed",
                "adjustment": "raw",
                "row_count": 252,
                "visible_at": NOW - timedelta(days=1),
                "end_at": NOW - timedelta(days=2),
            }
        ],
        authorities=3,
    )
    assert summary["stored_authority_records"] == 3
    assert summary["authority_revalidated"] is False
    assert summary["reasons"] == ["RAW_SERIES_NOT_CORPORATE_ACTION_ADJUSTED"]
    assert "ready" not in summary


@pytest.mark.asyncio
async def test_empty_holdings_do_not_invent_a_missing_price_blocker():
    summary, _ = await _read([], [])
    assert summary["reasons"] == []
    assert summary["latest_bar_at"] is None
    assert summary["minimum_series_rows"] == summary["maximum_series_rows"] == 0
