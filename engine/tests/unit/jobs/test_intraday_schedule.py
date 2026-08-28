"""Pure contracts for 15-second intraday scheduling."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from nexus.jobs.intraday_schedule import (
    SNAPSHOT_CONTRACT_HASH,
    IntradaySnapshotJobPayloadV1,
    bucket_for,
    build_intraday_payload,
)
from pydantic import ValidationError

AAPL_ID = UUID("019d0000-0000-7000-8000-0000000000a1")
MSFT_ID = UUID("019d0000-0000-7000-8000-0000000000a2")
CALENDAR_ID = UUID("019d0000-0000-7000-8000-0000000000c1")


def test_bucket_for_uses_exact_half_open_utc_window() -> None:
    bucket = bucket_for(datetime(2026, 8, 28, 14, 30, 29, tzinfo=UTC))
    assert bucket.start == datetime(2026, 8, 28, 14, 30, 15, tzinfo=UTC)
    assert bucket.end == datetime(2026, 8, 28, 14, 30, 30, tzinfo=UTC)


def test_payload_freezes_sorted_symbol_authority() -> None:
    bucket = bucket_for(datetime(2026, 8, 28, 14, 30, 29, tzinfo=UTC))
    payload = build_intraday_payload(
        calendar_authority_id=CALENDAR_ID,
        session="2026-08-28",
        bucket=bucket,
        instruments=((MSFT_ID, "MSFT"), (AAPL_ID, "AAPL")),
        feed="iex",
        delivery_mode="real_time",
    )

    assert payload.model_dump(mode="json") == {
        "schema": "intraday_snapshot_job_v1",
        "calendar_authority_id": str(CALENDAR_ID),
        "session": "2026-08-28",
        "bucket_start": "2026-08-28T14:30:15.000000Z",
        "bucket_end": "2026-08-28T14:30:30.000000Z",
        "instruments": [
            {"instrument_id": str(AAPL_ID), "symbol": "AAPL"},
            {"instrument_id": str(MSFT_ID), "symbol": "MSFT"},
        ],
        "symbol_set_hash": payload.symbol_set_hash,
        "feed": "iex",
        "delivery_mode": "real_time",
        "contract_hash": SNAPSHOT_CONTRACT_HASH,
    }


@pytest.mark.parametrize(
    "update",
    (
        {"contract_hash": "f" * 64},
        {"bucket_end": "2026-08-28T14:30:31.000000Z"},
        {"delivery_mode": "delayed"},
        {
            "instruments": [
                {"instrument_id": str(AAPL_ID), "symbol": "AAPL"},
                {"instrument_id": str(AAPL_ID), "symbol": "MSFT"},
            ]
        },
    ),
)
def test_payload_rejects_mutated_authority(update: dict[str, object]) -> None:
    payload = build_intraday_payload(
        calendar_authority_id=CALENDAR_ID,
        session="2026-08-28",
        bucket=bucket_for(datetime(2026, 8, 28, 14, 30, 29, tzinfo=UTC)),
        instruments=((AAPL_ID, "AAPL"), (MSFT_ID, "MSFT")),
        feed="iex",
        delivery_mode="real_time",
    ).model_dump(mode="json")
    payload.update(update)
    with pytest.raises(ValidationError):
        IntradaySnapshotJobPayloadV1.model_validate(payload)
