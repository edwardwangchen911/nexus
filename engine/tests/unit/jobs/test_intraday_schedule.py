"""Pure contracts for 15-second intraday scheduling."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from nexus.jobs.intraday_schedule import (
    ALPACA_SNAPSHOT_CONTRACT_HASH,
    SNAPSHOT_CONTRACT_HASH,
    IntradaySnapshotJobPayloadV2,
    bucket_for,
    build_alpaca_intraday_payload,
    build_intraday_payload,
)
from nexus.jobs.market_authority import FrozenFosunMarketAuthority
from pydantic import ValidationError

AAPL_ID = UUID("019d0000-0000-7000-8000-0000000000a1")
MSFT_ID = UUID("019d0000-0000-7000-8000-0000000000a2")
CALENDAR_ID = UUID("019d0000-0000-7000-8000-0000000000c1")


def _authority(operation: str) -> FrozenFosunMarketAuthority:
    return FrozenFosunMarketAuthority(
        operation=operation,
        delivery_mode="unknown",
        capability_id=UUID(
            "019d0000-0000-7000-8000-0000000000e1"
            if operation == "fosun_get_quotes"
            else "019d0000-0000-7000-8000-0000000000e2"
        ),
        capability_schema_version="1",
        capability_created_at=datetime(2026, 8, 28, 13, tzinfo=UTC),
        capability_expires_at=datetime(2026, 8, 29, 13, tzinfo=UTC),
        status_id=UUID("019d0000-0000-7000-8000-0000000000e0"),
        status_version=1,
        status_created_at=datetime(2026, 8, 28, 12, tzinfo=UTC),
        status_mode_changed_at=datetime(2026, 8, 28, 12, tzinfo=UTC),
        capability_invalidated_at=None,
        contract_hash="a" * 64,
    )


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
        quote_authority=_authority("fosun_get_quotes"),
        bars_authority=None,
    )

    document = payload.model_dump(mode="json")
    assert document == {
        "schema": "intraday_snapshot_job_v2",
        "calendar_authority_id": str(CALENDAR_ID),
        "session": "2026-08-28",
        "bucket_start": "2026-08-28T14:30:15.000000Z",
        "bucket_end": "2026-08-28T14:30:30.000000Z",
        "instruments": [
            {"instrument_id": str(AAPL_ID), "symbol": "AAPL"},
            {"instrument_id": str(MSFT_ID), "symbol": "MSFT"},
        ],
        "symbol_set_hash": payload.symbol_set_hash,
        "source": "fosun",
        "feed": "l1",
        "delivery_mode": "unknown",
        "include_minute_bars": False,
        "quote_authority": _authority("fosun_get_quotes").model_dump(mode="json"),
        "bars_authority": None,
        "contract_hash": SNAPSHOT_CONTRACT_HASH,
    }


def test_alpaca_payload_freezes_an_explicit_source_without_fosun_authority() -> None:
    bucket = bucket_for(datetime(2026, 8, 28, 14, 30, 29, tzinfo=UTC))
    payload = build_alpaca_intraday_payload(
        calendar_authority_id=CALENDAR_ID,
        session="2026-08-28",
        bucket=bucket,
        instruments=((MSFT_ID, "MSFT"), (AAPL_ID, "AAPL")),
        feed="iex",
        delivery_mode="real_time",
    )

    assert payload.model_dump(mode="json") == {
        "schema": "intraday_snapshot_job_v2",
        "calendar_authority_id": str(CALENDAR_ID),
        "session": "2026-08-28",
        "bucket_start": "2026-08-28T14:30:15.000000Z",
        "bucket_end": "2026-08-28T14:30:30.000000Z",
        "instruments": [
            {"instrument_id": str(AAPL_ID), "symbol": "AAPL"},
            {"instrument_id": str(MSFT_ID), "symbol": "MSFT"},
        ],
        "symbol_set_hash": payload.symbol_set_hash,
        "source": "alpaca",
        "feed": "iex",
        "delivery_mode": "real_time",
        "include_minute_bars": False,
        "quote_authority": None,
        "bars_authority": None,
        "contract_hash": ALPACA_SNAPSHOT_CONTRACT_HASH,
    }


def test_payload_rejects_cross_source_authority_smuggling() -> None:
    payload = build_intraday_payload(
        calendar_authority_id=CALENDAR_ID,
        session="2026-08-28",
        bucket=bucket_for(datetime(2026, 8, 28, 14, 30, 29, tzinfo=UTC)),
        instruments=((AAPL_ID, "AAPL"),),
        quote_authority=_authority("fosun_get_quotes"),
        bars_authority=None,
    ).model_dump(mode="json")
    payload.update(
        source="alpaca",
        feed="iex",
        delivery_mode="real_time",
        contract_hash=ALPACA_SNAPSHOT_CONTRACT_HASH,
    )

    with pytest.raises(ValidationError):
        IntradaySnapshotJobPayloadV2.model_validate(payload)


@pytest.mark.parametrize("missing", ("source", "feed"))
def test_payload_requires_explicit_source_and_feed(missing: str) -> None:
    payload = build_intraday_payload(
        calendar_authority_id=CALENDAR_ID,
        session="2026-08-28",
        bucket=bucket_for(datetime(2026, 8, 28, 14, 30, 29, tzinfo=UTC)),
        instruments=((AAPL_ID, "AAPL"),),
        quote_authority=_authority("fosun_get_quotes"),
        bars_authority=None,
    ).model_dump(mode="json")
    payload.pop(missing)

    with pytest.raises(ValidationError):
        IntradaySnapshotJobPayloadV2.model_validate(payload)


@pytest.mark.parametrize(
    "update",
    (
        {"contract_hash": "f" * 64},
        {"bucket_end": "2026-08-28T14:30:31.000000Z"},
        {
            "bucket_start": "2026-08-28T14:30:16.000000Z",
            "bucket_end": "2026-08-28T14:30:31.000000Z",
        },
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
        quote_authority=_authority("fosun_get_quotes"),
        bars_authority=None,
    ).model_dump(mode="json")
    payload.update(update)
    with pytest.raises(ValidationError):
        IntradaySnapshotJobPayloadV2.model_validate(payload)


def test_minute_boundary_requires_separate_bars_authority() -> None:
    bucket = bucket_for(datetime(2026, 8, 28, 14, 30, 1, tzinfo=UTC))
    payload = build_intraday_payload(
        calendar_authority_id=CALENDAR_ID,
        session="2026-08-28",
        bucket=bucket,
        instruments=((AAPL_ID, "AAPL"),),
        quote_authority=_authority("fosun_get_quotes"),
        bars_authority=_authority("fosun_get_bars"),
    )

    assert payload.include_minute_bars is True
    assert payload.bars_authority is not None
    assert payload.bars_authority.capability_id != payload.quote_authority.capability_id
