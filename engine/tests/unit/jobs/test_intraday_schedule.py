"""Pure contracts for 15-second intraday scheduling."""

from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID, uuid5

import pytest
from nexus.jobs import intraday_schedule
from nexus.jobs.intraday_schedule import (
    bucket_for,
    build_alpaca_intraday_payload,
    build_intraday_payload,
)
from nexus.jobs.market_authority import FrozenFosunMarketAuthority
from nexus.providers.settings import ProviderSettings
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
        "schema": "intraday_snapshot_job_v3",
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
        "contract_hash": intraday_schedule.FOSUN_SNAPSHOT_CONTRACT_HASH_V3,
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
        "schema": "intraday_snapshot_job_v3",
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
        "contract_hash": intraday_schedule.ALPACA_SNAPSHOT_CONTRACT_HASH_V3,
    }


def test_schwab_payload_freezes_realtime_l1_without_fosun_authority() -> None:
    bucket = bucket_for(datetime(2026, 8, 28, 14, 30, 29, tzinfo=UTC))

    payload = intraday_schedule.build_schwab_intraday_payload(
        calendar_authority_id=CALENDAR_ID,
        session="2026-08-28",
        bucket=bucket,
        instruments=((MSFT_ID, "MSFT"), (AAPL_ID, "AAPL")),
    )

    assert payload.model_dump(mode="json") == {
        "schema": "intraday_snapshot_job_v3",
        "calendar_authority_id": str(CALENDAR_ID),
        "session": "2026-08-28",
        "bucket_start": "2026-08-28T14:30:15.000000Z",
        "bucket_end": "2026-08-28T14:30:30.000000Z",
        "instruments": [
            {"instrument_id": str(AAPL_ID), "symbol": "AAPL"},
            {"instrument_id": str(MSFT_ID), "symbol": "MSFT"},
        ],
        "symbol_set_hash": payload.symbol_set_hash,
        "source": "schwab",
        "feed": "l1",
        "delivery_mode": "real_time",
        "include_minute_bars": False,
        "quote_authority": None,
        "bars_authority": None,
        "contract_hash": intraday_schedule.SCHWAB_SNAPSHOT_CONTRACT_HASH,
    }


def test_schwab_payload_accepts_thirty_unique_symbols() -> None:
    instruments = tuple(
        (uuid5(AAPL_ID, f"symbol-{number}"), f"S{number:02d}")
        for number in range(30)
    )

    payload = intraday_schedule.build_schwab_intraday_payload(
        calendar_authority_id=CALENDAR_ID,
        session="2026-08-28",
        bucket=bucket_for(datetime(2026, 8, 28, 14, 30, 29, tzinfo=UTC)),
        instruments=instruments,
    )

    assert len(payload.instruments) == 30


@pytest.mark.parametrize(
    "update",
    (
        {"delivery_mode": "delayed"},
        {"include_minute_bars": True},
        {
            "instruments": [
                {"instrument_id": str(AAPL_ID), "symbol": "AAPL"},
                {"instrument_id": str(MSFT_ID), "symbol": "AAPL"},
            ]
        },
    ),
)
def test_schwab_payload_rejects_non_realtime_or_duplicate_authority(
    update: dict[str, object],
) -> None:
    payload = intraday_schedule.build_schwab_intraday_payload(
        calendar_authority_id=CALENDAR_ID,
        session="2026-08-28",
        bucket=bucket_for(datetime(2026, 8, 28, 14, 30, 29, tzinfo=UTC)),
        instruments=((AAPL_ID, "AAPL"), (MSFT_ID, "MSFT")),
    ).model_dump(mode="json")
    payload.update(update)

    with pytest.raises(ValidationError):
        intraday_schedule.IntradaySnapshotJobPayloadV3.model_validate(payload)


@pytest.mark.asyncio
async def test_scheduler_selects_schwab_without_changing_daily_fosun(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class _Rows:
        def all(self) -> tuple[object, ...]:
            return ()

    class _Session:
        async def execute(self, _statement: object, _parameters: object) -> None:
            return None

        async def scalars(self, _statement: object) -> _Rows:
            return _Rows()

        async def scalar(self, _statement: object) -> None:
            return None

    async def current_authority(*_: object, **__: object) -> object:
        return intraday_schedule.IntradayDatabaseAuthority(
            calendar_authority_id=CALENDAR_ID,
            session=datetime(2026, 8, 28, tzinfo=UTC).date(),
            instruments=((AAPL_ID, "AAPL"),),
        )

    async def forbidden_fosun_authority(*_: object, **__: object) -> object:
        raise AssertionError("Schwab intraday scheduling must not load Fosun authority")

    async def enqueue(_session: object, **kwargs: object) -> object:
        captured.update(kwargs)
        return SimpleNamespace(
            schema_version=kwargs["schema_version"],
            payload=kwargs["payload"],
            scope_id=kwargs["key"].scope_id,  # type: ignore[union-attr]
        )

    monkeypatch.setattr(
        intraday_schedule,
        "_load_current_intraday_authority",
        current_authority,
    )
    monkeypatch.setattr(
        intraday_schedule,
        "load_frozen_fosun_market_authority",
        forbidden_fosun_authority,
    )
    monkeypatch.setattr(intraday_schedule, "enqueue_job", enqueue)
    settings = ProviderSettings(
        runtime_mode="production",
        market_provider="fosun",
        intraday_provider="schwab",
        intraday_monitor_mode="production",
        intraday_max_symbols=30,
        fosun_market_gateway_origin="https://fosun-gateway.internal:8443",
        fosun_market_gateway_key_id="synthetic-market-current",
        fosun_market_gateway_secret="synthetic-market-secret",
        fosun_market_gateway_client_cert="/synthetic/market.crt",
        fosun_market_gateway_client_key="/synthetic/market.key",
        fosun_market_gateway_client_ca="/synthetic/ca.crt",
        sec_contact="owner@example.com",
        fred_api_key="fred-canary",
    )

    job = await intraday_schedule.schedule_due_intraday_snapshot(
        _Session(),  # type: ignore[arg-type]
        now=datetime(2026, 8, 28, 14, 30, 29, tzinfo=UTC),
        settings=settings,
    )

    assert settings.market_provider.value == "fosun"
    assert job is not None
    assert job.schema_version == "intraday_snapshot_job_v3"
    assert job.scope_id == "schwab-intraday"
    assert job.payload["source"] == "schwab"
    assert captured["max_attempts"] == 1


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
        contract_hash=intraday_schedule.ALPACA_SNAPSHOT_CONTRACT_HASH_V3,
    )

    with pytest.raises(ValidationError):
        intraday_schedule.IntradaySnapshotJobPayloadV3.model_validate(payload)


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
        intraday_schedule.IntradaySnapshotJobPayloadV3.model_validate(payload)


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
        intraday_schedule.IntradaySnapshotJobPayloadV3.model_validate(payload)


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
