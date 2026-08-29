"""Strict wire bounds for display-only intraday market evidence."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from nexus.api.schemas.market import IntradayMonitorResponse
from pydantic import ValidationError

INSTRUMENT_ID = UUID("019d0000-0000-7000-8000-0000000000a1")
NOW = datetime(2026, 8, 28, 15, 31, tzinfo=UTC)


def _payload() -> dict[str, object]:
    bar = {
        "observed_at": "2026-08-28T15:30:00Z",
        "open": "102.90",
        "high": "103.10",
        "low": "102.80",
        "close": "103.00",
        "volume": "1200",
        "available_at": "2026-08-28T15:31:00Z",
        "quality_flags": [],
    }
    return {
        "instrument_id": str(INSTRUMENT_ID),
        "status": "ready",
        "source": "alpaca",
        "feed": "iex",
        "delivery_mode": "real_time",
        "last": "103.00",
        "bid": "102.98",
        "ask": "103.02",
        "trade_as_of": "2026-08-28T15:30:50Z",
        "quote_as_of": "2026-08-28T15:30:51Z",
        "bar_as_of": "2026-08-28T15:30:00Z",
        "return_5m": "0.010000000000",
        "return_30m": "0.030000000000",
        "range_30m": "0.040000000000",
        "spread_bps": "3.883872000000",
        "quote_age_seconds": 9,
        "bar_age_seconds": 60,
        "sample_count": 31,
        "quality_reasons": [],
        "bars": [bar],
    }


def test_intraday_monitor_accepts_only_the_exact_decimal_string_contract() -> None:
    response = IntradayMonitorResponse.model_validate(_payload())

    assert response.instrument_id == INSTRUMENT_ID
    assert response.status == "ready"
    assert response.bars[0].observed_at == datetime(2026, 8, 28, 15, 30, tzinfo=UTC)


@pytest.mark.parametrize(
    "mutation",
    (
        lambda payload: payload.update({"status": "buy"}),
        lambda payload: payload.update({"return_5m": 0.01}),
        lambda payload: payload.update({"quote_age_seconds": -1}),
        lambda payload: payload.update({"credential": "must-not-escape"}),
        lambda payload: payload.update({"bars": payload["bars"] * 61}),
        lambda payload: payload["bars"][0].update({"quality_flags": ["bad-flag"]}),
        lambda payload: payload["bars"][0].update(
            {"quality_flags": [f"FLAG_{index}" for index in range(17)]}
        ),
    ),
)
def test_intraday_monitor_rejects_malformed_or_oversized_shapes(mutation) -> None:
    payload = _payload()
    mutation(payload)
    with pytest.raises(ValidationError):
        IntradayMonitorResponse.model_validate(payload)
