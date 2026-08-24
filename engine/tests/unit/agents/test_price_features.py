"""Literal checks for deterministic frozen price feature arithmetic."""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from nexus.agents.price_features import compute_price_features
from nexus.persistence.models.data import PriceBarRecord


def test_price_features_compute_literal_trend_and_nonzero_realized_volatility() -> None:
    bars = tuple(_bar(index, Decimal(index)) for index in range(1, 201))

    result = compute_price_features(
        bars, ("close", "sma_20", "sma_200", "rsi_14", "volatility_20")
    )

    assert result.missing == ()
    assert result.values == {
        "close": "200.000000000000",
        "sma_20": "190.500000000000",
        "sma_200": "100.500000000000",
        "rsi_14": "100.000000000000",
        "volatility_20": "0.002554228858",
    }


def _bar(index: int, close: Decimal) -> PriceBarRecord:
    observed_at = datetime(2026, 1, 1, tzinfo=UTC) + timedelta(days=index)
    return PriceBarRecord(
        id=UUID(f"0198d500-1000-7000-8000-{index:012d}"),
        price_series_manifest_id=UUID("0198d500-1000-7000-8000-000000001707"),
        instrument_id=UUID("0198d500-1000-7000-8000-000000001601"),
        session=date(2026, 1, 1) + timedelta(days=index),
        open=close,
        high=close + Decimal(1),
        low=close,
        close=close,
        volume=Decimal(1000),
        observed_at=observed_at,
        available_at=observed_at + timedelta(minutes=1),
        received_at=observed_at + timedelta(minutes=2),
        revision="fixture-r1",
        quality_flags=[],
    )
