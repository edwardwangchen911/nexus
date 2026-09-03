"""Explicit quote receipt times remain compatible with older market responses."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from nexus.api.schemas.market import MarketItemResponse
from pydantic import ValidationError


def payload():
    return dict(
        instrument_id=UUID(int=1),
        symbol="TEST",
        price="1.25",
        source="fosun",
        feed="l1",
        delivery_mode="unknown",
        as_of="2026-09-03T03:00:00Z",
        quality="PARTIAL",
        quality_reasons=("FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",),
        watchlisted=False,
    )


def test_old_market_item_defaults_new_time_fields_to_unknown():
    result = MarketItemResponse.model_validate(payload()).model_dump(mode="json")
    assert result["source_observed_at"] is None
    assert result["received_at"] is None
    assert result["as_of"] == "2026-09-03T03:00:00Z"


@pytest.mark.parametrize("field", ["source_observed_at", "received_at"])
def test_explicit_quote_times_require_timezone_and_normalize_to_utc(field):
    result = MarketItemResponse.model_validate({**payload(), field: "2026-09-03T11:00:00+08:00"})
    assert getattr(result, field) == datetime(2026, 9, 3, 3, tzinfo=UTC)
    with pytest.raises(ValidationError):
        MarketItemResponse.model_validate({**payload(), field: "2026-09-03T11:00:00"})
