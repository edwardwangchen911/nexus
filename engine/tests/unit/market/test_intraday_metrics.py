"""Deterministic display-only intraday metric contracts."""

from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.market.intraday_metrics import (
    IntradayAuthorityView,
    IntradayQuoteView,
    IntradayTradeView,
    compute_intraday_metrics,
)
from nexus.providers.base import InstrumentId, PriceBar

INSTRUMENT_ID = UUID("019d0000-0000-7000-8000-0000000000a1")
DECISION_TIME = datetime(2026, 8, 28, 15, 31, tzinfo=UTC)


def _bars() -> tuple[PriceBar, ...]:
    start = datetime(2026, 8, 28, 15, 0, tzinfo=UTC)
    rows: list[PriceBar] = []
    for index in range(31):
        close = Decimal("100") + Decimal(index) / Decimal("10")
        if index == 25:
            close = Decimal("101.980198019801980198")
        if index == 30:
            close = Decimal("103")
        rows.append(
            PriceBar(
                instrument_id=InstrumentId(INSTRUMENT_ID),
                session=date(2026, 8, 28),
                open=close,
                high=Decimal("104") if index == 30 else close,
                low=Decimal("100") if index in (0, 1) else close,
                close=close,
                volume=Decimal("1000") + index,
                observed_at=start + timedelta(minutes=index),
                available_at=start + timedelta(minutes=index + 1),
                received_at=start + timedelta(minutes=index + 1, seconds=1),
                revision="1",
            )
        )
    return tuple(rows)


def _view(
    *,
    bars: tuple[PriceBar, ...] | None = None,
    quote_at: datetime | None = None,
    reasons: tuple[str, ...] = (),
) -> IntradayAuthorityView:
    return IntradayAuthorityView(
        instrument_id=INSTRUMENT_ID,
        source="alpaca",
        feed="iex",
        delivery_mode="real_time",
        bars=_bars() if bars is None else bars,
        trade=IntradayTradeView(
            last=Decimal("103"),
            observed_at=DECISION_TIME - timedelta(seconds=10),
            available_at=DECISION_TIME - timedelta(seconds=9),
            quality_flags=(),
        ),
        quote=IntradayQuoteView(
            bid=Decimal("99.977989"),
            ask=Decimal("100.022011"),
            observed_at=quote_at or DECISION_TIME - timedelta(seconds=10),
            available_at=quote_at or DECISION_TIME - timedelta(seconds=9),
            quality_flags=(),
        ),
        quality_reasons=reasons,
    )


def test_complete_windows_produce_exact_decimal_metrics() -> None:
    metrics = compute_intraday_metrics(_view(), DECISION_TIME)

    assert metrics.return_5m == Decimal("0.010000000000")
    assert metrics.return_30m == Decimal("0.030000000000")
    assert metrics.range_30m == Decimal("0.040000000000")
    assert metrics.spread_bps == Decimal("4.402200000000")
    assert metrics.sample_count == 31
    assert metrics.quality_reasons == ()


@pytest.mark.parametrize(
    ("mutation", "reason"),
    (
        ("gap", "INTRADAY_WINDOW_GAP"),
        ("duplicate", "INTRADAY_TIMESTAMP_DUPLICATE"),
        ("future", "INTRADAY_BAR_NOT_VISIBLE"),
        ("incomplete", "INTRADAY_BAR_INCOMPLETE"),
        ("flagged", "INTRADAY_BAR_QUALITY_FLAGS"),
    ),
)
def test_invalid_bar_evidence_never_fabricates_zero(
    mutation: str,
    reason: str,
) -> None:
    bars = list(_bars())
    if mutation == "gap":
        bars.pop(15)
    elif mutation == "duplicate":
        bars[15] = bars[15].model_copy(update={"observed_at": bars[14].observed_at})
    elif mutation == "future":
        bars[-1] = bars[-1].model_copy(
            update={"available_at": DECISION_TIME + timedelta(microseconds=1)}
        )
    elif mutation == "incomplete":
        bars[-1] = bars[-1].model_copy(
            update={"observed_at": DECISION_TIME - timedelta(seconds=30)}
        )
    else:
        bars[-1] = bars[-1].model_copy(update={"quality_flags": ("provider_flag",)})

    metrics = compute_intraday_metrics(_view(bars=tuple(bars)), DECISION_TIME)

    assert metrics.return_5m is None
    assert metrics.return_30m is None
    assert metrics.range_30m is None
    assert reason in metrics.quality_reasons


def test_stale_quote_removes_spread_without_changing_bar_returns() -> None:
    metrics = compute_intraday_metrics(
        _view(quote_at=DECISION_TIME - timedelta(seconds=46)),
        DECISION_TIME,
    )

    assert metrics.spread_bps is None
    assert metrics.return_5m == Decimal("0.010000000000")
    assert "INTRADAY_QUOTE_STALE" in metrics.quality_reasons


def test_cross_source_reason_blocks_all_composite_metrics() -> None:
    metrics = compute_intraday_metrics(
        _view(reasons=("INTRADAY_SOURCE_CONFLICT",)),
        DECISION_TIME,
    )

    assert metrics.return_5m is None
    assert metrics.return_30m is None
    assert metrics.range_30m is None
    assert metrics.spread_bps is None


def test_outside_session_preserves_facts_without_stale_or_metrics() -> None:
    outside = datetime(2026, 8, 29, 15, 31, tzinfo=UTC)
    metrics = compute_intraday_metrics(_view(), outside)

    assert metrics.return_5m is None
    assert metrics.spread_bps is None
    assert metrics.quote_age_seconds is None
    assert "INTRADAY_OUTSIDE_SESSION" in metrics.quality_reasons
