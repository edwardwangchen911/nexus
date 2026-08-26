from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.paper.fills import (
    EXECUTION_MODEL_V1,
    ExecutionBar,
    ExecutionCalendarSession,
    ExecutionQuote,
    calculate_fill,
    execution_model_hash,
    qualify_execution_window,
)
from nexus.paper.models import OrderEventInput, PaperOrder
from pydantic import ValidationError

ORDER_ID = UUID("61000000-0000-0000-0000-000000000001")
ACCOUNT_ID = UUID("61000000-0000-0000-0000-000000000002")
INSTRUMENT_ID = UUID("61000000-0000-0000-0000-000000000003")
AUTHORITY_ID = UUID("61000000-0000-0000-0000-000000000004")
OPEN = datetime(2026, 8, 27, 13, 30, tzinfo=UTC)
EVALUATED = datetime(2026, 8, 27, 13, 55, tzinfo=UTC)


def _calendar(*, close: datetime | None = None) -> ExecutionCalendarSession:
    return ExecutionCalendarSession(
        session=date(2026, 8, 27),
        market_open_at=OPEN,
        market_close_at=close or datetime(2026, 8, 27, 20, tzinfo=UTC),
    )


def _bars(
    *,
    closes: tuple[str, ...] = ("100", "101", "102", "103", "104"),
    volumes: tuple[str, ...] = ("100", "200", "300", "200", "200"),
    available_at: datetime = EVALUATED,
) -> tuple[ExecutionBar, ...]:
    return tuple(
        ExecutionBar(
            bar_id=UUID(int=0x61000000000000000000000000000100 + index),
            authority_id=AUTHORITY_ID,
            instrument_id=INSTRUMENT_ID,
            source="fixture",
            feed="fixture",
            delivery_mode="fixture",
            observed_at=OPEN + timedelta(minutes=index),
            available_at=available_at,
            close=Decimal(close),
            volume=Decimal(volume),
        )
        for index, (close, volume) in enumerate(zip(closes, volumes, strict=True))
    )


def _order(side: str = "buy", quantity: str = "20") -> PaperOrder:
    event_time = datetime(2026, 8, 26, 20, tzinfo=UTC)
    return PaperOrder(
        order_id=ORDER_ID,
        account_id=ACCOUNT_ID,
        strategy_decision_id=UUID(int=20),
        client_order_id="task19-order",
        instrument_id=INSTRUMENT_ID,
        side=side,
        quantity=Decimal(quantity),
        worst_price=Decimal("110"),
        events=(
            OrderEventInput(
                event_id=UUID(int=21),
                event_seq=1,
                occurred_at=event_time,
                from_status=None,
                to_status="proposed",
                actor_kind="task18",
                reason_code="PROPOSED",
                payload_hash="1" * 64,
            ),
            OrderEventInput(
                event_id=UUID(int=22),
                event_seq=2,
                occurred_at=event_time,
                from_status="proposed",
                to_status="risk_approved",
                actor_kind="task18",
                reason_code="RISK_APPROVED",
                payload_hash="2" * 64,
            ),
            OrderEventInput(
                event_id=UUID(int=23),
                event_seq=3,
                occurred_at=event_time,
                from_status="risk_approved",
                to_status="submitted",
                actor_kind="task18",
                reason_code="SUBMITTED",
                payload_hash="3" * 64,
            ),
        ),
    )


def test_execution_model_v1_is_content_addressed_and_frozen() -> None:
    assert EXECUTION_MODEL_V1.version == "execution_model_v1"
    assert EXECUTION_MODEL_V1.price_method == "close_volume_vwap_v1"
    assert execution_model_hash(EXECUTION_MODEL_V1) == (
        "18a4f8c7b9111e668baf011f33210df3f24ffc02ae15a44"
        "8891b91898e169433"
    )

    with pytest.raises(ValidationError):
        EXECUTION_MODEL_V1.model_copy(update={"unexpected": True}).model_validate(
            {**EXECUTION_MODEL_V1.model_dump(), "unexpected": True}
        )


def test_window_uses_close_volume_vwap_and_one_percent_capacity() -> None:
    window = qualify_execution_window(
        model=EXECUTION_MODEL_V1,
        calendar=_calendar(),
        bars=_bars(),
        quote=None,
        evaluated_at=EVALUATED,
    )

    assert window.window_start_at == OPEN
    assert window.window_end_at == OPEN + timedelta(minutes=5)
    assert window.vwap == Decimal("102.200000000000")
    assert window.volume == Decimal("1000.000000000000")
    assert window.capacity == Decimal("10.000000000000")
    assert window.quote_id is None
    assert window.content_hash != "0" * 64


@pytest.mark.parametrize(
    ("bars", "message"),
    (
        (_bars()[:4], "exactly five"),
        (
            tuple(
                bar.model_copy(
                    update={"observed_at": bar.observed_at + timedelta(minutes=1)}
                )
                if index == 3
                else bar
                for index, bar in enumerate(_bars())
            ),
            "continuous",
        ),
        (
            tuple(
                bar.model_copy(update={"authority_id": UUID(int=999)})
                if index == 4
                else bar
                for index, bar in enumerate(_bars())
            ),
            "authority",
        ),
        (
            _bars(volumes=("0", "0", "0", "0", "0")),
            "positive volume",
        ),
    ),
)
def test_window_rejects_incomplete_discontinuous_mixed_or_empty_bars(
    bars: tuple[ExecutionBar, ...], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        qualify_execution_window(
            model=EXECUTION_MODEL_V1,
            calendar=_calendar(),
            bars=bars,
            quote=None,
            evaluated_at=EVALUATED,
        )


def test_window_rejects_future_data_and_evaluation_before_0955() -> None:
    with pytest.raises(ValueError, match="not yet available"):
        qualify_execution_window(
            model=EXECUTION_MODEL_V1,
            calendar=_calendar(),
            bars=_bars(available_at=EVALUATED + timedelta(seconds=1)),
            quote=None,
            evaluated_at=EVALUATED,
        )

    with pytest.raises(ValueError, match="first evaluation"):
        qualify_execution_window(
            model=EXECUTION_MODEL_V1,
            calendar=_calendar(),
            bars=_bars(available_at=EVALUATED - timedelta(minutes=1)),
            quote=None,
            evaluated_at=EVALUATED - timedelta(seconds=1),
        )


def test_qualified_quote_applies_half_spread_and_ten_bps() -> None:
    quote = ExecutionQuote(
        quote_id=UUID(int=50),
        instrument_id=INSTRUMENT_ID,
        source="fixture",
        feed="fixture",
        bid=Decimal("101.80"),
        ask=Decimal("102.20"),
        observed_at=OPEN + timedelta(minutes=4),
        available_at=EVALUATED,
        revision="q1",
        quality_flags=(),
    )
    window = qualify_execution_window(
        model=EXECUTION_MODEL_V1,
        calendar=_calendar(),
        bars=_bars(),
        quote=quote,
        evaluated_at=EVALUATED,
    )
    fill = calculate_fill(
        model=EXECUTION_MODEL_V1,
        order=_order(),
        existing_filled_quantity=Decimal(0),
        reservation_outstanding=Decimal("2000"),
        window=window,
        tick_size=Decimal("0.01"),
    )

    assert window.half_spread == Decimal("0.200000000000")
    assert fill.quantity == Decimal("10.000000000000")
    assert fill.slippage_bps == 10
    assert fill.price == Decimal("102.510000000000")
    assert fill.notional == Decimal("1025.100000000000")
    assert fill.slippage_amount == Decimal("3.022000000000")


@pytest.mark.parametrize(
    "quote",
    (
        None,
        ExecutionQuote(
            quote_id=UUID(int=51),
            instrument_id=INSTRUMENT_ID,
            source="other",
            feed="fixture",
            bid=Decimal("101.8"),
            ask=Decimal("102.2"),
            observed_at=OPEN,
            available_at=EVALUATED,
            revision="q2",
            quality_flags=(),
        ),
        ExecutionQuote(
            quote_id=UUID(int=52),
            instrument_id=INSTRUMENT_ID,
            source="fixture",
            feed="fixture",
            bid=Decimal("101.8"),
            ask=Decimal("102.2"),
            observed_at=OPEN,
            available_at=EVALUATED,
            revision="q3",
            quality_flags=("STALE",),
        ),
    ),
)
def test_missing_or_ineligible_quote_uses_conservative_twenty_five_bps(
    quote: ExecutionQuote | None,
) -> None:
    window = qualify_execution_window(
        model=EXECUTION_MODEL_V1,
        calendar=_calendar(),
        bars=_bars(),
        quote=quote,
        evaluated_at=EVALUATED,
    )
    fill = calculate_fill(
        model=EXECUTION_MODEL_V1,
        order=_order(),
        existing_filled_quantity=Decimal(0),
        reservation_outstanding=Decimal("2000"),
        window=window,
        tick_size=Decimal("0.01"),
    )

    assert window.quote_id is None
    assert fill.slippage_bps == 25
    assert fill.price == Decimal("102.460000000000")


def test_sell_rounds_down_and_position_reservation_caps_fill() -> None:
    window = qualify_execution_window(
        model=EXECUTION_MODEL_V1,
        calendar=_calendar(),
        bars=_bars(closes=("10.001",) * 5, volumes=("1000",) * 5),
        quote=None,
        evaluated_at=EVALUATED,
    )
    fill = calculate_fill(
        model=EXECUTION_MODEL_V1,
        order=_order(side="sell", quantity="100"),
        existing_filled_quantity=Decimal("5"),
        reservation_outstanding=Decimal("7"),
        window=window,
        tick_size=Decimal("0.01"),
    )

    assert fill.quantity == Decimal("7.000000000000")
    assert fill.price == Decimal("9.970000000000")


def test_buy_cash_reservation_caps_quantity_without_going_negative() -> None:
    window = qualify_execution_window(
        model=EXECUTION_MODEL_V1,
        calendar=_calendar(),
        bars=_bars(volumes=("1000",) * 5),
        quote=None,
        evaluated_at=EVALUATED,
    )
    fill = calculate_fill(
        model=EXECUTION_MODEL_V1,
        order=_order(quantity="100"),
        existing_filled_quantity=Decimal("98"),
        reservation_outstanding=Decimal("102.25"),
        window=window,
        tick_size=Decimal("0.01"),
    )

    assert fill.quantity == Decimal("0E-12")
    assert fill.notional == Decimal("0E-12")


def test_calendar_accepts_early_close_but_rejects_invalid_bounds() -> None:
    early_close = _calendar(close=datetime(2026, 8, 27, 17, tzinfo=UTC))
    assert early_close.market_close_at.hour == 17

    with pytest.raises(ValidationError, match="market close"):
        ExecutionCalendarSession(
            session=date(2026, 8, 27),
            market_open_at=OPEN,
            market_close_at=OPEN,
        )


def test_calendar_uses_authoritative_utc_instants_across_dst() -> None:
    summer = _calendar()
    winter = ExecutionCalendarSession(
        session=date(2026, 12, 1),
        market_open_at=datetime(2026, 12, 1, 14, 30, tzinfo=UTC),
        market_close_at=datetime(2026, 12, 1, 21, tzinfo=UTC),
    )

    assert summer.market_open_at.hour == 13
    assert winter.market_open_at.hour == 14

    with pytest.raises(ValidationError, match="New York session date"):
        ExecutionCalendarSession(
            session=date(2026, 12, 1),
            market_open_at=datetime(2026, 11, 30, 14, 30, tzinfo=UTC),
            market_close_at=datetime(2026, 11, 30, 21, tzinfo=UTC),
        )
