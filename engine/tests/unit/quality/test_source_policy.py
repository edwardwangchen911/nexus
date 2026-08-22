from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import permutations
from uuid import UUID

from hypothesis import given
from hypothesis import strategies as st
from nexus.providers.base import PriceBar, PriceSeries
from nexus.quality.source_policy import SourcePolicy, select_price_series

INSTRUMENT_ID = UUID("29e18e7a-78fb-5fc1-a65a-6641fbc1cc64")
DECISION_TIME = datetime(2026, 8, 21, 22, tzinfo=UTC)


def _series(
    source: str,
    close: str,
    *,
    interval: str = "1Day",
    open_: str | None = None,
    available_at: datetime = DECISION_TIME,
    capability_expires_at: datetime | None = None,
) -> PriceSeries:
    price = Decimal(close)
    return PriceSeries(
        source=source,
        feed="authorized" if source == "fosun" else "sip",
        adjustment="all",
        interval=interval,
        mode="fixture",
        quality="PASS",
        capability_expires_at=capability_expires_at,
        bars=(
            PriceBar(
                instrument_id=INSTRUMENT_ID,
                session=date(2026, 8, 21),
                open=Decimal(open_ or close),
                high=max(price, Decimal(open_ or close)),
                low=min(price, Decimal(open_ or close)),
                close=price,
                volume=Decimal("1000"),
                available_at=available_at,
            ),
        ),
    )


def test_selects_one_complete_preferred_series_without_field_merge() -> None:
    fosun = _series(
        "fosun",
        "100.00",
        open_="99.00",
        capability_expires_at=DECISION_TIME + timedelta(hours=1),
    )
    alpaca = _series("alpaca", "100.01", open_="80.00")

    result = select_price_series(
        (alpaca, fosun),
        SourcePolicy.v1(instrument_tick_size=Decimal("0.01")),
        DECISION_TIME,
    )

    assert result.quality == "PASS"
    assert result.selected == fosun
    assert result.selected.bars[0].open == Decimal("99.00")


def test_daily_close_conflict_above_tick_or_twenty_bps_fails_closed() -> None:
    result = select_price_series(
        (
            _series(
                "fosun",
                "100.00",
                capability_expires_at=DECISION_TIME + timedelta(hours=1),
            ),
            _series("alpaca", "100.21"),
        ),
        SourcePolicy.v1(instrument_tick_size=Decimal("0.01")),
        DECISION_TIME,
    )

    assert result.selected is None
    assert result.quality == "FAIL"
    assert result.reasons == ("SOURCE_CONFLICT",)


def test_unknown_tick_size_makes_daily_cross_check_partial() -> None:
    result = select_price_series(
        (
            _series(
                "fosun",
                "100.00",
                capability_expires_at=DECISION_TIME + timedelta(hours=1),
            ),
            _series("alpaca", "100.00"),
        ),
        SourcePolicy.v1(instrument_tick_size=None),
        DECISION_TIME,
    )

    assert result.selected is not None
    assert result.selected.source == "fosun"
    assert result.quality == "PARTIAL"
    assert result.reasons == ("TICK_SIZE_UNKNOWN",)


def test_adjustment_mismatch_fails_even_when_tick_size_is_unknown() -> None:
    fosun = _series(
        "fosun",
        "100.00",
        capability_expires_at=DECISION_TIME + timedelta(hours=1),
    )
    alpaca = _series("alpaca", "100.00").model_copy(update={"adjustment": "raw"})

    result = select_price_series(
        (fosun, alpaca),
        SourcePolicy.v1(instrument_tick_size=None),
        DECISION_TIME,
    )

    assert result.selected is None
    assert result.quality == "FAIL"
    assert result.reasons == ("SOURCE_CONFLICT",)


def test_intraday_difference_above_fifty_bps_is_alert_only() -> None:
    result = select_price_series(
        (
            _series(
                "fosun",
                "100.00",
                interval="1Min",
                capability_expires_at=DECISION_TIME + timedelta(hours=1),
            ),
            _series("alpaca", "100.51", interval="1Min"),
        ),
        SourcePolicy.v1(instrument_tick_size=Decimal("0.01")),
        DECISION_TIME,
    )

    assert result.selected is not None
    assert result.quality == "PASS"
    assert result.reasons == ("INTRADAY_DIVERGENCE_ALERT",)


def test_expired_fosun_capability_uses_explicit_alpaca_fallback() -> None:
    result = select_price_series(
        (
            _series(
                "fosun",
                "100.00",
                capability_expires_at=DECISION_TIME - timedelta(seconds=1),
            ),
            _series("alpaca", "100.00"),
        ),
        SourcePolicy.v1(instrument_tick_size=Decimal("0.01")),
        DECISION_TIME,
    )

    assert result.selected is not None
    assert result.selected.source == "alpaca"
    assert result.reasons == ("FOSUN_CAPABILITY_EXPIRED",)


def test_future_bar_is_never_visible_to_a_decision() -> None:
    result = select_price_series(
        (_series("alpaca", "100", available_at=DECISION_TIME + timedelta(microseconds=1)),),
        SourcePolicy.v1(instrument_tick_size=Decimal("0.01")),
        DECISION_TIME,
    )

    assert result.selected is None
    assert result.quality == "FAIL"
    assert result.reasons == ("NO_POINT_IN_TIME_DATA",)


@given(st.permutations((0, 1, 2)))
def test_selection_is_deterministic_under_candidate_order(order: list[int]) -> None:
    candidates = (
        _series("alpaca", "100.00"),
        _series(
            "fosun",
            "100.00",
            capability_expires_at=DECISION_TIME + timedelta(hours=1),
        ),
        _series("other", "100.00"),
    )
    ordered = tuple(candidates[index] for index in order)

    result = select_price_series(
        ordered,
        SourcePolicy.v1(instrument_tick_size=Decimal("0.01")),
        DECISION_TIME,
    )

    assert result.selected is not None
    assert result.selected.source == "fosun"
    assert result.quality == "PASS"


def test_all_candidate_permutations_have_the_same_selection() -> None:
    candidates = (
        _series("alpaca", "100.00"),
        _series(
            "fosun",
            "100.00",
            capability_expires_at=DECISION_TIME + timedelta(hours=1),
        ),
        _series("other", "100.00"),
    )

    results = {
        select_price_series(
            ordered,
            SourcePolicy.v1(instrument_tick_size=Decimal("0.01")),
            DECISION_TIME,
        ).model_dump_json()
        for ordered in permutations(candidates)
    }

    assert len(results) == 1


def test_reasons_are_deterministic_when_candidates_are_reordered() -> None:
    expired = _series(
        "fosun",
        "100",
        capability_expires_at=DECISION_TIME - timedelta(seconds=1),
    )
    alpaca = _series("alpaca", "100")
    future_bar = alpaca.bars[0].model_copy(
        update={
            "session": date(2026, 8, 22),
            "available_at": DECISION_TIME + timedelta(seconds=1),
        }
    )
    partially_future = alpaca.model_copy(update={"bars": alpaca.bars + (future_bar,)})

    results = {
        select_price_series(
            ordered,
            SourcePolicy.v1(instrument_tick_size=Decimal("0.01")),
            DECISION_TIME,
        ).model_dump_json()
        for ordered in permutations((expired, partially_future))
    }

    assert len(results) == 1


def test_unapproved_source_is_not_a_silent_fallback() -> None:
    result = select_price_series(
        (_series("other", "100"),),
        SourcePolicy.v1(instrument_tick_size=Decimal("0.01")),
        DECISION_TIME,
    )

    assert result.selected is None
    assert result.quality == "FAIL"
    assert result.reasons == ("NO_ELIGIBLE_SOURCE",)
