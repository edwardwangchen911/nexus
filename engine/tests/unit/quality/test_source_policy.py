from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import permutations
from uuid import UUID

import pytest
from nexus.providers.base import PriceBar, PriceSeries
from nexus.quality.source_policy import (
    SOURCE_POLICY_ID,
    FosunMarketAdmission,
    SourcePolicy,
    select_price_series,
)
from pydantic import ValidationError

INSTRUMENT_ID = UUID("29e18e7a-78fb-5fc1-a65a-6641fbc1cc64")
DECISION_TIME = datetime(2026, 8, 21, 22, tzinfo=UTC)


def _bar(
    close: str,
    *,
    session: date = date(2026, 8, 21),
    available_at: datetime = DECISION_TIME,
    observed_at: datetime | None = None,
    received_at: datetime | None = None,
    revision: str = "1",
) -> PriceBar:
    price = Decimal(close)
    return PriceBar(
        instrument_id=INSTRUMENT_ID,
        session=session,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal("1000"),
        available_at=available_at,
        observed_at=observed_at,
        received_at=received_at or available_at,
        revision=revision,
    )


def _series(
    source: str,
    close: str,
    *,
    interval: str = "1Day",
    quality: str = "PASS",
    bars: tuple[PriceBar, ...] | None = None,
    adjustment: str = "all",
) -> PriceSeries:
    return PriceSeries(
        source=source,
        feed="authorized" if source == "fosun" else "sip",
        adjustment=adjustment,
        interval=interval,
        mode="read_only" if source == "fosun" else "delayed",
        quality=quality,
        contract_hash="a" * 64 if source == "fosun" else None,
        bars=bars or (_bar(close),),
    )


def _admission(
    *, expires_at: datetime | None = None, contract_hash: str = "a" * 64
) -> FosunMarketAdmission:
    return FosunMarketAdmission(
        provider="fosun",
        mode="read_only",
        operation="fosun_get_bars",
        market_scope="US",
        contract_hash=contract_hash,
        expires_at=expires_at or DECISION_TIME + timedelta(hours=1),
    )


def _select(
    candidates: tuple[PriceSeries, ...],
    *,
    tick_size: Decimal | None = Decimal("0.01"),
    admission: FosunMarketAdmission | bool | None = None,
):
    context = _admission() if admission is None else admission
    return select_price_series(
        candidates,
        SourcePolicy(record_id=SOURCE_POLICY_ID, instrument_tick_size=tick_size),
        DECISION_TIME,
        fosun_admission=context if isinstance(context, FosunMarketAdmission) else None,
    )


def test_request_cannot_override_published_priority() -> None:
    with pytest.raises(ValidationError):
        SourcePolicy(
            record_id=SOURCE_POLICY_ID,
            instrument_tick_size=Decimal("0.01"),
            priority=("fixture",),
        )


def test_selects_one_complete_preferred_series_without_field_merge() -> None:
    fosun = _series("fosun", "100.00")
    alpaca = _series("alpaca", "100.01")
    result = _select((alpaca, fosun))
    assert (result.quality, result.selected) == ("PASS", fosun)


def test_fosun_requires_matching_strictly_unexpired_admission() -> None:
    fosun = _series("fosun", "100")
    alpaca = _series("alpaca", "100")
    for admission in (_admission(expires_at=DECISION_TIME), _admission(contract_hash="b" * 64)):
        result = _select((fosun, alpaca), admission=admission)
        assert result.selected == alpaca
        assert result.reasons == ("FOSUN_CAPABILITY_EXPIRED",)


def test_daily_close_conflict_above_limit_fails_closed() -> None:
    result = _select((_series("fosun", "100.00"), _series("alpaca", "100.21")))
    assert (result.selected, result.quality, result.reasons) == (None, "FAIL", ("SOURCE_CONFLICT",))


def test_unknown_tick_and_adjustment_mismatch_rules() -> None:
    same = _select((_series("fosun", "100"), _series("alpaca", "100")), tick_size=None)
    mismatch = _select(
        (_series("fosun", "100"), _series("alpaca", "100", adjustment="raw")), tick_size=None
    )
    assert (same.quality, same.reasons) == ("PARTIAL", ("TICK_SIZE_UNKNOWN",))
    assert (mismatch.quality, mismatch.reasons) == ("FAIL", ("SOURCE_CONFLICT",))


def test_candidate_total_order_prefers_pass_and_is_permutation_invariant() -> None:
    candidates = (
        _series("alpaca", "100", quality="PARTIAL"),
        _series("alpaca", "101", quality="PASS"),
        _series("fixture", "99", quality="PASS"),
    )
    results = {
        _select(order, admission=False).model_dump_json() for order in permutations(candidates)
    }
    assert len(results) == 1
    assert _select(candidates, admission=False).selected == candidates[1]


def test_daily_latest_visible_revision_is_selected_under_all_bar_permutations() -> None:
    old = _bar("99", available_at=DECISION_TIME - timedelta(minutes=2), revision="1")
    latest = _bar("100", available_at=DECISION_TIME - timedelta(minutes=1), revision="2")
    future = _bar("500", available_at=DECISION_TIME + timedelta(seconds=1), revision="3")
    results = {
        _select((_series("alpaca", "0", bars=order),), admission=False).model_dump_json()
        for order in permutations((old, latest, future))
    }
    assert len(results) == 1
    selected = _select((_series("alpaca", "0", bars=(future, old, latest)),), admission=False)
    assert selected.selected is not None
    assert selected.selected.bars == (latest,)
    assert selected.reasons == ("FUTURE_DATA_EXCLUDED",)


def test_intraday_compares_only_matching_instant_latest_revision() -> None:
    instant = DECISION_TIME - timedelta(minutes=1)
    fosun = _series(
        "fosun",
        "0",
        interval="1Min",
        bars=(
            _bar("80", observed_at=instant, available_at=instant, revision="1"),
            _bar("100", observed_at=instant, available_at=DECISION_TIME, revision="2"),
        ),
    )
    mismatch = _series(
        "alpaca",
        "0",
        interval="1Min",
        bars=(_bar("200", observed_at=instant - timedelta(minutes=1)),),
    )
    matching = _series("alpaca", "0", interval="1Min", bars=(_bar("101", observed_at=instant),))
    assert _select((fosun, mismatch)).reasons == ()
    assert _select((fosun, matching)).reasons == ("INTRADAY_DIVERGENCE_ALERT",)


def test_future_only_and_unapproved_sources_fail_closed() -> None:
    future = _series(
        "alpaca", "100", bars=(_bar("100", available_at=DECISION_TIME + timedelta(microseconds=1)),)
    )
    assert _select((future,), admission=False).reasons == ("NO_POINT_IN_TIME_DATA",)
    assert _select((_series("other", "100"),), admission=False).reasons == ("NO_ELIGIBLE_SOURCE",)
