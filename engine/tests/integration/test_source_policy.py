from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from itertools import permutations
from uuid import UUID

import pytest
from nexus.persistence.models.data import ProviderCapability
from nexus.persistence.models.runtime import IntegrationStatus
from nexus.persistence.session import session_factory
from nexus.providers.base import PriceBar, PriceSeries
from nexus.quality.source_policy import (
    FosunMarketAdmission,
    SourcePolicy,
    load_fosun_market_admission,
    load_source_policy_v1,
    select_price_series,
)
from nexus.settings import EngineSettings

INSTRUMENT_ID = UUID("29e18e7a-78fb-5fc1-a65a-6641fbc1cc64")
DECISION_TIME = datetime(2026, 8, 21, 22, tzinfo=UTC)


def _bar(
    close: str,
    *,
    available_at: datetime = DECISION_TIME,
    observed_at: datetime | None = None,
    revision: str = "1",
) -> PriceBar:
    price = Decimal(close)
    return PriceBar(
        instrument_id=INSTRUMENT_ID,
        session=date(2026, 8, 21),
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal("1000"),
        available_at=available_at,
        observed_at=observed_at,
        received_at=available_at,
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
    contract_hash: str | None = None,
) -> PriceSeries:
    return PriceSeries(
        source=source,
        feed="authorized" if source == "fosun" else "sip",
        adjustment=adjustment,
        interval=interval,
        mode="read_only" if source == "fosun" else "delayed",
        quality=quality,
        contract_hash=contract_hash if source == "fosun" else None,
        bars=bars or (_bar(close),),
    )


def _select(
    candidates: tuple[PriceSeries, ...],
    policy: SourcePolicy,
    admission: FosunMarketAdmission | None = None,
):
    return select_price_series(candidates, policy, DECISION_TIME, fosun_admission=admission)


@pytest.mark.asyncio
async def test_selection_contract_uses_only_database_loaded_policy_and_admission(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        session.add(
            ProviderCapability(
                provider="fosun",
                capability="READ_ONLY",
                contract_hash="a" * 64,
                expires_at=DECISION_TIME + timedelta(hours=1),
                schema_version="1",
                payload={"operation": "fosun_get_bars", "market_scope": "US"},
                created_at=DECISION_TIME - timedelta(hours=2),
            )
        )
        session.add(
            IntegrationStatus(
                provider="fosun",
                status="read_only",
                admitted_contract_hash="a" * 64,
                mode_changed_at=DECISION_TIME - timedelta(hours=1),
            )
        )
        await session.commit()
        policy = await load_source_policy_v1(session, instrument_tick_size=Decimal("0.01"))
        unknown_tick_policy = await load_source_policy_v1(session, instrument_tick_size=None)
        admission = await load_fosun_market_admission(
            session,
            decision_time=DECISION_TIME,
            operation="fosun_get_bars",
            market_scope="US",
        )

    assert admission is not None
    fosun = _series("fosun", "100.00", contract_hash="a" * 64)
    alpaca = _series("alpaca", "100.01")
    preferred = _select((alpaca, fosun), policy, admission)
    assert (preferred.quality, preferred.selected) == ("PASS", fosun)

    conflict = _select((fosun, _series("alpaca", "100.21")), policy, admission)
    assert (conflict.selected, conflict.quality, conflict.reasons) == (
        None,
        "FAIL",
        ("SOURCE_CONFLICT",),
    )
    unknown_tick = _select((fosun, _series("alpaca", "100.00")), unknown_tick_policy, admission)
    adjustment_mismatch = _select(
        (fosun, _series("alpaca", "100.00", adjustment="raw")),
        unknown_tick_policy,
        admission,
    )
    assert (unknown_tick.quality, unknown_tick.reasons) == (
        "PARTIAL",
        ("TICK_SIZE_UNKNOWN",),
    )
    assert adjustment_mismatch.reasons == ("SOURCE_CONFLICT",)

    candidates = (
        _series("alpaca", "100", quality="PARTIAL"),
        _series("alpaca", "101", quality="PASS"),
        _series("fixture", "99", quality="PASS"),
    )
    candidate_results = {
        _select(order, policy).model_dump_json() for order in permutations(candidates)
    }
    assert len(candidate_results) == 1
    assert _select(candidates, policy).selected == candidates[1]

    old = _bar("99", available_at=DECISION_TIME - timedelta(minutes=2), revision="1")
    latest = _bar("100", available_at=DECISION_TIME - timedelta(minutes=1), revision="2")
    future = _bar("500", available_at=DECISION_TIME + timedelta(seconds=1), revision="3")
    revision_results = {
        _select((_series("alpaca", "0", bars=order),), policy).model_dump_json()
        for order in permutations((old, latest, future))
    }
    assert len(revision_results) == 1
    selected_revision = _select((_series("alpaca", "0", bars=(future, old, latest)),), policy)
    assert selected_revision.selected is not None
    assert selected_revision.selected.bars == (latest,)

    instant = DECISION_TIME - timedelta(minutes=1)
    intraday_fosun = _series(
        "fosun",
        "0",
        interval="1Min",
        contract_hash="a" * 64,
        bars=(
            _bar("80", observed_at=instant, available_at=instant, revision="1"),
            _bar("100", observed_at=instant, revision="2"),
        ),
    )
    mismatched_instant = _series(
        "alpaca",
        "0",
        interval="1Min",
        bars=(_bar("200", observed_at=instant - timedelta(minutes=1)),),
    )
    matching_instant = _series(
        "alpaca", "0", interval="1Min", bars=(_bar("101", observed_at=instant),)
    )
    assert _select((intraday_fosun, mismatched_instant), policy, admission).reasons == ()
    assert _select((intraday_fosun, matching_instant), policy, admission).reasons == (
        "INTRADAY_DIVERGENCE_ALERT",
    )

    future_only = _series(
        "alpaca",
        "100",
        bars=(
            _bar(
                "100",
                available_at=DECISION_TIME + timedelta(microseconds=1),
            ),
        ),
    )
    assert _select((future_only,), policy).reasons == ("NO_POINT_IN_TIME_DATA",)
    assert _select((_series("other", "100"),), policy).reasons == ("NO_ELIGIBLE_SOURCE",)
