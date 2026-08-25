"""Property checks for point-in-time backtest boundaries."""

import hashlib
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from uuid import UUID

from hypothesis import given
from hypothesis import strategies as st
from nexus.backtest.costs import CostModel
from nexus.backtest.engine import (
    AuthorityCollection,
    BacktestBar,
    BacktestDataAuthority,
    BacktestProtocol,
    BenchmarkInstrumentAuthority,
    EqualWeightBenchmarkAuthority,
    ExchangeCalendarAuthority,
    PointInTimeDataset,
    PriceSeriesAuthority,
    PriceSourceSelectionAuthority,
    SectorClassification,
    UniverseSnapshot,
    authority_collection_content_hash,
    exchange_calendar_content_hash,
    run_backtest,
)
from nexus.strategy.dsl import baseline_feature_registry, baseline_v1_document, compile_strategy


def _hash(value: object) -> str:
    return hashlib.sha256(str(value).encode()).hexdigest()


def _history(end: date) -> tuple[date, ...]:
    values: list[date] = []
    current = end - timedelta(days=1)
    while len(values) < 200:
        if current.weekday() < 5:
            values.append(current)
        current -= timedelta(days=1)
    return tuple(reversed(values))


def _decision_times(sessions: tuple[date, ...]) -> dict[date, datetime]:
    return {session: datetime.combine(session, time(21, 30), tzinfo=UTC) for session in sessions}


def _bar(instrument: UUID, session: date, price: Decimal, available_at: datetime) -> BacktestBar:
    return BacktestBar(
        instrument_id=instrument,
        session=session,
        open=price,
        high=price,
        low=price,
        close=price,
        volume=Decimal("7000000"),
        observed_at=min(available_at, datetime.combine(session, time(21), tzinfo=UTC)),
        available_at=available_at,
        received_at=available_at,
    )


def _dataset(bars: tuple[BacktestBar, ...], sessions: tuple[date, ...]) -> PointInTimeDataset:
    instrument = UUID(int=1)
    first = sessions[0]
    last = sessions[-1]
    authorized_bars = tuple(bar for bar in bars if bar.session in set(sessions))
    return PointInTimeDataset(
        bars=bars,
        universe_snapshots=(
            UniverseSnapshot(
                authority_id=UUID(int=201),
                version="us_liquid_v1",
                content_hash=_hash("universe"),
                source="fixture",
                effective_session=first,
                published_at=datetime.combine(first, time(20), tzinfo=UTC),
                members=(instrument,),
            ),
        ),
        asset_types={instrument: "stock"},
        risk_buckets={instrument: "EQUITY"},
        sector_classifications=(
            SectorClassification(
                authority_id=UUID(int=202),
                instrument_id=instrument,
                sector="information_technology",
                effective_session=first,
                published_at=datetime.combine(first, time(20), tzinfo=UTC),
                source_uri="memory://sector",
                content_hash=_hash("sector"),
                weight=Decimal(1),
                synthetic=True,
                reviewed=False,
            ),
        ),
        authority=BacktestDataAuthority(
            authority_bundle_id=UUID(int=100),
            data_snapshot_id=UUID(int=101),
            source_policy_id=UUID(int=102),
            cost_policy_id=UUID(int=103),
            cost_policy_version="backtest_cost_policy_v1",
            exchange_calendar=ExchangeCalendarAuthority(
                authority_id=UUID(int=107),
                version="xnys_test_v1",
                source="exchange_calendars",
                range_start=first,
                range_end=last,
                query_parameters={"exchange": "XNYS"},
                content_hash=exchange_calendar_content_hash(sessions, _decision_times(sessions)),
                sessions=sessions,
                decision_times=_decision_times(sessions),
            ),
            universe_collection=AuthorityCollection(
                authority_id=UUID(int=108),
                kind="universe",
                source="fixture",
                range_start=first,
                range_end=last,
                query_parameters={"key": "us_liquid"},
                member_ids=(UUID(int=201),),
                content_hash=authority_collection_content_hash(
                    kind="universe",
                    source="fixture",
                    range_start=first,
                    range_end=last,
                    query_parameters={"key": "us_liquid"},
                    member_ids=(UUID(int=201),),
                ),
            ),
            sector_collection=AuthorityCollection(
                authority_id=UUID(int=109),
                kind="sector",
                source="fixture",
                range_start=first,
                range_end=last,
                query_parameters={
                    "taxonomy": "NEXUS_SECTOR_V1",
                    "synthetic": True,
                    "reviewed": False,
                },
                member_ids=(UUID(int=202),),
                content_hash=authority_collection_content_hash(
                    kind="sector",
                    source="fixture",
                    range_start=first,
                    range_end=last,
                    query_parameters={
                        "taxonomy": "NEXUS_SECTOR_V1",
                        "synthetic": True,
                        "reviewed": False,
                    },
                    member_ids=(UUID(int=202),),
                ),
            ),
            corporate_action_collection=AuthorityCollection(
                authority_id=UUID(int=110),
                kind="corporate_action",
                source="fixture",
                range_start=first,
                range_end=last,
                query_parameters={
                    "provider_mapping_version": "corporate_action_mapping_v1",
                    "status": "complete",
                },
                member_ids=(),
                content_hash=authority_collection_content_hash(
                    kind="corporate_action",
                    source="fixture",
                    range_start=first,
                    range_end=last,
                    query_parameters={
                        "provider_mapping_version": "corporate_action_mapping_v1",
                        "status": "complete",
                    },
                    member_ids=(),
                ),
            ),
            price_series=tuple(
                PriceSeriesAuthority(
                    price_bar_set_authority_id=UUID(int=203 if adjustment == "raw" else 205),
                    price_series_manifest_id=UUID(int=204 if adjustment == "raw" else 206),
                    instrument_id=instrument,
                    source="fixture",
                    feed="test",
                    delivery_mode="fixture",
                    adjustment=adjustment,
                    manifest_hash=_hash((adjustment, "manifest")),
                    content_hash=_hash((adjustment, "bars")),
                    start_at=min(bar.observed_at for bar in authorized_bars),
                    end_at=max(bar.observed_at for bar in authorized_bars),
                    row_count=len(authorized_bars),
                )
                for adjustment in ("raw", "all")
            ),
            source_selections=tuple(
                PriceSourceSelectionAuthority(
                    instrument_id=instrument,
                    adjustment=adjustment,
                    request_start_session=sessions[0],
                    request_end_session=last,
                    decision_time=_decision_times(sessions)[last],
                    tick_size=Decimal("0.01"),
                    candidate_price_bar_set_authority_ids=(
                        UUID(int=203 if adjustment == "raw" else 205),
                    ),
                    selected_price_bar_set_authority_id=UUID(
                        int=203 if adjustment == "raw" else 205
                    ),
                )
                for adjustment in ("raw", "all")
            ),
            spy_benchmark=BenchmarkInstrumentAuthority(
                alias_id=UUID(int=91_005),
                instrument_id=instrument,
                valid_from=sessions[0],
                created_at=datetime.combine(sessions[0], time(20), tzinfo=UTC),
            ),
            equal_weight_benchmark=EqualWeightBenchmarkAuthority(
                universe_snapshot_id=UUID(int=201),
                universe_version="us_liquid_v1",
                content_hash=_hash("universe"),
                source="fixture",
                effective_session=first,
                published_at=datetime.combine(first, time(20), tzinfo=UTC),
                instrument_ids=(instrument,),
            ),
        ),
        adjusted_bars=tuple(bar.model_copy() for bar in bars),
        spy_instrument_id=instrument,
    )


@given(future_price=st.integers(min_value=1, max_value=1_000_000))
def test_unavailable_rows_cannot_add_sessions_or_change_hashes_and_results(
    future_price: int,
) -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    history_sessions = _history(friday)
    sessions = (*history_sessions, friday, monday)
    history = tuple(
        _bar(
            instrument,
            session,
            Decimal("8"),
            datetime.combine(session, time(21), tzinfo=UTC),
        )
        for session in history_sessions
    )
    visible = _bar(instrument, friday, Decimal("10"), datetime(2021, 1, 8, 20, tzinfo=UTC))
    fill = _bar(instrument, monday, Decimal("10"), datetime(2021, 1, 11, 21, tzinfo=UTC))
    phantom = _bar(
        instrument,
        date(2021, 1, 12),
        Decimal(future_price),
        datetime(2021, 1, 13, tzinfo=UTC),
    )
    protocol = BacktestProtocol(
        start=friday,
        end=monday,
        initial_nav=Decimal("10000"),
        cost_model=CostModel.zero(),
        walk_forward=False,
    )
    strategy = compile_strategy(baseline_v1_document(), baseline_feature_registry())

    without_future = run_backtest(strategy, _dataset((*history, visible, fill), sessions), protocol)
    with_future = run_backtest(
        strategy, _dataset((*history, visible, fill, phantom), sessions), protocol
    )

    assert with_future == without_future
