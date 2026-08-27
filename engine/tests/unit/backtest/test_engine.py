"""Deterministic point-in-time backtest engine behavior."""

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from time import monotonic
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest
from nexus.backtest.costs import CostModel
from nexus.backtest.engine import (
    AuthorityCollection,
    BacktestBar,
    BacktestDataAuthority,
    BacktestProtocol,
    BenchmarkInstrumentAuthority,
    CorporateActionEvent,
    DelistingEvent,
    EqualWeightBenchmarkAuthority,
    ExchangeCalendarAuthority,
    NavPoint,
    PointInTimeDataset,
    PriceSeriesAuthority,
    PriceSourceSelectionAuthority,
    SectorClassification,
    UniverseSnapshot,
    UnresolvedDelistingEvent,
    _engine_code_hash,
    _window_metrics,
    authority_collection_content_hash,
    exchange_calendar_content_hash,
    run_backtest,
    run_challenger_backtest_v1,
)
from nexus.evolution.candidate import apply_candidate_delta, generate_candidate_deltas
from nexus.evolution.models import StrategyExperimentProposalV1
from nexus.evolution.policies import candidate_budget_policy_v1, evolution_search_space_v1
from nexus.market.universe import EQUITY_SECTOR_CODES
from nexus.strategy.dsl import (
    baseline_feature_registry,
    baseline_v1_document,
    compile_strategy,
    evolution_feature_registry_v1,
)

DATA_SNAPSHOT_ID = UUID("0198d500-1000-7000-8000-000000001602")
SOURCE_POLICY_ID = UUID("0198d500-1000-7000-8000-000000001603")
COST_POLICY_ID = UUID("0198d500-1000-7000-8000-000000001604")


def _digest(value: object) -> str:
    return hashlib.sha256(str(value).encode()).hexdigest()


def _decision_times(sessions: tuple[date, ...]) -> dict[date, datetime]:
    return {session: datetime.combine(session, time(21, 30), tzinfo=UTC) for session in sessions}


def _compiled_baseline():
    return compile_strategy(baseline_v1_document(), baseline_feature_registry())


def _business_days(start: date, end: date) -> tuple[date, ...]:
    sessions: list[date] = []
    current = start
    while current <= end:
        if current.weekday() < 5:
            sessions.append(current)
        current += timedelta(days=1)
    return tuple(sessions)


def _history_sessions(end: date, count: int = 200) -> tuple[date, ...]:
    sessions: list[date] = []
    current = end - timedelta(days=1)
    while len(sessions) < count:
        if current.weekday() < 5:
            sessions.append(current)
        current -= timedelta(days=1)
    return tuple(reversed(sessions))


def _bar(
    instrument: UUID,
    session: date,
    price: str,
    *,
    available_days: int = 0,
) -> BacktestBar:
    observed = datetime.combine(session, time(21), tzinfo=UTC)
    return BacktestBar(
        instrument_id=instrument,
        session=session,
        open=Decimal(price),
        high=Decimal(price),
        low=Decimal(price),
        close=Decimal(price),
        volume=Decimal("7000000"),
        observed_at=observed,
        available_at=observed + timedelta(days=available_days),
        received_at=observed + timedelta(days=available_days),
    )


def _dataset(
    *,
    bars: tuple[BacktestBar, ...],
    sessions: tuple[date, ...],
    universe_members: tuple[UUID, ...],
    spy: UUID | None = None,
    equal_weight: tuple[UUID, ...] | None = None,
    asset_types: dict[UUID, str] | None = None,
    corporate_actions: tuple[CorporateActionEvent, ...] = (),
    delistings: tuple[DelistingEvent, ...] = (),
    unresolved_delistings: tuple[UnresolvedDelistingEvent, ...] = (),
    adjusted_prices: Mapping[tuple[UUID, date], str] | None = None,
) -> PointInTimeDataset:
    all_ids = {bar.instrument_id for bar in bars} | set(universe_members)
    spy = spy or universe_members[0]
    equal_weight = equal_weight or (spy,)
    all_ids |= {spy, *equal_weight}
    resolved_asset_types = {instrument_id: "stock" for instrument_id in all_ids}
    resolved_asset_types.update(asset_types or {})
    risk_buckets = {
        instrument_id: (
            "US_TREASURY" if resolved_asset_types[instrument_id] == "non_equity_etf" else "EQUITY"
        )
        for instrument_id in all_ids
    }
    first_session = min(sessions)
    last_session = max(sessions)
    universe_ids = (UUID(int=40_000),)
    sector_ids = tuple(
        UUID(int=50_000 + instrument_id.int)
        for instrument_id in sorted(universe_members, key=lambda value: value.int)
        if resolved_asset_types[instrument_id] != "non_equity_etf"
    )
    event_ids = tuple(
        action.authority_id for action in (*corporate_actions, *delistings, *unresolved_delistings)
    )

    def collection(
        kind: str,
        authority_id: UUID,
        member_ids: tuple[UUID, ...],
        query_parameters: dict[str, object],
    ) -> AuthorityCollection:
        return AuthorityCollection(
            authority_id=authority_id,
            kind=kind,
            source="fixture",
            range_start=first_session,
            range_end=last_session,
            query_parameters=query_parameters,
            member_ids=member_ids,
            content_hash=authority_collection_content_hash(
                kind=kind,
                source="fixture",
                range_start=first_session,
                range_end=last_session,
                query_parameters=query_parameters,
                member_ids=member_ids,
            ),
        )

    authority = BacktestDataAuthority(
        authority_bundle_id=UUID(int=90_000),
        data_snapshot_id=DATA_SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=COST_POLICY_ID,
        cost_policy_version="backtest_cost_policy_v1",
        exchange_calendar=ExchangeCalendarAuthority(
            authority_id=UUID(int=90_001),
            version="xnys_test_v1",
            source="exchange_calendars",
            range_start=first_session,
            range_end=last_session,
            query_parameters={"exchange": "XNYS"},
            content_hash=exchange_calendar_content_hash(sessions, _decision_times(sessions)),
            sessions=sessions,
            decision_times=_decision_times(sessions),
        ),
        universe_collection=collection(
            "universe",
            UUID(int=90_002),
            universe_ids,
            {"key": "us_liquid"},
        ),
        sector_collection=collection(
            "sector",
            UUID(int=90_003),
            sector_ids,
            {"taxonomy": "NEXUS_SECTOR_V1", "synthetic": True, "reviewed": False},
        ),
        corporate_action_collection=collection(
            "corporate_action",
            UUID(int=90_004),
            event_ids,
            {"provider_mapping_version": "corporate_action_mapping_v1", "status": "complete"},
        ),
        price_series=tuple(
            PriceSeriesAuthority(
                price_bar_set_authority_id=UUID(
                    int=(10_000 if adjustment == "raw" else 30_000) + instrument_id.int
                ),
                price_series_manifest_id=UUID(
                    int=(20_000 if adjustment == "raw" else 40_000) + instrument_id.int
                ),
                instrument_id=instrument_id,
                source="fixture",
                feed="test",
                delivery_mode="fixture",
                adjustment=adjustment,
                manifest_hash=_digest((instrument_id, adjustment, "manifest")),
                content_hash=_digest((instrument_id, adjustment, "bars")),
                start_at=min(bar.observed_at for bar in bars if bar.instrument_id == instrument_id),
                end_at=max(bar.observed_at for bar in bars if bar.instrument_id == instrument_id),
                row_count=sum(bar.instrument_id == instrument_id for bar in bars),
            )
            for instrument_id in sorted(all_ids, key=lambda value: value.int)
            for adjustment in ("raw", "all")
        ),
        source_selections=tuple(
            PriceSourceSelectionAuthority(
                instrument_id=instrument_id,
                adjustment=adjustment,
                request_start_session=first_session,
                request_end_session=last_session,
                decision_time=_decision_times(sessions)[last_session],
                tick_size=Decimal("0.01"),
                candidate_price_bar_set_authority_ids=(
                    UUID(int=(10_000 if adjustment == "raw" else 30_000) + instrument_id.int),
                ),
                selected_price_bar_set_authority_id=UUID(
                    int=(10_000 if adjustment == "raw" else 30_000) + instrument_id.int
                ),
            )
            for instrument_id in sorted(all_ids, key=lambda value: value.int)
            for adjustment in ("raw", "all")
        ),
        spy_benchmark=BenchmarkInstrumentAuthority(
            alias_id=UUID(int=90_005),
            instrument_id=spy,
            valid_from=first_session,
            created_at=datetime.combine(first_session, time(20), tzinfo=UTC),
        ),
        equal_weight_benchmark=EqualWeightBenchmarkAuthority(
            universe_snapshot_id=UUID(int=40_000),
            universe_version="us_liquid_v1",
            content_hash=_digest(universe_members),
            source="fixture",
            effective_session=first_session,
            published_at=datetime.combine(first_session, time(20), tzinfo=UTC),
            instrument_ids=equal_weight,
        ),
    )
    return PointInTimeDataset(
        bars=bars,
        adjusted_bars=tuple(
            bar.model_copy(
                update={
                    "open": Decimal(
                        (adjusted_prices or {}).get((bar.instrument_id, bar.session), bar.open)
                    ),
                    "high": Decimal(
                        (adjusted_prices or {}).get((bar.instrument_id, bar.session), bar.high)
                    ),
                    "low": Decimal(
                        (adjusted_prices or {}).get((bar.instrument_id, bar.session), bar.low)
                    ),
                    "close": Decimal(
                        (adjusted_prices or {}).get((bar.instrument_id, bar.session), bar.close)
                    ),
                }
            )
            for bar in bars
        ),
        universe_snapshots=(
            UniverseSnapshot(
                authority_id=UUID(int=40_000),
                version="us_liquid_v1",
                content_hash=_digest(universe_members),
                source="fixture",
                effective_session=first_session,
                published_at=datetime.combine(first_session, time(20), tzinfo=UTC),
                members=universe_members,
            ),
        ),
        asset_types=resolved_asset_types,
        risk_buckets=risk_buckets,
        sector_classifications=tuple(
            SectorClassification(
                authority_id=UUID(int=50_000 + instrument_id.int),
                instrument_id=instrument_id,
                sector=EQUITY_SECTOR_CODES[instrument_id.int % len(EQUITY_SECTOR_CODES)],
                effective_session=first_session,
                published_at=datetime.combine(first_session, time(20), tzinfo=UTC),
                source_uri="memory://sector",
                content_hash=_digest((instrument_id, "sector")),
                weight=Decimal(1),
                synthetic=True,
                reviewed=False,
            )
            for instrument_id in sorted(universe_members, key=lambda value: value.int)
            if resolved_asset_types[instrument_id] != "non_equity_etf"
        ),
        authority=authority,
        spy_instrument_id=spy,
        corporate_actions=corporate_actions,
        delistings=delistings,
        unresolved_delistings=unresolved_delistings,
    )


def _protocol(start: date, end: date, cost_model: CostModel | None = None) -> BacktestProtocol:
    return BacktestProtocol(
        start=start,
        end=end,
        initial_nav=Decimal("10000"),
        cost_model=cost_model or CostModel.zero(),
        walk_forward=False,
    )


def test_backtest_fills_on_next_calendar_session_and_applies_exact_decimal_costs() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    history = _history_sessions(friday)
    sessions = (*history, friday, monday)
    dataset = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (_bar(instrument, friday, "10"), _bar(instrument, monday, "10")),
        sessions=sessions,
        universe_members=(instrument,),
    )
    protocol = _protocol(
        friday,
        monday,
        CostModel(
            commission_bps=Decimal("1"),
            spread_bps=Decimal("2"),
            slippage_bps=Decimal("2"),
        ),
    )

    result = run_backtest(_compiled_baseline(), dataset, protocol)

    assert len(result.fills) == 1
    assert result.fills[0].signal_session == friday
    assert result.fills[0].fill_session == monday
    assert result.fills[0].notional == Decimal("1249.921879882507")
    assert result.fills[0].cost == Decimal("0.624960939941")
    assert result.nav[-1].nav == Decimal("9999.375039060059")
    assert result.order_outcomes[0].status == "filled"
    assert result.strategy_data_complete is True
    semantic_payload = result.model_dump(
        mode="json",
        exclude={"code_hash", "result_hash"},
    )
    golden = json.loads(
        (
            Path(__file__).resolve().parents[2]
            / "fixtures/backtest/baseline_v1_semantic_golden.json"
        ).read_text()
    )
    original_identity = {field: golden.pop(field) for field in ("code_hash", "result_hash")}
    assert semantic_payload == golden
    assert result.code_hash != original_identity["code_hash"]
    assert result.result_hash != original_identity["result_hash"]


def test_challenger_entry_executes_a_verified_candidate_and_stays_non_promotable() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    history = _history_sessions(friday)
    dataset = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (_bar(instrument, friday, "10"), _bar(instrument, monday, "10")),
        sessions=(*history, friday, monday),
        universe_members=(instrument,),
    )
    proposal = StrategyExperimentProposalV1.model_validate(
        {
            "schema_version": "strategy_experiment_proposal_v1",
            "parent_strategy_definition_id": "019d0000-0000-7000-8000-000000000001",
            "champion_generation": 1,
            "research_lineage_id": "019d0000-0000-7000-8000-000000000002",
            "hypothesis": "Registered weight variation may improve robustness.",
            "expected_mechanism": "The frozen validation protocol measures the effect.",
            "invalidation_conditions": ["The paired validation comparison is not positive."],
            "evidence_refs": ["019d0000-0000-7000-8000-000000000003"],
            "mutation_axes": [{"axis": "target_weight", "values": ["0.10"]}],
        }
    )
    parent = baseline_v1_document()
    delta = generate_candidate_deltas(proposal, parent, candidate_budget_policy_v1())[0]
    document = apply_candidate_delta(parent, delta, evolution_search_space_v1())
    challenger = compile_strategy(document, evolution_feature_registry_v1())

    result = run_challenger_backtest_v1(
        challenger,
        dataset,
        _protocol(friday, monday),
        experiment_attempt_count=7,
    )

    assert result.strategy_version == challenger.strategy_version
    assert result.expression_hash == challenger.expression_hash
    assert result.fills[0].notional == Decimal("1000.000000000000")
    assert result.promotable is False
    assert result.historical_agent_overlay is False


def test_nonzero_cost_rebalance_preserves_post_cost_target_weights_and_cash_floor() -> None:
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    history = _history_sessions(friday)
    instruments = tuple(UUID(int=value) for value in range(1, 7))
    dataset = _dataset(
        bars=tuple(
            _bar(instrument, session, "8") for instrument in instruments for session in history
        )
        + tuple(
            _bar(instrument, session, "10")
            for instrument in instruments
            for session in (friday, monday)
        ),
        sessions=(*history, friday, monday),
        universe_members=instruments,
        equal_weight=instruments,
    )
    result = run_backtest(
        _compiled_baseline(),
        dataset,
        _protocol(
            friday,
            monday,
            CostModel(
                commission_bps=Decimal("5"),
                spread_bps=Decimal("5"),
                slippage_bps=Decimal("5"),
            ),
        ),
    )

    final = result.nav[-1]
    assert len(result.fills) == 6
    assert final.cash / final.nav >= Decimal("0.25")
    assert all(
        abs(fill.notional / final.nav - Decimal("0.125")) <= Decimal("0.000000000001")
        for fill in result.fills
    )


def test_protocol_canonicalizes_initial_nav_before_hashing_or_persistence() -> None:
    protocol = BacktestProtocol(
        start=date(2021, 1, 8),
        end=date(2021, 1, 11),
        initial_nav=Decimal("10000.1234567890129"),
        cost_model=CostModel.zero(),
        walk_forward=False,
    )

    assert protocol.initial_nav == Decimal("10000.123456789013")


def test_dataset_rejects_unbound_extra_asset_metadata() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    history = _history_sessions(friday)
    base = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (_bar(instrument, friday, "10"), _bar(instrument, monday, "10")),
        sessions=(*history, friday, monday),
        universe_members=(instrument,),
    )

    with pytest.raises(ValueError, match="asset metadata must be complete"):
        PointInTimeDataset(
            bars=base.bars,
            adjusted_bars=base.adjusted_bars,
            universe_snapshots=base.universe_snapshots,
            asset_types={**base.asset_types, uuid4(): "stock"},
            risk_buckets=base.risk_buckets,
            sector_classifications=base.sector_classifications,
            authority=base.authority,
            spy_instrument_id=base.spy_instrument_id,
        )


def test_oos_metrics_include_the_return_from_opening_nav_to_the_first_oos_session() -> None:
    metrics = _window_metrics(
        date(2021, 1, 2),
        date(2021, 1, 4),
        nav_points=(
            NavPoint(session=date(2021, 1, 1), nav=Decimal("100"), cash=Decimal("100")),
            NavPoint(session=date(2021, 1, 2), nav=Decimal("50"), cash=Decimal("50")),
            NavPoint(session=date(2021, 1, 3), nav=Decimal("50"), cash=Decimal("50")),
        ),
        fills=(),
        initial_nav=Decimal("100"),
        experiment_attempt_count=1,
    )

    assert metrics.total_return == Decimal("-0.500000000000")
    assert metrics.max_drawdown == Decimal("-0.500000000000")
    assert metrics.expected_shortfall_95 == Decimal("-0.500000000000")
    assert metrics.annualized_volatility > 0


def test_future_adjustment_scale_cannot_change_the_raw_five_dollar_filter() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    history = _history_sessions(friday)
    bars = tuple(_bar(instrument, session, "8") for session in history) + (
        _bar(instrument, friday, "10"),
        _bar(instrument, monday, "10"),
    )
    adjusted_prices = {(instrument, bar.session): str(bar.close / Decimal(10)) for bar in bars}
    dataset = _dataset(
        bars=bars,
        sessions=(*history, friday, monday),
        universe_members=(instrument,),
        adjusted_prices=adjusted_prices,
    )

    result = run_backtest(_compiled_baseline(), dataset, _protocol(friday, monday))

    assert len(result.fills) == 1
    assert result.fills[0].instrument_id == instrument
    assert result.fills[0].price == Decimal("10.000000000000")


def test_split_on_fill_day_only_applies_to_preexisting_holdings() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    history = _history_sessions(friday)
    dataset = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (
            _bar(instrument, friday, "10"),
            _bar(instrument, monday, "5"),
        ),
        sessions=(*history, friday, monday),
        universe_members=(instrument,),
        corporate_actions=(
            CorporateActionEvent(
                authority_id=UUID(int=60_001),
                instrument_id=instrument,
                source="fixture",
                provider_event_id="split-on-fill",
                provider_action_type="stock_split",
                status="complete",
                provider_ex_date=monday,
                session=monday,
                action_type="split",
                value=Decimal("2"),
                available_at=datetime(2021, 1, 8, tzinfo=UTC),
                evidence_hash=_digest("split"),
            ),
        ),
        adjusted_prices={(instrument, monday): "10"},
    )

    result = run_backtest(_compiled_baseline(), dataset, _protocol(friday, monday))

    assert result.fills[0].quantity == Decimal("250.000000000000")
    assert result.nav[-1].nav == Decimal("10000.000000000000")


def test_backtest_applies_preexisting_splits_dividends_and_delisting_recovery() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    tuesday = date(2021, 1, 12)
    wednesday = date(2021, 1, 13)
    history = _history_sessions(friday)
    dataset = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (
            _bar(instrument, friday, "10"),
            _bar(instrument, monday, "10"),
            _bar(instrument, tuesday, "5"),
            _bar(instrument, wednesday, "5"),
        ),
        sessions=(*history, friday, monday, tuesday, wednesday),
        universe_members=(instrument,),
        corporate_actions=(
            CorporateActionEvent(
                authority_id=UUID(int=60_002),
                instrument_id=instrument,
                source="fixture",
                provider_event_id="split-preexisting",
                provider_action_type="stock_split",
                status="complete",
                provider_ex_date=tuesday,
                session=tuesday,
                action_type="split",
                value=Decimal("2"),
                available_at=datetime(2021, 1, 8, tzinfo=UTC),
                evidence_hash=_digest("split"),
            ),
            CorporateActionEvent(
                authority_id=UUID(int=60_003),
                instrument_id=instrument,
                source="fixture",
                provider_event_id="dividend-preexisting",
                provider_action_type="cash_dividend",
                status="complete",
                provider_ex_date=tuesday,
                provider_payable_date=tuesday,
                session=tuesday,
                payable_session=tuesday,
                action_type="dividend",
                value=Decimal("0.10"),
                available_at=datetime(2021, 1, 8, tzinfo=UTC),
                evidence_hash=_digest("dividend"),
            ),
        ),
        delistings=(
            DelistingEvent(
                authority_id=UUID(int=70_001),
                instrument_id=instrument,
                source="fixture",
                provider_event_id="delisting-after-fill",
                provider_action_type="merger_cash",
                recovery_method="cash_recovery",
                status="complete",
                provider_process_date=wednesday,
                session=wednesday,
                recovery_price=Decimal("4"),
                available_at=datetime(2021, 1, 8, tzinfo=UTC),
                evidence_hash=_digest("delisting"),
            ),
        ),
        adjusted_prices={(instrument, tuesday): "10", (instrument, wednesday): "10"},
    )

    result = run_backtest(_compiled_baseline(), dataset, _protocol(friday, wednesday))

    assert result.nav[-1].nav == Decimal("9775.000000000000")
    assert result.delisting_count == 1
    assert result.attribution.realized_pnl == Decimal("-225.000000000000")
    assert result.attribution.reconciled is True


def test_dividend_is_receivable_on_ex_date_and_investable_only_on_payable_date() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    tuesday = date(2021, 1, 12)
    wednesday = date(2021, 1, 13)
    history = _history_sessions(friday)
    dataset = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (
            _bar(instrument, friday, "10"),
            _bar(instrument, monday, "10"),
            _bar(instrument, tuesday, "9.9"),
            _bar(instrument, wednesday, "9.9"),
        ),
        sessions=(*history, friday, monday, tuesday, wednesday),
        universe_members=(instrument,),
        corporate_actions=(
            CorporateActionEvent(
                authority_id=UUID(int=60_004),
                instrument_id=instrument,
                source="fixture",
                provider_event_id="dividend-delayed-payment",
                provider_action_type="cash_dividend",
                status="complete",
                provider_ex_date=tuesday,
                provider_record_date=tuesday,
                provider_payable_date=wednesday,
                session=tuesday,
                payable_session=wednesday,
                action_type="dividend",
                value=Decimal("0.10"),
                available_at=datetime(2021, 1, 8, tzinfo=UTC),
                evidence_hash=_digest("dividend-delayed-payment"),
            ),
        ),
        adjusted_prices={(instrument, tuesday): "10", (instrument, wednesday): "10"},
    )

    result = run_backtest(_compiled_baseline(), dataset, _protocol(friday, wednesday))

    by_session = {point.session: point for point in result.nav}
    assert by_session[tuesday].cash == Decimal("8750.000000000000")
    assert by_session[tuesday].nav == Decimal("10000.000000000000")
    assert by_session[wednesday].cash == Decimal("8762.500000000000")
    assert by_session[wednesday].nav == Decimal("10000.000000000000")


def test_dividend_payable_after_protocol_end_remains_in_terminal_receivables() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    tuesday = date(2021, 1, 12)
    wednesday = date(2021, 1, 13)
    history = _history_sessions(friday)
    dataset = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (
            _bar(instrument, friday, "10"),
            _bar(instrument, monday, "10"),
            _bar(instrument, tuesday, "9.9"),
            _bar(instrument, wednesday, "9.9"),
        ),
        sessions=(*history, friday, monday, tuesday, wednesday),
        universe_members=(instrument,),
        corporate_actions=(
            CorporateActionEvent(
                authority_id=UUID(int=60_005),
                instrument_id=instrument,
                source="fixture",
                provider_event_id="dividend-after-end",
                provider_action_type="cash_dividend",
                status="complete",
                provider_ex_date=tuesday,
                provider_record_date=tuesday,
                provider_payable_date=date(2021, 1, 16),
                session=tuesday,
                payable_session=None,
                action_type="dividend",
                value=Decimal("0.10"),
                available_at=datetime(2021, 1, 8, tzinfo=UTC),
                evidence_hash=_digest("dividend-after-end"),
            ),
        ),
        adjusted_prices={(instrument, tuesday): "10", (instrument, wednesday): "10"},
    )

    result = run_backtest(_compiled_baseline(), dataset, _protocol(friday, wednesday))

    assert result.nav[-1].cash == Decimal("8750.000000000000")
    assert result.nav[-1].nav == Decimal("10000.000000000000")


def test_dividend_rejects_inverted_raw_provider_dates_without_a_record_date() -> None:
    with pytest.raises(ValueError, match="provider dates must be ordered"):
        CorporateActionEvent(
            authority_id=UUID(int=60_006),
            instrument_id=UUID(int=1),
            source="fixture",
            provider_event_id="inverted-provider-dates",
            provider_action_type="cash_dividend",
            status="complete",
            provider_ex_date=date(2021, 1, 10),
            provider_record_date=None,
            provider_payable_date=date(2021, 1, 9),
            session=date(2021, 1, 11),
            payable_session=date(2021, 1, 11),
            action_type="dividend",
            value=Decimal("0.10"),
            available_at=datetime(2021, 1, 8, tzinfo=UTC),
            evidence_hash=_digest("inverted-provider-dates"),
        )


def test_sector_attribution_uses_the_vintage_effective_on_each_session() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    tuesday = date(2021, 1, 12)
    wednesday = date(2021, 1, 13)
    history = _history_sessions(friday)
    base = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (
            _bar(instrument, friday, "10"),
            _bar(instrument, monday, "10"),
            _bar(instrument, tuesday, "12"),
            _bar(instrument, wednesday, "14"),
        ),
        sessions=(*history, friday, monday, tuesday, wednesday),
        universe_members=(instrument,),
    )
    old_sector = base.sector_classifications[0].model_copy(update={"sector": "energy"})
    new_sector = SectorClassification(
        authority_id=UUID(int=50_100),
        instrument_id=instrument,
        sector="materials",
        effective_session=wednesday,
        published_at=datetime.combine(tuesday, time(20), tzinfo=UTC),
        source_uri="memory://sector/new",
        content_hash=_digest((instrument, "new_sector")),
        weight=Decimal(1),
        synthetic=True,
        reviewed=False,
    )
    sector_member_ids = (old_sector.authority_id, new_sector.authority_id)
    sector_collection = base.authority.sector_collection.model_copy(
        update={
            "member_ids": sector_member_ids,
            "content_hash": authority_collection_content_hash(
                kind="sector",
                source=base.authority.sector_collection.source,
                range_start=base.authority.sector_collection.range_start,
                range_end=base.authority.sector_collection.range_end,
                query_parameters=dict(base.authority.sector_collection.query_parameters),
                member_ids=sector_member_ids,
            ),
        }
    )
    dataset = PointInTimeDataset(
        bars=base.bars,
        adjusted_bars=base.adjusted_bars,
        universe_snapshots=base.universe_snapshots,
        asset_types=base.asset_types,
        risk_buckets=base.risk_buckets,
        sector_classifications=(old_sector, new_sector),
        authority=base.authority.model_copy(update={"sector_collection": sector_collection}),
        spy_instrument_id=base.spy_instrument_id,
    )

    result = run_backtest(_compiled_baseline(), dataset, _protocol(friday, wednesday))

    attribution = {item.key: item.value for item in result.attribution.by_sector}
    assert attribution == {
        "energy": Decimal("250.000000000000"),
        "materials": Decimal("250.000000000000"),
    }


def test_stale_production_etf_sector_exposure_is_complete_but_blocks_new_position() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    history = _history_sessions(friday)
    base = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (_bar(instrument, friday, "10"), _bar(instrument, monday, "10")),
        sessions=(*history, friday, monday),
        universe_members=(instrument,),
        asset_types={instrument: "equity_etf"},
    )
    production_exposure = base.sector_classifications[0].model_copy(
        update={
            "source_uri": "file://controlled/etf-sector.csv",
            "synthetic": False,
            "reviewed": True,
        }
    )
    sector_collection = base.authority.sector_collection.model_copy(
        update={
            "source": "controlled_csv_v1",
            "query_parameters": {
                "taxonomy": "NEXUS_SECTOR_V1",
                "synthetic": False,
                "reviewed": True,
            },
            "content_hash": authority_collection_content_hash(
                kind="sector",
                source="controlled_csv_v1",
                range_start=base.authority.sector_collection.range_start,
                range_end=base.authority.sector_collection.range_end,
                query_parameters={
                    "taxonomy": "NEXUS_SECTOR_V1",
                    "synthetic": False,
                    "reviewed": True,
                },
                member_ids=(production_exposure.authority_id,),
            ),
        }
    )
    dataset = PointInTimeDataset(
        bars=base.bars,
        adjusted_bars=base.adjusted_bars,
        universe_snapshots=base.universe_snapshots,
        asset_types=base.asset_types,
        risk_buckets=base.risk_buckets,
        sector_classifications=(production_exposure,),
        authority=base.authority.model_copy(update={"sector_collection": sector_collection}),
        spy_instrument_id=base.spy_instrument_id,
    )

    result = run_backtest(_compiled_baseline(), dataset, _protocol(friday, monday))

    assert (friday - production_exposure.effective_session).days > 30
    assert result.strategy_data_complete is True
    assert result.fills == ()
    assert result.order_outcomes == ()


def test_delisted_instrument_cannot_fill_on_its_delisting_session() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    history = _history_sessions(friday)
    dataset = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (_bar(instrument, friday, "10"), _bar(instrument, monday, "10")),
        sessions=(*history, friday, monday),
        universe_members=(instrument,),
        delistings=(
            DelistingEvent(
                authority_id=UUID(int=70_002),
                instrument_id=instrument,
                source="fixture",
                provider_event_id="delisting-on-fill",
                provider_action_type="merger_cash",
                recovery_method="cash_recovery",
                status="complete",
                provider_process_date=friday + timedelta(days=1),
                session=monday,
                recovery_price=Decimal("4"),
                available_at=datetime(2021, 1, 8, tzinfo=UTC),
                evidence_hash=_digest("delisting"),
            ),
        ),
    )

    result = run_backtest(_compiled_baseline(), dataset, _protocol(friday, monday))

    assert result.fills == ()
    assert result.order_outcomes[0].status == "untradable"
    assert result.nav[-1].nav == Decimal("10000.000000000000")
    assert result.strategy_data_complete is True
    assert not any("order_untradable" in reason for reason in result.strategy_incomplete_reasons)


def test_delisting_taxonomy_requires_explicit_recovery_semantics() -> None:
    common = {
        "authority_id": UUID(int=70_003),
        "instrument_id": UUID(int=1),
        "source": "fixture",
        "provider_event_id": "invalid-recovery",
        "status": "complete",
        "provider_process_date": date(2021, 1, 11),
        "session": date(2021, 1, 11),
        "available_at": datetime(2021, 1, 8, tzinfo=UTC),
        "evidence_hash": _digest("invalid-recovery"),
    }

    with pytest.raises(ValueError, match="cash mergers require"):
        DelistingEvent(
            **common,
            provider_action_type="merger_cash",
            recovery_method="zero_recovery",
            recovery_price=Decimal(0),
        )
    with pytest.raises(ValueError, match="bankruptcy requires"):
        DelistingEvent(
            **common,
            provider_action_type="bankruptcy",
            recovery_method="cash_recovery",
            recovery_price=Decimal(1),
        )


def test_unresolved_delisting_never_fabricates_recovery_or_complete_benchmark() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    history = _history_sessions(friday)
    dataset = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (_bar(instrument, friday, "10"), _bar(instrument, monday, "10")),
        sessions=(*history, friday, monday),
        universe_members=(instrument,),
        unresolved_delistings=(
            UnresolvedDelistingEvent(
                authority_id=UUID(int=70_004),
                instrument_id=instrument,
                source="fixture",
                provider_event_id="unresolved-delisting",
                provider_action_type="delisting",
                status="complete",
                provider_process_date=monday,
                session=monday,
                available_at=datetime(2021, 1, 8, tzinfo=UTC),
                evidence_hash=_digest("unresolved-delisting"),
            ),
        ),
    )

    result = run_backtest(_compiled_baseline(), dataset, _protocol(friday, monday))

    assert result.fills == ()
    assert result.nav[-1].nav == Decimal("10000.000000000000")
    assert result.strategy_data_complete is False
    assert result.benchmark_comparison_complete is False
    assert any(
        "delisting_recovery_unresolved" in reason for reason in result.strategy_incomplete_reasons
    )


def test_sparse_calendar_arithmetic_does_not_count_as_valid_oos_windows() -> None:
    start = date(2010, 1, 4)
    end = date(2017, 1, 4)
    sparse_sessions = tuple(start + timedelta(days=91 * index) for index in range(27))
    sparse_sessions = tuple(session for session in sparse_sessions if session.weekday() < 5)
    calendar_sessions = _business_days(start, end)
    instrument = UUID(int=1)
    dataset = _dataset(
        bars=tuple(_bar(instrument, session, "10") for session in sparse_sessions),
        sessions=calendar_sessions,
        universe_members=(instrument,),
    )
    protocol = BacktestProtocol(
        start=start,
        end=end,
        initial_nav=Decimal("10000"),
        cost_model=CostModel.zero(),
    )

    result = run_backtest(_compiled_baseline(), dataset, protocol)

    assert len(result.windows) >= 8
    assert result.valid_oos_window_count == 0
    assert result.oos_window_count_sufficient is False
    assert all(not window.valid for window in result.windows)


def test_dense_seven_year_history_can_produce_eight_valid_oos_windows_but_stays_locked() -> None:
    start = date(2010, 1, 4)
    end = date(2017, 1, 4)
    sessions = _business_days(start, end)
    instrument = UUID(int=1)
    dataset = _dataset(
        bars=tuple(_bar(instrument, session, "10") for session in sessions),
        sessions=sessions,
        universe_members=(instrument,),
    )
    protocol = BacktestProtocol(
        start=start,
        end=end,
        initial_nav=Decimal("10000"),
        cost_model=CostModel.zero(),
    )

    first = run_backtest(_compiled_baseline(), dataset, protocol)
    second = run_backtest(_compiled_baseline(), dataset, protocol)

    assert first.history_requirement_met is True
    assert first.valid_oos_window_count >= 8
    assert first.oos_window_count_sufficient is True
    assert first.promotable is False
    assert first.result_hash == second.result_hash


def test_engine_code_hash_covers_shared_simulation_kernel() -> None:
    import nexus.backtest.engine as engine_module

    root = Path(engine_module.__file__).resolve().parents[1]
    files = (
        Path(engine_module.__file__).resolve(),
        root / "backtest" / "cost_policy.py",
        root / "backtest" / "metrics.py",
        root / "simulation" / "canonical.py",
        root / "simulation" / "costs.py",
        root / "simulation" / "engine.py",
        root / "simulation" / "models.py",
        root / "strategy" / "baseline.py",
        root / "strategy" / "dsl.py",
        root / "strategy" / "interpreter.py",
    )
    digest = hashlib.sha256()
    digest.update(b"nexus_backtest_engine_v1")
    for path in sorted(files, key=lambda item: (item.parent.name, item.name)):
        digest.update(f"{path.parent.name}/{path.name}".encode())
        digest.update(path.read_bytes())

    assert _engine_code_hash("nexus_backtest_engine_v1") == digest.hexdigest()


def test_seven_year_fifty_instrument_backtest_uses_shared_weekly_feature_indexes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import nexus.backtest.engine as engine_module

    start = date(2010, 1, 4)
    end = date(2017, 1, 4)
    sessions = _business_days(start, end)
    instruments = tuple(UUID(int=index + 1) for index in range(50))
    bars = tuple(
        _bar(instrument_id, session, str(10 + instrument_id.int % 7))
        for instrument_id in instruments
        for session in sessions
    )
    dataset = _dataset(
        bars=bars,
        sessions=sessions,
        universe_members=instruments,
        equal_weight=instruments,
    )
    calls = 0
    original = engine_module._strategy_instrument_snapshot

    def counted_snapshot(**kwargs):
        nonlocal calls
        calls += 1
        return original(**kwargs)

    monkeypatch.setattr(engine_module, "_strategy_instrument_snapshot", counted_snapshot)
    started = monotonic()

    result = run_backtest(
        _compiled_baseline(),
        dataset,
        BacktestProtocol(
            start=start,
            end=end,
            initial_nav=Decimal("10000"),
            cost_model=CostModel.zero(),
        ),
    )

    elapsed = monotonic() - started
    iso_weeks = len({session.isocalendar()[:2] for session in sessions})
    assert calls <= iso_weeks * len(instruments)
    assert elapsed < 25
    assert result.history_requirement_met is True


def test_higher_costs_reduce_fixed_data_terminal_nav_and_all_sensitivities_exist() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    history = _history_sessions(friday)
    dataset = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (_bar(instrument, friday, "10"), _bar(instrument, monday, "10")),
        sessions=(*history, friday, monday),
        universe_members=(instrument,),
    )
    result = run_backtest(
        _compiled_baseline(),
        dataset,
        _protocol(
            friday,
            monday,
            CostModel(
                commission_bps=Decimal("10"),
                spread_bps=Decimal("10"),
                slippage_bps=Decimal("10"),
            ),
        ),
    )

    assert result.cost_sensitivity.half_cost_terminal_nav > result.nav[-1].nav
    assert result.cost_sensitivity.double_cost_terminal_nav < result.nav[-1].nav
    assert result.parameter_sensitivity.base_terminal_nav == result.nav[-1].nav
    assert result.execution_sensitivity.next_open_terminal_nav == result.nav[-1].nav


def test_backtest_records_auditable_hashes_and_complete_required_benchmarks() -> None:
    candidate = UUID(int=1)
    spy = UUID(int=2)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    tuesday = date(2021, 1, 12)
    history = _history_sessions(friday)
    dataset = _dataset(
        bars=tuple(_bar(candidate, session, "8") for session in history)
        + (
            _bar(candidate, friday, "10"),
            _bar(candidate, monday, "10"),
            _bar(candidate, tuesday, "10"),
            _bar(spy, friday, "100"),
            _bar(spy, monday, "100"),
            _bar(spy, tuesday, "110"),
        ),
        sessions=(*history, friday, monday, tuesday),
        universe_members=(candidate, spy),
        spy=spy,
        equal_weight=(candidate, spy),
        asset_types={spy: "equity_etf"},
    )

    result = run_backtest(_compiled_baseline(), dataset, _protocol(friday, tuesday))

    assert len(result.data_hash) == 64
    assert len(result.code_hash) == 64
    assert len(result.authority_hash) == 64
    assert result.benchmark_comparison_complete is True
    comparisons = {comparison.comparison_key: comparison for comparison in result.comparisons}
    assert set(comparisons) == {"spy", "equal_weight", "champion"}
    assert comparisons["spy"].metrics.total_return == Decimal("0.100000000000")
    assert comparisons["equal_weight"].metrics.total_return == Decimal("0.050000000000")
    assert all(comparison.complete for comparison in comparisons.values())


def test_unresolved_delisting_fields_are_covered_by_data_and_authority_hashes() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    history = _history_sessions(friday)
    event = UnresolvedDelistingEvent(
        authority_id=UUID(int=70_004),
        instrument_id=instrument,
        source="fixture",
        provider_event_id="unresolved-delisting",
        provider_action_type="delisting",
        status="complete",
        provider_process_date=monday,
        session=monday,
        available_at=datetime(2021, 1, 8, tzinfo=UTC),
        evidence_hash=_digest("unresolved-delisting"),
    )
    dataset = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (_bar(instrument, friday, "10"), _bar(instrument, monday, "10")),
        sessions=(*history, friday, monday),
        universe_members=(instrument,),
        unresolved_delistings=(event,),
    )
    changed = dataset.model_copy(
        update={
            "unresolved_delistings": (
                event.model_copy(update={"evidence_hash": _digest("revised-evidence")}),
            )
        }
    )

    first = run_backtest(_compiled_baseline(), dataset, _protocol(friday, monday))
    second = run_backtest(_compiled_baseline(), changed, _protocol(friday, monday))

    assert first.data_hash != second.data_hash
    assert first.authority_hash != second.authority_hash


def test_passive_benchmarks_charge_cost_once_against_the_full_target() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    history = _history_sessions(friday)
    dataset = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (_bar(instrument, friday, "10"), _bar(instrument, monday, "10")),
        sessions=(*history, friday, monday),
        universe_members=(instrument,),
        spy=instrument,
        equal_weight=(instrument,),
        asset_types={instrument: "equity_etf"},
    )
    protocol = _protocol(
        friday,
        monday,
        CostModel(
            commission_bps=Decimal("4"),
            spread_bps=Decimal("3"),
            slippage_bps=Decimal("3"),
        ),
    )

    result = run_backtest(_compiled_baseline(), dataset, protocol)

    comparisons = {comparison.comparison_key: comparison for comparison in result.comparisons}
    assert comparisons["spy"].terminal_nav == Decimal("9990.009990009990")
    assert comparisons["equal_weight"].terminal_nav == Decimal("9990.009990009990")
    assert comparisons["spy"].metrics.total_cost == Decimal("9.990009990010")


def test_equal_weight_benchmark_must_bind_the_exact_start_universe_vintage() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    history = _history_sessions(friday)
    base = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (_bar(instrument, friday, "10"), _bar(instrument, monday, "10")),
        sessions=(*history, friday, monday),
        universe_members=(instrument,),
    )
    newer = UniverseSnapshot(
        authority_id=UUID(int=40_001),
        version="us_liquid_v1",
        content_hash=_digest((instrument, "v2")),
        source="fixture",
        effective_session=friday,
        published_at=datetime.combine(friday, time(20), tzinfo=UTC),
        members=(instrument,),
    )
    member_ids = (
        base.universe_snapshots[0].authority_id,
        newer.authority_id,
    )
    collection = base.authority.universe_collection.model_copy(
        update={
            "member_ids": member_ids,
            "content_hash": authority_collection_content_hash(
                kind="universe",
                source=base.authority.universe_collection.source,
                range_start=base.authority.universe_collection.range_start,
                range_end=base.authority.universe_collection.range_end,
                query_parameters=dict(base.authority.universe_collection.query_parameters),
                member_ids=member_ids,
            ),
        }
    )
    dataset = PointInTimeDataset(
        bars=base.bars,
        adjusted_bars=base.adjusted_bars,
        universe_snapshots=(*base.universe_snapshots, newer),
        asset_types=base.asset_types,
        risk_buckets=base.risk_buckets,
        sector_classifications=base.sector_classifications,
        authority=base.authority.model_copy(update={"universe_collection": collection}),
        spy_instrument_id=base.spy_instrument_id,
    )

    with pytest.raises(
        ValueError, match="equal-weight benchmark must match the strategy start universe"
    ):
        run_backtest(_compiled_baseline(), dataset, _protocol(friday, monday))


def test_missing_benchmark_fill_is_recorded_and_never_reported_complete() -> None:
    candidate = UUID(int=1)
    spy = UUID(int=2)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    history = _history_sessions(friday)
    dataset = _dataset(
        bars=tuple(_bar(candidate, session, "8") for session in history)
        + (
            _bar(candidate, friday, "10"),
            _bar(candidate, monday, "10"),
            _bar(spy, friday, "100"),
        ),
        sessions=(*history, friday, monday),
        universe_members=(candidate,),
        spy=spy,
        equal_weight=(candidate,),
        asset_types={spy: "equity_etf"},
    )

    result = run_backtest(_compiled_baseline(), dataset, _protocol(friday, monday))

    spy_comparison = next(item for item in result.comparisons if item.comparison_key == "spy")
    assert spy_comparison.complete is False
    assert spy_comparison.terminal_nav == Decimal("10000.000000000000")
    assert result.benchmark_comparison_complete is False


def test_equal_weight_benchmark_requires_marks_before_a_later_delisting() -> None:
    candidate = UUID(int=1)
    spy = UUID(int=2)
    start = date(2021, 1, 8)
    removal = date(2021, 1, 15)
    missing_mark = date(2021, 1, 19)
    delisting_session = date(2021, 1, 20)
    history = _history_sessions(start)
    run_sessions = _business_days(start, delisting_session)
    bars = tuple(
        _bar(instrument_id, session, "10")
        for instrument_id in (candidate, spy)
        for session in (*history, *run_sessions)
        if not (instrument_id == candidate and session == missing_mark)
    )
    base = _dataset(
        bars=bars,
        sessions=(*history, *run_sessions),
        universe_members=(candidate, spy),
        spy=spy,
        equal_weight=(candidate, spy),
        asset_types={spy: "equity_etf"},
        delistings=(
            DelistingEvent(
                authority_id=UUID(int=70_010),
                instrument_id=candidate,
                source="fixture",
                provider_event_id="later-delisting",
                provider_action_type="merger_cash",
                recovery_method="cash_recovery",
                status="complete",
                provider_process_date=delisting_session,
                session=delisting_session,
                recovery_price=Decimal("4"),
                available_at=datetime(2021, 1, 7, tzinfo=UTC),
                evidence_hash=_digest("later-delisting"),
            ),
        ),
    )
    removed = UniverseSnapshot(
        authority_id=UUID(int=40_010),
        version="us_liquid_v2",
        content_hash=_digest((spy, "removed-candidate")),
        source="fixture",
        effective_session=removal,
        published_at=datetime.combine(removal - timedelta(days=1), time(20), tzinfo=UTC),
        members=(spy,),
    )
    member_ids = (base.universe_snapshots[0].authority_id, removed.authority_id)
    universe_collection = base.authority.universe_collection.model_copy(
        update={
            "member_ids": member_ids,
            "content_hash": authority_collection_content_hash(
                kind="universe",
                source="fixture",
                range_start=base.authority.universe_collection.range_start,
                range_end=base.authority.universe_collection.range_end,
                query_parameters={"key": "us_liquid"},
                member_ids=member_ids,
            ),
        }
    )
    dataset = PointInTimeDataset(
        bars=base.bars,
        adjusted_bars=base.adjusted_bars,
        universe_snapshots=(*base.universe_snapshots, removed),
        asset_types=base.asset_types,
        risk_buckets=base.risk_buckets,
        sector_classifications=base.sector_classifications,
        corporate_actions=base.corporate_actions,
        delistings=base.delistings,
        authority=base.authority.model_copy(update={"universe_collection": universe_collection}),
        spy_instrument_id=base.spy_instrument_id,
    )

    result = run_backtest(_compiled_baseline(), dataset, _protocol(start, delisting_session))

    equal_weight = next(
        item for item in result.comparisons if item.comparison_key == "equal_weight"
    )
    assert equal_weight.complete is False
    assert f"{candidate}:mark_coverage_incomplete" in equal_weight.incomplete_reasons


def test_missing_strategy_mark_or_adjusted_bar_is_explicitly_incomplete() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    tuesday = date(2021, 1, 12)
    history = _history_sessions(friday)
    complete = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (_bar(instrument, friday, "10"), _bar(instrument, monday, "10")),
        sessions=(*history, friday, monday, tuesday),
        universe_members=(instrument,),
    )

    result = run_backtest(_compiled_baseline(), complete, _protocol(friday, tuesday))

    assert result.strategy_data_complete is False
    assert result.strategy_incomplete_reasons == (
        f"{tuesday}:{instrument}:adjusted_bar_missing",
        f"{tuesday}:{instrument}:raw_bar_missing",
        f"{tuesday}:{instrument}:strategy_warmup_incomplete",
    )


def test_removed_universe_member_still_requires_daily_marks_while_held() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    tuesday = date(2021, 1, 12)
    history = _history_sessions(friday)
    base = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (_bar(instrument, friday, "10"), _bar(instrument, monday, "10")),
        sessions=(*history, friday, monday, tuesday),
        universe_members=(instrument,),
    )
    removed = UniverseSnapshot(
        authority_id=UUID(int=40_001),
        version="us_liquid_v1",
        content_hash=_digest("empty-universe"),
        source="fixture",
        effective_session=tuesday,
        published_at=datetime.combine(tuesday, time(20), tzinfo=UTC),
        members=(),
    )
    collection = AuthorityCollection(
        authority_id=base.authority.universe_collection.authority_id,
        kind="universe",
        source="fixture",
        range_start=base.authority.universe_collection.range_start,
        range_end=base.authority.universe_collection.range_end,
        query_parameters={"key": "us_liquid"},
        member_ids=(UUID(int=40_000), removed.authority_id),
        content_hash=authority_collection_content_hash(
            kind="universe",
            source="fixture",
            range_start=base.authority.universe_collection.range_start,
            range_end=base.authority.universe_collection.range_end,
            query_parameters={"key": "us_liquid"},
            member_ids=(UUID(int=40_000), removed.authority_id),
        ),
    )
    dataset = PointInTimeDataset(
        bars=base.bars,
        adjusted_bars=base.adjusted_bars,
        universe_snapshots=(*base.universe_snapshots, removed),
        asset_types=base.asset_types,
        risk_buckets=base.risk_buckets,
        sector_classifications=base.sector_classifications,
        authority=base.authority.model_copy(update={"universe_collection": collection}),
        spy_instrument_id=base.spy_instrument_id,
    )

    result = run_backtest(_compiled_baseline(), dataset, _protocol(friday, tuesday))

    assert result.strategy_data_complete is False
    assert f"{tuesday}:{instrument}:raw_bar_missing" in result.strategy_incomplete_reasons
    assert f"{tuesday}:{instrument}:adjusted_bar_missing" in result.strategy_incomplete_reasons


def test_contradictory_or_forged_baseline_dsl_cannot_execute() -> None:
    instrument = UUID(int=1)
    friday = date(2021, 1, 8)
    monday = date(2021, 1, 11)
    history = _history_sessions(friday)
    dataset = _dataset(
        bars=tuple(_bar(instrument, session, "8") for session in history)
        + (_bar(instrument, friday, "10"), _bar(instrument, monday, "10")),
        sessions=(*history, friday, monday),
        universe_members=(instrument,),
    )
    document = baseline_v1_document()
    document["root"]["args"][0]["op"] = "lt"
    contradictory = compile_strategy(document, baseline_feature_registry())
    forged = _compiled_baseline().model_copy(update={"expression_hash": "f" * 64})

    with pytest.raises(ValueError, match="published declarative strategy"):
        run_backtest(contradictory, dataset, _protocol(friday, monday))
    with pytest.raises(ValueError, match="INTEGRITY"):
        run_backtest(forged, dataset, _protocol(friday, monday))


def test_backtest_models_expose_immutable_numeric_result_tables() -> None:
    from nexus.persistence.models.backtest import (
        BacktestFillRecord,
        BacktestNavPointRecord,
        BacktestRunRecord,
        BacktestWindowRecord,
        StrategyDefinitionRecord,
    )

    assert StrategyDefinitionRecord.__table__.name == "strategy_definitions"
    assert BacktestRunRecord.__table__.name == "backtest_runs"
    assert BacktestWindowRecord.__table__.name == "backtest_windows"
    assert BacktestNavPointRecord.__table__.c.nav.type.scale == 12
    assert BacktestFillRecord.__table__.c.cost.type.scale == 12


def test_strategies_openapi_is_read_only_and_labels_the_locked_baseline() -> None:
    from nexus.main import create_engine_app
    from nexus.settings import EngineSettings

    operation = create_engine_app(EngineSettings.for_test()).openapi()["paths"][
        "/internal/v1/strategies"
    ]

    assert set(operation) == {"get"}
    assert operation["get"]["operationId"] == "listStrategies"


def _persisted_baseline_definition() -> SimpleNamespace:
    strategy = _compiled_baseline()
    return SimpleNamespace(
        id=uuid4(),
        schema_version="2",
        expression_hash=strategy.expression_hash,
        document=baseline_v1_document(),
        lifecycle="champion",
        published_at=datetime(2020, 1, 1, tzinfo=UTC),
    )


@pytest.mark.asyncio
async def test_strategy_listing_does_not_invent_a_missing_champion() -> None:
    from nexus.api.auth import VerifiedServiceActor
    from nexus.api.routes.strategies import list_strategies

    session = AsyncMock()
    session.scalar.return_value = None

    response = await list_strategies(
        VerifiedServiceActor(actor_id="web", request_id=uuid4(), key_id="current"), session
    )

    assert response.data == ()


@pytest.mark.asyncio
async def test_strategy_listing_exposes_only_locked_deterministic_baseline() -> None:
    from nexus.api.auth import VerifiedServiceActor
    from nexus.api.routes.strategies import list_strategies

    session = AsyncMock()
    session.scalar.side_effect = (_persisted_baseline_definition(), None)
    response = await list_strategies(
        VerifiedServiceActor(actor_id="web", request_id=uuid4(), key_id="current"), session
    )

    assert len(response.data) == 1
    assert response.data[0].strategy_version == "baseline_v1"
    assert response.data[0].champion is True
    assert response.data[0].evolution_status == "locked"
    assert response.data[0].historical_agent_overlay is False
    assert response.data[0].live_execution_enabled is False
    assert response.data[0].latest_backtest is None


@pytest.mark.asyncio
async def test_strategy_listing_reads_latest_persisted_backtest_authority() -> None:
    from nexus.api.auth import VerifiedServiceActor
    from nexus.api.routes.strategies import list_strategies

    run_id = uuid4()
    bundle_id = uuid4()
    price_authority_id = uuid4()
    session = AsyncMock()
    run = SimpleNamespace(
        id=run_id,
        authority_bundle_id=bundle_id,
        result_hash="a" * 64,
        data_hash="b" * 64,
        code_hash="c" * 64,
        start_session=date(2018, 1, 2),
        end_session=date(2026, 8, 25),
        oos_window_count=8,
        strategy_data_complete=True,
        benchmark_comparison_complete=True,
        completed_at=datetime(2026, 8, 26, tzinfo=UTC),
    )
    session.scalar.side_effect = (_persisted_baseline_definition(), run)
    session.scalars.return_value = ("champion", "spy", "equal_weight")
    collection_ids = (uuid4(), uuid4(), uuid4())
    session.get.side_effect = (
        SimpleNamespace(
            source_selection_reports=(
                {"selected_price_bar_set_authority_id": str(price_authority_id)},
            ),
            universe_collection_id=collection_ids[0],
            sector_collection_id=collection_ids[1],
            corporate_action_collection_id=collection_ids[2],
            calendar_authority_id=uuid4(),
        ),
        SimpleNamespace(source="exchange_calendars:XNYS"),
    )
    session.execute.side_effect = (
        (
            (
                SimpleNamespace(id=price_authority_id),
                SimpleNamespace(source="fixture", feed="test", delivery_mode="fixture"),
            ),
        ),
        tuple(
            (SimpleNamespace(id=collection_id, kind=kind, source="fixture"),)
            for collection_id, kind in zip(
                collection_ids,
                ("universe", "sector", "corporate_action"),
                strict=True,
            )
        ),
    )

    response = await list_strategies(
        VerifiedServiceActor(actor_id="web", request_id=uuid4(), key_id="current"), session
    )

    evidence = response.data[0].latest_backtest
    assert evidence is not None
    assert evidence.result_hash == "a" * 64
    assert evidence.comparison_keys == ("spy", "equal_weight", "champion")
    assert evidence.benchmark_comparison_complete is True
    assert evidence.data_mode == "fixture"
    assert evidence.data_sources == (
        "calendar:exchange_calendars:XNYS",
        "corporate_action:fixture",
        "price:fixture:test:fixture",
        "sector:fixture",
        "universe:fixture",
    )


@pytest.mark.asyncio
async def test_strategy_listing_labels_mixed_authority_domains_and_exposes_all_sources() -> None:
    from nexus.api.auth import VerifiedServiceActor
    from nexus.api.routes.strategies import list_strategies

    price_authority_id = uuid4()
    collection_ids = (uuid4(), uuid4(), uuid4())
    session = AsyncMock()
    run = SimpleNamespace(
        id=uuid4(),
        authority_bundle_id=uuid4(),
        result_hash="a" * 64,
        data_hash="b" * 64,
        code_hash="c" * 64,
        start_session=date(2018, 1, 2),
        end_session=date(2026, 8, 25),
        oos_window_count=8,
        strategy_data_complete=True,
        benchmark_comparison_complete=True,
        completed_at=datetime(2026, 8, 26, tzinfo=UTC),
    )
    session.scalar.side_effect = (_persisted_baseline_definition(), run)
    session.scalars.return_value = ("champion", "spy", "equal_weight")
    session.get.side_effect = (
        SimpleNamespace(
            source_selection_reports=(
                {"selected_price_bar_set_authority_id": str(price_authority_id)},
            ),
            universe_collection_id=collection_ids[0],
            sector_collection_id=collection_ids[1],
            corporate_action_collection_id=collection_ids[2],
            calendar_authority_id=uuid4(),
        ),
        SimpleNamespace(source="exchange_calendars:XNYS"),
    )
    session.execute.side_effect = (
        (
            (
                SimpleNamespace(id=price_authority_id),
                SimpleNamespace(source="alpaca", feed="sip", delivery_mode="real_time"),
            ),
        ),
        tuple(
            (SimpleNamespace(id=collection_id, kind=kind, source=source),)
            for collection_id, kind, source in zip(
                collection_ids,
                ("universe", "sector", "corporate_action"),
                ("controlled_csv_v1", "fixture", "alpaca"),
                strict=True,
            )
        ),
    )

    response = await list_strategies(
        VerifiedServiceActor(actor_id="web", request_id=uuid4(), key_id="current"), session
    )

    evidence = response.data[0].latest_backtest
    assert evidence is not None
    assert evidence.data_mode == "mixed"
    assert evidence.data_sources == (
        "calendar:exchange_calendars:XNYS",
        "corporate_action:alpaca",
        "price:alpaca:sip:real_time",
        "sector:fixture",
        "universe:controlled_csv_v1",
    )


@pytest.mark.asyncio
async def test_strategy_listing_hides_evidence_when_collection_provenance_is_missing() -> None:
    from nexus.api.auth import VerifiedServiceActor
    from nexus.api.routes.strategies import list_strategies

    price_authority_id = uuid4()
    collection_ids = (uuid4(), uuid4(), uuid4())
    session = AsyncMock()
    run = SimpleNamespace(
        id=uuid4(),
        authority_bundle_id=uuid4(),
        strategy_data_complete=True,
        benchmark_comparison_complete=True,
    )
    session.scalar.side_effect = (_persisted_baseline_definition(), run)
    session.scalars.return_value = ("champion", "spy", "equal_weight")
    session.get.side_effect = (
        SimpleNamespace(
            source_selection_reports=(
                {"selected_price_bar_set_authority_id": str(price_authority_id)},
            ),
            universe_collection_id=collection_ids[0],
            sector_collection_id=collection_ids[1],
            corporate_action_collection_id=collection_ids[2],
            calendar_authority_id=uuid4(),
        ),
        SimpleNamespace(source="exchange_calendars:XNYS"),
    )
    session.execute.side_effect = (
        (
            (
                SimpleNamespace(id=price_authority_id),
                SimpleNamespace(source="alpaca", feed="sip", delivery_mode="delayed"),
            ),
        ),
        tuple(
            (SimpleNamespace(id=collection_id, kind=kind, source="fixture"),)
            for collection_id, kind in zip(
                collection_ids[:2],
                ("universe", "sector"),
                strict=True,
            )
        ),
    )

    response = await list_strategies(
        VerifiedServiceActor(actor_id="web", request_id=uuid4(), key_id="current"), session
    )

    assert response.data[0].latest_backtest is None


@pytest.mark.asyncio
async def test_strategy_listing_hides_incomplete_strategy_data() -> None:
    from nexus.api.auth import VerifiedServiceActor
    from nexus.api.routes.strategies import list_strategies

    session = AsyncMock()
    run = SimpleNamespace(
        id=uuid4(),
        authority_bundle_id=uuid4(),
        result_hash="a" * 64,
        data_hash="b" * 64,
        code_hash="c" * 64,
        start_session=date(2018, 1, 2),
        end_session=date(2026, 8, 25),
        oos_window_count=8,
        strategy_data_complete=False,
        benchmark_comparison_complete=True,
        completed_at=datetime(2026, 8, 26, tzinfo=UTC),
    )
    session.scalar.side_effect = (_persisted_baseline_definition(), run)
    session.scalars.return_value = ("champion", "spy", "equal_weight")

    response = await list_strategies(
        VerifiedServiceActor(actor_id="web", request_id=uuid4(), key_id="current"), session
    )

    assert response.data[0].latest_backtest is None


@pytest.mark.parametrize(
    "override",
    (
        {"result_hash": "invalid"},
        {"oos_window_count": -1},
        {"start_session": date(2027, 1, 1)},
        {"comparison_keys": ("spy", "spy", "champion")},
        {"benchmark_comparison_complete": False},
    ),
)
def test_strategy_evidence_contract_rejects_malformed_cross_field_values(
    override: dict[str, object],
) -> None:
    from nexus.api.schemas.strategies import BacktestEvidenceSummaryResponse
    from pydantic import ValidationError

    values: dict[str, object] = {
        "result_hash": "a" * 64,
        "data_hash": "b" * 64,
        "code_hash": "c" * 64,
        "start_session": date(2018, 1, 2),
        "end_session": date(2026, 8, 25),
        "oos_window_count": 8,
        "comparison_keys": ("spy", "equal_weight", "champion"),
        "benchmark_comparison_complete": True,
        "data_mode": "fixture",
        "data_sources": ("fixture:test:fixture",),
        "completed_at": datetime(2026, 8, 26, tzinfo=UTC),
    }

    with pytest.raises(ValidationError):
        BacktestEvidenceSummaryResponse.model_validate({**values, **override})
