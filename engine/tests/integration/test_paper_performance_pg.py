from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.paper_batch import CALENDAR_ID, PORTFOLIO_ID, UNIVERSE_ID
from helpers.paper_performance import (
    MARKET_CLOSE,
    seed_paper_performance,
    seed_second_paper_close,
)
from nexus.market.price_bar_authority import SOURCE_POLICY_V2_ID
from nexus.paper.agent_cycle_models import candidate_manifest_hash, floor_decision_slot
from nexus.paper.errors import PaperPerformanceError
from nexus.paper.performance import (
    PaperPerformanceHooks,
    _select_mark,
    publish_paper_performance_snapshot,
)
from nexus.persistence.models.data import PriceBarSetAuthority
from nexus.persistence.models.paper import (
    PAPER_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
    AgentPaperCycleRecord,
    PaperPerformanceAttributionRecord,
    PaperPerformanceSnapshotRecord,
    PaperPositionSnapshotRecord,
    StrategyDecisionRecord,
)
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


@pytest.fixture(autouse=True)
def _historical_performance_authority_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    """Exercise historical publication mechanics behind an explicit test gate."""
    monkeypatch.setattr(
        "nexus.paper.performance.current_paper_performance_price_authority_available",
        lambda: True,
    )


@pytest.mark.asyncio
async def test_v2_mark_selection_never_falls_back_to_legacy_close(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions() as session:
            mark = await _select_mark(
                session,
                instrument_id=seed.execution.instrument_id,
                session_date=seed.as_of.date(),
                as_of=seed.as_of,
                source_policy_id=SOURCE_POLICY_V2_ID,
            )
    finally:
        await engine.dispose()

    assert mark is None


@pytest.mark.asyncio
async def test_agent_paper_mark_requires_exact_adjusted_sip_daily_policy(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_performance(
        migrated_database_url,
        source="alpaca",
        feed="sip",
        delivery_mode="delayed",
        adjustment="all",
        source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
    )
    engine = create_async_engine(migrated_database_url)
    try:
        async with AsyncSession(engine) as session:
            mark = await _select_mark(
                session,
                instrument_id=seed.execution.instrument_id,
                session_date=seed.as_of.date(),
                as_of=seed.as_of,
                source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
            )
            assert mark is not None
            assert mark[1].source_policy_id == PAPER_DECISION_SOURCE_POLICY_ID
            assert mark[2].adjustment == "all"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_schwab_agent_decision_uses_existing_daily_mark_authority(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_performance(
        migrated_database_url,
        source="alpaca",
        feed="sip",
        delivery_mode="delayed",
        adjustment="all",
        source_policy_id=PAPER_DECISION_SOURCE_POLICY_ID,
    )
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    cycle_id = UUID("66200000-0000-0000-0000-000000000001")
    decision_time = datetime(2026, 8, 27, 19, tzinfo=UTC)
    try:
        async with sessions.begin() as session:
            session.add(
                AgentPaperCycleRecord(
                    id=cycle_id,
                    account_id=seed.execution.account_id,
                    portfolio_id=PORTFOLIO_ID,
                    decision_session=decision_time.date(),
                    decision_time=decision_time,
                    decision_slot_at=floor_decision_slot(decision_time),
                    execution_session=decision_time.date(),
                    universe_snapshot_id=UNIVERSE_ID,
                    quality_report_id=None,
                    decision_price_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                    execution_price_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
                    candidate_manifest=[],
                    candidate_hash=candidate_manifest_hash(()),
                    sizing_policy_version="agent_paper_sizing_v1",
                    sizing_policy_hash="8" * 64,
                    execution_locked=True,
                )
            )
            await session.flush()
            session.add(
                StrategyDecisionRecord(
                    id=UUID("66200000-0000-0000-0000-000000000002"),
                    account_id=seed.execution.account_id,
                    portfolio_id=PORTFOLIO_ID,
                    strategy_id="agent_research",
                    strategy_version="agent_paper_decision_v1",
                    strategy_definition_id=None,
                    registry_event_id=None,
                    champion_generation=None,
                    decision_origin="agent_research",
                    agent_paper_cycle_id=cycle_id,
                    decision_session=decision_time.date(),
                    decision_time=decision_time,
                    execution_session=decision_time.date(),
                    source_policy_id=PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
                    execution_source_policy_id=PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID,
                    calendar_authority_id=CALENDAR_ID,
                    universe_snapshot_id=UNIVERSE_ID,
                    quality_report_id=None,
                    portfolio_projection_hash="1" * 64,
                    authority_hash="2" * 64,
                    strategy_parameters_hash="3" * 64,
                    decision_hash="4" * 64,
                    cash_weight=Decimal("1"),
                    reason="AGENT_RESEARCH",
                )
            )

        async with sessions.begin() as session:
            publication = await publish_paper_performance_snapshot(
                session,
                account_id=seed.execution.account_id,
                as_of=seed.as_of,
            )
            mark_policy_id = await session.scalar(
                select(PriceBarSetAuthority.source_policy_id).where(
                    PriceBarSetAuthority.id == publication.positions[0].price_authority_id
                )
            )
    finally:
        await engine.dispose()

    assert publication.positions[0].reference_close == Decimal("110.000000000000")
    assert publication.positions[0].price_source == "alpaca"
    assert mark_policy_id == PAPER_DECISION_SOURCE_POLICY_ID


@pytest.mark.asyncio
async def test_new_performance_publication_rejects_open_position_without_current_authority(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    monkeypatch.setattr(
        "nexus.paper.performance.current_paper_performance_price_authority_available",
        lambda: False,
    )
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        with pytest.raises(
            PaperPerformanceError,
            match="PAPER_PERFORMANCE_PRICE_INCOMPLETE",
        ):
            async with sessions.begin() as session:
                await publish_paper_performance_snapshot(
                    session,
                    account_id=seed.execution.account_id,
                    as_of=seed.as_of,
                )
        async with sessions() as session:
            observed: list[int] = []
            for model in (
                PaperPerformanceSnapshotRecord,
                PaperPositionSnapshotRecord,
                PaperPerformanceAttributionRecord,
            ):
                observed.append(
                    int(await session.scalar(select(func.count()).select_from(model)) or 0)
                )
            counts = tuple(observed)
    finally:
        await engine.dispose()

    assert counts == (0, 0, 0)


@pytest.mark.asyncio
async def test_publish_performance_reconciles_close_ledger_positions_and_attribution(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            publication = await publish_paper_performance_snapshot(
                session,
                account_id=seed.execution.account_id,
                as_of=seed.as_of,
            )

        quantity = seed.execution.quantity
        expected_cost = quantity * Decimal("100.25")
        expected_cash = Decimal("10000") - expected_cost
        expected_market_value = quantity * Decimal("110")
        expected_unrealized = expected_market_value - expected_cost
        assert publication.replayed is False
        assert publication.performance.cash == expected_cash
        assert publication.performance.market_value == expected_market_value
        assert publication.performance.nav == expected_cash + expected_market_value
        assert publication.performance.realized_pnl == Decimal("0E-12")
        assert publication.performance.unrealized_pnl == expected_unrealized
        assert publication.performance.fees == Decimal("0E-12")
        assert publication.performance.peak_nav == expected_cash + expected_market_value
        assert publication.performance.drawdown_ratio == Decimal("0E-12")
        assert len(publication.positions) == 1
        position = publication.positions[0]
        assert position.quantity == quantity
        assert position.cost_basis == expected_cost
        assert position.reference_close == Decimal("110.000000000000")
        assert position.price_source == "fixture"
        assert position.price_feed == "fixture"
        assert position.price_available_at == MARKET_CLOSE

        by_dimension = {
            (item.dimension, item.dimension_key): item for item in publication.attributions
        }
        assert by_dimension[("cash", "USD")].market_value == expected_cash
        instrument = by_dimension[("instrument", str(seed.execution.instrument_id))]
        assert instrument.market_value == expected_market_value
        assert instrument.unrealized_pnl == expected_unrealized
        assert instrument.slippage == quantity * Decimal("0.25")
        sector = by_dimension[("sector", "technology")]
        assert sector.market_value == expected_market_value
        assert sector.completeness == "FULL"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("future_close", (False, True))
async def test_publish_performance_fails_closed_without_visible_complete_close(
    migrated_database_url: str,
    future_close: bool,
) -> None:
    seed = await seed_paper_performance(
        migrated_database_url,
        close=Decimal("110") if future_close else None,
        available_at=MARKET_CLOSE + timedelta(seconds=1),
    )
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions() as session:
            with pytest.raises(PaperPerformanceError) as raised:
                async with session.begin():
                    await publish_paper_performance_snapshot(
                        session,
                        account_id=seed.execution.account_id,
                        as_of=seed.as_of,
                    )
            assert raised.value.code == "PAPER_PERFORMANCE_PRICE_INCOMPLETE"
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(func.count()).select_from(PaperPerformanceSnapshotRecord)
                )
                == 0
            )
            assert (
                await session.scalar(select(func.count()).select_from(PaperPositionSnapshotRecord))
                == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_publish_performance_exact_replay_survives_authority_gate_closure(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            first = await publish_paper_performance_snapshot(
                session,
                account_id=seed.execution.account_id,
                as_of=seed.as_of,
            )
        async with sessions() as session:
            before_counts = tuple(
                [
                    int(await session.scalar(select(func.count()).select_from(model)) or 0)
                    for model in (
                        PaperPerformanceSnapshotRecord,
                        PaperPositionSnapshotRecord,
                        PaperPerformanceAttributionRecord,
                    )
                ]
            )
        monkeypatch.setattr(
            "nexus.paper.performance.current_paper_performance_price_authority_available",
            lambda: False,
        )
        async with sessions.begin() as session:
            replay = await publish_paper_performance_snapshot(
                session,
                account_id=seed.execution.account_id,
                as_of=seed.as_of,
            )

        assert first.replayed is False
        assert replay.replayed is True
        assert replay.snapshot_id == first.snapshot_id
        assert replay.performance == first.performance
        assert replay.positions == first.positions
        assert replay.attributions == first.attributions
        async with sessions() as session:
            after_counts = tuple(
                [
                    int(await session.scalar(select(func.count()).select_from(model)) or 0)
                    for model in (
                        PaperPerformanceSnapshotRecord,
                        PaperPositionSnapshotRecord,
                        PaperPerformanceAttributionRecord,
                    )
                ]
            )
        assert after_counts == before_counts == (1, 1, 3)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_publish_performance_keeps_peak_and_calculates_drawdown(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_performance(migrated_database_url, close=Decimal("110"))
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            first = await publish_paper_performance_snapshot(
                session,
                account_id=seed.execution.account_id,
                as_of=seed.as_of,
            )
        second_as_of = await seed_second_paper_close(
            migrated_database_url,
            execution=seed.execution,
            close=Decimal("90"),
        )
        async with sessions.begin() as session:
            publication = await publish_paper_performance_snapshot(
                session,
                account_id=seed.execution.account_id,
                as_of=second_as_of,
            )

        expected_nav = (
            Decimal("10000")
            - seed.execution.quantity * Decimal("100.25")
            + seed.execution.quantity * Decimal("90")
        )
        expected_drawdown = (first.performance.nav - expected_nav) / first.performance.nav
        assert publication.performance.nav == expected_nav
        assert publication.performance.peak_nav == first.performance.nav
        assert publication.performance.drawdown_ratio == expected_drawdown.quantize(
            Decimal("0.000000000001")
        )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_performance_publication_rejects_out_of_order_close(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_performance(migrated_database_url, close=Decimal("110"))
    second_as_of = await seed_second_paper_close(
        migrated_database_url,
        execution=seed.execution,
        close=Decimal("90"),
    )
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            later = await publish_paper_performance_snapshot(
                session,
                account_id=seed.execution.account_id,
                as_of=second_as_of,
            )
        with pytest.raises(PaperPerformanceError) as raised:
            async with sessions.begin() as session:
                await publish_paper_performance_snapshot(
                    session,
                    account_id=seed.execution.account_id,
                    as_of=seed.as_of,
                )

        assert raised.value.code == "PAPER_PERFORMANCE_CONFLICT"
        async with sessions() as session:
            snapshots = tuple(await session.scalars(select(PaperPerformanceSnapshotRecord)))
        assert len(snapshots) == 1
        assert snapshots[0].id == later.snapshot_id

        forged_nav = later.performance.nav + Decimal("1000")
        with pytest.raises(DBAPIError, match="performance snapshot chain is invalid"):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO paper_performance_snapshots "
                        "(id, account_id, as_of, nav, peak_nav, cash, realized_pnl, "
                        "unrealized_pnl, fees, drawdown_ratio, content_hash) VALUES "
                        "('66200000-0000-0000-0000-000000000001', :account_id, "
                        ":as_of, :nav, :nav, :nav, 0, 0, 0, 0, :content_hash)"
                    ),
                    {
                        "account_id": seed.execution.account_id,
                        "as_of": seed.as_of,
                        "nav": forged_nav,
                        "content_hash": "a" * 64,
                    },
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_publish_performance_marks_missing_exact_sector_as_partial(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_performance(migrated_database_url, exact_sector=False)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            publication = await publish_paper_performance_snapshot(
                session,
                account_id=seed.execution.account_id,
                as_of=seed.as_of,
            )

        sector = next(item for item in publication.attributions if item.dimension == "sector")
        assert sector.dimension_key == "unclassified"
        assert sector.market_value == seed.execution.quantity * Decimal("110")
        assert sector.completeness == "PARTIAL"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_publish_performance_does_not_use_late_sector_authority(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_performance(
        migrated_database_url,
        sector_created_at=MARKET_CLOSE + timedelta(microseconds=1),
    )
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            publication = await publish_paper_performance_snapshot(
                session,
                account_id=seed.execution.account_id,
                as_of=seed.as_of,
            )

        sector = next(item for item in publication.attributions if item.dimension == "sector")
        assert sector.dimension_key == "unclassified"
        assert sector.completeness == "PARTIAL"
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_two_performance_publishers_converge_on_one_snapshot(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def publish() -> tuple[UUID, bool]:
        async with sessions.begin() as session:
            result = await publish_paper_performance_snapshot(
                session,
                account_id=seed.execution.account_id,
                as_of=seed.as_of,
            )
            return result.snapshot_id, result.replayed

    try:
        results = await asyncio.gather(publish(), publish())
        assert results[0][0] == results[1][0]
        assert sorted(item[1] for item in results) == [False, True]
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(func.count()).select_from(PaperPerformanceSnapshotRecord)
                )
                == 1
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault_phase",
    ("snapshot_persisted", "positions_persisted", "attributions_persisted"),
)
async def test_performance_publication_faults_roll_back_every_projection(
    migrated_database_url: str,
    fault_phase: str,
) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    def fail_at(phase: str) -> None:
        if phase == fault_phase:
            raise RuntimeError("injected paper performance failure")

    try:
        async with sessions() as session:
            with pytest.raises(RuntimeError, match="injected paper performance failure"):
                async with session.begin():
                    await publish_paper_performance_snapshot(
                        session,
                        account_id=seed.execution.account_id,
                        as_of=seed.as_of,
                        hooks=PaperPerformanceHooks(after_phase=fail_at),
                    )
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(func.count()).select_from(PaperPerformanceSnapshotRecord)
                )
                == 0
            )
            assert (
                await session.scalar(select(func.count()).select_from(PaperPositionSnapshotRecord))
                == 0
            )
            assert (
                await session.scalar(
                    select(func.count()).select_from(PaperPerformanceAttributionRecord)
                )
                == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "table_name",
    (
        "paper_performance_snapshots",
        "paper_position_snapshots",
        "paper_performance_attributions",
    ),
)
async def test_published_performance_facts_are_database_immutable(
    migrated_database_url: str,
    table_name: str,
) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await publish_paper_performance_snapshot(
                session,
                account_id=seed.execution.account_id,
                as_of=seed.as_of,
            )
        async with sessions() as session:
            with pytest.raises(DBAPIError, match="paper fact is append-only"):
                async with session.begin():
                    await session.execute(text(f"UPDATE {table_name} SET created_at = created_at"))
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_database_rejects_standalone_performance_snapshot(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    try:
        with pytest.raises(DBAPIError, match="paper performance root is inconsistent"):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO paper_performance_snapshots "
                        "(id, account_id, as_of, nav, peak_nav, cash, realized_pnl, "
                        "unrealized_pnl, fees, drawdown_ratio, content_hash) VALUES "
                        "('66300000-0000-0000-0000-000000000001', :account_id, "
                        ":as_of, 10000, 10000, 10000, 0, 0, 0, 0, :content_hash)"
                    ),
                    {
                        "account_id": seed.execution.account_id,
                        "as_of": seed.as_of,
                        "content_hash": "b" * 64,
                    },
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_database_rejects_zero_value_forged_instrument_attribution(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_performance(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        with pytest.raises(DBAPIError, match="paper performance root is inconsistent"):
            async with sessions.begin() as session:
                publication = await publish_paper_performance_snapshot(
                    session,
                    account_id=seed.execution.account_id,
                    as_of=seed.as_of,
                )
                session.add(
                    PaperPerformanceAttributionRecord(
                        id=UUID("66300000-0000-0000-0000-000000000002"),
                        performance_snapshot_id=publication.snapshot_id,
                        account_id=seed.execution.account_id,
                        dimension="instrument",
                        dimension_key="66300000-0000-0000-0000-000000000003",
                        market_value=Decimal(0),
                        realized_pnl=Decimal(0),
                        unrealized_pnl=Decimal(0),
                        fees=Decimal(0),
                        slippage=Decimal(0),
                        completeness="FULL",
                        content_hash="c" * 64,
                    )
                )
    finally:
        await engine.dispose()
