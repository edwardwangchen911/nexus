from __future__ import annotations

import asyncio
from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.paper_performance import (
    MARKET_CLOSE,
    seed_paper_performance,
    seed_second_paper_close,
)
from nexus.paper.errors import PaperPerformanceError
from nexus.paper.performance import (
    PaperPerformanceHooks,
    publish_paper_performance_snapshot,
)
from nexus.persistence.models.paper import (
    PaperPerformanceAttributionRecord,
    PaperPerformanceSnapshotRecord,
    PaperPositionSnapshotRecord,
)
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


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
async def test_publish_performance_is_idempotent_for_account_and_close(
    migrated_database_url: str,
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
        async with sessions.begin() as session:
            replay = await publish_paper_performance_snapshot(
                session,
                account_id=seed.execution.account_id,
                as_of=seed.as_of,
            )

        assert replay.replayed is True
        assert replay.snapshot_id == first.snapshot_id
        assert replay.performance.content_hash == first.performance.content_hash
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(func.count()).select_from(PaperPerformanceSnapshotRecord)
                )
                == 1
            )
            assert (
                await session.scalar(select(func.count()).select_from(PaperPositionSnapshotRecord))
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count()).select_from(PaperPerformanceAttributionRecord)
                )
                == 3
            )
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
