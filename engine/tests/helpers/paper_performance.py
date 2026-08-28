"""PostgreSQL fixtures for Task 19 end-of-day paper performance."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

from helpers.paper_batch import CALENDAR_ID, EXECUTION_SESSION
from helpers.paper_execution import PaperExecutionSeed, seed_paper_execution
from nexus.market.price_bar_authority import finalize_price_bar_set
from nexus.paper.execution_service import advance_internal_paper_order
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.data import DataArtifact, PriceBarRecord, PriceSeriesManifest
from nexus.persistence.models.market import SectorExposure
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

MARKET_CLOSE = datetime(2026, 8, 27, 20, tzinfo=UTC)
SECOND_MARKET_CLOSE = datetime(2026, 8, 28, 20, tzinfo=UTC)


@dataclass(frozen=True)
class PaperPerformanceSeed:
    execution: PaperExecutionSeed
    as_of: datetime
    close: Decimal | None


async def seed_paper_performance(
    database_url: str,
    *,
    close: Decimal | None = Decimal("110"),
    available_at: datetime = MARKET_CLOSE,
    source: str = "fixture",
    feed: str = "fixture",
    delivery_mode: str = "fixture",
    exact_sector: bool = True,
    sector_created_at: datetime = MARKET_CLOSE,
) -> PaperPerformanceSeed:
    """Fill one buy order and optionally freeze its exact daily close and sector."""
    execution = await seed_paper_execution(database_url)
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await advance_internal_paper_order(
                session,
                order_id=execution.order_id,
                evaluated_at=execution.evaluation_times[0],
                idempotency_key="task19-performance-fill",
            )
        async with sessions.begin() as session:
            if close is not None:
                artifact_id = UUID("66000000-0000-0000-0000-000000000001")
                manifest_id = UUID("66000000-0000-0000-0000-000000000002")
                session.add(
                    DataArtifact(
                        id=artifact_id,
                        uri="memory://task19/performance/close",
                        sha256="d" * 64,
                        byte_size=100,
                        mime_type="application/x-parquet",
                        schema_version="v1",
                        source=source,
                        observed_at=MARKET_CLOSE,
                        available_at=available_at,
                        received_at=available_at,
                    )
                )
                manifest = PriceSeriesManifest(
                    id=manifest_id,
                    evidence_ref_id=None,
                    data_artifact_id=artifact_id,
                    instrument=str(execution.instrument_id),
                    source=source,
                    feed=feed,
                    delivery_mode=delivery_mode,
                    adjustment="raw",
                    interval="1Day",
                    start_at=MARKET_CLOSE,
                    end_at=MARKET_CLOSE,
                    row_count=1,
                )
                session.add(manifest)
                await session.flush()
                session.add(
                    PriceBarRecord(
                        id=UUID("66000000-0000-0000-0000-000000000003"),
                        price_series_manifest_id=manifest_id,
                        instrument_id=execution.instrument_id,
                        session=EXECUTION_SESSION,
                        open=close,
                        high=close,
                        low=close,
                        close=close,
                        volume=Decimal("1000000"),
                        observed_at=MARKET_CLOSE,
                        available_at=available_at,
                        received_at=available_at,
                        revision="v1",
                        quality_flags=[],
                    )
                )
                await session.flush()
                assert await finalize_price_bar_set(session, manifest) is not None
            if exact_sector:
                session.add(
                    SectorExposure(
                        id=UUID("66000000-0000-0000-0000-000000000004"),
                        instrument_id=execution.instrument_id,
                        sector_code="technology",
                        weight=Decimal(1),
                        as_of=EXECUTION_SESSION,
                        source_url="memory://task19/performance/sector",
                        content_hash="e" * 64,
                        synthetic=False,
                        reviewed=True,
                        created_at=sector_created_at,
                    )
                )
        return PaperPerformanceSeed(execution=execution, as_of=MARKET_CLOSE, close=close)
    finally:
        await engine.dispose()


async def seed_second_paper_close(
    database_url: str,
    *,
    execution: PaperExecutionSeed,
    close: Decimal,
) -> datetime:
    """Append the next complete calendar session and its frozen daily close."""
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=CALENDAR_ID,
                    session=date(2026, 8, 28),
                    decision_at=datetime(2026, 8, 28, 21, tzinfo=UTC),
                    market_open_at=datetime(2026, 8, 28, 13, 30, tzinfo=UTC),
                    market_close_at=SECOND_MARKET_CLOSE,
                )
            )
            artifact_id = UUID("66100000-0000-0000-0000-000000000001")
            manifest_id = UUID("66100000-0000-0000-0000-000000000002")
            session.add(
                DataArtifact(
                    id=artifact_id,
                    uri="memory://task19/performance/second-close",
                    sha256="c" * 64,
                    byte_size=100,
                    mime_type="application/x-parquet",
                    schema_version="v1",
                    source="fixture",
                    observed_at=SECOND_MARKET_CLOSE,
                    available_at=SECOND_MARKET_CLOSE,
                    received_at=SECOND_MARKET_CLOSE,
                )
            )
            manifest = PriceSeriesManifest(
                id=manifest_id,
                evidence_ref_id=None,
                data_artifact_id=artifact_id,
                instrument=str(execution.instrument_id),
                source="fixture",
                feed="fixture",
                delivery_mode="fixture",
                adjustment="raw",
                interval="1Day",
                start_at=SECOND_MARKET_CLOSE,
                end_at=SECOND_MARKET_CLOSE,
                row_count=1,
            )
            session.add(manifest)
            await session.flush()
            session.add(
                PriceBarRecord(
                    id=UUID("66100000-0000-0000-0000-000000000003"),
                    price_series_manifest_id=manifest_id,
                    instrument_id=execution.instrument_id,
                    session=date(2026, 8, 28),
                    open=close,
                    high=close,
                    low=close,
                    close=close,
                    volume=Decimal("1000000"),
                    observed_at=SECOND_MARKET_CLOSE,
                    available_at=SECOND_MARKET_CLOSE,
                    received_at=SECOND_MARKET_CLOSE,
                    revision="v1",
                    quality_flags=[],
                )
            )
            session.add(
                SectorExposure(
                    id=UUID("66100000-0000-0000-0000-000000000004"),
                    instrument_id=execution.instrument_id,
                    sector_code="technology",
                    weight=Decimal(1),
                    as_of=date(2026, 8, 28),
                    source_url="memory://task19/performance/second-sector",
                    content_hash="b" * 64,
                    synthetic=False,
                    reviewed=True,
                    created_at=SECOND_MARKET_CLOSE,
                )
            )
            await session.flush()
            assert await finalize_price_bar_set(session, manifest) is not None
        return SECOND_MARKET_CLOSE
    finally:
        await engine.dispose()
