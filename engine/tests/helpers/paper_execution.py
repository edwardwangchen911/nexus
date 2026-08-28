"""PostgreSQL fixtures for deterministic Task 19 internal-paper execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from helpers.paper_batch import EXECUTION_SESSION, INSTRUMENTS, seed_paper_batch
from nexus.market.price_bar_authority import finalize_price_bar_set
from nexus.paper.service import create_paper_order_batch
from nexus.persistence.models.data import DataArtifact, PriceBarRecord, PriceSeriesManifest
from nexus.persistence.models.market import LatestQuote
from nexus.persistence.models.paper import PaperOrderRecord
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

MARKET_OPEN = datetime(2026, 8, 27, 13, 30, tzinfo=UTC)
FIRST_EVALUATION = datetime(2026, 8, 27, 13, 55, tzinfo=UTC)


@dataclass(frozen=True)
class PaperExecutionSeed:
    account_id: UUID
    order_id: UUID
    instrument_id: UUID
    quantity: Decimal
    reservation_amount: Decimal
    evaluation_times: tuple[datetime, ...]


async def seed_paper_execution(
    database_url: str,
    *,
    window_bar_volumes: tuple[Decimal, ...] = (Decimal("2000"),),
    closes: tuple[Decimal, ...] | None = None,
    feeds: tuple[str, ...] | None = None,
    with_quote: bool = False,
    market_close_at: datetime | None = None,
) -> PaperExecutionSeed:
    """Create one submitted buy order plus zero or more exact five-minute authorities."""
    seed = await seed_paper_batch(
        database_url,
        execution_market_close_at=market_close_at,
    )
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await create_paper_order_batch(
                session,
                account_id=seed.account_id,
                strategy_decision_id=seed.decision_id,
                idempotency_key="task19-execution-seed",
                request_time=seed.request_time,
            )
        async with sessions.begin() as session:
            order = await session.scalar(
                select(PaperOrderRecord).where(PaperOrderRecord.instrument_id == INSTRUMENTS[0][0])
            )
            assert order is not None
            reservation_amount = await session.scalar(
                select(PaperOrderRecord.worst_price).where(PaperOrderRecord.id == order.id)
            )
            assert reservation_amount is not None
            reservation_amount *= order.quantity

            effective_closes = closes or tuple(Decimal("100") for _ in window_bar_volumes)
            if len(effective_closes) != len(window_bar_volumes):
                raise ValueError("closes must match the number of execution windows")
            effective_feeds = feeds or tuple("fixture" for _ in window_bar_volumes)
            if len(effective_feeds) != len(window_bar_volumes):
                raise ValueError("feeds must match the number of execution windows")
            evaluation_times: list[datetime] = []
            for window_index, (bar_volume, close, feed) in enumerate(
                zip(
                    window_bar_volumes,
                    effective_closes,
                    effective_feeds,
                    strict=True,
                )
            ):
                window_start = MARKET_OPEN + timedelta(minutes=5 * window_index)
                evaluated_at = FIRST_EVALUATION + timedelta(minutes=5 * window_index)
                evaluation_times.append(evaluated_at)
                artifact_id = UUID(int=0x65000000000000000000000000000100 + window_index)
                manifest_id = UUID(int=0x65000000000000000000000000000200 + window_index)
                session.add(
                    DataArtifact(
                        id=artifact_id,
                        uri=f"memory://task19/window/{window_index}",
                        sha256=f"{window_index + 1:x}".rjust(64, "0"),
                        byte_size=500,
                        mime_type="application/x-parquet",
                        schema_version="v1",
                        source="fixture",
                        observed_at=window_start,
                        available_at=evaluated_at,
                        received_at=evaluated_at,
                    )
                )
                manifest = PriceSeriesManifest(
                    id=manifest_id,
                    evidence_ref_id=None,
                    data_artifact_id=artifact_id,
                    instrument=str(order.instrument_id),
                    source="fixture",
                    feed=feed,
                    delivery_mode="fixture",
                    adjustment="raw",
                    interval="1Min",
                    start_at=window_start,
                    end_at=window_start + timedelta(minutes=4),
                    row_count=5,
                )
                session.add(manifest)
                await session.flush()
                session.add_all(
                    PriceBarRecord(
                        id=UUID(
                            int=(
                                0x65000000000000000000000000001000
                                + window_index * 16
                                + minute_index
                            )
                        ),
                        price_series_manifest_id=manifest_id,
                        instrument_id=order.instrument_id,
                        session=EXECUTION_SESSION,
                        open=close,
                        high=close,
                        low=close,
                        close=close,
                        volume=bar_volume,
                        observed_at=window_start + timedelta(minutes=minute_index),
                        available_at=evaluated_at,
                        received_at=evaluated_at,
                        revision="v1",
                        quality_flags=[],
                    )
                    for minute_index in range(5)
                )
                await session.flush()
                finalized = await finalize_price_bar_set(session, manifest)
                assert finalized is not None

            if with_quote:
                session.add(
                    LatestQuote(
                        id=UUID("65000000-0000-0000-0000-000000000099"),
                        instrument_id=order.instrument_id,
                        source="fixture",
                        feed="fixture",
                        delivery_mode="fixture",
                        bid=Decimal("99.90"),
                        ask=Decimal("100.10"),
                        last=Decimal("100"),
                        observed_at=MARKET_OPEN + timedelta(minutes=2),
                        available_at=FIRST_EVALUATION,
                        received_at=FIRST_EVALUATION,
                        revision="v1",
                        quality_flags=[],
                    )
                )
        return PaperExecutionSeed(
            account_id=seed.account_id,
            order_id=order.id,
            instrument_id=order.instrument_id,
            quantity=order.quantity,
            reservation_amount=reservation_amount,
            evaluation_times=tuple(evaluation_times),
        )
    finally:
        await engine.dispose()
