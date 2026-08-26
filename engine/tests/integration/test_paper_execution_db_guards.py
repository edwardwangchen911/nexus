"""Raw persistence guard cases for Task 19 execution-window evidence."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.paper_batch import EXECUTION_SESSION, seed_paper_batch
from nexus.market.price_bar_authority import finalize_price_bar_set
from nexus.paper.service import create_paper_order_batch
from nexus.persistence.models.data import DataArtifact, PriceBarRecord, PriceSeriesManifest
from nexus.persistence.models.paper import (
    PaperExecutionWindowAuthorityRecord,
    PaperExecutionWindowMemberRecord,
)
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

OPEN = datetime(2026, 8, 27, 13, 30, tzinfo=UTC)
EVALUATED = datetime(2026, 8, 27, 13, 55, tzinfo=UTC)
FIRST_WINDOW_EVALUATED = EVALUATED + timedelta(minutes=10)
ARTIFACT_ID = UUID("63000000-0000-0000-0000-000000000001")
MANIFEST_ID = UUID("63000000-0000-0000-0000-000000000002")
WINDOW_ID = UUID("63000000-0000-0000-0000-000000000003")


async def _seed_valid_window(database_url: str) -> tuple[UUID, tuple[UUID, ...]]:
    seed = await seed_paper_batch(database_url)
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            await create_paper_order_batch(
                session,
                account_id=seed.account_id,
                strategy_decision_id=seed.decision_id,
                idempotency_key="task19-window-guard",
                request_time=seed.request_time,
            )
        async with sessions.begin() as session:
            order = (
                await session.execute(
                    text(
                        "SELECT orders.id, orders.account_id, orders.instrument_id "
                        "FROM paper_orders orders "
                        "JOIN paper_order_events event ON event.order_id = orders.id "
                        "WHERE event.to_status = 'submitted' "
                        "ORDER BY orders.client_order_id LIMIT 1"
                    )
                )
            ).mappings().one()
            session.add(
                DataArtifact(
                    id=ARTIFACT_ID,
                    uri="memory://task19/minute-bars",
                    sha256="6" * 64,
                    byte_size=500,
                    mime_type="application/x-parquet",
                    schema_version="v1",
                    source="fixture",
                    observed_at=OPEN,
                    available_at=EVALUATED,
                    received_at=EVALUATED,
                )
            )
            manifest = PriceSeriesManifest(
                id=MANIFEST_ID,
                evidence_ref_id=None,
                data_artifact_id=ARTIFACT_ID,
                instrument=str(order["instrument_id"]),
                source="fixture",
                feed="fixture",
                delivery_mode="fixture",
                adjustment="raw",
                interval="1Min",
                start_at=OPEN,
                end_at=OPEN + timedelta(minutes=4),
                row_count=5,
            )
            session.add(manifest)
            await session.flush()
            bar_ids = tuple(
                UUID(int=0x63000000000000000000000000000100 + index)
                for index in range(5)
            )
            session.add_all(
                PriceBarRecord(
                    id=bar_id,
                    price_series_manifest_id=MANIFEST_ID,
                    instrument_id=order["instrument_id"],
                    session=EXECUTION_SESSION,
                    open=Decimal("100"),
                    high=Decimal("100"),
                    low=Decimal("100"),
                    close=Decimal("100"),
                    volume=Decimal("1000"),
                    observed_at=OPEN + timedelta(minutes=index),
                    available_at=EVALUATED,
                    received_at=EVALUATED,
                    revision="v1",
                    quality_flags=[],
                )
                for index, bar_id in enumerate(bar_ids)
            )
            await session.flush()
            finalized = await finalize_price_bar_set(session, manifest)
            assert finalized is not None
            authority, _ = finalized
            session.add(
                PaperExecutionWindowAuthorityRecord(
                    id=WINDOW_ID,
                    order_id=order["id"],
                    account_id=order["account_id"],
                    instrument_id=order["instrument_id"],
                    window_seq=1,
                    window_start_at=OPEN,
                    window_end_at=OPEN + timedelta(minutes=5),
                    evaluated_at=FIRST_WINDOW_EVALUATED,
                    data_available_at=EVALUATED,
                    source_authority_id=authority.id,
                    source=authority.source,
                    source_content_hash=authority.content_hash,
                    feed="fixture",
                    delivery_mode="fixture",
                    vwap=Decimal("100"),
                    volume=Decimal("5000"),
                    capacity=Decimal("50"),
                    quote_id=None,
                    quote_revision=None,
                    bid=None,
                    ask=None,
                    half_spread=Decimal(0),
                    content_hash="7" * 64,
                )
            )
            session.add_all(
                PaperExecutionWindowMemberRecord(
                    window_authority_id=WINDOW_ID,
                    bar_id=bar_id,
                    member_seq=index,
                )
                for index, bar_id in enumerate(bar_ids, start=1)
            )
        return order["id"], bar_ids
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_execution_window_exact_set_is_accepted_then_immutable(
    migrated_database_url: str,
) -> None:
    order_id, bar_ids = await _seed_valid_window(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    try:
        async with engine.connect() as connection:
            root = (
                await connection.execute(
                    text(
                        "SELECT order_id, vwap, volume, capacity "
                        "FROM paper_execution_window_authorities WHERE id = :id"
                    ),
                    {"id": WINDOW_ID},
                )
            ).one()
            members = (
                await connection.execute(
                    select(PaperExecutionWindowMemberRecord.bar_id)
                    .where(
                        PaperExecutionWindowMemberRecord.window_authority_id == WINDOW_ID
                    )
                    .order_by(PaperExecutionWindowMemberRecord.member_seq)
                )
            ).scalars().all()
        assert root == (order_id, Decimal("100"), Decimal("5000"), Decimal("50"))
        assert tuple(members) == bar_ids

        with pytest.raises(DBAPIError, match="append-only"):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE paper_execution_window_authorities SET capacity = 51 "
                        "WHERE id = :id"
                    ),
                    {"id": WINDOW_ID},
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_execution_window_rejects_an_incomplete_or_shifted_bar_set(
    migrated_database_url: str,
) -> None:
    order_id, bar_ids = await _seed_valid_window(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions() as read_session:
            root = await read_session.scalar(
                select(PaperExecutionWindowAuthorityRecord).where(
                    PaperExecutionWindowAuthorityRecord.id == WINDOW_ID
                )
            )
        assert root is not None
        invalid_window_id = UUID("63000000-0000-0000-0000-000000000099")
        with pytest.raises(DBAPIError, match="incomplete or inconsistent"):
            async with sessions.begin() as session:
                session.add(
                    PaperExecutionWindowAuthorityRecord(
                        id=invalid_window_id,
                        order_id=order_id,
                        account_id=root.account_id,
                        instrument_id=root.instrument_id,
                        window_seq=2,
                        window_start_at=OPEN + timedelta(minutes=5),
                        window_end_at=OPEN + timedelta(minutes=10),
                        evaluated_at=FIRST_WINDOW_EVALUATED + timedelta(minutes=5),
                        data_available_at=EVALUATED,
                        source_authority_id=root.source_authority_id,
                        source=root.source,
                        source_content_hash=root.source_content_hash,
                        feed=root.feed,
                        delivery_mode=root.delivery_mode,
                        vwap=Decimal("100"),
                        volume=Decimal("4000"),
                        capacity=Decimal("40"),
                        quote_id=None,
                        quote_revision=None,
                        bid=None,
                        ask=None,
                        half_spread=Decimal(0),
                        content_hash="8" * 64,
                    )
                )
                session.add_all(
                    PaperExecutionWindowMemberRecord(
                        window_authority_id=invalid_window_id,
                        bar_id=bar_id,
                        member_seq=index,
                    )
                    for index, bar_id in enumerate(bar_ids[:4], start=1)
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_execution_window_chain_rejects_backward_evaluation_time(
    migrated_database_url: str,
) -> None:
    order_id, _ = await _seed_valid_window(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions() as read_session:
            root = await read_session.scalar(
                select(PaperExecutionWindowAuthorityRecord).where(
                    PaperExecutionWindowAuthorityRecord.id == WINDOW_ID
                )
            )
        assert root is not None
        with pytest.raises(DBAPIError, match="execution window chain is invalid"):
            async with sessions.begin() as session:
                session.add(
                    PaperExecutionWindowAuthorityRecord(
                        id=UUID("63000000-0000-0000-0000-000000000098"),
                        order_id=order_id,
                        account_id=root.account_id,
                        instrument_id=root.instrument_id,
                        window_seq=2,
                        window_start_at=OPEN + timedelta(minutes=5),
                        window_end_at=OPEN + timedelta(minutes=10),
                        evaluated_at=EVALUATED + timedelta(minutes=5),
                        data_available_at=EVALUATED,
                        source_authority_id=root.source_authority_id,
                        source=root.source,
                        source_content_hash=root.source_content_hash,
                        feed=root.feed,
                        delivery_mode=root.delivery_mode,
                        vwap=Decimal("100"),
                        volume=Decimal("5000"),
                        capacity=Decimal("50"),
                        quote_id=None,
                        quote_revision=None,
                        bid=None,
                        ask=None,
                        half_spread=Decimal(0),
                        content_hash="8" * 64,
                    )
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_execution_window_rejects_future_cross_instrument_and_source_switch(
    migrated_database_url: str,
) -> None:
    await _seed_valid_window(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions() as read_session:
            root = await read_session.scalar(
                select(PaperExecutionWindowAuthorityRecord).where(
                    PaperExecutionWindowAuthorityRecord.id == WINDOW_ID
                )
            )
            other_instrument_id = await read_session.scalar(
                text("SELECT id FROM instruments WHERE id <> :id ORDER BY id LIMIT 1"),
                {"id": root.instrument_id if root is not None else UUID(int=0)},
            )
        assert root is not None
        assert other_instrument_id is not None

        variants = (
            {
                "id": UUID("63000000-0000-0000-0000-0000000000a1"),
                "data_available_at": EVALUATED + timedelta(seconds=1),
            },
            {
                "id": UUID("63000000-0000-0000-0000-0000000000a2"),
                "source": "switched-source",
            },
            {
                "id": UUID("63000000-0000-0000-0000-0000000000a3"),
                "instrument_id": other_instrument_id,
            },
        )
        for sequence, overrides in enumerate(variants, start=2):
            values = {
                "id": overrides["id"],
                "order_id": root.order_id,
                "account_id": root.account_id,
                "instrument_id": root.instrument_id,
                "window_seq": sequence,
                "window_start_at": OPEN + timedelta(minutes=5 * (sequence - 1)),
                "window_end_at": OPEN + timedelta(minutes=5 * sequence),
                "evaluated_at": EVALUATED,
                "data_available_at": EVALUATED,
                "source_authority_id": root.source_authority_id,
                "source": root.source,
                "source_content_hash": root.source_content_hash,
                "feed": root.feed,
                "delivery_mode": root.delivery_mode,
                "vwap": Decimal("100"),
                "volume": Decimal("5000"),
                "capacity": Decimal("50"),
                "quote_id": None,
                "quote_revision": None,
                "bid": None,
                "ask": None,
                "half_spread": Decimal(0),
                "content_hash": "9" * 64,
            }
            values.update(overrides)
            with pytest.raises(DBAPIError):
                async with sessions.begin() as session:
                    session.add(PaperExecutionWindowAuthorityRecord(**values))
    finally:
        await engine.dispose()
