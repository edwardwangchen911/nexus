"""Read real synthetic paper facts without changing policies or writing during reads."""

from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.paper_batch import INSTRUMENTS
from helpers.paper_execution import seed_paper_execution
from nexus.paper.execution_service import advance_internal_paper_order
from nexus.persistence.models.paper import PaperPositionAdjustmentEventRecord
from nexus.strategy.rank_continuity import load_held_position_continuity
from sqlalchemy import event
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine


@pytest.mark.asyncio
async def test_pg_verified_fill_and_exact_time_boundaries(migrated_database_url, monkeypatch):
    seed = await seed_paper_execution(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    factory = async_sessionmaker(engine)
    at = seed.evaluation_times[0]
    try:
        # Historical synthetic execution fixture only; no production gate is modified.
        with monkeypatch.context() as patch:
            patch.setattr(
                "nexus.paper.execution_service._paper_execution_authority_available", lambda _: True
            )
            async with factory.begin() as session:
                await advance_internal_paper_order(
                    session,
                    order_id=seed.order_id,
                    evaluated_at=at,
                    idempotency_key="rank-continuity-fill",
                )
        statements = []

        def record(conn, cursor, statement, parameters, context, executemany):
            statements.append(statement.lstrip().split(None, 1)[0].upper())

        event.listen(engine.sync_engine, "before_cursor_execute", record)
        async with factory.begin() as session:
            before = await load_held_position_continuity(
                session,
                account_id=seed.account_id,
                previous_cutoff=None,
                cutoff=at - timedelta(seconds=1),
            )
            assert before.positions == ()
            result = await load_held_position_continuity(
                session,
                account_id=seed.account_id,
                previous_cutoff=at - timedelta(seconds=1),
                cutoff=at,
            )
            assert len(result.positions) == 1
            position = result.positions[0]
            assert position.kind == "opened" and position.opening_fill_id is not None
            assert position.quantity_at_previous == 0
            assert position.quantity_at_cutoff == seed.quantity
            later = await load_held_position_continuity(
                session,
                account_id=seed.account_id,
                previous_cutoff=at,
                cutoff=at + timedelta(seconds=1),
            )
            assert later.positions[0].kind == "continuous"
            assert later.positions[0].previous_prefix_hash == position.prefix_hash
        async with factory() as session:
            pending = PaperPositionAdjustmentEventRecord(id=UUID(int=98765))
            session.add(pending)
            await load_held_position_continuity(
                session, account_id=seed.account_id, previous_cutoff=None, cutoff=at
            )
            assert pending in session.new
            await session.rollback()
        assert set(statements) == {"SELECT"}
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("late", [False, True])
async def test_pg_nonfill_opening_and_late_history_are_not_zero_initialization(
    migrated_database_url, late
):
    seed = await seed_paper_execution(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    factory = async_sessionmaker(engine)
    at = seed.evaluation_times[0]
    try:
        async with factory.begin() as session:
            session.add(
                PaperPositionAdjustmentEventRecord(
                    id=UUID(int=7654),
                    account_id=seed.account_id,
                    instrument_id=INSTRUMENTS[1][0],
                    event_seq=1,
                    event_type="correction",
                    quantity_delta=Decimal(10),
                    cost_delta=Decimal(100),
                    source_fill_id=None,
                    occurred_at=at - timedelta(seconds=2),
                    payload_hash="a" * 64,
                    created_at=at if late else at - timedelta(seconds=3),
                )
            )
        async with factory.begin() as session:
            if late:
                with pytest.raises(ValueError, match="BACKDATED"):
                    await load_held_position_continuity(
                        session,
                        account_id=seed.account_id,
                        previous_cutoff=at - timedelta(seconds=1),
                        cutoff=at,
                    )
            else:
                result = await load_held_position_continuity(
                    session,
                    account_id=seed.account_id,
                    previous_cutoff=at - timedelta(seconds=1),
                    cutoff=at,
                )
                assert result.positions[0].kind == "unavailable"
                assert result.positions[0].reason == "NON_FILL_OPENING"
    finally:
        await engine.dispose()
