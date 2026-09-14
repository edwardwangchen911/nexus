from datetime import UTC, datetime

import pytest
from helpers.paper_batch import seed_paper_batch
from nexus.paper import agent_cycle
from nexus.paper.agent_cycle_models import AgentPaperCycleResult
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


@pytest.mark.asyncio
async def test_empty_trigger_reservation_stops_replay_as_unknown(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    now = datetime(2026, 9, 14, 14, 1, tzinfo=UTC)
    try:
        async with sessions.begin() as session:
            reservation = await agent_cycle.reserve_schwab_agent_paper_trigger(
                session,
                account_id=seed.account_id,
                client_idempotency_key="slot-one",
                now=now,
            )
            assert reservation.decision_slot_at == datetime(
                2026, 9, 14, 14, 0, tzinfo=UTC
            )

        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="AGENT_PAPER_RESULT_UNKNOWN"):
                await agent_cycle.reserve_schwab_agent_paper_trigger(
                    session,
                    account_id=seed.account_id,
                    client_idempotency_key="slot-one",
                    now=now,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_finished_trigger_reservation_replays_fixed_result(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    now = datetime(2026, 9, 14, 14, 1, tzinfo=UTC)
    expected = AgentPaperCycleResult(
        cycle_id=seed.account_id,
        status="unavailable",
        result_code="RISK_INPUT_REQUIRED",
        replayed=False,
    )
    try:
        async with sessions.begin() as session:
            reservation = await agent_cycle.reserve_schwab_agent_paper_trigger(
                session,
                account_id=seed.account_id,
                client_idempotency_key="slot-one",
                now=now,
            )
            await agent_cycle.finish_schwab_agent_paper_trigger(
                session, reservation=reservation, result=expected
            )

        async with sessions.begin() as session:
            replay = await agent_cycle.reserve_schwab_agent_paper_trigger(
                session,
                account_id=seed.account_id,
                client_idempotency_key="slot-one",
                now=now,
            )

        assert replay.replay == expected.model_copy(update={"replayed": True})
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_failed_trigger_reservation_replays_safe_error(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    now = datetime(2026, 9, 14, 14, 1, tzinfo=UTC)
    try:
        async with sessions.begin() as session:
            reservation = await agent_cycle.reserve_schwab_agent_paper_trigger(
                session,
                account_id=seed.account_id,
                client_idempotency_key="slot-one",
                now=now,
            )
            await agent_cycle.finish_schwab_agent_paper_trigger(
                session, reservation=reservation, error_code="SCHWAB_L1_INCOMPLETE"
            )

        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="SCHWAB_L1_INCOMPLETE"):
                await agent_cycle.reserve_schwab_agent_paper_trigger(
                    session,
                    account_id=seed.account_id,
                    client_idempotency_key="slot-one",
                    now=now,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_trigger_reservation_rejects_another_key_but_allows_next_slot(
    migrated_database_url: str,
) -> None:
    seed = await seed_paper_batch(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            first = await agent_cycle.reserve_schwab_agent_paper_trigger(
                session,
                account_id=seed.account_id,
                client_idempotency_key="slot-one",
                now=datetime(2026, 9, 14, 14, 1, tzinfo=UTC),
            )

        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="AGENT_PAPER_IDEMPOTENCY_CONFLICT"):
                await agent_cycle.reserve_schwab_agent_paper_trigger(
                    session,
                    account_id=seed.account_id,
                    client_idempotency_key="different-key",
                    now=datetime(2026, 9, 14, 14, 4, 59, tzinfo=UTC),
                )

        async with sessions.begin() as session:
            second = await agent_cycle.reserve_schwab_agent_paper_trigger(
                session,
                account_id=seed.account_id,
                client_idempotency_key="slot-two",
                now=datetime(2026, 9, 14, 14, 6, tzinfo=UTC),
            )
            claim_count = await session.scalar(
                select(func.count()).select_from(agent_cycle.IdempotencyRecord)
            )

        assert second.claim_id != first.claim_id
        assert second.decision_slot_at == datetime(2026, 9, 14, 14, 5, tzinfo=UTC)
        assert claim_count == 2
    finally:
        await engine.dispose()
