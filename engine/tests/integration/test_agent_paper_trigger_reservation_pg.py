from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from nexus.paper import agent_cycle
from nexus.paper.agent_cycle_models import AgentPaperCycleResult
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from engine.tests.integration.test_agent_paper_candidate_batch_pg import (
    AAPL_ID,
    ACCOUNT_ID,
    NOW,
    UNIVERSE_ID,
    _seed_scope,
)


async def _seed_trigger(database_url):
    await _seed_scope(database_url)
    engine = create_async_engine(database_url)
    try:
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        async with sessions.begin() as session:
            batch = await create_agent_paper_candidate_batch(
                session,
                account_id=ACCOUNT_ID,
                universe_snapshot_id=UNIVERSE_ID,
                instrument_ids=(AAPL_ID,),
                idempotency_key="scope",
                created_at=NOW,
            )
            return SimpleNamespace(account_id=ACCOUNT_ID, batch_id=batch.batch_id)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_empty_trigger_reservation_stops_replay_as_unknown(
    migrated_database_url: str,
) -> None:
    seed = await _seed_trigger(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    now = datetime(2026, 9, 18, 14, 1, tzinfo=UTC)
    try:
        async with sessions.begin() as session:
            reservation = await agent_cycle.reserve_schwab_agent_paper_trigger(
                session,
                account_id=seed.account_id,
                candidate_batch_id=seed.batch_id,
                client_idempotency_key="slot-one",
                now=now,
            )
            assert reservation.decision_slot_at == datetime(2026, 9, 18, 14, 0, tzinfo=UTC)

        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="AGENT_PAPER_RESULT_UNKNOWN"):
                await agent_cycle.reserve_schwab_agent_paper_trigger(
                    session,
                    account_id=seed.account_id,
                    candidate_batch_id=seed.batch_id,
                    client_idempotency_key="slot-one",
                    now=now,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_finished_trigger_reservation_replays_fixed_result(
    migrated_database_url: str,
) -> None:
    seed = await _seed_trigger(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    now = datetime(2026, 9, 18, 14, 1, tzinfo=UTC)
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
                candidate_batch_id=seed.batch_id,
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
                candidate_batch_id=seed.batch_id,
                client_idempotency_key="slot-one",
                now=now,
            )

        assert replay.replay == expected.model_copy(update={"replayed": True})
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_code",
    ["SCHWAB_L1_INCOMPLETE", "AGENT_PAPER_PROVIDER_CAPACITY_EXCEEDED"],
)
async def test_failed_trigger_reservation_replays_safe_error(
    migrated_database_url: str,
    error_code: str,
) -> None:
    seed = await _seed_trigger(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    now = datetime(2026, 9, 18, 14, 1, tzinfo=UTC)
    try:
        async with sessions.begin() as session:
            reservation = await agent_cycle.reserve_schwab_agent_paper_trigger(
                session,
                account_id=seed.account_id,
                candidate_batch_id=seed.batch_id,
                client_idempotency_key="slot-one",
                now=now,
            )
            await agent_cycle.finish_schwab_agent_paper_trigger(
                session, reservation=reservation, error_code=error_code
            )

        async with sessions.begin() as session:
            with pytest.raises(ValueError, match=error_code):
                await agent_cycle.reserve_schwab_agent_paper_trigger(
                    session,
                    account_id=seed.account_id,
                    candidate_batch_id=seed.batch_id,
                    client_idempotency_key="slot-one",
                    now=now,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_invalid_provider_capacity_cannot_be_sealed_as_terminal_failure(
    migrated_database_url: str,
) -> None:
    seed = await _seed_trigger(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    now = datetime(2026, 9, 18, 14, 1, tzinfo=UTC)
    try:
        async with sessions.begin() as session:
            reservation = await agent_cycle.reserve_schwab_agent_paper_trigger(
                session,
                account_id=seed.account_id,
                candidate_batch_id=seed.batch_id,
                client_idempotency_key="invalid-capacity",
                now=now,
            )
            with pytest.raises(ValueError, match="^AGENT_PAPER_REQUEST_INVALID$"):
                await agent_cycle.finish_schwab_agent_paper_trigger(
                    session,
                    reservation=reservation,
                    error_code="AGENT_PAPER_PROVIDER_CAPACITY_INVALID",
                )

        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="^AGENT_PAPER_RESULT_UNKNOWN$"):
                await agent_cycle.reserve_schwab_agent_paper_trigger(
                    session,
                    account_id=seed.account_id,
                    candidate_batch_id=seed.batch_id,
                    client_idempotency_key="invalid-capacity",
                    now=now,
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_trigger_reservation_rejects_another_key_but_allows_next_slot(
    migrated_database_url: str,
) -> None:
    seed = await _seed_trigger(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            first = await agent_cycle.reserve_schwab_agent_paper_trigger(
                session,
                account_id=seed.account_id,
                candidate_batch_id=seed.batch_id,
                client_idempotency_key="slot-one",
                now=datetime(2026, 9, 18, 14, 1, tzinfo=UTC),
            )

        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="AGENT_PAPER_IDEMPOTENCY_CONFLICT"):
                await agent_cycle.reserve_schwab_agent_paper_trigger(
                    session,
                    account_id=seed.account_id,
                    candidate_batch_id=seed.batch_id,
                    client_idempotency_key="different-key",
                    now=datetime(2026, 9, 18, 14, 4, 59, tzinfo=UTC),
                )

        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="AGENT_PAPER_OUTSIDE_WINDOW"):
                await agent_cycle.reserve_schwab_agent_paper_trigger(
                    session,
                    account_id=seed.account_id,
                    candidate_batch_id=seed.batch_id,
                    client_idempotency_key="slot-two",
                    now=datetime(2026, 9, 18, 14, 6, tzinfo=UTC),
                )
            claim_count = await session.scalar(
                select(func.count())
                .select_from(agent_cycle.IdempotencyRecord)
                .where(agent_cycle.IdempotencyRecord.route == "agent-paper-schwab-trigger-v2")
            )

        assert first.decision_slot_at == datetime(2026, 9, 18, 14, 0, tzinfo=UTC)
        assert claim_count == 1
    finally:
        await engine.dispose()
