from datetime import UTC, datetime
from hashlib import sha256
from types import SimpleNamespace

import pytest
from nexus.jobs.models import job_input_hash
from nexus.paper import agent_cycle
from nexus.paper.agent_cycle_models import AgentPaperCycleResult
from nexus.paper.candidate_batch import create_agent_paper_candidate_batch
from nexus.persistence.models.paper import (
    PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID,
    AgentPaperCandidateBatchRecord,
)
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


@pytest.mark.asyncio
async def test_manual_trigger_uses_new_market_slot_without_replaying_failed_scheduled_slot(
    migrated_database_url: str,
) -> None:
    seed = await _seed_trigger(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    scheduled_at = datetime(2026, 9, 18, 14, 1, tzinfo=UTC)
    manual_at = datetime(2026, 9, 18, 16, 1, tzinfo=UTC)
    try:
        async with sessions.begin() as session:
            scheduled = await agent_cycle.reserve_schwab_agent_paper_trigger(
                session,
                account_id=seed.account_id,
                candidate_batch_id=seed.batch_id,
                client_idempotency_key="scheduled-failed",
                now=scheduled_at,
            )
            scheduled_claim = await session.get(agent_cycle.IdempotencyRecord, scheduled.claim_id)
            batch = await session.get(AgentPaperCandidateBatchRecord, seed.batch_id)
            assert scheduled_claim is not None and batch is not None
            assert scheduled_claim.request_hash == job_input_hash(
                {
                    "schema": "agent-paper-schwab-trigger-v2",
                    "account_id": str(seed.account_id),
                    "decision_session": "2026-09-18",
                    "decision_slot_at": "2026-09-18T14:00:00+00:00",
                    "source_policy_id": str(PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID),
                    "client_idempotency_hash": sha256(b"scheduled-failed").hexdigest(),
                    "candidate_batch_id": str(seed.batch_id),
                    "candidate_batch_hash": batch.batch_hash,
                }
            )
            await agent_cycle.finish_schwab_agent_paper_trigger(
                session, reservation=scheduled, error_code="SCHWAB_L1_INCOMPLETE"
            )

        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="AGENT_PAPER_OUTSIDE_WINDOW"):
                await agent_cycle.reserve_schwab_agent_paper_trigger(
                    session,
                    account_id=seed.account_id,
                    candidate_batch_id=seed.batch_id,
                    client_idempotency_key="ordinary-late",
                    now=manual_at,
                )
            manual = await agent_cycle.reserve_schwab_agent_paper_trigger(
                session,
                account_id=seed.account_id,
                candidate_batch_id=seed.batch_id,
                client_idempotency_key="explicit-manual",
                now=manual_at,
                trigger_mode="manual_recovery",
            )
            assert manual.decision_slot_at == datetime(2026, 9, 18, 16, 0, tzinfo=UTC)
            assert manual.decision_slot_at != scheduled.decision_slot_at

        async with sessions.begin() as session:
            await agent_cycle.finish_schwab_agent_paper_trigger(
                session, reservation=manual, error_code="SCHWAB_L1_INCOMPLETE"
            )

        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="SCHWAB_L1_INCOMPLETE"):
                await agent_cycle.reserve_schwab_agent_paper_trigger(
                    session,
                    account_id=seed.account_id,
                    candidate_batch_id=seed.batch_id,
                    client_idempotency_key="explicit-manual",
                    now=manual_at,
                    trigger_mode="manual_recovery",
                )
            with pytest.raises(ValueError, match="AGENT_PAPER_IDEMPOTENCY_CONFLICT"):
                await agent_cycle.reserve_schwab_agent_paper_trigger(
                    session,
                    account_id=seed.account_id,
                    candidate_batch_id=seed.batch_id,
                    client_idempotency_key="different-manual-key",
                    now=manual_at,
                    trigger_mode="manual_recovery",
                )
            with pytest.raises(ValueError, match="AGENT_PAPER_IDEMPOTENCY_CONFLICT"):
                await agent_cycle.reserve_schwab_agent_paper_trigger(
                    session,
                    account_id=seed.account_id,
                    candidate_batch_id=seed.batch_id,
                    client_idempotency_key="explicit-manual",
                    now=datetime(2026, 9, 18, 16, 6, tzinfo=UTC),
                    trigger_mode="manual_recovery",
                )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_manual_recovery_requires_failed_scheduled_claim_and_keeps_db_unchanged(
    migrated_database_url: str,
) -> None:
    seed = await _seed_trigger(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="AGENT_PAPER_MANUAL_RECOVERY_UNAVAILABLE"):
                await agent_cycle.reserve_schwab_agent_paper_trigger(
                    session,
                    account_id=seed.account_id,
                    candidate_batch_id=seed.batch_id,
                    client_idempotency_key="no-failed-slot",
                    now=datetime(2026, 9, 18, 16, 1, tzinfo=UTC),
                    trigger_mode="manual_recovery",
                )
            claim_count = await session.scalar(
                select(func.count())
                .select_from(agent_cycle.IdempotencyRecord)
                .where(agent_cycle.IdempotencyRecord.route == "agent-paper-manual-trigger-v1")
            )
            assert claim_count == 0
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_manual_recovery_rejects_non_market_scheduled_failure(
    migrated_database_url: str,
) -> None:
    seed = await _seed_trigger(migrated_database_url)
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            scheduled = await agent_cycle.reserve_schwab_agent_paper_trigger(
                session,
                account_id=seed.account_id,
                candidate_batch_id=seed.batch_id,
                client_idempotency_key="scheduled-invalid",
                now=datetime(2026, 9, 18, 14, 1, tzinfo=UTC),
            )
            await agent_cycle.finish_schwab_agent_paper_trigger(
                session, reservation=scheduled, error_code="CANDIDATE_BATCH_INVALID"
            )
        async with sessions.begin() as session:
            with pytest.raises(ValueError, match="AGENT_PAPER_MANUAL_RECOVERY_UNAVAILABLE"):
                await agent_cycle.reserve_schwab_agent_paper_trigger(
                    session,
                    account_id=seed.account_id,
                    candidate_batch_id=seed.batch_id,
                    client_idempotency_key="manual-invalid",
                    now=datetime(2026, 9, 18, 16, 1, tzinfo=UTC),
                    trigger_mode="manual_recovery",
                )
    finally:
        await engine.dispose()
