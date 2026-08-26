import asyncio
from datetime import timedelta
from uuid import UUID

import pytest
from helpers.paper_batch import seed_paper_batch
from nexus.paper.outbox import PaperOutboxMessage, dispatch_paper_outbox
from nexus.paper.service import PaperBatchHooks, create_paper_order_batch
from nexus.persistence.models.paper import (
    PaperFillRecord,
    PaperLedgerTransactionRecord,
    PaperOrderBatchRecord,
    PaperOrderRecord,
    PaperReservationRecord,
    RiskEvaluationRecord,
)
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord, OutboxEvent
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

PERSISTENCE_PHASES = (
    "batch_persisted",
    "orders_persisted",
    "risk_persisted",
    "reservations_and_ledger_persisted",
    "audit_outbox_persisted",
    "idempotency_completed",
)


async def _fail_and_count(database_url: str, fail_phase: str):
    seed = await seed_paper_batch(database_url)
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    phases: list[str] = []

    def observe(phase: str) -> None:
        phases.append(phase)
        if phase == fail_phase:
            raise RuntimeError("injected paper persistence failure")

    try:
        with pytest.raises(RuntimeError, match="injected paper persistence failure"):
            async with sessions.begin() as session:
                await create_paper_order_batch(
                    session,
                    account_id=seed.account_id,
                    strategy_decision_id=seed.decision_id,
                    idempotency_key=f"paper-fault-{fail_phase}",
                    request_time=seed.request_time,
                    hooks=PaperBatchHooks(after_phase=observe),
                )
        async with sessions() as session:
            counts = []
            for model in (
                PaperOrderBatchRecord,
                PaperOrderRecord,
                RiskEvaluationRecord,
                PaperReservationRecord,
                PaperFillRecord,
                IdempotencyRecord,
            ):
                counts.append(
                    int(await session.scalar(select(func.count()).select_from(model)) or 0)
                )
            ledger_count = int(
                await session.scalar(
                    select(func.count()).select_from(PaperLedgerTransactionRecord)
                )
                or 0
            )
            paper_audits = int(
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.schema_version == "paper_event_v1")
                )
                or 0
            )
            paper_outbox = int(
                await session.scalar(
                    select(func.count())
                    .select_from(OutboxEvent)
                    .where(OutboxEvent.schema_version == "paper_event_v1")
                )
                or 0
            )
        return phases, tuple(counts), ledger_count, paper_audits, paper_outbox
    finally:
        await engine.dispose()


@pytest.mark.parametrize("fail_phase", PERSISTENCE_PHASES)
def test_injected_failure_rolls_back_the_entire_task18_fact_set(
    migrated_database_url: str,
    fail_phase: str,
) -> None:
    phases, counts, ledger_count, paper_audits, paper_outbox = asyncio.run(
        _fail_and_count(migrated_database_url, fail_phase)
    )

    assert fail_phase in phases
    assert counts == (0, 0, 0, 0, 0, 0)
    assert ledger_count == 1
    assert paper_audits == 0
    assert paper_outbox == 0


async def _publish_failure_then_replay(database_url: str):
    seed = await seed_paper_batch(database_url)
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    delivered: list[UUID] = []

    async def fail_publish(message: PaperOutboxMessage) -> None:
        raise RuntimeError(f"publisher unavailable for {message.event_id}")

    async def record_publish(message: PaperOutboxMessage) -> None:
        delivered.append(message.event_id)

    try:
        async with sessions.begin() as session:
            await create_paper_order_batch(
                session,
                account_id=seed.account_id,
                strategy_decision_id=seed.decision_id,
                idempotency_key="paper-outbox-replay",
                request_time=seed.request_time,
            )

        async with sessions() as session:
            first_available_at = await session.scalar(
                select(func.min(OutboxEvent.available_at)).where(
                    OutboxEvent.schema_version == "paper_event_v1",
                    OutboxEvent.published_at.is_(None),
                )
            )
        assert first_available_at is not None
        failure_at = first_available_at + timedelta(seconds=1)

        with pytest.raises(RuntimeError, match="publisher unavailable"):
            async with sessions.begin() as session:
                await dispatch_paper_outbox(
                    session,
                    publisher=fail_publish,
                    published_at=failure_at,
                )

        async with sessions() as session:
            batch_count = int(
                await session.scalar(select(func.count()).select_from(PaperOrderBatchRecord))
                or 0
            )
            unpublished_after_failure = int(
                await session.scalar(
                    select(func.count())
                    .select_from(OutboxEvent)
                    .where(
                        OutboxEvent.schema_version == "paper_event_v1",
                        OutboxEvent.published_at.is_(None),
                    )
                )
                or 0
            )

        async with sessions.begin() as session:
            first_count = await dispatch_paper_outbox(
                session,
                publisher=record_publish,
                published_at=failure_at + timedelta(minutes=1),
            )
        async with sessions.begin() as session:
            replay_count = await dispatch_paper_outbox(
                session,
                publisher=record_publish,
                published_at=failure_at + timedelta(minutes=2),
            )
        return (
            batch_count,
            unpublished_after_failure,
            first_count,
            replay_count,
            delivered,
        )
    finally:
        await engine.dispose()


def test_outbox_publish_failure_preserves_business_facts_and_replay_is_idempotent(
    migrated_database_url: str,
) -> None:
    batch_count, unpublished, first_count, replay_count, delivered = asyncio.run(
        _publish_failure_then_replay(migrated_database_url)
    )

    assert batch_count == 1
    assert unpublished > 0
    assert first_count == unpublished
    assert replay_count == 0
    assert len(delivered) == len(set(delivered)) == unpublished
