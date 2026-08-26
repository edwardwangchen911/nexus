import asyncio
from datetime import timedelta

from helpers.paper_batch import seed_paper_batch
from nexus.paper.errors import PaperBatchError
from nexus.paper.service import create_paper_order_batch
from nexus.persistence.models.paper import (
    PaperLedgerTransactionRecord,
    PaperOrderBatchRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperReservationRecord,
)
from nexus.persistence.models.runtime import IdempotencyRecord
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


async def _exercise_idempotency(database_url: str):
    seed = await seed_paper_batch(database_url)
    engine = create_async_engine(database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    try:
        async with sessions.begin() as session:
            first = await create_paper_order_batch(
                session,
                account_id=seed.account_id,
                strategy_decision_id=seed.decision_id,
                idempotency_key="paper-idempotent",
                request_time=seed.request_time,
            )
        async with sessions.begin() as session:
            replay = await create_paper_order_batch(
                session,
                account_id=seed.account_id,
                strategy_decision_id=seed.decision_id,
                idempotency_key="paper-idempotent",
                request_time=seed.request_time,
            )
        async with sessions.begin() as session:
            converged = await create_paper_order_batch(
                session,
                account_id=seed.account_id,
                strategy_decision_id=seed.decision_id,
                idempotency_key="paper-other-key",
                request_time=seed.request_time + timedelta(seconds=1),
            )
        conflict_code = None
        try:
            async with sessions.begin() as session:
                await create_paper_order_batch(
                    session,
                    account_id=seed.account_id,
                    strategy_decision_id=seed.decision_id,
                    idempotency_key="paper-idempotent",
                    request_time=seed.request_time + timedelta(seconds=2),
                )
        except PaperBatchError as error:
            conflict_code = error.code
        async with sessions() as session:
            counts = []
            for model in (
                PaperOrderBatchRecord,
                PaperOrderRecord,
                PaperOrderEventRecord,
                PaperReservationRecord,
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
        return first, replay, converged, conflict_code, tuple(counts), ledger_count
    finally:
        await engine.dispose()


def test_replay_conflict_and_decision_convergence_are_deterministic(
    migrated_database_url: str,
) -> None:
    first, replay, converged, conflict_code, counts, ledger_count = asyncio.run(
        _exercise_idempotency(migrated_database_url)
    )

    assert replay == first
    assert converged == first
    assert conflict_code == "IDEMPOTENCY_CONFLICT"
    assert counts == (1, 3, 8, 2, 2)
    assert ledger_count == 3
