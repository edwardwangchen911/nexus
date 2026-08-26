import asyncio

from helpers.paper_batch import seed_paper_batch
from nexus.paper.service import create_paper_order_batch
from nexus.persistence.models.paper import PaperOrderBatchRecord, PaperOrderRecord
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine


async def _create_concurrently(database_url: str):
    seed = await seed_paper_batch(database_url)
    engine = create_async_engine(database_url, pool_size=8, max_overflow=0)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async def invoke(index: int):
        async with sessions.begin() as session:
            return await create_paper_order_batch(
                session,
                account_id=seed.account_id,
                strategy_decision_id=seed.decision_id,
                idempotency_key=f"paper-concurrent-{index}",
                request_time=seed.request_time,
            )

    try:
        results = await asyncio.gather(*(invoke(index) for index in range(8)))
        async with sessions() as session:
            batch_count = await session.scalar(
                select(func.count()).select_from(PaperOrderBatchRecord)
            )
            order_count = await session.scalar(select(func.count()).select_from(PaperOrderRecord))
        return results, batch_count, order_count
    finally:
        await engine.dispose()


def test_eight_sessions_converge_to_one_batch_without_duplicate_orders(
    migrated_database_url: str,
) -> None:
    results, batch_count, order_count = asyncio.run(
        _create_concurrently(migrated_database_url)
    )

    assert len({result.batch_id for result in results}) == 1
    assert len({result.batch_hash for result in results}) == 1
    assert batch_count == 1
    assert order_count == 3
