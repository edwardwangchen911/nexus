"""Concurrent automatic promotion requests serialize to one registry event."""

import asyncio

import pytest
from helpers.evolution_registry import seed_promotable_registry
from nexus.evolution.registry import promote_internal_paper_champion
from nexus.persistence.models.evolution import StrategyRegistryEventRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_concurrent_same_candidate_promotions_create_one_pending_event(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    seed = await seed_promotable_registry(migrated_database_url, factory)

    async def promote_once(idempotency_key: str):
        async with factory() as session, session.begin():
            return await promote_internal_paper_champion(
                session,
                experiment_id=seed.shadow.pair.experiment_id,
                expected_generation=seed.expected_generation,
                expected_version=seed.expected_version,
                effective_session=seed.effective_session,
                idempotency_key=idempotency_key,
                now=seed.now,
            )

    first, second = await asyncio.gather(
        promote_once("concurrent-promotion-a"),
        promote_once("concurrent-promotion-b"),
    )
    async with factory() as session:
        promote_count = int(
            await session.scalar(
                select(func.count())
                .select_from(StrategyRegistryEventRecord)
                .where(StrategyRegistryEventRecord.event_type == "PROMOTE")
            )
            or 0
        )

    assert first.status == second.status == "PENDING_BOUNDARY"
    assert first.pending_event_id == second.pending_event_id
    assert promote_count == 1
