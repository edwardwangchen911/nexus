"""Serialization races for exact-session Strategy Shadow advances."""

import asyncio
from datetime import UTC, datetime, time

import pytest
from helpers.evolution_shadow import (
    seed_strategy_shadow_ready,
    seed_strategy_shadow_session_evidence,
)
from nexus.evolution.shadow.service import advance_strategy_shadow, start_strategy_shadow
from nexus.persistence.models.evolution import (
    StrategyShadowLegStateRecord,
    StrategyShadowSessionAuthorityRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_concurrent_same_session_advance_commits_one_shared_fact_root(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    seed = await seed_strategy_shadow_ready(migrated_database_url, factory)
    async with factory() as session, session.begin():
        started = await start_strategy_shadow(
            session,
            experiment_id=seed.pair.experiment_id,
            now=seed.now,
        )
    observed_at = datetime.combine(started.start_session, time(23), tzinfo=UTC)
    async with factory() as session, session.begin():
        await seed_strategy_shadow_session_evidence(
            session,
            group_id=started.group_id,
            session_date=started.start_session,
            ordinal=1,
            observed_at=observed_at,
        )

    async def advance_once():
        async with factory() as session, session.begin():
            return await advance_strategy_shadow(
                session,
                group_id=started.group_id,
                requested_session=started.start_session,
                observed_at=observed_at,
            )

    first, second = await asyncio.gather(advance_once(), advance_once())
    async with factory() as session:
        authority_count = int(
            await session.scalar(
                select(func.count())
                .select_from(StrategyShadowSessionAuthorityRecord)
                .where(StrategyShadowSessionAuthorityRecord.group_id == started.group_id)
            )
            or 0
        )
        state_count = int(
            await session.scalar(select(func.count()).select_from(StrategyShadowLegStateRecord))
            or 0
        )

    assert sorted((first.status, second.status)) == ["applied", "duplicate"]
    assert authority_count == 1
    assert state_count == 2
