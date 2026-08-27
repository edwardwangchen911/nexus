"""Atomic same-start initialization for isolated Strategy Shadow legs."""

import pytest
from helpers.evolution_shadow import seed_strategy_shadow_ready
from nexus.evolution.shadow.service import start_strategy_shadow
from nexus.persistence.models.evolution import (
    StrategyExperimentStateProjectionRecord,
    StrategyShadowGroupRecord,
    StrategyShadowLegRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text


async def _protected_counts(session) -> tuple[int, ...]:
    counts: list[int] = []
    for table in (
        "shadow_groups",
        "shadow_portfolios",
        "paper_performance_snapshots",
        "paper_position_snapshots",
        "paper_orders",
        "broker_order_snapshots",
    ):
        counts.append(int(await session.scalar(text(f"SELECT count(*) FROM {table}")) or 0))
    return tuple(counts)


@pytest.mark.asyncio
async def test_start_clones_one_balanced_snapshot_into_two_identical_virtual_legs(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    seed = await seed_strategy_shadow_ready(migrated_database_url, factory)
    async with factory() as session:
        before = await _protected_counts(session)
    async with factory() as session, session.begin():
        result = await start_strategy_shadow(
            session,
            experiment_id=seed.pair.experiment_id,
            now=seed.now,
        )
    async with factory() as session, session.begin():
        replay = await start_strategy_shadow(
            session,
            experiment_id=seed.pair.experiment_id,
            now=seed.now,
        )
    async with factory() as session:
        group = await session.get(StrategyShadowGroupRecord, result.group_id)
        legs = tuple(
            await session.scalars(
                select(StrategyShadowLegRecord)
                .where(StrategyShadowLegRecord.group_id == result.group_id)
                .order_by(StrategyShadowLegRecord.role)
            )
        )
        projection = await session.get(StrategyExperimentStateProjectionRecord, seed.pair.family_id)
        group_count = await session.scalar(
            select(func.count()).select_from(StrategyShadowGroupRecord)
        )
        after = await _protected_counts(session)

    assert replay == result
    assert result.initial_nav == 100
    assert result.live_execution_enabled is False
    assert (result.start_session, result.evaluation_end_session, result.grace_end_session) == (
        seed.calendar_sessions[0],
        seed.calendar_sessions[62],
        seed.calendar_sessions[67],
    )
    assert group is not None
    assert group.baseline_snapshot_id == seed.baseline_snapshot_id
    assert group.evaluation_sessions == 63 and group.grace_sessions == 5
    assert [leg.role for leg in legs] == ["challenger", "champion"]
    assert len({leg.initial_state_hash for leg in legs}) == 1
    assert projection is not None and projection.current_state == "SHADOW_RUNNING"
    assert group_count == 1
    assert after == before
