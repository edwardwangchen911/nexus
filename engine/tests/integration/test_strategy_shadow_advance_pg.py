"""Exact-session atomic advances for the isolated Strategy Shadow."""

from datetime import UTC, datetime, time

import pytest
from helpers.evolution_shadow import (
    seed_strategy_shadow_ready,
    seed_strategy_shadow_session_evidence,
)
from nexus.evolution.shadow.service import advance_strategy_shadow, start_strategy_shadow
from nexus.persistence.models.evolution import (
    StrategyShadowAdvanceAttemptRecord,
    StrategyShadowLegStateRecord,
    StrategyShadowSessionAuthorityRecord,
    StrategyShadowSessionPriceInputRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text


async def _protected_counts(session) -> tuple[int, ...]:
    counts: list[int] = []
    for table in (
        "shadow_groups",
        "shadow_portfolios",
        "shadow_portfolio_states",
        "paper_orders",
        "paper_order_events",
        "broker_order_snapshots",
        "broker_sync_requests",
    ):
        counts.append(int(await session.scalar(text(f"SELECT count(*) FROM {table}")) or 0))
    return tuple(counts)


@pytest.mark.asyncio
async def test_advance_persists_one_shared_authority_and_two_isolated_leg_states(
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
    requested_session = started.start_session
    observed_at = datetime.combine(requested_session, time(23), tzinfo=UTC)
    async with factory() as session, session.begin():
        await seed_strategy_shadow_session_evidence(
            session,
            group_id=started.group_id,
            session_date=requested_session,
            ordinal=1,
            observed_at=observed_at,
        )
    async with factory() as session:
        before = await _protected_counts(session)
    async with factory() as session, session.begin():
        result = await advance_strategy_shadow(
            session,
            group_id=started.group_id,
            requested_session=requested_session,
            observed_at=observed_at,
        )
    async with factory() as session, session.begin():
        replay = await advance_strategy_shadow(
            session,
            group_id=started.group_id,
            requested_session=requested_session,
            observed_at=observed_at,
        )
    async with factory() as session:
        authorities = tuple(
            await session.scalars(
                select(StrategyShadowSessionAuthorityRecord).where(
                    StrategyShadowSessionAuthorityRecord.group_id == started.group_id
                )
            )
        )
        states = tuple(
            await session.scalars(
                select(StrategyShadowLegStateRecord)
                .join(
                    StrategyShadowSessionAuthorityRecord,
                    StrategyShadowSessionAuthorityRecord.id
                    == StrategyShadowLegStateRecord.session_authority_id,
                )
                .where(StrategyShadowSessionAuthorityRecord.group_id == started.group_id)
            )
        )
        price_input_count = await session.scalar(
            select(func.count()).select_from(StrategyShadowSessionPriceInputRecord)
        )
        attempt_statuses = tuple(
            await session.scalars(
                select(StrategyShadowAdvanceAttemptRecord.status)
                .where(StrategyShadowAdvanceAttemptRecord.group_id == started.group_id)
                .order_by(StrategyShadowAdvanceAttemptRecord.attempt_number)
            )
        )
        after = await _protected_counts(session)

    assert result.status == "applied" and result.ordinal == 1
    assert result.live_execution_enabled is False
    assert replay.status == "duplicate" and replay.ordinal == 1
    assert len(authorities) == 1
    assert len(states) == 2
    assert {item.session_authority_id for item in states} == {authorities[0].id}
    assert len({item.state_hash for item in states}) == 1
    assert price_input_count == 1
    assert attempt_statuses == ("applied", "duplicate")
    assert after == before


@pytest.mark.asyncio
async def test_out_of_order_session_seals_the_group_without_partial_leg_state(
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
    observed_at = datetime.combine(seed.calendar_sessions[1], time(23), tzinfo=UTC)
    async with factory() as session, session.begin():
        failed = await advance_strategy_shadow(
            session,
            group_id=started.group_id,
            requested_session=seed.calendar_sessions[1],
            observed_at=observed_at,
        )
    async with factory() as session, session.begin():
        sealed = await advance_strategy_shadow(
            session,
            group_id=started.group_id,
            requested_session=seed.calendar_sessions[0],
            observed_at=observed_at,
        )
    async with factory() as session:
        state_count = int(
            await session.scalar(select(func.count()).select_from(StrategyShadowLegStateRecord))
            or 0
        )
        authority_count = int(
            await session.scalar(
                select(func.count()).select_from(StrategyShadowSessionAuthorityRecord)
            )
            or 0
        )

    assert failed.status == "failed"
    assert failed.reason_code == "STRATEGY_SHADOW_SESSION_OUT_OF_ORDER"
    assert sealed.status == "sealed"
    assert sealed.reason_code == "STRATEGY_SHADOW_DATA_GAP_PERMANENT"
    assert state_count == 0 and authority_count == 0
