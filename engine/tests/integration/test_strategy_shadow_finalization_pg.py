"""Immutable Strategy Shadow final gates and stale-generation handling."""

from datetime import UTC, datetime, time

import pytest
from helpers.evolution_shadow import (
    seed_complete_strategy_shadow_history,
    seed_strategy_shadow_ready,
)
from nexus.domain.ids import new_uuid7
from nexus.evolution.registry import (
    registry_activation_content_hash,
    registry_event_content_hash,
)
from nexus.evolution.shadow.service import (
    StrategyShadowServiceError,
    finalize_strategy_shadow,
    start_strategy_shadow,
)
from nexus.persistence.models.evolution import (
    StrategyExperimentStateProjectionRecord,
    StrategyPromotionDecisionRecord,
    StrategyRegistryActivationRecord,
    StrategyRegistryEventRecord,
    StrategyRegistryHeadRecord,
    StrategyShadowFinalizationRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text


async def _protected_counts(session) -> tuple[int, ...]:
    counts: list[int] = []
    for table in (
        "paper_orders",
        "paper_order_events",
        "paper_fills",
        "broker_order_snapshots",
        "broker_sync_requests",
        "shadow_groups",
        "shadow_portfolios",
    ):
        counts.append(int(await session.scalar(text(f"SELECT count(*) FROM {table}")) or 0))
    return tuple(counts)


@pytest.mark.asyncio
async def test_complete_pass_finalization_is_idempotent_and_registry_read_only(
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
    async with factory() as session, session.begin():
        await seed_complete_strategy_shadow_history(
            session,
            group_id=started.group_id,
            calendar_sessions=seed.calendar_sessions,
        )
    finalized_at = datetime.combine(seed.calendar_sessions[62], time(23), tzinfo=UTC)
    async with factory() as session:
        before = await _protected_counts(session)
        head_before = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        assert head_before is not None
        head_identity = (
            head_before.active_definition_id,
            head_before.active_generation,
            head_before.last_event_id,
            head_before.event_sequence,
            head_before.version,
        )
    async with factory() as session, session.begin():
        result = await finalize_strategy_shadow(
            session,
            group_id=started.group_id,
            now=finalized_at,
        )
    async with factory() as session, session.begin():
        replay = await finalize_strategy_shadow(
            session,
            group_id=started.group_id,
            now=finalized_at,
        )
    async with factory() as session:
        finalization_count = int(
            await session.scalar(select(func.count()).select_from(StrategyShadowFinalizationRecord))
            or 0
        )
        shadow_decision = await session.scalar(
            select(StrategyPromotionDecisionRecord).where(
                StrategyPromotionDecisionRecord.stage == "shadow"
            )
        )
        projection = await session.get(StrategyExperimentStateProjectionRecord, seed.pair.family_id)
        head_after = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        after = await _protected_counts(session)

    assert replay == result
    assert result.status == "PASS"
    assert result.family_state == "PROMOTION_ELIGIBLE"
    assert result.live_execution_enabled is False
    assert finalization_count == 1
    assert shadow_decision is not None and shadow_decision.status == "PASS"
    assert projection is not None and projection.current_state == "PROMOTION_ELIGIBLE"
    assert head_after is not None
    assert (
        head_after.active_definition_id,
        head_after.active_generation,
        head_after.last_event_id,
        head_after.event_sequence,
        head_after.version,
    ) == head_identity
    assert after == before


@pytest.mark.asyncio
async def test_generation_change_finalizes_stale_without_shadow_promotion_decision(
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
    async with factory() as session, session.begin():
        await seed_complete_strategy_shadow_history(
            session,
            group_id=started.group_id,
            calendar_sessions=seed.calendar_sessions,
        )
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        assert head is not None
        prior = await session.get(StrategyRegistryEventRecord, head.last_event_id)
        assert prior is not None
        effective_session = seed.calendar_sessions[0]
        payload = {"schema_version": "strategy_shadow_stale_fixture_v1"}
        event = StrategyRegistryEventRecord(
            id=new_uuid7(),
            channel="internal_paper",
            schema_version="strategy_registry_event_v1",
            sequence=head.event_sequence + 1,
            event_type="PROMOTE",
            idempotency_key="strategy-shadow-stale-fixture",
            actor_kind="service",
            occurred_at=seed.now,
            from_definition_id=seed.pair.champion_definition_id,
            to_definition_id=seed.pair.challenger_definition_id,
            promotion_decision_id=None,
            champion_generation=head.active_generation + 1,
            effective_session=effective_session,
            payload=payload,
            previous_hash=prior.event_hash,
            event_hash=registry_event_content_hash(
                sequence=head.event_sequence + 1,
                event_type="PROMOTE",
                idempotency_key="strategy-shadow-stale-fixture",
                actor_kind="service",
                occurred_at=seed.now,
                from_definition_id=seed.pair.champion_definition_id,
                to_definition_id=seed.pair.challenger_definition_id,
                promotion_decision_id=None,
                champion_generation=head.active_generation + 1,
                effective_session=effective_session,
                payload=payload,
                previous_hash=prior.event_hash,
            ),
            created_at=seed.now,
        )
        session.add(event)
        await session.flush()
        head.last_event_id = event.id
        head.event_sequence += 1
        head.pending_event_id = event.id
        head.pending_effective_session = effective_session
        head.version += 1
        head.updated_at = seed.now
    async with factory() as session, session.begin():
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        assert head is not None
        session.add(
            StrategyRegistryActivationRecord(
                id=new_uuid7(),
                registry_event_id=event.id,
                decision_session=effective_session,
                activated_at=seed.now,
                from_definition_id=seed.pair.champion_definition_id,
                to_definition_id=seed.pair.challenger_definition_id,
                champion_generation=event.champion_generation,
                activation_hash=registry_activation_content_hash(
                    registry_event_id=event.id,
                    decision_session=effective_session,
                    activated_at=seed.now,
                    from_definition_id=seed.pair.champion_definition_id,
                    to_definition_id=seed.pair.challenger_definition_id,
                    champion_generation=event.champion_generation,
                ),
                created_at=seed.now,
            )
        )
        head.active_definition_id = seed.pair.challenger_definition_id
        head.active_generation += 1
        head.pending_event_id = None
        head.pending_effective_session = None
        head.version += 1
        head.updated_at = seed.now
    async with factory() as session, session.begin():
        result = await finalize_strategy_shadow(
            session,
            group_id=started.group_id,
            now=datetime.combine(seed.calendar_sessions[62], time(23), tzinfo=UTC),
        )
    async with factory() as session:
        shadow_decision_count = int(
            await session.scalar(
                select(func.count())
                .select_from(StrategyPromotionDecisionRecord)
                .where(StrategyPromotionDecisionRecord.stage == "shadow")
            )
            or 0
        )
        projection = await session.get(StrategyExperimentStateProjectionRecord, seed.pair.family_id)

    assert result.status == "STALE"
    assert result.reason_code == "STALE_CHAMPION_BASELINE"
    assert result.family_state == "STALE_CHAMPION_BASELINE"
    assert shadow_decision_count == 0
    assert projection is not None
    assert projection.current_state == "STALE_CHAMPION_BASELINE"


@pytest.mark.asyncio
async def test_incomplete_history_waits_for_grace_then_fails_closed_unknown(
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
    before_grace = datetime.combine(seed.calendar_sessions[62], time(23), tzinfo=UTC)
    async with factory() as session:
        with pytest.raises(StrategyShadowServiceError) as caught:
            async with session.begin():
                await finalize_strategy_shadow(
                    session,
                    group_id=started.group_id,
                    now=before_grace,
                )
    assert caught.value.reason_code == "STRATEGY_SHADOW_FINALIZATION_NOT_DUE"

    after_grace = datetime.combine(seed.calendar_sessions[67], time(23), tzinfo=UTC)
    async with factory() as session, session.begin():
        result = await finalize_strategy_shadow(
            session,
            group_id=started.group_id,
            now=after_grace,
        )

    assert result.status == "UNKNOWN"
    assert result.family_state == "SHADOW_FAILED"
    assert result.reason_code == "STRATEGY_SHADOW_EVIDENCE_INCOMPLETE"
