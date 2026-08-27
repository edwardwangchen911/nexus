"""Append-only compatible champion rollback and open-order activation guards."""

from datetime import UTC, datetime

import pytest
from helpers.evolution_evaluation import seed_evaluation_pair
from helpers.evolution_registry import seed_promotable_registry
from helpers.paper_batch import EXECUTION_SESSION
from helpers.paper_execution import seed_paper_execution
from nexus.domain.ids import new_uuid7
from nexus.evolution.registry import (
    activate_pending_transition,
    promote_internal_paper_champion,
    registry_event_content_hash,
    request_registry_rollback,
)
from nexus.persistence.models.evolution import (
    StrategyRegistryActivationRecord,
    StrategyRegistryEventRecord,
    StrategyRegistryHeadRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_effective_promotion_can_rollback_to_previous_compatible_champion(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    seed = await seed_promotable_registry(migrated_database_url, factory)
    async with factory() as session, session.begin():
        pending = await promote_internal_paper_champion(
            session,
            experiment_id=seed.shadow.pair.experiment_id,
            expected_generation=seed.expected_generation,
            expected_version=seed.expected_version,
            effective_session=seed.effective_session,
            idempotency_key="rollback-fixture-promote",
            now=seed.now,
        )
    async with factory() as session, session.begin():
        promoted = await activate_pending_transition(
            session,
            expected_generation=pending.active_generation,
            expected_version=pending.version,
            decision_session=seed.effective_session,
            now=seed.now,
        )
    rollback_session = seed.shadow.calendar_sessions[64]
    async with factory() as session, session.begin():
        rollback_pending = await request_registry_rollback(
            session,
            expected_generation=promoted.active_generation,
            expected_version=promoted.version,
            effective_session=rollback_session,
            idempotency_key="rollback-fixture-request",
            reason_code="USER_REQUESTED",
            now=seed.now,
        )
    async with factory() as session, session.begin():
        rolled_back = await activate_pending_transition(
            session,
            expected_generation=rollback_pending.active_generation,
            expected_version=rollback_pending.version,
            decision_session=rollback_session,
            now=seed.now,
        )
    async with factory() as session:
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        rollback_count = int(
            await session.scalar(
                select(func.count())
                .select_from(StrategyRegistryEventRecord)
                .where(StrategyRegistryEventRecord.event_type == "ROLLBACK")
            )
            or 0
        )
        activation_count = int(
            await session.scalar(
                select(func.count()).select_from(StrategyRegistryActivationRecord)
            )
            or 0
        )

    assert promoted.status == "EFFECTIVE"
    assert rollback_pending.status == "PENDING_BOUNDARY"
    assert rolled_back.status == "EFFECTIVE"
    assert rolled_back.active_definition_id == seed.active_definition_id
    assert rolled_back.active_generation == seed.expected_generation + 2
    assert head is not None and head.pending_event_id is None
    assert rollback_count == 1
    assert activation_count == 2


@pytest.mark.asyncio
async def test_genesis_has_no_rollback_target_and_creates_no_event(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    pair = await seed_evaluation_pair(factory)
    async with factory() as session:
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        assert head is not None
        before = int(
            await session.scalar(select(func.count()).select_from(StrategyRegistryEventRecord)) or 0
        )
    async with factory() as session, session.begin():
        result = await request_registry_rollback(
            session,
            expected_generation=head.active_generation,
            expected_version=head.version,
            effective_session=datetime(2026, 10, 1, tzinfo=UTC).date(),
            idempotency_key="rollback-without-target",
            reason_code="USER_REQUESTED",
            now=datetime(2026, 9, 30, 21, tzinfo=UTC),
        )
    async with factory() as session:
        after = int(
            await session.scalar(select(func.count()).select_from(StrategyRegistryEventRecord)) or 0
        )

    assert pair.champion_definition_id == result.active_definition_id
    assert result.status == "FAILED"
    assert result.reason_code == "ROLLBACK_TARGET_NOT_FOUND"
    assert after == before


@pytest.mark.asyncio
async def test_pending_transition_waits_for_nonterminal_internal_paper_orders(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    pair = await seed_evaluation_pair(factory)
    await seed_paper_execution(migrated_database_url)
    now = datetime(2026, 8, 27, 21, tzinfo=UTC)
    async with factory() as session, session.begin():
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        assert head is not None
        prior = await session.get(StrategyRegistryEventRecord, head.last_event_id)
        assert prior is not None
        payload = {"schema_version": "pending_order_guard_fixture_v1"}
        event = StrategyRegistryEventRecord(
            id=new_uuid7(),
            channel="internal_paper",
            schema_version="strategy_registry_event_v1",
            sequence=head.event_sequence + 1,
            event_type="PROMOTE",
            idempotency_key="pending-order-guard-fixture",
            actor_kind="service",
            occurred_at=now,
            from_definition_id=head.active_definition_id,
            to_definition_id=pair.challenger_definition_id,
            promotion_decision_id=None,
            champion_generation=head.active_generation + 1,
            effective_session=EXECUTION_SESSION,
            payload=payload,
            previous_hash=prior.event_hash,
            event_hash=registry_event_content_hash(
                sequence=head.event_sequence + 1,
                event_type="PROMOTE",
                idempotency_key="pending-order-guard-fixture",
                actor_kind="service",
                occurred_at=now,
                from_definition_id=head.active_definition_id,
                to_definition_id=pair.challenger_definition_id,
                promotion_decision_id=None,
                champion_generation=head.active_generation + 1,
                effective_session=EXECUTION_SESSION,
                payload=payload,
                previous_hash=prior.event_hash,
            ),
            created_at=now,
        )
        session.add(event)
        await session.flush()
        head.last_event_id = event.id
        head.event_sequence = event.sequence
        head.pending_event_id = event.id
        head.pending_effective_session = EXECUTION_SESSION
        head.version += 1
        head.updated_at = now
        expected_generation = head.active_generation
        expected_version = head.version
    async with factory() as session, session.begin():
        result = await activate_pending_transition(
            session,
            expected_generation=expected_generation,
            expected_version=expected_version,
            decision_session=EXECUTION_SESSION,
            now=now,
        )
    async with factory() as session:
        activation_count = int(
            await session.scalar(
                select(func.count()).select_from(StrategyRegistryActivationRecord)
            )
            or 0
        )

    assert result.status == "PENDING_ORDERS"
    assert result.reason_code == "PROMOTION_PENDING_ORDERS"
    assert result.active_definition_id == pair.champion_definition_id
    assert result.active_generation == 1
    assert activation_count == 0
