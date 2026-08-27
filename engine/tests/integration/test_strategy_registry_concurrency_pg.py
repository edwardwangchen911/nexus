"""CAS serialization for immutable registry activation facts."""

from __future__ import annotations

import asyncio

import pytest
from helpers.evolution_registry import seed_promotable_registry
from nexus.evolution.registry import (
    RegistryServiceError,
    activate_pending_transition,
    promote_internal_paper_champion,
)
from nexus.persistence.models.evolution import (
    StrategyRegistryActivationRecord,
    StrategyRegistryHeadRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_concurrent_boundary_activation_commits_exactly_one_fact(
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
            idempotency_key="registry-concurrent-promotion",
            now=seed.now,
        )

    async def activate():
        async with factory() as session, session.begin():
            return await activate_pending_transition(
                session,
                expected_generation=pending.active_generation,
                expected_version=pending.version,
                decision_session=seed.effective_session,
                now=seed.now,
            )

    results = await asyncio.gather(activate(), activate(), return_exceptions=True)
    effective = [item for item in results if not isinstance(item, BaseException)]
    conflicts = [item for item in results if isinstance(item, RegistryServiceError)]
    assert len(effective) == len(conflicts) == 1
    assert effective[0].status == "EFFECTIVE"
    assert conflicts[0].reason_code == "STRATEGY_REGISTRY_CONFLICT"

    async with factory() as session:
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        activation_count = int(
            await session.scalar(
                select(func.count()).select_from(StrategyRegistryActivationRecord)
            )
            or 0
        )
    assert activation_count == 1
    assert head is not None
    assert head.active_definition_id == seed.challenger_definition_id
    assert head.active_generation == seed.expected_generation + 1
    assert head.pending_event_id is None
