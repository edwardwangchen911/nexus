"""Irreversible lineage-level OOS exposure accounting."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

import pytest
from nexus.audit.canonical import canonical_json
from nexus.domain.ids import new_uuid7
from nexus.evolution.evaluation import (
    load_lineage_oos_trial_count,
    reserve_oos_exposure,
)
from nexus.evolution.repository import (
    append_candidate_event,
    load_verified_experiment_authority,
)
from nexus.persistence.models.backtest import StrategyDefinitionRecord
from nexus.persistence.models.evolution import (
    StrategyEvolutionPolicyRecord,
    StrategyExperimentCandidateProjectionRecord,
    StrategyExperimentEventRecord,
    StrategyExperimentFamilyRecord,
    StrategyExperimentOOSExposureLinkRecord,
    StrategyExperimentRecord,
    StrategyExperimentStateProjectionRecord,
    StrategyOOSExposureRecord,
    StrategyRegistryHeadRecord,
    StrategyResearchLineageProjectionRecord,
    StrategyResearchLineageRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select

NOW = datetime(2026, 8, 28, 11, tzinfo=UTC)
EXPRESSION_HASH = "e" * 64


@pytest.mark.asyncio
async def test_oos_reservation_is_committed_deduplicated_and_replay_safe(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    lineage_id, family_a, experiment_a = await _seed_selected_candidate(
        factory, key="a"
    )

    async def reserve_a():
        async with factory() as session, session.begin():
            return await reserve_oos_exposure(
                session, experiment_id=experiment_a, now=NOW
            )

    concurrent = await __import__("asyncio").gather(reserve_a(), reserve_a())
    assert concurrent[0].exposure_id == concurrent[1].exposure_id
    assert concurrent[0].ordinal == concurrent[1].ordinal == 1
    async with factory() as session, session.begin():
        replay = await reserve_oos_exposure(
            session, experiment_id=experiment_a, now=NOW + timedelta(minutes=1)
        )
    assert replay.exposure_id == concurrent[0].exposure_id

    async with factory() as session, session.begin():
        authority = await load_verified_experiment_authority(
            session, family_a, lock=True
        )
        await append_candidate_event(
            session,
            authority=authority,
            experiment_id=experiment_a,
            event_type="BACKTEST_FAILED",
            idempotency_key="oos-fixture-a-terminal",
            occurred_at=NOW + timedelta(minutes=2),
            payload={"schema_version": "fixture", "reason_code": "FIXTURE_TERMINAL"},
        )

    _, _, experiment_b = await _seed_selected_candidate(
        factory,
        key="b",
        lineage_id=lineage_id,
        expression_hash=EXPRESSION_HASH,
    )
    async with factory() as session, session.begin():
        second = await reserve_oos_exposure(
            session, experiment_id=experiment_b, now=NOW + timedelta(days=100)
        )
    assert second.exposure_id == replay.exposure_id
    assert second.ordinal == 1

    async with factory() as session:
        trial_count = await load_lineage_oos_trial_count(session, lineage_id)
        exposure_count = await session.scalar(
            select(func.count()).select_from(StrategyOOSExposureRecord)
        )
        link_count = await session.scalar(
            select(func.count()).select_from(StrategyExperimentOOSExposureLinkRecord)
        )
    assert trial_count == 1
    assert exposure_count == 1
    assert link_count == 2


async def _seed_selected_candidate(
    factory,
    *,
    key: str,
    lineage_id=None,
    expression_hash: str = EXPRESSION_HASH,
):
    now = NOW - timedelta(minutes=10)
    family_id = new_uuid7()
    experiment_id = new_uuid7()
    lineage_id = lineage_id or new_uuid7()
    async with factory() as session, session.begin():
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        policy = await session.scalar(
            select(StrategyEvolutionPolicyRecord).where(
                StrategyEvolutionPolicyRecord.policy_kind == "evolution_search_space"
            )
        )
        assert head is not None and policy is not None
        lineage = await session.get(StrategyResearchLineageRecord, lineage_id)
        if lineage is None:
            session.add(
                StrategyResearchLineageRecord(
                    id=lineage_id,
                    lineage_key=f"oos-{lineage_id}",
                    evolution_policy_id=policy.id,
                    cumulative_oos_trial_count=0,
                    status="active",
                    created_at=now,
                )
            )
            await session.flush()
            session.add(
                StrategyResearchLineageProjectionRecord(
                    lineage_id=lineage_id,
                    cumulative_oos_trial_count=0,
                    version=1,
                    updated_at=now,
                )
            )
        lineage_projection = await session.get(
            StrategyResearchLineageProjectionRecord, lineage_id
        )
        assert lineage_projection is not None
        definition = StrategyDefinitionRecord(
            id=new_uuid7(),
            strategy_version=f"oos_candidate_{key}_{family_id.hex[-8:]}",
            schema_version="strategy_dsl_v1",
            expression_hash=expression_hash,
            document={"schema_version": "fixture", "key": key},
            lifecycle="challenger",
            published_at=now,
            created_at=now,
        )
        session.add(definition)
        family = StrategyExperimentFamilyRecord(
            id=family_id,
            research_lineage_id=lineage_id,
            parent_strategy_definition_id=head.active_definition_id,
            rnd_output_id=None,
            champion_generation=head.active_generation,
            admission_status="ADMITTED",
            candidate_count=1,
            idempotency_key=f"oos-family-{key}-{family_id}",
            authority_hash="a" * 64,
            protocol_hash="b" * 64,
            code_hash="c" * 64,
            created_at=now,
        )
        session.add(family)
        await session.flush()
        experiment = StrategyExperimentRecord(
            id=experiment_id,
            family_id=family_id,
            candidate_strategy_definition_id=definition.id,
            candidate_ordinal=1,
            candidate_delta={"schema_version": "fixture"},
            candidate_hash=hashlib.sha256(f"candidate:{key}".encode()).hexdigest(),
            expression_hash=expression_hash,
            status="ADMITTED",
            created_at=now,
        )
        session.add(experiment)
        await session.flush()
        events = _candidate_events(family_id, experiment_id, key, now)
        session.add_all(events)
        await session.flush()
        session.add(
            StrategyExperimentCandidateProjectionRecord(
                experiment_id=experiment_id,
                family_id=family_id,
                current_state="OOS_SELECTED",
                event_sequence=3,
                version=3,
                updated_at=now,
            )
        )
        await session.flush()
        session.add(
            StrategyExperimentStateProjectionRecord(
                family_id=family_id,
                research_lineage_id=lineage_id,
                current_state="BACKTEST_RUNNING",
                event_sequence=3,
                cumulative_oos_trial_count=(
                    lineage_projection.cumulative_oos_trial_count
                ),
                version=1,
                updated_at=now,
            )
        )
    return lineage_id, family_id, experiment_id


def _candidate_events(family_id, experiment_id, key: str, now: datetime):
    events = []
    previous_hash = "0" * 64
    for sequence, event_type in enumerate(
        ("ADMITTED", "VALIDATION_PASSED", "OOS_SELECTED"), start=1
    ):
        idempotency_key = f"oos-{key}-{event_type.lower()}"
        payload = {"schema_version": "fixture", "state": event_type}
        identity = {
            "schema_version": "strategy_experiment_event_v1",
            "family_id": str(family_id),
            "sequence": sequence,
            "event_type": event_type,
            "idempotency_key": idempotency_key,
            "actor_kind": "service",
            "occurred_at": now.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "payload": payload,
            "previous_hash": previous_hash,
            "experiment_id": str(experiment_id),
        }
        event_hash = hashlib.sha256(canonical_json(identity).encode()).hexdigest()
        events.append(
            StrategyExperimentEventRecord(
                id=new_uuid7(),
                family_id=family_id,
                experiment_id=experiment_id,
                schema_version="strategy_experiment_event_v1",
                sequence=sequence,
                event_type=event_type,
                idempotency_key=idempotency_key,
                actor_kind="service",
                occurred_at=now,
                payload=payload,
                previous_hash=previous_hash,
                event_hash=event_hash,
                created_at=now,
            )
        )
        previous_hash = event_hash
    return events
