"""Fixed-query replay of long strategy-evolution histories."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

import pytest
from nexus.audit.canonical import canonical_json
from nexus.domain.ids import new_uuid7
from nexus.evolution.read_service import EvolutionReadService
from nexus.evolution.registry import registry_event_content_hash
from nexus.evolution.repository import _experiment_event_identity
from nexus.persistence.models.backtest import StrategyDefinitionRecord
from nexus.persistence.models.evolution import (
    StrategyEvolutionPolicyRecord,
    StrategyExperimentCandidateProjectionRecord,
    StrategyExperimentEventRecord,
    StrategyExperimentFamilyRecord,
    StrategyExperimentRecord,
    StrategyExperimentStateProjectionRecord,
    StrategyRegistryEventRecord,
    StrategyRegistryHeadRecord,
    StrategyResearchLineageProjectionRecord,
    StrategyResearchLineageRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import event as sqlalchemy_event
from sqlalchemy import insert, select, text

_NOW = datetime(2026, 8, 27, 16, tzinfo=UTC)
_FAMILY_COUNT = 500
_CANDIDATES_PER_FAMILY = 24
_REGISTRY_FACT_COUNT = 2_000


@pytest.mark.asyncio
async def test_overview_replays_large_histories_with_a_fixed_query_budget(
    migrated_database_url: str,
) -> None:
    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    await _seed_terminal_families(factory)
    await _seed_registry_history(factory)

    bind = factory.kw["bind"]
    query_count = 0

    def count_query(*_args) -> None:
        nonlocal query_count
        query_count += 1

    sqlalchemy_event.listen(bind.sync_engine, "before_cursor_execute", count_query)
    try:
        async with factory() as session:
            result = await EvolutionReadService(
                session,
                now=_NOW,
                cursor_secrets=("scale-test-cursor-secret",),
            ).overview(limit=20, cursor=None)
    finally:
        sqlalchemy_event.remove(bind.sync_engine, "before_cursor_execute", count_query)

    assert query_count <= 12
    assert result.status == "ready"
    assert result.write_enabled is True
    assert len(result.experiments) == 20
    assert len(result.registry_events) == 20
    assert [item.sequence for item in result.registry_events] == list(
        range(_REGISTRY_FACT_COUNT - 19, _REGISTRY_FACT_COUNT + 1)
    )


async def _seed_terminal_families(factory) -> None:
    async with factory() as session, session.begin():
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        policy = await session.scalar(
            select(StrategyEvolutionPolicyRecord).where(
                StrategyEvolutionPolicyRecord.policy_kind == "evolution_search_space"
            )
        )
        assert head is not None and policy is not None
        lineage_id = new_uuid7()
        session.add(
            StrategyResearchLineageRecord(
                id=lineage_id,
                lineage_key="read-scale-lineage-v1",
                evolution_policy_id=policy.id,
                cumulative_oos_trial_count=0,
                status="active",
                created_at=_NOW - timedelta(days=2),
            )
        )
        await session.flush()
        session.add(
            StrategyResearchLineageProjectionRecord(
                lineage_id=lineage_id,
                cumulative_oos_trial_count=0,
                version=1,
                updated_at=_NOW - timedelta(days=2),
            )
        )
        definitions = tuple(
            StrategyDefinitionRecord(
                id=new_uuid7(),
                strategy_version=f"read_scale_candidate_{ordinal:02d}",
                schema_version="strategy_dsl_v1",
                expression_hash=hashlib.sha256(
                    f"read-scale-expression:{ordinal}".encode()
                ).hexdigest(),
                document={
                    "schema_version": "strategy_dsl_v1",
                    "version": f"read_scale_candidate_{ordinal:02d}",
                },
                lifecycle="challenger",
                published_at=_NOW - timedelta(days=2),
                created_at=_NOW - timedelta(days=2),
            )
            for ordinal in range(1, _CANDIDATES_PER_FAMILY + 1)
        )
        session.add_all(definitions)
        await session.flush()
        parent_definition_id = head.active_definition_id

        family_values: list[dict[str, object]] = []
        experiment_values: list[dict[str, object]] = []
        event_values: list[dict[str, object]] = []
        candidate_values: list[dict[str, object]] = []
        projection_values: list[dict[str, object]] = []
        for family_index in range(_FAMILY_COUNT):
            family_id = new_uuid7()
            created_at = _NOW - timedelta(days=1, seconds=_FAMILY_COUNT - family_index)
            family_values.append(
                {
                    "id": family_id,
                    "research_lineage_id": lineage_id,
                    "parent_strategy_definition_id": parent_definition_id,
                    "rnd_output_id": None,
                    "champion_generation": 1,
                    "admission_status": "ADMITTED",
                    "candidate_count": _CANDIDATES_PER_FAMILY,
                    "idempotency_key": f"read-scale-family-{family_index}",
                    "authority_hash": "a" * 64,
                    "protocol_hash": "b" * 64,
                    "code_hash": "c" * 64,
                    "baseline_backtest_run_id": None,
                    "created_at": created_at,
                }
            )
            prior_hash = "0" * 64
            last_sequence = 0
            for ordinal, definition in enumerate(definitions, start=1):
                experiment_id = new_uuid7()
                candidate_hash = hashlib.sha256(
                    f"read-scale-candidate:{family_index}:{ordinal}".encode()
                ).hexdigest()
                experiment_values.append(
                    {
                        "id": experiment_id,
                        "family_id": family_id,
                        "candidate_strategy_definition_id": definition.id,
                        "candidate_ordinal": ordinal,
                        "candidate_delta": {"scale_fixture": ordinal},
                        "candidate_hash": candidate_hash,
                        "expression_hash": definition.expression_hash,
                        "status": "ADMITTED",
                        "created_at": created_at,
                    }
                )
                for event_type in ("ADMITTED", "VALIDATION_FAILED"):
                    last_sequence += 1
                    occurred_at = created_at + timedelta(microseconds=last_sequence)
                    payload = {
                        "schema_version": "read_scale_event_v1",
                        "candidate_ordinal": ordinal,
                        "state": event_type,
                    }
                    identity = _experiment_event_identity(
                        family_id=family_id,
                        experiment_id=experiment_id,
                        schema_version="strategy_experiment_event_v1",
                        sequence=last_sequence,
                        event_type=event_type,
                        idempotency_key=(
                            f"read-scale-event-{family_index}-{ordinal}-{event_type.lower()}"
                        ),
                        actor_kind="service",
                        occurred_at=occurred_at,
                        payload=payload,
                        previous_hash=prior_hash,
                    )
                    event_hash = hashlib.sha256(canonical_json(identity).encode()).hexdigest()
                    event_values.append(
                        {
                            "id": new_uuid7(),
                            "family_id": family_id,
                            "experiment_id": experiment_id,
                            "schema_version": "strategy_experiment_event_v1",
                            "sequence": last_sequence,
                            "event_type": event_type,
                            "idempotency_key": identity["idempotency_key"],
                            "actor_kind": "service",
                            "occurred_at": occurred_at,
                            "payload": payload,
                            "previous_hash": prior_hash,
                            "event_hash": event_hash,
                            "created_at": occurred_at,
                        }
                    )
                    prior_hash = event_hash
                candidate_values.append(
                    {
                        "experiment_id": experiment_id,
                        "family_id": family_id,
                        "current_state": "VALIDATION_FAILED",
                        "event_sequence": last_sequence,
                        "version": 2,
                        "updated_at": occurred_at,
                    }
                )
            projection_values.append(
                {
                    "family_id": family_id,
                    "research_lineage_id": lineage_id,
                    "current_state": "BACKTEST_FAILED",
                    "event_sequence": last_sequence,
                    "cumulative_oos_trial_count": 0,
                    "version": _CANDIDATES_PER_FAMILY + 1,
                    "updated_at": occurred_at,
                }
            )

        await session.execute(insert(StrategyExperimentFamilyRecord), family_values)
        await session.execute(insert(StrategyExperimentRecord), experiment_values)
        await session.execute(insert(StrategyExperimentEventRecord), event_values)
        await session.execute(
            text(
                "ALTER TABLE strategy_experiment_candidate_projections "
                "DISABLE TRIGGER USER"
            )
        )
        await session.execute(
            insert(StrategyExperimentCandidateProjectionRecord), candidate_values
        )
        await session.execute(
            text(
                "ALTER TABLE strategy_experiment_candidate_projections "
                "ENABLE TRIGGER USER"
            )
        )
        await session.execute(
            text("ALTER TABLE strategy_experiment_state_projections DISABLE TRIGGER USER")
        )
        await session.execute(
            insert(StrategyExperimentStateProjectionRecord), projection_values
        )
        await session.execute(
            text("ALTER TABLE strategy_experiment_state_projections ENABLE TRIGGER USER")
        )


async def _seed_registry_history(factory) -> None:
    async with factory() as session, session.begin():
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        assert head is not None
        prior = await session.get(StrategyRegistryEventRecord, head.last_event_id)
        assert prior is not None
        prior_hash = prior.event_hash
        paused = False
        values: list[dict[str, object]] = []
        last_event_id = prior.id
        for sequence in range(2, _REGISTRY_FACT_COUNT + 1):
            event_type = "PAUSE" if not paused else "RESUME"
            paused = not paused
            occurred_at = _NOW - timedelta(hours=1) + timedelta(microseconds=sequence)
            event_id = new_uuid7()
            payload = {
                "schema_version": "strategy_registry_control_payload_v1",
                "paused": paused,
            }
            event_hash = registry_event_content_hash(
                sequence=sequence,
                event_type=event_type,
                idempotency_key=f"read-scale-registry-{sequence}",
                actor_kind="service",
                occurred_at=occurred_at,
                from_definition_id=head.active_definition_id,
                to_definition_id=head.active_definition_id,
                promotion_decision_id=None,
                champion_generation=head.active_generation,
                effective_session=None,
                payload=payload,
                previous_hash=prior_hash,
            )
            values.append(
                {
                    "id": event_id,
                    "channel": "internal_paper",
                    "schema_version": "strategy_registry_event_v1",
                    "sequence": sequence,
                    "event_type": event_type,
                    "idempotency_key": f"read-scale-registry-{sequence}",
                    "actor_kind": "service",
                    "occurred_at": occurred_at,
                    "from_definition_id": head.active_definition_id,
                    "to_definition_id": head.active_definition_id,
                    "promotion_decision_id": None,
                    "champion_generation": head.active_generation,
                    "effective_session": None,
                    "payload": payload,
                    "previous_hash": prior_hash,
                    "event_hash": event_hash,
                    "created_at": occurred_at,
                }
            )
            last_event_id = event_id
            prior_hash = event_hash
        await session.execute(insert(StrategyRegistryEventRecord), values)
        await session.execute(
            text("ALTER TABLE strategy_registry_heads DISABLE TRIGGER USER")
        )
        head.last_event_id = last_event_id
        head.event_sequence = _REGISTRY_FACT_COUNT
        head.auto_promotion_paused = paused
        head.version = _REGISTRY_FACT_COUNT
        head.updated_at = _NOW
        await session.flush()
        await session.execute(text("ALTER TABLE strategy_registry_heads ENABLE TRIGGER USER"))
