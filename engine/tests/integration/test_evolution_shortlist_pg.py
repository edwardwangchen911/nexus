"""Database-owned validation results and deterministic candidate shortlist."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.domain.ids import new_uuid7
from nexus.evolution.evaluation import (
    finalize_validation_shortlist,
    persist_candidate_validation_result,
)
from nexus.evolution.repository import append_admitted_event
from nexus.persistence.models.backtest import StrategyDefinitionRecord
from nexus.persistence.models.evolution import (
    StrategyCandidateValidationResultRecord,
    StrategyEvolutionPolicyRecord,
    StrategyExperimentCandidateProjectionRecord,
    StrategyExperimentFamilyRecord,
    StrategyExperimentRecord,
    StrategyExperimentStateProjectionRecord,
    StrategyRegistryHeadRecord,
    StrategyResearchLineageProjectionRecord,
    StrategyResearchLineageRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select

NOW = datetime(2026, 8, 28, 10, tzinfo=UTC)


@pytest.mark.asyncio
async def test_twenty_four_validation_results_select_exact_top_three_concurrently(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    family_id, experiments = await _seed_family(factory, candidate_count=24)
    for experiment in experiments[:-1]:
        async with factory() as session, session.begin():
            await _persist_fixture_result(session, experiment)

    async def persist_last() -> UUID:
        async with factory() as session, session.begin():
            result = await _persist_fixture_result(session, experiments[-1])
            return result.id

    result_ids = await asyncio.gather(persist_last(), persist_last())
    assert result_ids[0] == result_ids[1]

    async with factory() as session, session.begin():
        replayed = await finalize_validation_shortlist(
            session, family_id=family_id, now=NOW + timedelta(hours=1)
        )
    async with factory() as session:
        rows = tuple(
            (
                await session.execute(
                    select(
                        StrategyExperimentRecord.candidate_ordinal,
                        StrategyExperimentCandidateProjectionRecord.current_state,
                    )
                    .join(
                        StrategyExperimentCandidateProjectionRecord,
                        StrategyExperimentCandidateProjectionRecord.experiment_id
                        == StrategyExperimentRecord.id,
                    )
                    .where(StrategyExperimentRecord.family_id == family_id)
                    .order_by(StrategyExperimentRecord.candidate_ordinal)
                )
            ).tuples()
        )
        result_count = await session.scalar(
            select(func.count()).select_from(StrategyCandidateValidationResultRecord)
        )
        family_projection = await session.get(
            StrategyExperimentStateProjectionRecord, family_id
        )
    selected_ordinals = tuple(
        ordinal for ordinal, state in rows if state == "OOS_SELECTED"
    )
    assert selected_ordinals == (3, 7, 11)
    assert replayed == tuple(experiments[index - 1].id for index in selected_ordinals)
    assert sum(state == "OOS_SELECTED" for _, state in rows) == 3
    assert sum(state == "OOS_NOT_SELECTED" for _, state in rows) == 21
    assert result_count == 24
    assert family_projection is not None
    assert family_projection.current_state == "BACKTEST_RUNNING"


async def _persist_fixture_result(session, experiment: StrategyExperimentRecord):
    ordinal = experiment.candidate_ordinal
    if ordinal == 3:
        sharpe, drawdown, cost = Decimal("2"), Decimal("-0.10"), Decimal("3")
    elif ordinal == 7:
        sharpe, drawdown, cost = Decimal("2"), Decimal("-0.10"), Decimal("4")
    elif ordinal == 11:
        sharpe, drawdown, cost = Decimal("2"), Decimal("-0.20"), Decimal("1")
    else:
        sharpe, drawdown, cost = Decimal("1"), Decimal("-0.05"), Decimal(ordinal)
    return await persist_candidate_validation_result(
        session,
        experiment_id=experiment.id,
        validation_sharpe=sharpe,
        validation_max_drawdown=drawdown,
        validation_cost=cost,
        valid_window_count=5,
        authority_complete=True,
        authority_hash="a" * 64,
        protocol_hash="b" * 64,
        code_hash="c" * 64,
        now=NOW + timedelta(minutes=ordinal),
    )


async def _seed_family(factory, *, candidate_count: int):
    family_id = new_uuid7()
    lineage_id = new_uuid7()
    now = NOW - timedelta(minutes=1)
    async with factory() as session, session.begin():
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        policy = await session.scalar(
            select(StrategyEvolutionPolicyRecord).where(
                StrategyEvolutionPolicyRecord.policy_kind == "evolution_search_space"
            )
        )
        assert head is not None and policy is not None
        session.add(
            StrategyResearchLineageRecord(
                id=lineage_id,
                lineage_key=f"shortlist-{lineage_id}",
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
        session.add(
            StrategyExperimentFamilyRecord(
                id=family_id,
                research_lineage_id=lineage_id,
                parent_strategy_definition_id=head.active_definition_id,
                rnd_output_id=None,
                champion_generation=head.active_generation,
                admission_status="ADMITTED",
                candidate_count=candidate_count,
                idempotency_key=f"shortlist-family-{family_id}",
                authority_hash="a" * 64,
                protocol_hash="b" * 64,
                code_hash="c" * 64,
                created_at=now,
            )
        )
        await session.flush()
        experiments: list[StrategyExperimentRecord] = []
        for ordinal in range(1, candidate_count + 1):
            definition = StrategyDefinitionRecord(
                id=new_uuid7(),
                strategy_version=f"shortlist_candidate_{ordinal}_{family_id.hex[-8:]}",
                schema_version="strategy_dsl_v1",
                expression_hash=f"{ordinal:064x}",
                document={"schema_version": "fixture", "ordinal": ordinal},
                lifecycle="challenger",
                published_at=now,
                created_at=now,
            )
            session.add(definition)
            experiment = StrategyExperimentRecord(
                id=new_uuid7(),
                family_id=family_id,
                candidate_strategy_definition_id=definition.id,
                candidate_ordinal=ordinal,
                candidate_delta={"schema_version": "fixture", "ordinal": ordinal},
                candidate_hash=f"{ordinal + 100:064x}",
                expression_hash=definition.expression_hash,
                status="ADMITTED",
                created_at=now,
            )
            session.add(experiment)
            experiments.append(experiment)
        await session.flush()
        previous_hash = "0" * 64
        for sequence, experiment in enumerate(experiments, start=1):
            event = await append_admitted_event(
                session,
                family_id=family_id,
                experiment_id=experiment.id,
                sequence=sequence,
                previous_hash=previous_hash,
                idempotency_key=f"shortlist-admitted-{family_id}-{sequence}",
                occurred_at=now,
                payload={"schema_version": "fixture", "ordinal": sequence},
            )
            previous_hash = event.event_hash
            session.add(
                StrategyExperimentCandidateProjectionRecord(
                    experiment_id=experiment.id,
                    family_id=family_id,
                    current_state="ADMITTED",
                    event_sequence=sequence,
                    version=1,
                    updated_at=now,
                )
            )
            await session.flush()
        session.add(
            StrategyExperimentStateProjectionRecord(
                family_id=family_id,
                research_lineage_id=lineage_id,
                current_state="ADMITTED",
                event_sequence=candidate_count,
                cumulative_oos_trial_count=0,
                version=1,
                updated_at=now,
            )
        )
    return family_id, tuple(experiments)
