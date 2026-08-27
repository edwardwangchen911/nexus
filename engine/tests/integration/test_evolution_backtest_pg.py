"""PostgreSQL-authoritative paired champion/challenger comparisons."""

from __future__ import annotations

from decimal import Decimal

import pytest
from helpers.evolution_evaluation import NOW, seed_evaluation_pair
from nexus.evolution.evaluation import (
    EvolutionEvaluationError,
    persist_champion_comparison,
)
from nexus.persistence.models.evolution import (
    StrategyChampionComparisonRecord,
    StrategyExperimentStateProjectionRecord,
    StrategyOOSEvidenceLinkRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select, text


@pytest.mark.asyncio
async def test_paired_comparison_persists_exact_authority_and_trial_count(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    pair = await seed_evaluation_pair(factory)
    async with factory() as session, session.begin():
        comparison = await persist_champion_comparison(
            session,
            experiment_id=pair.experiment_id,
            champion_backtest_run_id=pair.champion_run_id,
            challenger_backtest_run_id=pair.challenger_run_id,
            now=NOW,
        )
    async with factory() as session:
        stored = await session.get(StrategyChampionComparisonRecord, comparison.id)
        projection = await session.get(StrategyExperimentStateProjectionRecord, pair.family_id)
        evidence = await session.scalar(
            select(StrategyOOSEvidenceLinkRecord).where(
                StrategyOOSEvidenceLinkRecord.comparison_id == comparison.id
            )
        )

    assert stored is not None
    assert stored.authority_hash == "a" * 64
    assert stored.protocol_hash == "b" * 64
    assert stored.cumulative_oos_trial_count == 1
    assert Decimal(stored.metric_report["challenger_double_cost_return"]) == Decimal("0.06")
    assert Decimal(stored.metric_report["champion_double_cost_return"]) == Decimal("0.05")
    assert projection is not None
    assert projection.current_state == "BACKTEST_RUNNING"
    assert projection.cumulative_oos_trial_count == 1
    assert evidence is not None
    assert evidence.experiment_id == pair.experiment_id
    assert evidence.champion_backtest_run_id == pair.champion_run_id
    assert evidence.challenger_backtest_run_id == pair.challenger_run_id


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["authority", "protocol", "cost", "trial_count"])
async def test_paired_comparison_rejects_mismatched_authority(
    migrated_database_url: str, mismatch: str
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    pair = await seed_evaluation_pair(factory, mismatch=mismatch)
    async with factory() as session, session.begin():
        with pytest.raises(EvolutionEvaluationError, match="PAIRED_AUTHORITY_MISMATCH"):
            await persist_champion_comparison(
                session,
                experiment_id=pair.experiment_id,
                champion_backtest_run_id=pair.champion_run_id,
                challenger_backtest_run_id=pair.challenger_run_id,
                now=NOW,
            )


@pytest.mark.asyncio
async def test_paired_comparison_rejects_missing_committed_oos_exposure(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    pair = await seed_evaluation_pair(factory, mismatch="missing_exposure")
    async with factory() as session, session.begin():
        with pytest.raises(
            EvolutionEvaluationError, match="OOS_EXPOSURE_AUTHORITY_INCOMPLETE"
        ):
            await persist_champion_comparison(
                session,
                experiment_id=pair.experiment_id,
                champion_backtest_run_id=pair.champion_run_id,
                challenger_backtest_run_id=pair.challenger_run_id,
                now=NOW,
            )


@pytest.mark.asyncio
async def test_paired_comparison_rejects_parent_generation_mismatch(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    pair = await seed_evaluation_pair(factory, mismatch="generation")
    async with factory() as session, session.begin():
        with pytest.raises(EvolutionEvaluationError, match="STALE_CHAMPION_BASELINE"):
            await persist_champion_comparison(
                session,
                experiment_id=pair.experiment_id,
                champion_backtest_run_id=pair.champion_run_id,
                challenger_backtest_run_id=pair.challenger_run_id,
                now=NOW,
            )


@pytest.mark.asyncio
async def test_paired_comparison_rejects_corrupt_candidate_projection(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    pair = await seed_evaluation_pair(factory)
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "ALTER TABLE strategy_experiment_candidate_projections "
                "DISABLE TRIGGER USER"
            )
        )
        await session.execute(
            text(
                "UPDATE strategy_experiment_candidate_projections "
                "SET event_sequence=1 WHERE experiment_id=:experiment_id"
            ),
            {"experiment_id": pair.experiment_id},
        )
        await session.execute(
            text(
                "ALTER TABLE strategy_experiment_candidate_projections "
                "ENABLE TRIGGER USER"
            )
        )
    async with factory() as session, session.begin():
        with pytest.raises(
            EvolutionEvaluationError, match="STRATEGY_EXPERIMENT_CHAIN_INVALID"
        ):
            await persist_champion_comparison(
                session,
                experiment_id=pair.experiment_id,
                champion_backtest_run_id=pair.champion_run_id,
                challenger_backtest_run_id=pair.challenger_run_id,
                now=NOW,
            )
