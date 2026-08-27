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
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


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
            champion_generation=1,
            cumulative_oos_trial_count=3,
            now=NOW,
        )
    async with factory() as session:
        stored = await session.get(StrategyChampionComparisonRecord, comparison.id)
        projection = await session.get(StrategyExperimentStateProjectionRecord, pair.family_id)

    assert stored is not None
    assert stored.authority_hash == "a" * 64
    assert stored.protocol_hash == "b" * 64
    assert stored.cumulative_oos_trial_count == 3
    assert Decimal(stored.metric_report["challenger_double_cost_return"]) == Decimal("0.06")
    assert Decimal(stored.metric_report["champion_double_cost_return"]) == Decimal("0.05")
    assert projection is not None
    assert projection.current_state == "BACKTEST_RUNNING"
    assert projection.cumulative_oos_trial_count == 3


@pytest.mark.asyncio
@pytest.mark.parametrize("mismatch", ["authority", "protocol", "cost"])
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
                champion_generation=1,
                cumulative_oos_trial_count=3,
                now=NOW,
            )


@pytest.mark.asyncio
async def test_paired_comparison_rejects_parent_generation_mismatch(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    pair = await seed_evaluation_pair(factory)
    async with factory() as session, session.begin():
        with pytest.raises(EvolutionEvaluationError, match="STALE_CHAMPION_BASELINE"):
            await persist_champion_comparison(
                session,
                experiment_id=pair.experiment_id,
                champion_backtest_run_id=pair.champion_run_id,
                challenger_backtest_run_id=pair.challenger_run_id,
                champion_generation=2,
                cumulative_oos_trial_count=3,
                now=NOW,
            )
