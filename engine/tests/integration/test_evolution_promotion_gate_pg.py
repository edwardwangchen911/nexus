"""Historical promotion decisions and stale-generation fail-closed behavior."""

from __future__ import annotations

from uuid import UUID

import pytest
from helpers.evolution_evaluation import NOW, seed_evaluation_pair
from nexus.evolution.evaluation import (
    persist_champion_comparison,
    persist_historical_promotion_decision,
)
from nexus.persistence.models.evolution import (
    StrategyExperimentEventRecord,
    StrategyExperimentStateProjectionRecord,
    StrategyPromotionDecisionRecord,
    StrategyRegistryEventRecord,
    StrategyRegistryHeadRecord,
    StrategyShadowGroupRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_historical_pass_persists_ordered_gates_without_starting_shadow(
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
    async with factory() as session, session.begin():
        outcome = await persist_historical_promotion_decision(
            session,
            comparison_id=comparison.id,
            promotion_policy_id=pair.promotion_policy_id,
            now=NOW,
        )
    async with factory() as session:
        decision = await session.get(StrategyPromotionDecisionRecord, outcome.decision_id)
        projection = await session.get(StrategyExperimentStateProjectionRecord, pair.family_id)
        shadow_count = await session.scalar(
            select(func.count()).select_from(StrategyShadowGroupRecord)
        )
        registry_count = await session.scalar(
            select(func.count()).select_from(StrategyRegistryEventRecord)
        )

    assert outcome.status == "PASS"
    assert decision is not None and decision.status == "PASS"
    assert [gate["gate"] for gate in decision.gate_report["gates"]] == [
        "minimum_oos_windows",
        "deflated_sharpe_confidence",
        "sharpe_or_return_improvement",
        "maximum_drawdown_degradation",
        "double_cost_excess_positive",
        "evidence_completeness",
    ]
    assert projection is not None and projection.current_state == "BACKTEST_PASSED"
    assert shadow_count == 0
    assert registry_count == 1


@pytest.mark.asyncio
async def test_historical_failure_is_terminal_and_does_not_start_shadow(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    pair = await seed_evaluation_pair(factory, passing=False)
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
        outcome = await persist_historical_promotion_decision(
            session,
            comparison_id=comparison.id,
            promotion_policy_id=pair.promotion_policy_id,
            now=NOW,
        )
    async with factory() as session:
        projection = await session.get(StrategyExperimentStateProjectionRecord, pair.family_id)
        shadow_count = await session.scalar(
            select(func.count()).select_from(StrategyShadowGroupRecord)
        )

    assert outcome.status == "FAIL"
    assert projection is not None and projection.current_state == "BACKTEST_FAILED"
    assert shadow_count == 0


@pytest.mark.asyncio
async def test_generation_change_marks_comparison_stale_without_decision_or_registry_write(
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
    await _advance_registry_generation(factory, pair)
    async with factory() as session:
        before = await session.scalar(select(func.count()).select_from(StrategyRegistryEventRecord))
    async with factory() as session, session.begin():
        outcome = await persist_historical_promotion_decision(
            session,
            comparison_id=comparison.id,
            promotion_policy_id=pair.promotion_policy_id,
            now=NOW,
        )
    async with factory() as session:
        after = await session.scalar(select(func.count()).select_from(StrategyRegistryEventRecord))
        decision_count = await session.scalar(
            select(func.count()).select_from(StrategyPromotionDecisionRecord)
        )
        projection = await session.get(StrategyExperimentStateProjectionRecord, pair.family_id)
        events = list(
            await session.scalars(
                select(StrategyExperimentEventRecord)
                .where(StrategyExperimentEventRecord.family_id == pair.family_id)
                .order_by(StrategyExperimentEventRecord.sequence)
            )
        )

    assert outcome.status == "STALE"
    assert before == after == 2
    assert decision_count == 0
    assert projection is not None
    assert projection.current_state == "STALE_CHAMPION_BASELINE"
    assert events[-1].event_type == "STALE_CHAMPION_BASELINE"


async def _advance_registry_generation(factory, pair) -> None:
    async with factory() as session, session.begin():
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        assert head is not None
        prior = await session.get(StrategyRegistryEventRecord, head.last_event_id)
        assert prior is not None
        event = StrategyRegistryEventRecord(
            id=UUID("019d0000-0000-7000-8000-000000005699"),
            channel="internal_paper",
            schema_version="strategy_registry_event_v1",
            sequence=2,
            event_type="PROMOTE",
            idempotency_key="evaluation-fixture-external-promotion",
            actor_kind="service",
            occurred_at=NOW,
            from_definition_id=pair.champion_definition_id,
            to_definition_id=pair.challenger_definition_id,
            promotion_decision_id=None,
            champion_generation=2,
            effective_session=None,
            payload={"schema_version": "fixture"},
            previous_hash=prior.event_hash,
            event_hash="f" * 64,
            created_at=NOW,
        )
        session.add(event)
        await session.flush()
        head.active_definition_id = pair.challenger_definition_id
        head.active_generation = 2
        head.last_event_id = event.id
        head.event_sequence = 2
        head.version = 2
        head.updated_at = NOW
