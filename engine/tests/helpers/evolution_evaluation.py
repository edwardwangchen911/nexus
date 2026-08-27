"""Minimal PostgreSQL authorities for strategy-evolution comparison tests."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from nexus.audit.canonical import canonical_json
from nexus.backtest.cost_policy import BACKTEST_COST_POLICY_ID
from nexus.persistence.models.backtest import (
    BacktestAuthorityBundleRecord,
    BacktestAuthorityCollectionRecord,
    BacktestRunRecord,
    ExchangeCalendarAuthorityRecord,
    StrategyDefinitionRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.evolution import (
    StrategyEvolutionPolicyRecord,
    StrategyExperimentEventRecord,
    StrategyExperimentFamilyRecord,
    StrategyExperimentRecord,
    StrategyExperimentStateProjectionRecord,
    StrategyPromotionPolicyRecord,
    StrategyRegistryHeadRecord,
    StrategyResearchLineageRecord,
)
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.optimization import CostPolicyRecord
from nexus.quality.source_policy import SOURCE_POLICY_ID
from nexus.strategy.dsl import (
    baseline_v1_document,
    compile_strategy,
    evolution_feature_registry_v1,
)
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

NOW = datetime(2026, 8, 27, 15, tzinfo=UTC)
INSTRUMENT_ID = UUID("019d0000-0000-7000-8000-000000005601")
ALIAS_ID = UUID("019d0000-0000-7000-8000-000000005602")
SNAPSHOT_ID = UUID("019d0000-0000-7000-8000-000000005603")
CALENDAR_ID = UUID("019d0000-0000-7000-8000-000000005604")
UNIVERSE_COLLECTION_ID = UUID("019d0000-0000-7000-8000-000000005605")
SECTOR_COLLECTION_ID = UUID("019d0000-0000-7000-8000-000000005606")
ACTION_COLLECTION_ID = UUID("019d0000-0000-7000-8000-000000005607")
BUNDLE_ID = UUID("019d0000-0000-7000-8000-000000005608")
LINEAGE_ID = UUID("019d0000-0000-7000-8000-000000005609")
FAMILY_ID = UUID("019d0000-0000-7000-8000-000000005610")
EXPERIMENT_ID = UUID("019d0000-0000-7000-8000-000000005611")
CHALLENGER_DEFINITION_ID = UUID("019d0000-0000-7000-8000-000000005612")
CHAMPION_RUN_ID = UUID("019d0000-0000-7000-8000-000000005613")
CHALLENGER_RUN_ID = UUID("019d0000-0000-7000-8000-000000005614")
ALTERNATE_COST_POLICY_ID = UUID("019d0000-0000-7000-8000-000000005615")


@dataclass(frozen=True)
class SeededEvaluationPair:
    experiment_id: UUID
    family_id: UUID
    champion_definition_id: UUID
    challenger_definition_id: UUID
    champion_run_id: UUID
    challenger_run_id: UUID
    promotion_policy_id: UUID


async def seed_evaluation_pair(
    factory: async_sessionmaker[AsyncSession],
    *,
    mismatch: str | None = None,
    passing: bool = True,
) -> SeededEvaluationPair:
    async with factory() as session:
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        search_policy = await session.scalar(
            select(StrategyEvolutionPolicyRecord).where(
                StrategyEvolutionPolicyRecord.policy_kind == "evolution_search_space"
            )
        )
        promotion_policy = await session.scalar(select(StrategyPromotionPolicyRecord))
    assert head is not None and search_policy is not None and promotion_policy is not None
    champion_definition_id = head.active_definition_id
    document = baseline_v1_document()
    document["version"] = "challenger_evaluation_fixture"
    challenger = compile_strategy(document, evolution_feature_registry_v1())
    start = date(2018, 1, 2)
    end = date(2025, 12, 31)
    protocol = {
        "schema_version": "paired_protocol_fixture_v1",
        "experiment_attempt_count": 3,
        "random_seed": 0,
        "cost_model": {
            "commission_bps": "0",
            "spread_bps": "5",
            "slippage_bps": "10",
        },
    }
    async with factory() as session, session.begin():
        admitted_at = NOW - timedelta(minutes=1)
        admitted_payload = {"schema_version": "fixture"}
        admitted_key = "evaluation-fixture-admitted"
        admitted_hash = hashlib.sha256(
            canonical_json(
                {
                    "schema_version": "strategy_experiment_event_v1",
                    "family_id": str(FAMILY_ID),
                    "sequence": 1,
                    "event_type": "ADMITTED",
                    "idempotency_key": admitted_key,
                    "actor_kind": "service",
                    "occurred_at": admitted_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "payload": admitted_payload,
                    "previous_hash": "0" * 64,
                }
            ).encode()
        ).hexdigest()
        session.add_all(
            [
                Instrument(
                    id=INSTRUMENT_ID,
                    asset_type="equity_etf",
                    exchange="XNYS",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="equity",
                ),
                DataSnapshot(
                    id=SNAPSHOT_ID,
                    source="fixture",
                    provider_object_id="evolution-evaluation",
                    revision="v1",
                    observed_at=NOW - timedelta(minutes=2),
                    available_at=NOW - timedelta(minutes=1),
                    received_at=NOW - timedelta(minutes=1),
                    schema_version="backtest_authority_v1",
                    payload={},
                ),
                ExchangeCalendarAuthorityRecord(
                    id=CALENDAR_ID,
                    exchange="XNYS",
                    version="fixture-v1",
                    source="fixture",
                    range_start=start,
                    range_end=end,
                    query_parameters={},
                    completeness_status="complete",
                    content_hash="1" * 64,
                    published_at=NOW - timedelta(days=1),
                ),
                *(
                    BacktestAuthorityCollectionRecord(
                        id=identifier,
                        kind=kind,
                        source="fixture",
                        range_start=start,
                        range_end=end,
                        query_parameters={},
                        completeness_status="complete",
                        content_hash=digest * 64,
                        completed_at=NOW - timedelta(minutes=1),
                    )
                    for identifier, kind, digest in (
                        (UNIVERSE_COLLECTION_ID, "universe", "2"),
                        (SECTOR_COLLECTION_ID, "sector", "3"),
                        (ACTION_COLLECTION_ID, "corporate_action", "4"),
                    )
                ),
            ]
        )
        await session.flush()
        session.add(
            InstrumentAlias(
                id=ALIAS_ID,
                instrument_id=INSTRUMENT_ID,
                symbol="SPY",
                alias_type="ticker",
                valid_from=start,
                valid_to=None,
            )
        )
        await session.flush()
        session.add(
            BacktestAuthorityBundleRecord(
                id=BUNDLE_ID,
                data_snapshot_id=SNAPSHOT_ID,
                source_policy_id=SOURCE_POLICY_ID,
                cost_policy_id=BACKTEST_COST_POLICY_ID,
                calendar_authority_id=CALENDAR_ID,
                universe_collection_id=UNIVERSE_COLLECTION_ID,
                sector_collection_id=SECTOR_COLLECTION_ID,
                corporate_action_collection_id=ACTION_COLLECTION_ID,
                spy_instrument_alias_id=ALIAS_ID,
                range_start=start,
                range_end=end,
                completeness_status="complete",
                content_hash="a" * 64,
                source_selection_reports=[],
                completed_at=NOW,
            )
        )
        session.add(
            CostPolicyRecord(
                id=ALTERNATE_COST_POLICY_ID,
                key="backtest_cost_model_fixture_alternate",
                version="fixture-v1",
                schema_version="1",
                parameters={
                    "commission_bps": "0",
                    "spread_bps": "6",
                    "slippage_bps": "10",
                },
                evidence_refs=["fixture"],
                available_at=NOW - timedelta(days=1),
            )
        )
        await session.flush()
        session.add_all(
            [
                StrategyDefinitionRecord(
                    id=CHALLENGER_DEFINITION_ID,
                    strategy_version=challenger.strategy_version,
                    schema_version="strategy_dsl_v1",
                    expression_hash=challenger.expression_hash,
                    document=document,
                    lifecycle="challenger",
                    published_at=NOW - timedelta(minutes=1),
                ),
                StrategyResearchLineageRecord(
                    id=LINEAGE_ID,
                    lineage_key="evaluation-fixture-v1",
                    evolution_policy_id=search_policy.id,
                    cumulative_oos_trial_count=0,
                    status="active",
                ),
            ]
        )
        await session.flush()
        session.add(
            StrategyExperimentFamilyRecord(
                id=FAMILY_ID,
                research_lineage_id=LINEAGE_ID,
                parent_strategy_definition_id=champion_definition_id,
                rnd_output_id=None,
                champion_generation=1,
                admission_status="ADMITTED",
                candidate_count=1,
                idempotency_key="evaluation-fixture-family",
                authority_hash="a" * 64,
                protocol_hash="b" * 64,
                code_hash="c" * 64,
                created_at=NOW - timedelta(minutes=1),
            )
        )
        await session.flush()
        session.add_all(
            [
                StrategyExperimentRecord(
                    id=EXPERIMENT_ID,
                    family_id=FAMILY_ID,
                    candidate_strategy_definition_id=CHALLENGER_DEFINITION_ID,
                    candidate_ordinal=1,
                    candidate_delta={"schema_version": "fixture"},
                    candidate_hash="5" * 64,
                    expression_hash=challenger.expression_hash,
                    status="ADMITTED",
                    created_at=NOW - timedelta(minutes=1),
                ),
                StrategyExperimentEventRecord(
                    id=UUID("019d0000-0000-7000-8000-000000005616"),
                    family_id=FAMILY_ID,
                    schema_version="strategy_experiment_event_v1",
                    sequence=1,
                    event_type="ADMITTED",
                    idempotency_key=admitted_key,
                    actor_kind="service",
                    occurred_at=admitted_at,
                    payload=admitted_payload,
                    previous_hash="0" * 64,
                    event_hash=admitted_hash,
                    created_at=admitted_at,
                ),
                StrategyExperimentStateProjectionRecord(
                    family_id=FAMILY_ID,
                    research_lineage_id=LINEAGE_ID,
                    current_state="ADMITTED",
                    event_sequence=1,
                    cumulative_oos_trial_count=0,
                    version=1,
                    updated_at=NOW - timedelta(minutes=1),
                ),
            ]
        )
        await session.flush()
        champion_run = _backtest_run(
            run_id=CHAMPION_RUN_ID,
            definition_id=champion_definition_id,
            protocol=protocol,
            result_hash="6" * 64,
            role="champion",
            passing=passing,
        )
        challenger_run = _backtest_run(
            run_id=CHALLENGER_RUN_ID,
            definition_id=CHALLENGER_DEFINITION_ID,
            protocol=protocol,
            result_hash="7" * 64,
            role="challenger",
            passing=passing,
        )
        if mismatch == "authority":
            challenger_run.authority_hash = "9" * 64
        elif mismatch == "protocol":
            challenger_run.protocol_hash = "9" * 64
        elif mismatch == "cost":
            challenger_run.cost_policy_id = ALTERNATE_COST_POLICY_ID
        session.add_all([champion_run, challenger_run])

    return SeededEvaluationPair(
        experiment_id=EXPERIMENT_ID,
        family_id=FAMILY_ID,
        champion_definition_id=champion_definition_id,
        challenger_definition_id=CHALLENGER_DEFINITION_ID,
        champion_run_id=CHAMPION_RUN_ID,
        challenger_run_id=CHALLENGER_RUN_ID,
        promotion_policy_id=promotion_policy.id,
    )


def _backtest_run(
    *,
    run_id: UUID,
    definition_id: UUID,
    protocol: dict[str, object],
    result_hash: str,
    role: str,
    passing: bool,
) -> BacktestRunRecord:
    is_challenger = role == "challenger"
    sharpe = "1.15" if is_challenger and passing else "1.00"
    annual_return = "0.11" if is_challenger and passing else "0.10"
    drawdown = "-0.22" if is_challenger and passing else "-0.20"
    double_terminal = "106" if is_challenger and passing else "105"
    return BacktestRunRecord(
        id=run_id,
        strategy_definition_id=definition_id,
        authority_bundle_id=BUNDLE_ID,
        data_snapshot_id=SNAPSHOT_ID,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=BACKTEST_COST_POLICY_ID,
        start_session=date(2018, 1, 2),
        end_session=date(2025, 12, 31),
        initial_nav=Decimal("100"),
        protocol=protocol,
        result_payload={
            "metrics": {
                "deflated_sharpe_confidence": "0.95",
                "sharpe": sharpe,
                "annualized_return": annual_return,
                "max_drawdown": drawdown,
            },
            "cost_sensitivity": {
                "half_cost_terminal_nav": "107",
                "base_terminal_nav": "106",
                "double_cost_terminal_nav": double_terminal,
            },
            "parameter_sensitivity": {
                "adv20_40m_terminal_nav": "105",
                "base_terminal_nav": "106",
                "adv20_60m_terminal_nav": "104",
            },
        },
        protocol_hash="b" * 64,
        authority_hash="a" * 64,
        data_hash="d" * 64,
        code_hash="c" * 64,
        result_hash=result_hash,
        random_seed=0,
        oos_window_count=8,
        strategy_data_complete=True,
        benchmark_comparison_complete=True,
        history_requirement_met=True,
        promotable=False,
        historical_agent_overlay=False,
        graph_sealed=True,
        authority_frozen_at=NOW - timedelta(seconds=1),
        completed_at=NOW,
    )
