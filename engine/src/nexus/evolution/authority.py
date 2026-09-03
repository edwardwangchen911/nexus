"""Database-owned authorities for the restricted Strategy R&D executor."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import canonical_json
from nexus.backtest.authority_loader import (
    LoadedPointInTimeBacktest,
    load_point_in_time_dataset,
)
from nexus.backtest.canonical import canonical_decimal
from nexus.domain.time import require_utc
from nexus.evolution.agent import StrategyRNDRequestV1, load_strategy_rnd_prompt
from nexus.evolution.agent_tools import StrategyRNDToolContext, StrategyRNDTools
from nexus.evolution.codex import CODEX_RND_CONFIG_VERSION
from nexus.evolution.handover import (
    HandoverAuthorityV1,
    HandoverChallengerV1,
    HandoverEvaluationV1,
    HandoverInstrumentV1,
    HandoverSectorExposureV1,
    HandoverSnapshotV1,
    HandoverTargetV1,
    evaluate_handover,
)
from nexus.evolution.policies import (
    load_candidate_budget_policy_v1,
    load_evolution_search_space_v1,
)
from nexus.evolution.repository import (
    CandidateState,
    load_verified_experiment_authority,
)
from nexus.evolution.shadow.authority import (
    StrategyShadowAuthorityError,
    load_balanced_paper_baseline,
    load_strategy_shadow_session_evidence,
)
from nexus.evolution.shadow.canonical import content_hash
from nexus.evolution.shadow.models import (
    EVALUATION_SESSION_COUNT,
    StrategyShadowLegMetricsV1,
    StrategyShadowStateV1,
)
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.optimizer.cost_policy import load_cost_policy_v1
from nexus.persistence.models.agents import (
    AgentDataSnapshotEvidenceRecord,
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
)
from nexus.persistence.models.backtest import BacktestRunRecord, StrategyDefinitionRecord
from nexus.persistence.models.data import QualityReport
from nexus.persistence.models.evolution import (
    StrategyChampionComparisonRecord,
    StrategyEvolutionPolicyRecord,
    StrategyExperimentCandidateProjectionRecord,
    StrategyExperimentFamilyRecord,
    StrategyExperimentRecord,
    StrategyPromotionDecisionRecord,
    StrategyRegistryHeadRecord,
    StrategyResearchLineageRecord,
    StrategyShadowCheckpointMetricRecord,
    StrategyShadowCheckpointRecord,
    StrategyShadowFinalizationRecord,
    StrategyShadowGroupRecord,
    StrategyShadowLegRecord,
    StrategyShadowLegStateRecord,
    StrategyShadowSessionAuthorityRecord,
)
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.paper import PaperDrawdownLockEventRecord, RiskPolicyRecord
from nexus.quality.source_policy import (
    current_strategy_rnd_adjusted_authority_available,
    load_source_policy_v1,
)
from nexus.risk.policy import RiskPolicy
from nexus.strategy.dsl import (
    CompiledStrategy,
    compile_strategy,
    evolution_feature_registry_v1,
)


class StrategyRNDAuthorityError(ValueError):
    """A stable database authority gate failed closed."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


@dataclass(frozen=True)
class LoadedCandidateAuthority:
    family: StrategyExperimentFamilyRecord
    experiment: StrategyExperimentRecord
    projection: StrategyExperimentCandidateProjectionRecord
    definition: StrategyDefinitionRecord
    strategy: CompiledStrategy
    baseline: LoadedPointInTimeBacktest


@dataclass(frozen=True)
class LoadedGenerationAuthority:
    """Complete frozen input for one bounded family-generation call."""

    request: StrategyRNDRequestV1
    tools: StrategyRNDTools
    baseline: LoadedPointInTimeBacktest
    quality_report_id: UUID
    candidate_budget_policy_id: UUID
    prompt: AgentPromptVersionRecord
    model: AgentModelConfigRecord


class DatabaseStrategyRNDAuthorityResolver:
    """Resolve candidate execution only from immutable IDs in the current transaction."""

    async def load_candidate(
        self,
        session: AsyncSession,
        *,
        family_id: UUID,
        experiment_id: UUID,
        expected_generation: int,
        expected_state: CandidateState | None,
        expected_version: int | None,
        as_of: datetime,
    ) -> LoadedCandidateAuthority:
        cutoff = require_utc(as_of)
        if not current_strategy_rnd_adjusted_authority_available():
            raise StrategyRNDAuthorityError("BACKTEST_ADJUSTED_AUTHORITY_UNAVAILABLE")
        try:
            authority = await load_verified_experiment_authority(session, family_id, lock=True)
        except ValueError as error:
            raise StrategyRNDAuthorityError("STRATEGY_EXPERIMENT_CHAIN_INVALID") from error
        family = authority.family
        experiment = next(
            (item for item in authority.experiments if item.id == experiment_id), None
        )
        if (
            experiment is None
            or family.champion_generation != expected_generation
            or family.baseline_backtest_run_id is None
        ):
            raise StrategyRNDAuthorityError("CANDIDATE_AUTHORITY_INCOMPLETE")
        projection = authority.candidate_projection(experiment.id)
        if expected_state is not None and projection.current_state != expected_state:
            raise StrategyRNDAuthorityError("CANDIDATE_STATE_STALE")
        if expected_version is not None and projection.version != expected_version:
            raise StrategyRNDAuthorityError("CANDIDATE_STATE_STALE")
        definition = await session.get(
            StrategyDefinitionRecord,
            experiment.candidate_strategy_definition_id,
            populate_existing=True,
        )
        if (
            definition is None
            or definition.lifecycle != "challenger"
            or definition.expression_hash != experiment.expression_hash
            or definition.published_at > cutoff
        ):
            raise StrategyRNDAuthorityError("CANDIDATE_DEFINITION_INVALID")
        try:
            strategy = compile_strategy(definition.document, evolution_feature_registry_v1())
            baseline = await load_point_in_time_dataset(
                session, family.baseline_backtest_run_id, cutoff
            )
        except (TypeError, ValueError) as error:
            raise StrategyRNDAuthorityError("BACKTEST_AUTHORITY_INCOMPLETE") from error
        if (
            strategy.expression_hash != definition.expression_hash
            or baseline.stored_result.authority_hash != family.authority_hash
            or baseline.stored_result.protocol_hash != family.protocol_hash
            or baseline.stored_result.code_hash != family.code_hash
        ):
            raise StrategyRNDAuthorityError("CANDIDATE_BASELINE_MISMATCH")
        return LoadedCandidateAuthority(
            family=family,
            experiment=experiment,
            projection=projection,
            definition=definition,
            strategy=strategy,
            baseline=baseline,
        )

    async def load_generation(
        self,
        session: AsyncSession,
        *,
        expected_generation: int,
        as_of: datetime,
        model_id: str | None = None,
        model_version: str | None = None,
    ) -> LoadedGenerationAuthority:
        """Resolve generation exclusively from the effective registry and sealed facts."""
        cutoff = require_utc(as_of)
        if not current_strategy_rnd_adjusted_authority_available():
            raise StrategyRNDAuthorityError("BACKTEST_ADJUSTED_AUTHORITY_UNAVAILABLE")
        head = await session.get(StrategyRegistryHeadRecord, "internal_paper")
        lineages = tuple(
            await session.scalars(
                select(StrategyResearchLineageRecord)
                .where(StrategyResearchLineageRecord.status == "active")
                .order_by(StrategyResearchLineageRecord.id)
            )
        )
        if (
            head is None
            or head.active_generation != expected_generation
            or head.pending_event_id is not None
            or head.auto_promotion_paused
            or len(lineages) != 1
        ):
            raise StrategyRNDAuthorityError("GENERATION_AUTHORITY_INCOMPLETE")
        lineage = lineages[0]
        active_family = await session.scalar(
            select(StrategyExperimentFamilyRecord.id)
            .join(
                StrategyExperimentCandidateProjectionRecord,
                StrategyExperimentCandidateProjectionRecord.family_id
                == StrategyExperimentFamilyRecord.id,
            )
            .where(
                StrategyExperimentFamilyRecord.research_lineage_id == lineage.id,
                StrategyExperimentCandidateProjectionRecord.current_state.not_in(
                    (
                        "VALIDATION_FAILED",
                        "OOS_NOT_SELECTED",
                        "BACKTEST_FAILED",
                        "SHADOW_FAILED",
                        "EXPIRED",
                        "VETOED",
                        "PROMOTED",
                        "STALE_CHAMPION_BASELINE",
                    )
                ),
            )
            .limit(1)
        )
        if active_family is not None:
            raise StrategyRNDAuthorityError("ACTIVE_FAMILY_EXISTS")
        baseline_record = await session.scalar(
            select(BacktestRunRecord)
            .where(
                BacktestRunRecord.strategy_definition_id == head.active_definition_id,
                BacktestRunRecord.graph_sealed.is_(True),
                BacktestRunRecord.completed_at <= cutoff,
            )
            .order_by(BacktestRunRecord.completed_at.desc(), BacktestRunRecord.id.desc())
            .limit(1)
        )
        if baseline_record is None:
            raise StrategyRNDAuthorityError("BACKTEST_AUTHORITY_INCOMPLETE")
        try:
            baseline = await load_point_in_time_dataset(session, baseline_record.id, cutoff)
        except (TypeError, ValueError) as error:
            raise StrategyRNDAuthorityError("BACKTEST_AUTHORITY_INCOMPLETE") from error
        quality = await session.scalar(
            select(QualityReport)
            .where(
                QualityReport.data_snapshot_id == baseline_record.data_snapshot_id,
                QualityReport.status == "PASS",
                QualityReport.created_at <= cutoff,
            )
            .order_by(QualityReport.created_at.desc(), QualityReport.id.desc())
            .limit(1)
        )
        evidence = tuple(
            await session.scalars(
                select(AgentDataSnapshotEvidenceRecord)
                .where(
                    AgentDataSnapshotEvidenceRecord.data_snapshot_id
                    == baseline_record.data_snapshot_id,
                    AgentDataSnapshotEvidenceRecord.authority_kind == "research",
                    AgentDataSnapshotEvidenceRecord.available_at <= cutoff,
                    AgentDataSnapshotEvidenceRecord.created_at <= cutoff,
                )
                .order_by(AgentDataSnapshotEvidenceRecord.evidence_ref_id)
            )
        )
        evidence_refs = tuple(dict.fromkeys(item.evidence_ref_id for item in evidence))
        prompt_asset = load_strategy_rnd_prompt()
        prompt = await session.scalar(
            select(AgentPromptVersionRecord).where(
                AgentPromptVersionRecord.role == prompt_asset.role,
                AgentPromptVersionRecord.version == prompt_asset.version,
                AgentPromptVersionRecord.content_hash == prompt_asset.content_hash,
                AgentPromptVersionRecord.content == prompt_asset.content,
                AgentPromptVersionRecord.created_at <= cutoff,
            )
        )
        model_query = select(AgentModelConfigRecord).where(
            AgentModelConfigRecord.pricing_available_at <= cutoff,
            AgentModelConfigRecord.pricing_expires_at > cutoff,
            AgentModelConfigRecord.max_tool_calls <= 4,
            AgentModelConfigRecord.created_at <= cutoff,
        )
        if model_id is not None:
            model_query = model_query.where(AgentModelConfigRecord.model_id == model_id)
        if model_version is not None:
            model_query = model_query.where(AgentModelConfigRecord.version == model_version)
        else:
            model_query = model_query.where(
                AgentModelConfigRecord.version != CODEX_RND_CONFIG_VERSION
            )
        model = await session.scalar(
            model_query.order_by(
                AgentModelConfigRecord.pricing_available_at.desc(),
                AgentModelConfigRecord.id.desc(),
            ).limit(1)
        )
        search = await session.get(StrategyEvolutionPolicyRecord, lineage.evolution_policy_id)
        budget = await session.scalar(
            select(StrategyEvolutionPolicyRecord)
            .where(
                StrategyEvolutionPolicyRecord.policy_kind == "candidate_budget",
                StrategyEvolutionPolicyRecord.published_at <= cutoff,
            )
            .order_by(StrategyEvolutionPolicyRecord.published_at.desc())
            .limit(1)
        )
        if (
            quality is None
            or not evidence_refs
            or prompt is None
            or model is None
            or search is None
            or budget is None
            or search.published_at > cutoff
        ):
            raise StrategyRNDAuthorityError("GENERATION_AUTHORITY_INCOMPLETE")
        try:
            search_policy = load_evolution_search_space_v1(search.document, search.content_hash)
            load_candidate_budget_policy_v1(budget.document, budget.content_hash)
        except ValueError as error:
            raise StrategyRNDAuthorityError("GENERATION_POLICY_INVALID") from error

        sealed_summaries = await _sealed_experiment_summaries(
            session, lineage_id=lineage.id, cutoff=cutoff
        )
        request = StrategyRNDRequestV1(
            schema_version="strategy_rnd_request_v1",
            parent_strategy_definition_id=head.active_definition_id,
            champion_generation=head.active_generation,
            research_lineage_id=lineage.id,
            evolution_policy_id=search.id,
            training_validation_summary_id=baseline_record.id,
            data_snapshot_id=baseline_record.data_snapshot_id,
            evidence_refs=evidence_refs[:32],
            as_of=cutoff,
        )
        parent_output = _parent_tool_output(
            baseline, strategy_definition_id=head.active_definition_id
        )
        training_output = _training_validation_tool_output(
            baseline, evidence_refs=request.evidence_refs
        )
        context = StrategyRNDToolContext(
            parent_strategy_definition_id=head.active_definition_id,
            champion_generation=head.active_generation,
            research_lineage_id=lineage.id,
            evolution_policy_id=search.id,
            training_validation_summary_id=baseline_record.id,
            as_of=cutoff,
            sealed_experiment_ids=tuple(sealed_summaries),
            current_family_experiment_ids=(),
        )
        tools = StrategyRNDTools(
            context=context,
            executors={
                "get_strategy_parent": lambda _: parent_output,
                "get_evolution_search_space": lambda _: {
                    "policy_id": str(search.id),
                    "policy_version": search_policy.version,
                    "content_hash": search.content_hash,
                    "axes": [
                        {
                            "axis": axis.axis,
                            "values": list(axis.allowed_values),
                        }
                        for axis in search_policy.axes
                    ],
                },
                "get_training_validation_summary": lambda _: training_output,
                "get_sealed_experiment_summary": lambda payload: sealed_summaries[
                    cast(UUID, payload["experiment_id"])
                ],
            },
        )
        return LoadedGenerationAuthority(
            request=request,
            tools=tools,
            baseline=baseline,
            quality_report_id=quality.id,
            candidate_budget_policy_id=budget.id,
            prompt=prompt,
            model=model,
        )


def _parent_tool_output(
    baseline: LoadedPointInTimeBacktest, *, strategy_definition_id: UUID
) -> Mapping[str, object]:
    strategy = baseline.strategy
    parameters = {item.name: format(item.value, "f") for item in strategy.parameters}
    required = {name: parameters[name] for name in ("min_adv20_usd", "min_close", "target_weight")}
    return {
        "strategy_definition_id": str(strategy_definition_id),
        "strategy_version": strategy.strategy_version,
        "expression_hash": strategy.expression_hash,
        "registered_parameters": required,
        "ranking_feature": strategy.ranking.feature,
        "positive_return_filter_enabled": _positive_return_filter_enabled(
            baseline.strategy.root.model_dump(mode="python")
        ),
    }


def _positive_return_filter_enabled(node: Mapping[str, object]) -> bool:
    if (
        node.get("op") == "gt"
        and node.get("feature") == "return_126"
        and str(node.get("value")) == "0"
    ):
        return True
    args = node.get("args")
    if isinstance(args, tuple | list):
        for child in args:
            if isinstance(child, Mapping) and _positive_return_filter_enabled(child):
                return True
    child = node.get("arg")
    return isinstance(child, Mapping) and _positive_return_filter_enabled(child)


def _training_validation_tool_output(
    baseline: LoadedPointInTimeBacktest, *, evidence_refs: tuple[UUID, ...]
) -> Mapping[str, object]:
    result = baseline.stored_result
    valid_windows = tuple(item for item in result.windows if item.valid and item.metrics)
    if not valid_windows:
        raise StrategyRNDAuthorityError("TRAINING_VALIDATION_AUTHORITY_INCOMPLETE")
    metrics = result.metrics
    return {
        "summary_id": str(baseline.baseline_run_id),
        "authority_hash": result.authority_hash,
        "quality_status": "PASS",
        "training_window_count": len(valid_windows),
        "validation_window_count": len(valid_windows),
        "training_return": format(metrics.total_return, "f"),
        "validation_return": format(metrics.total_return, "f"),
        "training_sharpe": format(metrics.sharpe, "f"),
        "validation_sharpe": format(metrics.sharpe, "f"),
        "maximum_drawdown": format(metrics.max_drawdown, "f"),
        "evidence_refs": [str(item) for item in evidence_refs],
    }


async def _sealed_experiment_summaries(
    session: AsyncSession, *, lineage_id: UUID, cutoff: datetime
) -> dict[UUID, Mapping[str, object]]:
    allowed_states = (
        "BACKTEST_FAILED",
        "BACKTEST_PASSED",
        "SHADOW_FAILED",
        "EXPIRED",
        "VETOED",
        "PROMOTED",
        "STALE_CHAMPION_BASELINE",
    )
    rows = tuple(
        (
            await session.execute(
                select(
                    StrategyExperimentRecord,
                    StrategyExperimentCandidateProjectionRecord,
                )
                .join(
                    StrategyExperimentFamilyRecord,
                    StrategyExperimentFamilyRecord.id == StrategyExperimentRecord.family_id,
                )
                .join(
                    StrategyExperimentCandidateProjectionRecord,
                    StrategyExperimentCandidateProjectionRecord.experiment_id
                    == StrategyExperimentRecord.id,
                )
                .where(
                    StrategyExperimentFamilyRecord.research_lineage_id == lineage_id,
                    StrategyExperimentCandidateProjectionRecord.current_state.in_(allowed_states),
                    StrategyExperimentCandidateProjectionRecord.updated_at <= cutoff,
                )
                .order_by(StrategyExperimentRecord.id)
                .limit(32)
            )
        ).tuples()
    )
    summaries: dict[UUID, Mapping[str, object]] = {}
    for experiment, projection in rows:
        comparison = await session.scalar(
            select(StrategyChampionComparisonRecord).where(
                StrategyChampionComparisonRecord.experiment_id == experiment.id
            )
        )
        decision = (
            None
            if comparison is None
            else await session.scalar(
                select(StrategyPromotionDecisionRecord)
                .where(
                    StrategyPromotionDecisionRecord.comparison_id == comparison.id,
                    StrategyPromotionDecisionRecord.stage == "historical",
                )
                .order_by(StrategyPromotionDecisionRecord.created_at.desc())
                .limit(1)
            )
        )
        identity = {
            "experiment_id": str(experiment.id),
            "candidate_hash": experiment.candidate_hash,
            "status": projection.current_state,
            "gate_status": "UNKNOWN" if decision is None else decision.status,
        }
        summaries[experiment.id] = {
            **identity,
            "summary_hash": hashlib.sha256(canonical_json(identity).encode()).hexdigest(),
            "evidence_refs": [str(experiment.id)],
        }
    return summaries


async def build_strategy_handover_evaluation(
    session: AsyncSession,
    *,
    group: StrategyShadowGroupRecord,
    now: datetime,
) -> HandoverEvaluationV1:
    """Assemble the handover gate only from the sealed paper and Shadow authorities."""
    cutoff = require_utc(now)
    finalization = await session.scalar(
        select(StrategyShadowFinalizationRecord).where(
            StrategyShadowFinalizationRecord.group_id == group.id
        )
    )
    session_authority = await session.scalar(
        select(StrategyShadowSessionAuthorityRecord).where(
            StrategyShadowSessionAuthorityRecord.group_id == group.id,
            StrategyShadowSessionAuthorityRecord.session_ordinal == EVALUATION_SESSION_COUNT,
        )
    )
    legs = tuple(
        await session.scalars(
            select(StrategyShadowLegRecord)
            .where(StrategyShadowLegRecord.group_id == group.id)
            .order_by(StrategyShadowLegRecord.role)
        )
    )
    if (
        finalization is None
        or finalization.status != "PASS"
        or session_authority is None
        or len(legs) != 2
        or {item.role for item in legs} != {"champion", "challenger"}
    ):
        raise StrategyRNDAuthorityError("HANDOVER_AUTHORITY_INCOMPLETE")
    try:
        loaded_session = await load_strategy_shadow_session_evidence(
            session,
            group_id=group.id,
            session_date=session_authority.session,
            ordinal=EVALUATION_SESSION_COUNT,
            calendar_authority_id=group.calendar_authority_id,
            observed_at=cutoff,
        )
        paper = await load_balanced_paper_baseline(session, now=cutoff)
        source_policy = await load_source_policy_v1(session, instrument_tick_size=None)
        cost_policy = await load_cost_policy_v1(session)
    except (StrategyShadowAuthorityError, ValueError) as error:
        raise StrategyRNDAuthorityError("HANDOVER_AUTHORITY_INCOMPLETE") from error
    risk_record = await session.scalar(
        select(RiskPolicyRecord).where(RiskPolicyRecord.active.is_(True))
    )
    risk_policy = RiskPolicy.v1()
    if (
        risk_record is None
        or risk_record.version != risk_policy.version
        or risk_record.content_hash != risk_policy.content_hash
        or paper.snapshot_id != group.baseline_snapshot_id
        or source_policy.record_id != session_authority.source_policy_id
        or cost_policy.id != session_authority.cost_policy_id
    ):
        raise StrategyRNDAuthorityError("HANDOVER_AUTHORITY_INCOMPLETE")
    states: dict[str, StrategyShadowStateV1] = {}
    metrics: dict[str, StrategyShadowLegMetricsV1] = {}
    checkpoint = await session.scalar(
        select(StrategyShadowCheckpointRecord).where(
            StrategyShadowCheckpointRecord.group_id == group.id,
            StrategyShadowCheckpointRecord.checkpoint_ordinal == EVALUATION_SESSION_COUNT,
        )
    )
    if checkpoint is None:
        raise StrategyRNDAuthorityError("HANDOVER_AUTHORITY_INCOMPLETE")
    for leg in legs:
        state_record = await session.scalar(
            select(StrategyShadowLegStateRecord).where(
                StrategyShadowLegStateRecord.leg_id == leg.id,
                StrategyShadowLegStateRecord.session_ordinal == EVALUATION_SESSION_COUNT,
            )
        )
        metric_record = await session.scalar(
            select(StrategyShadowCheckpointMetricRecord).where(
                StrategyShadowCheckpointMetricRecord.checkpoint_id == checkpoint.id,
                StrategyShadowCheckpointMetricRecord.leg_id == leg.id,
            )
        )
        try:
            state = (
                None
                if state_record is None
                else StrategyShadowStateV1.model_validate(state_record.positions)
            )
            metric = (
                None
                if metric_record is None
                else StrategyShadowLegMetricsV1.model_validate(metric_record.metric_report)
            )
        except ValueError as error:
            raise StrategyRNDAuthorityError("HANDOVER_AUTHORITY_INCOMPLETE") from error
        if (
            state_record is None
            or state is None
            or metric_record is None
            or metric is None
            or state.cash != state_record.cash
            or state.nav != state_record.nav
            or state.hard_risk_violation != state_record.hard_risk_violation
            or content_hash(state) != state_record.state_hash
            or hashlib.sha256(
                canonical_json(metric.model_dump(mode="json", exclude={"metrics_hash"})).encode()
            ).hexdigest()
            != metric_record.metric_hash
        ):
            raise StrategyRNDAuthorityError("HANDOVER_AUTHORITY_INCOMPLETE")
        states[leg.role] = state
        metrics[leg.role] = metric
    champion_metric = metrics["champion"]
    challenger_metric = metrics["challenger"]
    if champion_metric.net_return is None or challenger_metric.net_return is None:
        raise StrategyRNDAuthorityError("HANDOVER_AUTHORITY_INCOMPLETE")

    paper_state = paper.normalized_state
    challenger_state = states["challenger"]
    current_positions = {
        item.instrument_id: item
        for item in paper_state.positions
        if item.quantity > 0 and item.last_close is not None
    }
    target_positions = {
        item.instrument_id: item
        for item in challenger_state.positions
        if item.quantity > 0 and item.last_close is not None
    }
    required_ids = set(current_positions) | set(target_positions)
    snapshots = {
        item.instrument_id: item
        for item in loaded_session.evidence.strategy_snapshot.instruments
        if item.instrument_id in required_ids
    }
    masters = {
        item.id: item
        for item in await session.scalars(select(Instrument).where(Instrument.id.in_(required_ids)))
    }
    if set(snapshots) != required_ids or set(masters) != required_ids:
        raise StrategyRNDAuthorityError("HANDOVER_AUTHORITY_INCOMPLETE")
    drawdown = await session.scalar(
        select(PaperDrawdownLockEventRecord)
        .where(PaperDrawdownLockEventRecord.account_id == paper.account_id)
        .order_by(PaperDrawdownLockEventRecord.event_seq.desc())
        .limit(1)
    )
    instruments: list[HandoverInstrumentV1] = []
    for instrument_id in sorted(required_ids, key=lambda value: value.int):
        snapshot = snapshots[instrument_id]
        master = masters[instrument_id]
        current = current_positions.get(instrument_id)
        asset_type = {
            "stock": "common_stock",
            "equity_etf": "ETF",
        }.get(snapshot.asset_type)
        if (
            asset_type is None
            or master.currency != "USD"
            or master.exchange not in US_EQUITY_EXCHANGES
            or not snapshot.sector_exposures
            or len(snapshot.dollar_volumes) < 20
        ):
            raise StrategyRNDAuthorityError("HANDOVER_AUTHORITY_INCOMPLETE")
        if current is not None and current.last_close is None:
            raise StrategyRNDAuthorityError("HANDOVER_AUTHORITY_INCOMPLETE")
        instruments.append(
            HandoverInstrumentV1(
                instrument_id=instrument_id,
                current_weight=(
                    Decimal(0)
                    if current is None
                    else canonical_decimal(
                        current.quantity * _required_close(current.last_close) / paper_state.nav
                    )
                ),
                market="US",
                currency="USD",
                asset_type=cast(Literal["ETF", "common_stock"], asset_type),
                reference_close=snapshot.raw_close,
                adv20_usd=canonical_decimal(
                    sum(snapshot.dollar_volumes[-20:], start=Decimal(0)) / Decimal(20)
                ),
                available_at=loaded_session.maximum_available_at,
                blocked=not snapshot.eligible_for_new_position,
                sector_exposures=tuple(
                    HandoverSectorExposureV1(
                        sector=item.sector,
                        weight=item.weight,
                    )
                    for item in snapshot.sector_exposures
                ),
            )
        )
    targets = tuple(
        HandoverTargetV1(
            instrument_id=instrument_id,
            weight=canonical_decimal(
                position.quantity * _required_close(position.last_close) / challenger_state.nav
            ),
        )
        for instrument_id, position in sorted(
            target_positions.items(), key=lambda item: item[0].int
        )
    )
    try:
        cost_rate = canonical_decimal(
            (
                Decimal(str(cost_policy.parameters["commission_bps"]))
                + Decimal(str(cost_policy.parameters["slippage_bps"]))
            )
            / Decimal(10_000)
        )
        return evaluate_handover(
            HandoverSnapshotV1(
                paper_snapshot_id=paper.snapshot_id,
                nav=paper_state.nav,
                current_cash_weight=canonical_decimal(paper_state.cash / paper_state.nav),
                drawdown_state=cast(
                    Literal["unlocked", "locked", "recovery_eligible"],
                    "unlocked" if drawdown is None else drawdown.to_state,
                ),
                instruments=tuple(instruments),
                shadow_excess_return=canonical_decimal(
                    challenger_metric.net_return - champion_metric.net_return
                ),
            ),
            HandoverChallengerV1(
                strategy_definition_id=group.challenger_strategy_definition_id,
                target_cash_weight=canonical_decimal(challenger_state.cash / challenger_state.nav),
                targets=targets,
            ),
            HandoverAuthorityV1(
                source_policy_id=source_policy.record_id,
                cost_policy_id=cost_policy.id,
                risk_policy_id=risk_record.id,
                decision_time=cutoff,
                available_at=loaded_session.maximum_available_at,
                estimated_cost_rate=cost_rate,
                complete=True,
                authority_hash=finalization.final_authority_hash,
            ),
            risk_policy,
        )
    except (ArithmeticError, KeyError, TypeError, ValueError) as error:
        raise StrategyRNDAuthorityError("HANDOVER_AUTHORITY_INCOMPLETE") from error


def _required_close(value: Decimal | None) -> Decimal:
    if value is None:
        raise StrategyRNDAuthorityError("HANDOVER_AUTHORITY_INCOMPLETE")
    return value
