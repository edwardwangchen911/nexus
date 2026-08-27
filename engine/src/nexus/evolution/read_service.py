"""Verified, redacted read models for the strategy-evolution workbench."""

from __future__ import annotations

from calendar import monthrange
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import and_, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.schemas.evolution import (
    ActiveFamilyResponse,
    ChampionStateResponse,
    ComparisonResponse,
    EvolutionCursorCodec,
    EvolutionCursorState,
    EvolutionState,
    EvolutionViewStatus,
    ExperimentDetailResponse,
    ExperimentEventResponse,
    ExperimentSummaryResponse,
    GateResultResponse,
    MetricPairResponse,
    RegistryEventResponse,
    SearchAxisSummaryResponse,
    ShadowProgressResponse,
    StrategyEvolutionData,
)
from nexus.evolution.evaluation import ChampionComparisonRunV1, PromotionDecisionV1
from nexus.evolution.handover import HandoverEvaluationV1
from nexus.evolution.jobs import strategy_generation_eligibility
from nexus.evolution.models import StrategyExperimentProposalV1
from nexus.evolution.registry import registry_chain_is_valid
from nexus.evolution.repository import (
    experiment_chain_is_valid,
    experiment_projection_is_valid,
)
from nexus.evolution.shadow.models import (
    StrategyShadowFinalGateV1,
    StrategyShadowLegMetricsV1,
)
from nexus.persistence.models.backtest import StrategyDefinitionRecord
from nexus.persistence.models.evolution import (
    StrategyChampionComparisonRecord,
    StrategyExperimentEventRecord,
    StrategyExperimentFamilyRecord,
    StrategyExperimentRecord,
    StrategyExperimentStateProjectionRecord,
    StrategyPromotionDecisionRecord,
    StrategyRegistryEventRecord,
    StrategyRegistryHeadRecord,
    StrategyRNDOutputRecord,
    StrategyShadowCheckpointMetricRecord,
    StrategyShadowCheckpointRecord,
    StrategyShadowFinalizationRecord,
    StrategyShadowGroupRecord,
    StrategyShadowLegRecord,
    StrategyShadowSessionAuthorityRecord,
)

_TERMINAL_STATES = (
    "REJECTED",
    "BACKTEST_FAILED",
    "SHADOW_FAILED",
    "EXPIRED",
    "VETOED",
    "PROMOTED",
    "STALE_CHAMPION_BASELINE",
)


class EvolutionReadError(ValueError):
    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class EvolutionReadService:
    """Assemble browser-safe views only after verifying persisted event authorities."""

    def __init__(
        self,
        session: AsyncSession,
        *,
        now: datetime,
        cursor_secrets: tuple[str, ...],
    ) -> None:
        self._session = session
        self._now = _utc(now)
        self._cursor = EvolutionCursorCodec(cursor_secrets, self._now)

    async def overview(self, *, limit: int, cursor: str | None) -> StrategyEvolutionData:
        head, champion, registry_events, registry_valid = await self._registry_authority()
        family_projections_valid = await self._all_family_projections_valid()
        generation = await strategy_generation_eligibility(
            self._session,
            now=self._now,
        )
        cursor_state = None
        if cursor is not None:
            try:
                cursor_state = self._cursor.decode(cursor)
            except ValueError as error:
                raise EvolutionReadError("CURSOR_INVALID") from error

        statement = (
            select(
                StrategyExperimentRecord,
                StrategyExperimentFamilyRecord,
                StrategyExperimentStateProjectionRecord,
                StrategyDefinitionRecord,
            )
            .join(
                StrategyExperimentFamilyRecord,
                StrategyExperimentFamilyRecord.id == StrategyExperimentRecord.family_id,
            )
            .join(
                StrategyExperimentStateProjectionRecord,
                StrategyExperimentStateProjectionRecord.family_id
                == StrategyExperimentFamilyRecord.id,
            )
            .join(
                StrategyDefinitionRecord,
                StrategyDefinitionRecord.id
                == StrategyExperimentRecord.candidate_strategy_definition_id,
            )
            .order_by(
                StrategyExperimentRecord.created_at.desc(),
                StrategyExperimentRecord.id.desc(),
            )
            .limit(limit + 1)
        )
        if cursor_state is not None:
            statement = statement.where(
                or_(
                    StrategyExperimentRecord.created_at < cursor_state.after_created_at,
                    and_(
                        StrategyExperimentRecord.created_at == cursor_state.after_created_at,
                        StrategyExperimentRecord.id < cursor_state.after_experiment_id,
                    ),
                )
            )
        rows = tuple((await self._session.execute(statement)).tuples())
        visible_rows = rows[:limit]
        experiments = tuple(
            _experiment_summary(experiment, family, projection, definition)
            for experiment, family, projection, definition in visible_rows
        )
        next_cursor = None
        if len(rows) > limit and visible_rows:
            last = visible_rows[-1][0]
            next_cursor = self._cursor.encode(
                EvolutionCursorState(
                    decision_time=self._now,
                    after_created_at=last.created_at,
                    after_experiment_id=last.id,
                )
            )

        active_row = (
            (
                await self._session.execute(
                    select(StrategyExperimentFamilyRecord, StrategyExperimentStateProjectionRecord)
                    .join(
                        StrategyExperimentStateProjectionRecord,
                        StrategyExperimentStateProjectionRecord.family_id
                        == StrategyExperimentFamilyRecord.id,
                    )
                    .where(
                        StrategyExperimentStateProjectionRecord.current_state.not_in(
                            _TERMINAL_STATES
                        )
                    )
                    .order_by(
                        StrategyExperimentFamilyRecord.created_at.desc(),
                        StrategyExperimentFamilyRecord.id.desc(),
                    )
                    .limit(1)
                )
            )
            .tuples()
            .first()
        )
        active_family = (
            None
            if active_row is None
            else await self._family_response(active_row[0], active_row[1])
        )
        latest_family_at = await self._session.scalar(
            select(func.max(StrategyExperimentFamilyRecord.created_at))
        )
        cooldown_until = (
            None if latest_family_at is None else _calendar_quarter_after(latest_family_at)
        )
        safe_history = await self._registry_history(head, registry_events)
        status: EvolutionViewStatus = (
            "stale"
            if not registry_valid
            else "partial"
            if not family_projections_valid
            else "empty"
            if not experiments and active_family is None
            else "ready"
        )
        return StrategyEvolutionData(
            status=status,
            write_enabled=registry_valid and family_projections_valid,
            write_block_reason=(
                None
                if registry_valid and family_projections_valid
                else "REGISTRY_AUTHORITY_INVALID"
                if not registry_valid
                else "EXPERIMENT_PROJECTION_INVALID"
            ),
            experiment_start_enabled=(
                registry_valid and family_projections_valid and generation.eligible
            ),
            experiment_start_reason=(
                generation.reason_code
                if registry_valid and family_projections_valid
                else "REGISTRY_AUTHORITY_INVALID"
                if not registry_valid
                else "EXPERIMENT_PROJECTION_INVALID"
            ),
            champion=champion,
            active_family=active_family,
            cooldown_until=cooldown_until,
            experiments=experiments,
            registry_events=safe_history[:20],
            next_cursor=next_cursor,
        )

    async def detail(self, experiment_id: UUID) -> ExperimentDetailResponse:
        head, _, _, registry_valid = await self._registry_authority()
        row = (
            (
                await self._session.execute(
                    select(
                        StrategyExperimentRecord,
                        StrategyExperimentFamilyRecord,
                        StrategyExperimentStateProjectionRecord,
                        StrategyDefinitionRecord,
                    )
                    .join(
                        StrategyExperimentFamilyRecord,
                        StrategyExperimentFamilyRecord.id == StrategyExperimentRecord.family_id,
                    )
                    .join(
                        StrategyExperimentStateProjectionRecord,
                        StrategyExperimentStateProjectionRecord.family_id
                        == StrategyExperimentFamilyRecord.id,
                    )
                    .join(
                        StrategyDefinitionRecord,
                        StrategyDefinitionRecord.id
                        == StrategyExperimentRecord.candidate_strategy_definition_id,
                    )
                    .where(StrategyExperimentRecord.id == experiment_id)
                )
            )
            .tuples()
            .first()
        )
        if row is None:
            raise EvolutionReadError("EXPERIMENT_NOT_FOUND")
        experiment, family, projection, definition = row
        events = tuple(
            await self._session.scalars(
                select(StrategyExperimentEventRecord)
                .where(StrategyExperimentEventRecord.family_id == family.id)
                .order_by(StrategyExperimentEventRecord.sequence)
            )
        )
        family_valid = experiment_chain_is_valid(family.id, events, projection)
        comparison, comparison_valid = await self._comparison(experiment.id)
        shadow, shadow_valid = await self._shadow(experiment.id)
        gates, gates_valid = await self._gates(experiment.id)
        if projection.current_state not in {"PROPOSED", "ADMITTED"}:
            comparison_valid = comparison_valid and comparison.status != "unavailable"
        if projection.current_state in {
            "SHADOW_RUNNING",
            "SHADOW_FAILED",
            "PROMOTION_ELIGIBLE",
            "PROMOTED",
        }:
            shadow_valid = shadow_valid and shadow.status != "not_started"
        if projection.current_state in {
            "BACKTEST_PASSED",
            "SHADOW_RUNNING",
            "SHADOW_FAILED",
            "PROMOTION_ELIGIBLE",
            "PROMOTED",
        }:
            gates_valid = gates_valid and bool(gates)
        integrity_valid = registry_valid and family_valid and comparison_valid and shadow_valid
        evidence_valid = comparison_valid and shadow_valid and gates_valid
        integrity_status: Literal["verified", "partial", "stale"] = (
            "stale"
            if not registry_valid or family.champion_generation != head.active_generation
            else "verified"
            if integrity_valid and evidence_valid
            else "partial"
        )
        write_enabled = integrity_status == "verified" and projection.current_state in {
            "ADMITTED",
            "BACKTEST_RUNNING",
            "BACKTEST_PASSED",
            "SHADOW_RUNNING",
            "PROMOTION_ELIGIBLE",
        }
        block_reason = (
            None
            if write_enabled
            else "STALE_CHAMPION_BASELINE"
            if integrity_status == "stale"
            else "EVIDENCE_INCOMPLETE"
            if integrity_status == "partial"
            else "EXPERIMENT_STATE_NOT_WRITABLE"
        )
        return ExperimentDetailResponse(
            experiment=_experiment_summary(experiment, family, projection, definition),
            family=await self._family_response(family, projection),
            comparison=comparison,
            shadow=shadow,
            gates=gates,
            events=tuple(
                ExperimentEventResponse(
                    sequence=event.sequence,
                    event_type=event.event_type,
                    occurred_at=event.occurred_at,
                    event_hash=event.event_hash,
                )
                for event in events
            ),
            integrity_status=integrity_status,
            write_enabled=write_enabled,
            write_block_reason=block_reason,
        )

    async def require_registry_integrity(self) -> StrategyRegistryHeadRecord:
        head, _, _, valid = await self._registry_authority(lock=True)
        if not valid or not await self._all_family_projections_valid():
            raise EvolutionReadError("REGISTRY_AUTHORITY_INVALID")
        return head

    async def require_registry_chain_integrity(self) -> StrategyRegistryHeadRecord:
        """Allow partial family evidence reads while still binding the response ETag."""
        head, _, _, valid = await self._registry_authority()
        if not valid:
            raise EvolutionReadError("REGISTRY_AUTHORITY_INVALID")
        return head

    async def _all_family_projections_valid(self) -> bool:
        projections = tuple(
            await self._session.scalars(
                select(StrategyExperimentStateProjectionRecord).order_by(
                    StrategyExperimentStateProjectionRecord.family_id
                )
            )
        )
        for projection in projections:
            if not await experiment_projection_is_valid(self._session, projection):
                return False
        return True

    async def _registry_authority(
        self, *, lock: bool = False
    ) -> tuple[
        StrategyRegistryHeadRecord,
        ChampionStateResponse,
        tuple[StrategyRegistryEventRecord, ...],
        bool,
    ]:
        head_statement = select(StrategyRegistryHeadRecord).where(
            StrategyRegistryHeadRecord.channel == "internal_paper"
        )
        if lock:
            head_statement = head_statement.with_for_update()
        head = await self._session.scalar(head_statement)
        if head is None:
            raise EvolutionReadError("STRATEGY_REGISTRY_INCOMPLETE")
        definition = await self._session.get(StrategyDefinitionRecord, head.active_definition_id)
        if definition is None:
            raise EvolutionReadError("STRATEGY_REGISTRY_INCOMPLETE")
        events = tuple(
            await self._session.scalars(
                select(StrategyRegistryEventRecord)
                .where(StrategyRegistryEventRecord.channel == "internal_paper")
                .order_by(StrategyRegistryEventRecord.sequence)
            )
        )
        valid = registry_chain_is_valid(head, events)
        active_event = next(
            (
                event
                for event in reversed(events)
                if event.event_type in {"GENESIS", "PROMOTE", "ROLLBACK"}
                and event.champion_generation == head.active_generation
                and event.to_definition_id == head.active_definition_id
            ),
            None,
        )
        pending = (
            None
            if head.pending_event_id is None
            else next((item for item in events if item.id == head.pending_event_id), None)
        )
        if active_event is None or (
            pending is not None and pending.event_type not in {"PROMOTE", "ROLLBACK"}
        ):
            valid = False
        champion = ChampionStateResponse(
            strategy_version=definition.strategy_version,
            generation=head.active_generation,
            registry_version=head.version,
            event_sequence=head.event_sequence,
            effective_session=None if active_event is None else active_event.effective_session,
            auto_promotion_paused=head.auto_promotion_paused,
            pending_transition=cast(
                Literal["PROMOTE", "ROLLBACK"] | None,
                None
                if pending is None or pending.event_type not in {"PROMOTE", "ROLLBACK"}
                else pending.event_type,
            ),
            pending_effective_session=head.pending_effective_session,
        )
        return head, champion, events, valid

    async def _registry_history(
        self,
        head: StrategyRegistryHeadRecord,
        events: tuple[StrategyRegistryEventRecord, ...],
    ) -> tuple[RegistryEventResponse, ...]:
        definitions = {
            item.id: item
            for item in tuple(
                await self._session.scalars(
                    select(StrategyDefinitionRecord).where(
                        StrategyDefinitionRecord.id.in_(
                            {event.to_definition_id for event in events}
                        )
                    )
                )
            )
        }
        return tuple(
            RegistryEventResponse(
                sequence=event.sequence,
                event_type=cast(
                    Literal["GENESIS", "PROMOTE", "ROLLBACK", "PAUSE", "RESUME", "VETO"],
                    event.event_type,
                ),
                target_strategy_version=(
                    definitions[event.to_definition_id].strategy_version
                    if event.to_definition_id in definitions
                    else "unavailable"
                ),
                champion_generation=event.champion_generation,
                occurred_at=event.occurred_at,
                effective_session=event.effective_session,
                status=(
                    "pending"
                    if event.id == head.pending_event_id
                    else "effective"
                    if event.event_type in {"GENESIS", "PROMOTE", "ROLLBACK"}
                    and event.champion_generation <= head.active_generation
                    else "recorded"
                ),
                event_hash=event.event_hash,
            )
            for event in reversed(events)
        )

    async def _family_response(
        self,
        family: StrategyExperimentFamilyRecord,
        projection: StrategyExperimentStateProjectionRecord,
    ) -> ActiveFamilyResponse:
        proposal = None
        if family.rnd_output_id is not None:
            output = await self._session.get(StrategyRNDOutputRecord, family.rnd_output_id)
            if output is not None and output.validation_status == "VALID":
                try:
                    proposal = StrategyExperimentProposalV1.model_validate(output.proposal)
                except ValueError:
                    proposal = None
        axes = (
            ()
            if proposal is None
            else tuple(
                SearchAxisSummaryResponse(axis=item.axis, candidate_value_count=len(item.values))
                for item in proposal.mutation_axes
            )
        )
        return ActiveFamilyResponse(
            family_id=family.id,
            created_at=family.created_at,
            hypothesis=None if proposal is None else proposal.hypothesis,
            expected_mechanism=None if proposal is None else proposal.expected_mechanism,
            search_axes=axes,
            candidate_count=family.candidate_count,
            state=cast(EvolutionState, projection.current_state),
            projection_version=projection.version,
            cumulative_oos_trial_count=projection.cumulative_oos_trial_count,
            authority_hash=family.authority_hash,
            protocol_hash=family.protocol_hash,
            code_hash=family.code_hash,
            next_step=_next_step(projection.current_state),
        )

    async def _comparison(self, experiment_id: UUID) -> tuple[ComparisonResponse, bool]:
        record = await self._session.scalar(
            select(StrategyChampionComparisonRecord).where(
                StrategyChampionComparisonRecord.experiment_id == experiment_id
            )
        )
        unavailable = ComparisonResponse(
            status="unavailable",
            annualized_return=_empty_pair(),
            sharpe=_empty_pair(),
            max_drawdown=_empty_pair(),
            double_cost_return=_empty_pair(),
            spy_comparison_complete=None,
            equal_weight_comparison_complete=None,
            data_mode="unavailable",
        )
        if record is None:
            return unavailable, True
        try:
            report = ChampionComparisonRunV1.model_validate(record.metric_report)
        except ValueError:
            return unavailable.model_copy(update={"status": "partial"}), False
        return (
            ComparisonResponse(
                status="available",
                valid_oos_window_count=report.valid_oos_window_count,
                cumulative_oos_trial_count=report.cumulative_oos_trial_count,
                annualized_return=_pair(
                    report.challenger_annualized_return,
                    report.champion_annualized_return,
                ),
                sharpe=_pair(report.challenger_sharpe, report.champion_sharpe),
                max_drawdown=_pair(
                    report.challenger_max_drawdown,
                    report.champion_max_drawdown,
                ),
                double_cost_return=_pair(
                    report.challenger_double_cost_return,
                    report.champion_double_cost_return,
                ),
                spy_comparison_complete=report.benchmark_comparison_complete,
                equal_weight_comparison_complete=report.benchmark_comparison_complete,
                data_mode="point_in_time",
                authority_hash=record.authority_hash,
                protocol_hash=record.protocol_hash,
                cost_policy_hash=record.cost_policy_hash,
                comparison_hash=record.comparison_hash,
            ),
            True,
        )

    async def _shadow(self, experiment_id: UUID) -> tuple[ShadowProgressResponse, bool]:
        group = await self._session.scalar(
            select(StrategyShadowGroupRecord).where(
                StrategyShadowGroupRecord.experiment_id == experiment_id
            )
        )
        if group is None:
            return (
                ShadowProgressResponse(
                    status="not_started",
                    completed_sessions=0,
                    checkpoints_completed=(),
                    full_fill_count=0,
                    after_cost_excess_return=None,
                    drawdown_difference=None,
                    hard_risk_violation_count=0,
                    data_complete=True,
                ),
                True,
            )
        session_count = int(
            await self._session.scalar(
                select(func.count())
                .select_from(StrategyShadowSessionAuthorityRecord)
                .where(StrategyShadowSessionAuthorityRecord.group_id == group.id)
            )
            or 0
        )
        checkpoints = tuple(
            await self._session.scalars(
                select(StrategyShadowCheckpointRecord)
                .where(StrategyShadowCheckpointRecord.group_id == group.id)
                .order_by(StrategyShadowCheckpointRecord.checkpoint_ordinal)
            )
        )
        latest = checkpoints[-1] if checkpoints else None
        metrics_by_role: dict[str, StrategyShadowLegMetricsV1] = {}
        valid = True
        if latest is not None:
            metric_rows = tuple(
                (
                    await self._session.execute(
                        select(
                            StrategyShadowLegRecord.role,
                            StrategyShadowCheckpointMetricRecord.metric_report,
                        )
                        .join(
                            StrategyShadowLegRecord,
                            StrategyShadowLegRecord.id
                            == StrategyShadowCheckpointMetricRecord.leg_id,
                        )
                        .where(StrategyShadowCheckpointMetricRecord.checkpoint_id == latest.id)
                    )
                ).tuples()
            )
            try:
                metrics_by_role = {
                    role: StrategyShadowLegMetricsV1.model_validate(report)
                    for role, report in metric_rows
                }
            except ValueError:
                valid = False
                metrics_by_role = {}
        challenger = metrics_by_role.get("challenger")
        champion = metrics_by_role.get("champion")
        finalization = await self._session.scalar(
            select(StrategyShadowFinalizationRecord).where(
                StrategyShadowFinalizationRecord.group_id == group.id
            )
        )
        status: Literal["not_started", "running", "failed", "finalized", "stale"] = (
            "stale"
            if group.status == "stale"
            else "finalized"
            if finalization is not None
            else "failed"
            if group.status == "failed"
            else "running"
        )
        return (
            ShadowProgressResponse(
                status=status,
                completed_sessions=session_count,
                checkpoints_completed=cast(
                    tuple[Literal[5, 21, 63], ...],
                    tuple(item.checkpoint_ordinal for item in checkpoints),
                ),
                full_fill_count=0 if challenger is None else challenger.full_fill_count,
                after_cost_excess_return=_difference(
                    None if challenger is None else challenger.net_return,
                    None if champion is None else champion.net_return,
                ),
                drawdown_difference=_difference(
                    None if challenger is None else challenger.max_drawdown,
                    None if champion is None else champion.max_drawdown,
                ),
                hard_risk_violation_count=(
                    0 if challenger is None else challenger.hard_risk_violation_count
                ),
                data_complete=(
                    session_count <= group.evaluation_sessions + group.grace_sessions
                    and (latest is None or latest.checkpoint_ordinal <= session_count)
                ),
                authority_hash=group.authority_hash,
            ),
            valid,
        )

    async def _gates(self, experiment_id: UUID) -> tuple[tuple[GateResultResponse, ...], bool]:
        comparison = await self._session.scalar(
            select(StrategyChampionComparisonRecord).where(
                StrategyChampionComparisonRecord.experiment_id == experiment_id
            )
        )
        if comparison is None:
            return (), True
        decisions = tuple(
            await self._session.scalars(
                select(StrategyPromotionDecisionRecord)
                .where(StrategyPromotionDecisionRecord.comparison_id == comparison.id)
                .order_by(StrategyPromotionDecisionRecord.created_at)
            )
        )
        responses: list[GateResultResponse] = []
        try:
            for decision in sorted(
                decisions,
                key=lambda item: {"historical": 0, "shadow": 1, "handover": 2}[item.stage],
            ):
                if decision.stage == "historical":
                    historical_report = PromotionDecisionV1.model_validate(decision.gate_report)
                    responses.extend(
                        GateResultResponse(
                            stage="historical",
                            gate=gate.gate,
                            status=gate.status,
                            observed=gate.observed,
                            thresholds=gate.thresholds,
                            reason_code=gate.reason_code,
                        )
                        for gate in historical_report.gates
                    )
                elif decision.stage == "shadow":
                    shadow_report = StrategyShadowFinalGateV1.model_validate(decision.gate_report)
                    responses.extend(
                        GateResultResponse(
                            stage="shadow",
                            gate=gate.gate,
                            status=gate.status,
                            observed=gate.observed,
                            thresholds=gate.thresholds,
                            reason_code=gate.reason_code,
                        )
                        for gate in shadow_report.gates
                    )
                else:
                    handover_report = HandoverEvaluationV1.model_validate(decision.gate_report)
                    responses.extend(
                        GateResultResponse(
                            stage="handover",
                            gate=gate.gate,
                            status=gate.status,
                            observed=() if gate.observed is None else (gate.observed,),
                            thresholds=(gate.threshold,),
                            reason_code=gate.reason_code,
                        )
                        for gate in handover_report.gates
                    )
        except (ValueError, KeyError):
            return tuple(responses), False
        return tuple(responses), True


def _experiment_summary(
    experiment: StrategyExperimentRecord,
    family: StrategyExperimentFamilyRecord,
    projection: StrategyExperimentStateProjectionRecord,
    definition: StrategyDefinitionRecord,
) -> ExperimentSummaryResponse:
    return ExperimentSummaryResponse(
        experiment_id=experiment.id,
        family_id=family.id,
        candidate_ordinal=experiment.candidate_ordinal,
        strategy_version=definition.strategy_version,
        state=cast(EvolutionState, projection.current_state),
        created_at=experiment.created_at,
        candidate_hash=experiment.candidate_hash,
        expression_hash=experiment.expression_hash,
    )


def _pair(challenger: Decimal | None, champion: Decimal | None) -> MetricPairResponse:
    return MetricPairResponse(
        challenger=None if challenger is None else str(challenger),
        champion=None if champion is None else str(champion),
        delta=_difference(challenger, champion),
    )


def _empty_pair() -> MetricPairResponse:
    return MetricPairResponse(challenger=None, champion=None, delta=None)


def _difference(left: Decimal | None, right: Decimal | None) -> str | None:
    if left is None or right is None:
        return None
    try:
        return str(left - right)
    except (InvalidOperation, ArithmeticError):
        return None


def _next_step(state: str) -> str:
    return {
        "PROPOSED": "Await deterministic admission.",
        "ADMITTED": "Run deterministic paired backtests.",
        "BACKTEST_RUNNING": "Await point-in-time backtest evidence.",
        "BACKTEST_PASSED": "Start isolated Strategy Shadow.",
        "SHADOW_RUNNING": "Accumulate 5/21/63 session checkpoints.",
        "PROMOTION_ELIGIBLE": "Await deterministic handover and boundary activation.",
        "PROMOTED": "Champion transition is recorded.",
        "VETOED": "Experiment was vetoed; no promotion is permitted.",
        "STALE_CHAMPION_BASELINE": "Restart against the current champion generation.",
    }.get(state, "No further automatic action is permitted.")


def _calendar_quarter_after(value: datetime) -> datetime:
    month = ((value.month - 1 + 3) % 12) + 1
    year = value.year + ((value.month - 1 + 3) // 12)
    day = min(value.day, monthrange(year, month)[1])
    return value.replace(year=year, month=month, day=day)


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError("strategy evolution read time must use UTC")
    return value.astimezone(UTC)
