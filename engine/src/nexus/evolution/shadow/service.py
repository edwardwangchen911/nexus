"""Atomic application service for isolated champion/challenger Strategy Shadow."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.evolution.evaluation import (
    validate_champion_comparison_record,
    validate_promotion_decision_record,
)
from nexus.evolution.policies import load_promotion_policy_v1
from nexus.evolution.repository import (
    FamilyAggregateState,
    append_candidate_event,
    load_verified_experiment_authority,
)
from nexus.evolution.shadow.authority import (
    StrategyShadowAuthorityError,
    load_balanced_paper_baseline,
    load_strategy_shadow_session_evidence,
)
from nexus.evolution.shadow.canonical import (
    content_hash,
    group_authority_content_hash,
)
from nexus.evolution.shadow.metrics import (
    calculate_strategy_shadow_metrics,
    evaluate_strategy_shadow_final_gate,
)
from nexus.evolution.shadow.models import (
    CHECKPOINT_ORDINALS,
    EVALUATION_SESSION_COUNT,
    GRACE_SESSION_COUNT,
    INITIAL_NAV,
    StrategyShadowAdvanceResultV1,
    StrategyShadowFinalGateV1,
    StrategyShadowFinalizationResultV1,
    StrategyShadowLegMetricsV1,
    StrategyShadowPositionV1,
    StrategyShadowSessionEvidenceV1,
    StrategyShadowSessionPointV1,
    StrategyShadowStartResultV1,
    StrategyShadowStateV1,
)
from nexus.evolution.shadow.repository import load_exact_calendar_sessions
from nexus.optimizer.cost_policy import COST_POLICY_ID, load_cost_policy_v1
from nexus.persistence.models.backtest import (
    ExchangeCalendarSessionRecord,
    StrategyDefinitionRecord,
)
from nexus.persistence.models.evolution import (
    StrategyChampionComparisonRecord,
    StrategyExperimentFamilyRecord,
    StrategyExperimentRecord,
    StrategyPromotionDecisionRecord,
    StrategyPromotionPolicyRecord,
    StrategyRegistryHeadRecord,
    StrategyShadowAdvanceAttemptRecord,
    StrategyShadowCheckpointMetricRecord,
    StrategyShadowCheckpointRecord,
    StrategyShadowFillRecord,
    StrategyShadowFinalizationRecord,
    StrategyShadowGroupRecord,
    StrategyShadowLegRecord,
    StrategyShadowLegStateRecord,
    StrategyShadowOrderRecord,
    StrategyShadowSessionAuthorityRecord,
    StrategyShadowSessionPriceInputRecord,
)
from nexus.persistence.models.paper import RiskPolicyRecord
from nexus.persistence.models.runtime import OutboxEvent
from nexus.quality.source_policy import SOURCE_POLICY_ID, load_source_policy_v1
from nexus.risk.policy import RiskPolicy
from nexus.shadow.cost_policy import cost_model_from_policy
from nexus.shadow.repository import load_frozen_calendar
from nexus.simulation.costs import CostModel
from nexus.simulation.engine import execute_target_weights, mark_to_market
from nexus.simulation.models import (
    SimulationBar,
    SimulationFill,
    SimulationInstrumentState,
    SimulationMark,
    SimulationState,
    TargetWeight,
    TargetWeightCommand,
)
from nexus.strategy.baseline import PositionRankState, StrategyDataSnapshot
from nexus.strategy.dsl import (
    baseline_feature_registry,
    compile_strategy,
    evolution_feature_registry_v1,
)
from nexus.strategy.interpreter import CompiledStrategyDecision, evaluate_compiled_strategy

_NEW_YORK = ZoneInfo("America/New_York")


class StrategyShadowServiceError(ValueError):
    """A stable fail-closed Strategy Shadow application error."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


async def start_strategy_shadow(
    session: AsyncSession,
    *,
    experiment_id: UUID,
    now: datetime,
) -> StrategyShadowStartResultV1:
    """Start one same-snapshot comparison without touching either paper ledger."""
    now = _utc(now)
    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended('strategy-shadow-start:' || :experiment_id, 2206))"
        ),
        {"experiment_id": str(experiment_id)},
    )
    experiment_root = await session.get(StrategyExperimentRecord, experiment_id)
    try:
        authority = (
            None
            if experiment_root is None
            else await load_verified_experiment_authority(
                session, experiment_root.family_id, lock=True
            )
        )
    except ValueError as error:
        raise StrategyShadowServiceError("STRATEGY_EXPERIMENT_CHAIN_INVALID") from error
    experiment = (
        None
        if authority is None
        else next(
            (item for item in authority.experiments if item.id == experiment_id),
            None,
        )
    )
    family = None if authority is None else authority.family
    head = await session.scalar(
        select(StrategyRegistryHeadRecord)
        .where(StrategyRegistryHeadRecord.channel == "internal_paper")
        .with_for_update()
    )
    if experiment is None or family is None or authority is None or head is None:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_AUTHORITY_INCOMPLETE")
    candidate_projection = authority.candidate_projection(experiment.id)

    existing = await session.scalar(
        select(StrategyShadowGroupRecord).where(
            StrategyShadowGroupRecord.experiment_id == experiment.id
        )
    )
    if existing is not None:
        return await _replay_start(session, existing, experiment, family)

    if candidate_projection.current_state != "BACKTEST_PASSED":
        raise StrategyShadowServiceError("HISTORICAL_GATES_NOT_PASSED")
    if (
        head.active_generation != family.champion_generation
        or head.active_definition_id != family.parent_strategy_definition_id
    ):
        raise StrategyShadowServiceError("STALE_CHAMPION_BASELINE")

    comparison = await session.scalar(
        select(StrategyChampionComparisonRecord).where(
            StrategyChampionComparisonRecord.experiment_id == experiment.id
        )
    )
    decision = (
        None
        if comparison is None
        else await session.scalar(
            select(StrategyPromotionDecisionRecord).where(
                StrategyPromotionDecisionRecord.comparison_id == comparison.id,
                StrategyPromotionDecisionRecord.stage == "historical",
            )
        )
    )
    if comparison is None or decision is None:
        raise StrategyShadowServiceError("HISTORICAL_DECISION_INCOMPLETE")
    policy_record = await session.get(StrategyPromotionPolicyRecord, decision.promotion_policy_id)
    if policy_record is None:
        raise StrategyShadowServiceError("HISTORICAL_DECISION_INCOMPLETE")
    try:
        policy = load_promotion_policy_v1(policy_record.document, policy_record.content_hash)
        report = validate_champion_comparison_record(comparison)
        persisted_decision = validate_promotion_decision_record(
            decision,
            report,
            policy,
            expected_promotion_policy_id=decision.promotion_policy_id,
        )
    except ValueError as error:
        raise StrategyShadowServiceError("HISTORICAL_DECISION_INCOMPLETE") from error
    if persisted_decision.status != "PASS":
        raise StrategyShadowServiceError("HISTORICAL_GATES_NOT_PASSED")

    baseline = await load_balanced_paper_baseline(session, now=now)
    try:
        source_policy = await load_source_policy_v1(session, instrument_tick_size=None)
        cost_policy = await load_cost_policy_v1(session)
        cost_model_from_policy(cost_policy, started_at=now)
    except ValueError as error:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_POLICY_INCOMPLETE") from error
    if source_policy.record_id != SOURCE_POLICY_ID or source_policy.available_at > now:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_POLICY_INCOMPLETE")

    market_date = now.astimezone(_NEW_YORK).date()
    try:
        calendar, calendar_sessions = await load_frozen_calendar(
            session, market_date=market_date, started_at=now
        )
    except ValueError as error:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_CALENDAR_INCOMPLETE") from error
    group_hash = group_authority_content_hash(
        experiment_id=experiment.id,
        family_id=family.id,
        historical_decision_id=decision.id,
        champion_definition_id=family.parent_strategy_definition_id,
        challenger_definition_id=experiment.candidate_strategy_definition_id,
        baseline_snapshot_id=baseline.snapshot_id,
        calendar_authority_id=calendar.id,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=COST_POLICY_ID,
        champion_generation=family.champion_generation,
        initial_nav=INITIAL_NAV,
        initial_state_hash=baseline.initial_state_hash,
    )
    group = StrategyShadowGroupRecord(
        id=new_uuid7(),
        experiment_id=experiment.id,
        historical_promotion_decision_id=decision.id,
        champion_strategy_definition_id=family.parent_strategy_definition_id,
        challenger_strategy_definition_id=experiment.candidate_strategy_definition_id,
        baseline_snapshot_id=baseline.snapshot_id,
        calendar_authority_id=calendar.id,
        champion_generation=family.champion_generation,
        status="running",
        evaluation_sessions=EVALUATION_SESSION_COUNT,
        grace_sessions=GRACE_SESSION_COUNT,
        initial_nav=INITIAL_NAV,
        idempotency_key=f"strategy-shadow-start-{experiment.id}",
        authority_hash=group_hash,
        created_at=now,
    )
    session.add(group)
    await session.flush()
    session.add_all(
        (
            StrategyShadowLegRecord(
                id=new_uuid7(),
                group_id=group.id,
                role="champion",
                strategy_definition_id=family.parent_strategy_definition_id,
                initial_state_hash=baseline.initial_state_hash,
                created_at=now,
            ),
            StrategyShadowLegRecord(
                id=new_uuid7(),
                group_id=group.id,
                role="challenger",
                strategy_definition_id=experiment.candidate_strategy_definition_id,
                initial_state_hash=baseline.initial_state_hash,
                created_at=now,
            ),
        )
    )
    await append_candidate_event(
        session,
        authority=authority,
        experiment_id=experiment.id,
        event_type="SHADOW_RUNNING",
        idempotency_key=f"strategy-shadow-running-{group.id}",
        occurred_at=now,
        payload={
            "schema_version": "strategy_shadow_running_payload_v1",
            "group_id": str(group.id),
            "authority_hash": group.authority_hash,
            "champion_generation": group.champion_generation,
            "start_session": calendar_sessions[0].isoformat(),
            "evaluation_end_session": calendar_sessions[62].isoformat(),
            "grace_end_session": calendar_sessions[67].isoformat(),
        },
    )
    await _append_start_audit_and_outbox(session, group=group, now=now)
    await session.flush()
    return _start_result(group, calendar_sessions)


@dataclass(frozen=True)
class _LegTransition:
    leg: StrategyShadowLegRecord
    state: StrategyShadowStateV1
    state_hash: str
    transition_hash: str
    fills: tuple[SimulationFill, ...]


async def advance_strategy_shadow(
    session: AsyncSession,
    *,
    group_id: UUID,
    requested_session: date,
    observed_at: datetime,
) -> StrategyShadowAdvanceResultV1:
    """Advance both virtual legs atomically for exactly the next session."""
    observed_at = _utc(observed_at)
    group = await session.scalar(
        select(StrategyShadowGroupRecord)
        .where(StrategyShadowGroupRecord.id == group_id)
        .with_for_update()
    )
    if group is None:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_NOT_FOUND")
    sealed = await session.scalar(
        select(func.count())
        .select_from(StrategyShadowCheckpointRecord)
        .where(
            StrategyShadowCheckpointRecord.group_id == group.id,
            StrategyShadowCheckpointRecord.checkpoint_ordinal == 63,
        )
    )
    if sealed:
        return StrategyShadowAdvanceResultV1(
            group_id=group.id,
            requested_session=requested_session,
            ordinal=None,
            status="sealed",
            reason_code="STRATEGY_SHADOW_EVALUATION_COMPLETE",
        )
    permanent_failure = await session.scalar(
        select(StrategyShadowAdvanceAttemptRecord)
        .where(
            StrategyShadowAdvanceAttemptRecord.group_id == group.id,
            StrategyShadowAdvanceAttemptRecord.status == "failed",
        )
        .order_by(StrategyShadowAdvanceAttemptRecord.created_at)
        .limit(1)
    )
    if permanent_failure is not None:
        return StrategyShadowAdvanceResultV1(
            group_id=group.id,
            requested_session=requested_session,
            ordinal=None,
            status="sealed",
            reason_code="STRATEGY_SHADOW_DATA_GAP_PERMANENT",
        )
    sessions = await load_exact_calendar_sessions(
        session,
        calendar_authority_id=group.calendar_authority_id,
        after=group.created_at.astimezone(_NEW_YORK).date(),
    )
    existing = await session.scalar(
        select(StrategyShadowSessionAuthorityRecord).where(
            StrategyShadowSessionAuthorityRecord.group_id == group.id,
            StrategyShadowSessionAuthorityRecord.session == requested_session,
        )
    )
    if existing is not None:
        await _persist_advance_attempt(
            session,
            group=group,
            requested_session=requested_session,
            observed_at=observed_at,
            status="duplicate",
            reason_code="STRATEGY_SHADOW_SESSION_DUPLICATE",
            evidence_hash=existing.authority_hash,
        )
        return StrategyShadowAdvanceResultV1(
            group_id=group.id,
            requested_session=requested_session,
            ordinal=existing.session_ordinal,
            status="duplicate",
            reason_code="STRATEGY_SHADOW_SESSION_DUPLICATE",
            checkpoint_ordinal=(
                cast(Literal[5, 21, 63], existing.session_ordinal)
                if existing.session_ordinal in CHECKPOINT_ORDINALS
                else None
            ),
        )
    coverage = int(
        await session.scalar(
            select(func.count())
            .select_from(StrategyShadowSessionAuthorityRecord)
            .where(StrategyShadowSessionAuthorityRecord.group_id == group.id)
        )
        or 0
    )
    if coverage >= EVALUATION_SESSION_COUNT:
        return StrategyShadowAdvanceResultV1(
            group_id=group.id,
            requested_session=requested_session,
            ordinal=None,
            status="sealed",
            reason_code="STRATEGY_SHADOW_EVALUATION_COMPLETE",
        )
    ordinal = coverage + 1
    expected_session = sessions[coverage]
    if requested_session != expected_session:
        reason = "STRATEGY_SHADOW_SESSION_OUT_OF_ORDER"
        await _persist_advance_attempt(
            session,
            group=group,
            requested_session=requested_session,
            observed_at=observed_at,
            status="failed",
            reason_code=reason,
            evidence_hash=None,
        )
        await _append_advance_audit_and_outbox(
            session,
            group=group,
            requested_session=requested_session,
            ordinal=ordinal,
            status="failed",
            reason_code=reason,
            observed_at=observed_at,
        )
        return StrategyShadowAdvanceResultV1(
            group_id=group.id,
            requested_session=requested_session,
            ordinal=ordinal,
            status="failed",
            reason_code=reason,
        )
    try:
        loaded = await load_strategy_shadow_session_evidence(
            session,
            group_id=group.id,
            session_date=requested_session,
            ordinal=ordinal,
            calendar_authority_id=group.calendar_authority_id,
            observed_at=observed_at,
        )
        legs = tuple(
            await session.scalars(
                select(StrategyShadowLegRecord)
                .where(StrategyShadowLegRecord.group_id == group.id)
                .order_by(StrategyShadowLegRecord.role)
            )
        )
        if len(legs) != 2 or {item.role for item in legs} != {
            "champion",
            "challenger",
        }:
            raise StrategyShadowServiceError("STRATEGY_SHADOW_LEGS_INCOMPLETE")
        cost_policy = await load_cost_policy_v1(session)
        cost_model = cost_model_from_policy(cost_policy, started_at=group.created_at)
        risk_policy = await _load_exact_risk_policy(session)
        transitions = tuple(
            [
                await _evaluate_leg_transition(
                    session,
                    group=group,
                    leg=leg,
                    ordinal=ordinal,
                    evidence=loaded.evidence,
                    cost_model=cost_model,
                    risk_policy=risk_policy,
                )
                for leg in legs
            ]
        )
    except (StrategyShadowAuthorityError, StrategyShadowServiceError, ValueError) as error:
        reason = getattr(error, "reason_code", "STRATEGY_SHADOW_EVALUATION_FAILED")
        await _persist_advance_attempt(
            session,
            group=group,
            requested_session=requested_session,
            observed_at=observed_at,
            status="failed",
            reason_code=reason,
            evidence_hash=None,
        )
        await _append_advance_audit_and_outbox(
            session,
            group=group,
            requested_session=requested_session,
            ordinal=ordinal,
            status="failed",
            reason_code=reason,
            observed_at=observed_at,
        )
        return StrategyShadowAdvanceResultV1(
            group_id=group.id,
            requested_session=requested_session,
            ordinal=ordinal,
            status="failed",
            reason_code=reason,
        )

    authority_hash = content_hash(
        {
            "schema_version": "strategy_shadow_session_authority_v1",
            "group_authority_hash": group.authority_hash,
            "session_evidence_id": str(loaded.record_id),
            "session_evidence_hash": loaded.evidence.content_hash,
            "session": requested_session.isoformat(),
            "ordinal": ordinal,
            "maximum_available_at": loaded.maximum_available_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        }
    )
    authority = StrategyShadowSessionAuthorityRecord(
        id=new_uuid7(),
        group_id=group.id,
        session=requested_session,
        session_ordinal=ordinal,
        calendar_authority_id=group.calendar_authority_id,
        source_policy_id=SOURCE_POLICY_ID,
        cost_policy_id=COST_POLICY_ID,
        authority_hash=authority_hash,
        available_at=loaded.maximum_available_at,
        created_at=observed_at,
    )
    session.add(authority)
    await session.flush()
    session.add_all(
        StrategyShadowSessionPriceInputRecord(
            id=new_uuid7(),
            session_authority_id=authority.id,
            instrument_id=item.instrument_id,
            raw_price_authority_id=item.raw_price_authority_id,
            adjusted_price_authority_id=item.adjusted_price_authority_id,
            input_hash=content_hash(
                {
                    "schema_version": "strategy_shadow_price_input_v1",
                    "session_authority_hash": authority_hash,
                    **item.model_dump(mode="json"),
                }
            ),
            created_at=observed_at,
        )
        for item in loaded.evidence.execution_bars
    )
    for transition in transitions:
        await _persist_leg_transition(
            session,
            authority=authority,
            transition=transition,
            ordinal=ordinal,
            observed_at=observed_at,
        )
    await _persist_advance_attempt(
        session,
        group=group,
        requested_session=requested_session,
        observed_at=observed_at,
        status="applied",
        reason_code="STRATEGY_SHADOW_SESSION_APPLIED",
        evidence_hash=authority_hash,
    )
    checkpoint = None
    if ordinal in CHECKPOINT_ORDINALS:
        checkpoint = cast(Literal[5, 21, 63], ordinal)
        await _persist_checkpoint(
            session,
            group=group,
            legs=legs,
            ordinal=checkpoint,
            session_date=requested_session,
            observed_at=observed_at,
        )
    await _append_advance_audit_and_outbox(
        session,
        group=group,
        requested_session=requested_session,
        ordinal=ordinal,
        status="applied",
        reason_code="STRATEGY_SHADOW_SESSION_APPLIED",
        observed_at=observed_at,
    )
    await session.flush()
    return StrategyShadowAdvanceResultV1(
        group_id=group.id,
        requested_session=requested_session,
        ordinal=ordinal,
        status="applied",
        reason_code="STRATEGY_SHADOW_SESSION_APPLIED",
        checkpoint_ordinal=checkpoint,
    )


async def finalize_strategy_shadow(
    session: AsyncSession,
    *,
    group_id: UUID,
    now: datetime,
) -> StrategyShadowFinalizationResultV1:
    """Seal immutable final gates without changing the active strategy registry."""
    now = _utc(now)
    group = await session.scalar(
        select(StrategyShadowGroupRecord)
        .where(StrategyShadowGroupRecord.id == group_id)
        .with_for_update()
    )
    if group is None:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_NOT_FOUND")
    existing = await session.scalar(
        select(StrategyShadowFinalizationRecord).where(
            StrategyShadowFinalizationRecord.group_id == group.id
        )
    )
    if existing is not None:
        experiment_root = await session.get(StrategyExperimentRecord, group.experiment_id)
        try:
            authority = (
                None
                if experiment_root is None
                else await load_verified_experiment_authority(
                    session, experiment_root.family_id, lock=True
                )
            )
        except ValueError as error:
            raise StrategyShadowServiceError("STRATEGY_EXPERIMENT_CHAIN_INVALID") from error
        if authority is None:
            raise StrategyShadowServiceError(
                "STRATEGY_SHADOW_FINALIZATION_AUTHORITY_INCOMPLETE"
            )
        return await _replay_finalization(
            session,
            group=group,
            finalization=existing,
            family_state=cast(
                FamilyAggregateState, authority.family_projection.current_state
            ),
        )

    experiment_root = await session.get(StrategyExperimentRecord, group.experiment_id)
    try:
        authority = (
            None
            if experiment_root is None
            else await load_verified_experiment_authority(
                session, experiment_root.family_id, lock=True
            )
        )
    except ValueError as error:
        raise StrategyShadowServiceError("STRATEGY_EXPERIMENT_CHAIN_INVALID") from error
    experiment = (
        None
        if authority is None
        else next(
            (item for item in authority.experiments if item.id == group.experiment_id),
            None,
        )
    )
    family = None if authority is None else authority.family
    head = await session.scalar(
        select(StrategyRegistryHeadRecord)
        .where(StrategyRegistryHeadRecord.channel == "internal_paper")
        .with_for_update()
    )
    historical_decision = await session.get(
        StrategyPromotionDecisionRecord, group.historical_promotion_decision_id
    )
    if (
        experiment is None
        or family is None
        or authority is None
        or head is None
        or historical_decision is None
        or authority.candidate_projection(experiment.id).current_state
        != "SHADOW_RUNNING"
    ):
        raise StrategyShadowServiceError("STRATEGY_SHADOW_FINALIZATION_AUTHORITY_INCOMPLETE")

    legs = tuple(
        await session.scalars(
            select(StrategyShadowLegRecord)
            .where(StrategyShadowLegRecord.group_id == group.id)
            .order_by(StrategyShadowLegRecord.role)
        )
    )
    if len(legs) != 2 or {item.role for item in legs} != {"champion", "challenger"}:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_LEGS_INCOMPLETE")
    checkpoint = await session.scalar(
        select(StrategyShadowCheckpointRecord).where(
            StrategyShadowCheckpointRecord.group_id == group.id,
            StrategyShadowCheckpointRecord.checkpoint_ordinal == EVALUATION_SESSION_COUNT,
        )
    )
    if checkpoint is None:
        await _require_grace_elapsed(session, group=group, now=now)
    metrics = await _load_final_metrics(session, legs=legs, checkpoint=checkpoint)
    evidence_ids, evidence_hashes = await _load_final_evidence_chain(
        session,
        group=group,
        legs=legs,
        historical_decision=historical_decision,
    )
    champion = metrics[next(item.id for item in legs if item.role == "champion")]
    challenger = metrics[next(item.id for item in legs if item.role == "challenger")]
    final_gate = evaluate_strategy_shadow_final_gate(
        champion=champion,
        challenger=challenger,
        champion_generation=group.champion_generation,
        active_generation=head.active_generation,
        evidence_ids=evidence_ids,
        evidence_hashes=evidence_hashes,
    )
    finalization = StrategyShadowFinalizationRecord(
        id=new_uuid7(),
        group_id=group.id,
        promotion_policy_id=historical_decision.promotion_policy_id,
        status=final_gate.status,
        gate_report=final_gate.model_dump(mode="json"),
        reason_code=final_gate.reason_code,
        final_authority_hash=final_gate.final_authority_hash,
        finalization_hash=final_gate.finalization_hash,
        created_at=now,
    )
    session.add(finalization)
    await session.flush()
    if final_gate.status != "STALE":
        session.add(
            StrategyPromotionDecisionRecord(
                id=new_uuid7(),
                comparison_id=historical_decision.comparison_id,
                promotion_policy_id=historical_decision.promotion_policy_id,
                stage="shadow",
                status=final_gate.status,
                gate_report=final_gate.model_dump(mode="json"),
                reason_code=final_gate.reason_code,
                decision_hash=content_hash(
                    {
                        "schema_version": "strategy_shadow_promotion_decision_v1",
                        "comparison_id": str(historical_decision.comparison_id),
                        "promotion_policy_id": str(historical_decision.promotion_policy_id),
                        "finalization_id": str(finalization.id),
                        "finalization_hash": finalization.finalization_hash,
                        "status": final_gate.status,
                    }
                ),
                created_at=now,
            )
        )
    family_state = _final_family_state(final_gate)
    await append_candidate_event(
        session,
        authority=authority,
        experiment_id=experiment.id,
        event_type=family_state,
        idempotency_key=f"strategy-shadow-finalized-{group.id}",
        occurred_at=now,
        payload={
            "schema_version": "strategy_shadow_finalized_payload_v1",
            "group_id": str(group.id),
            "finalization_id": str(finalization.id),
            "status": final_gate.status,
            "reason_code": final_gate.reason_code,
            "final_authority_hash": final_gate.final_authority_hash,
            "champion_generation": group.champion_generation,
            "active_generation": head.active_generation,
            "live_execution_enabled": False,
        },
    )
    await _append_finalization_audit_and_outbox(
        session,
        group=group,
        finalization=finalization,
        family_state=cast(
            FamilyAggregateState, authority.family_projection.current_state
        ),
        now=now,
    )
    await session.flush()
    return StrategyShadowFinalizationResultV1(
        group_id=group.id,
        finalization_id=finalization.id,
        status=final_gate.status,
        family_state=cast(
            FamilyAggregateState, authority.family_projection.current_state
        ),
        reason_code=final_gate.reason_code,
    )


async def _require_grace_elapsed(
    session: AsyncSession,
    *,
    group: StrategyShadowGroupRecord,
    now: datetime,
) -> None:
    sessions = await load_exact_calendar_sessions(
        session,
        calendar_authority_id=group.calendar_authority_id,
        after=group.created_at.astimezone(_NEW_YORK).date(),
    )
    grace = await session.get(
        ExchangeCalendarSessionRecord,
        {
            "calendar_authority_id": group.calendar_authority_id,
            "session": sessions[EVALUATION_SESSION_COUNT + GRACE_SESSION_COUNT - 1],
        },
    )
    if grace is None or now < grace.decision_at:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_FINALIZATION_NOT_DUE")


async def _load_final_metrics(
    session: AsyncSession,
    *,
    legs: tuple[StrategyShadowLegRecord, ...],
    checkpoint: StrategyShadowCheckpointRecord | None,
) -> dict[UUID, StrategyShadowLegMetricsV1]:
    if checkpoint is None:
        return {
            leg.id: calculate_strategy_shadow_metrics(
                await _load_leg_points(
                    session,
                    leg=leg,
                    through_ordinal=EVALUATION_SESSION_COUNT,
                ),
                checkpoint_ordinal=EVALUATION_SESSION_COUNT,
            )
            for leg in legs
        }
    rows = tuple(
        await session.scalars(
            select(StrategyShadowCheckpointMetricRecord)
            .where(StrategyShadowCheckpointMetricRecord.checkpoint_id == checkpoint.id)
            .order_by(StrategyShadowCheckpointMetricRecord.leg_id)
        )
    )
    if len(rows) != 2 or {item.leg_id for item in rows} != {item.id for item in legs}:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_FINAL_METRICS_INCOMPLETE")
    loaded: dict[UUID, StrategyShadowLegMetricsV1] = {}
    for leg in legs:
        record = next(item for item in rows if item.leg_id == leg.id)
        try:
            persisted = StrategyShadowLegMetricsV1.model_validate(record.metric_report)
        except ValueError as error:
            raise StrategyShadowServiceError("STRATEGY_SHADOW_FINAL_METRICS_INVALID") from error
        recalculated = calculate_strategy_shadow_metrics(
            await _load_leg_points(
                session,
                leg=leg,
                through_ordinal=EVALUATION_SESSION_COUNT,
            ),
            checkpoint_ordinal=EVALUATION_SESSION_COUNT,
        )
        if persisted.metrics_hash != record.metric_hash or persisted != recalculated:
            raise StrategyShadowServiceError("STRATEGY_SHADOW_FINAL_METRICS_INVALID")
        loaded[leg.id] = persisted
    return loaded


async def _load_final_evidence_chain(
    session: AsyncSession,
    *,
    group: StrategyShadowGroupRecord,
    legs: tuple[StrategyShadowLegRecord, ...],
    historical_decision: StrategyPromotionDecisionRecord,
) -> tuple[tuple[UUID, ...], tuple[str, ...]]:
    identifiers: list[UUID] = [
        group.id,
        group.historical_promotion_decision_id,
        group.baseline_snapshot_id,
        group.calendar_authority_id,
    ]
    hashes: list[str] = [group.authority_hash, historical_decision.decision_hash]
    for leg in legs:
        identifiers.append(leg.id)
        hashes.append(leg.initial_state_hash)
    authorities = tuple(
        await session.scalars(
            select(StrategyShadowSessionAuthorityRecord)
            .where(StrategyShadowSessionAuthorityRecord.group_id == group.id)
            .order_by(StrategyShadowSessionAuthorityRecord.session_ordinal)
        )
    )
    identifiers.extend(item.id for item in authorities)
    hashes.extend(item.authority_hash for item in authorities)
    authority_ids = tuple(item.id for item in authorities)
    if authority_ids:
        price_inputs = tuple(
            await session.scalars(
                select(StrategyShadowSessionPriceInputRecord)
                .where(
                    StrategyShadowSessionPriceInputRecord.session_authority_id.in_(authority_ids)
                )
                .order_by(
                    StrategyShadowSessionPriceInputRecord.session_authority_id,
                    StrategyShadowSessionPriceInputRecord.instrument_id,
                )
            )
        )
        identifiers.extend(item.id for item in price_inputs)
        identifiers.extend(item.raw_price_authority_id for item in price_inputs)
        identifiers.extend(item.adjusted_price_authority_id for item in price_inputs)
        hashes.extend(item.input_hash for item in price_inputs)
    states = tuple(
        await session.scalars(
            select(StrategyShadowLegStateRecord)
            .where(StrategyShadowLegStateRecord.leg_id.in_(tuple(item.id for item in legs)))
            .order_by(
                StrategyShadowLegStateRecord.session_ordinal,
                StrategyShadowLegStateRecord.leg_id,
            )
        )
    )
    identifiers.extend(item.id for item in states)
    for state in states:
        hashes.extend((state.state_hash, state.transition_hash))
    state_ids = tuple(item.id for item in states)
    orders: tuple[StrategyShadowOrderRecord, ...] = ()
    if state_ids:
        orders = tuple(
            await session.scalars(
                select(StrategyShadowOrderRecord)
                .where(StrategyShadowOrderRecord.leg_state_id.in_(state_ids))
                .order_by(StrategyShadowOrderRecord.id)
            )
        )
        identifiers.extend(item.id for item in orders)
        hashes.extend(item.order_hash for item in orders)
    order_ids = tuple(item.id for item in orders)
    if order_ids:
        fills = tuple(
            await session.scalars(
                select(StrategyShadowFillRecord)
                .where(StrategyShadowFillRecord.order_id.in_(order_ids))
                .order_by(StrategyShadowFillRecord.id)
            )
        )
        identifiers.extend(item.id for item in fills)
        hashes.extend(item.fill_hash for item in fills)
    checkpoints = tuple(
        await session.scalars(
            select(StrategyShadowCheckpointRecord)
            .where(StrategyShadowCheckpointRecord.group_id == group.id)
            .order_by(StrategyShadowCheckpointRecord.checkpoint_ordinal)
        )
    )
    identifiers.extend(item.id for item in checkpoints)
    hashes.extend(item.authority_hash for item in checkpoints)
    checkpoint_ids = tuple(item.id for item in checkpoints)
    if checkpoint_ids:
        metric_rows = tuple(
            await session.scalars(
                select(StrategyShadowCheckpointMetricRecord)
                .where(StrategyShadowCheckpointMetricRecord.checkpoint_id.in_(checkpoint_ids))
                .order_by(
                    StrategyShadowCheckpointMetricRecord.checkpoint_id,
                    StrategyShadowCheckpointMetricRecord.leg_id,
                )
            )
        )
        identifiers.extend(item.id for item in metric_rows)
        hashes.extend(item.metric_hash for item in metric_rows)
    return _unique_identifiers(identifiers), tuple(hashes)


def _unique_identifiers(values: list[UUID]) -> tuple[UUID, ...]:
    return tuple(dict.fromkeys(values))


def _final_family_state(
    gate: StrategyShadowFinalGateV1,
) -> Literal["PROMOTION_ELIGIBLE", "SHADOW_FAILED", "STALE_CHAMPION_BASELINE"]:
    if gate.status == "PASS":
        return "PROMOTION_ELIGIBLE"
    if gate.status == "STALE":
        return "STALE_CHAMPION_BASELINE"
    return "SHADOW_FAILED"


async def _replay_finalization(
    session: AsyncSession,
    *,
    group: StrategyShadowGroupRecord,
    finalization: StrategyShadowFinalizationRecord,
    family_state: FamilyAggregateState,
) -> StrategyShadowFinalizationResultV1:
    gate = await validate_shadow_finalization_record(
        session,
        group=group,
        finalization=finalization,
    )
    return StrategyShadowFinalizationResultV1(
        group_id=group.id,
        finalization_id=finalization.id,
        status=gate.status,
        family_state=family_state,
        reason_code=gate.reason_code,
    )


async def validate_shadow_finalization_record(
    session: AsyncSession,
    *,
    group: StrategyShadowGroupRecord,
    finalization: StrategyShadowFinalizationRecord,
) -> StrategyShadowFinalGateV1:
    """Rebuild and verify one immutable Shadow finalization and its evidence chain."""
    try:
        gate = StrategyShadowFinalGateV1.model_validate(finalization.gate_report)
    except ValueError as error:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_FINALIZATION_INVALID") from error
    historical_decision = await session.get(
        StrategyPromotionDecisionRecord, group.historical_promotion_decision_id
    )
    legs = tuple(
        await session.scalars(
            select(StrategyShadowLegRecord)
            .where(StrategyShadowLegRecord.group_id == group.id)
            .order_by(StrategyShadowLegRecord.role)
        )
    )
    checkpoint = await session.scalar(
        select(StrategyShadowCheckpointRecord).where(
            StrategyShadowCheckpointRecord.group_id == group.id,
            StrategyShadowCheckpointRecord.checkpoint_ordinal == EVALUATION_SESSION_COUNT,
        )
    )
    if (
        historical_decision is None
        or historical_decision.stage != "historical"
        or len(legs) != 2
        or {item.role for item in legs} != {"champion", "challenger"}
    ):
        raise StrategyShadowServiceError("STRATEGY_SHADOW_FINALIZATION_INVALID")
    metrics = await _load_final_metrics(session, legs=legs, checkpoint=checkpoint)
    evidence_ids, evidence_hashes = await _load_final_evidence_chain(
        session,
        group=group,
        legs=legs,
        historical_decision=historical_decision,
    )
    champion = metrics[next(item.id for item in legs if item.role == "champion")]
    challenger = metrics[next(item.id for item in legs if item.role == "challenger")]
    expected = evaluate_strategy_shadow_final_gate(
        champion=champion,
        challenger=challenger,
        champion_generation=group.champion_generation,
        active_generation=gate.active_generation,
        evidence_ids=evidence_ids,
        evidence_hashes=evidence_hashes,
    )
    if (
        finalization.group_id != group.id
        or finalization.promotion_policy_id != historical_decision.promotion_policy_id
        or gate.champion_generation != group.champion_generation
        or gate != expected
        or gate.status != finalization.status
        or gate.reason_code != finalization.reason_code
        or gate.final_authority_hash != finalization.final_authority_hash
        or gate.finalization_hash != finalization.finalization_hash
    ):
        raise StrategyShadowServiceError("STRATEGY_SHADOW_FINALIZATION_INVALID")
    return gate


def validate_shadow_promotion_decision_record(
    record: StrategyPromotionDecisionRecord,
    *,
    comparison_id: UUID,
    historical_decision: StrategyPromotionDecisionRecord,
    finalization: StrategyShadowFinalizationRecord,
    gate: StrategyShadowFinalGateV1,
) -> StrategyShadowFinalGateV1:
    """Verify the Shadow decision binds the exact finalization and historical policy."""
    expected_hash = content_hash(
        {
            "schema_version": "strategy_shadow_promotion_decision_v1",
            "comparison_id": str(comparison_id),
            "promotion_policy_id": str(historical_decision.promotion_policy_id),
            "finalization_id": str(finalization.id),
            "finalization_hash": finalization.finalization_hash,
            "status": gate.status,
        }
    )
    try:
        persisted = StrategyShadowFinalGateV1.model_validate(record.gate_report)
    except ValueError as error:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_DECISION_INVALID") from error
    if (
        record.stage != "shadow"
        or record.comparison_id != comparison_id
        or record.promotion_policy_id != historical_decision.promotion_policy_id
        or record.status != gate.status
        or record.reason_code != gate.reason_code
        or record.decision_hash != expected_hash
        or persisted != gate
    ):
        raise StrategyShadowServiceError("STRATEGY_SHADOW_DECISION_INVALID")
    return persisted


async def _append_finalization_audit_and_outbox(
    session: AsyncSession,
    *,
    group: StrategyShadowGroupRecord,
    finalization: StrategyShadowFinalizationRecord,
    family_state: str,
    now: datetime,
) -> None:
    payload = {
        "schema_version": "strategy_shadow_finalized_v1",
        "group_id": str(group.id),
        "finalization_id": str(finalization.id),
        "status": finalization.status,
        "family_state": family_state,
        "reason_code": finalization.reason_code,
        "final_authority_hash": finalization.final_authority_hash,
        "live_execution_enabled": False,
    }
    await append_audit_event(
        session,
        actor_id="strategy-shadow-service",
        action="strategy.shadow.finalized",
        schema_version="1",
        payload={
            "schema_version": "strategy_shadow_finalized_v1",
            "event_type": "SHADOW_FINALIZED",
            "status": finalization.status,
            "reason_code": finalization.reason_code,
            "final_authority_hash": finalization.final_authority_hash,
            "live_execution_enabled": False,
        },
        occurred_at=now,
    )
    session.add(
        OutboxEvent(
            event_type="strategy.shadow.finalized",
            aggregate_id=group.id,
            schema_version="1",
            payload=payload,
            published_at=None,
        )
    )


async def _replay_start(
    session: AsyncSession,
    group: StrategyShadowGroupRecord,
    experiment: StrategyExperimentRecord,
    family: StrategyExperimentFamilyRecord,
) -> StrategyShadowStartResultV1:
    if (
        group.champion_strategy_definition_id != family.parent_strategy_definition_id
        or group.challenger_strategy_definition_id != experiment.candidate_strategy_definition_id
        or group.champion_generation != family.champion_generation
        or group.initial_nav != INITIAL_NAV
    ):
        raise StrategyShadowServiceError("STRATEGY_SHADOW_IDEMPOTENCY_CONFLICT")
    sessions = await load_exact_calendar_sessions(
        session,
        calendar_authority_id=group.calendar_authority_id,
        after=group.created_at.astimezone(_NEW_YORK).date(),
    )
    return _start_result(group, sessions)


def _start_result(
    group: StrategyShadowGroupRecord, sessions: tuple[date, ...]
) -> StrategyShadowStartResultV1:
    return StrategyShadowStartResultV1(
        group_id=group.id,
        experiment_id=group.experiment_id,
        start_session=sessions[0],
        evaluation_end_session=sessions[62],
        grace_end_session=sessions[67],
        authority_hash=group.authority_hash,
    )


async def _append_start_audit_and_outbox(
    session: AsyncSession, *, group: StrategyShadowGroupRecord, now: datetime
) -> None:
    payload = {
        "schema_version": "strategy_shadow_started_v1",
        "group_id": str(group.id),
        "authority_hash": group.authority_hash,
        "champion_generation": group.champion_generation,
        "live_execution_enabled": False,
    }
    await append_audit_event(
        session,
        actor_id="strategy-shadow-service",
        action="strategy.shadow.started",
        schema_version="1",
        payload={
            "schema_version": "strategy_shadow_started_v1",
            "event_type": "SHADOW_STARTED",
            "status": "RUNNING",
            "authority_hash": group.authority_hash,
            "live_execution_enabled": False,
        },
        occurred_at=now,
    )
    session.add(
        OutboxEvent(
            event_type="strategy.shadow.started",
            aggregate_id=group.id,
            schema_version="1",
            payload=payload,
            published_at=None,
        )
    )


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise StrategyShadowServiceError("STRATEGY_SHADOW_TIME_INVALID")
    return value.astimezone(UTC)


async def _evaluate_leg_transition(
    session: AsyncSession,
    *,
    group: StrategyShadowGroupRecord,
    leg: StrategyShadowLegRecord,
    ordinal: int,
    evidence: StrategyShadowSessionEvidenceV1,
    cost_model: CostModel,
    risk_policy: RiskPolicy,
) -> _LegTransition:
    previous, previous_hash = await _load_previous_leg_state(
        session, group=group, leg=leg, ordinal=ordinal
    )
    definition = await session.get(StrategyDefinitionRecord, leg.strategy_definition_id)
    if definition is None:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_DEFINITION_INVALID")
    registry = (
        baseline_feature_registry()
        if definition.strategy_version == "baseline_v1"
        else evolution_feature_registry_v1()
    )
    try:
        compiled = compile_strategy(definition.document, registry)
    except ValueError as error:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_DEFINITION_INVALID") from error
    current_positions = tuple(
        item.instrument_id for item in previous.positions if item.quantity > 0
    )
    strategy_ids = {item.instrument_id for item in evidence.strategy_snapshot.instruments}
    if not set(current_positions) <= strategy_ids:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_UNIVERSE_INCOMPLETE")
    snapshot: StrategyDataSnapshot = evidence.strategy_snapshot.model_copy(
        update={
            "current_positions": current_positions,
            "position_rank_states": previous.position_rank_states,
        }
    )
    try:
        decision = evaluate_compiled_strategy(compiled, snapshot, snapshot.decision_session)
    except ValueError as error:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_EVALUATION_FAILED") from error
    prior_peak = await _leg_peak_nav(session, leg_id=leg.id, initial_nav=group.initial_nav)
    risk_violation = _hard_risk_violation(
        decision=decision,
        snapshot=snapshot,
        previous=previous,
        prior_peak=prior_peak,
        policy=risk_policy,
    )
    simulation = _simulation_state(previous)
    bars = tuple(
        SimulationBar(instrument_id=item.instrument_id, open=item.open, close=item.close)
        for item in evidence.execution_bars
    )
    fills: tuple[SimulationFill, ...] = ()
    if decision.reason == "WEEKLY_REBALANCE" and not risk_violation:
        execution = execute_target_weights(
            simulation,
            TargetWeightCommand(
                signal_session=snapshot.decision_session,
                target_session=evidence.session,
                targets=tuple(
                    TargetWeight(instrument_id=item.instrument_id, weight=item.weight)
                    for item in decision.targets
                ),
                bars=bars,
                fallback_marks=tuple(
                    SimulationMark(instrument_id=item.instrument_id, price=item.close)
                    for item in evidence.execution_bars
                ),
                cost_model=cost_model,
                execution_price="open",
            ),
        )
        simulation = execution.state
        fills = execution.fills
    valuation = mark_to_market(
        simulation,
        tuple(
            SimulationMark(instrument_id=item.instrument_id, price=item.close)
            for item in evidence.execution_bars
        ),
    )
    if valuation.missing_instruments:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_MARKS_INCOMPLETE")
    state = _domain_state(
        valuation.state,
        nav=valuation.nav,
        rank_states=decision.position_rank_states,
        hard_risk_violation=risk_violation,
    )
    state_hash = content_hash(state)
    transition_hash = content_hash(
        {
            "schema_version": "strategy_shadow_leg_transition_v1",
            "group_authority_hash": group.authority_hash,
            "leg_id": str(leg.id),
            "role": leg.role,
            "strategy_definition_id": str(leg.strategy_definition_id),
            "session_evidence_hash": evidence.content_hash,
            "ordinal": ordinal,
            "previous_state_hash": previous_hash,
            "decision": decision.model_dump(mode="json"),
            "fills": [
                {
                    "instrument_id": str(item.instrument_id),
                    "side": item.side,
                    "quantity": format(item.quantity, "f"),
                    "price": format(item.price, "f"),
                    "notional": format(item.notional, "f"),
                    "cost": format(item.cost, "f"),
                }
                for item in fills
            ],
            "hard_risk_violation": risk_violation,
            "state_hash": state_hash,
        }
    )
    return _LegTransition(
        leg=leg,
        state=state,
        state_hash=state_hash,
        transition_hash=transition_hash,
        fills=fills,
    )


async def _load_previous_leg_state(
    session: AsyncSession,
    *,
    group: StrategyShadowGroupRecord,
    leg: StrategyShadowLegRecord,
    ordinal: int,
) -> tuple[StrategyShadowStateV1, str]:
    if ordinal == 1:
        baseline = await load_balanced_paper_baseline(session, now=group.created_at)
        if (
            baseline.snapshot_id != group.baseline_snapshot_id
            or baseline.initial_state_hash != leg.initial_state_hash
        ):
            raise StrategyShadowServiceError("STRATEGY_SHADOW_INITIAL_STATE_INVALID")
        return baseline.normalized_state, baseline.initial_state_hash
    record = await session.scalar(
        select(StrategyShadowLegStateRecord).where(
            StrategyShadowLegStateRecord.leg_id == leg.id,
            StrategyShadowLegStateRecord.session_ordinal == ordinal - 1,
        )
    )
    if record is None:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_PREVIOUS_STATE_MISSING")
    try:
        state = StrategyShadowStateV1.model_validate(record.positions)
    except ValueError as error:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_PREVIOUS_STATE_INVALID") from error
    if (
        state.cash != record.cash
        or state.nav != record.nav
        or state.hard_risk_violation != record.hard_risk_violation
        or content_hash(state) != record.state_hash
    ):
        raise StrategyShadowServiceError("STRATEGY_SHADOW_PREVIOUS_STATE_INVALID")
    return state, record.state_hash


async def _leg_peak_nav(session: AsyncSession, *, leg_id: UUID, initial_nav: Decimal) -> Decimal:
    observed = await session.scalar(
        select(func.max(StrategyShadowLegStateRecord.nav)).where(
            StrategyShadowLegStateRecord.leg_id == leg_id
        )
    )
    return max(initial_nav, observed or initial_nav)


def _simulation_state(state: StrategyShadowStateV1) -> SimulationState:
    return SimulationState(
        cash=state.cash,
        instruments=tuple(
            SimulationInstrumentState(
                instrument_id=item.instrument_id,
                quantity=item.quantity,
                cost_basis=item.cost_basis,
                realized_pnl=item.realized_pnl,
                cumulative_cost=item.cumulative_fees,
                last_close=item.last_close,
                untradable=item.untradable,
            )
            for item in state.positions
        ),
        receivables=(),
    )


def _domain_state(
    state: SimulationState,
    *,
    nav: Decimal,
    rank_states: tuple[PositionRankState, ...],
    hard_risk_violation: bool,
) -> StrategyShadowStateV1:
    return StrategyShadowStateV1(
        cash=state.cash,
        nav=nav,
        positions=tuple(
            StrategyShadowPositionV1(
                instrument_id=item.instrument_id,
                quantity=item.quantity,
                cost_basis=item.cost_basis,
                realized_pnl=item.realized_pnl,
                cumulative_fees=item.cumulative_cost,
                last_close=item.last_close,
                untradable=item.untradable,
            )
            for item in state.instruments
        ),
        position_rank_states=rank_states,
        hard_risk_violation=hard_risk_violation,
    )


def _hard_risk_violation(
    *,
    decision: CompiledStrategyDecision,
    snapshot: StrategyDataSnapshot,
    previous: StrategyShadowStateV1,
    prior_peak: Decimal,
    policy: RiskPolicy,
) -> bool:
    if decision.reason != "WEEKLY_REBALANCE":
        return False
    if decision.cash_weight is None:
        return True
    if any(item.weight > policy.single_name_limit for item in decision.targets):
        return True
    if decision.cash_weight < policy.minimum_cash_ratio:
        return True
    if sum((item.weight for item in decision.targets), start=decision.cash_weight) != 1:
        return True
    instrument_map = {item.instrument_id: item for item in snapshot.instruments}
    sectors: dict[str, Decimal] = {}
    for target in decision.targets:
        instrument = instrument_map.get(target.instrument_id)
        if instrument is None:
            return True
        for exposure in instrument.sector_exposures:
            sectors[exposure.sector] = sectors.get(exposure.sector, Decimal(0)) + (
                target.weight * exposure.weight
            )
    if any(value > policy.sector_limit for value in sectors.values()):
        return True
    drawdown = Decimal(1) - previous.nav / prior_peak
    return drawdown >= policy.drawdown_lock_ratio and bool(decision.targets)


async def _load_exact_risk_policy(session: AsyncSession) -> RiskPolicy:
    rows = tuple(await session.scalars(select(RiskPolicyRecord)))
    expected = RiskPolicy.v1()
    matches = tuple(
        row
        for row in rows
        if row.active
        and row.version == expected.version
        and row.single_name_limit == expected.single_name_limit
        and row.sector_limit == expected.sector_limit
        and row.minimum_cash_ratio == expected.minimum_cash_ratio
        and row.drawdown_lock_ratio == expected.drawdown_lock_ratio
        and row.drawdown_recovery_ratio == expected.drawdown_recovery_ratio
        and row.minimum_buy_notional == expected.minimum_buy_notional
        and row.buy_reserve_markup == expected.buy_reserve_markup
        and row.commission == expected.commission
        and row.supported_market == expected.supported_market
        and row.supported_currency == expected.supported_currency
        and tuple(row.supported_asset_types) == expected.supported_asset_types
        and row.content_hash == expected.content_hash
    )
    if len(matches) != 1:
        raise StrategyShadowServiceError("STRATEGY_SHADOW_RISK_POLICY_INVALID")
    return expected


async def _persist_leg_transition(
    session: AsyncSession,
    *,
    authority: StrategyShadowSessionAuthorityRecord,
    transition: _LegTransition,
    ordinal: int,
    observed_at: datetime,
) -> None:
    state_record = StrategyShadowLegStateRecord(
        id=new_uuid7(),
        leg_id=transition.leg.id,
        session_authority_id=authority.id,
        session_ordinal=ordinal,
        cash=transition.state.cash,
        nav=transition.state.nav,
        positions=transition.state.model_dump(mode="json"),
        hard_risk_violation=transition.state.hard_risk_violation,
        state_hash=transition.state_hash,
        transition_hash=transition.transition_hash,
        created_at=observed_at,
    )
    session.add(state_record)
    await session.flush()
    for fill in transition.fills:
        order_hash = content_hash(
            {
                "schema_version": "strategy_shadow_order_v1",
                "transition_hash": transition.transition_hash,
                "instrument_id": str(fill.instrument_id),
                "side": fill.side.upper(),
                "quantity": format(fill.quantity, "f"),
                "status": "FILLED",
            }
        )
        order = StrategyShadowOrderRecord(
            id=new_uuid7(),
            leg_state_id=state_record.id,
            instrument_id=fill.instrument_id,
            side=fill.side.upper(),
            quantity=fill.quantity,
            status="FILLED",
            reason_code="STRATEGY_SHADOW_FULL_FILL",
            order_hash=order_hash,
            created_at=observed_at,
        )
        session.add(order)
        await session.flush()
        session.add(
            StrategyShadowFillRecord(
                id=new_uuid7(),
                order_id=order.id,
                quantity=fill.quantity,
                price=fill.price,
                fees=fill.cost,
                filled_at=observed_at,
                fill_hash=content_hash(
                    {
                        "schema_version": "strategy_shadow_fill_v1",
                        "order_hash": order_hash,
                        "quantity": format(fill.quantity, "f"),
                        "price": format(fill.price, "f"),
                        "fees": format(fill.cost, "f"),
                        "filled_at": observed_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    }
                ),
                created_at=observed_at,
            )
        )


async def _persist_advance_attempt(
    session: AsyncSession,
    *,
    group: StrategyShadowGroupRecord,
    requested_session: date,
    observed_at: datetime,
    status: Literal["applied", "failed", "duplicate"],
    reason_code: str,
    evidence_hash: str | None,
) -> None:
    attempt_number = (
        int(
            await session.scalar(
                select(func.max(StrategyShadowAdvanceAttemptRecord.attempt_number)).where(
                    StrategyShadowAdvanceAttemptRecord.group_id == group.id,
                    StrategyShadowAdvanceAttemptRecord.session == requested_session,
                )
            )
            or 0
        )
        + 1
    )
    session.add(
        StrategyShadowAdvanceAttemptRecord(
            id=new_uuid7(),
            group_id=group.id,
            session=requested_session,
            attempt_number=attempt_number,
            status=status,
            reason_code=reason_code,
            input_hash=content_hash(
                {
                    "schema_version": "strategy_shadow_advance_attempt_v1",
                    "group_authority_hash": group.authority_hash,
                    "requested_session": requested_session.isoformat(),
                    "observed_at": observed_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
                    "status": status,
                    "reason_code": reason_code,
                    "evidence_hash": evidence_hash,
                }
            ),
            observed_at=observed_at,
            created_at=observed_at,
        )
    )


async def _persist_checkpoint(
    session: AsyncSession,
    *,
    group: StrategyShadowGroupRecord,
    legs: tuple[StrategyShadowLegRecord, ...],
    ordinal: Literal[5, 21, 63],
    session_date: date,
    observed_at: datetime,
) -> None:
    metrics_by_leg = {
        leg.id: calculate_strategy_shadow_metrics(
            await _load_leg_points(session, leg=leg, through_ordinal=ordinal),
            checkpoint_ordinal=ordinal,
        )
        for leg in legs
    }
    authority_hash = content_hash(
        {
            "schema_version": "strategy_shadow_checkpoint_authority_v1",
            "group_authority_hash": group.authority_hash,
            "checkpoint_ordinal": ordinal,
            "session": session_date.isoformat(),
            "metric_hashes": sorted(item.metrics_hash for item in metrics_by_leg.values()),
        }
    )
    checkpoint = StrategyShadowCheckpointRecord(
        id=new_uuid7(),
        group_id=group.id,
        checkpoint_ordinal=ordinal,
        session=session_date,
        authority_hash=authority_hash,
        created_at=observed_at,
    )
    session.add(checkpoint)
    await session.flush()
    session.add_all(
        StrategyShadowCheckpointMetricRecord(
            id=new_uuid7(),
            checkpoint_id=checkpoint.id,
            leg_id=leg.id,
            metric_report=metrics_by_leg[leg.id].model_dump(mode="json"),
            metric_hash=metrics_by_leg[leg.id].metrics_hash,
            created_at=observed_at,
        )
        for leg in legs
    )


async def _load_leg_points(
    session: AsyncSession,
    *,
    leg: StrategyShadowLegRecord,
    through_ordinal: int,
) -> tuple[StrategyShadowSessionPointV1, ...]:
    states = tuple(
        await session.scalars(
            select(StrategyShadowLegStateRecord)
            .where(
                StrategyShadowLegStateRecord.leg_id == leg.id,
                StrategyShadowLegStateRecord.session_ordinal <= through_ordinal,
            )
            .order_by(StrategyShadowLegStateRecord.session_ordinal)
        )
    )
    points: list[StrategyShadowSessionPointV1] = []
    previous_nav = INITIAL_NAV
    for state in states:
        try:
            domain_state = StrategyShadowStateV1.model_validate(state.positions)
        except ValueError as error:
            raise StrategyShadowServiceError("STRATEGY_SHADOW_STATE_INVALID") from error
        if (
            domain_state.cash != state.cash
            or domain_state.nav != state.nav
            or domain_state.hard_risk_violation != state.hard_risk_violation
            or content_hash(domain_state) != state.state_hash
        ):
            raise StrategyShadowServiceError("STRATEGY_SHADOW_STATE_INVALID")
        authority = await session.get(
            StrategyShadowSessionAuthorityRecord, state.session_authority_id
        )
        if authority is None:
            raise StrategyShadowServiceError("STRATEGY_SHADOW_AUTHORITY_INCOMPLETE")
        orders = tuple(
            await session.scalars(
                select(StrategyShadowOrderRecord).where(
                    StrategyShadowOrderRecord.leg_state_id == state.id
                )
            )
        )
        fills = tuple(
            await session.scalars(
                select(StrategyShadowFillRecord)
                .join(StrategyShadowOrderRecord)
                .where(StrategyShadowOrderRecord.leg_state_id == state.id)
            )
        )
        turnover = (
            sum((item.quantity * item.price for item in fills), start=Decimal(0)) / previous_nav
        )
        points.append(
            StrategyShadowSessionPointV1(
                session=authority.session,
                ordinal=state.session_ordinal,
                nav=state.nav,
                turnover=turnover,
                fees=sum((item.fees for item in fills), start=Decimal(0)),
                filled_order_count=sum(1 for item in orders if item.status == "FILLED"),
                hard_risk_violation=state.hard_risk_violation,
                authority_hash=authority.authority_hash,
            )
        )
        previous_nav = state.nav
    return tuple(points)


async def _append_advance_audit_and_outbox(
    session: AsyncSession,
    *,
    group: StrategyShadowGroupRecord,
    requested_session: date,
    ordinal: int,
    status: str,
    reason_code: str,
    observed_at: datetime,
) -> None:
    payload = {
        "schema_version": "strategy_shadow_advance_v1",
        "group_id": str(group.id),
        "session": requested_session.isoformat(),
        "ordinal": ordinal,
        "status": status,
        "reason_code": reason_code,
        "live_execution_enabled": False,
    }
    await append_audit_event(
        session,
        actor_id="strategy-shadow-service",
        action="strategy.shadow.advanced",
        schema_version="1",
        payload={
            "schema_version": "strategy_shadow_advance_v1",
            "event_type": "SHADOW_ADVANCED",
            "status": status,
            "reason_code": reason_code,
            "live_execution_enabled": False,
        },
        occurred_at=observed_at,
    )
    session.add(
        OutboxEvent(
            event_type="strategy.shadow.advanced",
            aggregate_id=group.id,
            schema_version="1",
            payload=payload,
            published_at=None,
        )
    )
