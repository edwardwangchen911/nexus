"""CAS-serialized internal-paper champion registry, activation, and rollback."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.backtest.canonical import canonical_decimal
from nexus.domain.ids import new_uuid7
from nexus.evolution.handover import HandoverEvaluationV1, evaluate_handover
from nexus.evolution.repository import experiment_projection_is_valid
from nexus.evolution.shadow.authority import (
    StrategyShadowAuthorityError,
    load_balanced_paper_baseline,
)
from nexus.evolution.shadow.canonical import content_hash
from nexus.evolution.shadow.repository import append_strategy_experiment_event
from nexus.optimizer.cost_policy import load_cost_policy_v1
from nexus.persistence.models.backtest import StrategyDefinitionRecord
from nexus.persistence.models.evolution import (
    StrategyChampionComparisonRecord,
    StrategyExperimentFamilyRecord,
    StrategyExperimentRecord,
    StrategyExperimentStateProjectionRecord,
    StrategyPromotionDecisionRecord,
    StrategyPromotionPolicyRecord,
    StrategyRegistryEventRecord,
    StrategyRegistryHeadRecord,
    StrategyShadowFinalizationRecord,
    StrategyShadowGroupRecord,
)
from nexus.persistence.models.paper import (
    PaperOrderEventRecord,
    PaperOrderRecord,
    PaperPerformanceSnapshotRecord,
    RiskPolicyRecord,
)
from nexus.persistence.models.runtime import OutboxEvent
from nexus.quality.source_policy import load_source_policy_v1
from nexus.risk.policy import RiskPolicy
from nexus.strategy.dsl import (
    baseline_feature_registry,
    compile_strategy,
    evolution_feature_registry_v1,
)

RegistryTransitionStatus = Literal[
    "PENDING_BOUNDARY",
    "PENDING_ORDERS",
    "EFFECTIVE",
    "STALE",
    "PAUSED",
    "WAITING_HANDOVER",
    "FAILED",
    "NO_PENDING_TRANSITION",
    "VETOED",
]


class RegistryServiceError(ValueError):
    """Stable fail-closed registry service error."""

    def __init__(self, reason_code: str) -> None:
        self.reason_code = reason_code
        super().__init__(reason_code)


class RegistryTransitionResultV1(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    status: RegistryTransitionStatus
    reason_code: str
    active_definition_id: UUID
    active_generation: int
    registry_event_id: UUID
    event_sequence: int
    version: int
    pending_event_id: UUID | None
    pending_effective_session: date | None
    auto_promotion_paused: bool
    live_execution_enabled: Literal[False] = False


async def persist_handover_promotion_decision(
    session: AsyncSession,
    *,
    comparison_id: UUID,
    promotion_policy_id: UUID,
    evaluation: HandoverEvaluationV1,
    now: datetime,
) -> StrategyPromotionDecisionRecord:
    """Persist one immutable handover gate report; never create an order."""
    now = _utc(now)
    comparison = await session.get(StrategyChampionComparisonRecord, comparison_id)
    experiment = (
        None
        if comparison is None
        else await session.get(StrategyExperimentRecord, comparison.experiment_id)
    )
    paper_snapshot = await session.get(PaperPerformanceSnapshotRecord, evaluation.paper_snapshot_id)
    definition = await session.get(StrategyDefinitionRecord, evaluation.strategy_definition_id)
    risk_record = await session.get(RiskPolicyRecord, evaluation.authority.risk_policy_id)
    promotion_policy = await session.get(StrategyPromotionPolicyRecord, promotion_policy_id)
    expected_risk = RiskPolicy.v1()
    try:
        source_policy = await load_source_policy_v1(session, instrument_tick_size=None)
        cost_policy = await load_cost_policy_v1(session)
        baseline = await load_balanced_paper_baseline(
            session,
            now=evaluation.authority.decision_time,
        )
    except (StrategyShadowAuthorityError, ValueError):
        source_policy = None
        cost_policy = None
        baseline = None
    if (
        experiment is None
        or paper_snapshot is None
        or definition is None
        or source_policy is None
        or cost_policy is None
        or risk_record is None
        or promotion_policy is None
        or baseline is None
        or source_policy.record_id != evaluation.authority.source_policy_id
        or source_policy.available_at > evaluation.authority.decision_time
        or cost_policy.id != evaluation.authority.cost_policy_id
        or cost_policy.available_at > evaluation.authority.decision_time
        or _published_cost_rate(cost_policy.parameters) != evaluation.authority.estimated_cost_rate
        or baseline.snapshot_id != evaluation.paper_snapshot_id
        or evaluation.snapshot.paper_snapshot_id != evaluation.paper_snapshot_id
        or not _evaluation_matches_paper_baseline(evaluation, baseline.normalized_state)
        or experiment.candidate_strategy_definition_id != evaluation.strategy_definition_id
        or definition.published_at > evaluation.authority.decision_time
        or paper_snapshot.as_of > evaluation.authority.decision_time
        or evaluation.authority.decision_time > now
        or risk_record.version != expected_risk.version
        or risk_record.content_hash != expected_risk.content_hash
        or evaluation.risk_policy_hash != expected_risk.content_hash
        or evaluate_handover(
            evaluation.snapshot,
            evaluation.challenger,
            evaluation.authority,
            expected_risk,
        )
        != evaluation
    ):
        raise RegistryServiceError("HANDOVER_AUTHORITY_INVALID")
    historical_policy_id = await session.scalar(
        select(StrategyPromotionDecisionRecord.promotion_policy_id).where(
            StrategyPromotionDecisionRecord.comparison_id == comparison_id,
            StrategyPromotionDecisionRecord.stage == "historical",
        )
    )
    if historical_policy_id != promotion_policy_id:
        raise RegistryServiceError("HANDOVER_AUTHORITY_INVALID")
    existing = await session.scalar(
        select(StrategyPromotionDecisionRecord).where(
            StrategyPromotionDecisionRecord.comparison_id == comparison_id,
            StrategyPromotionDecisionRecord.stage == "handover",
        )
    )
    if existing is not None:
        if (
            existing.decision_hash != evaluation.evaluation_hash
            or existing.status != evaluation.status
        ):
            raise RegistryServiceError("HANDOVER_DECISION_CONFLICT")
        return existing
    record = StrategyPromotionDecisionRecord(
        id=new_uuid7(),
        comparison_id=comparison_id,
        promotion_policy_id=promotion_policy_id,
        stage="handover",
        status=evaluation.status,
        gate_report=evaluation.model_dump(mode="json"),
        reason_code=evaluation.reason_code,
        decision_hash=evaluation.evaluation_hash,
        created_at=now,
    )
    session.add(record)
    await session.flush()
    return record


def _published_cost_rate(parameters: dict[str, object]) -> Decimal | None:
    commission = parameters.get("commission_bps")
    slippage = parameters.get("slippage_bps")
    if not isinstance(commission, str) or not isinstance(slippage, str):
        return None
    try:
        return canonical_decimal((Decimal(commission) + Decimal(slippage)) / Decimal(10000))
    except ArithmeticError:
        return None


def _evaluation_matches_paper_baseline(
    evaluation: HandoverEvaluationV1,
    state: object,
) -> bool:
    """Bind claimed current weights to the server-reconstructed sealed snapshot."""
    from nexus.evolution.shadow.models import StrategyShadowStateV1

    if not isinstance(state, StrategyShadowStateV1):
        return False
    snapshot = evaluation.snapshot
    if snapshot.nav != state.nav or state.nav <= 0:
        return False
    if snapshot.current_cash_weight != canonical_decimal(state.cash / state.nav):
        return False
    claimed = {
        item.instrument_id: item.current_weight
        for item in snapshot.instruments
        if item.current_weight > 0
    }
    actual = {
        item.instrument_id: canonical_decimal(item.quantity * item.last_close / state.nav)
        for item in state.positions
        if item.quantity > 0 and item.last_close is not None
    }
    return claimed == actual


async def promote_internal_paper_champion(
    session: AsyncSession,
    *,
    experiment_id: UUID,
    expected_generation: int,
    expected_version: int,
    effective_session: date,
    idempotency_key: str,
    now: datetime,
) -> RegistryTransitionResultV1:
    """Create one pending automatic PROMOTE after every deterministic gate passes."""
    now = _utc(now)
    _idempotency_key(idempotency_key)
    head = await _lock_head(session)
    experiment = await session.scalar(
        select(StrategyExperimentRecord)
        .where(StrategyExperimentRecord.id == experiment_id)
        .with_for_update()
    )
    family = (
        None
        if experiment is None
        else await session.scalar(
            select(StrategyExperimentFamilyRecord)
            .where(StrategyExperimentFamilyRecord.id == experiment.family_id)
            .with_for_update()
        )
    )
    projection = (
        None
        if family is None
        else await session.scalar(
            select(StrategyExperimentStateProjectionRecord)
            .where(StrategyExperimentStateProjectionRecord.family_id == family.id)
            .with_for_update()
        )
    )
    if experiment is None or family is None or projection is None:
        raise RegistryServiceError("PROMOTION_AUTHORITY_INCOMPLETE")
    if not await experiment_projection_is_valid(session, projection):
        raise RegistryServiceError("STRATEGY_EXPERIMENT_CHAIN_INVALID")
    replay = await _registry_replay(
        session,
        head=head,
        idempotency_key=idempotency_key,
        expected_event_type="PROMOTE",
        expected_effective_session=effective_session,
        expected_to_definition_id=experiment.candidate_strategy_definition_id,
    )
    if replay is not None:
        return replay
    if head.pending_event_id is not None:
        pending = await session.get(StrategyRegistryEventRecord, head.pending_event_id)
        if (
            pending is not None
            and pending.event_type == "PROMOTE"
            and pending.to_definition_id == experiment.candidate_strategy_definition_id
            and pending.champion_generation == family.champion_generation + 1
        ):
            return _result(
                head,
                status="PENDING_BOUNDARY",
                reason_code="PROMOTION_PENDING_BOUNDARY",
                registry_event_id=pending.id,
            )
    if (
        head.active_definition_id == experiment.candidate_strategy_definition_id
        and head.active_generation == family.champion_generation + 1
    ):
        effective_event = await session.scalar(
            select(StrategyRegistryEventRecord)
            .where(
                StrategyRegistryEventRecord.event_type == "PROMOTE",
                StrategyRegistryEventRecord.to_definition_id
                == experiment.candidate_strategy_definition_id,
                StrategyRegistryEventRecord.champion_generation == head.active_generation,
            )
            .order_by(StrategyRegistryEventRecord.sequence.desc())
            .limit(1)
        )
        if effective_event is None:
            raise RegistryServiceError("STRATEGY_REGISTRY_CHAIN_INVALID")
        return _result(
            head,
            status="EFFECTIVE",
            reason_code="PROMOTION_ALREADY_EFFECTIVE",
            registry_event_id=effective_event.id,
        )
    if (
        head.active_generation != expected_generation
        or head.version != expected_version
        or head.active_generation != family.champion_generation
        or head.active_definition_id != family.parent_strategy_definition_id
        or head.pending_event_id is not None
    ):
        await _mark_family_stale(session, family=family, projection=projection, now=now)
        return _result(head, status="STALE", reason_code="STALE_CHAMPION_BASELINE")
    if head.auto_promotion_paused:
        return _result(head, status="PAUSED", reason_code="AUTO_PROMOTION_PAUSED")
    if projection.current_state != "PROMOTION_ELIGIBLE":
        raise RegistryServiceError("PROMOTION_STATE_INVALID")
    handover = await _load_promotion_evidence(
        session,
        experiment=experiment,
        expected_definition_id=experiment.candidate_strategy_definition_id,
    )
    if handover is None or handover.status != "PASS":
        return _result(
            head,
            status="WAITING_HANDOVER",
            reason_code="PROMOTION_WAITING_HANDOVER",
        )
    event = await _append_registry_event(
        session,
        head=head,
        event_type="PROMOTE",
        idempotency_key=idempotency_key,
        actor_kind="service",
        from_definition_id=head.active_definition_id,
        to_definition_id=experiment.candidate_strategy_definition_id,
        promotion_decision_id=handover.record_id,
        effective_session=effective_session,
        now=now,
        payload={
            "schema_version": "strategy_registry_promote_payload_v1",
            "experiment_id": str(experiment.id),
            "family_id": str(family.id),
            "handover_evaluation_hash": handover.evaluation_hash,
        },
    )
    _set_pending_head(head, event=event, effective_session=effective_session, now=now)
    await _append_registry_audit_and_outbox(
        session,
        action="strategy.registry.promotion_requested",
        event=event,
        head=head,
        status="PENDING_BOUNDARY",
        now=now,
    )
    await session.flush()
    return _result(
        head,
        status="PENDING_BOUNDARY",
        reason_code="PROMOTION_PENDING_BOUNDARY",
    )


async def pause_auto_promotion(
    session: AsyncSession,
    *,
    expected_generation: int,
    expected_version: int,
    idempotency_key: str,
    now: datetime,
) -> RegistryTransitionResultV1:
    return await _set_auto_promotion_control(
        session,
        paused=True,
        expected_generation=expected_generation,
        expected_version=expected_version,
        idempotency_key=idempotency_key,
        now=now,
    )


async def resume_auto_promotion(
    session: AsyncSession,
    *,
    expected_generation: int,
    expected_version: int,
    idempotency_key: str,
    now: datetime,
) -> RegistryTransitionResultV1:
    return await _set_auto_promotion_control(
        session,
        paused=False,
        expected_generation=expected_generation,
        expected_version=expected_version,
        idempotency_key=idempotency_key,
        now=now,
    )


async def veto_strategy_experiment(
    session: AsyncSession,
    *,
    experiment_id: UUID,
    expected_generation: int,
    expected_version: int,
    idempotency_key: str,
    reason_code: str,
    now: datetime,
) -> RegistryTransitionResultV1:
    """Append a user veto before promotion without changing the champion."""
    now = _utc(now)
    _idempotency_key(idempotency_key)
    if reason_code not in {"USER_VETO", "EVIDENCE_CONCERN", "OPERATIONAL_RISK"}:
        raise RegistryServiceError("VETO_REASON_INVALID")
    head = await _lock_head(session)
    experiment = await session.scalar(
        select(StrategyExperimentRecord)
        .where(StrategyExperimentRecord.id == experiment_id)
        .with_for_update()
    )
    family = (
        None
        if experiment is None
        else await session.scalar(
            select(StrategyExperimentFamilyRecord)
            .where(StrategyExperimentFamilyRecord.id == experiment.family_id)
            .with_for_update()
        )
    )
    projection = (
        None
        if family is None
        else await session.scalar(
            select(StrategyExperimentStateProjectionRecord)
            .where(StrategyExperimentStateProjectionRecord.family_id == family.id)
            .with_for_update()
        )
    )
    if experiment is None or family is None or projection is None:
        raise RegistryServiceError("EXPERIMENT_NOT_FOUND")
    if not await experiment_projection_is_valid(session, projection):
        raise RegistryServiceError("STRATEGY_EXPERIMENT_CHAIN_INVALID")
    existing = await session.scalar(
        select(StrategyRegistryEventRecord).where(
            StrategyRegistryEventRecord.idempotency_key == idempotency_key
        )
    )
    if existing is not None:
        if (
            existing.event_type != "VETO"
            or existing.payload.get("experiment_id") != str(experiment_id)
            or existing.payload.get("reason_code") != reason_code
            or projection.current_state != "VETOED"
        ):
            raise RegistryServiceError("REGISTRY_IDEMPOTENCY_CONFLICT")
        return _result(
            head,
            status="VETOED",
            reason_code="EXPERIMENT_VETOED",
            registry_event_id=existing.id,
        )
    _exact_cas(head, expected_generation=expected_generation, expected_version=expected_version)
    if family.champion_generation != head.active_generation:
        raise RegistryServiceError("STALE_CHAMPION_BASELINE")
    if head.pending_event_id is not None:
        raise RegistryServiceError("REGISTRY_TRANSITION_PENDING")
    if projection.current_state not in {
        "ADMITTED",
        "BACKTEST_RUNNING",
        "BACKTEST_PASSED",
        "SHADOW_RUNNING",
        "PROMOTION_ELIGIBLE",
    }:
        raise RegistryServiceError("VETO_STATE_INVALID")
    event = await _append_registry_event(
        session,
        head=head,
        event_type="VETO",
        idempotency_key=idempotency_key,
        actor_kind="user",
        from_definition_id=head.active_definition_id,
        to_definition_id=head.active_definition_id,
        promotion_decision_id=None,
        effective_session=None,
        now=now,
        payload={
            "schema_version": "strategy_registry_veto_payload_v1",
            "experiment_id": str(experiment.id),
            "family_id": str(family.id),
            "reason_code": reason_code,
        },
    )
    family_event = await append_strategy_experiment_event(
        session,
        family_id=family.id,
        event_type="VETOED",
        idempotency_key=f"strategy-veto-{event.id}",
        occurred_at=now,
        payload={
            "schema_version": "strategy_experiment_vetoed_payload_v1",
            "registry_event_id": str(event.id),
            "reason_code": reason_code,
        },
    )
    projection.current_state = "VETOED"
    projection.event_sequence = family_event.sequence
    projection.version += 1
    projection.updated_at = now
    head.last_event_id = event.id
    head.event_sequence = event.sequence
    head.version += 1
    head.updated_at = now
    await _append_registry_audit_and_outbox(
        session,
        action="strategy.experiment.vetoed",
        event=event,
        head=head,
        status="VETOED",
        now=now,
    )
    await session.flush()
    return _result(head, status="VETOED", reason_code="EXPERIMENT_VETOED")


async def request_registry_rollback(
    session: AsyncSession,
    *,
    expected_generation: int,
    expected_version: int,
    effective_session: date,
    idempotency_key: str,
    reason_code: str,
    now: datetime,
) -> RegistryTransitionResultV1:
    """Create a pending rollback to the previous compatible effective champion."""
    now = _utc(now)
    _idempotency_key(idempotency_key)
    if reason_code not in {
        "USER_REQUESTED",
        "PERFORMANCE_REGRESSION",
        "DATA_QUALITY_CONCERN",
        "OPERATIONAL_RISK",
    }:
        raise RegistryServiceError("ROLLBACK_REASON_INVALID")
    head = await _lock_head(session)
    replay = await _registry_replay(
        session,
        head=head,
        idempotency_key=idempotency_key,
        expected_event_type="ROLLBACK",
        expected_effective_session=effective_session,
        expected_reason_code=reason_code,
    )
    if replay is not None:
        return replay
    _exact_cas(head, expected_generation=expected_generation, expected_version=expected_version)
    if head.pending_event_id is not None:
        raise RegistryServiceError("REGISTRY_TRANSITION_PENDING")
    target = await _previous_compatible_definition(session, head=head)
    if target is None:
        await append_audit_event(
            session,
            actor_id="strategy-registry-service",
            action="strategy.registry.rollback_failed",
            schema_version="1",
            payload={
                "schema_version": "strategy_registry_change_v1",
                "event_type": "ROLLBACK",
                "status": "FAILED",
                "reason_code": "ROLLBACK_TARGET_NOT_FOUND",
                "live_execution_enabled": False,
            },
            occurred_at=now,
        )
        return _result(head, status="FAILED", reason_code="ROLLBACK_TARGET_NOT_FOUND")
    event = await _append_registry_event(
        session,
        head=head,
        event_type="ROLLBACK",
        idempotency_key=idempotency_key,
        actor_kind="user",
        from_definition_id=head.active_definition_id,
        to_definition_id=target.id,
        promotion_decision_id=None,
        effective_session=effective_session,
        now=now,
        payload={
            "schema_version": "strategy_registry_rollback_payload_v1",
            "reason_code": reason_code,
            "target_expression_hash": target.expression_hash,
        },
    )
    _set_pending_head(head, event=event, effective_session=effective_session, now=now)
    await _append_registry_audit_and_outbox(
        session,
        action="strategy.registry.rollback_requested",
        event=event,
        head=head,
        status="PENDING_BOUNDARY",
        now=now,
    )
    await session.flush()
    return _result(head, status="PENDING_BOUNDARY", reason_code="ROLLBACK_PENDING_BOUNDARY")


async def activate_pending_transition(
    session: AsyncSession,
    *,
    expected_generation: int,
    expected_version: int,
    decision_session: date,
    now: datetime,
) -> RegistryTransitionResultV1:
    """Activate a pending event only at its boundary and with no open paper orders."""
    now = _utc(now)
    head = await _lock_head(session)
    _exact_cas(head, expected_generation=expected_generation, expected_version=expected_version)
    if head.pending_event_id is None or head.pending_effective_session is None:
        return _result(
            head,
            status="NO_PENDING_TRANSITION",
            reason_code="REGISTRY_TRANSITION_NOT_PENDING",
        )
    event = await session.get(StrategyRegistryEventRecord, head.pending_event_id)
    if event is None or event.event_type not in {"PROMOTE", "ROLLBACK"}:
        raise RegistryServiceError("REGISTRY_PENDING_EVENT_INVALID")
    if decision_session < head.pending_effective_session:
        return _result(
            head,
            status="PENDING_BOUNDARY",
            reason_code="REGISTRY_TRANSITION_PENDING_BOUNDARY",
            registry_event_id=event.id,
        )
    if await _has_nonterminal_paper_orders(session):
        return _result(
            head,
            status="PENDING_ORDERS",
            reason_code=(
                "PROMOTION_PENDING_ORDERS"
                if event.event_type == "PROMOTE"
                else "ROLLBACK_PENDING_ORDERS"
            ),
            registry_event_id=event.id,
        )
    head.active_definition_id = event.to_definition_id
    head.active_generation = event.champion_generation
    head.pending_event_id = None
    head.pending_effective_session = None
    head.version += 1
    head.updated_at = now
    if event.event_type == "PROMOTE":
        await _mark_promoted_family(session, event=event, now=now)
    await _append_registry_audit_and_outbox(
        session,
        action=(
            "strategy.registry.promotion_effective"
            if event.event_type == "PROMOTE"
            else "strategy.registry.rollback_effective"
        ),
        event=event,
        head=head,
        status="EFFECTIVE",
        now=now,
    )
    await session.flush()
    return _result(
        head,
        status="EFFECTIVE",
        reason_code="REGISTRY_TRANSITION_EFFECTIVE",
        registry_event_id=event.id,
    )


class _PromotionEvidence(BaseModel):
    model_config = ConfigDict(frozen=True)

    record_id: UUID
    status: Literal["PASS", "FAIL", "UNKNOWN"]
    evaluation_hash: str


async def _load_promotion_evidence(
    session: AsyncSession,
    *,
    experiment: StrategyExperimentRecord,
    expected_definition_id: UUID,
) -> _PromotionEvidence | None:
    comparison = await session.scalar(
        select(StrategyChampionComparisonRecord).where(
            StrategyChampionComparisonRecord.experiment_id == experiment.id
        )
    )
    if comparison is None:
        return None
    decisions = tuple(
        await session.scalars(
            select(StrategyPromotionDecisionRecord).where(
                StrategyPromotionDecisionRecord.comparison_id == comparison.id
            )
        )
    )
    shadow = next((item for item in decisions if item.stage == "shadow"), None)
    handover = next((item for item in decisions if item.stage == "handover"), None)
    group = await session.scalar(
        select(StrategyShadowGroupRecord).where(
            StrategyShadowGroupRecord.experiment_id == experiment.id
        )
    )
    finalization = (
        None
        if group is None
        else await session.scalar(
            select(StrategyShadowFinalizationRecord).where(
                StrategyShadowFinalizationRecord.group_id == group.id
            )
        )
    )
    if (
        shadow is None
        or shadow.status != "PASS"
        or handover is None
        or finalization is None
        or finalization.status != "PASS"
    ):
        return None
    try:
        evaluation = HandoverEvaluationV1.model_validate(handover.gate_report)
    except ValueError as error:
        raise RegistryServiceError("HANDOVER_DECISION_INVALID") from error
    if (
        evaluation.strategy_definition_id != expected_definition_id
        or evaluation.status != handover.status
        or evaluation.evaluation_hash != handover.decision_hash
    ):
        raise RegistryServiceError("HANDOVER_DECISION_INVALID")
    return _PromotionEvidence(
        record_id=handover.id,
        status=evaluation.status,
        evaluation_hash=evaluation.evaluation_hash,
    )


async def _set_auto_promotion_control(
    session: AsyncSession,
    *,
    paused: bool,
    expected_generation: int,
    expected_version: int,
    idempotency_key: str,
    now: datetime,
) -> RegistryTransitionResultV1:
    now = _utc(now)
    _idempotency_key(idempotency_key)
    head = await _lock_head(session)
    event_type: Literal["PAUSE", "RESUME"] = "PAUSE" if paused else "RESUME"
    replay = await _registry_replay(
        session,
        head=head,
        idempotency_key=idempotency_key,
        expected_event_type=event_type,
        expected_effective_session=None,
        expected_to_definition_id=head.active_definition_id,
    )
    if replay is not None:
        return replay
    _exact_cas(head, expected_generation=expected_generation, expected_version=expected_version)
    if head.auto_promotion_paused == paused:
        raise RegistryServiceError("REGISTRY_CONTROL_STATE_CONFLICT")
    event = await _append_registry_event(
        session,
        head=head,
        event_type=event_type,
        idempotency_key=idempotency_key,
        actor_kind="user",
        from_definition_id=head.active_definition_id,
        to_definition_id=head.active_definition_id,
        promotion_decision_id=None,
        effective_session=None,
        now=now,
        payload={
            "schema_version": "strategy_registry_control_payload_v1",
            "paused": paused,
        },
    )
    head.last_event_id = event.id
    head.event_sequence = event.sequence
    head.auto_promotion_paused = paused
    head.version += 1
    head.updated_at = now
    await _append_registry_audit_and_outbox(
        session,
        action=("strategy.registry.paused" if paused else "strategy.registry.resumed"),
        event=event,
        head=head,
        status="PAUSED" if paused else "EFFECTIVE",
        now=now,
    )
    await session.flush()
    return _result(
        head,
        status="PAUSED" if paused else "EFFECTIVE",
        reason_code="AUTO_PROMOTION_PAUSED" if paused else "AUTO_PROMOTION_RESUMED",
    )


async def _lock_head(session: AsyncSession) -> StrategyRegistryHeadRecord:
    head = await session.scalar(
        select(StrategyRegistryHeadRecord)
        .where(StrategyRegistryHeadRecord.channel == "internal_paper")
        .with_for_update()
    )
    if head is None:
        raise RegistryServiceError("STRATEGY_REGISTRY_INCOMPLETE")
    events = tuple(
        await session.scalars(
            select(StrategyRegistryEventRecord)
            .where(StrategyRegistryEventRecord.channel == "internal_paper")
            .order_by(StrategyRegistryEventRecord.sequence)
        )
    )
    if not registry_chain_is_valid(head, events):
        raise RegistryServiceError("STRATEGY_REGISTRY_CHAIN_INVALID")
    return head


def _exact_cas(
    head: StrategyRegistryHeadRecord, *, expected_generation: int, expected_version: int
) -> None:
    if head.active_generation != expected_generation or head.version != expected_version:
        raise RegistryServiceError("STRATEGY_REGISTRY_CONFLICT")


def registry_event_content_hash(
    *,
    sequence: int,
    event_type: str,
    idempotency_key: str,
    actor_kind: str,
    occurred_at: datetime,
    from_definition_id: UUID,
    to_definition_id: UUID,
    promotion_decision_id: UUID | None,
    champion_generation: int,
    effective_session: date | None,
    payload: dict[str, object],
    previous_hash: str,
) -> str:
    """Hash every immutable registry-event business field in canonical form."""
    return content_hash(
        {
            "schema_version": "strategy_registry_event_v1",
            "channel": "internal_paper",
            "sequence": sequence,
            "event_type": event_type,
            "idempotency_key": idempotency_key,
            "actor_kind": actor_kind,
            "occurred_at": _utc(occurred_at).strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "from_definition_id": str(from_definition_id),
            "to_definition_id": str(to_definition_id),
            "promotion_decision_id": (
                None if promotion_decision_id is None else str(promotion_decision_id)
            ),
            "champion_generation": champion_generation,
            "effective_session": (
                None if effective_session is None else effective_session.isoformat()
            ),
            "payload": payload,
            "previous_hash": previous_hash,
        }
    )


def registry_chain_is_valid(
    head: StrategyRegistryHeadRecord,
    events: Sequence[StrategyRegistryEventRecord],
) -> bool:
    """Verify the complete immutable chain and its mutable head projection."""
    if not events or events[-1].id != head.last_event_id:
        return False
    prior_hash = "0" * 64
    for index, event in enumerate(events, start=1):
        if event.sequence != index or event.previous_hash != prior_hash:
            return False
        if event.event_type == "GENESIS":
            expected = content_hash(
                {
                    "channel": event.channel,
                    "schema_version": event.schema_version,
                    "sequence": event.sequence,
                    "event_type": event.event_type,
                    "idempotency_key": event.idempotency_key,
                    "actor_kind": event.actor_kind,
                    "occurred_at": _utc(event.occurred_at).isoformat().replace("+00:00", "Z"),
                    "from_definition_id": None,
                    "to_definition_id": str(event.to_definition_id),
                    "champion_generation": event.champion_generation,
                    "effective_session": None,
                    "payload": event.payload,
                    "previous_hash": event.previous_hash,
                }
            )
        else:
            if event.from_definition_id is None:
                return False
            expected = registry_event_content_hash(
                sequence=event.sequence,
                event_type=event.event_type,
                idempotency_key=event.idempotency_key,
                actor_kind=event.actor_kind,
                occurred_at=event.occurred_at,
                from_definition_id=event.from_definition_id,
                to_definition_id=event.to_definition_id,
                promotion_decision_id=event.promotion_decision_id,
                champion_generation=event.champion_generation,
                effective_session=event.effective_session,
                payload=event.payload,
                previous_hash=event.previous_hash,
            )
        if expected != event.event_hash:
            return False
        prior_hash = event.event_hash
    return head.event_sequence == events[-1].sequence and (head.pending_event_id is None) == (
        head.pending_effective_session is None
    )


async def _append_registry_event(
    session: AsyncSession,
    *,
    head: StrategyRegistryHeadRecord,
    event_type: Literal["PROMOTE", "ROLLBACK", "PAUSE", "RESUME", "VETO"],
    idempotency_key: str,
    actor_kind: Literal["service", "user"],
    from_definition_id: UUID,
    to_definition_id: UUID,
    promotion_decision_id: UUID | None,
    effective_session: date | None,
    now: datetime,
    payload: dict[str, object],
) -> StrategyRegistryEventRecord:
    prior = await session.get(StrategyRegistryEventRecord, head.last_event_id)
    if prior is None or prior.sequence != head.event_sequence:
        raise RegistryServiceError("STRATEGY_REGISTRY_CHAIN_INVALID")
    sequence = prior.sequence + 1
    generation = (
        prior.champion_generation + 1
        if event_type in {"PROMOTE", "ROLLBACK"}
        else prior.champion_generation
    )
    event = StrategyRegistryEventRecord(
        id=new_uuid7(),
        channel="internal_paper",
        schema_version="strategy_registry_event_v1",
        sequence=sequence,
        event_type=event_type,
        idempotency_key=idempotency_key,
        actor_kind=actor_kind,
        occurred_at=now,
        from_definition_id=from_definition_id,
        to_definition_id=to_definition_id,
        promotion_decision_id=promotion_decision_id,
        champion_generation=generation,
        effective_session=effective_session,
        payload=payload,
        previous_hash=prior.event_hash,
        event_hash=registry_event_content_hash(
            sequence=sequence,
            event_type=event_type,
            idempotency_key=idempotency_key,
            actor_kind=actor_kind,
            occurred_at=now,
            from_definition_id=from_definition_id,
            to_definition_id=to_definition_id,
            promotion_decision_id=promotion_decision_id,
            champion_generation=generation,
            effective_session=effective_session,
            payload=payload,
            previous_hash=prior.event_hash,
        ),
        created_at=now,
    )
    session.add(event)
    await session.flush()
    return event


def _set_pending_head(
    head: StrategyRegistryHeadRecord,
    *,
    event: StrategyRegistryEventRecord,
    effective_session: date,
    now: datetime,
) -> None:
    head.last_event_id = event.id
    head.event_sequence = event.sequence
    head.pending_event_id = event.id
    head.pending_effective_session = effective_session
    head.version += 1
    head.updated_at = now


async def _registry_replay(
    session: AsyncSession,
    *,
    head: StrategyRegistryHeadRecord,
    idempotency_key: str,
    expected_event_type: Literal["PROMOTE", "ROLLBACK", "PAUSE", "RESUME"],
    expected_effective_session: date | None,
    expected_to_definition_id: UUID | None = None,
    expected_reason_code: str | None = None,
) -> RegistryTransitionResultV1 | None:
    event = await session.scalar(
        select(StrategyRegistryEventRecord).where(
            StrategyRegistryEventRecord.idempotency_key == idempotency_key
        )
    )
    if event is None:
        return None
    if (
        event.event_type != expected_event_type
        or event.effective_session != expected_effective_session
        or (
            expected_to_definition_id is not None
            and event.to_definition_id != expected_to_definition_id
        )
        or (
            expected_reason_code is not None
            and event.payload.get("reason_code") != expected_reason_code
        )
    ):
        raise RegistryServiceError("REGISTRY_IDEMPOTENCY_CONFLICT")
    if head.pending_event_id == event.id:
        return _result(
            head,
            status="PENDING_BOUNDARY",
            reason_code="REGISTRY_TRANSITION_PENDING_BOUNDARY",
            registry_event_id=event.id,
        )
    if head.active_generation >= event.champion_generation:
        return _result(
            head,
            status="EFFECTIVE",
            reason_code="REGISTRY_EVENT_REPLAYED",
            registry_event_id=event.id,
        )
    raise RegistryServiceError("REGISTRY_IDEMPOTENCY_CONFLICT")


async def _previous_compatible_definition(
    session: AsyncSession,
    *,
    head: StrategyRegistryHeadRecord,
) -> StrategyDefinitionRecord | None:
    effective = await session.scalar(
        select(StrategyRegistryEventRecord)
        .where(
            StrategyRegistryEventRecord.event_type.in_(("PROMOTE", "ROLLBACK")),
            StrategyRegistryEventRecord.to_definition_id == head.active_definition_id,
            StrategyRegistryEventRecord.champion_generation == head.active_generation,
        )
        .order_by(StrategyRegistryEventRecord.sequence.desc())
        .limit(1)
    )
    if effective is None or effective.from_definition_id is None:
        return None
    definition = await session.get(StrategyDefinitionRecord, effective.from_definition_id)
    if definition is None:
        return None
    registry = (
        baseline_feature_registry()
        if definition.strategy_version == "baseline_v1"
        else evolution_feature_registry_v1()
    )
    try:
        compiled = compile_strategy(definition.document, registry)
    except ValueError:
        return None
    if compiled.expression_hash != definition.expression_hash:
        return None
    return definition


async def _has_nonterminal_paper_orders(session: AsyncSession) -> bool:
    latest = (
        select(
            PaperOrderEventRecord.order_id,
            func.max(PaperOrderEventRecord.event_seq).label("event_seq"),
        )
        .group_by(PaperOrderEventRecord.order_id)
        .subquery()
    )
    latest_status = await session.scalar(
        select(PaperOrderEventRecord.to_status)
        .join(
            latest,
            (latest.c.order_id == PaperOrderEventRecord.order_id)
            & (latest.c.event_seq == PaperOrderEventRecord.event_seq),
        )
        .join(PaperOrderRecord, PaperOrderRecord.id == PaperOrderEventRecord.order_id)
        .where(
            PaperOrderEventRecord.to_status.in_(
                (
                    "proposed",
                    "risk_approved",
                    "awaiting_approval",
                    "submitted",
                    "partially_filled",
                    "reconciling",
                )
            )
        )
        .limit(1)
    )
    return latest_status is not None


async def _mark_family_stale(
    session: AsyncSession,
    *,
    family: StrategyExperimentFamilyRecord,
    projection: StrategyExperimentStateProjectionRecord,
    now: datetime,
) -> None:
    if projection.current_state in {"PROMOTED", "STALE_CHAMPION_BASELINE"}:
        return
    event = await append_strategy_experiment_event(
        session,
        family_id=family.id,
        event_type="STALE_CHAMPION_BASELINE",
        idempotency_key=f"strategy-registry-stale-{family.id}",
        occurred_at=now,
        payload={
            "schema_version": "strategy_registry_stale_payload_v1",
            "champion_generation": family.champion_generation,
        },
    )
    projection.current_state = "STALE_CHAMPION_BASELINE"
    projection.event_sequence = event.sequence
    projection.version += 1
    projection.updated_at = now


async def _mark_promoted_family(
    session: AsyncSession,
    *,
    event: StrategyRegistryEventRecord,
    now: datetime,
) -> None:
    if event.promotion_decision_id is None:
        raise RegistryServiceError("PROMOTION_DECISION_MISSING")
    decision = await session.get(StrategyPromotionDecisionRecord, event.promotion_decision_id)
    comparison = (
        None
        if decision is None
        else await session.get(StrategyChampionComparisonRecord, decision.comparison_id)
    )
    experiment = (
        None
        if comparison is None
        else await session.get(StrategyExperimentRecord, comparison.experiment_id)
    )
    projection = (
        None
        if experiment is None
        else await session.scalar(
            select(StrategyExperimentStateProjectionRecord)
            .where(StrategyExperimentStateProjectionRecord.family_id == experiment.family_id)
            .with_for_update()
        )
    )
    if experiment is None or projection is None:
        raise RegistryServiceError("PROMOTION_FAMILY_MISSING")
    if not await experiment_projection_is_valid(session, projection):
        raise RegistryServiceError("STRATEGY_EXPERIMENT_CHAIN_INVALID")
    family_event = await append_strategy_experiment_event(
        session,
        family_id=experiment.family_id,
        event_type="PROMOTED",
        idempotency_key=f"strategy-registry-promoted-{event.id}",
        occurred_at=now,
        payload={
            "schema_version": "strategy_registry_promoted_payload_v1",
            "registry_event_id": str(event.id),
            "champion_generation": event.champion_generation,
        },
    )
    projection.current_state = "PROMOTED"
    projection.event_sequence = family_event.sequence
    projection.version += 1
    projection.updated_at = now


async def _append_registry_audit_and_outbox(
    session: AsyncSession,
    *,
    action: str,
    event: StrategyRegistryEventRecord,
    head: StrategyRegistryHeadRecord,
    status: str,
    now: datetime,
) -> None:
    payload = {
        "schema_version": "strategy_registry_change_v1",
        "registry_event_id": str(event.id),
        "event_type": event.event_type,
        "event_hash": event.event_hash,
        "status": status,
        "active_generation": head.active_generation,
        "pending_generation": event.champion_generation,
        "live_execution_enabled": False,
    }
    await append_audit_event(
        session,
        actor_id="strategy-registry-service",
        action=action,
        schema_version="1",
        payload={
            "schema_version": "strategy_registry_change_v1",
            "event_type": event.event_type,
            "event_hash": event.event_hash,
            "status": status,
            "live_execution_enabled": False,
        },
        occurred_at=now,
    )
    session.add(
        OutboxEvent(
            event_type=action,
            aggregate_id=event.id,
            schema_version="1",
            payload=payload,
            published_at=None,
        )
    )


def _result(
    head: StrategyRegistryHeadRecord,
    *,
    status: RegistryTransitionStatus,
    reason_code: str,
    registry_event_id: UUID | None = None,
) -> RegistryTransitionResultV1:
    return RegistryTransitionResultV1(
        status=status,
        reason_code=reason_code,
        active_definition_id=head.active_definition_id,
        active_generation=head.active_generation,
        registry_event_id=registry_event_id or head.last_event_id,
        event_sequence=head.event_sequence,
        version=head.version,
        pending_event_id=head.pending_event_id,
        pending_effective_session=head.pending_effective_session,
        auto_promotion_paused=head.auto_promotion_paused,
    )


def _idempotency_key(value: str) -> None:
    if not value or value.strip() != value or len(value) > 100:
        raise RegistryServiceError("REGISTRY_IDEMPOTENCY_KEY_INVALID")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise RegistryServiceError("REGISTRY_TIME_INVALID")
    return value.astimezone(UTC)
