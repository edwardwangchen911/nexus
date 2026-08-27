"""Deterministic shortlist, paired comparison, and historical promotion gates."""

from __future__ import annotations

import hashlib
from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import canonical_json
from nexus.audit.service import append_audit_event
from nexus.backtest.canonical import canonical_decimal
from nexus.backtest.cost_policy import load_backtest_cost_policy_v1
from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.evolution.models import _reject_binary_float
from nexus.evolution.policies import (
    CandidateBudgetPolicyV1,
    PromotionPolicyV1,
    load_promotion_policy_v1,
)
from nexus.persistence.models.backtest import BacktestRunRecord
from nexus.persistence.models.evolution import (
    StrategyChampionComparisonRecord,
    StrategyExperimentEventRecord,
    StrategyExperimentFamilyRecord,
    StrategyExperimentRecord,
    StrategyExperimentStateProjectionRecord,
    StrategyPromotionDecisionRecord,
    StrategyPromotionPolicyRecord,
    StrategyRegistryHeadRecord,
)
from nexus.persistence.models.optimization import CostPolicyRecord

PromotionGateKey = Literal[
    "minimum_oos_windows",
    "deflated_sharpe_confidence",
    "sharpe_or_return_improvement",
    "maximum_drawdown_degradation",
    "double_cost_excess_positive",
    "evidence_completeness",
]
GateStatus = Literal["PASS", "FAIL", "UNKNOWN"]
_GATE_ORDER: tuple[PromotionGateKey, ...] = (
    "minimum_oos_windows",
    "deflated_sharpe_confidence",
    "sharpe_or_return_improvement",
    "maximum_drawdown_degradation",
    "double_cost_excess_positive",
    "evidence_completeness",
)


class _FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    @model_validator(mode="before")
    @classmethod
    def _no_binary_float(cls, value: object) -> object:
        _reject_binary_float(value)
        return value


class ValidationCandidateResultV1(_FrozenModel):
    experiment_id: UUID
    expression_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    validation_sharpe: Decimal
    validation_max_drawdown: Decimal
    validation_cost: Decimal = Field(ge=0)
    authority_complete: bool

    @field_validator(
        "validation_sharpe",
        "validation_max_drawdown",
        "validation_cost",
        mode="after",
    )
    @classmethod
    def _canonical_decimal(cls, value: Decimal) -> Decimal:
        return canonical_decimal(value)


class ChampionComparisonRunV1(_FrozenModel):
    schema_version: Literal["champion_comparison_run_v1"]
    comparison_id: UUID
    experiment_id: UUID
    research_lineage_id: UUID
    champion_definition_id: UUID
    challenger_definition_id: UUID
    champion_backtest_run_id: UUID
    challenger_backtest_run_id: UUID
    authority_bundle_id: UUID
    cost_policy_id: UUID
    champion_generation: int = Field(ge=1)
    cumulative_oos_trial_count: int = Field(ge=0)
    valid_oos_window_count: int | None = Field(default=None, ge=0)
    challenger_deflated_sharpe_confidence: Decimal | None = None
    challenger_sharpe: Decimal | None = None
    champion_sharpe: Decimal | None = None
    challenger_annualized_return: Decimal | None = None
    champion_annualized_return: Decimal | None = None
    challenger_max_drawdown: Decimal | None = None
    champion_max_drawdown: Decimal | None = None
    challenger_double_cost_return: Decimal | None = None
    champion_double_cost_return: Decimal | None = None
    strategy_data_complete: bool | None
    benchmark_comparison_complete: bool | None
    history_requirement_met: bool | None
    authority_complete: bool | None
    cost_sensitivity_complete: bool | None
    parameter_sensitivity_complete: bool | None
    evidence_ids: tuple[UUID, ...] = Field(max_length=32)
    authority_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    protocol_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    cost_policy_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    comparison_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator(
        "challenger_deflated_sharpe_confidence",
        "challenger_sharpe",
        "champion_sharpe",
        "challenger_annualized_return",
        "champion_annualized_return",
        "challenger_max_drawdown",
        "champion_max_drawdown",
        "challenger_double_cost_return",
        "champion_double_cost_return",
        mode="after",
    )
    @classmethod
    def _canonical_optional_decimal(cls, value: Decimal | None) -> Decimal | None:
        return None if value is None else canonical_decimal(value)

    @model_validator(mode="after")
    def _evidence_is_unique(self) -> ChampionComparisonRunV1:
        if len(self.evidence_ids) != len(set(self.evidence_ids)):
            raise ValueError("comparison evidence identities must be unique")
        return self


class PromotionGateResultV1(_FrozenModel):
    gate: PromotionGateKey
    status: GateStatus
    observed: tuple[str, ...]
    thresholds: tuple[str, ...]
    evidence_ids: tuple[UUID, ...]
    reason_code: str = Field(pattern=r"^[A-Z][A-Z0-9_]{0,63}$")


class PromotionDecisionV1(_FrozenModel):
    schema_version: Literal["promotion_decision_v1"] = "promotion_decision_v1"
    comparison_id: UUID
    status: GateStatus
    gates: tuple[PromotionGateResultV1, ...]
    reason_code: str = Field(pattern=r"^[A-Z][A-Z0-9_]{0,63}$")
    decision_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def _exact_gate_order(self) -> PromotionDecisionV1:
        if tuple(item.gate for item in self.gates) != _GATE_ORDER:
            raise ValueError("historical promotion gates must use the fixed order")
        return self

    def gate(self, name: PromotionGateKey) -> PromotionGateResultV1:
        for item in self.gates:
            if item.gate == name:
                return item
        raise KeyError(name)


def select_oos_candidates(
    validation_results: Sequence[ValidationCandidateResultV1],
    policy: CandidateBudgetPolicyV1,
) -> tuple[ValidationCandidateResultV1, ...]:
    """Select complete candidates by the one registered validation ranking tuple."""
    complete = (item for item in validation_results if item.authority_complete)
    ranked = sorted(
        complete,
        key=lambda item: (
            -item.validation_sharpe,
            -item.validation_max_drawdown,
            item.validation_cost,
            item.expression_hash,
        ),
    )
    return tuple(ranked[: policy.max_oos_candidates])


def evaluate_historical_promotion(
    comparison: ChampionComparisonRunV1,
    policy: PromotionPolicyV1,
) -> PromotionDecisionV1:
    """Evaluate the immutable six-gate historical policy without caller overrides."""
    evidence = comparison.evidence_ids
    gates = (
        _minimum_oos_gate(comparison, policy, evidence),
        _deflated_sharpe_gate(comparison, policy, evidence),
        _improvement_gate(comparison, policy, evidence),
        _drawdown_gate(comparison, policy, evidence),
        _double_cost_gate(comparison, evidence),
        _completeness_gate(comparison, evidence),
    )
    statuses = {gate.status for gate in gates}
    status: GateStatus = (
        "UNKNOWN" if "UNKNOWN" in statuses else "FAIL" if "FAIL" in statuses else "PASS"
    )
    reason_code = {
        "PASS": "HISTORICAL_GATES_PASSED",
        "FAIL": "HISTORICAL_GATES_FAILED",
        "UNKNOWN": "HISTORICAL_EVIDENCE_INCOMPLETE",
    }[status]
    identity = {
        "schema_version": "promotion_decision_v1",
        "comparison_id": str(comparison.comparison_id),
        "policy_version": policy.version,
        "status": status,
        "gates": [gate.model_dump(mode="json") for gate in gates],
        "reason_code": reason_code,
    }
    return PromotionDecisionV1(
        comparison_id=comparison.comparison_id,
        status=status,
        gates=gates,
        reason_code=reason_code,
        decision_hash=hashlib.sha256(canonical_json(identity).encode()).hexdigest(),
    )


def _minimum_oos_gate(
    comparison: ChampionComparisonRunV1,
    policy: PromotionPolicyV1,
    evidence: tuple[UUID, ...],
) -> PromotionGateResultV1:
    value = comparison.valid_oos_window_count
    if value is None:
        return _unknown_gate("minimum_oos_windows", evidence, "valid_oos_window_count")
    return _gate(
        "minimum_oos_windows",
        value >= policy.minimum_oos_windows,
        (f"valid_oos_window_count={value}",),
        (f"minimum={policy.minimum_oos_windows}",),
        evidence,
        "INSUFFICIENT_OOS_WINDOWS",
    )


def _deflated_sharpe_gate(
    comparison: ChampionComparisonRunV1,
    policy: PromotionPolicyV1,
    evidence: tuple[UUID, ...],
) -> PromotionGateResultV1:
    value = comparison.challenger_deflated_sharpe_confidence
    if value is None or comparison.cumulative_oos_trial_count < 1:
        return _unknown_gate("deflated_sharpe_confidence", evidence, "deflated_sharpe_authority")
    return _gate(
        "deflated_sharpe_confidence",
        value >= policy.minimum_deflated_sharpe_confidence,
        (
            f"challenger={value}",
            f"cumulative_trials={comparison.cumulative_oos_trial_count}",
        ),
        (f"minimum={policy.minimum_deflated_sharpe_confidence}",),
        evidence,
        "DEFLATED_SHARPE_BELOW_THRESHOLD",
    )


def _improvement_gate(
    comparison: ChampionComparisonRunV1,
    policy: PromotionPolicyV1,
    evidence: tuple[UUID, ...],
) -> PromotionGateResultV1:
    values = (
        comparison.challenger_sharpe,
        comparison.champion_sharpe,
        comparison.challenger_annualized_return,
        comparison.champion_annualized_return,
    )
    if any(value is None for value in values):
        return _unknown_gate("sharpe_or_return_improvement", evidence, "improvement_metrics")
    challenger_sharpe, champion_sharpe, challenger_return, champion_return = values
    assert challenger_sharpe is not None
    assert champion_sharpe is not None
    assert challenger_return is not None
    assert champion_return is not None
    sharpe_delta = challenger_sharpe - champion_sharpe
    return_delta = challenger_return - champion_return
    return _gate(
        "sharpe_or_return_improvement",
        sharpe_delta >= policy.minimum_sharpe_improvement
        or return_delta >= policy.minimum_annual_return_improvement,
        (f"sharpe_delta={sharpe_delta}", f"annual_return_delta={return_delta}"),
        (
            f"minimum_sharpe_delta={policy.minimum_sharpe_improvement}",
            f"minimum_annual_return_delta={policy.minimum_annual_return_improvement}",
        ),
        evidence,
        "IMPROVEMENT_BELOW_THRESHOLD",
    )


def _drawdown_gate(
    comparison: ChampionComparisonRunV1,
    policy: PromotionPolicyV1,
    evidence: tuple[UUID, ...],
) -> PromotionGateResultV1:
    challenger = comparison.challenger_max_drawdown
    champion = comparison.champion_max_drawdown
    if challenger is None or champion is None:
        return _unknown_gate("maximum_drawdown_degradation", evidence, "drawdown_metrics")
    delta = challenger - champion
    minimum_delta = -policy.maximum_drawdown_degradation
    return _gate(
        "maximum_drawdown_degradation",
        delta >= minimum_delta,
        (f"challenger_minus_champion={delta}",),
        (f"minimum_delta={minimum_delta}",),
        evidence,
        "DRAWDOWN_DEGRADATION_EXCEEDED",
    )


def _double_cost_gate(
    comparison: ChampionComparisonRunV1,
    evidence: tuple[UUID, ...],
) -> PromotionGateResultV1:
    challenger = comparison.challenger_double_cost_return
    champion = comparison.champion_double_cost_return
    if challenger is None or champion is None:
        return _unknown_gate("double_cost_excess_positive", evidence, "double_cost_metrics")
    excess = challenger - champion
    return _gate(
        "double_cost_excess_positive",
        excess > 0,
        (f"challenger_minus_champion={excess}",),
        ("minimum_exclusive=0",),
        evidence,
        "DOUBLE_COST_EXCESS_NOT_POSITIVE",
    )


def _completeness_gate(
    comparison: ChampionComparisonRunV1,
    evidence: tuple[UUID, ...],
) -> PromotionGateResultV1:
    values = {
        "strategy_data": comparison.strategy_data_complete,
        "benchmarks": comparison.benchmark_comparison_complete,
        "history": comparison.history_requirement_met,
        "authority": comparison.authority_complete,
        "cost_sensitivity": comparison.cost_sensitivity_complete,
        "parameter_sensitivity": comparison.parameter_sensitivity_complete,
        "trial_count": comparison.cumulative_oos_trial_count >= 1,
        "evidence": bool(evidence),
    }
    observed = tuple(
        f"{name}={'UNKNOWN' if value is None else str(value).lower()}"
        for name, value in values.items()
    )
    if not all(value is True for value in values.values()):
        return PromotionGateResultV1(
            gate="evidence_completeness",
            status="UNKNOWN",
            observed=observed,
            thresholds=("all_complete=true",),
            evidence_ids=evidence,
            reason_code="EVIDENCE_INCOMPLETE",
        )
    return PromotionGateResultV1(
        gate="evidence_completeness",
        status="PASS",
        observed=observed,
        thresholds=("all_complete=true",),
        evidence_ids=evidence,
        reason_code="GATE_PASS",
    )


def _unknown_gate(
    gate: PromotionGateKey,
    evidence: tuple[UUID, ...],
    field: str,
) -> PromotionGateResultV1:
    return PromotionGateResultV1(
        gate=gate,
        status="UNKNOWN",
        observed=(f"{field}=UNKNOWN",),
        thresholds=(),
        evidence_ids=evidence,
        reason_code="EVIDENCE_INCOMPLETE",
    )


def _gate(
    gate: PromotionGateKey,
    passed: bool,
    observed: tuple[str, ...],
    thresholds: tuple[str, ...],
    evidence: tuple[UUID, ...],
    failure_reason: str,
) -> PromotionGateResultV1:
    return PromotionGateResultV1(
        gate=gate,
        status="PASS" if passed else "FAIL",
        observed=observed,
        thresholds=thresholds,
        evidence_ids=evidence,
        reason_code="GATE_PASS" if passed else failure_reason,
    )


class EvolutionEvaluationError(ValueError):
    """A paired historical authority or state transition failed closed."""

    def __init__(self, reason_code: str) -> None:
        super().__init__(reason_code)
        self.reason_code = reason_code


class HistoricalPromotionOutcomeV1(_FrozenModel):
    comparison_id: UUID
    decision_id: UUID | None
    status: Literal["PASS", "FAIL", "UNKNOWN", "STALE"]
    family_state: Literal["BACKTEST_PASSED", "BACKTEST_FAILED", "STALE_CHAMPION_BASELINE"]
    reason_code: str = Field(pattern=r"^[A-Z][A-Z0-9_]{0,63}$")


async def persist_champion_comparison(
    session: AsyncSession,
    *,
    experiment_id: UUID,
    champion_backtest_run_id: UUID,
    challenger_backtest_run_id: UUID,
    champion_generation: int,
    cumulative_oos_trial_count: int,
    now: datetime,
) -> StrategyChampionComparisonRecord:
    """Persist one same-authority champion/challenger comparison and running event."""
    now = require_utc(now)
    if champion_generation < 1 or cumulative_oos_trial_count < 1:
        raise EvolutionEvaluationError("PAIRED_AUTHORITY_INCOMPLETE")
    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended('strategy-comparison:' || :experiment_id, 2205))"
        ),
        {"experiment_id": str(experiment_id)},
    )
    existing = await session.scalar(
        select(StrategyChampionComparisonRecord).where(
            StrategyChampionComparisonRecord.experiment_id == experiment_id
        )
    )
    if existing is not None:
        if (
            existing.champion_backtest_run_id != champion_backtest_run_id
            or existing.challenger_backtest_run_id != challenger_backtest_run_id
            or existing.champion_generation != champion_generation
            or existing.cumulative_oos_trial_count != cumulative_oos_trial_count
        ):
            raise EvolutionEvaluationError("COMPARISON_IDEMPOTENCY_CONFLICT")
        _comparison_model(existing)
        return existing

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
    head = await session.scalar(
        select(StrategyRegistryHeadRecord)
        .where(StrategyRegistryHeadRecord.channel == "internal_paper")
        .with_for_update()
    )
    champion = await session.get(BacktestRunRecord, champion_backtest_run_id)
    challenger = await session.get(BacktestRunRecord, challenger_backtest_run_id)
    if (
        experiment is None
        or family is None
        or projection is None
        or head is None
        or champion is None
        or challenger is None
    ):
        raise EvolutionEvaluationError("PAIRED_AUTHORITY_INCOMPLETE")
    if projection.current_state not in {"ADMITTED", "BACKTEST_RUNNING"}:
        raise EvolutionEvaluationError("FAMILY_TERMINAL")
    if (
        family.champion_generation != champion_generation
        or head.active_generation != champion_generation
        or head.active_definition_id != family.parent_strategy_definition_id
        or champion.strategy_definition_id != family.parent_strategy_definition_id
        or challenger.strategy_definition_id != experiment.candidate_strategy_definition_id
    ):
        raise EvolutionEvaluationError("STALE_CHAMPION_BASELINE")
    _require_identical_backtest_authority(
        champion=champion,
        challenger=challenger,
        family=family,
        cumulative_oos_trial_count=cumulative_oos_trial_count,
    )
    try:
        cost_policy = await load_backtest_cost_policy_v1(session)
    except ValueError as error:
        raise EvolutionEvaluationError("PAIRED_AUTHORITY_INCOMPLETE") from error
    if cost_policy.id != champion.cost_policy_id:
        raise EvolutionEvaluationError("PAIRED_AUTHORITY_INCOMPLETE")
    cost_policy_hash = _cost_policy_hash(cost_policy)
    comparison_id = new_uuid7()
    report_without_hash = _paired_metric_report(
        comparison_id=comparison_id,
        experiment_id=experiment.id,
        research_lineage_id=family.research_lineage_id,
        champion=champion,
        challenger=challenger,
        champion_generation=champion_generation,
        cumulative_oos_trial_count=cumulative_oos_trial_count,
        cost_policy_hash=cost_policy_hash,
    )
    provisional_report = ChampionComparisonRunV1.model_validate(
        {**report_without_hash, "comparison_hash": "0" * 64}
    )
    comparison_hash = hashlib.sha256(
        canonical_json(
            provisional_report.model_dump(mode="json", exclude={"comparison_hash"})
        ).encode()
    ).hexdigest()
    report = provisional_report.model_copy(update={"comparison_hash": comparison_hash})
    record = StrategyChampionComparisonRecord(
        id=comparison_id,
        experiment_id=experiment.id,
        champion_backtest_run_id=champion.id,
        challenger_backtest_run_id=challenger.id,
        champion_generation=champion_generation,
        cumulative_oos_trial_count=cumulative_oos_trial_count,
        metric_report=report.model_dump(mode="json"),
        authority_hash=champion.authority_hash,
        protocol_hash=champion.protocol_hash,
        cost_policy_hash=cost_policy_hash,
        comparison_hash=comparison_hash,
        created_at=now,
    )
    session.add(record)
    event = await _append_family_event(
        session,
        family_id=family.id,
        event_type="BACKTEST_RUNNING",
        idempotency_key=f"comparison-running-{comparison_id}",
        occurred_at=now,
        payload={
            "schema_version": "strategy_backtest_running_payload_v1",
            "comparison_hash": comparison_hash,
            "champion_generation": champion_generation,
            "cumulative_oos_trial_count": cumulative_oos_trial_count,
        },
    )
    projection.current_state = "BACKTEST_RUNNING"
    projection.event_sequence = event.sequence
    projection.cumulative_oos_trial_count = max(
        projection.cumulative_oos_trial_count, cumulative_oos_trial_count
    )
    projection.version += 1
    projection.updated_at = now
    await append_audit_event(
        session,
        actor_id="service:strategy_rnd",
        action="strategy.backtest.completed",
        schema_version="strategy_evolution_event_v1",
        payload={
            "schema_version": "strategy_evolution_event_v1",
            "event_type": "BACKTEST_COMPLETED",
            "status": "COMPLETED",
            "authority_hash": champion.authority_hash,
            "protocol_hash": champion.protocol_hash,
        },
        occurred_at=now,
    )
    await session.flush()
    return record


async def persist_historical_promotion_decision(
    session: AsyncSession,
    *,
    comparison_id: UUID,
    promotion_policy_id: UUID,
    now: datetime,
) -> HistoricalPromotionOutcomeV1:
    """Persist the historical gate decision or a stale-generation terminal event."""
    now = require_utc(now)
    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended('strategy-promotion:' || :comparison_id, 2205))"
        ),
        {"comparison_id": str(comparison_id)},
    )
    comparison = await session.scalar(
        select(StrategyChampionComparisonRecord)
        .where(StrategyChampionComparisonRecord.id == comparison_id)
        .with_for_update()
    )
    if comparison is None:
        raise EvolutionEvaluationError("COMPARISON_AUTHORITY_INCOMPLETE")
    experiment = await session.get(StrategyExperimentRecord, comparison.experiment_id)
    family = (
        None
        if experiment is None
        else await session.get(StrategyExperimentFamilyRecord, experiment.family_id)
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
    head = await session.scalar(
        select(StrategyRegistryHeadRecord)
        .where(StrategyRegistryHeadRecord.channel == "internal_paper")
        .with_for_update()
    )
    if experiment is None or family is None or projection is None or head is None:
        raise EvolutionEvaluationError("COMPARISON_AUTHORITY_INCOMPLETE")
    report = _comparison_model(comparison)
    existing = await session.scalar(
        select(StrategyPromotionDecisionRecord).where(
            StrategyPromotionDecisionRecord.comparison_id == comparison.id,
            StrategyPromotionDecisionRecord.stage == "historical",
        )
    )
    if existing is not None:
        if existing.promotion_policy_id != promotion_policy_id:
            raise EvolutionEvaluationError("PROMOTION_DECISION_IDEMPOTENCY_CONFLICT")
        policy = await _load_promotion_policy(session, promotion_policy_id)
        persisted = _promotion_decision_model(existing, report, policy)
        return HistoricalPromotionOutcomeV1(
            comparison_id=comparison.id,
            decision_id=existing.id,
            status=persisted.status,
            family_state=("BACKTEST_PASSED" if persisted.status == "PASS" else "BACKTEST_FAILED"),
            reason_code=persisted.reason_code,
        )
    if (
        head.active_generation != comparison.champion_generation
        or head.active_definition_id != family.parent_strategy_definition_id
    ):
        if projection.current_state != "STALE_CHAMPION_BASELINE":
            event = await _append_family_event(
                session,
                family_id=family.id,
                event_type="STALE_CHAMPION_BASELINE",
                idempotency_key=f"comparison-stale-{comparison.id}",
                occurred_at=now,
                payload={
                    "schema_version": "strategy_comparison_stale_payload_v1",
                    "comparison_hash": comparison.comparison_hash,
                    "bound_generation": comparison.champion_generation,
                    "active_generation": head.active_generation,
                    "reason_code": "STALE_CHAMPION_BASELINE",
                },
            )
            projection.current_state = "STALE_CHAMPION_BASELINE"
            projection.event_sequence = event.sequence
            projection.version += 1
            projection.updated_at = now
            await append_audit_event(
                session,
                actor_id="service:strategy_rnd",
                action="strategy.promotion.gated",
                schema_version="strategy_evolution_event_v1",
                payload={
                    "schema_version": "strategy_evolution_event_v1",
                    "event_type": "HISTORICAL_PROMOTION_GATE",
                    "status": "STALE",
                    "reason_code": "STALE_CHAMPION_BASELINE",
                    "authority_hash": comparison.authority_hash,
                },
                occurred_at=now,
            )
            await session.flush()
        return HistoricalPromotionOutcomeV1(
            comparison_id=comparison.id,
            decision_id=None,
            status="STALE",
            family_state="STALE_CHAMPION_BASELINE",
            reason_code="STALE_CHAMPION_BASELINE",
        )
    if projection.current_state != "BACKTEST_RUNNING":
        raise EvolutionEvaluationError("FAMILY_STATE_INVALID")

    policy = await _load_promotion_policy(session, promotion_policy_id)
    decision = evaluate_historical_promotion(report, policy)
    record = StrategyPromotionDecisionRecord(
        id=new_uuid7(),
        comparison_id=comparison.id,
        promotion_policy_id=promotion_policy_id,
        stage="historical",
        status=decision.status,
        gate_report=decision.model_dump(mode="json"),
        reason_code=decision.reason_code,
        decision_hash=decision.decision_hash,
        created_at=now,
    )
    session.add(record)
    state: Literal["BACKTEST_PASSED", "BACKTEST_FAILED"] = (
        "BACKTEST_PASSED" if decision.status == "PASS" else "BACKTEST_FAILED"
    )
    event = await _append_family_event(
        session,
        family_id=family.id,
        event_type=state,
        idempotency_key=f"historical-decision-{comparison.id}",
        occurred_at=now,
        payload={
            "schema_version": "strategy_historical_decision_payload_v1",
            "comparison_hash": comparison.comparison_hash,
            "decision_hash": decision.decision_hash,
            "status": decision.status,
            "reason_code": decision.reason_code,
        },
    )
    projection.current_state = state
    projection.event_sequence = event.sequence
    projection.version += 1
    projection.updated_at = now
    await append_audit_event(
        session,
        actor_id="service:strategy_rnd",
        action="strategy.promotion.gated",
        schema_version="strategy_evolution_event_v1",
        payload={
            "schema_version": "strategy_evolution_event_v1",
            "event_type": "HISTORICAL_PROMOTION_GATE",
            "status": decision.status,
            "reason_code": decision.reason_code,
            "decision_hash": decision.decision_hash,
        },
        occurred_at=now,
    )
    await session.flush()
    return HistoricalPromotionOutcomeV1(
        comparison_id=comparison.id,
        decision_id=record.id,
        status=decision.status,
        family_state=state,
        reason_code=decision.reason_code,
    )


def _require_identical_backtest_authority(
    *,
    champion: BacktestRunRecord,
    challenger: BacktestRunRecord,
    family: StrategyExperimentFamilyRecord,
    cumulative_oos_trial_count: int,
) -> None:
    fields = (
        "authority_bundle_id",
        "data_snapshot_id",
        "source_policy_id",
        "cost_policy_id",
        "start_session",
        "end_session",
        "initial_nav",
        "protocol",
        "protocol_hash",
        "authority_hash",
        "data_hash",
        "code_hash",
        "random_seed",
        "oos_window_count",
    )
    if any(getattr(champion, field) != getattr(challenger, field) for field in fields):
        raise EvolutionEvaluationError("PAIRED_AUTHORITY_MISMATCH")
    protocol_attempts = champion.protocol.get("experiment_attempt_count")
    if (
        protocol_attempts != cumulative_oos_trial_count
        or family.authority_hash != champion.authority_hash
        or family.protocol_hash != champion.protocol_hash
        or family.code_hash != champion.code_hash
        or not champion.graph_sealed
        or not challenger.graph_sealed
        or champion.promotable
        or challenger.promotable
        or champion.historical_agent_overlay
        or challenger.historical_agent_overlay
    ):
        raise EvolutionEvaluationError("PAIRED_AUTHORITY_MISMATCH")


def _paired_metric_report(
    *,
    comparison_id: UUID,
    experiment_id: UUID,
    research_lineage_id: UUID,
    champion: BacktestRunRecord,
    challenger: BacktestRunRecord,
    champion_generation: int,
    cumulative_oos_trial_count: int,
    cost_policy_hash: str,
) -> dict[str, object]:
    champion_metrics = _mapping(champion.result_payload.get("metrics"))
    challenger_metrics = _mapping(challenger.result_payload.get("metrics"))
    champion_cost = _mapping(champion.result_payload.get("cost_sensitivity"))
    challenger_cost = _mapping(challenger.result_payload.get("cost_sensitivity"))
    champion_parameter = _mapping(champion.result_payload.get("parameter_sensitivity"))
    challenger_parameter = _mapping(challenger.result_payload.get("parameter_sensitivity"))
    report: dict[str, object] = {
        "schema_version": "champion_comparison_run_v1",
        "comparison_id": str(comparison_id),
        "experiment_id": str(experiment_id),
        "research_lineage_id": str(research_lineage_id),
        "champion_definition_id": str(champion.strategy_definition_id),
        "challenger_definition_id": str(challenger.strategy_definition_id),
        "champion_backtest_run_id": str(champion.id),
        "challenger_backtest_run_id": str(challenger.id),
        "authority_bundle_id": str(champion.authority_bundle_id),
        "cost_policy_id": str(champion.cost_policy_id),
        "champion_generation": champion_generation,
        "cumulative_oos_trial_count": cumulative_oos_trial_count,
        "valid_oos_window_count": challenger.oos_window_count,
        "challenger_deflated_sharpe_confidence": _decimal_or_none(
            challenger_metrics.get("deflated_sharpe_confidence")
        ),
        "challenger_sharpe": _decimal_or_none(challenger_metrics.get("sharpe")),
        "champion_sharpe": _decimal_or_none(champion_metrics.get("sharpe")),
        "challenger_annualized_return": _decimal_or_none(
            challenger_metrics.get("annualized_return")
        ),
        "champion_annualized_return": _decimal_or_none(champion_metrics.get("annualized_return")),
        "challenger_max_drawdown": _decimal_or_none(challenger_metrics.get("max_drawdown")),
        "champion_max_drawdown": _decimal_or_none(champion_metrics.get("max_drawdown")),
        "challenger_double_cost_return": _terminal_return(
            challenger_cost.get("double_cost_terminal_nav"), challenger.initial_nav
        ),
        "champion_double_cost_return": _terminal_return(
            champion_cost.get("double_cost_terminal_nav"), champion.initial_nav
        ),
        "strategy_data_complete": (
            champion.strategy_data_complete and challenger.strategy_data_complete
        ),
        "benchmark_comparison_complete": (
            champion.benchmark_comparison_complete and challenger.benchmark_comparison_complete
        ),
        "history_requirement_met": (
            champion.history_requirement_met and challenger.history_requirement_met
        ),
        "authority_complete": True,
        "cost_sensitivity_complete": _has_exact_keys(
            champion_cost,
            {"half_cost_terminal_nav", "base_terminal_nav", "double_cost_terminal_nav"},
        )
        and _has_exact_keys(
            challenger_cost,
            {"half_cost_terminal_nav", "base_terminal_nav", "double_cost_terminal_nav"},
        ),
        "parameter_sensitivity_complete": _has_exact_keys(
            champion_parameter,
            {"adv20_40m_terminal_nav", "base_terminal_nav", "adv20_60m_terminal_nav"},
        )
        and _has_exact_keys(
            challenger_parameter,
            {"adv20_40m_terminal_nav", "base_terminal_nav", "adv20_60m_terminal_nav"},
        ),
        "evidence_ids": [
            str(champion.id),
            str(challenger.id),
            str(champion.authority_bundle_id),
            str(champion.data_snapshot_id),
            str(champion.cost_policy_id),
        ],
        "authority_hash": champion.authority_hash,
        "protocol_hash": champion.protocol_hash,
        "cost_policy_hash": cost_policy_hash,
    }
    return report


def _comparison_model(
    record: StrategyChampionComparisonRecord,
) -> ChampionComparisonRunV1:
    try:
        report = ChampionComparisonRunV1.model_validate(record.metric_report)
    except ValueError as error:
        raise EvolutionEvaluationError("COMPARISON_AUTHORITY_INCOMPLETE") from error
    if (
        report.comparison_id != record.id
        or report.experiment_id != record.experiment_id
        or report.champion_backtest_run_id != record.champion_backtest_run_id
        or report.challenger_backtest_run_id != record.challenger_backtest_run_id
        or report.champion_generation != record.champion_generation
        or report.cumulative_oos_trial_count != record.cumulative_oos_trial_count
        or report.authority_hash != record.authority_hash
        or report.protocol_hash != record.protocol_hash
        or report.cost_policy_hash != record.cost_policy_hash
        or report.comparison_hash != record.comparison_hash
    ):
        raise EvolutionEvaluationError("COMPARISON_AUTHORITY_INCOMPLETE")
    computed_hash = hashlib.sha256(
        canonical_json(report.model_dump(mode="json", exclude={"comparison_hash"})).encode()
    ).hexdigest()
    if computed_hash != report.comparison_hash:
        raise EvolutionEvaluationError("COMPARISON_HASH_MISMATCH")
    return report


def _promotion_decision_model(
    record: StrategyPromotionDecisionRecord,
    comparison: ChampionComparisonRunV1,
    policy: PromotionPolicyV1,
) -> PromotionDecisionV1:
    try:
        persisted = PromotionDecisionV1.model_validate(record.gate_report)
    except ValueError as error:
        raise EvolutionEvaluationError("DECISION_AUTHORITY_INCOMPLETE") from error
    expected = evaluate_historical_promotion(comparison, policy)
    if (
        record.stage != "historical"
        or record.comparison_id != comparison.comparison_id
        or record.status != persisted.status
        or record.reason_code != persisted.reason_code
        or record.decision_hash != persisted.decision_hash
        or persisted != expected
    ):
        raise EvolutionEvaluationError("DECISION_AUTHORITY_INCOMPLETE")
    return persisted


async def _load_promotion_policy(
    session: AsyncSession, promotion_policy_id: UUID
) -> PromotionPolicyV1:
    policy_record = await session.get(StrategyPromotionPolicyRecord, promotion_policy_id)
    if policy_record is None:
        raise EvolutionEvaluationError("PROMOTION_POLICY_INCOMPLETE")
    try:
        return load_promotion_policy_v1(policy_record.document, policy_record.content_hash)
    except ValueError as error:
        raise EvolutionEvaluationError("PROMOTION_POLICY_INCOMPLETE") from error


def _cost_policy_hash(policy: CostPolicyRecord) -> str:
    identity = {
        "id": str(policy.id),
        "key": policy.key,
        "version": policy.version,
        "schema_version": policy.schema_version,
        "parameters": policy.parameters,
        "evidence_refs": policy.evidence_refs,
        "available_at": policy.available_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
    }
    return hashlib.sha256(canonical_json(identity).encode()).hexdigest()


async def _append_family_event(
    session: AsyncSession,
    *,
    family_id: UUID,
    event_type: str,
    idempotency_key: str,
    occurred_at: datetime,
    payload: dict[str, object],
) -> StrategyExperimentEventRecord:
    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended('strategy-experiment-event:' || :family_id, 0))"
        ),
        {"family_id": str(family_id)},
    )
    existing = await session.scalar(
        select(StrategyExperimentEventRecord).where(
            StrategyExperimentEventRecord.idempotency_key == idempotency_key
        )
    )
    if existing is not None:
        return existing
    prior = await session.scalar(
        select(StrategyExperimentEventRecord)
        .where(StrategyExperimentEventRecord.family_id == family_id)
        .order_by(StrategyExperimentEventRecord.sequence.desc())
        .limit(1)
    )
    sequence = 1 if prior is None else prior.sequence + 1
    previous_hash = "0" * 64 if prior is None else prior.event_hash
    identity = {
        "schema_version": "strategy_experiment_event_v1",
        "family_id": str(family_id),
        "sequence": sequence,
        "event_type": event_type,
        "idempotency_key": idempotency_key,
        "actor_kind": "service",
        "occurred_at": occurred_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "payload": payload,
        "previous_hash": previous_hash,
    }
    event = StrategyExperimentEventRecord(
        id=new_uuid7(),
        family_id=family_id,
        schema_version="strategy_experiment_event_v1",
        sequence=sequence,
        event_type=event_type,
        idempotency_key=idempotency_key,
        actor_kind="service",
        occurred_at=occurred_at,
        payload=payload,
        previous_hash=previous_hash,
        event_hash=hashlib.sha256(canonical_json(identity).encode()).hexdigest(),
        created_at=occurred_at,
    )
    session.add(event)
    await session.flush()
    return event


def _mapping(value: object) -> dict[str, object]:
    return dict(value) if isinstance(value, dict) else {}


def _decimal_or_none(value: object) -> Decimal | None:
    if isinstance(value, bool | float) or not isinstance(value, Decimal | str | int):
        return None
    try:
        return canonical_decimal(Decimal(value))
    except (ArithmeticError, ValueError, TypeError):
        return None


def _terminal_return(value: object, initial_nav: Decimal) -> Decimal | None:
    terminal = _decimal_or_none(value)
    return None if terminal is None or initial_nav <= 0 else terminal / initial_nav - 1


def _has_exact_keys(value: dict[str, object], expected: set[str]) -> bool:
    return set(value) == expected and all(
        _decimal_or_none(item) is not None for item in value.values()
    )
