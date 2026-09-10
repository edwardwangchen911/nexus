"""Pure deterministic generation of one reduce-only USD portfolio scenario."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime
from decimal import ROUND_CEILING, ROUND_HALF_UP, Decimal, localcontext
from typing import Literal, cast
from uuid import UUID

from nexus.diagnostics.models import InstrumentIdentity, PortfolioDiagnosticReport
from nexus.diagnostics.policy import RealAdvisoryPolicy
from nexus.market.scope import US_EQUITY_EXCHANGES
from nexus.optimizer.compare import TypedMetric
from nexus.optimizer.scenario_contracts import (
    AdjustmentScenarioConstraintsV1,
    PortfolioScenarioEvaluationV2,
    ScenarioCostBreakdownV1,
    ScenarioMetricSetV1,
    ScenarioObjectiveStatus,
    ScenarioPositionV1,
    canonical_scenario_hash,
)
from nexus.optimizer.scenario_cost_evidence import regulatory_fee_reference
from nexus.providers.broker.base import BrokerPosition, PortfolioSnapshot

_ZERO = Decimal("0")
_ONE = Decimal("1")
_ONE_BP = Decimal("0.0001")
_WEIGHT_QUANTUM = Decimal("0.000000000001")
_SUPPORTED_SCENARIO_ASSET_TYPES = frozenset(
    {"common_stock", "equity", "equity_etf", "exchange_traded_note"}
)


def evaluate_adjustment_scenario(
    *,
    snapshot: PortfolioSnapshot,
    report: PortfolioDiagnosticReport,
    identities: Mapping[UUID, InstrumentIdentity],
    constraints: AdjustmentScenarioConstraintsV1,
    policy: RealAdvisoryPolicy,
    diagnostic_report_id: UUID,
    diagnostic_input_hash: str,
    request_hash: str,
) -> PortfolioScenarioEvaluationV2:
    """Return one ephemeral scenario without persistence or external I/O."""
    with localcontext() as context:
        context.prec = 28
        context.rounding = ROUND_HALF_UP
        return _evaluate(
            snapshot=snapshot,
            report=report,
            identities=identities,
            constraints=constraints,
            policy=policy,
            diagnostic_report_id=diagnostic_report_id,
            diagnostic_input_hash=diagnostic_input_hash,
            request_hash=request_hash,
        )


def _evaluate(
    *,
    snapshot: PortfolioSnapshot,
    report: PortfolioDiagnosticReport,
    identities: Mapping[UUID, InstrumentIdentity],
    constraints: AdjustmentScenarioConstraintsV1,
    policy: RealAdvisoryPolicy,
    diagnostic_report_id: UUID,
    diagnostic_input_hash: str,
    request_hash: str,
) -> PortfolioScenarioEvaluationV2:
    _validate_binding(snapshot, report, policy, diagnostic_input_hash)
    assert snapshot.as_of is not None
    positions = _usd_positions(snapshot, identities)
    cash = _usd_cash(snapshot)
    values = {
        position.instrument_id: position.quantity * cast(Decimal, position.current_price)
        for position in positions
    }
    total = cash + sum(values.values(), _ZERO)
    if total <= _ZERO or not total.is_finite():
        raise ValueError("SCENARIO_INPUT_INVALID")
    current, current_cash = _weights(values, cash, total)
    minimum = _minimum_weights(positions, total, current)
    target = dict(current)
    target_cash = current_cash
    baseline_top1, _, _ = _security_metrics(current)
    _validate_concentration_authority(report, baseline_top1, policy, snapshot.as_of)
    effective_cash = max(policy.cash_minimum, constraints.minimum_cash)
    reason: str | None = None
    status: ScenarioObjectiveStatus

    while True:
        top1, _, _ = _security_metrics(target)
        if top1 <= policy.single_name_warning and target_cash >= effective_cash:
            status = "unchanged" if target == current else "met"
            break
        turnover = target_cash - current_cash
        remaining = constraints.max_turnover - turnover
        if remaining <= _ZERO:
            reason = "TURNOVER_BUDGET_EXHAUSTED"
            status = _stopped_status(baseline_top1, top1)
            break
        selection = _select_reduction(
            target,
            minimum,
            top1=top1,
            threshold=policy.single_name_warning,
        )
        if selection is None:
            reason = (
                "STRUCTURAL_CONCENTRATION_FLOOR"
                if _all_active_weights_equal(target)
                else "SELLABLE_QUANTITY_EXHAUSTED"
            )
            status = _stopped_status(baseline_top1, top1)
            break
        instrument_id, capacity = selection
        delta = min(_ONE_BP, remaining, capacity)
        if delta <= _ZERO:
            reason = "SELLABLE_QUANTITY_EXHAUSTED"
            status = _stopped_status(baseline_top1, top1)
            break
        target[instrument_id] -= delta
        target_cash += delta

    turnover = _one_way_turnover(current, target, current_cash, target_cash)
    ordered_positions = tuple(
        ScenarioPositionV1(
            instrument_id=instrument_id,
            current_weight=current[instrument_id],
            target_weight=target[instrument_id],
            action="reduce" if target[instrument_id] < current[instrument_id] else "maintain",
            reason_code=(
                "CONCENTRATION_REDUCTION"
                if target[instrument_id] < current[instrument_id]
                else "BASELINE_WEIGHT"
            ),
        )
        for instrument_id in sorted(current)
    )
    draft = PortfolioScenarioEvaluationV2(
        schema_version="portfolio_scenario_evaluation_v2",
        request_hash=request_hash,
        result_hash="0" * 64,
        portfolio_snapshot_id=snapshot.id,
        diagnostic_report_id=diagnostic_report_id,
        diagnostic_input_hash=diagnostic_input_hash,
        valuation_time=snapshot.as_of,
        valuation_scope="currency:USD",
        objective=constraints.objective,
        analysis_status="partial",
        objective_status=status,
        baseline=_metric_set(current, current_cash, _ZERO, baseline=True),
        candidate=_metric_set(target, target_cash, turnover, baseline=False),
        positions=ordered_positions,
        cash_current_weight=current_cash,
        cash_target_weight=target_cash,
        hard_constraint_reasons=() if reason is None else (reason,),
        unknown_metrics=("estimated_cost",),
        cost_breakdown=_unavailable_real_costs(snapshot.as_of),
        execution_locked=True,
    )
    return draft.model_copy(update={"result_hash": canonical_scenario_hash(draft)})


def _validate_binding(
    snapshot: PortfolioSnapshot,
    report: PortfolioDiagnosticReport,
    policy: RealAdvisoryPolicy,
    diagnostic_input_hash: str,
) -> None:
    if (
        snapshot.status != "READY"
        or snapshot.as_of is None
        or report.optimization_need == "BLOCKED"
    ):
        raise ValueError("SCENARIO_BASELINE_NOT_READY")
    if (
        report.portfolio_snapshot_id != snapshot.id
        or report.snapshot_as_of != snapshot.as_of
        or report.policy_version != policy.version
    ):
        raise ValueError("SCENARIO_DIAGNOSTIC_MISMATCH")
    if len(diagnostic_input_hash) != 64 or any(
        character not in "0123456789abcdef" for character in diagnostic_input_hash
    ):
        raise ValueError("SCENARIO_DIAGNOSTIC_MISMATCH")


def _usd_positions(
    snapshot: PortfolioSnapshot,
    identities: Mapping[UUID, InstrumentIdentity],
) -> tuple[BrokerPosition, ...]:
    selected: list[BrokerPosition] = []
    for position in snapshot.positions:
        if position.quantity == _ZERO or position.currency != "USD":
            continue
        identity = identities.get(position.instrument_id)
        if (
            identity is None
            or identity.restriction != "STANDARD"
            or identity.asset_type not in _SUPPORTED_SCENARIO_ASSET_TYPES
            or identity.currency != "USD"
            or identity.exchange not in US_EQUITY_EXCHANGES
        ):
            raise ValueError("SCENARIO_US_EQUITY_SCOPE_INVALID")
        if (
            position.quantity <= _ZERO
            or not position.quantity.is_finite()
            or position.current_price is None
            or position.current_price <= _ZERO
            or not position.current_price.is_finite()
            or position.available_quantity < _ZERO
            or position.available_quantity > position.quantity
            or position.frozen_quantity < _ZERO
            or position.available_quantity + position.frozen_quantity != position.quantity
        ):
            raise ValueError("SCENARIO_INPUT_INVALID")
        selected.append(position)
    if not selected:
        raise ValueError("SCENARIO_US_EQUITY_SCOPE_INVALID")
    if len({position.instrument_id for position in selected}) != len(selected):
        raise ValueError("SCENARIO_INPUT_INVALID")
    return tuple(sorted(selected, key=lambda item: item.instrument_id))


def _validate_concentration_authority(
    report: PortfolioDiagnosticReport,
    calculated_top1: Decimal,
    policy: RealAdvisoryPolicy,
    valuation_time: datetime,
) -> None:
    matches = tuple(
        item
        for item in report.items
        if item.diagnostic_code == "CONCENTRATION_TOP1" and item.policy_scope == "currency:USD"
    )
    if len(matches) != 1:
        raise ValueError("SCENARIO_DIAGNOSTIC_MISMATCH")
    item = matches[0]
    if calculated_top1 > policy.single_name_critical:
        expected = ("critical", "reduce_only", policy.single_name_critical)
    elif calculated_top1 > policy.single_name_warning:
        expected = ("warning", "research", policy.single_name_warning)
    else:
        expected = ("info", "monitor", policy.single_name_warning)
    if (
        item.metric_value is None
        or item.unknown_reason is not None
        or item.unit != "ratio"
        or item.reference_limit is None
        or (item.severity, item.actionability, Decimal(item.reference_limit)) != expected
        or Decimal(item.metric_value)
        != calculated_top1.quantize(_WEIGHT_QUANTUM, rounding=ROUND_HALF_UP)
        or not item.evidence_refs
        or any(ref.available_at > valuation_time for ref in item.evidence_refs)
    ):
        raise ValueError("SCENARIO_DIAGNOSTIC_MISMATCH")


def _usd_cash(snapshot: PortfolioSnapshot) -> Decimal:
    rows = tuple(item for item in snapshot.cash if item.currency == "USD")
    if any(not item.cash_balance.is_finite() or item.cash_balance < _ZERO for item in rows):
        raise ValueError("SCENARIO_INPUT_INVALID")
    return sum((item.cash_balance for item in rows), _ZERO)


def _weights(
    values: Mapping[UUID, Decimal], cash: Decimal, total: Decimal
) -> tuple[dict[UUID, Decimal], Decimal]:
    weights = {
        instrument_id: (value / total).quantize(_WEIGHT_QUANTUM)
        for instrument_id, value in values.items()
    }
    cash_weight = (cash / total).quantize(_WEIGHT_QUANTUM)
    residue = _ONE - cash_weight - sum(weights.values(), _ZERO)
    largest = min(values, key=lambda instrument_id: (-values[instrument_id], instrument_id))
    weights[largest] += residue
    return weights, cash_weight


def _minimum_weights(
    positions: tuple[BrokerPosition, ...],
    total: Decimal,
    current: Mapping[UUID, Decimal],
) -> dict[UUID, Decimal]:
    result: dict[UUID, Decimal] = {}
    for position in positions:
        assert position.current_price is not None
        locked_value = (position.quantity - position.available_quantity) * position.current_price
        minimum = (locked_value / total).quantize(_WEIGHT_QUANTUM, rounding=ROUND_CEILING)
        result[position.instrument_id] = min(current[position.instrument_id], minimum)
    return result


def _security_metrics(weights: Mapping[UUID, Decimal]) -> tuple[Decimal, Decimal, Decimal]:
    positive = sorted((weight for weight in weights.values() if weight > _ZERO), reverse=True)
    total = sum(positive, _ZERO)
    if total <= _ZERO:
        return _ONE, _ONE, _ONE
    normalized = tuple(weight / total for weight in positive)
    return (
        normalized[0],
        sum(normalized[:3], _ZERO),
        sum((weight * weight for weight in normalized), _ZERO),
    )


def _select_reduction(
    target: Mapping[UUID, Decimal],
    minimum: Mapping[UUID, Decimal],
    *,
    top1: Decimal,
    threshold: Decimal,
) -> tuple[UUID, Decimal] | None:
    sellable = {
        instrument_id: target[instrument_id] - minimum[instrument_id]
        for instrument_id in target
        if target[instrument_id] > minimum[instrument_id]
    }
    if not sellable:
        return None
    if top1 > threshold:
        active = {key: value for key, value in target.items() if value > _ZERO}
        if len(set(active.values())) == 1:
            return None
        maximum = max(active.values())
        maxima = tuple(key for key, value in active.items() if value == maximum)
        if any(key not in sellable for key in maxima):
            return None
        selected = min(maxima)
        if len(maxima) == 1 and len(active) > 1:
            next_weight = max(value for key, value in active.items() if key != selected)
            return selected, min(sellable[selected], maximum - next_weight)
        return selected, sellable[selected]

    candidates: list[tuple[Decimal, Decimal, UUID]] = []
    for instrument_id, capacity in sellable.items():
        delta = min(_ONE_BP, capacity)
        simulated = dict(target)
        simulated[instrument_id] -= delta
        simulated_top1, _, _ = _security_metrics(simulated)
        if simulated_top1 <= threshold:
            candidates.append((simulated_top1, -target[instrument_id], instrument_id))
    if not candidates:
        return None
    _, _, selected = min(candidates)
    return selected, sellable[selected]


def _all_active_weights_equal(weights: Mapping[UUID, Decimal]) -> bool:
    active = {value for value in weights.values() if value > _ZERO}
    return len(active) == 1


def _stopped_status(
    baseline_top1: Decimal, candidate_top1: Decimal
) -> Literal["improved_not_met", "infeasible"]:
    return "improved_not_met" if candidate_top1 < baseline_top1 else "infeasible"


def _one_way_turnover(
    current: Mapping[UUID, Decimal],
    target: Mapping[UUID, Decimal],
    current_cash: Decimal,
    target_cash: Decimal,
) -> Decimal:
    return (
        sum((abs(target[key] - current[key]) for key in current), _ZERO)
        + abs(target_cash - current_cash)
    ) / Decimal("2")


def _metric_set(
    weights: Mapping[UUID, Decimal],
    cash: Decimal,
    turnover: Decimal,
    *,
    baseline: bool,
) -> ScenarioMetricSetV1:
    top1, top3, hhi = _security_metrics(weights)
    return ScenarioMetricSetV1(
        security_top1=TypedMetric.known(top1),
        security_top3=TypedMetric.known(top3),
        security_hhi=TypedMetric.known(hhi),
        cash_weight=TypedMetric.known(cash),
        one_way_turnover=TypedMetric.known(turnover),
        estimated_cost=(
            TypedMetric.known(_ZERO)
            if baseline
            else TypedMetric.unknown("COST_EVIDENCE_INCOMPLETE")
        ),
    )


def _unavailable_real_costs(as_of: datetime) -> ScenarioCostBreakdownV1:
    """Describe current real-account cost gaps without substituting simulation assumptions."""
    regulatory_reference = regulatory_fee_reference(as_of)
    return ScenarioCostBreakdownV1(
        currency="USD",
        valuation_time=as_of,
        basis="portfolio_value_ratio",
        evidence_version=(None if regulatory_reference is None else regulatory_reference.version),
        evidence_hash=(
            None if regulatory_reference is None else regulatory_reference.evidence_hash
        ),
        available_at=(None if regulatory_reference is None else regulatory_reference.available_at),
        commission=TypedMetric.unknown("BROKER_COMMISSION_SCHEDULE_UNAVAILABLE"),
        spread=TypedMetric.unknown("BID_ASK_EVIDENCE_UNAVAILABLE"),
        slippage=TypedMetric.unknown("SLIPPAGE_MODEL_UNAVAILABLE"),
        regulatory_fees=TypedMetric.unknown(
            "REGULATORY_FEE_SCHEDULE_UNAVAILABLE"
            if regulatory_reference is None
            else "BROKER_REGULATORY_FEE_PASS_THROUGH_UNAVAILABLE"
        ),
        tax_impact=TypedMetric.unknown("TAX_DATA_UNAVAILABLE"),
        total_estimated_cost_ex_tax=TypedMetric.unknown("COST_EVIDENCE_INCOMPLETE"),
    )
