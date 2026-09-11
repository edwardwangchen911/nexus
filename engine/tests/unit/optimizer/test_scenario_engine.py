"""Deterministic USD-scope portfolio adjustment scenarios."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.diagnostics.models import DiagnosticItem, InstrumentIdentity, PortfolioDiagnosticReport
from nexus.diagnostics.policy import RealAdvisoryPolicy
from nexus.domain.decimal import decimal_to_string
from nexus.domain.evidence import EvidenceRef
from nexus.optimizer.scenario_contracts import (
    AdjustmentScenarioConstraintsV1,
    canonical_scenario_hash,
)
from nexus.optimizer.scenario_engine import evaluate_adjustment_scenario
from nexus.providers.broker.base import BrokerPosition, CashBalance, PortfolioSnapshot

_AS_OF = datetime(2026, 9, 10, 8, 0, tzinfo=UTC)
_SNAPSHOT_ID = UUID("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e720")
_PORTFOLIO_ID = UUID("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e721")
_REPORT_ID = UUID("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e722")
_DIAGNOSTIC_HASH = "c" * 64
_REQUEST_HASH = "d" * 64


def _instrument(index: int) -> UUID:
    return UUID(f"018f3f4e-8e8f-7f5b-9f26-a3cc4da1e7{index:02d}")


def _position(
    index: int,
    value: str,
    *,
    currency: str = "USD",
    available: str | None = None,
) -> BrokerPosition:
    quantity = Decimal(value)
    return BrokerPosition(
        instrument_id=_instrument(index),
        currency=currency,
        quantity=quantity,
        available_quantity=quantity if available is None else Decimal(available),
        frozen_quantity=Decimal("0") if available is None else quantity - Decimal(available),
        avg_cost=Decimal("1"),
        diluted_cost=Decimal("1"),
        current_price=Decimal("1"),
        provider_as_of=_AS_OF,
    )


def _snapshot(
    values: tuple[str, ...],
    *,
    usd_cash: str = "0",
    extra_positions: tuple[BrokerPosition, ...] = (),
    extra_cash: tuple[CashBalance, ...] = (),
) -> PortfolioSnapshot:
    return PortfolioSnapshot(
        id=_SNAPSHOT_ID,
        portfolio_id=_PORTFOLIO_ID,
        status="READY",
        cash=(
            CashBalance(
                currency="USD",
                cash_balance=Decimal(usd_cash),
                available_cash=Decimal(usd_cash),
                provider_as_of=_AS_OF,
            ),
            *extra_cash,
        ),
        positions=tuple(_position(index + 1, value) for index, value in enumerate(values))
        + extra_positions,
        open_orders=(),
        as_of=_AS_OF,
    )


def _report(**updates: object) -> PortfolioDiagnosticReport:
    payload: dict[str, object] = {
        "portfolio_snapshot_id": _SNAPSHOT_ID,
        "snapshot_as_of": _AS_OF,
        "policy_version": "real_advisory_policy_v1",
        "optimization_need": "RESEARCH_REQUIRED",
        "items": (),
    }
    payload.update(updates)
    return PortfolioDiagnosticReport.model_validate(payload)


def _qualified_report(snapshot: PortfolioSnapshot, **updates: object) -> PortfolioDiagnosticReport:
    values = tuple(
        position.quantity * position.current_price
        for position in snapshot.positions
        if position.currency == "USD" and position.current_price is not None
    )
    top1 = max(values) / sum(values, Decimal("0"))
    policy = RealAdvisoryPolicy.v1()
    if top1 > policy.single_name_critical:
        severity, actionability, limit = "critical", "reduce_only", policy.single_name_critical
    elif top1 > policy.single_name_warning:
        severity, actionability, limit = "warning", "research", policy.single_name_warning
    else:
        severity, actionability, limit = "info", "monitor", policy.single_name_warning
    item = DiagnosticItem(
        diagnostic_code="CONCENTRATION_TOP1",
        metric_value=decimal_to_string(top1.quantize(Decimal("0.000000000001"))),
        unit="ratio",
        reference_limit=decimal_to_string(limit),
        policy_scope="currency:USD",
        severity=severity,
        actionability=actionability,
        unknown_reason=None,
        evidence_refs=(
            EvidenceRef(
                source="fixture",
                object_id="usd-concentration",
                available_at=_AS_OF,
                location="fixture://usd-concentration",
                content_hash="e" * 64,
            ),
        ),
    )
    return _report(items=(item,), **updates)


def _identities(snapshot: PortfolioSnapshot) -> dict[UUID, InstrumentIdentity]:
    return {
        position.instrument_id: (
            InstrumentIdentity.standard(position.instrument_id)
            if position.currency == "USD"
            else InstrumentIdentity.fosun_read_only(position.instrument_id)
        )
        for position in snapshot.positions
    }


def _constraints(
    *, max_turnover: str = "0.20", minimum_cash: str = "0.10"
) -> AdjustmentScenarioConstraintsV1:
    return AdjustmentScenarioConstraintsV1(
        schema_version="adjustment_scenario_constraints_v1",
        objective="reduce_security_concentration",
        max_turnover=max_turnover,
        minimum_cash=minimum_cash,
        universe_policy="current_holdings_only",
        allow_new_positions=False,
    )


def _evaluate(
    snapshot: PortfolioSnapshot,
    *,
    constraints: AdjustmentScenarioConstraintsV1 | None = None,
    report: PortfolioDiagnosticReport | None = None,
    identities: dict[UUID, InstrumentIdentity] | None = None,
):
    return evaluate_adjustment_scenario(
        snapshot=snapshot,
        report=report or _qualified_report(snapshot),
        identities=identities or _identities(snapshot),
        constraints=constraints or _constraints(),
        policy=RealAdvisoryPolicy.v1(),
        diagnostic_report_id=_REPORT_ID,
        diagnostic_input_hash=_DIAGNOSTIC_HASH,
        request_hash=_REQUEST_HASH,
    )


def test_budget_sufficient_meets_top1_with_cash_leg_turnover() -> None:
    result = _evaluate(_snapshot(("30", "10", "10", "10", "10", "10", "10", "10")))

    assert result.objective_status == "met"
    assert result.diagnostic_report_id == _REPORT_ID
    assert result.candidate.security_top1.value is not None
    assert result.candidate.security_top1.value <= Decimal("0.15")
    assert result.candidate.one_way_turnover.value is not None
    assert result.candidate.one_way_turnover.value <= Decimal("0.20")
    assert result.candidate.one_way_turnover.value == (
        result.cash_target_weight - result.cash_current_weight
    )
    assert all(item.target_weight <= item.current_weight for item in result.positions)
    assert result.execution_locked is True


def test_unknown_real_costs_are_split_without_claiming_a_total() -> None:
    result = _evaluate(_snapshot(("30", "10", "10", "10", "10", "10", "10", "10")))

    assert result.result_hash == canonical_scenario_hash(result)
    assert result.cost_breakdown.currency == "USD"
    assert result.cost_breakdown.valuation_time == _AS_OF
    assert result.cost_breakdown.basis == "portfolio_value_ratio"
    assert (
        result.cost_breakdown.evidence_version
        == "us_regulatory_fee_reference_2026_verified_20260911_v1"
    )
    assert result.cost_breakdown.evidence_hash is not None
    assert len(result.cost_breakdown.evidence_hash) == 64
    assert result.cost_breakdown.available_at == datetime(2026, 2, 28, tzinfo=UTC)
    assert {
        "commission": result.cost_breakdown.commission.reason,
        "spread": result.cost_breakdown.spread.reason,
        "slippage": result.cost_breakdown.slippage.reason,
        "regulatory_fees": result.cost_breakdown.regulatory_fees.reason,
        "tax_impact": result.cost_breakdown.tax_impact.reason,
        "total_estimated_cost_ex_tax": result.cost_breakdown.total_estimated_cost_ex_tax.reason,
    } == {
        "commission": "BROKER_COMMISSION_SCHEDULE_UNAVAILABLE",
        "spread": "BID_ASK_EVIDENCE_UNAVAILABLE",
        "slippage": "SLIPPAGE_MODEL_UNAVAILABLE",
        "regulatory_fees": "BROKER_REGULATORY_FEE_PASS_THROUGH_UNAVAILABLE",
        "tax_impact": "TAX_DATA_UNAVAILABLE",
        "total_estimated_cost_ex_tax": "COST_EVIDENCE_INCOMPLETE",
    }
    assert all(
        metric.status == "unknown"
        for metric in (
            result.cost_breakdown.commission,
            result.cost_breakdown.spread,
            result.cost_breakdown.slippage,
            result.cost_breakdown.regulatory_fees,
            result.cost_breakdown.tax_impact,
            result.cost_breakdown.total_estimated_cost_ex_tax,
        )
    )


def test_budget_exhaustion_returns_strict_improvement_without_overspend() -> None:
    result = _evaluate(
        _snapshot(("30", "10", "10", "10", "10", "10", "10", "10")),
        constraints=_constraints(max_turnover="0.05"),
    )

    assert result.objective_status == "improved_not_met"
    assert result.candidate.one_way_turnover.value == Decimal("0.05")
    assert result.candidate.security_top1.value < result.baseline.security_top1.value
    assert result.hard_constraint_reasons == ("TURNOVER_BUDGET_EXHAUSTED",)


def test_already_satisfied_baseline_does_not_force_a_trade() -> None:
    result = _evaluate(
        _snapshot(("10",) * 8, usd_cash="20"),
        constraints=_constraints(minimum_cash="0.10"),
    )

    assert result.objective_status == "unchanged"
    assert result.cash_target_weight == result.cash_current_weight == Decimal("0.20")
    assert result.candidate.one_way_turnover.value == Decimal("0")
    assert all(item.action == "maintain" for item in result.positions)


def test_system_cash_minimum_cannot_be_relaxed_by_request() -> None:
    result = _evaluate(
        _snapshot(("11.875",) * 8, usd_cash="5"),
        constraints=_constraints(max_turnover="0.10", minimum_cash="0.01"),
    )

    assert result.objective_status == "met"
    assert result.cash_target_weight == Decimal("0.10")
    assert result.candidate.one_way_turnover.value == Decimal("0.05")


def test_single_security_cannot_be_sold_to_claim_lower_security_concentration() -> None:
    result = _evaluate(_snapshot(("100",)), constraints=_constraints(max_turnover="1"))

    assert result.objective_status == "infeasible"
    assert result.candidate.security_top1.value == Decimal("1")
    assert result.candidate.one_way_turnover.value == Decimal("0")
    assert result.hard_constraint_reasons == ("STRUCTURAL_CONCENTRATION_FLOOR",)


def test_sub_basis_point_equalization_stops_without_oscillating() -> None:
    result = _evaluate(
        _snapshot(("70.003", "29.997")),
        constraints=_constraints(max_turnover="0.50", minimum_cash="0"),
    )

    assert result.objective_status == "improved_not_met"
    assert result.candidate.security_top1.value == Decimal("0.5")
    assert result.candidate.one_way_turnover.value == Decimal("0.40006")
    assert result.hard_constraint_reasons == ("STRUCTURAL_CONCENTRATION_FLOOR",)


def test_diagnostic_baseline_uses_raw_values_before_weight_rounding() -> None:
    snapshot = _snapshot(
        ("23758", "31677", "39596", "47515", "55434"),
        usd_cash="12345",
    )

    result = _evaluate(snapshot)

    assert result.objective_status in {"met", "improved_not_met"}


def test_unavailable_top_holding_is_not_bypassed_by_selling_other_names() -> None:
    snapshot = _snapshot(("30", "10", "10", "10", "10", "10", "10", "10"))
    frozen_top = _position(1, "30", available="0")
    snapshot = snapshot.model_copy(update={"positions": (frozen_top, *snapshot.positions[1:])})

    result = _evaluate(snapshot, constraints=_constraints(max_turnover="0.20"))

    assert result.objective_status == "infeasible"
    assert result.candidate.security_top1.value == result.baseline.security_top1.value
    assert result.hard_constraint_reasons == ("SELLABLE_QUANTITY_EXHAUSTED",)


def test_missing_qualified_usd_concentration_metric_fails_closed() -> None:
    snapshot = _snapshot(("30", "10", "10", "10", "10", "10", "10", "10"))

    with pytest.raises(ValueError, match="SCENARIO_DIAGNOSTIC_MISMATCH"):
        _evaluate(snapshot, report=_report(items=()))


def test_non_usd_read_only_holding_and_cash_stay_outside_usd_scenario() -> None:
    hk_position = _position(90, "9000", currency="HKD")
    hk_cash = CashBalance(
        currency="HKD",
        cash_balance=Decimal("8000"),
        available_cash=Decimal("8000"),
        provider_as_of=_AS_OF,
    )
    snapshot = _snapshot(
        ("30", "10", "10", "10", "10", "10", "10", "10"),
        extra_positions=(hk_position,),
        extra_cash=(hk_cash,),
    )

    result = _evaluate(snapshot)

    assert result.valuation_scope == "currency:USD"
    assert hk_position.instrument_id not in {item.instrument_id for item in result.positions}
    assert result.cash_current_weight == Decimal("0")


def test_real_usd_listed_security_asset_types_are_supported() -> None:
    snapshot = _snapshot(("40", "30", "20", "10"))
    identities = _identities(snapshot)
    real_asset_types = (
        ("common_stock", "XNYS"),
        ("equity", "XNAS"),
        ("equity_etf", "XNAS"),
        ("exchange_traded_note", "ARCX"),
    )
    for position, (asset_type, exchange) in zip(snapshot.positions, real_asset_types, strict=True):
        identities[position.instrument_id] = identities[position.instrument_id].model_copy(
            update={"asset_type": asset_type, "exchange": exchange}
        )

    result = _evaluate(snapshot, identities=identities)

    assert len(result.positions) == 4
    assert result.execution_locked is True


def test_usd_holding_without_standard_us_equity_identity_fails_closed() -> None:
    snapshot = _snapshot(("30", "10", "10", "10", "10", "10", "10", "10"))
    identities = _identities(snapshot)
    identities.pop(snapshot.positions[0].instrument_id)

    with pytest.raises(ValueError, match="SCENARIO_US_EQUITY_SCOPE_INVALID"):
        _evaluate(snapshot, identities=identities)


def test_binding_mismatch_fails_before_candidate_generation() -> None:
    snapshot = _snapshot(("30", "10", "10", "10", "10", "10", "10", "10"))

    with pytest.raises(ValueError, match="SCENARIO_DIAGNOSTIC_MISMATCH"):
        _evaluate(snapshot, report=_report(portfolio_snapshot_id=UUID(int=1)))


def test_position_order_does_not_change_weights_or_result_hash() -> None:
    snapshot = _snapshot(("30", "10", "10", "10", "10", "10", "10", "10"))
    reversed_snapshot = snapshot.model_copy(
        update={"positions": tuple(reversed(snapshot.positions))}
    )

    first = _evaluate(snapshot)
    second = _evaluate(reversed_snapshot)

    assert first.positions == second.positions
    assert first.result_hash == second.result_hash
