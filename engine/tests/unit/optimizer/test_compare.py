# ruff: noqa: E501
"""Unknown-safe deterministic proposal comparison."""

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.diagnostics.models import DiagnosticItem, PortfolioDiagnosticReport
from nexus.domain.evidence import EvidenceRef
from nexus.optimizer.compare import (
    ComparableProposal,
    ComparisonDimensions,
    TypedMetric,
    compare_proposals,
    evaluate_dimensions,
)
from nexus.optimizer.models import OptimizationProposal, ProposalPosition


def test_typed_metric_rejects_binary_float() -> None:
    with pytest.raises(TypeError, match="binary floats"):
        TypedMetric(status="known", value=0.1)


def _dimensions(
    *,
    violations: int,
    top1: str,
    drawdown: str,
    cost: str,
    unknown: str | None = None,
) -> ComparisonDimensions:
    metric = TypedMetric.known
    values = {
        "top1": metric(top1),
        "top3": metric("0.6"),
        "hhi": metric("0.4"),
        "sector_exposure": metric("0.3"),
        "currency_exposure": metric("0.8"),
        "volatility": metric("0.2"),
        "drawdown": metric(drawdown),
        "correlation": metric("0.5"),
        "liquidity": metric("10"),
        "turnover": metric("0.1"),
        "cost": metric(cost),
    }
    if unknown is not None:
        values[unknown] = TypedMetric.unknown("NOT_PROVEN")
    return ComparisonDimensions(hard_violations=violations, **values)


def test_comparison_requires_every_typed_dimension_and_no_risk_deterioration() -> None:
    """A concentration improvement cannot rank with unknown or worse drawdown authority."""
    result = compare_proposals(
        (
            ComparableProposal("hold_current", _dimensions(violations=1, top1="0.5", drawdown="0.2", cost="0")),
            ComparableProposal("minimum_change", _dimensions(violations=0, top1="0.4", drawdown="0.2", cost="0")),
            ComparableProposal("strategy_target", _dimensions(violations=0, top1="0.3", drawdown="0.3", cost="0.02")),
        )
    )
    assert result.recommendation == "minimum_change"
    assert result.ranked_variants == ("minimum_change", "hold_current")
    assert result.excluded_variants == ("strategy_target",)

    unknown = compare_proposals(
        (
            ComparableProposal("hold_current", _dimensions(violations=1, top1="0.5", drawdown="0.2", cost="0")),
            ComparableProposal("minimum_change", _dimensions(violations=0, top1="0.4", drawdown="0.2", cost="0.01", unknown="liquidity")),
            ComparableProposal("strategy_target", _dimensions(violations=0, top1="0.3", drawdown="0.2", cost="0.02", unknown="drawdown")),
        )
    )
    assert unknown.recommendation == "hold_current"
    assert unknown.excluded_variants == ("minimum_change", "strategy_target")
    assert unknown.tax_impact.status == "unknown"


def test_comparison_excludes_unknown_cost_from_ranking_and_can_recommend_hold() -> None:
    """Coercing unknown costs to zero would make an unsafe alternative win the ranking."""
    result = compare_proposals(
        (
            ComparableProposal(
                "hold_current",
                _dimensions(violations=1, top1="0.5", drawdown="0.2", cost="0", unknown="cost"),
            ),
            ComparableProposal(
                "minimum_change",
                _dimensions(violations=0, top1="0.4", drawdown="0.2", cost="0.01", unknown="cost"),
            ),
            ComparableProposal(
                "strategy_target",
                _dimensions(violations=0, top1="0.3", drawdown="0.2", cost="0.01"),
                requires_fx=True,
            ),
        )
    )
    assert result.recommendation == "hold_current"
    assert result.tax_impact.status == "unknown"
    assert result.ranked_variants == ()
    assert result.excluded_variants == (
        "hold_current",
        "minimum_change",
        "strategy_target",
    )


def test_comparison_excludes_partial_stale_and_incomplete_evidence() -> None:
    result = compare_proposals(
        (
            ComparableProposal("hold_current", _dimensions(violations=1, top1="0.5", drawdown="0.2", cost="0")),
            ComparableProposal(
                "minimum_change",
                _dimensions(violations=0, top1="0.4", drawdown="0.2", cost="0.01"),
                stale=True,
            ),
            ComparableProposal(
                "strategy_target",
                _dimensions(violations=0, top1="0.3", drawdown="0.2", cost="0.01"),
                evidence_complete=False,
            ),
        )
    )
    assert result.recommendation == "hold_current"
    assert result.ranked_variants == ("hold_current",)
    assert result.excluded_variants == ("minimum_change", "strategy_target")


def test_dimension_evaluator_uses_raw_portfolio_weights_except_security_hhi() -> None:
    first = UUID("0198d500-1000-7000-8000-000000000e01")
    second = UUID("0198d500-1000-7000-8000-000000000e02")
    baseline = UUID("0198d500-1000-7000-8000-000000000e03")
    proposal = OptimizationProposal(
        variant="minimum_change",
        baseline_portfolio_snapshot_id=baseline,
        positions=(
            ProposalPosition(
                instrument_id=first, current_weight="0.5", target_weight="0.4",
                action="reduce", reason_code="TEST",
            ),
            ProposalPosition(
                instrument_id=second, current_weight="0.3", target_weight="0.2",
                action="reduce", reason_code="TEST",
            ),
        ),
        cash_current_weight="0.2", cash_target_weight="0.4", analysis_only=True,
        strategy_evidence_complete=False,
    )
    now = datetime(2026, 8, 24, tzinfo=UTC)
    evidence = EvidenceRef(
        source="fixture", object_id="liquidity", available_at=now,
        location="fixture://liquidity", content_hash="a" * 64,
    )
    items = tuple(
        DiagnosticItem(
            diagnostic_code=f"LIQUIDITY_DAYS_TO_EXIT:{instrument_id}",
            metric_value=value, unit="trading_days", reference_limit="2",
            policy_scope="currency:USD", severity="info", actionability="monitor",
            unknown_reason=None, evidence_refs=(evidence,),
        )
        for instrument_id, value in ((first, "1"), (second, "5"))
    )
    report = PortfolioDiagnosticReport(
        portfolio_snapshot_id=baseline, snapshot_as_of=now,
        policy_version="real_advisory_policy_v1", optimization_need="OPTIMIZATION_CANDIDATE",
        items=items,
    )
    result = evaluate_dimensions(
        proposal, report, cost=Decimal("0.001"),
        instrument_currencies={first: "USD", second: "HKD"},
    )
    assert result.hard_violations == 1
    assert result.top1.value == Decimal("0.4")
    assert result.top3.value == Decimal("0.6")
    assert result.hhi.value == Decimal("5") / Decimal("9")
    assert result.currency_exposure.value == Decimal("0.4")
    assert result.liquidity.value == Decimal("5") / Decimal("12")
    assert result.volatility.status == result.drawdown.status == "unknown"


def test_cash_denominator_prevents_false_concentration_recommendation() -> None:
    instrument = UUID("0198d500-1000-7000-8000-000000000e11")
    baseline = UUID("0198d500-1000-7000-8000-000000000e12")
    now = datetime(2026, 8, 24, tzinfo=UTC)
    report = PortfolioDiagnosticReport(
        portfolio_snapshot_id=baseline, snapshot_as_of=now,
        policy_version="real_advisory_policy_v1", optimization_need="OPTIMIZATION_CANDIDATE",
        items=(),
    )

    def proposal(variant: str, target: str) -> OptimizationProposal:
        target_weight = Decimal(target)
        return OptimizationProposal(
            variant=variant,
            baseline_portfolio_snapshot_id=baseline,
            positions=(ProposalPosition(
                instrument_id=instrument, current_weight="0.10", target_weight=target_weight,
                action="maintain" if target_weight == Decimal("0.10") else
                "reduce" if target_weight < Decimal("0.10") else "increase",
                reason_code="CASH_DENOMINATOR",
            ),),
            cash_current_weight="0.90", cash_target_weight=Decimal("1") - target_weight,
            analysis_only=True, strategy_evidence_complete=variant == "strategy_target",
        )

    proposals = (
        proposal("hold_current", "0.10"),
        proposal("minimum_change", "0.08"),
        proposal("strategy_target", "0.20"),
    )
    dimensions = tuple(
        evaluate_dimensions(
            item, report, cost=Decimal("0"), instrument_currencies={instrument: "USD"},
        ).model_copy(update={
            "sector_exposure": TypedMetric.known("0"),
            "volatility": TypedMetric.known("0"),
            "drawdown": TypedMetric.known("0"),
            "correlation": TypedMetric.known("0"),
            "liquidity": TypedMetric.known("1"),
        })
        for item in proposals
    )
    assert dimensions[0].top1.value == dimensions[0].top3.value == Decimal("0.10")
    assert dimensions[0].hhi.value == Decimal("1")
    assert dimensions[0].currency_exposure.value == Decimal("0.10")
    assert dimensions[0].hard_violations == 0
    comparison = compare_proposals(tuple(
        ComparableProposal(item.variant, metric)
        for item, metric in zip(proposals, dimensions, strict=True)
    ))
    assert comparison.recommendation == "minimum_change"
    assert comparison.excluded_variants == ("strategy_target",)
