"""Frozen contracts and pure spread-cost calculation for portfolio scenarios."""

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.optimizer.compare import TypedMetric
from nexus.optimizer.scenario_contracts import (
    PortfolioScenarioEvaluationV2,
    ScenarioCostBreakdownV1,
    ScenarioMetricSetV1,
    ScenarioPositionV1,
    canonical_scenario_hash,
)
from nexus.optimizer.scenario_spread_cost import (
    ScenarioSpreadCalendarAuthorityV1,
    ScenarioSpreadEvidenceInputV1,
    ScenarioSpreadEvidenceV1,
    SelectedSpreadQuoteV1,
    VerifiedSpreadQuoteV1,
    apply_scenario_spread_cost,
    canonical_scenario_spread_manifest_hash,
    finalize_scenario_spread_manifest,
    scenario_cost_basis_hash,
    scenario_spread_bundle_hash,
)
from nexus.providers.broker.base import BrokerPosition, CashBalance, PortfolioSnapshot
from pydantic import ValidationError

_AS_OF = datetime(2026, 9, 10, 15, 0, tzinfo=UTC)
_SNAPSHOT_ID = UUID("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e701")
_PORTFOLIO_ID = UUID("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e702")
_REPORT_ID = UUID("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e703")
_FIRST_ID = UUID("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e704")
_SECOND_ID = UUID("018f3f4e-8e8f-7f5b-9f26-a3cc4da1e705")
_POLICY_ID = UUID("7733d99e-1594-5f1e-87ea-e5cc23575159")


def _snapshot(*, first_price: str = "40") -> PortfolioSnapshot:
    return PortfolioSnapshot(
        id=_SNAPSHOT_ID,
        portfolio_id=_PORTFOLIO_ID,
        status="READY",
        cash=(
            CashBalance(
                currency="USD",
                cash_balance=Decimal("100"),
                available_cash=Decimal("100"),
                provider_as_of=_AS_OF,
            ),
        ),
        positions=(
            BrokerPosition(
                instrument_id=_FIRST_ID,
                currency="USD",
                quantity=Decimal("15"),
                available_quantity=Decimal("15"),
                frozen_quantity=Decimal("0"),
                avg_cost=Decimal("35"),
                diluted_cost=Decimal("35"),
                current_price=Decimal(first_price),
                provider_as_of=_AS_OF,
            ),
            BrokerPosition(
                instrument_id=_SECOND_ID,
                currency="USD",
                quantity=Decimal("10"),
                available_quantity=Decimal("10"),
                frozen_quantity=Decimal("0"),
                avg_cost=Decimal("25"),
                diluted_cost=Decimal("25"),
                current_price=Decimal("30"),
                provider_as_of=_AS_OF,
            ),
        ),
        open_orders=(),
        as_of=_AS_OF,
    )


def _metrics(*, cash: str, turnover: str, estimated_cost: TypedMetric) -> ScenarioMetricSetV1:
    return ScenarioMetricSetV1(
        security_top1=TypedMetric.known("0.60"),
        security_top3=TypedMetric.known("0.90"),
        security_hhi=TypedMetric.known("0.45"),
        cash_weight=TypedMetric.known(cash),
        one_way_turnover=TypedMetric.known(turnover),
        estimated_cost=estimated_cost,
    )


def _base_result(
    *, first_target: str = "0.50", second_target: str = "0.30"
) -> PortfolioScenarioEvaluationV2:
    cash_target = Decimal("1") - Decimal(first_target) - Decimal(second_target)
    positions = (
        ScenarioPositionV1(
            instrument_id=_FIRST_ID,
            current_weight=Decimal("0.60"),
            target_weight=Decimal(first_target),
            action="reduce" if Decimal(first_target) < Decimal("0.60") else "maintain",
            reason_code=(
                "CONCENTRATION_REDUCTION"
                if Decimal(first_target) < Decimal("0.60")
                else "BASELINE_WEIGHT"
            ),
        ),
        ScenarioPositionV1(
            instrument_id=_SECOND_ID,
            current_weight=Decimal("0.30"),
            target_weight=Decimal(second_target),
            action="reduce" if Decimal(second_target) < Decimal("0.30") else "maintain",
            reason_code=(
                "CONCENTRATION_REDUCTION"
                if Decimal(second_target) < Decimal("0.30")
                else "BASELINE_WEIGHT"
            ),
        ),
    )
    costs = ScenarioCostBreakdownV1(
        currency="USD",
        valuation_time=_AS_OF,
        basis="portfolio_value_ratio",
        commission=TypedMetric.unknown("BROKER_COMMISSION_SCHEDULE_UNAVAILABLE"),
        spread=TypedMetric.unknown("BID_ASK_EVIDENCE_UNAVAILABLE"),
        slippage=TypedMetric.unknown("SLIPPAGE_MODEL_UNAVAILABLE"),
        regulatory_fees=TypedMetric.unknown("REGULATORY_FEE_SCHEDULE_UNAVAILABLE"),
        tax_impact=TypedMetric.unknown("TAX_DATA_UNAVAILABLE"),
        total_estimated_cost_ex_tax=TypedMetric.unknown("COST_EVIDENCE_INCOMPLETE"),
    )
    draft = PortfolioScenarioEvaluationV2(
        schema_version="portfolio_scenario_evaluation_v2",
        request_hash="a" * 64,
        result_hash="0" * 64,
        portfolio_snapshot_id=_SNAPSHOT_ID,
        diagnostic_report_id=_REPORT_ID,
        diagnostic_input_hash="b" * 64,
        valuation_time=_AS_OF,
        valuation_scope="currency:USD",
        objective="reduce_security_concentration",
        analysis_status="partial",
        objective_status="improved_not_met",
        baseline=_metrics(cash="0.10", turnover="0", estimated_cost=TypedMetric.known("0")),
        candidate=_metrics(
            cash=str(cash_target),
            turnover=str(cash_target - Decimal("0.10")),
            estimated_cost=TypedMetric.unknown("COST_EVIDENCE_INCOMPLETE"),
        ),
        positions=positions,
        cash_current_weight=Decimal("0.10"),
        cash_target_weight=cash_target,
        hard_constraint_reasons=("TURNOVER_BUDGET_EXHAUSTED",),
        unknown_metrics=("estimated_cost",),
        cost_breakdown=costs,
        execution_locked=True,
    )
    return draft.model_copy(update={"result_hash": canonical_scenario_hash(draft)})


def _calendar() -> ScenarioSpreadCalendarAuthorityV1:
    return ScenarioSpreadCalendarAuthorityV1(
        library="exchange_calendars",
        library_version="4.13.2",
        calendar="XNYS",
        session_window_contract="xnys_regular_open_closed_v1",
        authority_hash="d" * 64,
        session_date=date(2026, 9, 10),
        session_open=datetime(2026, 9, 10, 13, 30, tzinfo=UTC),
        session_close=datetime(2026, 9, 10, 20, 0, tzinfo=UTC),
    )


def _selected(instrument_id: UUID, suffix: int) -> SelectedSpreadQuoteV1:
    return SelectedSpreadQuoteV1(
        instrument_id=instrument_id,
        latest_quote_id=UUID(f"019d0000-0000-7000-8000-{suffix:012d}"),
        content_hash=f"{suffix:x}" * 64,
        artifact_id=UUID(f"019d0000-0000-7000-8001-{suffix:012d}"),
        artifact_hash=f"{suffix + 1:x}" * 64,
    )


def _input(
    snapshot: PortfolioSnapshot,
    base_result: PortfolioScenarioEvaluationV2,
    *,
    selected_quotes: tuple[SelectedSpreadQuoteV1, ...],
) -> ScenarioSpreadEvidenceInputV1:
    return ScenarioSpreadEvidenceInputV1(
        manifest_schema="scenario_spread_evidence_manifest_v1",
        evidence_selected_at=datetime(2026, 9, 10, 14, 59, 59, tzinfo=UTC),
        policy_status="available",
        source_policy_id=_POLICY_ID,
        source_policy_version="scenario_spread_cost_policy_v1",
        source_policy_payload_hash="c" * 64,
        calendar_authority=_calendar(),
        calculation_version="scenario_spread_cost_v1",
        no_cost_candidate_hash=canonical_scenario_hash(base_result),
        scenario_cost_basis_hash=scenario_cost_basis_hash(snapshot, base_result),
        regulatory_reference_hash=None,
        selected_quotes=selected_quotes,
    )


def _evidence(
    snapshot: PortfolioSnapshot,
    base_result: PortfolioScenarioEvaluationV2,
    *,
    quotes: tuple[VerifiedSpreadQuoteV1, ...],
    selected_quotes: tuple[SelectedSpreadQuoteV1, ...],
) -> ScenarioSpreadEvidenceV1:
    evidence_input = _input(snapshot, base_result, selected_quotes=selected_quotes)
    return ScenarioSpreadEvidenceV1(
        input=evidence_input,
        quotes=quotes,
        bundle_hash=scenario_spread_bundle_hash(evidence_input),
        available_at=datetime(2026, 9, 10, 14, 59, 59, tzinfo=UTC),
    )


def test_apply_spread_uses_snapshot_mark_to_convert_sold_weight_to_quantity() -> None:
    snapshot = _snapshot()
    base_result = _base_result()
    evidence = _evidence(
        snapshot,
        base_result,
        quotes=(VerifiedSpreadQuoteV1(instrument_id=_FIRST_ID, bid="39.90", ask="40.10"),),
        selected_quotes=(_selected(_FIRST_ID, 1),),
    )

    result = apply_scenario_spread_cost(
        snapshot=snapshot, base_result=base_result, evidence=evidence
    )

    assert result.cost_breakdown.spread == TypedMetric.known("0.00025")
    assert result.cost_breakdown.evidence_version == "scenario_cost_evidence_bundle_v1"
    assert result.cost_breakdown.evidence_hash == evidence.bundle_hash
    assert result.candidate.estimated_cost == TypedMetric.unknown("COST_EVIDENCE_INCOMPLETE")
    assert result.cost_breakdown.total_estimated_cost_ex_tax == TypedMetric.unknown(
        "COST_EVIDENCE_INCOMPLETE"
    )
    assert result.execution_locked is True
    assert result.result_hash == canonical_scenario_hash(result)


def test_partial_quote_coverage_keeps_the_entire_spread_unknown() -> None:
    snapshot = _snapshot()
    base_result = _base_result(first_target="0.55", second_target="0.25")
    evidence = _evidence(
        snapshot,
        base_result,
        quotes=(VerifiedSpreadQuoteV1(instrument_id=_FIRST_ID, bid="39.90", ask="40.10"),),
        selected_quotes=(_selected(_FIRST_ID, 1),),
    )

    result = apply_scenario_spread_cost(
        snapshot=snapshot, base_result=base_result, evidence=evidence
    )

    assert result.cost_breakdown.spread == TypedMetric.unknown("BID_ASK_EVIDENCE_UNAVAILABLE")
    assert result.cost_breakdown.evidence_version is None
    assert result.result_hash == base_result.result_hash


def test_no_reduction_does_not_claim_zero_spread() -> None:
    snapshot = _snapshot()
    base_result = _base_result(first_target="0.60", second_target="0.30")
    evidence = _evidence(snapshot, base_result, quotes=(), selected_quotes=())

    result = apply_scenario_spread_cost(
        snapshot=snapshot, base_result=base_result, evidence=evidence
    )

    assert result.cost_breakdown.spread == TypedMetric.unknown("BID_ASK_EVIDENCE_UNAVAILABLE")
    assert result.cost_breakdown.evidence_version is None
    assert result.result_hash == base_result.result_hash


def test_cost_basis_hash_changes_when_snapshot_current_price_changes() -> None:
    base_result = _base_result()

    first = scenario_cost_basis_hash(_snapshot(first_price="40"), base_result)
    second = scenario_cost_basis_hash(_snapshot(first_price="41"), base_result)

    assert first != second


def test_apply_rejects_a_snapshot_outside_the_frozen_cost_basis() -> None:
    snapshot = _snapshot()
    base_result = _base_result()
    evidence = _evidence(
        snapshot,
        base_result,
        quotes=(VerifiedSpreadQuoteV1(instrument_id=_FIRST_ID, bid="39.90", ask="40.10"),),
        selected_quotes=(_selected(_FIRST_ID, 1),),
    )

    with pytest.raises(ValueError, match="SCENARIO_SPREAD_COST_BASIS_INVALID"):
        apply_scenario_spread_cost(
            snapshot=_snapshot(first_price="41"), base_result=base_result, evidence=evidence
        )


def test_manifest_hash_is_canonical_and_includes_expected_result_hash() -> None:
    snapshot = _snapshot()
    base_result = _base_result(first_target="0.55", second_target="0.25")
    first = _selected(_FIRST_ID, 1)
    second = _selected(_SECOND_ID, 2)
    left = _input(snapshot, base_result, selected_quotes=(second, first))
    right = _input(snapshot, base_result, selected_quotes=(first, second))

    left_manifest = finalize_scenario_spread_manifest(left, expected_result_hash="e" * 64)
    right_manifest = finalize_scenario_spread_manifest(right, expected_result_hash="e" * 64)
    changed = finalize_scenario_spread_manifest(right, expected_result_hash="f" * 64)

    assert left.selected_quotes == (first, second)
    assert canonical_scenario_spread_manifest_hash(left_manifest) == (
        canonical_scenario_spread_manifest_hash(right_manifest)
    )
    assert canonical_scenario_spread_manifest_hash(right_manifest) != (
        canonical_scenario_spread_manifest_hash(changed)
    )


def test_internal_contracts_reject_binary_floats_and_duplicate_quotes() -> None:
    with pytest.raises((TypeError, ValidationError), match="binary floats"):
        VerifiedSpreadQuoteV1(instrument_id=_FIRST_ID, bid=39.9, ask="40.10")

    snapshot = _snapshot()
    base_result = _base_result()
    selected = _selected(_FIRST_ID, 1)
    with pytest.raises(ValidationError, match="unique instruments"):
        _input(snapshot, base_result, selected_quotes=(selected, selected))


def test_runtime_quotes_must_match_the_frozen_manifest_identities() -> None:
    snapshot = _snapshot()
    base_result = _base_result(first_target="0.55", second_target="0.25")
    evidence_input = _input(
        snapshot,
        base_result,
        selected_quotes=(_selected(_FIRST_ID, 1),),
    )

    with pytest.raises(ValidationError, match="frozen manifest"):
        ScenarioSpreadEvidenceV1(
            input=evidence_input,
            quotes=(
                VerifiedSpreadQuoteV1(instrument_id=_FIRST_ID, bid="39.90", ask="40.10"),
                VerifiedSpreadQuoteV1(instrument_id=_SECOND_ID, bid="29.90", ask="30.10"),
            ),
            bundle_hash=scenario_spread_bundle_hash(evidence_input),
            available_at=datetime(2026, 9, 10, 14, 59, 59, tzinfo=UTC),
        )
