"""Target the actual concentration warning without exposing another holding."""

from decimal import Decimal
from uuid import UUID

import pytest
from nexus.agents.portfolio_context import _concentration_focus
from nexus.persistence.models.diagnostics import (
    PortfolioDiagnosticItemRecord,
    PortfolioDiagnosticReportRecord,
)
from nexus.persistence.models.portfolio import PositionSnapshot


def position(index: int, value: str, currency: str = "USD") -> PositionSnapshot:
    return PositionSnapshot(
        instrument_id=UUID(int=index), currency=currency,
        quantity=Decimal(1), current_price=Decimal(value),
    )


def warning(**changes) -> PortfolioDiagnosticItemRecord:
    values = dict(
        diagnostic_code="CONCENTRATION_TOP1", policy_scope="currency:USD",
        metric_value=Decimal("0.7"), reference_limit=Decimal("0.35"),
        severity="warning", unknown_reason=None,
    )
    return PortfolioDiagnosticItemRecord(**(values | changes))


def test_only_the_largest_holding_in_the_matching_currency_gets_the_question():
    large, small = position(1, "70"), position(2, "30")
    report = PortfolioDiagnosticReportRecord(policy_version="real_advisory_v1")
    focus = _concentration_focus(large, [large, small], [warning()], report)
    assert focus is not None
    assert focus.observed_weight == "0.7"
    assert focus.reference_limit == "0.35"
    assert focus.weight_basis == "same_currency_securities_excluding_cash"
    assert _concentration_focus(small, [large, small], [warning()], report) is None
    assert "instrument_id" not in focus.model_dump()


@pytest.mark.parametrize("changes", [
    {"policy_scope": "currency:HKD"}, {"severity": "info"},
    {"unknown_reason": "POSITION_PRICE_EVIDENCE_MISSING"},
    {"metric_value": Decimal("0.6")}, {"reference_limit": None},
])
def test_mismatched_or_unverified_warning_cannot_become_a_research_question(changes):
    large, small = position(1, "70"), position(2, "30")
    assert _concentration_focus(
        large, [large, small], [warning(**changes)],
        PortfolioDiagnosticReportRecord(policy_version="test"),
    ) is None
