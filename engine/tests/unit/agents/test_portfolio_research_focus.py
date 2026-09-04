"""Target the actual concentration warning without exposing another holding."""

from decimal import Decimal
from uuid import UUID

import pytest
from nexus.agents.portfolio_context import _concentration_focus
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID, REAL_ADVISORY_POLICY_VERSION
from nexus.diagnostics.repository import _stored_decimal
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
        metric_value=Decimal("0.7"), reference_limit=Decimal("0.30"), unit="ratio",
        severity="critical", unknown_reason=None,
    )
    return PortfolioDiagnosticItemRecord(**(values | changes))


def test_only_the_largest_holding_in_the_matching_currency_gets_the_question():
    large, small = position(1, "70"), position(2, "30")
    report = PortfolioDiagnosticReportRecord(
        policy_id=REAL_ADVISORY_POLICY_ID, policy_version=REAL_ADVISORY_POLICY_VERSION,
    )
    focus = _concentration_focus(large, [large, small], [warning()], report)
    assert focus is not None
    assert focus.observed_weight == "0.7"
    assert Decimal(focus.reference_limit) == Decimal("0.30")
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
        PortfolioDiagnosticReportRecord(
            policy_id=REAL_ADVISORY_POLICY_ID, policy_version=REAL_ADVISORY_POLICY_VERSION,
        ),
    ) is None


@pytest.mark.parametrize("changes", [
    {"reference_limit": Decimal("0.35")}, {"unit": "percent"},
    {"severity": "warning"},
])
def test_policy_inconsistent_diagnostic_does_not_establish_focus(changes):
    large, small = position(1, "70"), position(2, "30")
    report = PortfolioDiagnosticReportRecord(
        policy_id=REAL_ADVISORY_POLICY_ID, policy_version=REAL_ADVISORY_POLICY_VERSION,
    )
    assert _concentration_focus(large, [large, small], [warning(**changes)], report) is None


@pytest.mark.parametrize("changes", [
    {"policy_id": UUID(int=999)}, {"policy_version": "invented"},
])
def test_unpublished_policy_identity_does_not_establish_focus(changes):
    large, small = position(1, "70"), position(2, "30")
    report = PortfolioDiagnosticReportRecord(**({
        "policy_id": REAL_ADVISORY_POLICY_ID, "policy_version": REAL_ADVISORY_POLICY_VERSION,
    } | changes))
    assert _concentration_focus(large, [large, small], [warning()], report) is None


@pytest.mark.parametrize("weight,severity,limit,eligible", [
    ("0.15", "info", "0.15", False),
    ("0.150000000001", "warning", "0.15", True),
    ("0.30", "warning", "0.15", True),
    ("0.300000000001", "critical", "0.30", True),
    ("0.1500000000001", "warning", "0.15", True),
    ("0.3000000000001", "critical", "0.30", True),
])
def test_exact_advisory_boundaries(weight, severity, limit, eligible):
    fraction = Decimal(weight)
    held = position(1, str(fraction * 100))
    peers = [position(i, str((1 - fraction) * 10)) for i in range(2, 12)]
    report = PortfolioDiagnosticReportRecord(
        policy_id=REAL_ADVISORY_POLICY_ID, policy_version=REAL_ADVISORY_POLICY_VERSION,
    )
    focus = _concentration_focus(held, [held, *peers], [warning(
        metric_value=_stored_decimal(str(fraction)), severity=severity,
        reference_limit=Decimal(limit),
    )], report)
    assert (focus is not None) is eligible


def test_tied_largest_holding_is_eligible_but_duplicate_members_are_not():
    left, right = position(1, "50"), position(2, "50")
    report = PortfolioDiagnosticReportRecord(
        policy_id=REAL_ADVISORY_POLICY_ID, policy_version=REAL_ADVISORY_POLICY_VERSION,
    )
    item = warning(metric_value=Decimal("0.5"))
    assert _concentration_focus(left, [left, right], [item], report) is not None
    assert _concentration_focus(left, [left, right, right], [item], report) is None


def test_conflicting_top1_rows_cannot_select_only_the_favorable_match():
    large, small = position(1, "70"), position(2, "30")
    report = PortfolioDiagnosticReportRecord(
        policy_id=REAL_ADVISORY_POLICY_ID, policy_version=REAL_ADVISORY_POLICY_VERSION,
    )
    assert _concentration_focus(large, [large, small], [
        warning(), warning(metric_value=Decimal("0.6")),
    ], report) is None


@pytest.mark.parametrize("changes", [
    {"current_price": None}, {"current_price": Decimal(0)},
    {"current_price": Decimal("NaN")}, {"quantity": Decimal(0)},
    {"quantity": Decimal("Infinity")}, {"currency": "HKD"},
])
def test_incomplete_or_cross_currency_denominator_is_not_accepted(changes):
    large, small = position(1, "70"), position(2, "30")
    for key, value in changes.items():
        setattr(small, key, value)
    report = PortfolioDiagnosticReportRecord(
        policy_id=REAL_ADVISORY_POLICY_ID, policy_version=REAL_ADVISORY_POLICY_VERSION,
    )
    assert _concentration_focus(large, [large, small], [warning()], report) is None
