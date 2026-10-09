"""Admission checks the proposed ledger; formal F and economic H convert once."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from nexus.paper.formal_budget_protection import decision_within_slot, proposed_budget_total


@pytest.mark.parametrize(
    "offset,want", [(-1, False), (0, True), (30, True), (299, True), (300, False)]
)
def test_collected_decision_time_is_within_original_five_minute_slot(offset, want):
    slot = datetime(2026, 8, 27, 14, tzinfo=UTC)
    assert decision_within_slot(slot, slot + timedelta(seconds=offset)) is want


@pytest.mark.parametrize(
    "native,hold,future,amount,formal,child,want",
    [
        ("1", "2", "4", "1", "0", "0", "8"),
        ("1", "2", "4", "1", "1", "0", "7"),
        ("1", "2", "4", "1", "0", "1", "7"),
        ("0", "0", "0", "0", "0", "0", "0"),
        ("0.25", "0", "0.75", "0.25", "0.25", "0", "1"),
    ],
)
def test_post_admission_count_converts_its_own_protection_once(
    native, hold, future, amount, formal, child, want
):
    assert proposed_budget_total(
        native=Decimal(native),
        hold=Decimal(hold),
        future=Decimal(future),
        amount=Decimal(amount),
        consume_formal=Decimal(formal),
        consume_hold=Decimal(child),
    ) == Decimal(want)


@pytest.mark.parametrize(
    "changes",
    [
        {"future": None},
        {"native": Decimal("NaN")},
        {"hold": Decimal(-1)},
        {"consume_formal": Decimal(5)},
        {"consume_hold": Decimal(3)},
        {"consume_formal": Decimal("0.5")},
        {"consume_formal": Decimal(1), "consume_hold": Decimal(1)},
    ],
)
def test_unknown_or_inexact_credit_cannot_authorize_a_new_reservation(changes):
    values = dict(
        native=Decimal(1),
        hold=Decimal(2),
        future=Decimal(4),
        amount=Decimal(1),
        consume_formal=Decimal(0),
        consume_hold=Decimal(0),
    )
    values.update(changes)
    with pytest.raises(ValueError, match="FORMAL_BUDGET_COVERAGE_UNKNOWN"):
        proposed_budget_total(**values)
