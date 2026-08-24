"""Tests for fail-closed, Decimal-only daily research budget accounting."""

from decimal import Decimal

import pytest
from nexus.agents.budget import BudgetExhausted, DailyBudget


def test_budget_reserves_worst_case_and_refunds_only_reported_unused_cost() -> None:
    """Removing the reservation would permit simultaneous work to overrun the daily limit."""
    budget = DailyBudget.usd("3.00")
    reservation = budget.reserve(Decimal("2.00"))
    assert reservation == Decimal("2.00")
    assert budget.remaining_usd == Decimal("1.00")
    budget.settle(reservation, Decimal("0.75"))
    assert budget.spent_usd == Decimal("0.75")
    assert budget.remaining_usd == Decimal("2.25")


def test_budget_rejects_stale_pricing_and_cost_above_reservation() -> None:
    """Treating stale estimates or excess usage as valid would defeat the hard daily cap."""
    budget = DailyBudget.usd("3.00")
    with pytest.raises(BudgetExhausted, match="stale pricing"):
        budget.reserve(Decimal("1.00"), pricing_fresh=False)
    reservation = budget.reserve(Decimal("1.00"))
    with pytest.raises(BudgetExhausted, match="exceeds reservation"):
        budget.settle(reservation, Decimal("1.01"))


def test_budget_release_returns_only_unspent_reservation() -> None:
    """A failed invocation must not strand capacity or erase already settled usage."""
    budget = DailyBudget.usd("3.00")
    first = budget.reserve(Decimal("1.50"))
    budget.settle(first, Decimal("0.40"))
    second = budget.reserve(Decimal("1.00"))
    budget.release(second)
    assert budget.spent_usd == Decimal("0.40")
    assert budget.remaining_usd == Decimal("2.60")


def test_budget_rejects_release_of_unknown_reservation() -> None:
    """Releasing more than reserved would manufacture daily capacity."""
    budget = DailyBudget.usd("3.00")
    budget.reserve(Decimal("0.50"))
    with pytest.raises(BudgetExhausted, match="reservation is not available"):
        budget.release(Decimal("0.51"))
