from __future__ import annotations

import pytest
from helpers.broker_facts import NOW, cash, order, position, read_set
from nexus.reconciliation.facts import BrokerReadSet, compare_read_sets


@pytest.mark.parametrize(
    ("nav", "cash_b", "consistent"),
    [
        ("1000", "1001", True),
        ("1000", "1001.000000000001", False),
        ("1000000", "1500", True),
        ("1000000", "1500.000000000001", False),
    ],
)
def test_cash_tolerance_uses_exact_one_unit_or_five_bps_boundary(
    nav: str,
    cash_b: str,
    consistent: bool,
) -> None:
    result = compare_read_sets(
        read_set(nav_value=nav),
        read_set(cash_value=cash_b, nav_value=nav),
    )

    assert result.consistent is consistent
    assert ("cash:USD" in result.reasons) is (not consistent)


def test_available_cash_and_nav_coverage_are_reconciled_per_currency() -> None:
    changed_available = BrokerReadSet(
        cash=(cash("1000", available="998.999999999999"),),
        positions=(position(),),
        open_orders=(order(),),
        completed_at=NOW,
        nav_by_currency=read_set(nav_value="1000").nav_by_currency,
    )

    available_result = compare_read_sets(read_set(nav_value="1000"), changed_available)
    without_nav = BrokerReadSet(
        cash=read_set().cash,
        positions=read_set().positions,
        open_orders=read_set().open_orders,
        completed_at=NOW,
    )
    missing_nav = compare_read_sets(without_nav, without_nav)

    assert available_result.consistent is False
    assert available_result.reasons == ("available_cash:USD",)
    assert missing_nav.consistent is False
    assert missing_nav.reasons == ("nav:USD",)
