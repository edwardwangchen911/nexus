"""Formal demand remains visible before Jobs exist and is not charged twice."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest
from nexus.paper.formal_budget_inventory import describe_coverage


@pytest.mark.parametrize(
    "state,reserved,settled,run_status,want_state,want_native,want_future",
    [
        (None, None, None, None, "future_not_created", "0", "3"),
        ("reserved", "3", None, "prepared", "reserved", "3", "0"),
        ("reserved", "3", None, "failed", "reserved_usage_unsettled", "3", "0"),
        ("settled", "3", "1", "completed", "settled", "1", "0"),
        ("settled", "3", "0", "failed", "settled", "0", "0"),
        ("released", "3", "0", "failed", "released_terminal", "0", "0"),
        ("released", "3", "0", "prepared", "unknown", "0", None),
        ("settled", "3", None, "failed", "unknown", "3", None),
        ("released", "3", None, "failed", "unknown", "3", None),
        (None, None, None, "prepared", "unknown", "0", None),
    ],
)
def test_exact_task_coverage_does_not_recreate_settled_demand(
    state, reserved, settled, run_status, want_state, want_native, want_future
):
    now = datetime(2026, 10, 9, 13, tzinfo=UTC)
    reservation = (
        None
        if state is None
        else SimpleNamespace(
            state=state,
            reserved_usd=Decimal(reserved),
            settled_usd=None if settled is None else Decimal(settled),
        )
    )
    result = describe_coverage(
        maximum=Decimal(3),
        now=now,
        slot=now + timedelta(hours=1),
        run_status=run_status,
        reservation=reservation,
        no_start_confirmed=True,
    )
    assert result == (
        want_state,
        Decimal(want_native),
        None if want_future is None else Decimal(want_future),
    )


def test_past_slot_only_removes_demand_when_no_start_is_confirmed():
    now = datetime(2026, 10, 9, 15, tzinfo=UTC)
    slot = now - timedelta(hours=1)
    assert describe_coverage(
        maximum=Decimal(3),
        now=now,
        slot=slot,
        run_status=None,
        reservation=None,
        no_start_confirmed=True,
    ) == ("window_passed_no_start", Decimal(0), Decimal(0))
    assert describe_coverage(
        maximum=Decimal(3),
        now=now,
        slot=slot,
        run_status=None,
        reservation=None,
        no_start_confirmed=False,
    ) == ("unknown", Decimal(0), None)
