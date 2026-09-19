import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.paper.target_service import load_risk_remainder_coverage, requires_portfolio_plan


@pytest.mark.parametrize(
    ("target_created", "gap", "expected"),
    [
        (True, Decimal("0.019999999999"), False),
        (True, Decimal("0.02"), True),
        (True, Decimal("0.025"), True),
        (False, Decimal("0.029999999999"), False),
        (False, Decimal("0.03"), True),
    ],
)
def test_plan_threshold_distinguishes_new_target_from_same_target_drift(
    target_created: bool,
    gap: Decimal,
    expected: bool,
) -> None:
    assert requires_portfolio_plan((gap,), target_created=target_created) is expected


def test_risk_remainder_is_fixed_and_does_not_double_count_later_fills() -> None:
    instrument_id = uuid4()
    first_order = SimpleNamespace(
        id=uuid4(), instrument_id=instrument_id, side="buy", reference_close=Decimal(100)
    )
    second_order = SimpleNamespace(
        id=uuid4(), instrument_id=instrument_id, side="buy", reference_close=Decimal(101)
    )
    third_order = SimpleNamespace(
        id=uuid4(), instrument_id=instrument_id, side="buy", reference_close=Decimal(102)
    )
    reverse_order = SimpleNamespace(
        id=uuid4(), instrument_id=instrument_id, side="sell", reference_close=Decimal(103)
    )
    at = datetime(2026, 9, 18, 14, 0, tzinfo=UTC)
    evaluations = (
        (
            first_order,
            SimpleNamespace(
                desired_quantity=Decimal(10),
                approved_quantity=Decimal(4),
                created_at=at,
            ),
        ),
        (
            second_order,
            SimpleNamespace(
                desired_quantity=Decimal(8),
                approved_quantity=Decimal(6),
                created_at=at + timedelta(minutes=1),
            ),
        ),
    )
    fills = (
        (
            first_order,
            SimpleNamespace(
                id=uuid4(), quantity=Decimal(4), occurred_at=at + timedelta(seconds=30)
            ),
        ),
        (
            second_order,
            SimpleNamespace(
                id=uuid4(), quantity=Decimal(2), occurred_at=at + timedelta(minutes=3)
            ),
        ),
        (
            third_order,
            SimpleNamespace(
                id=uuid4(), quantity=Decimal(2), occurred_at=at + timedelta(minutes=4)
            ),
        ),
        (
            reverse_order,
            SimpleNamespace(
                id=uuid4(), quantity=Decimal(100), occurred_at=at + timedelta(minutes=5)
            ),
        ),
    )

    class Result:
        def __init__(self, rows):
            self.rows = rows

        def tuples(self):
            return self.rows

    class Session:
        calls = 0

        async def execute(self, _statement):
            self.calls += 1
            return Result(evaluations if self.calls == 1 else fills)

    session = Session()
    coverage = asyncio.run(
        load_risk_remainder_coverage(
            session, account_id=uuid4(), target_version_id=uuid4()  # type: ignore[arg-type]
        )
    )

    assert session.calls == 1
    assert coverage[(instrument_id, "buy")].quantity == Decimal(8)
    assert set(type(coverage[(instrument_id, "buy")]).model_fields) == {"quantity"}
    assert (instrument_id, "sell") not in coverage
