import inspect
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from nexus.paper.errors import PaperBatchError
from nexus.paper.service import (
    _subtract_portfolio_risk_remainders,
    canonical_paper_batch_request_hash,
    create_paper_order_batch,
    meets_portfolio_minimum_notional,
)
from nexus.risk.models import OrderCandidate


def _candidate(*, side: str, quantity: str) -> OrderCandidate:
    return OrderCandidate(
        instrument_id=uuid4(),
        side=side,
        desired_quantity=Decimal(quantity),
        target_weight=Decimal("0.2"),
        reference_close=Decimal("100"),
        strategy_priority=1,
        asset_type="common_stock",
        currency="USD",
        sector_exposures=(),
    )


def test_public_batch_request_has_no_caller_supplied_order_fields() -> None:
    parameters = inspect.signature(create_paper_order_batch).parameters

    assert tuple(parameters) == (
        "session",
        "account_id",
        "strategy_decision_id",
        "idempotency_key",
        "request_time",
        "target_version_id",
        "hooks",
    )
    assert not {
        "instrument_id",
        "target_weight",
        "side",
        "quantity",
        "price",
        "risk_outcome",
    } & set(parameters)


def test_request_hash_is_stable_and_requires_utc() -> None:
    values = {
        "account_id": UUID(int=1),
        "strategy_decision_id": UUID(int=2),
        "request_time": datetime(2026, 8, 26, 21, tzinfo=UTC),
    }

    assert canonical_paper_batch_request_hash(**values) == (
        canonical_paper_batch_request_hash(**values)
    )
    with pytest.raises(PaperBatchError) as captured:
        canonical_paper_batch_request_hash(
            **{
                **values,
                "request_time": datetime(
                    2026,
                    8,
                    27,
                    5,
                    tzinfo=timezone(timedelta(hours=8)),
                ),
            }
        )
    assert captured.value.code == "STRATEGY_DECISION_INVALID"


def test_stable_errors_expose_only_safe_messages() -> None:
    error = PaperBatchError("RISK_AUTHORITY_INCOMPLETE")

    assert error.code == "RISK_AUTHORITY_INCOMPLETE"
    assert "SQL" not in error.safe_message
    assert "provider" not in error.safe_message.lower()
    assert "account" not in error.safe_message.lower()


@pytest.mark.parametrize(
    ("side", "desired", "covered_side", "covered", "remaining"),
    [
        ("buy", "5", "buy", "5", None),
        ("buy", "8", "buy", "5", Decimal("3")),
        ("sell", "5", "buy", "5", Decimal("5")),
    ],
)
def test_portfolio_risk_remainder_only_subtracts_fixed_same_side_quantity(
    side: str,
    desired: str,
    covered_side: str,
    covered: str,
    remaining: Decimal | None,
) -> None:
    candidate = _candidate(side=side, quantity=desired)

    filtered = _subtract_portfolio_risk_remainders(
        (candidate,),
        {(candidate.instrument_id, covered_side): Decimal(covered)},
    )

    if remaining is None:
        assert filtered == ()
    else:
        assert len(filtered) == 1
        assert filtered[0].desired_quantity == remaining


@pytest.mark.parametrize("side", ["buy", "sell"])
@pytest.mark.parametrize(
    ("quantity", "reference_close", "expected"),
    [("1", "99.99", False), ("1", "100", True)],
)
def test_portfolio_minimum_notional_applies_to_buys_and_sells(
    side: str, quantity: str, reference_close: str, expected: bool
) -> None:
    candidate = _candidate(side=side, quantity=quantity).model_copy(
        update={"reference_close": Decimal(reference_close)}
    )
    assert meets_portfolio_minimum_notional(candidate) is expected


@pytest.mark.parametrize("side", ["buy", "sell"])
def test_risk_remainder_cannot_leave_a_subminimum_portfolio_order(side: str) -> None:
    candidate = _candidate(side=side, quantity="3").model_copy(
        update={"reference_close": Decimal("40")}
    )
    assert meets_portfolio_minimum_notional(candidate)

    remaining = _subtract_portfolio_risk_remainders(
        (candidate,), {(candidate.instrument_id, side): Decimal("1")}
    )

    assert tuple(filter(meets_portfolio_minimum_notional, remaining)) == ()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scenario",
    ["expired", "occupied", "below-drift", "fully-covered-remainder"],
)
async def test_public_target_batch_always_requires_activation(scenario: str) -> None:
    class NoDatabaseAccess:
        def __getattr__(self, name: str) -> object:
            raise AssertionError(f"unexpected database access: {name}")

    with pytest.raises(PaperBatchError) as captured:
        await create_paper_order_batch(
            NoDatabaseAccess(),  # type: ignore[arg-type]
            account_id=uuid4(),
            strategy_decision_id=uuid4(),
            idempotency_key=scenario,
            request_time=datetime(2026, 9, 19, 14, tzinfo=UTC),
            target_version_id=uuid4(),
        )

    assert captured.value.code == "PORTFOLIO_ACTIVATION_REQUIRED"
