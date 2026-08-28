from decimal import Decimal
from uuid import UUID

import pytest
from nexus.broker_simulation.domain import BrokerOrderDocument
from nexus.broker_simulation.risk import (
    BrokerRiskContext,
    BrokerRiskInstrument,
    BrokerRiskPosition,
    evaluate_broker_order_batch,
)


def _order(**overrides: object) -> BrokerOrderDocument:
    values: dict[str, object] = {
        "instrument_id": UUID(int=11),
        "side": "buy",
        "quantity": 10,
        "limit_price": Decimal("100.00"),
        "sort_key": 1,
    }
    values.update(overrides)
    return BrokerOrderDocument(**values)


def _context(**overrides: object) -> BrokerRiskContext:
    values: dict[str, object] = {
        "available_cash": Decimal("10000"),
        "nav": Decimal("20000"),
        "drawdown_locked": False,
        "minimum_cash_ratio": Decimal("0.10"),
        "single_name_limit": Decimal("0.20"),
        "sector_limit": Decimal("0.30"),
        "positions": (BrokerRiskPosition(instrument_id=UUID(int=11), quantity=Decimal("5")),),
        "instruments": (
            BrokerRiskInstrument(
                instrument_id=UUID(int=11),
                asset_type="common_stock",
                sector="technology",
                whitelisted=True,
                existing_notional=Decimal("1000"),
            ),
        ),
        "sector_notionals": {"technology": Decimal("1000")},
    }
    values.update(overrides)
    return BrokerRiskContext(**values)


def test_valid_whole_share_long_only_batch_passes() -> None:
    decision = evaluate_broker_order_batch((_order(),), _context())

    assert decision.passed is True
    assert decision.reason_codes == ()
    assert decision.projected_cash == Decimal("9000")


@pytest.mark.parametrize(
    ("orders", "context", "reason"),
    [
        ((_order(),), _context(drawdown_locked=True), "DRAWDOWN_LOCKED"),
        (
            (_order(),),
            _context(
                instruments=(
                    BrokerRiskInstrument(
                        instrument_id=UUID(int=11),
                        asset_type="common_stock",
                        sector="technology",
                        whitelisted=False,
                        existing_notional=Decimal("1000"),
                    ),
                )
            ),
            "INSTRUMENT_NOT_WHITELISTED",
        ),
        (
            (_order(side="sell", quantity=6),),
            _context(),
            "LONG_ONLY_POSITION_EXCEEDED",
        ),
        (
            (_order(quantity=101),),
            _context(),
            "INSUFFICIENT_CASH",
        ),
        (
            (_order(quantity=31),),
            _context(),
            "SINGLE_NAME_LIMIT_EXCEEDED",
        ),
    ],
)
def test_hard_risk_rejects_unsafe_batches(
    orders: tuple[BrokerOrderDocument, ...],
    context: BrokerRiskContext,
    reason: str,
) -> None:
    decision = evaluate_broker_order_batch(orders, context)

    assert decision.passed is False
    assert reason in decision.reason_codes


def test_risk_contract_rejects_binary_float() -> None:
    with pytest.raises(ValueError, match="binary float"):
        _context(available_cash=10000.0)
