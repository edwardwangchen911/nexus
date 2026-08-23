from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

from nexus.providers.broker.base import (
    BrokerOrderFact,
    BrokerPosition,
    CashBalance,
)
from nexus.reconciliation.facts import (
    BrokerReadSet,
    DerivedCurrencyNav,
)

ACCOUNT_ID = UUID("0198d400-0000-7000-8000-000000000001")
INSTRUMENT_A = UUID("0198d400-0000-7000-8000-000000000002")
INSTRUMENT_B = UUID("0198d400-0000-7000-8000-000000000003")
SYNC_RUN_ID = UUID("0198d400-0000-7000-8000-000000000004")
NOW = datetime(2026, 8, 23, 4, 0, tzinfo=UTC)


def cash(value: str, *, available: str | None = None) -> CashBalance:
    return CashBalance(
        currency="USD",
        cash_balance=Decimal(value),
        available_cash=Decimal(available or value),
        provider_as_of=NOW,
    )


def position(
    instrument_id: UUID = INSTRUMENT_A,
    *,
    quantity: str = "10",
    available: str = "8",
    frozen: str = "2",
    avg_cost: str | None = "100.00",
    diluted_cost: str | None = "101.25",
    current_price: str | None = "123.45",
    provider_as_of: datetime | None = NOW,
) -> BrokerPosition:
    return BrokerPosition(
        instrument_id=instrument_id,
        currency="USD",
        quantity=Decimal(quantity),
        available_quantity=Decimal(available),
        frozen_quantity=Decimal(frozen),
        avg_cost=Decimal(avg_cost) if avg_cost is not None else None,
        diluted_cost=Decimal(diluted_cost) if diluted_cost is not None else None,
        current_price=Decimal(current_price) if current_price is not None else None,
        provider_as_of=provider_as_of,
    )


def order(
    provider_order_id: str = "synthetic-order-1",
    *,
    instrument_id: UUID = INSTRUMENT_A,
    filled_quantity: str = "1",
    status: str = "submitted",
) -> BrokerOrderFact:
    return BrokerOrderFact(
        provider_order_id=provider_order_id,
        instrument_id=instrument_id,
        side="buy",
        quantity=Decimal("5"),
        filled_quantity=Decimal(filled_quantity),
        price=Decimal("99.50"),
        status=status,
        updated_at=NOW,
    )


def read_set(
    *,
    cash_value: str = "1000",
    positions: tuple[BrokerPosition, ...] | None = None,
    orders: tuple[BrokerOrderFact, ...] | None = None,
    nav_value: str | None = None,
) -> BrokerReadSet:
    read_positions = positions if positions is not None else (position(),)
    read_cash = (cash(cash_value),)
    derived_value = (
        Decimal(nav_value)
        if nav_value is not None
        else Decimal(cash_value)
        + sum(item.quantity * item.current_price for item in read_positions if item.current_price)
    )
    return BrokerReadSet(
        cash=read_cash,
        positions=read_positions,
        open_orders=orders if orders is not None else (order(),),
        completed_at=NOW,
        nav_by_currency=(
            DerivedCurrencyNav(
                currency="USD",
                value=derived_value,
                provider_as_of=NOW,
                source_request_ids=("synthetic-nav",),
            ),
        ),
    )
