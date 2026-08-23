"""Fail-closed valuation primitives for diagnostic scopes."""

from collections.abc import Iterable
from decimal import Decimal

from nexus.providers.broker.base import BrokerPosition


def position_values(
    positions: Iterable[BrokerPosition],
) -> tuple[list[tuple[BrokerPosition, Decimal]], str | None]:
    """Return only positive, priced positions and the deterministic invalid reason."""
    values: list[tuple[BrokerPosition, Decimal]] = []
    unknown: str | None = None
    for position in positions:
        if position.current_price is None:
            unknown = "POSITION_PRICE_MISSING"
            continue
        if position.current_price <= 0:
            unknown = "POSITION_PRICE_INVALID"
            continue
        if position.quantity <= 0:
            unknown = "POSITION_QUANTITY_INVALID"
            continue
        values.append((position, position.quantity * position.current_price))
    return values, unknown
