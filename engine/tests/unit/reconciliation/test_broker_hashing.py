from __future__ import annotations

import pytest
from helpers.broker_facts import INSTRUMENT_A, INSTRUMENT_B, NOW, order, position
from nexus.reconciliation.facts import (
    DuplicateBrokerFact,
    canonical_order_hash,
    canonical_position_hash,
)


def test_canonical_hashes_cover_reconciliation_fields_and_ignore_market_observations() -> None:
    positions = (position(INSTRUMENT_B), position(INSTRUMENT_A))
    reverse_positions = tuple(reversed(positions))
    orders = (
        order("synthetic-order-2", instrument_id=INSTRUMENT_B),
        order("synthetic-order-1", instrument_id=INSTRUMENT_A),
    )

    assert canonical_position_hash(positions) == canonical_position_hash(reverse_positions)
    assert canonical_order_hash(orders) == canonical_order_hash(tuple(reversed(orders)))
    assert canonical_position_hash(positions) != canonical_position_hash(
        (position(INSTRUMENT_B, diluted_cost="101.26"), position(INSTRUMENT_A))
    )
    assert canonical_position_hash(positions) == canonical_position_hash(
        (position(INSTRUMENT_B, current_price="123.46"), position(INSTRUMENT_A))
    )
    assert canonical_position_hash(positions) == canonical_position_hash(
        (
            position(INSTRUMENT_B, provider_as_of=NOW.replace(microsecond=1)),
            position(INSTRUMENT_A),
        )
    )
    assert canonical_order_hash(orders) != canonical_order_hash(
        (
            order("synthetic-order-2", instrument_id=INSTRUMENT_B, filled_quantity="2"),
            order("synthetic-order-1", instrument_id=INSTRUMENT_A),
        )
    )


def test_canonical_hash_is_versioned_and_decimal_normalization_is_fixed() -> None:
    normalized = position(
        quantity="10.000",
        available="8.0",
        frozen="-0.000",
        avg_cost=None,
        diluted_cost="101.2500",
    )

    assert canonical_position_hash((normalized,)) == (
        "f4a511eaee87de3e8809bc916e504f05cad9840a38c35be3307e289677d38188"
    )


def test_duplicate_fact_identity_and_unknown_order_status_fail_closed() -> None:
    with pytest.raises(DuplicateBrokerFact, match="position"):
        canonical_position_hash((position(), position(quantity="11")))
    with pytest.raises(ValueError, match="status"):
        order(status="synthetic-unknown")
