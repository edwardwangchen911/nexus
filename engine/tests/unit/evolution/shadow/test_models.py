"""Strict value-object boundaries for Strategy Shadow."""

from datetime import date
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.evolution.shadow.models import (
    EVALUATION_SESSION_COUNT,
    GRACE_SESSION_COUNT,
    INITIAL_NAV,
    StrategyShadowPositionV1,
    StrategyShadowSessionPointV1,
    StrategyShadowStateV1,
)

INSTRUMENT_A = UUID("019d0000-0000-7000-8000-000000006101")
INSTRUMENT_B = UUID("019d0000-0000-7000-8000-000000006102")


def test_state_is_canonical_same_start_and_nav_100() -> None:
    positions = (
        StrategyShadowPositionV1(
            instrument_id=INSTRUMENT_B,
            quantity=Decimal("0.25"),
            cost_basis=Decimal("25"),
            realized_pnl=Decimal("0"),
            cumulative_fees=Decimal("0"),
            last_close=Decimal("100"),
            untradable=False,
        ),
        StrategyShadowPositionV1(
            instrument_id=INSTRUMENT_A,
            quantity=Decimal("0.50"),
            cost_basis=Decimal("50"),
            realized_pnl=Decimal("0"),
            cumulative_fees=Decimal("0"),
            last_close=Decimal("100"),
            untradable=False,
        ),
    )

    state = StrategyShadowStateV1(
        cash=Decimal("25"),
        nav=INITIAL_NAV,
        positions=positions,
        position_rank_states=(),
        hard_risk_violation=False,
    )

    assert state.nav == Decimal("100.000000000000")
    assert tuple(item.instrument_id for item in state.positions) == (
        INSTRUMENT_A,
        INSTRUMENT_B,
    )
    assert EVALUATION_SESSION_COUNT == 63
    assert GRACE_SESSION_COUNT == 5


def test_models_reject_binary_floats_duplicate_positions_and_bad_ordinals() -> None:
    with pytest.raises((TypeError, ValueError), match="binary float"):
        StrategyShadowStateV1(
            cash=1.5,
            nav=INITIAL_NAV,
            positions=(),
            position_rank_states=(),
            hard_risk_violation=False,
        )

    position = StrategyShadowPositionV1(
        instrument_id=INSTRUMENT_A,
        quantity=Decimal("1"),
        cost_basis=Decimal("1"),
        realized_pnl=Decimal("0"),
        cumulative_fees=Decimal("0"),
        last_close=Decimal("1"),
        untradable=False,
    )
    with pytest.raises(ValueError, match="unique"):
        StrategyShadowStateV1(
            cash=Decimal("98"),
            nav=INITIAL_NAV,
            positions=(position, position),
            position_rank_states=(),
            hard_risk_violation=False,
        )

    with pytest.raises(ValueError):
        StrategyShadowSessionPointV1(
            session=date(2026, 8, 28),
            ordinal=64,
            nav=INITIAL_NAV,
            turnover=Decimal("0"),
            fees=Decimal("0"),
            filled_order_count=0,
            hard_risk_violation=False,
            authority_hash="a" * 64,
        )
