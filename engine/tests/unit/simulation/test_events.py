"""Corporate-action and valuation behavior for the shared simulator."""

from datetime import date
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.simulation.engine import apply_session_events, mark_to_market
from nexus.simulation.models import (
    DelistingEvent,
    DividendEvent,
    DividendReceivable,
    SessionEvents,
    SimulationInstrumentState,
    SimulationMark,
    SimulationState,
    SplitEvent,
)

SESSION = date(2026, 1, 5)


def _held_state() -> SimulationState:
    return SimulationState(
        cash=Decimal("0"),
        instruments=(
            SimulationInstrumentState(
                instrument_id=UUID(int=1),
                quantity=Decimal("10"),
                cost_basis=Decimal("100"),
                realized_pnl=Decimal("0"),
                cumulative_cost=Decimal("0"),
                last_close=Decimal("10"),
                untradable=False,
            ),
        ),
        receivables=(),
    )


def test_session_events_apply_split_dividend_settlement_then_delisting() -> None:
    instrument = UUID(int=1)
    result = apply_session_events(
        _held_state(),
        SessionEvents(
            session=SESSION,
            splits=(
                SplitEvent(authority_id=UUID(int=10), instrument_id=instrument, ratio=Decimal("2")),
            ),
            dividends=(
                DividendEvent(
                    authority_id=UUID(int=11),
                    instrument_id=instrument,
                    amount_per_share=Decimal("0.5"),
                    entitlement_session=SESSION,
                    payable_session=SESSION,
                ),
            ),
            delistings=(
                DelistingEvent(
                    authority_id=UUID(int=12),
                    instrument_id=instrument,
                    recovery_price=Decimal("4"),
                ),
            ),
        ),
    )

    position = result.state.instruments[0]
    assert result.state.cash == Decimal("90.000000000000")
    assert position.quantity == Decimal("0.000000000000")
    assert position.cost_basis == Decimal("0.000000000000")
    assert position.realized_pnl == Decimal("-10.000000000000")
    assert position.untradable is True
    assert result.state.receivables == ()
    assert result.delisting_count == 1


def test_dividend_receivable_is_not_spendable_before_payable_session() -> None:
    instrument = UUID(int=1)
    authority = UUID(int=11)

    result = apply_session_events(
        _held_state(),
        SessionEvents(
            session=SESSION,
            dividends=(
                DividendEvent(
                    authority_id=authority,
                    instrument_id=instrument,
                    amount_per_share=Decimal("0.5"),
                    entitlement_session=SESSION,
                    payable_session=date(2026, 1, 7),
                ),
            ),
        ),
    )

    assert result.state.cash == Decimal("0.000000000000")
    assert result.state.receivables == (
        DividendReceivable(
            authority_id=authority,
            instrument_id=instrument,
            amount=Decimal("5.000000000000"),
            payable_session=date(2026, 1, 7),
        ),
    )


def test_session_events_reject_unresolved_delisting() -> None:
    with pytest.raises(ValueError, match="unresolved delisting"):
        apply_session_events(
            _held_state(),
            SessionEvents(
                session=SESSION,
                delistings=(
                    DelistingEvent(
                        authority_id=UUID(int=12),
                        instrument_id=UUID(int=1),
                        recovery_price=None,
                    ),
                ),
            ),
        )


def test_zero_recovery_realizes_full_basis_loss_and_marks_untradable() -> None:
    result = apply_session_events(
        _held_state(),
        SessionEvents(
            session=SESSION,
            delistings=(
                DelistingEvent(
                    authority_id=UUID(int=12),
                    instrument_id=UUID(int=1),
                    recovery_price=Decimal("0"),
                ),
            ),
        ),
    )

    position = result.state.instruments[0]
    assert result.state.cash == Decimal("0")
    assert position.quantity == Decimal("0")
    assert position.realized_pnl == Decimal("-100")
    assert position.untradable is True
    assert result.delisting_count == 1


def test_mark_to_market_values_cash_holdings_and_receivables() -> None:
    state = _held_state()
    state = SimulationState(
        cash=Decimal("5"),
        instruments=state.instruments,
        receivables=(
            DividendReceivable(
                authority_id=UUID(int=11),
                instrument_id=UUID(int=1),
                amount=Decimal("2"),
                payable_session=date(2026, 1, 7),
            ),
        ),
    )

    result = mark_to_market(
        state,
        (SimulationMark(instrument_id=UUID(int=1), price=Decimal("11")),),
    )

    assert result.nav == Decimal("117.000000000000")
    assert result.missing_instruments == ()


def test_mark_to_market_reports_missing_active_holding_without_carry_forward() -> None:
    result = mark_to_market(_held_state(), ())

    assert result.nav == Decimal("0.000000000000")
    assert result.missing_instruments == (UUID(int=1),)
