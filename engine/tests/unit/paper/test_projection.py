from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.paper.ledger import make_buy_reservation_transaction, post_ledger_transaction
from nexus.paper.models import (
    Ledger,
    LedgerAccount,
    LedgerEntry,
    LedgerTransaction,
    OrderEventInput,
    PaperOrder,
    Reservation,
    ReservationEvent,
)
from nexus.paper.projection import DrawdownLockEvent, build_paper_projection
from nexus.risk.canonical import (
    canonical_paper_ledger_transaction_hash,
    canonical_paper_projection_hash,
)

NOW = datetime(2026, 8, 21, 20, tzinfo=UTC)
ACCOUNT_ID = UUID(int=1)
PORTFOLIO_ID = UUID(int=2)
ORDER_ID = UUID(int=3)
INSTRUMENT_ID = UUID(int=4)


def _ledger() -> Ledger:
    account_specs = (
        (10, "asset.cash.available.usd", "debit"),
        (11, "asset.cash.reserved.usd", "debit"),
        (12, "asset.position_cost.usd", "debit"),
        (13, "equity.contributed_capital.usd", "credit"),
        (14, "income.realized_pnl.usd", "credit"),
        (15, "income.dividend.usd", "credit"),
        (16, "expense.fees.usd", "debit"),
    )
    accounts = tuple(
        LedgerAccount(
            account_id=UUID(int=index),
            paper_account_id=ACCOUNT_ID,
            code=code,
            normal_side=normal_side,
            currency="USD",
        )
        for index, code, normal_side in account_specs
    )
    draft = LedgerTransaction(
        transaction_id=UUID(int=100),
        account_id=ACCOUNT_ID,
        transaction_type="funding",
        reference_type="initial_funding",
        reference_hash="a" * 64,
        occurred_at=NOW,
        entries=(
            LedgerEntry(
                entry_id=UUID(int=101),
                entry_seq=1,
                account_code="asset.cash.available.usd",
                currency="USD",
                debit_amount=Decimal("10000"),
                credit_amount=Decimal(0),
            ),
            LedgerEntry(
                entry_id=UUID(int=102),
                entry_seq=2,
                account_code="equity.contributed_capital.usd",
                currency="USD",
                debit_amount=Decimal(0),
                credit_amount=Decimal("10000"),
            ),
        ),
        transaction_hash="0" * 64,
    )
    funding = draft.model_copy(
        update={"transaction_hash": canonical_paper_ledger_transaction_hash(draft)}
    )
    return post_ledger_transaction(
        Ledger(paper_account_id=ACCOUNT_ID, accounts=accounts, transactions=()),
        funding,
    )


def _drawdown_events() -> tuple[DrawdownLockEvent, ...]:
    return (
        DrawdownLockEvent(
            event_id=UUID(int=200),
            event_seq=1,
            from_state=None,
            to_state="unlocked",
            drawdown_ratio=Decimal(0),
            user_acknowledged=False,
            reason_code="PAPER_ACCOUNT_INITIALIZED",
            occurred_at=NOW,
            payload_hash="b" * 64,
        ),
    )


def _submitted_buy() -> PaperOrder:
    return PaperOrder(
        order_id=ORDER_ID,
        account_id=ACCOUNT_ID,
        strategy_decision_id=UUID(int=5),
        client_order_id="order-1",
        instrument_id=INSTRUMENT_ID,
        side="buy",
        quantity=Decimal("10"),
        worst_price=Decimal("101"),
        events=(
            OrderEventInput(
                event_id=UUID(int=301),
                event_seq=1,
                occurred_at=NOW,
                from_status=None,
                to_status="proposed",
                actor_kind="task18",
                reason_code="PROPOSED",
                payload_hash="c" * 64,
            ),
            OrderEventInput(
                event_id=UUID(int=302),
                event_seq=2,
                occurred_at=NOW,
                from_status="proposed",
                to_status="risk_approved",
                actor_kind="task18",
                reason_code="APPROVED",
                payload_hash="d" * 64,
            ),
            OrderEventInput(
                event_id=UUID(int=303),
                event_seq=3,
                occurred_at=NOW,
                from_status="risk_approved",
                to_status="submitted",
                actor_kind="task18",
                reason_code="SUBMITTED",
                payload_hash="e" * 64,
            ),
        ),
    )


def _reservation() -> Reservation:
    return Reservation(
        reservation_id=UUID(int=400),
        order_id=ORDER_ID,
        account_id=ACCOUNT_ID,
        reservation_type="cash",
        currency="USD",
        instrument_id=None,
        amount=Decimal("1010"),
        events=(
            ReservationEvent(
                event_id=UUID(int=401),
                event_seq=1,
                event_type="reserved",
                amount=Decimal("1010"),
                occurred_at=NOW,
                payload_hash="f" * 64,
            ),
        ),
    )


def _cancelled_buy() -> PaperOrder:
    return _submitted_buy().model_copy(
        update={
            "events": (
                *_submitted_buy().events,
                OrderEventInput(
                    event_id=UUID(int=304),
                    event_seq=4,
                    occurred_at=NOW,
                    from_status="submitted",
                    to_status="cancelled",
                    actor_kind="task19",
                    reason_code="CANCELLED",
                    payload_hash="2" * 64,
                ),
            )
        }
    )


def _released_reservation() -> Reservation:
    return _reservation().model_copy(
        update={
            "events": (
                *_reservation().events,
                ReservationEvent(
                    event_id=UUID(int=402),
                    event_seq=2,
                    event_type="released",
                    amount=Decimal("1010"),
                    occurred_at=NOW,
                    payload_hash="3" * 64,
                ),
            )
        }
    )


def _build(**overrides: object):
    values: dict[str, object] = {
        "account_id": ACCOUNT_ID,
        "portfolio_id": PORTFOLIO_ID,
        "as_of": NOW,
        "ledger": _ledger(),
        "orders": (),
        "reservations": (),
        "positions": (),
        "peak_nav": Decimal("10000"),
        "drawdown_events": _drawdown_events(),
    }
    values.update(overrides)
    return build_paper_projection(**values)


def test_initial_cash_projection_recomputes_its_hash() -> None:
    projection = _build()

    assert projection.available_cash == Decimal("10000")
    assert projection.reserved_cash == Decimal(0)
    assert projection.nav == Decimal("10000")
    assert projection.drawdown_state == "unlocked"
    assert projection.projection_hash == canonical_paper_projection_hash(projection)


def test_projection_hash_binds_the_exact_source_fact_chain() -> None:
    original = _build()
    changed_event = _drawdown_events()[0].model_copy(
        update={"reason_code": "DIFFERENT_GENESIS_REASON"}
    )
    changed = _build(drawdown_events=(changed_event,))

    assert changed.available_cash == original.available_cash
    assert changed.nav == original.nav
    assert changed.drawdown_state == original.drawdown_state
    assert changed.source_facts_hash != original.source_facts_hash
    assert changed.projection_hash != original.projection_hash


def test_open_buy_uses_matching_cash_reservation_and_ledger_balance() -> None:
    reserved_ledger = post_ledger_transaction(
        _ledger(),
        make_buy_reservation_transaction(
            transaction_id=UUID(int=500),
            debit_entry_id=UUID(int=501),
            credit_entry_id=UUID(int=502),
            paper_account_id=ACCOUNT_ID,
            reference_hash="1" * 64,
            amount=Decimal("1010"),
            occurred_at=NOW,
        ),
    )
    projection = _build(
        ledger=reserved_ledger,
        orders=(_submitted_buy(),),
        reservations=(_reservation(),),
    )

    assert projection.available_cash == Decimal("8990")
    assert projection.reserved_cash == Decimal("1010")
    assert projection.nav == Decimal("10000")
    assert projection.open_orders[0].reserved_cash == Decimal("1010")


def test_projection_rejects_hash_mismatch_invalid_event_chain_and_orphan_reservation() -> None:
    invalid_transaction = (
        _ledger().transactions[0].model_copy(update={"transaction_hash": "9" * 64})
    )
    with pytest.raises(ValueError, match="hash"):
        _build(ledger=_ledger().model_copy(update={"transactions": (invalid_transaction,)}))

    invalid_order = _submitted_buy().model_copy(update={"events": _submitted_buy().events[1:]})
    with pytest.raises(ValueError, match="order event"):
        _build(orders=(invalid_order,))

    with pytest.raises(ValueError, match="reservation has no matching open order"):
        _build(reservations=(_reservation(),))


def test_projection_rejects_ledger_reservation_mismatch() -> None:
    with pytest.raises(ValueError, match="reserved cash does not reconcile"):
        _build(orders=(_submitted_buy(),), reservations=(_reservation(),))


def test_closed_order_accepts_only_a_fully_released_reservation() -> None:
    projection = _build(
        orders=(_cancelled_buy(),),
        reservations=(_released_reservation(),),
    )

    assert projection.open_orders == ()

    with pytest.raises(
        ValueError,
        match="closed order reservation must be fully consumed or released",
    ):
        _build(orders=(_cancelled_buy(),), reservations=(_reservation(),))


def test_drawdown_chain_requires_thresholds_and_user_acknowledgement() -> None:
    locked = DrawdownLockEvent(
        event_id=UUID(int=201),
        event_seq=2,
        from_state="unlocked",
        to_state="locked",
        drawdown_ratio=Decimal("0.08"),
        user_acknowledged=False,
        reason_code="DRAWDOWN_LOCKED",
        occurred_at=NOW,
        payload_hash="1" * 64,
    )
    assert _build(drawdown_events=(*_drawdown_events(), locked)).drawdown_state == "locked"

    illegal_unlock = DrawdownLockEvent(
        event_id=UUID(int=202),
        event_seq=3,
        from_state="locked",
        to_state="unlocked",
        drawdown_ratio=Decimal("0.05"),
        user_acknowledged=True,
        reason_code="INVALID_DIRECT_UNLOCK",
        occurred_at=NOW,
        payload_hash="2" * 64,
    )
    with pytest.raises(ValueError, match="drawdown transition"):
        _build(drawdown_events=(*_drawdown_events(), locked, illegal_unlock))
