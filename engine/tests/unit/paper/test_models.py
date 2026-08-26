from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.paper.models import (
    LedgerAccount,
    LedgerEntry,
    LedgerTransaction,
    OrderEventInput,
    PaperOrder,
    Reservation,
    ReservationEvent,
)
from pydantic import ValidationError

NOW = datetime(2026, 8, 21, 20, tzinfo=UTC)


def test_order_event_requires_aware_time_and_canonical_hash() -> None:
    with pytest.raises(ValidationError, match="timezone-aware"):
        OrderEventInput(
            event_id=UUID(int=1),
            event_seq=1,
            occurred_at=datetime(2026, 8, 21, 20),
            from_status=None,
            to_status="proposed",
            actor_kind="task18",
            reason_code="CREATED",
            payload_hash="a" * 64,
        )
    with pytest.raises(ValidationError, match="64"):
        OrderEventInput(
            event_id=UUID(int=1),
            event_seq=1,
            occurred_at=NOW,
            from_status=None,
            to_status="proposed",
            actor_kind="task18",
            reason_code="CREATED",
            payload_hash="not-a-hash",
        )


def test_paper_order_requires_positive_whole_quantity() -> None:
    with pytest.raises(ValidationError, match="whole"):
        PaperOrder(
            order_id=UUID(int=1),
            account_id=UUID(int=2),
            strategy_decision_id=UUID(int=3),
            client_order_id="paper-1",
            instrument_id=UUID(int=4),
            side="buy",
            quantity=Decimal("1.5"),
            events=(),
        )


def test_reservation_fields_are_exclusive_by_type() -> None:
    with pytest.raises(ValidationError, match="cash reservation"):
        Reservation(
            reservation_id=UUID(int=1),
            order_id=UUID(int=2),
            account_id=UUID(int=3),
            reservation_type="cash",
            currency="USD",
            instrument_id=UUID(int=4),
            amount=Decimal("100"),
            events=(),
        )
    with pytest.raises(ValidationError, match="position reservation"):
        Reservation(
            reservation_id=UUID(int=1),
            order_id=UUID(int=2),
            account_id=UUID(int=3),
            reservation_type="position",
            currency="USD",
            instrument_id=None,
            amount=Decimal("1"),
            events=(),
        )


def test_reservation_event_requires_positive_amount() -> None:
    with pytest.raises(ValidationError, match="positive"):
        ReservationEvent(
            event_id=UUID(int=1),
            event_seq=1,
            event_type="reserved",
            amount=Decimal("0"),
            occurred_at=NOW,
            payload_hash="a" * 64,
        )


def test_ledger_entry_allows_exactly_one_side() -> None:
    with pytest.raises(ValidationError, match="exactly one"):
        LedgerEntry(
            entry_id=UUID(int=1),
            entry_seq=1,
            account_code="asset.cash.available.usd",
            currency="USD",
            debit_amount=Decimal("10"),
            credit_amount=Decimal("10"),
        )
    with pytest.raises(ValidationError, match="exactly one"):
        LedgerEntry(
            entry_id=UUID(int=1),
            entry_seq=1,
            account_code="asset.cash.available.usd",
            currency="USD",
            debit_amount=Decimal("0"),
            credit_amount=Decimal("0"),
        )


def test_ledger_transaction_rejects_duplicate_entry_ids() -> None:
    entry = LedgerEntry(
        entry_id=UUID(int=1),
        entry_seq=1,
        account_code="asset.cash.available.usd",
        currency="USD",
        debit_amount=Decimal("10"),
        credit_amount=Decimal("0"),
    )
    with pytest.raises(ValidationError, match="entry ids must be unique"):
        LedgerTransaction(
            transaction_id=UUID(int=10),
            account_id=UUID(int=11),
            transaction_type="funding",
            reference_type="initial_funding",
            reference_hash="b" * 64,
            occurred_at=NOW,
            entries=(entry, entry),
            transaction_hash="a" * 64,
        )


def test_ledger_account_normal_side_and_currency_are_closed() -> None:
    with pytest.raises(ValidationError):
        LedgerAccount(
            account_id=UUID(int=1),
            paper_account_id=UUID(int=2),
            code="asset.cash.available.usd",
            normal_side="asset",  # type: ignore[arg-type]
            currency="USD",
        )
    with pytest.raises(ValidationError, match="USD"):
        LedgerAccount(
            account_id=UUID(int=1),
            paper_account_id=UUID(int=2),
            code="asset.cash.available.usd",
            normal_side="debit",
            currency="HKD",
        )
