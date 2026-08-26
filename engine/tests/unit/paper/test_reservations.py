from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.paper.models import Reservation, ReservationEvent
from nexus.paper.orders import append_reservation_event, fold_reservation_events

NOW = datetime(2026, 8, 21, 20, tzinfo=UTC)


def _event(
    seq: int,
    event_type: str,
    amount: Decimal,
    *,
    event_id: UUID | None = None,
) -> ReservationEvent:
    return ReservationEvent(
        event_id=event_id or UUID(int=100 + seq),
        event_seq=seq,
        event_type=event_type,
        amount=amount,
        occurred_at=NOW,
        payload_hash=f"{seq:x}".rjust(64, "0"),
    )


def _reservation(events: tuple[ReservationEvent, ...] = ()) -> Reservation:
    return Reservation(
        reservation_id=UUID(int=1),
        order_id=UUID(int=2),
        account_id=UUID(int=3),
        reservation_type="cash",
        currency="USD",
        instrument_id=None,
        amount=Decimal("101"),
        events=events,
    )


def test_reservation_folds_partial_consumption_and_release_to_zero() -> None:
    reservation = _reservation()
    reservation = append_reservation_event(
        reservation,
        _event(1, "reserved", Decimal("101")),
    )
    reservation = append_reservation_event(
        reservation,
        _event(2, "consumed", Decimal("80")),
    )
    reservation = append_reservation_event(
        reservation,
        _event(3, "released", Decimal("21")),
    )

    assert fold_reservation_events(reservation) == Decimal("0")


def test_first_reservation_event_must_match_root_amount() -> None:
    with pytest.raises(ValueError, match="first reservation event"):
        append_reservation_event(
            _reservation(),
            _event(1, "reserved", Decimal("100")),
        )
    with pytest.raises(ValueError, match="first reservation event"):
        append_reservation_event(
            _reservation(),
            _event(1, "consumed", Decimal("101")),
        )


def test_reservation_rejects_sequence_gap_duplicate_and_overconsumption() -> None:
    reserved = append_reservation_event(
        _reservation(),
        _event(1, "reserved", Decimal("101"), event_id=UUID(int=999)),
    )
    with pytest.raises(ValueError, match="continuous"):
        append_reservation_event(
            reserved,
            _event(3, "released", Decimal("1")),
        )
    with pytest.raises(ValueError, match="event id"):
        append_reservation_event(
            reserved,
            _event(2, "released", Decimal("1"), event_id=UUID(int=999)),
        )
    with pytest.raises(ValueError, match="exceeds outstanding"):
        append_reservation_event(
            reserved,
            _event(2, "consumed", Decimal("102")),
        )


def test_closed_reservation_is_immutable() -> None:
    reserved = append_reservation_event(
        _reservation(),
        _event(1, "reserved", Decimal("101")),
    )
    closed = append_reservation_event(
        reserved,
        _event(2, "released", Decimal("101")),
    )

    with pytest.raises(ValueError, match="closed"):
        append_reservation_event(
            closed,
            _event(3, "released", Decimal("1")),
        )
