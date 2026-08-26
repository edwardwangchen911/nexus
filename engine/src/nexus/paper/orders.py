"""Pure append-only folds for internal paper order and reservation events."""

from __future__ import annotations

from decimal import Decimal

from nexus.paper.models import (
    InternalOrderStatus,
    OrderEventInput,
    PaperOrder,
    Reservation,
    ReservationEvent,
)

_GENERIC_TRANSITIONS: dict[InternalOrderStatus, frozenset[InternalOrderStatus]] = {
    "proposed": frozenset(("risk_rejected", "risk_approved")),
    "risk_rejected": frozenset(),
    "risk_approved": frozenset(("submitted", "awaiting_approval")),
    "awaiting_approval": frozenset(("submitted", "cancelled", "rejected")),
    "submitted": frozenset(("partially_filled", "filled", "cancelled", "rejected", "reconciling")),
    "partially_filled": frozenset(
        ("partially_filled", "filled", "cancelled", "rejected", "reconciling")
    ),
    "filled": frozenset(),
    "cancelled": frozenset(),
    "rejected": frozenset(),
    "reconciling": frozenset(("submitted", "partially_filled", "filled", "cancelled", "rejected")),
}
_TERMINAL = frozenset(("risk_rejected", "filled", "cancelled", "rejected"))
_TASK18_TRANSITIONS = frozenset(
    (
        (None, "proposed"),
        ("proposed", "risk_rejected"),
        ("proposed", "risk_approved"),
        ("risk_approved", "submitted"),
    )
)


def _validate_actor_transition(
    event: OrderEventInput,
    source: InternalOrderStatus | None,
) -> None:
    if event.actor_kind == "task18" and (source, event.to_status) not in _TASK18_TRANSITIONS:
        raise ValueError("task18 cannot write this order transition")
    if event.actor_kind == "task19" and source not in {
        "submitted",
        "partially_filled",
        "reconciling",
    }:
        raise ValueError("task19 cannot write this order transition")


def _fold_order_events(
    events: tuple[OrderEventInput, ...],
) -> InternalOrderStatus | None:
    status: InternalOrderStatus | None = None
    event_ids = set()
    previous_time = None
    for index, event in enumerate(events, start=1):
        if event.event_id in event_ids:
            raise ValueError("order event id must be unique")
        event_ids.add(event.event_id)
        if event.event_seq != index:
            raise ValueError("order event sequence must be continuous")
        if previous_time is not None and event.occurred_at < previous_time:
            raise ValueError("order event time must be monotonic")
        previous_time = event.occurred_at
        if index == 1:
            if event.from_status is not None or event.to_status != "proposed":
                raise ValueError("first order event must be sequence one proposed")
        else:
            if status is None:
                raise ValueError("order event chain has no prior status")
            if status in _TERMINAL:
                raise ValueError("terminal order cannot accept another event")
            if event.from_status != status:
                raise ValueError("order event from_status must match prior status")
            if event.to_status not in _GENERIC_TRANSITIONS[status]:
                raise ValueError("invalid order transition")
        _validate_actor_transition(event, status)
        status = event.to_status
    return status


def current_order_status(order: PaperOrder) -> InternalOrderStatus | None:
    return _fold_order_events(order.events)


def transition_order(
    order: PaperOrder,
    target: InternalOrderStatus,
    event: OrderEventInput,
) -> PaperOrder:
    source = current_order_status(order)
    if source in _TERMINAL:
        raise ValueError("terminal order cannot accept another event")
    if event.event_id in {existing.event_id for existing in order.events}:
        raise ValueError("order event id must be unique")
    if event.to_status != target:
        raise ValueError("event target must match requested order status")
    if not order.events:
        if event.event_seq != 1 or event.from_status is not None or target != "proposed":
            raise ValueError("first order event must be sequence one proposed")
    else:
        if source is None:
            raise ValueError("order event chain has no prior status")
        if event.event_seq != len(order.events) + 1:
            raise ValueError("order event sequence must be continuous")
        if event.from_status != source:
            raise ValueError("order event from_status must match prior status")
        if target not in _GENERIC_TRANSITIONS[source]:
            raise ValueError("invalid order transition")
    _validate_actor_transition(event, source)
    updated = order.model_copy(update={"events": (*order.events, event)})
    _fold_order_events(updated.events)
    return updated


def fold_reservation_events(reservation: Reservation) -> Decimal:
    if not reservation.events:
        return Decimal(0)
    outstanding = Decimal(0)
    event_ids = set()
    previous_time = None
    for index, event in enumerate(reservation.events, start=1):
        if event.event_id in event_ids:
            raise ValueError("reservation event id must be unique")
        event_ids.add(event.event_id)
        if event.event_seq != index:
            raise ValueError("reservation event sequence must be continuous")
        if previous_time is not None and event.occurred_at < previous_time:
            raise ValueError("reservation event time must be monotonic")
        previous_time = event.occurred_at
        if index == 1:
            if event.event_type != "reserved" or event.amount != reservation.amount:
                raise ValueError("first reservation event must reserve the root amount")
            outstanding = event.amount
            continue
        if outstanding == 0:
            raise ValueError("closed reservation cannot accept another event")
        if event.event_type == "reserved":
            raise ValueError("reservation can only be reserved once")
        if event.amount > outstanding:
            raise ValueError("reservation event exceeds outstanding amount")
        outstanding -= event.amount
    return outstanding


def append_reservation_event(
    reservation: Reservation,
    event: ReservationEvent,
) -> Reservation:
    if event.event_id in {existing.event_id for existing in reservation.events}:
        raise ValueError("reservation event id must be unique")
    if event.event_seq != len(reservation.events) + 1:
        raise ValueError("reservation event sequence must be continuous")
    if reservation.events and fold_reservation_events(reservation) == 0:
        raise ValueError("closed reservation cannot accept another event")
    updated = reservation.model_copy(update={"events": (*reservation.events, event)})
    fold_reservation_events(updated)
    return updated
