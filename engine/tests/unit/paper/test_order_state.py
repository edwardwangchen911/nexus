from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.paper.models import OrderEventInput, PaperOrder
from nexus.paper.orders import current_order_status, transition_order

NOW = datetime(2026, 8, 21, 20, tzinfo=UTC)


def _order(events: tuple[OrderEventInput, ...] = ()) -> PaperOrder:
    return PaperOrder(
        order_id=UUID(int=1),
        account_id=UUID(int=2),
        strategy_decision_id=UUID(int=3),
        client_order_id="paper-order-1",
        instrument_id=UUID(int=4),
        side="buy",
        quantity=Decimal("1"),
        events=events,
    )


def _event(
    seq: int,
    *,
    source: str | None,
    target: str,
    actor: str = "task18",
    event_id: UUID | None = None,
) -> OrderEventInput:
    return OrderEventInput(
        event_id=event_id or UUID(int=100 + seq),
        event_seq=seq,
        occurred_at=NOW,
        from_status=source,
        to_status=target,
        actor_kind=actor,
        reason_code="TEST",
        payload_hash=f"{seq:x}".rjust(64, "0"),
    )


def test_task18_happy_path_requires_explicit_risk_event() -> None:
    proposed = transition_order(
        _order(),
        "proposed",
        _event(1, source=None, target="proposed"),
    )
    approved = transition_order(
        proposed,
        "risk_approved",
        _event(2, source="proposed", target="risk_approved"),
    )
    submitted = transition_order(
        approved,
        "submitted",
        _event(3, source="risk_approved", target="submitted"),
    )

    assert current_order_status(submitted) == "submitted"
    assert [event.to_status for event in submitted.events] == [
        "proposed",
        "risk_approved",
        "submitted",
    ]


def test_task18_can_end_at_risk_rejected() -> None:
    proposed = transition_order(
        _order(),
        "proposed",
        _event(1, source=None, target="proposed"),
    )
    rejected = transition_order(
        proposed,
        "risk_rejected",
        _event(2, source="proposed", target="risk_rejected"),
    )

    assert current_order_status(rejected) == "risk_rejected"


def test_order_rejects_skipped_risk_and_awaiting_approval_for_task18() -> None:
    proposed = transition_order(
        _order(),
        "proposed",
        _event(1, source=None, target="proposed"),
    )
    with pytest.raises(ValueError, match="invalid order transition"):
        transition_order(
            proposed,
            "submitted",
            _event(2, source="proposed", target="submitted"),
        )
    approved = transition_order(
        proposed,
        "risk_approved",
        _event(2, source="proposed", target="risk_approved"),
    )
    with pytest.raises(ValueError, match="task18"):
        transition_order(
            approved,
            "awaiting_approval",
            _event(3, source="risk_approved", target="awaiting_approval"),
        )


def test_task18_cannot_write_fill_or_cancellation_states() -> None:
    submitted = _order(
        (
            _event(1, source=None, target="proposed"),
            _event(2, source="proposed", target="risk_approved"),
            _event(3, source="risk_approved", target="submitted"),
        )
    )

    with pytest.raises(ValueError, match="task18"):
        transition_order(
            submitted,
            "filled",
            _event(4, source="submitted", target="filled"),
        )


def test_task19_can_append_a_valid_fill_transition() -> None:
    submitted = _order(
        (
            _event(1, source=None, target="proposed"),
            _event(2, source="proposed", target="risk_approved"),
            _event(3, source="risk_approved", target="submitted"),
        )
    )

    filled = transition_order(
        submitted,
        "filled",
        _event(4, source="submitted", target="filled", actor="task19"),
    )

    assert current_order_status(filled) == "filled"


def test_task19_can_record_more_than_one_partial_fill_without_fake_state() -> None:
    first_partial = _order(
        (
            _event(1, source=None, target="proposed"),
            _event(2, source="proposed", target="risk_approved"),
            _event(3, source="risk_approved", target="submitted"),
            _event(
                4,
                source="submitted",
                target="partially_filled",
                actor="task19",
            ),
        )
    )

    second_partial = transition_order(
        first_partial,
        "partially_filled",
        _event(
            5,
            source="partially_filled",
            target="partially_filled",
            actor="task19",
        ),
    )

    assert current_order_status(second_partial) == "partially_filled"


@pytest.mark.parametrize(
    "event",
    (
        _event(2, source=None, target="proposed"),
        _event(1, source="proposed", target="proposed"),
        _event(1, source=None, target="risk_approved"),
    ),
)
def test_first_event_requires_sequence_one_and_proposed(event: OrderEventInput) -> None:
    with pytest.raises(ValueError, match="first order event"):
        transition_order(_order(), event.to_status, event)


def test_order_rejects_sequence_gap_source_mismatch_and_duplicate_event_id() -> None:
    proposed = transition_order(
        _order(),
        "proposed",
        _event(1, source=None, target="proposed", event_id=UUID(int=999)),
    )
    with pytest.raises(ValueError, match="continuous"):
        transition_order(
            proposed,
            "risk_approved",
            _event(3, source="proposed", target="risk_approved"),
        )
    with pytest.raises(ValueError, match="from_status"):
        transition_order(
            proposed,
            "risk_approved",
            _event(2, source="risk_rejected", target="risk_approved"),
        )
    with pytest.raises(ValueError, match="event id"):
        transition_order(
            proposed,
            "risk_approved",
            _event(
                2,
                source="proposed",
                target="risk_approved",
                event_id=UUID(int=999),
            ),
        )


def test_terminal_order_is_immutable() -> None:
    rejected = _order(
        (
            _event(1, source=None, target="proposed"),
            _event(2, source="proposed", target="risk_rejected"),
        )
    )

    with pytest.raises(ValueError, match="terminal"):
        transition_order(
            rejected,
            "proposed",
            _event(3, source="risk_rejected", target="proposed"),
        )
