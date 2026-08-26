from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.api.errors import ErrorCode, NexusError
from nexus.paper.cursor import PaperOrderCursorCodec, PaperOrderCursorState

NOW = datetime(2026, 8, 27, 20, tzinfo=UTC)
ORDER_ID = UUID("67000000-0000-0000-0000-000000000002")


def test_paper_order_cursor_round_trips_keyset_and_filter() -> None:
    state = PaperOrderCursorState(
        decision_time=NOW,
        after_created_at=NOW - timedelta(minutes=1),
        after_order_id=ORDER_ID,
        status="partially_filled",
    )
    codec = PaperOrderCursorCodec(("current-secret",), NOW)

    cursor = codec.encode(state)

    assert codec.decode(cursor, requested_status="partially_filled") == state


@pytest.mark.parametrize("mutation", ("tampered", "filter", "expired"))
def test_paper_order_cursor_rejects_tamper_filter_drift_and_expiry(mutation: str) -> None:
    issued_at = NOW - timedelta(minutes=6) if mutation == "expired" else NOW
    codec = PaperOrderCursorCodec(("current-secret",), NOW)
    cursor = codec.encode(
        PaperOrderCursorState(
            decision_time=issued_at,
            after_created_at=NOW - timedelta(minutes=1),
            after_order_id=ORDER_ID,
            status="submitted",
        )
    )
    if mutation == "tampered":
        cursor = f"{cursor[:-1]}{'A' if cursor[-1] != 'A' else 'B'}"
    requested_status = "filled" if mutation == "filter" else "submitted"

    with pytest.raises(NexusError) as raised:
        codec.decode(cursor, requested_status=requested_status)

    assert raised.value.code is ErrorCode.VALIDATION_ERROR
    assert raised.value.status_code == 422


def test_paper_cursor_accepts_previous_rotation_secret() -> None:
    state = PaperOrderCursorState(NOW, NOW - timedelta(minutes=1), ORDER_ID, None)
    old = PaperOrderCursorCodec(("old-secret",), NOW).encode(state)

    assert (
        PaperOrderCursorCodec(("new-secret", "old-secret"), NOW).decode(old, requested_status=None)
        == state
    )
