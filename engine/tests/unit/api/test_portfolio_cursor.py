from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.api.errors import ErrorCode, NexusError
from nexus.portfolio.cursor import PortfolioCursorCodec, PortfolioCursorState

_NOW = datetime(2026, 8, 24, 4, 0, tzinfo=UTC)
_PORTFOLIO_ID = UUID("0198d500-0000-7000-8000-000000000010")


def test_portfolio_cursor_is_deterministic_and_round_trips_exact_keyset() -> None:
    codec = PortfolioCursorCodec(("current-secret",), _NOW)
    state = PortfolioCursorState(
        decision_time=_NOW,
        after_name="Broker HKD",
        after_portfolio_id=_PORTFOLIO_ID,
        kind="real_read_only",
    )

    first = codec.encode(state)
    second = codec.encode(state)

    assert first == second
    assert codec.decode(first, requested_kind="real_read_only") == state


@pytest.mark.parametrize("mutation", ("tampered", "wrong_filter", "expired"))
def test_portfolio_cursor_rejects_tamper_filter_drift_and_expiry(mutation: str) -> None:
    issued_at = _NOW - timedelta(minutes=6) if mutation == "expired" else _NOW
    codec = PortfolioCursorCodec(("current-secret",), _NOW)
    cursor = codec.encode(
        PortfolioCursorState(
            decision_time=issued_at,
            after_name="Broker",
            after_portfolio_id=_PORTFOLIO_ID,
            kind="real_read_only",
        )
    )
    requested_kind = "shadow" if mutation == "wrong_filter" else "real_read_only"
    if mutation == "tampered":
        cursor = f"{cursor[:-1]}{'A' if cursor[-1] != 'A' else 'B'}"

    with pytest.raises(NexusError) as error:
        codec.decode(cursor, requested_kind=requested_kind)

    assert error.value.code is ErrorCode.VALIDATION_ERROR
    assert error.value.status_code == 422


def test_portfolio_cursor_accepts_previous_rotation_secret() -> None:
    old = PortfolioCursorCodec(("previous-secret",), _NOW).encode(
        PortfolioCursorState(_NOW, "Broker", _PORTFOLIO_ID, None)
    )

    assert PortfolioCursorCodec(("current-secret", "previous-secret"), _NOW).decode(
        old, requested_kind=None
    ).after_portfolio_id == _PORTFOLIO_ID
