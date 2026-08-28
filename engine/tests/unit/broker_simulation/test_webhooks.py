from datetime import UTC, datetime, timedelta

import pytest
from nexus.broker_simulation.webhooks import (
    NotAdmittedWebhookAuthenticator,
    WebhookContractError,
    parse_webhook_candidate,
)

_NOW = datetime(2026, 8, 28, 14, tzinfo=UTC)


def _body(**overrides: object) -> bytes:
    values: dict[str, object] = {
        "event_id": "synthetic-event-001",
        "occurred_at": "2026-08-28T13:59:00Z",
        "provider_order_ref": "synthetic-order-001",
        "order_status": "working",
    }
    values.update(overrides)
    import json

    return json.dumps(values, separators=(",", ":")).encode()


def test_webhook_candidate_is_closed_bounded_and_non_authoritative() -> None:
    candidate = parse_webhook_candidate(_body(), received_at=_NOW)

    assert candidate.event_id == "synthetic-event-001"
    assert candidate.authoritative is False
    with pytest.raises(WebhookContractError, match="WEBHOOK_VALIDATION_ERROR"):
        parse_webhook_candidate(_body(url="https://forbidden.invalid"), received_at=_NOW)
    with pytest.raises(WebhookContractError, match="WEBHOOK_TOO_LARGE"):
        parse_webhook_candidate(b"x" * 65_537, received_at=_NOW)


def test_old_or_future_webhook_is_retained_only_as_stale_hint() -> None:
    old = parse_webhook_candidate(
        _body(occurred_at=(_NOW - timedelta(days=2)).isoformat()), received_at=_NOW
    )
    future = parse_webhook_candidate(
        _body(occurred_at=(_NOW + timedelta(minutes=6)).isoformat()), received_at=_NOW
    )

    assert old.safe_code == "WEBHOOK_STALE_HINT"
    assert future.safe_code == "WEBHOOK_STALE_HINT"


@pytest.mark.asyncio
async def test_default_authenticator_is_not_admitted() -> None:
    status = await NotAdmittedWebhookAuthenticator().authenticate({}, _body())

    assert status == "not_admitted"
