import base64
import json
from uuid import UUID

import httpx
import pytest
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from nexus.notifications.bark import (
    BarkPublisher,
    BarkPublishError,
    map_bark_notification,
)
from nexus.outbox.models import OutboxMessage

_EVENT_ID = UUID("0199d500-1000-7000-8000-000000000001")
_AGGREGATE_ID = UUID("0199d500-1000-7000-8000-000000000002")
_KEY = b"0123456789abcdef"
_IV = b"abcdefghijklmnop"
_SENSITIVE_VALUES = (
    "account-secret-123",
    "999999.99",
    "evidence body must stay local",
    "executable-token-value",
)


def _message(
    event_type: str = "strategy_decision.persisted",
    schema_version: str = "paper_event_v1",
) -> OutboxMessage:
    return OutboxMessage(
        event_id=_EVENT_ID,
        event_type=event_type,
        aggregate_id=_AGGREGATE_ID,
        schema_version=schema_version,
        payload={
            "account_id": _SENSITIVE_VALUES[0],
            "amount": _SENSITIVE_VALUES[1],
            "evidence": _SENSITIVE_VALUES[2],
            "token": _SENSITIVE_VALUES[3],
        },
    )


@pytest.mark.parametrize(
    ("event_type", "schema_version"),
    (
        ("strategy_decision.persisted", "paper_event_v1"),
        ("paper.order_batch.evaluated", "paper_event_v1"),
        ("paper.order.risk_rejected", "paper_event_v1"),
        ("paper.order.submitted", "paper_event_v1"),
        ("paper.execution.advance.partially_filled", "paper_execution_v1"),
        ("paper.execution.advance.filled", "paper_execution_v1"),
        ("paper.execution.advance.cancelled", "paper_execution_v1"),
        ("paper.execution.cancelled", "paper_execution_v1"),
    ),
)
def test_notification_mapping_never_copies_outbox_payload(
    event_type: str,
    schema_version: str,
) -> None:
    notification = map_bark_notification(
        _message(event_type, schema_version),
        read_only_url="http://nexus.local/paper",
    )

    rendered = json.dumps(notification, ensure_ascii=False)
    assert all(value not in rendered for value in _SENSITIVE_VALUES)
    assert notification["url"] == "http://nexus.local/paper"
    assert notification["id"] == str(_EVENT_ID)


def test_notification_mapping_rejects_unregistered_event() -> None:
    with pytest.raises(BarkPublishError, match="^BARK_EVENT_UNSUPPORTED$"):
        map_bark_notification(_message("paper.order.failed"), read_only_url=None)


def test_notification_mapping_rejects_non_paper_link() -> None:
    with pytest.raises(ValueError, match="read-only Paper"):
        map_bark_notification(
            _message(),
            read_only_url="http://nexus.local/orders/approve",
        )


@pytest.mark.asyncio
async def test_publisher_rejects_non_localhost_server() -> None:
    async with httpx.AsyncClient() as client:
        with pytest.raises(ValueError, match="localhost"):
            BarkPublisher(
                client=client,
                server_url="https://public.example.com",
                device_key="device-key",
                encryption_key=_KEY,
            )


def _decrypt(ciphertext: str, iv: str) -> dict[str, object]:
    decryptor = Cipher(algorithms.AES(_KEY), modes.CBC(iv.encode("ascii"))).decryptor()
    padded = decryptor.update(base64.b64decode(ciphertext)) + decryptor.finalize()
    unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
    cleartext = unpadder.update(padded) + unpadder.finalize()
    return json.loads(cleartext)


@pytest.mark.asyncio
async def test_publisher_encrypts_all_notification_content_and_uses_stable_id() -> None:
    requests: list[dict[str, object]] = []

    async def endpoint(request: httpx.Request) -> httpx.Response:
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"code": 200, "message": "success"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as client:
        publisher = BarkPublisher(
            client=client,
            server_url="http://127.0.0.1:8080",
            device_key="device-key-only-outside",
            encryption_key=_KEY,
            read_only_url="http://nexus.local/paper",
            iv_factory=lambda: _IV,
        )
        assert "device-key-only-outside" not in repr(publisher)
        assert "0123456789abcdef" not in repr(publisher)
        await publisher.publish(_message())
        await publisher.publish(_message())

    assert len(requests) == 2
    assert requests[0] == requests[1]
    assert set(requests[0]) == {"device_key", "ciphertext", "iv"}
    assert requests[0]["device_key"] == "device-key-only-outside"
    inner = _decrypt(str(requests[0]["ciphertext"]), str(requests[0]["iv"]))
    assert set(inner) == {"title", "body", "group", "id", "url"}
    assert inner["id"] == str(_EVENT_ID)
    assert all(value not in json.dumps(inner, ensure_ascii=False) for value in _SENSITIVE_VALUES)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    (
        httpx.Response(503, text="private upstream response"),
        httpx.Response(200, json={"code": 500, "message": "private Bark response"}),
        httpx.Response(200, text="not-json"),
    ),
)
async def test_publisher_rejects_failed_or_malformed_responses_without_details(
    response: httpx.Response,
) -> None:
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda request: response)) as client:
        publisher = BarkPublisher(
            client=client,
            server_url="http://127.0.0.1:8080",
            device_key="device-key",
            encryption_key=_KEY,
            iv_factory=lambda: _IV,
        )
        with pytest.raises(BarkPublishError, match="^BARK_RESPONSE_INVALID$") as caught:
            await publisher.publish(_message())

    assert "private" not in str(caught.value)


@pytest.mark.asyncio
async def test_publisher_converts_timeout_to_value_free_error() -> None:
    async def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("private timeout detail", request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(timeout)) as client:
        publisher = BarkPublisher(
            client=client,
            server_url="http://127.0.0.1:8080",
            device_key="device-key",
            encryption_key=_KEY,
            iv_factory=lambda: _IV,
        )
        with pytest.raises(BarkPublishError, match="^BARK_TRANSPORT_FAILED$") as caught:
            await publisher.publish(_message())

    assert "private" not in str(caught.value)
