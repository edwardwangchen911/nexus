"""Desensitized, encrypted Bark publisher for persisted Paper outbox events."""

from __future__ import annotations

import base64
import json
import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from urllib.parse import urlsplit

import httpx
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from nexus.outbox.models import OutboxMessage

BARK_EVENT_SCHEMAS: tuple[tuple[str, str], ...] = (
    ("strategy_decision.persisted", "paper_event_v1"),
    ("paper.order_batch.evaluated", "paper_event_v1"),
    ("paper.order.risk_rejected", "paper_event_v1"),
    ("paper.order.submitted", "paper_event_v1"),
    ("paper.execution.advance.partially_filled", "paper_execution_v1"),
    ("paper.execution.advance.filled", "paper_execution_v1"),
    ("paper.execution.advance.cancelled", "paper_execution_v1"),
    ("paper.execution.cancelled", "paper_execution_v1"),
)

_COPY: dict[tuple[str, str], tuple[str, str]] = {
    ("strategy_decision.persisted", "paper_event_v1"): (
        "NEXUS · Agent 决策",
        "Agent 决策已持久化，可在 NEXUS 查看。",
    ),
    ("paper.order_batch.evaluated", "paper_event_v1"): (
        "NEXUS · 模拟批次",
        "模拟订单批次已完成风控评估。",
    ),
    ("paper.order.risk_rejected", "paper_event_v1"): (
        "NEXUS · 风控拒绝",
        "一项内部模拟订单被确定性风控拒绝。",
    ),
    ("paper.order.submitted", "paper_event_v1"): (
        "NEXUS · 待处理订单",
        "一项内部模拟订单已提交，等待后续处理。",
    ),
    ("paper.execution.advance.partially_filled", "paper_execution_v1"): (
        "NEXUS · 部分成交",
        "一项内部模拟订单发生部分成交。",
    ),
    ("paper.execution.advance.filled", "paper_execution_v1"): (
        "NEXUS · 已成交",
        "一项内部模拟订单已全部成交。",
    ),
    ("paper.execution.advance.cancelled", "paper_execution_v1"): (
        "NEXUS · 已取消",
        "一项内部模拟订单已取消。",
    ),
    ("paper.execution.cancelled", "paper_execution_v1"): (
        "NEXUS · 已取消",
        "一项内部模拟订单已取消。",
    ),
}


class BarkPublishError(RuntimeError):
    """A value-free Bark delivery failure suitable for bounded outbox recording."""


def validate_local_bark_server_url(value: str) -> None:
    """Require the publisher transport to remain on this network namespace."""
    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError as error:
        raise ValueError("Bark server must be a localhost HTTP origin") from error
    if (
        parsed.scheme != "http"
        or parsed.hostname not in {"127.0.0.1", "localhost"}
        or port is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Bark server must be a localhost HTTP origin")


def validate_bark_read_only_url(value: str) -> None:
    """Allow notification navigation only to the existing read-only Paper page."""
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or parsed.hostname is None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != "/paper"
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("Bark link must target the read-only Paper page")


def map_bark_notification(
    message: OutboxMessage,
    *,
    read_only_url: str | None,
) -> dict[str, str]:
    """Map an exact event contract to fixed copy without reading its payload."""
    copy = _COPY.get((message.event_type, message.schema_version))
    if copy is None:
        raise BarkPublishError("BARK_EVENT_UNSUPPORTED")
    title, body = copy
    notification = {
        "title": title,
        "body": body,
        "group": "NEXUS",
        "id": str(message.event_id),
    }
    if read_only_url is not None:
        validate_bark_read_only_url(read_only_url)
        notification["url"] = read_only_url
    return notification


def _random_iv() -> bytes:
    return secrets.token_urlsafe(12).encode("ascii")


def _encrypt(payload: dict[str, str], *, key: bytes, iv: bytes) -> str:
    if len(key) != 16 or len(iv) != 16 or not iv.isascii():
        raise BarkPublishError("BARK_ENCRYPTION_CONFIGURATION_INVALID")
    cleartext = json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    padder = padding.PKCS7(algorithms.AES.block_size).padder()
    padded = padder.update(cleartext) + padder.finalize()
    encryptor = Cipher(algorithms.AES(key), modes.CBC(iv)).encryptor()
    ciphertext = encryptor.update(padded) + encryptor.finalize()
    return base64.b64encode(ciphertext).decode("ascii")


@dataclass(frozen=True)
class BarkPublisher:
    """Publish an encrypted notification through Bark's localhost V2 endpoint."""

    client: httpx.AsyncClient
    server_url: str
    device_key: str = field(repr=False)
    encryption_key: bytes = field(repr=False)
    read_only_url: str | None = None
    iv_factory: Callable[[], bytes] = _random_iv

    def __post_init__(self) -> None:
        validate_local_bark_server_url(self.server_url)
        if self.read_only_url is not None:
            validate_bark_read_only_url(self.read_only_url)

    async def __call__(self, message: OutboxMessage) -> None:
        await self.publish(message)

    async def publish(self, message: OutboxMessage) -> None:
        notification = map_bark_notification(
            message,
            read_only_url=self.read_only_url,
        )
        iv = self.iv_factory()
        request = {
            "device_key": self.device_key,
            "ciphertext": _encrypt(notification, key=self.encryption_key, iv=iv),
            "iv": iv.decode("ascii"),
        }
        try:
            response = await self.client.post(
                f"{self.server_url.rstrip('/')}/push",
                json=request,
            )
        except httpx.HTTPError as error:
            raise BarkPublishError("BARK_TRANSPORT_FAILED") from error
        try:
            document = response.json()
        except ValueError:
            raise BarkPublishError("BARK_RESPONSE_INVALID") from None
        if not response.is_success or not isinstance(document, dict) or document.get("code") != 200:
            raise BarkPublishError("BARK_RESPONSE_INVALID")
