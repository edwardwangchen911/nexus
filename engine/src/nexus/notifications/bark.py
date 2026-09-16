"""Desensitized, encrypted Bark publisher for persisted Paper outbox events."""

from __future__ import annotations

import base64
import json
import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from urllib.parse import urlsplit

import httpx
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes

from nexus.outbox.models import OutboxMessage
from nexus.paper.notification import (
    PaperNotificationError,
    PaperOrderNotificationSnapshot,
    parse_notification_snapshot,
)

BARK_EVENT_SCHEMAS: tuple[tuple[str, str], ...] = (
    ("paper.order.risk_rejected", "paper_event_v1"),
    ("paper.execution.advance.filled", "paper_execution_v1"),
    ("paper.execution.advance.cancelled", "paper_execution_v1"),
    ("paper.execution.cancelled", "paper_execution_v1"),
)

_EVENT_STATUS: dict[tuple[str, str], str] = {
    ("paper.order.risk_rejected", "paper_event_v1"): "risk_rejected",
    ("paper.execution.advance.filled", "paper_execution_v1"): "filled",
    ("paper.execution.advance.cancelled", "paper_execution_v1"): "cancelled",
    ("paper.execution.cancelled", "paper_execution_v1"): "cancelled",
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
    """Render one validated event-time order outcome without later data lookups."""
    expected_status = _EVENT_STATUS.get((message.event_type, message.schema_version))
    if expected_status is None:
        raise BarkPublishError("BARK_EVENT_UNSUPPORTED")
    try:
        snapshot = parse_notification_snapshot(message.payload)
    except PaperNotificationError:
        raise BarkPublishError("BARK_NOTIFICATION_INVALID") from None
    if snapshot.status != expected_status or snapshot.order_id != message.aggregate_id:
        raise BarkPublishError("BARK_NOTIFICATION_INVALID")
    title, body = _render_order_outcome(snapshot)
    notification = {
        "title": title,
        "body": body,
        "group": "NEXUS",
        "id": str(snapshot.order_id),
    }
    if read_only_url is not None:
        validate_bark_read_only_url(read_only_url)
        notification["url"] = read_only_url
    return notification


def _quantity(value: Decimal) -> str:
    return str(int(value))


def _money(value: Decimal, currency: str) -> str:
    rendered = f"{value:f}"
    whole, _, fraction = rendered.partition(".")
    fraction = fraction.rstrip("0")
    if len(fraction) < 2:
        fraction = fraction.ljust(2, "0")
    grouped = f"{int(whole):,}.{fraction}"
    return f"${grouped}" if currency == "USD" else f"{grouped} {currency}"


def _render_order_outcome(snapshot: PaperOrderNotificationSnapshot) -> tuple[str, str]:
    side = {"buy": "买入", "sell": "卖出"}[snapshot.side]
    currency = snapshot.price.currency
    agent_reason = snapshot.agent_reason or "暂不可用"
    invalidation = snapshot.invalidation or "暂不可用"
    account = (
        f"{snapshot.symbol} {_quantity(snapshot.position.quantity)} 股｜"
        f"可用现金 {_money(snapshot.cash.available, currency)}"
    )

    if snapshot.status == "filled":
        assert snapshot.fee is not None
        title = (
            f"[内部模拟] {snapshot.symbol} {side} {_quantity(snapshot.filled_quantity)} 股 · 已成交"
        )
        body = "\n".join(
            (
                f"成交均价：{_money(snapshot.price.value, currency)}｜"
                f"成交额 {_money(snapshot.notional, currency)}｜"
                f"费用 {_money(snapshot.fee, currency)}",
                f"成交后：{account}",
                f"Agent：{agent_reason}",
                f"失效：{invalidation}",
            )
        )
        return title, body

    if snapshot.status == "risk_rejected":
        price_label = "参考价" if snapshot.price.kind == "reference" else "最坏价格"
        title = (
            f"[内部模拟] {snapshot.symbol} {side} "
            f"{_quantity(snapshot.requested_quantity)} 股 · 风控拒绝"
        )
        body = "\n".join(
            (
                f"{price_label}：{_money(snapshot.price.value, currency)}｜"
                f"计划金额 {_money(snapshot.notional, currency)}",
                f"原因：{snapshot.risk_reason or '暂不可用'}",
                f"账户未变化：{account}",
                f"Agent：{agent_reason}",
                f"失效：{invalidation}",
            )
        )
        return title, body

    title = (
        f"[内部模拟] {snapshot.symbol} {side} {_quantity(snapshot.approved_quantity)} 股 · 已取消"
    )
    if snapshot.filled_quantity > 0:
        assert snapshot.fee is not None
        price_line = (
            f"成交均价：{_money(snapshot.price.value, currency)}｜"
            f"成交额 {_money(snapshot.notional, currency)}｜"
            f"费用 {_money(snapshot.fee, currency)}"
        )
    else:
        price_label = "参考价" if snapshot.price.kind == "reference" else "最坏价格"
        price_line = f"{price_label}：{_money(snapshot.price.value, currency)}｜未成交"
    body = "\n".join(
        (
            f"执行结果：已成交 {_quantity(snapshot.filled_quantity)} 股｜"
            f"未成交 {_quantity(snapshot.remaining_quantity)} 股",
            price_line,
            f"取消后：{account}",
            f"原因：{snapshot.risk_reason or '暂不可用'}",
            f"Agent：{agent_reason}",
            f"失效：{invalidation}",
        )
    )
    return title, body


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
        if message.payload.get("notification_delivery") == "unavailable":
            if "notification_snapshot" in message.payload:
                raise BarkPublishError("BARK_NOTIFICATION_INVALID")
            return
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
