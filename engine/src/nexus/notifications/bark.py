"""Desensitized, encrypted Bark publisher for persisted notification facts."""

from __future__ import annotations

import base64
import json
import secrets
from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from urllib.parse import urlsplit, urlunsplit

import httpx
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from pydantic import ValidationError

from nexus.notifications.diagnostic import DIAGNOSTIC_EVENT, DiagnosticNotification
from nexus.notifications.research_monitor import (
    RESEARCH_MONITOR_EVENT,
    ResearchPriceTriggerNotification,
)
from nexus.notifications.research_monitor_batch import (
    RESEARCH_MONITOR_BATCH_EVENT,
    ResearchMonitorBatchNotification,
)
from nexus.outbox.failure import FailureReason, PublisherFailure
from nexus.outbox.models import OutboxMessage
from nexus.paper.notification import (
    PaperNotificationError,
    PaperOrderNotificationSnapshot,
    parse_notification_snapshot,
)
from nexus.paper.session_summary import PaperSessionSummarySnapshot

BARK_EVENT_SCHEMAS: tuple[tuple[str, str], ...] = (
    ("paper.session.summary.ready", "paper_session_summary_v1"),
    ("paper.order.risk_rejected", "paper_event_v1"),
    ("paper.execution.advance.filled", "paper_execution_v1"),
    ("paper.execution.advance.cancelled", "paper_execution_v1"),
    ("paper.execution.cancelled", "paper_execution_v1"),
    RESEARCH_MONITOR_EVENT,
    RESEARCH_MONITOR_BATCH_EVENT,
    DIAGNOSTIC_EVENT,
)

_EVENT_STATUS: dict[tuple[str, str], str] = {
    ("paper.order.risk_rejected", "paper_event_v1"): "risk_rejected",
    ("paper.execution.advance.filled", "paper_execution_v1"): "filled",
    ("paper.execution.advance.cancelled", "paper_execution_v1"): "cancelled",
    ("paper.execution.cancelled", "paper_execution_v1"): "cancelled",
}


class BarkPublishError(PublisherFailure):
    """A value-free Bark delivery failure suitable for bounded outbox recording."""

    def __init__(self, message: str, *, reason: FailureReason | None = None) -> None:
        known = {
            "BARK_NOTIFICATION_INVALID": FailureReason.PAYLOAD_INVALID,
            "BARK_EVENT_UNSUPPORTED": FailureReason.EVENT_UNSUPPORTED,
            "BARK_ENCRYPTION_CONFIGURATION_INVALID": FailureReason.ENCRYPTION_INVALID,
        }
        super().__init__(message, reason=reason or known.get(message, FailureReason.UNKNOWN))


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
    """Render one validated frozen notification without later data lookups."""
    if (message.event_type, message.schema_version) == DIAGNOSTIC_EVENT:
        try:
            diagnostic = DiagnosticNotification.model_validate(message.payload)
        except (ValueError, TypeError, ValidationError):
            raise BarkPublishError("BARK_NOTIFICATION_INVALID") from None
        if diagnostic.test_id != message.aggregate_id or diagnostic.test_id != message.event_id:
            raise BarkPublishError("BARK_NOTIFICATION_INVALID")
        return diagnostic.render()
    if (message.event_type, message.schema_version) == RESEARCH_MONITOR_BATCH_EVENT:
        try:
            batch = ResearchMonitorBatchNotification.model_validate(message.payload)
        except (ValueError, TypeError, ValidationError):
            raise BarkPublishError("BARK_NOTIFICATION_INVALID") from None
        if batch.batch_id != message.aggregate_id:
            raise BarkPublishError("BARK_NOTIFICATION_INVALID")
        title, body = batch.render()
        notification = {
            "title": title,
            "body": body,
            "group": "NEXUS",
            "id": f"research-monitor-batch:{batch.batch_id}",
            "message_id": str(message.event_id),
        }
        if read_only_url is not None:
            validate_bark_read_only_url(read_only_url)
            origin = urlsplit(read_only_url)
            notification["url"] = urlunsplit(
                (
                    origin.scheme,
                    origin.netloc,
                    f"/research/{batch.instrument_id}",
                    f"run_id={batch.source_run_ids[0]}",
                    "",
                )
            )
        return notification
    if (message.event_type, message.schema_version) == RESEARCH_MONITOR_EVENT:
        try:
            trigger = ResearchPriceTriggerNotification.model_validate(message.payload)
        except (ValueError, TypeError, ValidationError):
            raise BarkPublishError("BARK_NOTIFICATION_INVALID") from None
        if trigger.trigger_id != message.aggregate_id:
            raise BarkPublishError("BARK_NOTIFICATION_INVALID")
        title, body = trigger.render()
        notification = {
            "title": title,
            "body": body,
            "group": "NEXUS",
            "id": f"research-monitor:{trigger.trigger_id}",
        }
        if read_only_url is not None:
            validate_bark_read_only_url(read_only_url)
            origin = urlsplit(read_only_url)
            notification["url"] = urlunsplit(
                (
                    origin.scheme,
                    origin.netloc,
                    f"/research/{trigger.instrument_id}",
                    f"run_id={trigger.source_run_id}",
                    "",
                )
            )
        return notification
    if (message.event_type, message.schema_version) == (
        "paper.session.summary.ready",
        "paper_session_summary_v1",
    ):
        try:
            summary = PaperSessionSummarySnapshot.model_validate(
                message.payload.get("notification_snapshot")
            )
        except (ValueError, TypeError, ValidationError):
            raise BarkPublishError("BARK_NOTIFICATION_INVALID") from None
        if summary.account_id != message.aggregate_id:
            raise BarkPublishError("BARK_NOTIFICATION_INVALID")
        title, body = _render_session_summary(summary)
        notification = {
            "title": title,
            "body": body,
            "group": "NEXUS",
            "id": f"{summary.account_id}:{summary.market}:{summary.session_date}",
        }
        if read_only_url is not None:
            validate_bark_read_only_url(read_only_url)
            notification["url"] = read_only_url
        return notification
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


def _summary_money(value: Decimal | None, currency: str) -> str:
    if value is None:
        return "暂不可用"
    return ("-" if value < 0 else "") + _money(abs(value), currency)


def _summary_ratio(value: Decimal | None) -> str:
    if value is None:
        return "暂不可用"
    rendered = format(value * 100, "f")
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return f"{rendered}%"


def _render_session_summary(snapshot: PaperSessionSummarySnapshot) -> tuple[str, str]:
    def money(value: Decimal | None) -> str:
        return _summary_money(value, snapshot.currency)

    ratio = _summary_ratio
    return (
        f"[内部模拟] 日终简报 · {snapshot.session_date}",
        "\n".join(
            (
                f"XNYS 交易日 {snapshot.session_date}｜绩效截至收盘 {snapshot.as_of:%H:%M} UTC",
                f"运行状态截至 {snapshot.frozen_at:%Y-%m-%d %H:%M} UTC",
                f"周期 {snapshot.cycle_count}｜完成 {snapshot.completed_count}｜"
                f"不可用 {snapshot.unavailable_count}｜未完成 {snapshot.pending_count}",
                f"合理不交易 {snapshot.no_order_count}（完成中）｜订单 {snapshot.order_count}｜"
                f"成交 {snapshot.fill_count} 笔｜风控拒绝 {snapshot.risk_rejected_count}",
                f"组合净值 {money(snapshot.nav)}｜当日盈亏：{money(snapshot.daily_pnl)}",
                f"当日收益率：{ratio(snapshot.daily_return_ratio)}（分母：前一交易日净值）",
                f"当日费用 {money(snapshot.daily_fees)}｜收盘回撤 {ratio(snapshot.drawdown_ratio)}",
                f"累计对照（起点 {snapshot.comparison_start}，分母：初始资金）：",
                f"hold_current（初始现金）：{money(snapshot.hold_current_nav)}｜"
                f"{ratio(snapshot.hold_current_return_ratio)}",
                f"SPY：{money(snapshot.spy_nav)}｜{ratio(snapshot.spy_return_ratio)}",
            )
        ),
    )


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
        if (message.event_type, message.schema_version) not in {
            RESEARCH_MONITOR_EVENT,
            RESEARCH_MONITOR_BATCH_EVENT,
            DIAGNOSTIC_EVENT,
        }:
            if (
                "notification_snapshot" not in message.payload
                and "notification_delivery" not in message.payload
            ):
                return
            if message.payload.get("notification_delivery") == "unavailable":
                if "notification_snapshot" in message.payload:
                    raise BarkPublishError("BARK_NOTIFICATION_INVALID")
                return
        try:
            notification = map_bark_notification(message, read_only_url=self.read_only_url)
        except BarkPublishError:
            raise
        except Exception:
            raise BarkPublishError("BARK_NOTIFICATION_INVALID") from None
        try:
            iv = self.iv_factory()
            request = {
                "device_key": self.device_key,
                "ciphertext": _encrypt(notification, key=self.encryption_key, iv=iv),
                "iv": iv.decode("ascii"),
            }
        except BarkPublishError:
            raise
        except Exception:
            raise BarkPublishError(
                "BARK_ENCRYPTION_FAILED", reason=FailureReason.ENCRYPTION_FAILED
            ) from None
        try:
            response = await self.client.post(
                f"{self.server_url.rstrip('/')}/push",
                json=request,
            )
        except httpx.HTTPError as error:
            reasons = {
                httpx.ConnectTimeout: FailureReason.CONNECT_TIMEOUT,
                httpx.ReadTimeout: FailureReason.READ_TIMEOUT,
                httpx.WriteTimeout: FailureReason.WRITE_TIMEOUT,
                httpx.PoolTimeout: FailureReason.POOL_TIMEOUT,
            }
            reason = next(
                (value for kind, value in reasons.items() if isinstance(error, kind)),
                FailureReason.TRANSPORT_ERROR,
            )
            raise BarkPublishError("BARK_TRANSPORT_FAILED", reason=reason) from None
        except Exception:
            raise BarkPublishError(
                "BARK_TRANSPORT_FAILED", reason=FailureReason.TRANSPORT_ERROR
            ) from None
        if not response.is_success:
            raise BarkPublishError("BARK_RESPONSE_INVALID", reason=FailureReason.HTTP_REJECTED)
        try:
            document = response.json()
        except ValueError:
            raise BarkPublishError(
                "BARK_RESPONSE_INVALID", reason=FailureReason.RESPONSE_DECODE
            ) from None
        if not isinstance(document, dict) or document.get("code") != 200:
            raise BarkPublishError("BARK_RESPONSE_INVALID", reason=FailureReason.PROVIDER_REJECTED)
