import base64
import json
from dataclasses import replace
from uuid import UUID

import httpx
import pytest
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from nexus.notifications.bark import (
    BARK_EVENT_SCHEMAS,
    BarkPublisher,
    BarkPublishError,
    map_bark_notification,
)
from nexus.outbox.models import OutboxMessage

_EVENT_ID = UUID("0199d500-1000-7000-8000-000000000001")
_ORDER_ID = UUID("0199d500-1000-7000-8000-000000000003")
_KEY = b"0123456789abcdef"
_IV = b"abcdefghijklmnop"
_SENSITIVE_VALUES = (
    "account-secret-123",
    "999999.99",
    "evidence body must stay local",
    "executable-token-value",
)


def _message(
    event_type: str = "paper.execution.advance.filled",
    schema_version: str = "paper_execution_v1",
    *,
    status: str = "filled",
) -> OutboxMessage:
    if status == "risk_rejected":
        snapshot = {
            "schema_version": "paper_notification_v1",
            "mode": "internal_paper",
            "order_id": str(_ORDER_ID),
            "agent_cycle_id": None,
            "occurred_at": "2026-09-16T02:15:00Z",
            "symbol": "AAPL",
            "side": "buy",
            "status": "risk_rejected",
            "requested_quantity": "20",
            "approved_quantity": "0",
            "filled_quantity": "0",
            "remaining_quantity": "0",
            "price": {"kind": "reference", "value": "187", "currency": "USD"},
            "notional": "3740",
            "fee": None,
            "cash": {"kind": "unchanged", "available": "23654.50"},
            "position": {
                "kind": "unchanged",
                "quantity": "30",
                "portfolio_ratio": "0.18",
            },
            "agent_reason": "盈利预期上修，但确定性仓位限制不允许本次增仓。",
            "invalidation": None,
            "risk_reason": "成交后单一标的仓位将超过 20% 上限。",
        }
    elif status == "cancelled":
        snapshot = {
            "schema_version": "paper_notification_v1",
            "mode": "internal_paper",
            "order_id": str(_ORDER_ID),
            "agent_cycle_id": None,
            "occurred_at": "2026-09-16T02:15:00Z",
            "symbol": "AAPL",
            "side": "sell",
            "status": "cancelled",
            "requested_quantity": "12",
            "approved_quantity": "12",
            "filled_quantity": "5",
            "remaining_quantity": "7",
            "price": {"kind": "average_fill", "value": "188.125", "currency": "USD"},
            "notional": "940.625",
            "fee": "0.25",
            "cash": {"kind": "actual_after", "available": "24595.125"},
            "position": {
                "kind": "actual_after",
                "quantity": "25",
                "portfolio_ratio": None,
            },
            "agent_reason": None,
            "invalidation": None,
            "risk_reason": None,
        }
    else:
        snapshot = {
            "schema_version": "paper_notification_v1",
            "mode": "internal_paper",
            "order_id": str(_ORDER_ID),
            "agent_cycle_id": None,
            "occurred_at": "2026-09-16T02:15:00Z",
            "symbol": "AAPL",
            "side": "buy",
            "status": "filled",
            "requested_quantity": "12",
            "approved_quantity": "12",
            "filled_quantity": "12",
            "remaining_quantity": "0",
            "price": {"kind": "average_fill", "value": "187.36", "currency": "USD"},
            "notional": "2248.32",
            "fee": "0",
            "cash": {"kind": "actual_after", "available": "21406.18"},
            "position": {
                "kind": "actual_after",
                "quantity": "42",
                "portfolio_ratio": None,
            },
            "agent_reason": "盈利预期上修，价格突破近期区间。",
            "invalidation": "价格跌破既定阈值。",
            "risk_reason": None,
        }
    return OutboxMessage(
        event_id=_EVENT_ID,
        event_type=event_type,
        aggregate_id=_ORDER_ID,
        schema_version=schema_version,
        payload={
            "notification_snapshot": snapshot,
            "account_id": _SENSITIVE_VALUES[0],
            "amount": _SENSITIVE_VALUES[1],
            "evidence": _SENSITIVE_VALUES[2],
            "token": _SENSITIVE_VALUES[3],
        },
    )


def test_filled_notification_renders_actionable_frozen_facts() -> None:
    notification = map_bark_notification(
        _message(),
        read_only_url="http://nexus.local/paper",
    )

    rendered = json.dumps(notification, ensure_ascii=False)
    assert all(value not in rendered for value in _SENSITIVE_VALUES)
    assert notification["url"] == "http://nexus.local/paper"
    assert notification["id"] == str(_ORDER_ID)
    assert notification["title"] == "[内部模拟] AAPL 买入 12 股 · 已成交"
    assert notification["body"] == (
        "成交均价：$187.36｜成交额 $2,248.32｜费用 $0.00\n"
        "成交后：AAPL 42 股｜可用现金 $21,406.18\n"
        "Agent：盈利预期上修，价格突破近期区间。\n"
        "失效：价格跌破既定阈值。"
    )


def test_risk_rejected_notification_distinguishes_unchanged_account() -> None:
    notification = map_bark_notification(
        _message(
            "paper.order.risk_rejected",
            "paper_event_v1",
            status="risk_rejected",
        ),
        read_only_url=None,
    )

    assert notification["id"] == str(_ORDER_ID)
    assert notification["title"] == "[内部模拟] AAPL 买入 20 股 · 风控拒绝"
    assert notification["body"] == (
        "参考价：$187.00｜计划金额 $3,740.00\n"
        "原因：成交后单一标的仓位将超过 20% 上限。\n"
        "账户未变化：AAPL 30 股｜可用现金 $23,654.50\n"
        "Agent：盈利预期上修，但确定性仓位限制不允许本次增仓。\n"
        "失效：暂不可用"
    )


def test_first_delivery_consumes_terminal_outcomes_and_session_summary() -> None:
    assert BARK_EVENT_SCHEMAS == (
        ("paper.session.summary.ready", "paper_session_summary_v1"),
        ("paper.order.risk_rejected", "paper_event_v1"),
        ("paper.execution.advance.filled", "paper_execution_v1"),
        ("paper.execution.advance.cancelled", "paper_execution_v1"),
        ("paper.execution.cancelled", "paper_execution_v1"),
        ("research.monitor.triggered", "research_price_trigger_v1"),
        ("research.monitor.batch_triggered", "research_monitor_batch_v1"),
        ("diagnostic.notification.test", "diagnostic_notification_v1"),
    )


def test_cancelled_notification_reports_filled_and_unfilled_quantities() -> None:
    notification = map_bark_notification(
        _message(
            "paper.execution.advance.cancelled",
            "paper_execution_v1",
            status="cancelled",
        ),
        read_only_url=None,
    )

    assert notification["title"] == "[内部模拟] AAPL 卖出 12 股 · 已取消"
    assert notification["body"] == (
        "执行结果：已成交 5 股｜未成交 7 股\n"
        "成交均价：$188.125｜成交额 $940.625｜费用 $0.25\n"
        "取消后：AAPL 25 股｜可用现金 $24,595.125\n"
        "原因：暂不可用\n"
        "Agent：暂不可用\n"
        "失效：暂不可用"
    )


def test_notification_mapping_rejects_unregistered_event() -> None:
    with pytest.raises(BarkPublishError, match="^BARK_EVENT_UNSUPPORTED$"):
        map_bark_notification(
            _message("strategy_decision.persisted", "paper_event_v1"),
            read_only_url=None,
        )


def test_notification_mapping_rejects_missing_or_invalid_snapshot_without_values() -> None:
    message = _message()
    message.payload.clear()

    with pytest.raises(BarkPublishError, match="^BARK_NOTIFICATION_INVALID$") as caught:
        map_bark_notification(message, read_only_url=None)

    assert "AAPL" not in str(caught.value)


def test_notification_mapping_rejects_snapshot_for_another_order() -> None:
    message = replace(
        _message(),
        aggregate_id=UUID("0199d500-1000-7000-8000-000000000099"),
    )

    with pytest.raises(BarkPublishError, match="^BARK_NOTIFICATION_INVALID$"):
        map_bark_notification(message, read_only_url=None)


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


@pytest.mark.asyncio
async def test_publisher_acknowledges_explicitly_unavailable_legacy_notification() -> None:
    requests: list[httpx.Request] = []

    async def endpoint(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"code": 200})

    message = _message()
    message.payload.clear()
    message.payload["notification_delivery"] = "unavailable"
    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as client:
        publisher = BarkPublisher(
            client=client,
            server_url="http://127.0.0.1:8080",
            device_key="device-key",
            encryption_key=_KEY,
        )
        await publisher.publish(message)

    assert requests == []


@pytest.mark.asyncio
async def test_publisher_acknowledges_unmarked_legacy_notification_without_transport() -> None:
    requests: list[httpx.Request] = []

    async def endpoint(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"code": 200})

    message = _message()
    message.payload.clear()
    message.payload["legacy_order_id"] = "not-a-notification-snapshot"
    async with httpx.AsyncClient(transport=httpx.MockTransport(endpoint)) as client:
        publisher = BarkPublisher(
            client=client,
            server_url="http://127.0.0.1:8080",
            device_key="device-key",
            encryption_key=_KEY,
        )
        await publisher.publish(message)

    assert requests == []


@pytest.mark.asyncio
async def test_publisher_rejects_malformed_new_notification_snapshot() -> None:
    message = _message()
    message.payload["notification_snapshot"] = {"status": "filled"}

    async with httpx.AsyncClient() as client:
        publisher = BarkPublisher(
            client=client,
            server_url="http://127.0.0.1:8080",
            device_key="device-key",
            encryption_key=_KEY,
        )
        with pytest.raises(BarkPublishError, match="^BARK_NOTIFICATION_INVALID$"):
            await publisher.publish(message)


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
    assert inner["id"] == str(_ORDER_ID)
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


def _diagnostic_message() -> OutboxMessage:
    return OutboxMessage(
        event_id=_EVENT_ID,
        aggregate_id=_EVENT_ID,
        event_type="diagnostic.notification.test",
        schema_version="diagnostic_notification_v1",
        payload={
            "schema_version": "diagnostic_notification_v1",
            "test_id": str(_EVENT_ID),
            "occurred_at": "2026-10-27T01:00:00Z",
        },
    )


def test_diagnostic_notice_is_fixed_non_business_and_registered() -> None:
    notice = map_bark_notification(_diagnostic_message(), read_only_url="http://nexus.local/paper")
    assert notice["title"] == "NEXUS 测试通知｜非投资预警"
    assert notice["body"].startswith("NEXUS 测试通知｜非投资预警")
    assert "url" not in notice
    assert notice["id"] == f"diagnostic-test:{_EVENT_ID}"
    assert notice["message_id"] == str(_EVENT_ID)
    assert ("diagnostic.notification.test", "diagnostic_notification_v1") in BARK_EVENT_SCHEMAS


@pytest.mark.parametrize(
    "change",
    [
        {"body": "free-form content"},
        {"test_id": str(_ORDER_ID)},
        {"occurred_at": "2026-10-27T01:00:00"},
        {"occurred_at": 1793062800},
        {"schema_version": "paper_event_v1"},
    ],
)
def test_diagnostic_notice_rejects_payload_drift(change: dict[str, object]) -> None:
    original = _diagnostic_message()
    message = replace(original, payload={**original.payload, **change})
    with pytest.raises(BarkPublishError, match="BARK_NOTIFICATION_INVALID"):
        map_bark_notification(message, read_only_url=None)


@pytest.mark.asyncio
async def test_diagnostic_uses_encrypted_publisher_without_paper_snapshot() -> None:
    calls = []

    async def accepted(request: httpx.Request) -> httpx.Response:
        calls.append(json.loads(request.content))
        return httpx.Response(200, json={"code": 200})

    async with httpx.AsyncClient(transport=httpx.MockTransport(accepted)) as client:
        publisher = BarkPublisher(
            client=client,
            server_url="http://127.0.0.1:8080",
            device_key="test-device",
            encryption_key=_KEY,
            iv_factory=lambda: _IV,
        )
        await publisher.publish(_diagnostic_message())
    assert len(calls) == 1
    assert set(calls[0]) == {"device_key", "ciphertext", "iv"}
    decryptor = Cipher(algorithms.AES(_KEY), modes.CBC(_IV)).decryptor()
    padded = decryptor.update(base64.b64decode(calls[0]["ciphertext"])) + decryptor.finalize()
    unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
    rendered = json.loads(unpadder.update(padded) + unpadder.finalize())
    assert rendered["title"] == "NEXUS 测试通知｜非投资预警"
    assert rendered["id"] == f"diagnostic-test:{_EVENT_ID}"
