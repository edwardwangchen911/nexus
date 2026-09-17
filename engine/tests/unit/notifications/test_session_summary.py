"""Synthetic summary facts; exercise the real mapper and encrypted publisher."""

import base64
import json
from dataclasses import replace
from uuid import UUID

import httpx
import pytest
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from nexus.notifications.bark import BarkPublisher, BarkPublishError, map_bark_notification
from nexus.outbox.models import OutboxMessage

ACCOUNT = UUID("10000000-0000-0000-0000-000000000001")


def summary_message(**changes: object) -> OutboxMessage:
    snapshot = {
        "schema_version": "paper_session_summary_v1",
        "mode": "internal_paper",
        "account_id": str(ACCOUNT),
        "market": "XNYS",
        "session_date": "2026-08-28",
        "as_of": "2026-08-28T20:00:00Z",
        "frozen_at": "2026-08-28T20:05:00Z",
        "currency": "USD",
        "cycle_count": 4,
        "completed_count": 2,
        "unavailable_count": 1,
        "pending_count": 1,
        "no_order_count": 1,
        "order_count": 2,
        "fill_count": 1,
        "risk_rejected_count": 1,
        "nav": "1010.25",
        "daily_pnl": "-0.25",
        "daily_return_ratio": "-0.000247402276",
        "daily_fees": "0.125",
        "drawdown_ratio": "0.015",
        "comparison_start": "2026-08-26",
        "hold_current_nav": "1000",
        "hold_current_return_ratio": "0",
        "spy_nav": None,
        "spy_return_ratio": None,
    }
    snapshot.update(changes)
    return OutboxMessage(
        event_id=UUID("10000000-0000-0000-0000-000000000002"),
        event_type="paper.session.summary.ready",
        aggregate_id=ACCOUNT,
        schema_version="paper_session_summary_v1",
        payload={"notification_snapshot": snapshot, "evidence": "private raw evidence"},
    )


def test_summary_renders_frozen_daily_facts_and_unknown_benchmark() -> None:
    # Removing summary dispatch, mislabelling cumulative P&L, or coercing unknown to zero breaks it.
    result = map_bark_notification(summary_message(), read_only_url=None)
    assert result["id"] == f"{ACCOUNT}:XNYS:2026-08-28"
    assert "内部模拟" in result["title"] and "2026-08-28" in result["title"]
    for fact in (
        "周期 4",
        "完成 2",
        "不可用 1",
        "未完成 1",
        "合理不交易 1",
        "订单 2",
        "成交 1",
        "风控拒绝 1",
        "$1,010.25",
        "-$0.25",
        "费用 $0.125",
        "回撤 1.5%",
        "hold_current（初始现金）",
        "SPY：暂不可用",
        "2026-08-26",
        "前一交易日净值",
    ):
        assert fact in result["body"]
    assert str(ACCOUNT) not in result["body"]
    assert "private raw evidence" not in json.dumps(result)


def test_summary_retry_uses_session_identity_instead_of_event_identity() -> None:
    original = summary_message()
    retry = replace(original, event_id=UUID("10000000-0000-0000-0000-000000000003"))
    assert map_bark_notification(original, read_only_url=None) == map_bark_notification(
        retry, read_only_url=None
    )
    tomorrow = summary_message(
        session_date="2026-08-31", as_of="2026-08-31T20:00:00Z", frozen_at="2026-08-31T20:05:00Z"
    )
    assert (
        map_bark_notification(tomorrow, read_only_url=None)["id"]
        != (map_bark_notification(original, read_only_url=None)["id"])
    )


def test_summary_missing_previous_close_is_unknown_not_zero() -> None:
    result = map_bark_notification(
        summary_message(daily_pnl=None, daily_return_ratio=None), read_only_url=None
    )
    assert "当日盈亏：暂不可用" in result["body"]
    assert "当日收益率：暂不可用" in result["body"]


def test_summary_distinguishes_close_and_frozen_runtime_timestamps() -> None:
    result = map_bark_notification(
        summary_message(frozen_at="2026-08-28T20:05:00Z"), read_only_url=None
    )
    assert "绩效截至收盘 20:00 UTC" in result["body"]
    assert "运行状态截至 2026-08-28 20:05 UTC" in result["body"]


@pytest.mark.parametrize(("value", "want"), [("1", "100%"), ("0.1", "10%"), ("-0.1", "-10%")])
def test_summary_keeps_integer_percentage_magnitude(value: str, want: str) -> None:
    result = map_bark_notification(
        summary_message(spy_nav="1100", spy_return_ratio=value), read_only_url=None
    )
    assert f"SPY：$1,100.00｜{want}" in result["body"]


@pytest.mark.parametrize(
    "changes",
    [
        {"prompt": "secret"},
        {"nav": 1000.5},
        {"market": "secret"},
        {"cycle_count": 0},
        {"completed_count": 5},
        {"no_order_count": 3},
        {"daily_pnl": "NaN"},
        {"as_of": "2026-08-28T20:00:00"},
        {"nav": "0"},
        {"daily_fees": "-1"},
        {"drawdown_ratio": "1.1"},
        {"frozen_at": "2026-08-28T19:59:59Z"},
        {"session_date": "2026-08-27"},
    ],
)
def test_summary_rejects_invalid_closed_contract_without_leaking(changes: dict) -> None:
    with pytest.raises(BarkPublishError, match="^BARK_NOTIFICATION_INVALID$"):
        map_bark_notification(summary_message(**changes), read_only_url=None)


@pytest.mark.asyncio
async def test_summary_transport_is_encrypted_and_contains_only_notification_fields() -> None:
    requests = []

    def accept(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, json={"code": 200})

    async with httpx.AsyncClient(transport=httpx.MockTransport(accept)) as client:
        await BarkPublisher(
            client,
            "http://127.0.0.1:8080",
            "fixture-device",
            b"0123456789abcdef",
            iv_factory=lambda: b"abcdefghijklmnop",
        ).publish(summary_message())
    wire = json.loads(requests[0].content)
    assert set(wire) == {"device_key", "ciphertext", "iv"}
    assert b"1010.25" not in requests[0].content
    decryptor = Cipher(
        algorithms.AES(b"0123456789abcdef"), modes.CBC(wire["iv"].encode())
    ).decryptor()
    padded = decryptor.update(base64.b64decode(wire["ciphertext"])) + decryptor.finalize()
    unpadder = padding.PKCS7(128).unpadder()
    notification = json.loads(unpadder.update(padded) + unpadder.finalize())
    assert set(notification) == {"title", "body", "group", "id"}
    assert notification["id"] == f"{ACCOUNT}:XNYS:2026-08-28"
    assert str(ACCOUNT) not in notification["body"]
    assert "private raw evidence" not in str(notification)
