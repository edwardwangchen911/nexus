"""Batch notifications retain frozen identities and only render bounded safe prose."""

import base64
import json
from dataclasses import replace
from datetime import UTC, datetime
from uuid import UUID

import httpx
import pytest
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from nexus.notifications.bark import BarkPublisher, BarkPublishError, map_bark_notification
from nexus.notifications.research_monitor_batch import ResearchMonitorBatchNotification
from nexus.notifications.worker import build_bark_registry
from nexus.outbox.models import OutboxMessage

EVENT = ("research.monitor.batch_triggered", "research_monitor_batch_v1")
KEY = b"0123456789abcdef"
IV = b"abcdefghijklmnop"


def message(**changes):
    payload = {
        "schema_version": EVENT[1],
        "batch_id": str(UUID(int=1)),
        "instrument_id": str(UUID(int=2)),
        "trigger_ids": [str(UUID(int=3)), str(UUID(int=4))],
        "event_ids": [f"price:{UUID(int=8)}", "sec:0000320193:0000320193-26-000001"],
        "source_run_ids": [str(UUID(int=5)), str(UUID(int=6))],
        "occurred_at": "2026-10-24T13:30:00Z",
        **changes,
    }
    return OutboxMessage(UUID(int=7), EVENT[0], UUID(int=1), EVENT[1], payload)


def test_batch_uses_fixed_summary_and_exact_source_link_without_mutating_frozen_ids():
    event = message()
    before = json.dumps(event.payload, sort_keys=True)
    result = map_bark_notification(event, read_only_url="https://nexus.local:8443/paper")
    assert result == {
        "title": "[研究复核] 监控批次已触发",
        "body": "本批次记录 2 个监控事件，关联 2 份来源研究。\n"
        "新增收录不等于新发布，不代表重要性或影响判断。\n"
        "触发时研究待准入；用量未知时预算不自动释放。\n"
        "通知服务接受不等于设备送达。\n"
        "触发时间：2026-10-24 13:30:00 UTC",
        "group": "NEXUS",
        "id": f"research-monitor-batch:{UUID(int=1)}",
        "message_id": str(event.event_id),
        "url": f"https://nexus.local:8443/research/{UUID(int=2)}?run_id={UUID(int=5)}",
    }
    assert json.dumps(event.payload, sort_keys=True) == before
    assert result == map_bark_notification(event, read_only_url="https://nexus.local:8443/paper")
    assert "url" not in map_bark_notification(event, read_only_url=None)


def test_batch_is_registered_only_at_the_exact_version():
    async def publisher(_message):
        pass

    registry = build_bark_registry(publisher)
    assert registry.resolve(*EVENT) is not None
    assert registry.resolve(*EVENT).consumer_idempotent is True
    assert registry.resolve(EVENT[0], "research_monitor_batch_v2") is None
    assert registry.resolve("research.completed", EVENT[1]) is None


@pytest.mark.parametrize(
    "changes",
    [
        {"batch_id": str(UUID(int=99))},
        {"instrument_id": "https://private.invalid"},
        {"schema_version": "research_monitor_batch_v2"},
        {"trigger_ids": []},
        {"event_ids": []},
        {"source_run_ids": []},
        {"trigger_ids": [str(UUID(int=3))]},
        {"event_ids": [f"price:{UUID(int=8)}"]},
        {"source_run_ids": [str(UUID(int=5)), str(UUID(int=6)), str(UUID(int=9))]},
        {"trigger_ids": [str(UUID(int=4)), str(UUID(int=3))]},
        {"trigger_ids": [str(UUID(int=3)), str(UUID(int=3))]},
        {"event_ids": ["sec:0000320193:0000320193-26-000001", f"price:{UUID(int=8)}"]},
        {"event_ids": [f"price:{UUID(int=8)}", f"price:{UUID(int=8)}"]},
        {"source_run_ids": [str(UUID(int=6)), str(UUID(int=5))]},
        {"source_run_ids": [str(UUID(int=5)), str(UUID(int=5))]},
        {"event_ids": [f"price:{UUID(int=8)}", "sec:320193:0000320193-26-000001"]},
        {"event_ids": [f"price:{UUID(int=8)}", "sec:0000320193:000032019326000001"]},
        {"event_ids": [f"price:{UUID(int=8)}", "https://private.invalid"]},
        {"event_ids": [f"price:{UUID(int=8)}", "sec:0000320193:0000320193-26-000001\n"]},
        {"event_ids": ["price:not-a-uuid", "sec:0000320193:0000320193-26-000001"]},
        {"event_ids": ["arbitrary-label", "private account detail"]},
        {"event_ids": {f"price:{UUID(int=8)}", f"price:{UUID(int=9)}"}},
        {"trigger_ids": {str(UUID(int=3)), str(UUID(int=4))}},
        {"source_run_ids": {str(UUID(int=5)), str(UUID(int=6))}},
        {"occurred_at": "2026-10-24T13:30:00"},
        {"occurred_at": "2026-10-24T21:30:00+08:00"},
        {"occurred_at": "2026-02-30T13:30:00Z"},
        {"occurred_at": "0"},
        {"occurred_at": 0},
        {"occurred_at": datetime(2026, 10, 24, tzinfo=UTC)},
        {"url": "https://private.invalid"},
        {"notification_delivery": "unavailable"},
        {"account_id": "private-account"},
        {"price": "9999.99"},
    ],
)
@pytest.mark.asyncio
async def test_invalid_batch_rejected_before_transport_with_fixed_safe_error(changes):
    requests = []

    async def transport(request):
        requests.append(request)
        return httpx.Response(200, json={"code": 200})

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        publisher = BarkPublisher(client, "http://localhost:8080", "fixture-device", KEY)
        with pytest.raises(BarkPublishError, match="^BARK_NOTIFICATION_INVALID$"):
            await publisher(message(**changes))
    assert requests == []


@pytest.mark.parametrize("field", list(message().payload))
def test_each_of_the_seven_frozen_fields_is_required(field):
    event = message()
    payload = dict(event.payload)
    del payload[field]
    with pytest.raises(BarkPublishError, match="^BARK_NOTIFICATION_INVALID$"):
        map_bark_notification(replace(event, payload=payload), read_only_url=None)


@pytest.mark.parametrize("count", [1, 1001])
def test_batch_summary_remains_bounded_at_both_allowed_limits(count):
    triggers = tuple(str(UUID(int=number + 10)) for number in range(count))
    events = tuple(f"price:{UUID(int=number + 10)}" for number in range(count))
    event = message(trigger_ids=triggers, event_ids=events, source_run_ids=[str(UUID(int=5))])
    result = map_bark_notification(event, read_only_url=None)
    assert f"{count} 个监控事件" in result["body"]
    assert len(result["body"]) < 250
    assert all(item not in result["body"] for item in events)
    assert event.payload["trigger_ids"] == triggers and event.payload["event_ids"] == events


def test_batch_above_limit_is_rejected_without_truncating():
    values = tuple(str(UUID(int=number + 10)) for number in range(1002))
    event = message(trigger_ids=values, event_ids=[f"price:{item}" for item in values])
    with pytest.raises(BarkPublishError, match="^BARK_NOTIFICATION_INVALID$"):
        map_bark_notification(event, read_only_url=None)
    assert len(event.payload["trigger_ids"]) == 1002


def test_validated_snapshot_keeps_all_frozen_events_at_the_limit():
    triggers = [str(UUID(int=number + 10)) for number in range(1001)]
    events = [f"price:{UUID(int=number + 10)}" for number in range(1000)]
    events.append("sec:0000320193:0000320193-26-000001")
    event = message(trigger_ids=triggers, event_ids=events)
    snapshot = ResearchMonitorBatchNotification.model_validate(event.payload)
    assert snapshot.model_dump(mode="json") == event.payload


@pytest.mark.parametrize(
    "url",
    [
        "https://nexus.local/orders",
        "https://nexus.local/paper?redirect=https://private.invalid",
        "https://user:password@nexus.local/paper",
        "file:///paper",
    ],
)
def test_batch_cannot_expand_the_configured_read_only_link_boundary(url):
    with pytest.raises(ValueError, match="^Bark link must target the read-only Paper page$"):
        map_bark_notification(message(), read_only_url=url)


@pytest.mark.parametrize("timestamp", ["2026-10-24T13:30:00Z", "2026-10-24T13:30:00.123456+00:00"])
def test_utc_string_is_accepted_without_inferring_another_time_zone(timestamp):
    assert (
        "13:30:00 UTC"
        in map_bark_notification(message(occurred_at=timestamp), read_only_url=None)["body"]
    )


def decrypt(document):
    decryptor = Cipher(algorithms.AES(KEY), modes.CBC(document["iv"].encode())).decryptor()
    padded = decryptor.update(base64.b64decode(document["ciphertext"])) + decryptor.finalize()
    unpadder = padding.PKCS7(algorithms.AES.block_size).unpadder()
    return json.loads(unpadder.update(padded) + unpadder.finalize())


@pytest.mark.asyncio
async def test_native_message_id_is_the_stable_outbox_identity_inside_encrypted_payload():
    requests = []

    async def transport(request):
        assert str(request.url) == "http://localhost:8080/push"
        requests.append(json.loads(request.content))
        return httpx.Response(200, json={"code": 200})

    event = message()
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        publisher = BarkPublisher(
            client, "http://localhost:8080", "fixture-device", KEY, iv_factory=lambda: IV
        )
        await publisher(event)
        await publisher(event)
        await publisher(replace(event, event_id=UUID(int=99)))
    assert len(requests) == 3
    assert set(requests[0]) == {"device_key", "ciphertext", "iv"}
    assert (
        decrypt(requests[0])["message_id"]
        == decrypt(requests[1])["message_id"]
        == str(event.event_id)
    )
    assert decrypt(requests[2])["message_id"] == str(UUID(int=99))


@pytest.mark.parametrize(
    "cause,description",
    [
        ("initial_match", "首次观察到价格复核条件已满足"),
        ("crossed", "价格复核条件由不满足转为满足"),
    ],
)
def test_legacy_price_trigger_mapping_is_byte_for_byte_unchanged(cause, description):
    payload = {
        "schema_version": "research_price_trigger_v1",
        "trigger_id": str(UUID(int=1)),
        "plan_id": str(UUID(int=2)),
        "source_run_id": str(UUID(int=3)),
        "instrument_id": str(UUID(int=4)),
        "cause": cause,
        "occurred_at": "2026-10-23T15:00:00+00:00",
    }
    event = OutboxMessage(
        UUID(int=5), "research.monitor.triggered", UUID(int=1), "research_price_trigger_v1", payload
    )
    expected = {
        "title": "[研究复核] 价格条件满足",
        "body": f"{description}。\n触发时研究待准入，待后续复核。\n"
        "触发时间：2026-10-23 15:00:00 UTC",
        "group": "NEXUS",
        "id": f"research-monitor:{UUID(int=1)}",
        "url": f"https://nexus.local/research/{UUID(int=4)}?run_id={UUID(int=3)}",
    }
    actual = map_bark_notification(event, read_only_url="https://nexus.local/paper")
    assert (
        json.dumps(actual, ensure_ascii=False, sort_keys=True).encode()
        == json.dumps(expected, ensure_ascii=False, sort_keys=True).encode()
    )
