"""Frozen price-review triggers notify without upgrading their research status."""

from dataclasses import replace
from uuid import UUID

import httpx
import pytest
from nexus.notifications.bark import BarkPublisher, BarkPublishError, map_bark_notification
from nexus.notifications.worker import build_bark_registry
from nexus.outbox.models import OutboxMessage

EVENT = ("research.monitor.triggered", "research_price_trigger_v1")


def message(**changes):
    payload = {
        "schema_version": EVENT[1],
        "trigger_id": str(UUID(int=1)),
        "plan_id": str(UUID(int=2)),
        "source_run_id": str(UUID(int=3)),
        "instrument_id": str(UUID(int=4)),
        "cause": "initial_match",
        "occurred_at": "2026-10-23T15:00:00+00:00",
        **changes,
    }
    return OutboxMessage(UUID(int=5), EVENT[0], UUID(int=1), EVENT[1], payload)


@pytest.mark.parametrize("cause", ["initial_match", "crossed"])
def test_trigger_maps_frozen_pending_semantics_and_exact_internal_research_link(cause):
    event = message(cause=cause)
    result = map_bark_notification(event, read_only_url="https://nexus.local:8443/paper")
    assert result["title"] == "[研究复核] 价格条件满足"
    assert "触发时研究待准入，待后续复核" in result["body"]
    assert "2026-10-23 15:00:00 UTC" in result["body"]
    assert not any(text in result["body"] for text in ("已下单", "观点失效", "已启动"))
    assert result["id"] == f"research-monitor:{UUID(int=1)}"
    assert result["url"] == f"https://nexus.local:8443/research/{UUID(int=4)}?run_id={UUID(int=3)}"
    assert result == map_bark_notification(event, read_only_url="https://nexus.local:8443/paper")
    assert "url" not in map_bark_notification(event, read_only_url=None)


@pytest.mark.parametrize(
    "changes",
    [
        {"trigger_id": str(UUID(int=99))},
        {"plan_id": "private-invalid-id"},
        {"source_run_id": "https://private.invalid"},
        {"instrument_id": None},
        {"schema_version": "research_price_trigger_v2"},
        {"cause": "thesis_invalidated"},
        {"occurred_at": "2026-10-23T15:00:00"},
        {"occurred_at": 0},
        {"url": "https://private.invalid"},
        {"notification_delivery": "unavailable"},
    ],
)
@pytest.mark.asyncio
async def test_invalid_trigger_fails_safely_before_transport(changes):
    requests = []

    async def transport(request):
        requests.append(request)
        return httpx.Response(200, json={"code": 200})

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        publisher = BarkPublisher(
            client, "http://localhost:8080", "fixture-device", b"0123456789abcdef"
        )
        with pytest.raises(BarkPublishError, match="^BARK_NOTIFICATION_INVALID$"):
            await publisher(message(**changes))
    assert requests == []


@pytest.mark.parametrize(
    "field", ["plan_id", "source_run_id", "instrument_id", "cause", "occurred_at"]
)
def test_missing_frozen_field_is_rejected(field):
    event = message()
    payload = dict(event.payload)
    del payload[field]
    with pytest.raises(BarkPublishError, match="^BARK_NOTIFICATION_INVALID$"):
        map_bark_notification(replace(event, payload=payload), read_only_url=None)


def test_only_exact_monitor_event_schema_is_registered():
    async def publisher(_message):
        pass

    registry = build_bark_registry(publisher)
    assert registry.resolve(*EVENT).consumer_idempotent is True
    assert registry.resolve(EVENT[0], "research_price_trigger_v2") is None
    assert registry.resolve("research.completed", EVENT[1]) is None


@pytest.mark.parametrize(
    "url",
    [
        "https://nexus.local/orders",
        "https://nexus.local/paper?redirect=https://private.invalid",
        "https://user:password@nexus.local/paper",
        "file:///paper",
    ],
)
def test_trigger_cannot_expand_the_configured_link_boundary(url):
    with pytest.raises(ValueError):
        map_bark_notification(message(), read_only_url=url)
