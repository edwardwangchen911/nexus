"""A discovery notice has its own fact identity and never claims recent publication."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import httpx
import pytest
from nexus.notifications.bark import BARK_EVENT_SCHEMAS, BarkPublishError, map_bark_notification
from nexus.notifications.durable_sec import DURABLE_SEC_EVENT, DurableSecNotification
from nexus.outbox.models import OutboxMessage


def payload():
    now = datetime(2026, 11, 17, 15, tzinfo=UTC)
    return DurableSecNotification(
        discovery_id=UUID(int=1),
        instrument_id=UUID(int=2),
        rule_id=UUID(int=3),
        symbol="NVDA",
        event_key="sec:0001045810:0001045810-26-000100",
        form="8-K",
        accepted_at=now - timedelta(hours=1),
        discovered_at=now,
    )


def message(aggregate_id=UUID(int=1)):
    return OutboxMessage(
        event_id=UUID(int=4),
        aggregate_id=aggregate_id,
        event_type=DURABLE_SEC_EVENT[0],
        schema_version=DURABLE_SEC_EVENT[1],
        payload=payload().model_dump(mode="json"),
    )


def test_normal_mapper_preserves_discovery_and_acceptance_times():
    assert DURABLE_SEC_EVENT in BARK_EVENT_SCHEMAS
    notice = map_bark_notification(message(), read_only_url=None)
    assert "NVDA" in notice["body"]
    assert "SEC 接受时间" in notice["body"] and "首次发现" in notice["body"]
    assert "投资判断待更新" in notice["body"]
    assert "刚发布" not in notice["body"]
    assert notice["id"] == "sec-discovery:00000000-0000-0000-0000-000000000001"


def test_normal_mapper_rejects_other_aggregate():
    with pytest.raises(BarkPublishError, match="BARK_NOTIFICATION_INVALID"):
        map_bark_notification(message(UUID(int=99)), read_only_url=None)


def test_future_acceptance_and_unknown_fields_rejected():
    data = payload().model_dump()
    with pytest.raises(ValueError):
        DurableSecNotification.model_validate(
            data | {"accepted_at": data["discovered_at"] + timedelta(seconds=1)}
        )
    with pytest.raises(ValueError):
        DurableSecNotification.model_validate(data | {"free_research_credit": True})


@pytest.mark.asyncio
async def test_normal_publisher_sends_discovery_through_existing_transport():
    from nexus.notifications.bark import BarkPublisher

    requests = []

    def accepted(request):
        requests.append(request)
        return httpx.Response(200, json={"code": 200, "message": "success"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(accepted)) as client:
        publisher = BarkPublisher(
            client=client,
            server_url="http://127.0.0.1:8080",
            device_key="synthetic-only",
            encryption_key=b"0123456789abcdef",
            iv_factory=lambda: b"abcdefghijklmnop",
        )
        await publisher.publish(message())
    assert len(requests) == 1
