"""Persisted monitor triggers use the normal Bark worker, without research admission."""

import asyncio
from datetime import timedelta

import httpx
import pytest
from integration.test_bark_outbox_pg import _decrypted_bark_notification
from integration.test_research_monitoring_pg import _NOW, quote, seed
from nexus.monitoring.storage import record_check
from nexus.notifications.bark import BarkPublisher
from nexus.notifications.worker import (
    build_bark_registry,
    load_bark_worker_settings,
    run_bark_worker,
)
from nexus.outbox.dispatcher import dispatch_outbox_once
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.monitoring import MonitorTrigger
from nexus.persistence.models.runtime import Job, OutboxDeliveryAttempt, OutboxEvent
from sqlalchemy import func, select


def settings(database_url):
    return load_bark_worker_settings(
        {
            "NEXUS_BARK_ENABLED": "true",
            "DATABASE_URL": database_url,
            "NEXUS_BARK_SERVER_URL": "http://localhost:8080",
            "NEXUS_BARK_DEVICE_KEY": "test-device-key",
            "NEXUS_BARK_ENCRYPTION_KEY": "0123456789abcdef",
            "NEXUS_BARK_ACTIVATED_AT": "2025-12-31T23:59:59Z",
            "NEXUS_BARK_READ_ONLY_URL": "https://nexus.local/paper",
        }
    )


async def captured_trigger(database_url):
    factory, plan = await seed(database_url)
    async with factory() as session, session.begin():
        await record_check(
            session,
            plan_id=plan.id,
            scheduled_at=_NOW,
            checked_at=_NOW,
            observation=quote(plan, _NOW),
        )
        trigger = await session.scalar(select(MonitorTrigger))
        baseline = await research_counts(session)
    return factory, trigger, baseline


async def research_counts(session):
    return tuple(
        [
            await session.scalar(select(func.count()).select_from(model))
            for model in (AgentResearchRunRecord, Job)
        ]
    )


async def assert_capture_unchanged(factory, trigger, baseline):
    async with factory() as session:
        saved = await session.get(MonitorTrigger, trigger.id)
        assert (saved.status, saved.reason, saved.research_run_id) == ("pending", None, None)
        assert await research_counts(session) == baseline
        assert await session.scalar(select(func.count()).select_from(MonitorTrigger)) == 1


@pytest.mark.asyncio
async def test_concurrent_worker_claims_and_repeat_delivery_do_not_start_research(
    migrated_database_url,
):
    factory, trigger, baseline = await captured_trigger(migrated_database_url)
    entered, release = asyncio.Event(), asyncio.Event()
    seen = []

    async def transport(request):
        seen.append(_decrypted_bark_notification(request))
        entered.set()
        await release.wait()
        return httpx.Response(200, json={"code": 200})

    options = dict(
        settings=settings(migrated_database_url),
        run_forever=False,
        clock=lambda: _NOW + timedelta(seconds=1),
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        first = asyncio.create_task(run_bark_worker(client=client, **options))
        try:
            await asyncio.wait_for(entered.wait(), timeout=10)
            assert await run_bark_worker(client=client, **options) == 0
        finally:
            release.set()
        assert await first == 1
        assert await run_bark_worker(client=client, **options) == 0
    assert len(seen) == 1 and seen[0]["id"] == f"research-monitor:{trigger.id}"
    assert "待准入" in seen[0]["body"]
    await assert_capture_unchanged(factory, trigger, baseline)
    async with factory() as session:
        event = await session.get(OutboxEvent, trigger.outbox_id)
        assert (event.status, event.attempt_count) == ("published", 1)


@pytest.mark.asyncio
async def test_bark_failure_retries_same_trigger_without_research_or_sensitive_error(
    migrated_database_url,
):
    factory, trigger, baseline = await captured_trigger(migrated_database_url)
    seen = []

    async def transport(request):
        seen.append(_decrypted_bark_notification(request))
        return (
            httpx.Response(503, text="private provider detail")
            if len(seen) == 1
            else httpx.Response(200, json={"code": 200})
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        for seconds in (1, 4):
            assert (
                await run_bark_worker(
                    settings(migrated_database_url),
                    run_forever=False,
                    client=client,
                    clock=lambda: _NOW + timedelta(seconds=seconds),
                )
                == 1
            )
    assert len(seen) == 2 and seen[0] == seen[1]
    await assert_capture_unchanged(factory, trigger, baseline)
    async with factory() as session:
        event = await session.get(OutboxEvent, trigger.outbox_id)
        attempts = tuple(
            await session.scalars(
                select(OutboxDeliveryAttempt)
                .where(OutboxDeliveryAttempt.outbox_event_id == event.id)
                .order_by(OutboxDeliveryAttempt.attempt_no)
            )
        )
        assert (event.status, event.attempt_count, event.last_error_code) == ("published", 2, None)
        assert [(a.outcome, a.error_code) for a in attempts] == [
            ("failed", "PUBLISHER_FAILED"),
            ("published", None),
        ]


@pytest.mark.asyncio
async def test_lost_confirmation_redelivers_stable_notification_id_only(migrated_database_url):
    factory, trigger, baseline = await captured_trigger(migrated_database_url)
    seen = []

    async def transport(request):
        seen.append(_decrypted_bark_notification(request))
        return httpx.Response(200, json={"code": 200})

    async def lost_confirmation(_message):
        raise RuntimeError("synthetic confirmation loss")

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        publisher = BarkPublisher(
            client,
            "http://localhost:8080",
            "test-device-key",
            b"0123456789abcdef",
            read_only_url="https://nexus.local/paper",
        )
        with pytest.raises(RuntimeError, match="synthetic confirmation loss"):
            await dispatch_outbox_once(
                factory,
                registry=build_bark_registry(publisher),
                worker_id="monitor-notification-fixture",
                clock=lambda: _NOW + timedelta(seconds=1),
                claim_registered_only=True,
                after_publish=lost_confirmation,
            )
        assert (
            await run_bark_worker(
                settings(migrated_database_url),
                run_forever=False,
                client=client,
                clock=lambda: _NOW + timedelta(seconds=62),
            )
            == 1
        )
    assert len(seen) == 2 and seen[0] == seen[1]
    await assert_capture_unchanged(factory, trigger, baseline)


@pytest.mark.asyncio
async def test_invalid_monitor_payload_is_not_silently_acknowledged(migrated_database_url):
    from nexus.domain.ids import new_uuid7

    factory, _ = await seed(migrated_database_url)
    async with factory() as session, session.begin():
        baseline = await research_counts(session)
        event = OutboxEvent(
            aggregate_id=new_uuid7(),
            event_type="research.monitor.triggered",
            schema_version="research_price_trigger_v1",
            payload={},
            available_at=_NOW,
        )
        session.add(event)
        await session.flush()

    async def forbidden(_request):
        pytest.fail("invalid notification reached transport")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
        assert (
            await run_bark_worker(
                settings(migrated_database_url),
                run_forever=False,
                client=client,
                clock=lambda: _NOW + timedelta(seconds=1),
            )
            == 1
        )
    async with factory() as session:
        saved = await session.get(OutboxEvent, event.id)
        assert (saved.status, saved.last_error_code) == ("queued", "PUBLISHER_FAILED")
        assert await research_counts(session) == baseline
        assert await session.scalar(select(func.count()).select_from(MonitorTrigger)) == 0
