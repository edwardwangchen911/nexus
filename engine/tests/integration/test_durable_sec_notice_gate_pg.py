"""Old normal scanner compatibility and durable pause via real audited PostgreSQL facts."""

from uuid import UUID, uuid4

import httpx
import pytest
from integration.test_monitor_batches_pg import installed
from nexus.monitoring.batches import scan_stock_batch
from nexus.monitoring.durable_sec_claims import audit_fact, claim_notice
from nexus.notifications.bark import BarkPublisher
from nexus.notifications.durable_sec_gate import DurableSecPublisher
from nexus.notifications.worker import load_bark_worker_settings, run_bark_worker
from nexus.outbox.models import OutboxMessage
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorPlan, MonitorTrigger
from nexus.persistence.models.runtime import OutboxDeliveryAttempt, OutboxEvent
from nexus.persistence.models.sec_monitor import (
    SecMonitorRule,
    SecMonitorRuleState,
    SecNotificationClaim,
)
from sqlalchemy import func, select, text


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["paused", "revoked"])
async def test_queued_legacy_shared_claim_paused_never_posts_or_consumes_attempt(
    migrated_database_url, monkeypatch, tmp_path, state
):
    factory, settings, store, captured, pointer = await installed(
        migrated_database_url, monkeypatch, tmp_path, kinds=("disclosure",)
    )
    try:
        async with factory() as session, session.begin():
            result = await scan_stock_batch(
                session,
                account_id=pointer.account_id,
                instrument_id=pointer.instrument_id,
                now=captured["clock"][0],
            )
            assert result["new_triggers"] == 2
            assert await session.scalar(select(func.count()).select_from(SecNotificationClaim)) == 0
        async with factory() as session, session.begin():
            plan = await session.scalar(select(MonitorPlan).where(MonitorPlan.kind == "disclosure"))
            baseline = await session.get(DataSnapshot, plan.baseline_snapshot_id)
            at = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            rule = SecMonitorRule(
                id=uuid4(),
                account_id=pointer.account_id,
                instrument_id=pointer.instrument_id,
                issuer_cik=baseline.payload["index"]["issuer_cik"],
                identity_snapshot_id=UUID(baseline.payload["index"]["identity_snapshot_id"]),
                anchor_plan_id=plan.id,
                effective_at=at,
                created_at=at,
                forms=plan.condition["forms"],
                policy_version="durable_sec_v1",
            )
            session.add(rule)
            await audit_fact(session, rule)
            active = SecMonitorRuleState(
                id=uuid4(),
                rule_id=rule.id,
                sequence=0,
                state="active",
                effective_at=at,
                created_at=at,
            )
            session.add(active)
            await audit_fact(session, active)
            rule_id = rule.id
        async with factory() as session, session.begin():
            now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            old_key = await session.scalar(
                select(MonitorTrigger.event_key)
                .where(
                    MonitorTrigger.account_id == pointer.account_id,
                    MonitorTrigger.instrument_id == pointer.instrument_id,
                    MonitorTrigger.kind == "disclosure",
                )
                .limit(1)
            )
            for event_key in (old_key, "sec:0000000001:0000000001-26-000099"):
                assert not await claim_notice(
                    session,
                    account_id=pointer.account_id,
                    instrument_id=pointer.instrument_id,
                    event_key=event_key,
                    outbox_id=uuid4(),
                    legacy_trigger_id=None,
                    now=now,
                )
            assert await session.scalar(select(func.count()).select_from(SecNotificationClaim)) == 0
        async with factory() as session, session.begin():
            at = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            for trigger in await session.scalars(
                select(MonitorTrigger).where(
                    MonitorTrigger.account_id == pointer.account_id,
                    MonitorTrigger.instrument_id == pointer.instrument_id,
                    MonitorTrigger.kind == "disclosure",
                )
            ):
                claim = SecNotificationClaim(
                    id=uuid4(),
                    account_id=trigger.account_id,
                    instrument_id=trigger.instrument_id,
                    event_key=trigger.event_key,
                    outbox_id=trigger.outbox_id,
                    legacy_trigger_id=trigger.id,
                    created_at=at,
                )
                session.add(claim)
                await audit_fact(session, claim)
        async with factory() as session, session.begin():
            at = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            paused = SecMonitorRuleState(
                id=uuid4(),
                rule_id=rule_id,
                sequence=1,
                state=state,
                effective_at=at,
                created_at=at,
            )
            session.add(paused)
            await audit_fact(session, paused)
        async with factory() as session, session.begin():
            now = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
            assert not await claim_notice(
                session,
                account_id=pointer.account_id,
                instrument_id=pointer.instrument_id,
                event_key="sec:0000000001:0000000001-26-000099",
                outbox_id=uuid4(),
                legacy_trigger_id=None,
                now=now,
            )
            assert await session.scalar(select(func.count()).select_from(SecNotificationClaim)) == 2
        posts = []

        async def forbidden(request):
            posts.append(request)
            raise AssertionError("paused notice must not POST")

        worker_settings = load_bark_worker_settings(
            {
                "NEXUS_BARK_ENABLED": "true",
                "DATABASE_URL": migrated_database_url,
                "NEXUS_BARK_SERVER_URL": "http://127.0.0.1:8080",
                "NEXUS_BARK_DEVICE_KEY": "test-device-key",
                "NEXUS_BARK_ENCRYPTION_KEY": "0123456789abcdef",
                "NEXUS_BARK_ACTIVATED_AT": "2025-12-31T23:59:59Z",
            }
        )
        async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
            assert (
                await run_bark_worker(
                    worker_settings, run_forever=False, client=client, clock=lambda: now
                )
                == 0
            )
        assert not posts
        async with factory() as session:
            original_claim = await session.scalar(select(SecNotificationClaim).limit(1))
            original = await session.get(OutboxEvent, original_claim.outbox_id)
            message = OutboxMessage(
                event_id=original.id,
                event_type=original.event_type,
                aggregate_id=original.aggregate_id,
                schema_version=original.schema_version,
                payload=original.payload,
            )
        async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
            publisher = BarkPublisher(
                client=client,
                server_url="http://127.0.0.1:8080",
                device_key="test-device-key",
                encryption_key=b"0123456789abcdef",
            )
            with pytest.raises(ValueError, match="DURABLE_SEC_NOTICE_NOT_ACTIVE"):
                await DurableSecPublisher(factory, publisher)(message)
        assert not posts
        async with factory() as session:
            assert (
                await session.scalar(select(func.count()).select_from(OutboxDeliveryAttempt)) == 0
            )
            claimed = tuple(await session.scalars(select(SecNotificationClaim)))
            for claim in claimed:
                event = await session.get(OutboxEvent, claim.outbox_id)
                assert event.status == "queued" and event.attempt_count == 0
    finally:
        await factory.kw["bind"].dispose()
