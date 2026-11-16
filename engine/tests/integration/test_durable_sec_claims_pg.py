"""Old normal scanner compatibility and durable pause via real audited PostgreSQL facts."""

from uuid import UUID, uuid4

import pytest
from integration.test_monitor_batches_pg import installed
from nexus.monitoring.batches import scan_stock_batch
from nexus.monitoring.durable_sec_claims import audit_fact, claim_notice
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorPlan, MonitorTrigger
from nexus.persistence.models.sec_monitor import (
    SecMonitorRule,
    SecMonitorRuleState,
    SecNotificationClaim,
)
from sqlalchemy import func, select, text


@pytest.mark.asyncio
async def test_old_normal_scan_and_audited_pause(migrated_database_url, monkeypatch, tmp_path):
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
            paused = SecMonitorRuleState(
                id=uuid4(),
                rule_id=rule_id,
                sequence=1,
                state="paused",
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
            assert await session.scalar(select(func.count()).select_from(SecNotificationClaim)) == 0
    finally:
        await factory.kw["bind"].dispose()
