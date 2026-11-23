"""Normal archived index and immutable rules, without model or provider requests."""

import pytest
from integration.test_monitor_disclosure_refresh_pg import setup
from nexus.audit.service import append_audit_event
from nexus.monitoring import durable_sec_storage as durable
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.persistence.models.runtime import OutboxEvent
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError


@pytest.mark.asyncio
async def test_deferred_discovery_audit_uses_indexed_check_lookup(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, store, _captured, pointer, calls, client, _invoke = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    monkeypatch.setattr(durable, "configured_exhibit_store", lambda session: store)
    seq_scans = text("""
        SELECT seq_scan FROM pg_stat_xact_user_tables
        WHERE relid = 'public.sec_discovery_checks'::regclass
    """)
    try:
        async with factory.begin() as session:
            plan = await session.scalar(select(MonitorPlan).where(MonitorPlan.kind == "disclosure"))
            assert plan is not None
            rule = await durable.enable_rule(
                session, account_id=pointer.account_id, plan_id=plan.id
            )
            rule_id = rule.id
        async with factory.begin() as session:
            now = await session.scalar(text("SELECT clock_timestamp()"))
            check = await durable.record_discovery(
                session,
                account_id=pointer.account_id,
                rule_id=rule_id,
                capture_id=None,
                scheduled_at=now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0),
            )
            assert check.result == "unknown"
            # An indexable predicate must avoid a sequential scan even when the
            # small test relation would normally be cheaper to scan.
            await session.execute(text("SET LOCAL enable_seqscan = off"))
            before = await session.scalar(seq_scans)
            await session.execute(text("SET CONSTRAINTS sec_durable_complete IMMEDIATE"))
            assert await session.scalar(seq_scans) == before
        with pytest.raises(IntegrityError, match="SEC_DURABLE_AUDIT_INVALID"):
            async with factory.begin() as session:
                await append_audit_event(
                    session,
                    actor_id="research-monitor",
                    action="research.monitor.durable_fact",
                    schema_version="durable_sec_v1",
                    payload={
                        "table": "sec_discovery_checks",
                        "row_id": "not-a-uuid",
                        "binding": {},
                    },
                    occurred_at=await session.scalar(text("SELECT clock_timestamp()")),
                )
        assert not calls
    finally:
        await client.aclose()
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_rule_has_no_research_ttl_and_state_changes_are_append_only(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, store, captured, pointer, calls, client, invoke = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    monkeypatch.setattr(durable, "configured_exhibit_store", lambda session: store)
    try:
        async with factory() as session, session.begin():
            plan = await session.scalar(select(MonitorPlan).where(MonitorPlan.kind == "disclosure"))
            assert plan is not None
            rule = await durable.enable_rule(
                session, account_id=pointer.account_id, plan_id=plan.id
            )
            identity = rule.id
            effective = rule.effective_at
        async with factory() as session, session.begin():
            repeated = await durable.enable_rule(
                session, account_id=pointer.account_id, plan_id=plan.id
            )
            assert repeated.id == identity and repeated.effective_at == effective
            assert await durable.rule_state(session, repeated) == "active"
            await durable.change_state(
                session, account_id=pointer.account_id, rule_id=identity, state="paused"
            )
        async with factory() as session, session.begin():
            assert await durable.rule_state(session, rule) == "paused"
            await durable.change_state(
                session, account_id=pointer.account_id, rule_id=identity, state="active"
            )
        async with factory() as session, session.begin():
            await durable.change_state(
                session, account_id=pointer.account_id, rule_id=identity, state="revoked"
            )
        async with factory() as session, session.begin():
            with pytest.raises(ValueError, match="SEC_DURABLE_RULE_REVOKED"):
                await durable.change_state(
                    session, account_id=pointer.account_id, rule_id=identity, state="active"
                )
            assert rule.effective_at == effective
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0
        assert not calls
    finally:
        await client.aclose()
        await factory.kw["bind"].dispose()
