"""Normal archived index and immutable rules, without model or provider requests."""

import pytest
from integration.test_monitor_disclosure_refresh_pg import setup
from nexus.monitoring import durable_sec_storage as durable
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.persistence.models.runtime import OutboxEvent
from sqlalchemy import func, select


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
