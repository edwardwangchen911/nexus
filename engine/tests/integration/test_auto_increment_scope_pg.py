"""Normal scope registration is limiting metadata, not new research permission."""

import argparse
from unittest.mock import AsyncMock

import pytest
from integration.test_monitor_batches_pg import installed
from nexus.monitoring import auto_increment_policy as policy
from nexus.monitoring.durable_sec_storage import change_state, enable_rule
from nexus.ops.research_monitor import run
from nexus.paper.candidate_batch import load_agent_paper_candidate_batch
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.persistence.models.runtime import Job, OutboxEvent
from nexus.persistence.models.sec_monitor import SecAutoIncrementDayClaim, SecMonitorRule
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError


@pytest.mark.asyncio
async def test_independent_scope_covers_price_without_sec_and_survives_sec_pause(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, settings, store, captured, pointer = await installed(
        migrated_database_url, monkeypatch, tmp_path, kinds=("price", "disclosure")
    )
    monkeypatch.setattr(
        "nexus.monitoring.durable_sec_storage.configured_exhibit_store", lambda session: store
    )
    try:
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(SecMonitorRule)) == 0
            assert (
                await session.scalar(
                    text("SELECT sec_auto_increment_scoped(:account,:instrument)"),
                    dict(account=pointer.account_id, instrument=pointer.instrument_id),
                )
                is False
            )
            jobs = await session.scalar(select(func.count()).select_from(Job))
            notices = await session.scalar(select(func.count()).select_from(OutboxEvent))
            candidate = await load_agent_paper_candidate_batch(
                session, batch_id=settings.agent_paper_candidate_batch_id
            )
        first = await run(settings, argparse.Namespace(action="enable-auto-increment-limit"))
        again = await run(settings, argparse.Namespace(action="enable-auto-increment-limit"))
        assert first["snapshot_id"] == again["snapshot_id"]
        assert first["research_authority_granted"] is False
        for statement in (
            "UPDATE data_snapshots SET source='other' WHERE id=CAST(:id AS uuid)",
            "DELETE FROM data_snapshots WHERE id=CAST(:id AS uuid)",
        ):
            with pytest.raises(DBAPIError) as immutable:
                async with factory() as session, session.begin():
                    await session.execute(text(statement), {"id": first["snapshot_id"]})
            assert getattr(immutable.value.orig, "sqlstate", None) == "23514"
        async with factory() as session, session.begin():
            # Both the originally monitored stock and another candidate with no
            # SEC rule are subject to the same independent quota scope.
            for instrument in candidate.instrument_ids:
                assert (
                    await session.scalar(
                        text("SELECT sec_auto_increment_scoped(:account,:instrument)"),
                        dict(account=pointer.account_id, instrument=instrument),
                    )
                    is True
                )
            assert await session.scalar(select(func.count()).select_from(SecMonitorRule)) == 0
            plan = await session.scalar(select(MonitorPlan).where(MonitorPlan.kind == "disclosure"))
            rule = await enable_rule(session, account_id=pointer.account_id, plan_id=plan.id)
        async with factory() as session, session.begin():
            await change_state(
                session, account_id=pointer.account_id, rule_id=rule.id, state="paused"
            )
        async with factory() as session:
            assert (
                await session.scalar(
                    text("SELECT sec_auto_increment_scoped(:account,:instrument)"),
                    dict(account=pointer.account_id, instrument=pointer.instrument_id),
                )
                is True
            )
            assert (
                await session.scalar(select(func.count()).select_from(SecAutoIncrementDayClaim))
                == 0
            )
            assert await session.scalar(select(func.count()).select_from(Job)) == jobs
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == notices
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_scope_publication_without_exact_audit_cannot_commit(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, settings, store, captured, pointer = await installed(
        migrated_database_url, monkeypatch, tmp_path, kinds=("price",)
    )
    try:
        with monkeypatch.context() as failure:
            failure.setattr(policy, "append_audit_event", AsyncMock())
            with pytest.raises(DBAPIError) as rejected:
                async with factory() as session, session.begin():
                    await policy.enable_scope(
                        session,
                        account_id=pointer.account_id,
                        candidate_batch_id=settings.agent_paper_candidate_batch_id,
                    )
            assert getattr(rejected.value.orig, "sqlstate", None) == "23514"
        async with factory() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(DataSnapshot)
                    .where(DataSnapshot.source == policy.SOURCE)
                )
                == 0
            )
        result = await run(settings, argparse.Namespace(action="enable-auto-increment-limit"))
        assert result["status"] == "registered"
    finally:
        await factory.kw["bind"].dispose()
