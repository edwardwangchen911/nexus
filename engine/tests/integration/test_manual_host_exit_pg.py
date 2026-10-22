"""A stopped local host is auditable and cannot revive or hide live work."""

import asyncio

import pytest
from nexus.jobs.models import QueueName
from nexus.jobs.repository import upsert_worker_heartbeat
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.runtime import WorkerHeartbeat
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError


async def seed_host(factory):
    async with factory.begin() as session:
        now = await session.scalar(select(func.clock_timestamp()))
        host = await upsert_worker_heartbeat(
            session,
            queue=QueueName.AGENT_PAPER,
            worker_id="agent-paper-loop-00000000-0000-0000-0000-000000000001:manual:" + "b" * 64,
            process_version="nexus-manual-paper-v1+" + "a" * 40,
            migration_head=CURRENT_MIGRATION_HEAD,
            started_at=now,
            seen_at=now,
        )
        return host


EXIT_SQL = """INSERT INTO manual_host_exits
 (heartbeat_id,queue,worker_id,started_at,process_version,migration_head,final_seen_at)
 SELECT id,queue,worker_id,started_at,process_version,migration_head,seen_at
 FROM worker_heartbeats WHERE id=:id RETURNING ended_at"""


def test_host_exit_migration_roundtrip_and_audit_preservation(temporary_database_url, monkeypatch):
    from pathlib import Path

    from alembic import command
    from alembic.config import Config

    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    command.upgrade(config, "0084_research_only_requests")
    command.upgrade(config, "head")
    command.downgrade(config, "0084_research_only_requests")
    command.upgrade(config, "head")

    async def leave_audit():
        factory = session_factory(EngineSettings(database_url=temporary_database_url))
        try:
            host = await seed_host(factory)
            async with factory.begin() as session:
                await session.execute(text(EXIT_SQL), {"id": host.id})
        finally:
            await factory.kw["bind"].dispose()

    asyncio.run(leave_audit())
    with pytest.raises(DBAPIError, match="MANUAL_HOST_EXIT_DOWNGRADE_FORBIDDEN"):
        command.downgrade(config, "0084_research_only_requests")


@pytest.mark.asyncio
async def test_exit_retains_heartbeat_and_forbids_late_heartbeat(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        host = await seed_host(factory)
        async with factory.begin() as session:
            ended = await session.scalar(text(EXIT_SQL), {"id": host.id})
            assert ended >= host.seen_at
        async with factory.begin() as session:
            kept = await session.get(WorkerHeartbeat, host.id)
            assert kept.seen_at == host.seen_at and kept.process_version == host.process_version
        with pytest.raises(DBAPIError, match="MANUAL_HOST_ALREADY_EXITED"):
            async with factory.begin() as session:
                await session.execute(
                    text("UPDATE worker_heartbeats SET seen_at=clock_timestamp() WHERE id=:id"),
                    {"id": host.id},
                )
        # The normal writer must not revive the same identity via ON CONFLICT.
        with pytest.raises(DBAPIError, match="MANUAL_HOST_ALREADY_EXITED"):
            async with factory.begin() as session:
                await upsert_worker_heartbeat(
                    session,
                    queue=QueueName.AGENT_PAPER,
                    worker_id=host.worker_id,
                    process_version=host.process_version,
                    migration_head=host.migration_head,
                    started_at=host.started_at,
                    seen_at=ended,
                )
        for statement in (
            "UPDATE manual_host_exits SET ended_at=ended_at",
            "DELETE FROM manual_host_exits",
        ):
            with pytest.raises(DBAPIError):
                async with factory.begin() as session:
                    await session.execute(text(statement))
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_heartbeat_then_waiting_exit_rejects_stale_final_seen(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        host = await seed_host(factory)
        async with factory() as owner:
            async with owner.begin():
                await owner.execute(
                    text("UPDATE worker_heartbeats SET seen_at=clock_timestamp() WHERE id=:id"),
                    {"id": host.id},
                )

                async def late_exit():
                    async with factory.begin() as session:
                        pid.set_result(await session.scalar(text("SELECT pg_backend_pid()")))
                        await session.execute(
                            text(
                                EXIT_SQL.replace(
                                    "migration_head,seen_at\n", "migration_head,:old_seen\n"
                                )
                            ),
                            {"id": host.id, "old_seen": host.seen_at},
                        )

                pid = asyncio.get_running_loop().create_future()
                task = asyncio.create_task(late_exit())
                backend = await pid
                async with asyncio.timeout(30):
                    while (
                        await owner.scalar(
                            text("SELECT wait_event_type FROM pg_stat_activity WHERE pid=:pid"),
                            {"pid": backend},
                        )
                        != "Lock"
                    ):
                        await owner.execute(text("SELECT pg_stat_clear_snapshot()"))
                        await asyncio.sleep(0.01)
                assert not task.done()
            with pytest.raises(DBAPIError, match="MANUAL_HOST_EXIT_INVALID"):
                await task
        from nexus.paper.manual_host_exit import record_host_exit

        async with factory.begin() as session:
            record = await record_host_exit(
                session,
                worker_id=host.worker_id,
                started_at=host.started_at,
                process_version=host.process_version,
            )
            assert record.final_seen_at > host.seen_at
            ended_at = record.ended_at
        async with factory.begin() as session:
            repeated = await record_host_exit(
                session,
                worker_id=host.worker_id,
                started_at=host.started_at,
                process_version=host.process_version,
            )
            assert repeated.ended_at == ended_at
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "time_expression", ["clock_timestamp()", "clock_timestamp()+interval '1 day'"]
)
async def test_exit_time_is_not_supplied_by_caller(migrated_database_url, time_expression):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        host = await seed_host(factory)
        sql = EXIT_SQL.replace("final_seen_at)", "final_seen_at,ended_at)").replace(
            "migration_head,seen_at\n", f"migration_head,seen_at,{time_expression}\n"
        )
        with pytest.raises(DBAPIError, match="MANUAL_HOST_EXIT_INVALID"):
            async with factory.begin() as session:
                await session.execute(text(sql), {"id": host.id})
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_rolled_back_exit_does_not_retire_host(migrated_database_url):
    from nexus.paper.manual_host_exit import valid_host_exit

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        host = await seed_host(factory)
        async with factory() as session:
            await session.execute(text(EXIT_SQL), {"id": host.id})
            await session.rollback()
        async with factory.begin() as session:
            await upsert_worker_heartbeat(
                session,
                queue=QueueName.AGENT_PAPER,
                worker_id=host.worker_id,
                process_version=host.process_version,
                migration_head=host.migration_head,
                started_at=host.started_at,
                seen_at=await session.scalar(select(func.clock_timestamp())),
            )
            assert not await session.scalar(
                select(valid_host_exit())
                .select_from(WorkerHeartbeat)
                .where(WorkerHeartbeat.id == host.id)
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("isolation", ["REPEATABLE READ", "SERIALIZABLE"])
async def test_old_snapshot_cannot_revive_exited_host(migrated_database_url, isolation):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        host = await seed_host(factory)
        async with factory() as old:
            await old.execute(text(f"SET TRANSACTION ISOLATION LEVEL {isolation}"))
            await old.execute(text("SELECT * FROM worker_heartbeats WHERE id=:id"), {"id": host.id})
            async with factory.begin() as session:
                await session.execute(text(EXIT_SQL), {"id": host.id})
            with pytest.raises(DBAPIError) as caught:
                await old.execute(
                    text("UPDATE worker_heartbeats SET seen_at=clock_timestamp() WHERE id=:id"),
                    {"id": host.id},
                )
            assert caught.value.orig.sqlstate == "40001"
            await old.rollback()
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_normal_research_exit_allows_immediate_next_preparation(
    migrated_database_url, tmp_path, monkeypatch
):
    import json

    from integration.test_research_request_pg import prepare_scope
    from nexus.agents.responses_budget import UsageSnapshot
    from nexus.ops.manual_paper_host import run_pure_research_once
    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
    from nexus.persistence.models.runtime import ManualHostExit
    from unit.agents.test_codex_sdk import _assessment_payload, _staged_batch

    factory, settings, store, _ = await prepare_scope(migrated_database_url, tmp_path, bounded=True)
    settings = settings.model_copy(update={"release_revision": "a" * 40})
    calls = []

    async def invoke(payload, **options):
        options["dispatch_guard"]()
        calls.append(payload["stage"])
        body = (
            _assessment_payload(payload["previous_claims"])
            if payload["stage"] == "synthesis"
            else _staged_batch(payload["stage"], payload["requested_roles"])
        )
        return json.dumps(body), UsageSnapshot(input_tokens=300, output_tokens=80, provider_calls=1)

    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", invoke)
    monkeypatch.setattr(
        "nexus.research.sec_exhibit_storage.S3ProviderEvidenceStore", lambda *_: store
    )
    try:
        first = await run_pure_research_once(settings, idempotency_key="first-complete")
        assert first.status == "completed" and len(calls) == 9

        async def stop_before_model(*_, **__):
            return False

        monkeypatch.setattr("nexus.ops.agent_paper_once.run_cycle_job_once", stop_before_model)
        second = await run_pure_research_once(settings, idempotency_key="second-preparation")
        assert second.request_id != first.request_id and second.status == "unavailable"
        assert len(second.research.members) == 3
        assert all(m.status == "abstained" and m.usage_known for m in second.research.members)
        assert len(calls) == 9
        async with factory.begin() as session:
            exits = list(
                await session.scalars(select(ManualHostExit).order_by(ManualHostExit.ended_at))
            )
            assert len(exits) == 2
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(WorkerHeartbeat)
                    .where(WorkerHeartbeat.id.in_([e.heartbeat_id for e in exits]))
                )
                == 2
            )
            assert (second.requested_at - exits[0].final_seen_at).total_seconds() < 30
            budgets = list(await session.scalars(select(AgentDailyBudgetReservationRecord)))
            assert sorted(b.state for b in budgets) == ["released"] * 3 + ["settled"] * 3
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_exited_host_does_not_release_unknown_usage(
    migrated_database_url, tmp_path, monkeypatch
):
    from integration.test_research_request_pg import prepare_scope
    from nexus.agents.codex_sdk import CodexInvocationError
    from nexus.ops.manual_paper_host import run_pure_research_once
    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord
    from nexus.persistence.models.runtime import ManualHostExit

    factory, settings, store, _ = await prepare_scope(migrated_database_url, tmp_path, bounded=True)
    settings = settings.model_copy(update={"release_revision": "a" * 40})
    calls = []

    async def unknown(*_, **__):
        calls.append(True)
        raise CodexInvocationError("CODEX_TIMEOUT", provider_call_started=True, usage=None)

    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", unknown)
    monkeypatch.setattr(
        "nexus.research.sec_exhibit_storage.S3ProviderEvidenceStore", lambda *_: store
    )
    try:
        with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
            await run_pure_research_once(settings, idempotency_key="unknown-first")
        async with factory.begin() as session:
            assert await session.scalar(select(func.count()).select_from(ManualHostExit)) == 1
            budgets = list(await session.scalars(select(AgentDailyBudgetReservationRecord)))
            assert sorted(b.state for b in budgets) == ["released", "released", "reserved"]
            unknown_budget = next(b for b in budgets if b.state == "reserved")
            assert unknown_budget.settled_usd is None
        with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
            await run_pure_research_once(settings, idempotency_key="next-still-blocked")
        assert calls == [True]
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_provider_heartbeat_needs_no_exit_table_permission(migrated_database_url):
    from uuid import uuid4

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    role = "synthetic_exit_" + uuid4().hex
    try:
        async with factory.begin() as session:
            now = await session.scalar(select(func.clock_timestamp()))
            host = await upsert_worker_heartbeat(
                session,
                queue=QueueName.PROVIDER,
                worker_id="synthetic-provider",
                process_version="test",
                migration_head=CURRENT_MIGRATION_HEAD,
                started_at=now,
                seen_at=now,
            )
            await session.execute(text(f'CREATE ROLE "{role}"'))
            await session.execute(text(f'GRANT USAGE ON SCHEMA public TO "{role}"'))
            await session.execute(text(f'GRANT SELECT,UPDATE ON worker_heartbeats TO "{role}"'))
        async with factory.begin() as session:
            await session.execute(text(f'SET LOCAL ROLE "{role}"'))
            assert not await session.scalar(
                text("SELECT has_table_privilege(current_user,'manual_host_exits','SELECT')")
            )
            result = await session.execute(
                text("UPDATE worker_heartbeats SET seen_at=clock_timestamp() WHERE id=:id"),
                {"id": host.id},
            )
            assert result.rowcount == 1
    finally:
        async with factory.begin() as session:
            await session.execute(text(f'DROP OWNED BY "{role}"'))
            await session.execute(text(f'DROP ROLE "{role}"'))
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,expression",
    [
        ("worker_id", "worker_id || '-wrong'"),
        ("queue", "'general'"),
        ("started_at", "started_at - interval '1 second'"),
        ("process_version", "'wrong'"),
        ("migration_head", "'wrong'"),
        ("final_seen_at", "seen_at - interval '1 second'"),
    ],
)
async def test_mismatched_exit_cannot_close_host(migrated_database_url, field, expression):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        host = await seed_host(factory)
        columns = [
            "heartbeat_id",
            "queue",
            "worker_id",
            "started_at",
            "process_version",
            "migration_head",
            "final_seen_at",
        ]
        values = [
            "id",
            "queue",
            "worker_id",
            "started_at",
            "process_version",
            "migration_head",
            "seen_at",
        ]
        values[columns.index(field)] = expression
        with pytest.raises(DBAPIError, match="MANUAL_HOST_EXIT_INVALID"):
            async with factory.begin() as session:
                await session.execute(
                    text(
                        f"INSERT INTO manual_host_exits ({','.join(columns)}) "
                        f"SELECT {','.join(values)} FROM worker_heartbeats WHERE id=:id"
                    ),
                    {"id": host.id},
                )
        async with factory.begin() as session:
            assert await session.scalar(text("SELECT count(*) FROM manual_host_exits")) == 0
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_exit_then_waiting_heartbeat_is_rejected(migrated_database_url):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        host = await seed_host(factory)
        async with factory() as owner:
            async with owner.begin():
                await owner.execute(text(EXIT_SQL), {"id": host.id})

                async def late():
                    async with factory.begin() as session:
                        pid.set_result(await session.scalar(text("SELECT pg_backend_pid()")))
                        await session.execute(
                            text(
                                "UPDATE worker_heartbeats SET seen_at=clock_timestamp() "
                                "WHERE id=:id"
                            ),
                            {"id": host.id},
                        )

                pid = asyncio.get_running_loop().create_future()
                task = asyncio.create_task(late())
                backend = await pid
                async with asyncio.timeout(30):
                    while (
                        await owner.scalar(
                            text("SELECT wait_event_type FROM pg_stat_activity WHERE pid=:pid"),
                            {"pid": backend},
                        )
                        != "Lock"
                    ):
                        await owner.execute(text("SELECT pg_stat_clear_snapshot()"))
                        await asyncio.sleep(0.01)
                assert not task.done()
            with pytest.raises(DBAPIError, match="MANUAL_HOST_ALREADY_EXITED"):
                await task
    finally:
        await factory.kw["bind"].dispose()
