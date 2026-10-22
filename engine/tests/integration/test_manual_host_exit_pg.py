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
                await old.execute(text(
                    "UPDATE worker_heartbeats SET seen_at=clock_timestamp() WHERE id=:id"
                ), {"id": host.id})
            assert caught.value.orig.sqlstate == "40001"
            await old.rollback()
    finally:
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
