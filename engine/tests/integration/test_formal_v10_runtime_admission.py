import asyncio
from datetime import UTC, datetime

import pytest
from nexus.jobs.daily_local import LOCAL_SAFE_DAILY_PROFILE
from nexus.jobs.daily_pipeline import (
    assert_daily_pipeline_schema_ready,
    schedule_due_daily_pipeline,
)
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine


def test_normal_worker_first_poll_persists_current_heartbeat(migrated_database_url, monkeypatch):
    from nexus import worker
    from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
    from nexus.settings import EngineSettings

    class PollObserved(Exception):
        pass

    original_poll = worker.run_job_worker_once

    async def observe_poll(factory, **kwargs):
        try:
            assert await original_poll(factory, **kwargs) is False
            async with factory() as session:
                assert (
                    await session.scalar(
                        text("SELECT migration_head FROM worker_heartbeats WHERE queue = 'general'")
                    )
                    == CURRENT_MIGRATION_HEAD
                )
            raise PollObserved
        finally:
            await factory.kw["bind"].dispose()

    monkeypatch.setattr(worker, "run_job_worker_once", observe_poll)
    settings = EngineSettings(
        database_url=migrated_database_url,
        environment="development",
        agent_runtime_mode="fixture",
    )
    with pytest.raises(PollObserved):
        asyncio.run(worker.run_worker(settings, run_forever=True))


def test_normal_worker_scheduler_accepts_migrated_head_without_jobs(migrated_database_url):
    async def verify():
        engine = create_async_engine(migrated_database_url)
        try:
            async with engine.begin() as connection:
                async with AsyncSession(bind=connection) as session:
                    await assert_daily_pipeline_schema_ready(session)
                    assert (
                        await schedule_due_daily_pipeline(
                            session,
                            now=datetime(2026, 10, 14, 23, tzinfo=UTC),
                            profile_id=LOCAL_SAFE_DAILY_PROFILE,
                            market_source="fosun",
                        )
                        is None
                    )
        finally:
            await engine.dispose()

    asyncio.run(verify())


def test_normal_worker_rejects_old_schema(migrated_database_url):
    async def verify():
        engine = create_async_engine(migrated_database_url)
        try:
            async with engine.begin() as connection:
                await connection.execute(
                    text("UPDATE alembic_version SET version_num = '0077_formal_v9_output_binding'")
                )
                async with AsyncSession(bind=connection) as session:
                    with pytest.raises(RuntimeError, match="DAILY_PIPELINE_DATABASE_HEAD_MISMATCH"):
                        await assert_daily_pipeline_schema_ready(session)
        finally:
            await engine.dispose()

    asyncio.run(verify())
