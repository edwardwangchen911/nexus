"""Admission contract for automatic Schwab intraday Jobs."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.runtime import Job
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

ENGINE_ROOT = Path(__file__).parents[2]
MIGRATION_PATH = ENGINE_ROOT / "alembic/versions/0055_schwab_intraday_job_v3.py"


def test_0055_precedes_the_agent_paper_cycle_head() -> None:
    scripts = ScriptDirectory.from_config(Config(str(ENGINE_ROOT / "alembic.ini")))

    assert CURRENT_MIGRATION_HEAD == "0060_agent_paper_loop_heartbeat"
    assert scripts.get_heads() == [CURRENT_MIGRATION_HEAD]
    migration = scripts.get_revision("0055_schwab_intraday_job_v3")
    assert migration is not None
    assert migration.down_revision == "0054_schwab_stream_policy"


def test_0055_fences_v2_before_replacing_the_admission_contract() -> None:
    source = MIGRATION_PATH.read_text()
    upgrade = source.split("def upgrade() -> None:", maxsplit=1)[1].split(
        "def downgrade() -> None:", maxsplit=1
    )[0]

    jobs_lock = upgrade.index("LOCK TABLE public.jobs IN ACCESS EXCLUSIVE MODE")
    v2_fence = upgrade.index("schema_version = 'intraday_snapshot_job_v2'")
    drop_v2 = upgrade.index("DROP INDEX public.uq_jobs_intraday_v2_bucket")
    assert jobs_lock < v2_fence < drop_v2
    assert "status IN ('queued', 'running', 'failed')" in upgrade
    assert "DELETE FROM" not in upgrade


def test_0055_admits_exact_schwab_realtime_l1_shape() -> None:
    scripts = ScriptDirectory.from_config(Config(str(ENGINE_ROOT / "alembic.ini")))
    migration = scripts.get_revision("0055_schwab_intraday_job_v3")
    assert migration is not None
    predicate = migration.module._source_feed_predicate("NEW", version="v3")

    assert "NEW.payload ->> 'source' = 'schwab'" in predicate
    assert "NEW.payload ->> 'feed' = 'l1'" in predicate
    assert "NEW.payload ->> 'delivery_mode' = 'real_time'" in predicate
    assert "NEW.payload ->> 'include_minute_bars' = 'false'" in predicate
    assert "NEW.payload -> 'quote_authority' = 'null'::pg_catalog.jsonb" in predicate
    assert "NEW.payload -> 'bars_authority' = 'null'::pg_catalog.jsonb" in predicate


def test_0055_downgrade_fences_v3_and_restores_v2_admission() -> None:
    source = MIGRATION_PATH.read_text()
    downgrade = source.split("def downgrade() -> None:", maxsplit=1)[1]

    assert downgrade.index("LOCK TABLE public.jobs IN ACCESS EXCLUSIVE MODE") < downgrade.index(
        "schema_version = 'intraday_snapshot_job_v3'"
    )
    assert "uq_jobs_intraday_v3_bucket" in downgrade
    assert '_create_admission(version="v2")' in downgrade
    assert "DELETE FROM" not in downgrade


def test_job_model_declares_only_the_v3_intraday_bucket_index() -> None:
    intraday_indexes = {
        index.name: str(index.dialect_options["postgresql"]["where"])
        for index in Job.__table__.indexes
        if index.name is not None and index.name.startswith("uq_jobs_intraday_")
    }

    assert intraday_indexes == {
        "uq_jobs_intraday_v3_bucket": (
            "job_type = 'collect.intraday_snapshot' AND schema_version = 'intraday_snapshot_job_v3'"
        )
    }


def test_0055_upgrade_refuses_actionable_v2_without_deleting_it(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(ENGINE_ROOT / "alembic.ini"))
    command.upgrade(config, "0054_schwab_stream_policy")
    job_id = asyncio.run(
        _insert_intraday_job(
            temporary_database_url,
            schema="intraday_snapshot_job_v2",
            source="fosun",
            feed="l1",
        )
    )

    with pytest.raises(DBAPIError, match="intraday v2 Jobs must drain") as error:
        command.upgrade(config, CURRENT_MIGRATION_HEAD)

    assert error.value.orig.sqlstate == "P3D01"
    assert asyncio.run(_job_exists(temporary_database_url, job_id)) is True
    assert asyncio.run(_migration_head(temporary_database_url)) == "0054_schwab_stream_policy"


@pytest.mark.parametrize(
    ("source", "feed", "delivery_mode"),
    (
        ("fosun", "l1", "unknown"),
        ("alpaca", "iex", "real_time"),
        ("schwab", "l1", "real_time"),
    ),
)
def test_0055_admits_each_exact_v3_provider_shape(
    migrated_database_url: str,
    source: str,
    feed: str,
    delivery_mode: str,
) -> None:
    asyncio.run(
        _insert_intraday_job(
            migrated_database_url,
            schema="intraday_snapshot_job_v3",
            source=source,
            feed=feed,
            delivery_mode=delivery_mode,
        )
    )


@pytest.mark.parametrize(
    ("delivery_mode", "include_minute_bars"),
    (("delayed", False), ("real_time", True)),
)
def test_0055_rejects_non_realtime_or_bar_carrying_schwab_jobs(
    migrated_database_url: str,
    delivery_mode: str,
    include_minute_bars: bool,
) -> None:
    with pytest.raises(DBAPIError, match="intraday Job schema is not admitted") as error:
        asyncio.run(
            _insert_intraday_job(
                migrated_database_url,
                schema="intraday_snapshot_job_v3",
                source="schwab",
                feed="l1",
                delivery_mode=delivery_mode,
                include_minute_bars=include_minute_bars,
            )
        )

    assert error.value.orig.sqlstate == "23514"


def test_0055_downgrade_refuses_actionable_v3_and_preserves_terminal_history(
    monkeypatch: pytest.MonkeyPatch,
    migrated_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    config = Config(str(ENGINE_ROOT / "alembic.ini"))
    job_id = asyncio.run(
        _insert_intraday_job(
            migrated_database_url,
            schema="intraday_snapshot_job_v3",
            source="schwab",
            feed="l1",
            delivery_mode="real_time",
        )
    )

    with pytest.raises(DBAPIError, match="intraday v3 admission prevents downgrade") as error:
        command.downgrade(config, "0054_schwab_stream_policy")

    assert error.value.orig.sqlstate == "P3D01"
    assert asyncio.run(_job_exists(migrated_database_url, job_id)) is True
    asyncio.run(_terminalize_job(migrated_database_url, job_id))
    command.downgrade(config, "0054_schwab_stream_policy")
    assert asyncio.run(_job_exists(migrated_database_url, job_id)) is True
    assert asyncio.run(_migration_head(migrated_database_url)) == "0054_schwab_stream_policy"


async def _insert_intraday_job(
    database_url: str,
    *,
    schema: str,
    source: str,
    feed: str,
    delivery_mode: str = "unknown",
    include_minute_bars: bool = False,
) -> UUID:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    job_id = uuid4()
    payload = {
        "schema": schema,
        "source": source,
        "feed": feed,
        "delivery_mode": delivery_mode,
        "include_minute_bars": include_minute_bars,
        "quote_authority": None,
        "bars_authority": None,
        "bucket_start": now.isoformat().replace("+00:00", "Z"),
        "bucket_end": (now + timedelta(seconds=15)).isoformat().replace("+00:00", "Z"),
    }
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO jobs ("
                    "id,job_key,input_hash,status,schema_version,payload,job_type,queue,"
                    "scope_kind,scope_id,effective_at,priority,retry_policy,max_attempts,"
                    "attempt_count,manual_retry_count,available_at,started_at,finished_at,"
                    "last_error_code,result_code,version,created_at"
                    ") VALUES ("
                    ":id,:job_key,:input_hash,'queued',:schema,CAST(:payload AS jsonb),"
                    "'collect.intraday_snapshot','provider','provider',:scope_id,:now,50,"
                    "'never',1,0,0,:now,NULL,NULL,NULL,NULL,1,:now)"
                ),
                {
                    "id": job_id,
                    "job_key": f"schwab-v3-admission:{job_id}",
                    "input_hash": "a" * 64,
                    "schema": schema,
                    "payload": json.dumps(payload),
                    "scope_id": f"{source}-intraday",
                    "now": now,
                },
            )
    finally:
        await engine.dispose()
    return job_id


async def _job_exists(database_url: str, job_id: UUID) -> bool:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return bool(
                await connection.scalar(
                    text("SELECT EXISTS (SELECT 1 FROM jobs WHERE id=:id)"),
                    {"id": job_id},
                )
            )
    finally:
        await engine.dispose()


async def _migration_head(database_url: str) -> str | None:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return await connection.scalar(text("SELECT version_num FROM alembic_version"))
    finally:
        await engine.dispose()


async def _terminalize_job(database_url: str, job_id: UUID) -> None:
    now = datetime(2026, 1, 1, 0, 1, tzinfo=UTC)
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE jobs SET status='running',started_at=:now,version=version+1 "
                    "WHERE id=:id"
                ),
                {"id": job_id, "now": now},
            )
            await connection.execute(
                text(
                    "UPDATE jobs SET status='dead_letter',finished_at=:now,"
                    "last_error_code='DRAINED_FOR_DOWNGRADE',version=version+1 WHERE id=:id"
                ),
                {"id": job_id, "now": now},
            )
    finally:
        await engine.dispose()
