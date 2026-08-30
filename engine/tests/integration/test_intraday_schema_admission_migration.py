"""Database admission guard for source-explicit intraday Job schemas."""

from __future__ import annotations

import asyncio
import json
import threading
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.provider_worker import _assert_intraday_job_queue_drained
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

_INTRADAY_JOB_TYPE = "collect.intraday_snapshot"
_INTRADAY_V2 = "intraday_snapshot_job_v2"


def test_0031_is_single_head_after_daily_staging() -> None:
    scripts = ScriptDirectory.from_config(Config("engine/alembic.ini"))

    assert scripts.get_heads() == ["0032_ticker_alias_authority"]
    admission = scripts.get_revision("0031_intraday_schema_admission")
    assert admission is not None
    assert admission.down_revision == "0030_fosun_daily_staging"


def test_0031_downgrade_locks_runtime_tables_before_inspection() -> None:
    source = Path("engine/alembic/versions/0031_intraday_schema_admission.py").read_text()
    downgrade = source.split("def downgrade() -> None:", maxsplit=1)[1]

    heartbeat_lock = downgrade.index("LOCK TABLE public.worker_heartbeats IN ACCESS EXCLUSIVE MODE")
    jobs_lock = downgrade.index("LOCK TABLE public.jobs IN ACCESS EXCLUSIVE MODE")
    inspection = downgrade.index("DO $$ BEGIN")
    assert heartbeat_lock < jobs_lock < inspection


def test_0030_downgrade_locks_staging_before_inspecting_or_dropping_it() -> None:
    source = Path("engine/alembic/versions/0030_fosun_daily_staging.py").read_text()
    downgrade = source.split("def downgrade() -> None:", maxsplit=1)[1]

    assert downgrade.index(
        "LOCK TABLE public.fosun_daily_bar_staging IN ACCESS EXCLUSIVE MODE"
    ) < downgrade.index("DO $$ BEGIN")


def test_0030_staging_constraint_matches_the_verified_49_instrument_budget() -> None:
    source = Path("engine/alembic/versions/0030_fosun_daily_staging.py").read_text()

    assert "total_instruments BETWEEN 1 AND 49" in source
    assert "ordinal < total_instruments" in source


def test_0030_downgrade_lock_conflicts_with_the_concurrent_insert_lane(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(Config("engine/alembic.ini"), "0030_fosun_daily_staging")

    lock_error = asyncio.run(_staging_downgrade_lock_conflict(temporary_database_url))

    assert lock_error == "55P03"


def test_0030_orders_manifest_binding_before_transport_locks(
    migrated_database_url: str,
) -> None:
    names = asyncio.run(_manifest_before_insert_triggers(migrated_database_url))

    assert names.index("trg_price_series_manifests_fosun_market_binding") < names.index(
        "trg_price_series_manifests_z_fosun_transport"
    )


@pytest.mark.parametrize(
    ("schema_version", "payload_schema"),
    (
        pytest.param("intraday_snapshot_job_v1", "intraday_snapshot_job_v1", id="v1"),
        pytest.param("intraday_snapshot_job_v0", "intraday_snapshot_job_v0", id="v0"),
        pytest.param("intraday_snapshot_job_v3", "intraday_snapshot_job_v3", id="v3"),
        pytest.param(_INTRADAY_V2, "intraday_snapshot_job_v1", id="payload-smuggling"),
        pytest.param(_INTRADAY_V2, None, id="payload-missing"),
    ),
)
def test_0031_rejects_new_incompatible_intraday_jobs(
    migrated_database_url: str,
    schema_version: str,
    payload_schema: str | None,
) -> None:
    with pytest.raises(DBAPIError, match="intraday Job schema is not admitted") as error:
        asyncio.run(
            _insert_job(
                migrated_database_url,
                job_type=_INTRADAY_JOB_TYPE,
                schema_version=schema_version,
                payload_schema=payload_schema,
            )
        )

    assert error.value.orig.sqlstate == "23514"


def test_0031_allows_exact_v2_and_unrelated_job_types(
    migrated_database_url: str,
) -> None:
    asyncio.run(
        _insert_job(
            migrated_database_url,
            job_type=_INTRADAY_JOB_TYPE,
            schema_version=_INTRADAY_V2,
            payload_schema=_INTRADAY_V2,
        )
    )
    asyncio.run(
        _insert_job(
            migrated_database_url,
            job_type="collect.market",
            schema_version="daily_stage_v1",
            payload_schema=None,
        )
    )


@pytest.mark.parametrize(
    ("retry_policy", "max_attempts"),
    (
        pytest.param("automatic_safe", 3, id="automatic-retry"),
        pytest.param("manual_safe", 1, id="manual-retry"),
        pytest.param("never", 2, id="multiple-attempts"),
    ),
)
def test_0031_rejects_non_one_shot_intraday_jobs(
    migrated_database_url: str,
    retry_policy: str,
    max_attempts: int,
) -> None:
    with pytest.raises(DBAPIError, match="intraday Job schema is not admitted") as error:
        asyncio.run(
            _insert_job(
                migrated_database_url,
                job_type=_INTRADAY_JOB_TYPE,
                schema_version=_INTRADAY_V2,
                payload_schema=_INTRADAY_V2,
                retry_policy=retry_policy,
                max_attempts=max_attempts,
            )
        )

    assert error.value.orig.sqlstate == "23514"


def test_0031_rejects_a_second_v2_job_for_the_same_scope_bucket(
    migrated_database_url: str,
) -> None:
    asyncio.run(
        _insert_job(
            migrated_database_url,
            job_type=_INTRADAY_JOB_TYPE,
            schema_version=_INTRADAY_V2,
            payload_schema=_INTRADAY_V2,
        )
    )

    with pytest.raises(DBAPIError) as error:
        asyncio.run(
            _insert_job(
                migrated_database_url,
                job_type=_INTRADAY_JOB_TYPE,
                schema_version=_INTRADAY_V2,
                payload_schema=_INTRADAY_V2,
            )
        )

    assert error.value.orig.sqlstate == "23505"


def test_0031_rejects_job_time_that_does_not_match_the_frozen_bucket(
    migrated_database_url: str,
) -> None:
    bucket_start = datetime(2026, 1, 1, tzinfo=UTC)

    with pytest.raises(DBAPIError, match="intraday Job schema is not admitted") as error:
        asyncio.run(
            _insert_job(
                migrated_database_url,
                job_type=_INTRADAY_JOB_TYPE,
                schema_version=_INTRADAY_V2,
                payload_schema=_INTRADAY_V2,
                effective_at=bucket_start + timedelta(microseconds=1),
                bucket_start=bucket_start,
            )
        )

    assert error.value.orig.sqlstate == "23514"


@pytest.mark.parametrize(
    ("source", "feed"),
    (
        pytest.param(None, "l1", id="missing-source"),
        pytest.param("fosun", None, id="missing-feed"),
        pytest.param("fosun", "iex", id="fosun-cross-feed"),
        pytest.param("alpaca", "l1", id="alpaca-cross-feed"),
        pytest.param("unknown", "l1", id="unknown-source"),
    ),
)
def test_0031_rejects_nonexplicit_or_invalid_source_feed_pairs(
    migrated_database_url: str,
    source: str | None,
    feed: str | None,
) -> None:
    with pytest.raises(DBAPIError, match="intraday Job schema is not admitted") as error:
        asyncio.run(
            _insert_job(
                migrated_database_url,
                job_type=_INTRADAY_JOB_TYPE,
                schema_version=_INTRADAY_V2,
                payload_schema=_INTRADAY_V2,
                source=source,
                feed=feed,
            )
        )

    assert error.value.orig.sqlstate == "23514"


@pytest.mark.parametrize(
    ("source", "feed"),
    (
        ("fosun", "l1"),
        ("alpaca", "iex"),
        ("alpaca", "sip"),
        ("alpaca", "delayed_sip"),
    ),
)
def test_0031_allows_each_explicit_source_feed_pair(
    migrated_database_url: str,
    source: str,
    feed: str,
) -> None:
    asyncio.run(
        _insert_job(
            migrated_database_url,
            job_type=_INTRADAY_JOB_TYPE,
            schema_version=_INTRADAY_V2,
            payload_schema=_INTRADAY_V2,
            source=source,
            feed=feed,
        )
    )


def test_0031_gate_rejects_preexisting_payload_schema_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0030_fosun_daily_staging")
    asyncio.run(
        _insert_job(
            temporary_database_url,
            job_type=_INTRADAY_JOB_TYPE,
            schema_version=_INTRADAY_V2,
            payload_schema="intraday_snapshot_job_v1",
        )
    )
    command.upgrade(config, "head")

    with pytest.raises(RuntimeError, match="INTRADAY_V1_QUEUE_DRAIN_REQUIRED"):
        asyncio.run(_run_intraday_gate(temporary_database_url))


def test_0031_gate_rejects_preexisting_v2_without_explicit_source(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0030_fosun_daily_staging")
    asyncio.run(
        _insert_job(
            temporary_database_url,
            job_type=_INTRADAY_JOB_TYPE,
            schema_version=_INTRADAY_V2,
            payload_schema=_INTRADAY_V2,
            source=None,
            feed="l1",
        )
    )
    command.upgrade(config, "head")

    with pytest.raises(RuntimeError, match="INTRADAY_V1_QUEUE_DRAIN_REQUIRED"):
        asyncio.run(_run_intraday_gate(temporary_database_url))


def test_0031_gate_rejects_preexisting_v2_with_retryable_tuple(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0030_fosun_daily_staging")
    asyncio.run(
        _insert_job(
            temporary_database_url,
            job_type=_INTRADAY_JOB_TYPE,
            schema_version=_INTRADAY_V2,
            payload_schema=_INTRADAY_V2,
            retry_policy="automatic_safe",
            max_attempts=3,
        )
    )
    command.upgrade(config, "head")

    with pytest.raises(RuntimeError, match="INTRADAY_V1_QUEUE_DRAIN_REQUIRED"):
        asyncio.run(_run_intraday_gate(temporary_database_url))


def test_0031_allows_drain_transitions_but_rejects_legacy_manual_retry(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0030_fosun_daily_staging")
    queued = asyncio.run(
        _insert_job(
            temporary_database_url,
            job_type=_INTRADAY_JOB_TYPE,
            schema_version="intraday_snapshot_job_v1",
            payload_schema="intraday_snapshot_job_v1",
        )
    )
    running_for_retry = asyncio.run(
        _insert_job(
            temporary_database_url,
            job_type=_INTRADAY_JOB_TYPE,
            schema_version="intraday_snapshot_job_v1",
            payload_schema="intraday_snapshot_job_v1",
            status="running",
        )
    )
    running_for_success = asyncio.run(
        _insert_job(
            temporary_database_url,
            job_type=_INTRADAY_JOB_TYPE,
            schema_version="intraday_snapshot_job_v1",
            payload_schema="intraday_snapshot_job_v1",
            status="running",
        )
    )
    failed = asyncio.run(
        _insert_job(
            temporary_database_url,
            job_type=_INTRADAY_JOB_TYPE,
            schema_version="intraday_snapshot_job_v1",
            payload_schema="intraday_snapshot_job_v1",
            status="failed",
        )
    )
    dead_letter = asyncio.run(
        _insert_job(
            temporary_database_url,
            job_type=_INTRADAY_JOB_TYPE,
            schema_version="intraday_snapshot_job_v1",
            payload_schema="intraday_snapshot_job_v1",
            status="dead_letter",
        )
    )
    wrong_time = asyncio.run(
        _insert_job(
            temporary_database_url,
            job_type=_INTRADAY_JOB_TYPE,
            schema_version=_INTRADAY_V2,
            payload_schema=_INTRADAY_V2,
            status="failed",
            effective_at=datetime(2026, 1, 1, tzinfo=UTC) + timedelta(microseconds=1),
            bucket_start=datetime(2026, 1, 1, tzinfo=UTC),
        )
    )
    command.upgrade(config, "head")

    asyncio.run(_transition_job(temporary_database_url, queued, "running"))
    asyncio.run(_transition_job(temporary_database_url, running_for_retry, "queued"))
    asyncio.run(_transition_job(temporary_database_url, running_for_success, "succeeded"))
    for job_id in (failed, dead_letter, wrong_time):
        with pytest.raises(DBAPIError, match="legacy intraday Job retry is not admitted"):
            asyncio.run(
                _transition_job(
                    temporary_database_url,
                    job_id,
                    "queued",
                    manual_retry=True,
                )
            )


def test_0031_malformed_legacy_bucket_can_terminalize_but_cannot_requeue(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    """Legacy corruption must not strand a running Job during fail-closed drain."""
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0030_fosun_daily_staging")
    job_id = asyncio.run(
        _insert_job(
            temporary_database_url,
            job_type=_INTRADAY_JOB_TYPE,
            schema_version=_INTRADAY_V2,
            payload_schema=_INTRADAY_V2,
            status="running",
            bucket_start_text="not-a-timestamp",
        )
    )
    command.upgrade(config, "head")

    with pytest.raises(RuntimeError, match="INTRADAY_V1_QUEUE_DRAIN_REQUIRED"):
        asyncio.run(_run_intraday_gate(temporary_database_url))
    asyncio.run(_transition_job(temporary_database_url, job_id, "dead_letter"))
    asyncio.run(_run_intraday_gate(temporary_database_url))
    with pytest.raises(DBAPIError, match="legacy intraday Job retry is not admitted") as error:
        asyncio.run(
            _transition_job(
                temporary_database_url,
                job_id,
                "queued",
                manual_retry=True,
            )
        )

    assert error.value.orig.sqlstate == "23514"


def test_0031_empty_upgrade_downgrade_upgrade_round_trip(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "head")
    assert asyncio.run(_admission_trigger_exists(temporary_database_url)) is True

    command.downgrade(config, "0030_fosun_daily_staging")
    assert asyncio.run(_admission_trigger_exists(temporary_database_url)) is False
    asyncio.run(
        _insert_job(
            temporary_database_url,
            job_type=_INTRADAY_JOB_TYPE,
            schema_version="intraday_snapshot_job_v1",
            payload_schema="intraday_snapshot_job_v1",
        )
    )

    command.upgrade(config, "head")
    assert asyncio.run(_admission_trigger_exists(temporary_database_url)) is True


@pytest.mark.parametrize("runtime_fact", ("actionable-job", "recent-heartbeat"))
def test_0031_downgrade_rejects_active_provider_runtime(
    monkeypatch: pytest.MonkeyPatch,
    migrated_database_url: str,
    runtime_fact: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    command.downgrade(Config("engine/alembic.ini"), "0031_intraday_schema_admission")
    if runtime_fact == "actionable-job":
        asyncio.run(
            _insert_job(
                migrated_database_url,
                job_type=_INTRADAY_JOB_TYPE,
                schema_version=_INTRADAY_V2,
                payload_schema=_INTRADAY_V2,
            )
        )
    else:
        asyncio.run(_insert_provider_heartbeat(migrated_database_url))

    with pytest.raises(DBAPIError, match="intraday admission prevents downgrade") as error:
        command.downgrade(Config("engine/alembic.ini"), "0030_fosun_daily_staging")

    assert error.value.orig.sqlstate == "P3D01"
    assert asyncio.run(_admission_trigger_exists(migrated_database_url)) is True


def test_0031_dead_letter_is_terminal_and_does_not_permanently_block_rollback(
    monkeypatch: pytest.MonkeyPatch,
    migrated_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    asyncio.run(
        _insert_job(
            migrated_database_url,
            job_type=_INTRADAY_JOB_TYPE,
            schema_version=_INTRADAY_V2,
            payload_schema=_INTRADAY_V2,
            status="dead_letter",
        )
    )

    command.downgrade(Config("engine/alembic.ini"), "0030_fosun_daily_staging")
    assert asyncio.run(_admission_trigger_exists(migrated_database_url)) is False
    command.upgrade(Config("engine/alembic.ini"), "head")
    assert asyncio.run(_admission_trigger_exists(migrated_database_url)) is True


def test_0031_downgrade_lock_serializes_with_worker_schedule(
    monkeypatch: pytest.MonkeyPatch,
    migrated_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    command.downgrade(Config("engine/alembic.ini"), "0031_intraday_schema_admission")

    lock_error = asyncio.run(_commit_v2_job_while_downgrade_lock_contends(migrated_database_url))
    assert lock_error == "55P03"
    with pytest.raises(DBAPIError, match="intraday admission prevents downgrade") as error:
        command.downgrade(Config("engine/alembic.ini"), "0030_fosun_daily_staging")

    assert error.value.orig.sqlstate == "P3D01"
    assert asyncio.run(_admission_trigger_exists(migrated_database_url)) is True


def test_0031_downgrade_rechecks_after_direct_writer_contention(
    monkeypatch: pytest.MonkeyPatch,
    migrated_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    command.downgrade(Config("engine/alembic.ini"), "0031_intraday_schema_admission")

    error_code = asyncio.run(_race_direct_v2_writer_against_0031_downgrade(migrated_database_url))

    assert error_code == "P3D01"
    assert asyncio.run(_admission_trigger_exists(migrated_database_url)) is True


async def _run_intraday_gate(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            await _assert_intraday_job_queue_drained(connection)  # type: ignore[arg-type]
    finally:
        await engine.dispose()


async def _staging_downgrade_lock_conflict(database_url: str) -> str | None:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as writer:
            writer_transaction = await writer.begin()
            try:
                await writer.execute(
                    text("LOCK TABLE public.fosun_daily_bar_staging IN ROW EXCLUSIVE MODE")
                )
                async with engine.connect() as downgrade:
                    downgrade_transaction = await downgrade.begin()
                    try:
                        await downgrade.execute(text("SET LOCAL lock_timeout='50ms'"))
                        try:
                            await downgrade.execute(
                                text(
                                    "LOCK TABLE public.fosun_daily_bar_staging "
                                    "IN ACCESS EXCLUSIVE MODE"
                                )
                            )
                        except DBAPIError as error:
                            return error.orig.sqlstate
                        return None
                    finally:
                        await downgrade_transaction.rollback()
            finally:
                await writer_transaction.rollback()
    finally:
        await engine.dispose()


async def _commit_v2_job_while_downgrade_lock_contends(database_url: str) -> str | None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    job_id = uuid4()
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as worker:
            worker_transaction = await worker.begin()
            try:
                await worker.execute(
                    text(
                        "SELECT pg_catalog.pg_advisory_xact_lock_shared("
                        "pg_catalog.hashtextextended(:key, 0))"
                    ),
                    {"key": "nexus:intraday-schema-admission:v1"},
                )
                async with engine.connect() as downgrade:
                    downgrade_transaction = await downgrade.begin()
                    try:
                        await downgrade.execute(text("SET LOCAL lock_timeout='50ms'"))
                        try:
                            await downgrade.execute(
                                text(
                                    "SELECT pg_catalog.pg_advisory_xact_lock("
                                    "pg_catalog.hashtextextended(:key, 0))"
                                ),
                                {"key": "nexus:intraday-schema-admission:v1"},
                            )
                        except DBAPIError as error:
                            lock_error = error.orig.sqlstate
                        else:
                            lock_error = None
                    finally:
                        await downgrade_transaction.rollback()
                await worker.execute(
                    text(
                        "INSERT INTO jobs ("
                        "id,job_key,input_hash,status,schema_version,payload,job_type,queue,"
                        "scope_kind,scope_id,effective_at,priority,retry_policy,max_attempts,"
                        "attempt_count,manual_retry_count,available_at,started_at,finished_at,"
                        "last_error_code,result_code,version,created_at"
                        ") VALUES ("
                        ":id,:job_key,:input_hash,'queued',:schema_version,"
                        "CAST(:payload AS jsonb),:job_type,'provider','global','intraday',"
                        ":now,50,'never',1,0,0,:now,NULL,NULL,NULL,NULL,1,:now)"
                    ),
                    {
                        "id": job_id,
                        "job_key": f"schema-admission-lock:{job_id}",
                        "input_hash": "b" * 64,
                        "schema_version": _INTRADAY_V2,
                        "payload": json.dumps(
                            {
                                "schema": _INTRADAY_V2,
                                "source": "fosun",
                                "feed": "l1",
                                "bucket_start": now.isoformat().replace("+00:00", "Z"),
                                "bucket_end": (now + timedelta(seconds=15))
                                .isoformat()
                                .replace("+00:00", "Z"),
                            }
                        ),
                        "job_type": _INTRADAY_JOB_TYPE,
                        "now": now,
                    },
                )
                await worker_transaction.commit()
            finally:
                if worker_transaction.is_active:
                    await worker_transaction.rollback()
    finally:
        await engine.dispose()
    return lock_error


async def _race_direct_v2_writer_against_0031_downgrade(database_url: str) -> str | None:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as writer:
            transaction = await writer.begin()
            await writer.execute(text("LOCK TABLE public.jobs IN ROW EXCLUSIVE MODE"))
            downgrade_started = threading.Event()

            def downgrade() -> str | None:
                downgrade_started.set()
                try:
                    command.downgrade(
                        Config("engine/alembic.ini"),
                        "0030_fosun_daily_staging",
                    )
                except DBAPIError as error:
                    return error.orig.sqlstate
                return None

            downgrade_task = asyncio.create_task(asyncio.to_thread(downgrade))
            try:
                for _ in range(500):
                    if downgrade_started.is_set():
                        break
                    if downgrade_task.done():
                        await downgrade_task
                        raise AssertionError("0031 downgrade exited before starting")
                    await asyncio.sleep(0.01)
                else:
                    raise AssertionError("0031 downgrade worker did not start")

                async with engine.connect() as observer:
                    for _ in range(500):
                        jobs_lock_is_waiting = await observer.scalar(
                            text(
                                "SELECT EXISTS ("
                                "SELECT 1 FROM pg_catalog.pg_locks "
                                "WHERE database = ("
                                "SELECT oid FROM pg_catalog.pg_database "
                                "WHERE datname = pg_catalog.current_database()"
                                ") AND relation = 'public.jobs'::pg_catalog.regclass "
                                "AND mode = 'AccessExclusiveLock' AND NOT granted"
                                ")"
                            )
                        )
                        if jobs_lock_is_waiting:
                            break
                        if downgrade_task.done():
                            result = await downgrade_task
                            raise AssertionError(
                                "0031 downgrade exited before the jobs lock boundary "
                                f"with result {result!r}"
                            )
                        await asyncio.sleep(0.01)
                    else:
                        raise AssertionError("0031 downgrade did not reach the jobs lock boundary")
                await _insert_v2_job_on_connection(writer)
                await transaction.commit()
                return await asyncio.wait_for(downgrade_task, timeout=5)
            finally:
                if transaction.is_active:
                    await transaction.rollback()
                if not downgrade_task.done():
                    downgrade_task.cancel()
                    await asyncio.gather(downgrade_task, return_exceptions=True)
    finally:
        await engine.dispose()


async def _insert_v2_job_on_connection(connection: object) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    job_id = uuid4()
    await connection.execute(  # type: ignore[attr-defined]
        text(
            "INSERT INTO jobs ("
            "id,job_key,input_hash,status,schema_version,payload,job_type,queue,"
            "scope_kind,scope_id,effective_at,priority,retry_policy,max_attempts,"
            "attempt_count,manual_retry_count,available_at,started_at,finished_at,"
            "last_error_code,result_code,version,created_at"
            ") VALUES ("
            ":id,:job_key,:input_hash,'queued',:schema_version,"
            "CAST(:payload AS jsonb),:job_type,'provider','global','intraday',"
            ":now,50,'never',1,0,0,:now,NULL,NULL,NULL,NULL,1,:now)"
        ),
        {
            "id": job_id,
            "job_key": f"schema-admission-direct-race:{job_id}",
            "input_hash": "c" * 64,
            "schema_version": _INTRADAY_V2,
            "payload": json.dumps(
                {
                    "schema": _INTRADAY_V2,
                    "source": "fosun",
                    "feed": "l1",
                    "bucket_start": now.isoformat().replace("+00:00", "Z"),
                    "bucket_end": (now + timedelta(seconds=15))
                    .isoformat()
                    .replace("+00:00", "Z"),
                }
            ),
            "job_type": _INTRADAY_JOB_TYPE,
            "now": now,
        },
    )


async def _admission_trigger_exists(database_url: str) -> bool:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return bool(
                await connection.scalar(
                    text(
                        "SELECT EXISTS (SELECT 1 FROM pg_trigger "
                        "WHERE tgrelid='public.jobs'::regclass "
                        "AND tgname='trg_jobs_intraday_schema_admission')"
                    )
                )
            )
    finally:
        await engine.dispose()


async def _manifest_before_insert_triggers(database_url: str) -> list[str]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            rows = await connection.scalars(
                text(
                    "SELECT tgname FROM pg_catalog.pg_trigger "
                    "WHERE tgrelid='public.price_series_manifests'::regclass "
                    "AND NOT tgisinternal "
                    "AND (tgtype & 2) = 2 AND (tgtype & 4) = 4 "
                    "ORDER BY tgname"
                )
            )
            return list(rows)
    finally:
        await engine.dispose()


async def _insert_provider_heartbeat(database_url: str) -> None:
    now = datetime.now(UTC)
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO worker_heartbeats ("
                    "id,queue,worker_id,process_version,migration_head,"
                    "started_at,seen_at,created_at"
                    ") VALUES ("
                    ":id,'provider',:worker_id,'test-provider',"
                    "'0031_intraday_schema_admission',:now,:now,:now)"
                ),
                {"id": uuid4(), "worker_id": f"provider-{uuid4()}", "now": now},
            )
    finally:
        await engine.dispose()


async def _insert_job(
    database_url: str,
    *,
    job_type: str,
    schema_version: str,
    payload_schema: str | None,
    status: str = "queued",
    source: str | None = "fosun",
    feed: str | None = "l1",
    retry_policy: str = "never",
    max_attempts: int = 1,
    effective_at: datetime | None = None,
    bucket_start: datetime | None = None,
    bucket_start_text: str | None = None,
) -> UUID:
    now = effective_at or datetime(2026, 1, 1, tzinfo=UTC)
    frozen_bucket_start = bucket_start or now
    payload = {} if payload_schema is None else {"schema": payload_schema}
    if source is not None:
        payload["source"] = source
    if feed is not None:
        payload["feed"] = feed
    if payload_schema is not None:
        payload["bucket_start"] = bucket_start_text or frozen_bucket_start.isoformat().replace(
            "+00:00", "Z"
        )
        payload["bucket_end"] = (frozen_bucket_start + timedelta(seconds=15)).isoformat().replace(
            "+00:00", "Z"
        )
    job_id = uuid4()
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
                    ":id,:job_key,:input_hash,:status,:schema_version,"
                    "CAST(:payload AS jsonb),:job_type,'provider','global','intraday',"
                    ":effective_at,50,:retry_policy,:max_attempts,0,0,:available_at,"
                    ":started_at,:finished_at,"
                    "NULL,NULL,1,:created_at)"
                ),
                {
                    "id": job_id,
                    "job_key": f"schema-admission:{job_id}",
                    "input_hash": "a" * 64,
                    "status": status,
                    "schema_version": schema_version,
                    "payload": json.dumps(payload),
                    "job_type": job_type,
                    "retry_policy": retry_policy,
                    "max_attempts": max_attempts,
                    "effective_at": now,
                    "available_at": now,
                    "started_at": None if status == "queued" else now,
                    "finished_at": now if status in {"failed", "dead_letter"} else None,
                    "created_at": now,
                },
            )
    finally:
        await engine.dispose()
    return job_id


async def _transition_job(
    database_url: str,
    job_id: UUID,
    target: str,
    *,
    manual_retry: bool = False,
) -> None:
    now = datetime(2026, 1, 1, 0, 1, tzinfo=UTC)
    assignments = {
        "running": ("status='running', started_at=:now, finished_at=NULL, version=version+1"),
        "queued": (
            "status='queued', started_at=NULL, finished_at=NULL, "
            "last_error_code=NULL, "
            + ("manual_retry_count=manual_retry_count+1, " if manual_retry else "")
            + "version=version+1"
        ),
        "succeeded": (
            "status='succeeded', finished_at=:now, result_code='DRAINED', version=version+1"
        ),
        "dead_letter": (
            "status='dead_letter', finished_at=:now, "
            "last_error_code='DRAINED_INVALID_LEGACY_JOB', version=version+1"
        ),
    }
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(f"UPDATE jobs SET {assignments[target]} WHERE id=:id"),
                {"id": job_id, "now": now},
            )
    finally:
        await engine.dispose()
