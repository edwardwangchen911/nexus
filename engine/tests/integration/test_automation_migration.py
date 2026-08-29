"""Task 20 resilient automation and audit migration contract."""

import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_CONFIG = Path(__file__).parents[2] / "alembic.ini"
TASK20_TABLES = {
    "job_attempts",
    "job_dependencies",
    "worker_heartbeats",
    "outbox_delivery_attempts",
    "audit_chain_manifests",
}
JOB_COLUMNS = {
    "job_type",
    "queue",
    "scope_kind",
    "scope_id",
    "effective_at",
    "priority",
    "retry_policy",
    "max_attempts",
    "attempt_count",
    "manual_retry_count",
    "available_at",
    "started_at",
    "finished_at",
    "last_error_code",
    "result_code",
    "version",
}
AUDIT_COLUMNS = {
    "event_seq",
    "occurred_at",
    "previous_hash",
    "canonical_event",
    "event_hash",
    "chain_version",
}


def _config() -> Config:
    return Config(str(ALEMBIC_CONFIG))


async def _rows(database_url: str, statement: str) -> list[tuple[object, ...]]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return list((await connection.execute(text(statement))).tuples())
    finally:
        await engine.dispose()


async def _seed_legacy_runtime(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO jobs "
                    "(id, job_key, input_hash, status, schema_version, payload, created_at) "
                    "VALUES "
                    "('72000000-0000-0000-0000-000000000001', 'agent-research:legacy', "
                    ":first_hash, 'QUEUED', 'agent_research_job_v1', "
                    "'{\"run_id\":\"72000000-0000-0000-0000-000000000011\"}'::jsonb, "
                    "'2026-08-27T01:00:00Z'), "
                    "('72000000-0000-0000-0000-000000000002', "
                    "'optimization-proposal:legacy', :second_hash, 'COMPLETED', '1', "
                    "'{\"portfolio_snapshot_id\":\"72000000-0000-0000-0000-000000000012\"}'"
                    "::jsonb, '2026-08-27T01:01:00Z')"
                ),
                {"first_hash": "a" * 64, "second_hash": "b" * 64},
            )
            await connection.execute(
                text(
                    "INSERT INTO audit_events "
                    "(id, actor_id, action, schema_version, payload, created_at) VALUES "
                    "('72000000-0000-0000-0000-000000000101', 'system:legacy', "
                    "'job.queued', '1', '{\"status\":\"queued\"}'::jsonb, "
                    "'2026-08-27T01:00:00Z'), "
                    "('72000000-0000-0000-0000-000000000102', 'system:legacy', "
                    "'job.succeeded', '1', '{\"status\":\"succeeded\"}'::jsonb, "
                    "'2026-08-27T01:01:00Z')"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO outbox_events "
                    "(id, event_type, aggregate_id, schema_version, payload, published_at, "
                    "created_at) VALUES "
                    "('72000000-0000-0000-0000-000000000201', 'paper.one', "
                    "'72000000-0000-0000-0000-000000000001', '1', '{}'::jsonb, NULL, "
                    "'2026-08-27T01:00:00Z'), "
                    "('72000000-0000-0000-0000-000000000202', 'paper.two', "
                    "'72000000-0000-0000-0000-000000000002', '1', '{}'::jsonb, "
                    "'2026-08-27T01:02:00Z', '2026-08-27T01:01:00Z')"
                )
            )
    finally:
        await engine.dispose()


async def _audit_mutations_are_rejected(database_url: str) -> tuple[bool, bool]:
    engine = create_async_engine(database_url)
    try:
        with pytest.raises(DBAPIError, match="audit"):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE audit_events SET action = 'rewritten' "
                        "WHERE event_seq = 1"
                    )
                )
        with pytest.raises(DBAPIError, match="audit"):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO audit_events "
                        "(id, actor_id, action, schema_version, payload, event_seq, "
                        "occurred_at, previous_hash, canonical_event, event_hash, "
                        "chain_version) VALUES "
                        "('72000000-0000-0000-0000-000000000103', 'system:test', "
                        "'audit.forged', '1', '{}'::jsonb, 3, now(), :previous_hash, "
                        "'{}', :event_hash, 'audit_jcs_sha256_v1')"
                    ),
                    {"previous_hash": "f" * 64, "event_hash": "e" * 64},
                )
        return True, True
    finally:
        await engine.dispose()


async def _runtime_mutations_are_rejected(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        with pytest.raises(DBAPIError, match="job transition"):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE jobs SET status='succeeded', started_at=created_at, "
                        "finished_at=created_at, attempt_count=1, version=version+1 "
                        "WHERE id='72000000-0000-0000-0000-000000000001'"
                    )
                )
        with pytest.raises(DBAPIError, match="terminal job"):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE jobs SET status='running', finished_at=NULL, "
                        "version=version+1 "
                        "WHERE id='72000000-0000-0000-0000-000000000002'"
                    )
                )
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO job_leases "
                    "(id, job_id, worker_id, expires_at, lease_token, fence_token, "
                    "acquired_at, heartbeat_at, created_at) VALUES "
                    "('72000000-0000-0000-0000-000000000301', "
                    "'72000000-0000-0000-0000-000000000001', 'worker-one', "
                    "'2026-08-27T01:02:00Z', "
                    "'72000000-0000-0000-0000-000000000302', 5, "
                    "'2026-08-27T01:00:00Z', '2026-08-27T01:01:00Z', "
                    "'2026-08-27T01:00:00Z')"
                )
            )
        with pytest.raises(DBAPIError, match="job lease fence"):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE job_leases SET fence_token=4, "
                        "lease_token='72000000-0000-0000-0000-000000000303' "
                        "WHERE id='72000000-0000-0000-0000-000000000301'"
                    )
                )
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO worker_heartbeats "
                    "(id, queue, worker_id, process_version, migration_head, "
                    "started_at, seen_at, created_at) VALUES "
                    "('72000000-0000-0000-0000-000000000401', 'general', "
                    "'worker-one', 'v1', '0021_manual_job_retry_allowance', "
                    "'2026-08-27T01:00:00Z', '2026-08-27T01:01:00Z', "
                    "'2026-08-27T01:00:00Z')"
                )
            )
        with pytest.raises(DBAPIError, match="worker heartbeat"):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE worker_heartbeats SET seen_at='2026-08-27T00:59:00Z' "
                        "WHERE id='72000000-0000-0000-0000-000000000401'"
                    )
                )
        with pytest.raises(DBAPIError, match="terminal outbox"):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE outbox_events SET payload=CAST(:payload AS jsonb) "
                        "WHERE id='72000000-0000-0000-0000-000000000202'"
                    ),
                    {"payload": '{"forged":true}'},
                )
        with pytest.raises(DBAPIError, match="outbox attempt"):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "UPDATE outbox_delivery_attempts SET outcome='failed', "
                        "error_code='forged' "
                        "WHERE outbox_event_id="
                        "'72000000-0000-0000-0000-000000000202'"
                    )
                )
    finally:
        await engine.dispose()


def test_0020_is_the_single_head_after_task19() -> None:
    scripts = ScriptDirectory.from_config(_config())

    assert scripts.get_heads() == ["0026_intraday_review_hardening"]
    automation = scripts.get_revision("0020_resilient_automation_audit")
    retry_allowance = scripts.get_revision("0021_manual_job_retry_allowance")
    assert automation is not None
    assert automation.down_revision == "0019_paper_performance_root"
    assert retry_allowance is not None
    assert retry_allowance.down_revision == "0020_resilient_automation_audit"


def test_0020_backfills_runtime_authorities_guards_audit_and_round_trips(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = _config()
    command.upgrade(config, "0019_paper_performance_root")
    asyncio.run(_seed_legacy_runtime(temporary_database_url))

    command.upgrade(config, "head")
    tables = {
        str(row[0])
        for row in asyncio.run(
            _rows(
                temporary_database_url,
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public'",
            )
        )
    }
    assert TASK20_TABLES <= tables
    columns = {
        str(row[0])
        for row in asyncio.run(
            _rows(
                temporary_database_url,
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'jobs'",
            )
        )
    }
    assert JOB_COLUMNS <= columns
    audit_columns = {
        str(row[0])
        for row in asyncio.run(
            _rows(
                temporary_database_url,
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'audit_events'",
            )
        )
    }
    assert AUDIT_COLUMNS <= audit_columns

    jobs = asyncio.run(
        _rows(
            temporary_database_url,
            "SELECT job_type, queue, scope_kind, scope_id, status, priority, "
            "retry_policy, max_attempts, attempt_count, manual_retry_count, version "
            "FROM jobs ORDER BY created_at",
        )
    )
    assert jobs == [
        (
            "agent.research",
            "general",
            "global",
            "global",
            "queued",
            180,
            "automatic_safe",
            3,
            0,
            0,
            1,
        ),
        (
            "optimization.propose",
            "general",
            "global",
            "global",
            "succeeded",
            180,
            "manual_safe",
            3,
            1,
            0,
            1,
        ),
    ]
    assert asyncio.run(
        _rows(
            temporary_database_url,
            "SELECT attempt_no, outcome FROM job_attempts ORDER BY job_id, attempt_no",
        )
    ) == [(1, "succeeded")]
    assert asyncio.run(
        _rows(
            temporary_database_url,
            "SELECT status, attempt_count, max_attempts FROM outbox_events ORDER BY id",
        )
    ) == [("queued", 0, 3), ("published", 1, 3)]

    audit = asyncio.run(
        _rows(
            temporary_database_url,
            "SELECT event_seq, occurred_at, actor_id, action, schema_version, payload, "
            "previous_hash, canonical_event, event_hash, chain_version "
            "FROM audit_events ORDER BY event_seq",
        )
    )
    previous_hash = "0" * 64
    for expected_seq, row in enumerate(audit, start=1):
        assert row[0] == expected_seq
        event = AuditChainInput(
            event_seq=expected_seq,
            occurred_at=row[1],
            actor_id=row[2],
            action=row[3],
            schema_version=row[4],
            payload=row[5],
        )
        canonical = canonical_audit_event(event)
        assert row[6] == previous_hash
        assert row[7] == canonical
        assert row[8] == audit_event_hash(previous_hash, canonical)
        assert row[9] == "audit_jcs_sha256_v1"
        previous_hash = row[8]
    assert asyncio.run(_audit_mutations_are_rejected(temporary_database_url)) == (True, True)
    asyncio.run(_runtime_mutations_are_rejected(temporary_database_url))

    command.downgrade(config, "0019_paper_performance_root")
    downgraded_tables = {
        str(row[0])
        for row in asyncio.run(
            _rows(
                temporary_database_url,
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'public'",
            )
        )
    }
    assert TASK20_TABLES.isdisjoint(downgraded_tables)
    downgraded_job_columns = {
        str(row[0])
        for row in asyncio.run(
            _rows(
                temporary_database_url,
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'jobs'",
            )
        )
    }
    assert JOB_COLUMNS.isdisjoint(downgraded_job_columns)
    downgraded_audit_columns = {
        str(row[0])
        for row in asyncio.run(
            _rows(
                temporary_database_url,
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = 'public' AND table_name = 'audit_events'",
            )
        )
    }
    assert AUDIT_COLUMNS.isdisjoint(downgraded_audit_columns)
    assert asyncio.run(
        _rows(
            temporary_database_url,
            "SELECT status FROM jobs ORDER BY created_at",
        )
    ) == [("QUEUED",), ("COMPLETED",)]

    command.upgrade(config, "head")
    assert asyncio.run(
        _rows(temporary_database_url, "SELECT version_num FROM alembic_version")
    ) == [("0026_intraday_review_hardening",)]
