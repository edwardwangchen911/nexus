import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.autogenerate import compare_metadata
from alembic.config import Config
from alembic.migration import MigrationContext
from nexus.persistence.base import Base
from nexus.persistence.models import data, runtime  # noqa: F401
from sqlalchemy import DateTime, inspect, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

EXPECTED_TABLES = {
    "provider_runs",
    "provider_capabilities",
    "source_policies",
    "data_snapshots",
    "quality_reports",
    "data_artifacts",
    "price_series_manifests",
    "macro_observations",
    "integration_status",
    "jobs",
    "job_leases",
    "outbox_events",
    "idempotency_records",
    "audit_events",
    "used_nonces",
}

EXPECTED_FOREIGN_KEYS = {
    "quality_reports": "fk_quality_reports_data_snapshot_id_data_snapshots",
    "price_series_manifests": "fk_price_series_manifests_data_artifact_id_data_artifacts",
    "job_leases": "fk_job_leases_job_id_jobs",
}


def test_initial_migration_creates_an_exact_reversible_schema(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    """An empty isolated database upgrades, downgrades, and upgrades without schema drift."""
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))

    command.upgrade(config, "head")
    _assert_schema(temporary_database_url)
    command.downgrade(config, "base")
    assert _table_names(temporary_database_url) == {"alembic_version"}
    command.upgrade(config, "head")
    _assert_schema(temporary_database_url)


def _assert_schema(database_url: str) -> None:
    asyncio.run(_assert_schema_async(database_url))


async def _assert_schema_async(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            await connection.run_sync(_assert_reflected_schema)
    finally:
        await engine.dispose()


def _assert_reflected_schema(connection: Connection) -> None:
    inspector = inspect(connection)
    assert set(inspector.get_table_names()) == EXPECTED_TABLES | {"alembic_version"}
    assert {inspector.get_pk_constraint(table_name)["name"] for table_name in EXPECTED_TABLES} == {
        f"pk_{table_name}" for table_name in EXPECTED_TABLES
    }
    assert {constraint["name"] for constraint in inspector.get_unique_constraints("jobs")} == {
        "uq_jobs_job_key"
    }
    idempotency_constraints = inspector.get_unique_constraints("idempotency_records")
    assert {constraint["name"] for constraint in idempotency_constraints} == {
        "uq_idempotency_records_actor_route_key"
    }
    nonce_constraints = inspector.get_unique_constraints("used_nonces")
    assert {constraint["name"] for constraint in nonce_constraints} == {
        "uq_used_nonces_key_nonce"
    }
    for table_name, foreign_key_name in EXPECTED_FOREIGN_KEYS.items():
        assert {foreign_key["name"] for foreign_key in inspector.get_foreign_keys(table_name)} == {
            foreign_key_name
        }

    artifacts = {column["name"]: column for column in inspector.get_columns("data_artifacts")}
    assert isinstance(artifacts["id"]["type"], postgresql.UUID)
    assert artifacts["uri"]["nullable"] is False
    observed_at_type = artifacts["observed_at"]["type"]
    assert isinstance(observed_at_type, DateTime)
    assert observed_at_type.timezone is True
    macro_columns = {
        column["name"]: column for column in inspector.get_columns("macro_observations")
    }
    macro_value = macro_columns["value"]
    assert isinstance(macro_value["type"], postgresql.NUMERIC)
    assert macro_value["type"].precision == 38
    assert macro_value["type"].scale == 12
    for table_name in EXPECTED_TABLES:
        assert _created_at_trigger_exists(connection, table_name)

    context = MigrationContext.configure(connection)
    assert compare_metadata(context, Base.metadata) == []


def _created_at_trigger_exists(connection: Connection, table_name: str) -> bool:
    trigger_name = f"trg_{table_name}_created_at_immutable"
    return bool(
        connection.execute(
            text(
                "SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = :trigger_name "
                "AND NOT tgisinternal)"
            ),
            {"trigger_name": trigger_name},
        ).scalar_one()
    )


def _table_names(database_url: str) -> set[str]:
    return asyncio.run(_table_names_async(database_url))


async def _table_names_async(database_url: str) -> set[str]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return await connection.run_sync(
                lambda sync_connection: set(inspect(sync_connection).get_table_names())
            )
    finally:
        await engine.dispose()
