import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import create_async_engine

POSTGRES_TEST_URL = "postgresql+asyncpg://nexus:nexus-local-fixture@127.0.0.1:5432/nexus"

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


def test_initial_migration_creates_the_runtime_and_data_tables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An empty PostgreSQL database receives every Phase 0–1 runtime/data table."""
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", POSTGRES_TEST_URL)
    config = Config(str(engine_root / "alembic.ini"))

    command.downgrade(config, "base")
    command.upgrade(config, "head")

    actual_tables = asyncio.run(_table_names())

    assert EXPECTED_TABLES <= actual_tables


async def _table_names() -> set[str]:
    engine = create_async_engine(POSTGRES_TEST_URL)
    try:
        async with engine.connect() as connection:
            return await connection.run_sync(
                lambda sync_connection: set(inspect(sync_connection).get_table_names())
            )
    finally:
        await engine.dispose()
