"""Empty monitoring schemas can roll back; persisted audit history cannot."""

import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from integration.test_research_monitoring_pg import seed
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def revision(url):
    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            return await connection.scalar(text("SELECT version_num FROM alembic_version"))
    finally:
        await engine.dispose()


def test_empty_schema_round_trip_and_history_downgrade_fence(migrated_database_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    config = Config(str(Path(__file__).parents[2] / "alembic.ini"))
    command.upgrade(config, "head")
    assert asyncio.run(revision(migrated_database_url)) == "0093_monitor_kinds"
    command.downgrade(config, "0086_accounting_adoption")
    assert asyncio.run(revision(migrated_database_url)) == "0086_accounting_adoption"
    command.upgrade(config, "head")
    asyncio.run(seed(migrated_database_url))
    with pytest.raises(Exception, match="MONITOR_HISTORY_DOWNGRADE_FORBIDDEN"):
        command.downgrade(config, "0086_accounting_adoption")
    assert asyncio.run(revision(migrated_database_url)) == "0093_monitor_kinds"
