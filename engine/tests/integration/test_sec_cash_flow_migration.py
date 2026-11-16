"""The additive cash-flow enum has an honest downgrade boundary."""

import asyncio

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.mark.asyncio
async def test_empty_component_schema_can_return_to_0120(migrated_database_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    await asyncio.to_thread(
        command.downgrade, Config("engine/alembic.ini"), "0120_sol_formal_output_binding"
    )
    engine = create_async_engine(migrated_database_url)
    try:
        async with engine.connect() as conn:
            assert (
                await conn.scalar(text("SELECT version_num FROM alembic_version"))
                == "0120_sol_formal_output_binding"
            )
    finally:
        await engine.dispose()
    await asyncio.to_thread(command.upgrade, Config("engine/alembic.ini"), "head")
