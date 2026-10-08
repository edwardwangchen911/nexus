"""Ownership migration preserves existing guards and supports an empty rollback."""

import asyncio
from io import StringIO
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def definition(database_url):
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return await connection.scalar(
                text(
                    "SELECT pg_get_functiondef("
                    "'public.nexus_validate_agent_snapshot_evidence()'::regprocedure)"
                )
            )
    finally:
        await engine.dispose()


async def apply_script(database_url, script):
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            raw = await connection.get_raw_connection()
            await raw.driver_connection.execute(script)
    finally:
        await engine.dispose()


def test_identity_migration_roundtrip_preserves_existing_guards(
    temporary_database_url, monkeypatch
):
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    scripts = ScriptDirectory.from_config(config)
    assert scripts.get_heads() == [CURRENT_MIGRATION_HEAD]
    command.upgrade(config, "0065_portfolio_management")
    before = asyncio.run(definition(temporary_database_url))
    command.upgrade(config, "head")
    after = asyncio.run(definition(temporary_database_url))
    assert "research_current_identity" in after and "research_current_identity" not in before
    for guard in (
        "Agent price evidence authority is invalid",
        "future evidence cannot bind",
        "fosun_security_classification",
        "alpaca_current_trends",
    ):
        assert guard in before and guard in after
    command.downgrade(config, "0065_portfolio_management")
    assert asyncio.run(definition(temporary_database_url)) == before
    # Generated SQL must actually apply, rather than merely render successfully.
    buffer = StringIO()
    offline = Config(config.config_file_name, output_buffer=buffer)
    command.upgrade(offline, "0065_portfolio_management:head", sql=True)
    asyncio.run(apply_script(temporary_database_url, buffer.getvalue()))
    assert asyncio.run(definition(temporary_database_url)) == after
    buffer.truncate(0)
    buffer.seek(0)
    command.downgrade(offline, "head:0065_portfolio_management", sql=True)
    asyncio.run(apply_script(temporary_database_url, buffer.getvalue()))
    assert asyncio.run(definition(temporary_database_url)) == before
