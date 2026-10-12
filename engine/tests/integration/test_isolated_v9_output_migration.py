"""The new guard is reversible before qualified v9 facts and preserves old SQL."""

import asyncio
from pathlib import Path

from alembic import command
from alembic.config import Config
from nexus.agents.codex_capacity import capacity_config_hash
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


async def definition(url):
    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            return await connection.scalar(
                text(
                    "SELECT pg_get_functiondef("
                    "'public.nexus_agent_output_binding_valid()'::regprocedure)"
                )
            )
    finally:
        await engine.dispose()


def test_isolated_v9_migration_roundtrip_preserves_frozen_old_guard(migrated_database_url):
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    upgraded = asyncio.run(definition(migrated_database_url))
    assert "codex-sdk-chatgpt-v9-full-input-capacity" in upgraded
    assert capacity_config_hash() in upgraded
    assert "current_database() ~ '^nexus_(test|diag)_'" in upgraded
    command.downgrade(config, "0075_manual_paper_requests")
    old = asyncio.run(definition(migrated_database_url))
    assert "codex-sdk-chatgpt-v9-full-input-capacity" not in old
    assert "codex-sdk-chatgpt-v8-stage-capacity" in old
    assert "codex-sdk-chatgpt-v7-stage-capacity-candidate" in old
    command.upgrade(config, "head")
    assert asyncio.run(definition(migrated_database_url)) == upgraded
