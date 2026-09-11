"""Migration boundary for research-only portfolio scenario selections."""

from __future__ import annotations

import asyncio

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


def test_0051_is_single_migration_head() -> None:
    config = Config("engine/alembic.ini")
    scripts = ScriptDirectory.from_config(config)
    revision = scripts.get_revision("0051_scenario_selections")

    assert revision is not None
    assert revision.down_revision == "0050_scenario_spread_cost_policy"
    assert scripts.get_heads() == [CURRENT_MIGRATION_HEAD]


def test_0051_clean_downgrade_removes_selection_projection(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = Config("engine/alembic.ini")
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(config, "0051_scenario_selections")
    assert asyncio.run(_table_exists(temporary_database_url)) is True

    command.downgrade(config, "0050_scenario_spread_cost_policy")

    assert asyncio.run(_table_exists(temporary_database_url)) is False


async def _table_exists(database_url: str) -> bool:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return bool(
                await connection.scalar(
                    text(
                        "SELECT to_regclass('public.portfolio_scenario_selections') "
                        "IS NOT NULL"
                    )
                )
            )
    finally:
        await engine.dispose()
