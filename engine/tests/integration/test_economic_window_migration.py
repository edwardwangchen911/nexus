"""Candidate common-window DDL rollback remains empty-only."""

import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine


@pytest.mark.parametrize(
    "revision",
    [
        "0071_economic_pair_windows",
        "0072_economic_later_windows",
        "0073_portfolio_plan_continuation",
        "0074_economic_plan_windows",
    ],
)
def test_empty_common_windows_roundtrip(monkeypatch, temporary_database_url, revision):
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    command.upgrade(config, revision)
    command.downgrade(config, "0070_economic_pair_identity")

    async def verify():
        engine = create_async_engine(temporary_database_url)
        try:
            async with engine.connect() as connection:
                for name in ("economic_common_windows", "economic_window_orders"):
                    assert (
                        await connection.scalar(text("SELECT to_regclass(:name)"), {"name": name})
                        is None
                    )
                assert await connection.scalar(text("SELECT to_regclass('economic_pair_slots')"))
                function = await connection.scalar(
                    text(
                        "SELECT pg_get_functiondef("
                        "'nexus_paper_execution_window_chain_valid()'::regprocedure)"
                    )
                )
                assert "economic_common_windows" not in function
                assert "window_start_at IS DISTINCT FROM prior_end_at" in function
        finally:
            await engine.dispose()

    asyncio.run(verify())
