"""Agent paper-market binding schema and rollback fences."""

import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.paper import AgentPaperMarketWindowRecord
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

ENGINE_ROOT = Path(__file__).parents[2]
MIGRATION_PATH = ENGINE_ROOT / "alembic/versions/0057_agent_paper_market_window.py"


async def _tables(database_url: str) -> set[str]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return {
                str(row.table_name)
                for row in await connection.execute(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema='public'"
                    )
                )
            }
    finally:
        await engine.dispose()


def test_0057_remains_in_the_forward_chain_and_declares_market_binding() -> None:
    scripts = ScriptDirectory.from_config(Config(str(ENGINE_ROOT / "alembic.ini")))

    assert CURRENT_MIGRATION_HEAD == "0059_agent_paper_intraday_cycles"
    assert scripts.get_heads() == [CURRENT_MIGRATION_HEAD]
    revision = scripts.get_revision("0057_agent_paper_market_window")
    assert revision is not None
    assert revision.down_revision == "0056_agent_paper_cycle"
    assert AgentPaperMarketWindowRecord.__tablename__ == "agent_paper_market_windows"


def test_0057_has_database_binding_and_data_preserving_rollback_fences() -> None:
    source = MIGRATION_PATH.read_text()
    downgrade = source.split("def downgrade() -> None:", maxsplit=1)[1]

    assert "nexus_validate_agent_paper_market_window" in source
    assert "DEFERRABLE INITIALLY DEFERRED" in source
    assert "price_bar_set_authorities" in downgrade
    assert "agent paper market facts prevent downgrade" in downgrade


def test_0057_empty_upgrade_and_downgrade_round_trip(
    monkeypatch: pytest.MonkeyPatch, temporary_database_url: str
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(ENGINE_ROOT / "alembic.ini"))
    command.upgrade(config, "0056_agent_paper_cycle")

    command.upgrade(config, "0057_agent_paper_market_window")
    assert "agent_paper_market_windows" in asyncio.run(_tables(temporary_database_url))

    command.downgrade(config, "0056_agent_paper_cycle")
    assert "agent_paper_market_windows" not in asyncio.run(_tables(temporary_database_url))
