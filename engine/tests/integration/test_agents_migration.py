"""Alembic boundary tests for append-only Agent research persistence."""

import asyncio
import subprocess
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import create_async_engine


def test_0011_agents_is_the_single_head_with_the_research_data_parent() -> None:
    """A split head or wrong parent could silently omit immutable Agent audit records."""
    engine_root = Path(__file__).resolve().parents[2]
    scripts = ScriptDirectory.from_config(Config(str(engine_root / "alembic.ini")))
    revision = scripts.get_revision("0011_agents")
    assert scripts.get_heads() == ["0011_agents"]
    assert revision is not None
    assert revision.down_revision == "0010_research_data"


def test_0011_round_trips_from_previous_release(temporary_database_url: str, monkeypatch) -> None:
    """The unreleased migration must support 0010->0011->0010->0011 without residue."""
    engine_root = Path(__file__).resolve().parents[2]
    config = Config(str(engine_root / "alembic.ini"))
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(config, "0010_research_data")
    before = _agent_tables(temporary_database_url)
    command.upgrade(config, "0011_agents")
    first = _agent_tables(temporary_database_url)
    command.downgrade(config, "0010_research_data")
    downgraded = _agent_tables(temporary_database_url)
    command.upgrade(config, "0011_agents")
    second = _agent_tables(temporary_database_url)
    assert before == downgraded == set()
    assert (
        first
        == second
        == {
            "agent_prompt_versions",
            "agent_model_configs",
            "agent_research_runs",
            "agent_research_outputs",
            "agent_research_output_evidence",
            "agent_daily_budget_reservations",
        }
    )


def test_migrations_0001_through_0010_match_rescue_base() -> None:
    """Task 15 may edit only its unreleased 0011 migration."""
    root = Path(__file__).resolve().parents[3]
    migration_dir = root / "engine/alembic/versions"
    for path in sorted(migration_dir.glob("00*.py")):
        if path.name.startswith("0011_"):
            continue
        relative = path.relative_to(root)
        base_bytes = subprocess.check_output(
            ["git", "show", f"9e63793d5be220e3b404d316156528607c31f561:{relative}"],
            cwd=root,
        )
        assert path.read_bytes() == base_bytes


def _agent_tables(database_url: str) -> set[str]:
    return asyncio.run(_agent_tables_async(database_url))


async def _agent_tables_async(database_url: str) -> set[str]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            names = await connection.run_sync(lambda sync: inspect(sync).get_table_names())
        return {name for name in names if name.startswith("agent_")}
    finally:
        await engine.dispose()
