"""Alembic boundary tests for append-only Agent research persistence."""

import asyncio
import subprocess
from pathlib import Path

from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import inspect
from sqlalchemy.ext.asyncio import create_async_engine

EXPECTED_TASK_15A_FUNCTIONS = {
    "nexus_price_bar_set_canonical_v1",
    "nexus_prepare_price_bar_set",
    "nexus_validate_price_bar_set_authority",
    "nexus_guard_price_bar_set_mutation",
}
EXPECTED_TASK_15A_TRIGGERS = {
    "trg_price_bar_set_authorities_validate",
    "trg_price_bar_set_authorities_immutable",
    "trg_00_price_bar_records_authority_guard",
}


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
    assert _task_15a_tables(temporary_database_url) == set()
    assert _task_15a_functions(temporary_database_url) == set()
    assert _task_15a_triggers(temporary_database_url) == set()
    before = _agent_tables(temporary_database_url)
    command.upgrade(config, "0011_agents")
    assert _task_15a_tables(temporary_database_url) == {"price_bar_set_authorities"}
    assert _task_15a_functions(temporary_database_url) == EXPECTED_TASK_15A_FUNCTIONS
    assert _task_15a_triggers(temporary_database_url) == EXPECTED_TASK_15A_TRIGGERS
    assert _extensions(temporary_database_url) >= {"pgcrypto"}
    first = _agent_tables(temporary_database_url)
    command.downgrade(config, "0010_research_data")
    assert _task_15a_tables(temporary_database_url) == set()
    assert _task_15a_functions(temporary_database_url) == set()
    assert _task_15a_triggers(temporary_database_url) == set()
    assert _extensions(temporary_database_url) >= {"pgcrypto"}
    downgraded = _agent_tables(temporary_database_url)
    command.upgrade(config, "0011_agents")
    assert _task_15a_tables(temporary_database_url) == {"price_bar_set_authorities"}
    assert _task_15a_functions(temporary_database_url) == EXPECTED_TASK_15A_FUNCTIONS
    assert _task_15a_triggers(temporary_database_url) == EXPECTED_TASK_15A_TRIGGERS
    assert _extensions(temporary_database_url) >= {"pgcrypto"}
    second = _agent_tables(temporary_database_url)
    assert before == downgraded == set()
    assert (
        first
        == second
        == {
            "agent_prompt_versions",
            "agent_model_configs",
            "agent_research_runs",
            "agent_research_events",
            "agent_research_outputs",
            "agent_research_output_evidence",
            "agent_data_snapshot_evidence",
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


def _task_15a_tables(database_url: str) -> set[str]:
    async def inspect_tables() -> set[str]:
        engine = create_async_engine(database_url)
        try:
            async with engine.connect() as connection:
                names = await connection.run_sync(lambda sync: inspect(sync).get_table_names())
            return {name for name in names if name == "price_bar_set_authorities"}
        finally:
            await engine.dispose()

    return asyncio.run(inspect_tables())


def _task_15a_functions(database_url: str) -> set[str]:
    async def inspect_functions() -> set[str]:
        engine = create_async_engine(database_url)
        try:
            async with engine.connect() as connection:
                rows = await connection.exec_driver_sql(
                    "SELECT proname FROM pg_proc "
                    "WHERE pronamespace = current_schema()::regnamespace "
                    "AND proname IN ("
                    "'nexus_guard_price_bar_set_mutation',"
                    "'nexus_prepare_price_bar_set',"
                    "'nexus_price_bar_set_canonical_v1',"
                    "'nexus_validate_price_bar_set_authority')"
                )
                return {str(name) for name in rows.scalars()}
        finally:
            await engine.dispose()

    return asyncio.run(inspect_functions())


def _task_15a_triggers(database_url: str) -> set[str]:
    async def inspect_triggers() -> set[str]:
        engine = create_async_engine(database_url)
        try:
            async with engine.connect() as connection:
                rows = await connection.exec_driver_sql(
                    "SELECT trigger.tgname FROM pg_trigger AS trigger "
                    "JOIN pg_class AS relation ON relation.oid = trigger.tgrelid "
                    "WHERE NOT trigger.tgisinternal "
                    "AND relation.relnamespace = current_schema()::regnamespace "
                    "AND trigger.tgname IN ("
                    "'trg_00_price_bar_records_authority_guard',"
                    "'trg_price_bar_set_authorities_validate',"
                    "'trg_price_bar_set_authorities_immutable')"
                )
                return {str(name) for name in rows.scalars()}
        finally:
            await engine.dispose()

    return asyncio.run(inspect_triggers())


def _extensions(database_url: str) -> set[str]:
    async def inspect_extensions() -> set[str]:
        engine = create_async_engine(database_url)
        try:
            async with engine.connect() as connection:
                rows = await connection.exec_driver_sql("SELECT extname FROM pg_extension")
                return {str(name) for name in rows.scalars()}
        finally:
            await engine.dispose()

    return asyncio.run(inspect_extensions())


async def _agent_tables_async(database_url: str) -> set[str]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            names = await connection.run_sync(lambda sync: inspect(sync).get_table_names())
        return {name for name in names if name.startswith("agent_")}
    finally:
        await engine.dispose()
