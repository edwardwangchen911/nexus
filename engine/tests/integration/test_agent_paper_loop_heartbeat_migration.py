"""Agent paper-loop heartbeat schema and rollback fences."""

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.jobs.models import QueueName
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

ENGINE_ROOT = Path(__file__).parents[2]
MIGRATION_HEAD = "0060_agent_paper_loop_heartbeat"
PREVIOUS_HEAD = "0059_agent_paper_intraday_cycles"


def test_0060_is_the_single_head_and_exposes_agent_paper_queue() -> None:
    scripts = ScriptDirectory.from_config(Config(str(ENGINE_ROOT / "alembic.ini")))

    assert CURRENT_MIGRATION_HEAD == MIGRATION_HEAD
    assert scripts.get_heads() == [MIGRATION_HEAD]
    revision = scripts.get_revision(MIGRATION_HEAD)
    assert revision is not None
    assert revision.down_revision == PREVIOUS_HEAD
    assert QueueName.AGENT_PAPER.value == "agent_paper"


def test_0060_upgrade_admits_agent_paper_heartbeat_and_downgrade_refuses_history(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(ENGINE_ROOT / "alembic.ini"))
    command.upgrade(config, PREVIOUS_HEAD)

    command.upgrade(config, MIGRATION_HEAD)
    asyncio.run(_insert_agent_paper_heartbeat(temporary_database_url))

    with pytest.raises(DBAPIError, match="agent paper loop heartbeat prevents downgrade") as error:
        command.downgrade(config, PREVIOUS_HEAD)

    assert error.value.orig.sqlstate == "P3D01"
    assert asyncio.run(_migration_head(temporary_database_url)) == MIGRATION_HEAD


def test_0060_empty_round_trip_restores_the_previous_queue_contract(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(ENGINE_ROOT / "alembic.ini"))
    command.upgrade(config, MIGRATION_HEAD)

    command.downgrade(config, PREVIOUS_HEAD)

    with pytest.raises(DBAPIError) as error:
        asyncio.run(_insert_agent_paper_heartbeat(temporary_database_url))
    assert error.value.orig.sqlstate == "23514"


async def _insert_agent_paper_heartbeat(database_url: str) -> None:
    now = datetime.now(UTC)
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO worker_heartbeats ("
                    "id,queue,worker_id,process_version,migration_head,"
                    "started_at,seen_at,created_at"
                    ") VALUES ("
                    ":id,'agent_paper',:worker_id,'agent-paper-loop-v1',"
                    ":migration_head,:now,:now,:now)"
                ),
                {
                    "id": uuid4(),
                    "worker_id": f"agent-paper-loop-{uuid4()}",
                    "migration_head": MIGRATION_HEAD,
                    "now": now,
                },
            )
    finally:
        await engine.dispose()


async def _migration_head(database_url: str) -> str | None:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return await connection.scalar(text("SELECT version_num FROM alembic_version"))
    finally:
        await engine.dispose()
