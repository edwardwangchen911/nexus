import asyncio

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.persistence.base import Base
from nexus.persistence.models import broker_simulation as broker_simulation_models
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

assert broker_simulation_models is not None


BROKER_TABLES = {
    "broker_account_bindings",
    "broker_execution_intents",
    "broker_execution_intent_orders",
    "broker_batch_approvals",
    "broker_commands",
    "broker_command_attempts",
    "broker_order_events",
    "broker_webhook_receipts",
    "broker_reconciliation_runs",
    "broker_reconciliation_diffs",
    "broker_simulation_admissions",
}


def test_broker_simulation_metadata_has_exact_authority_tables() -> None:
    assert BROKER_TABLES <= set(Base.metadata.tables)

    bindings = Base.metadata.tables["broker_account_bindings"]
    assert {"provider", "environment", "account_type", "locked", "version"} <= set(
        bindings.columns.keys()
    )
    attempts = Base.metadata.tables["broker_command_attempts"]
    assert {"command_id", "attempt_number", "completed_at", "safe_outcome_code"} <= set(
        attempts.columns.keys()
    )


def test_0024_is_single_head_over_0023() -> None:
    config = Config("engine/alembic.ini")
    script = ScriptDirectory.from_config(config)

    assert script.get_heads() == ["0025_intraday_market_monitor"]
    revision = script.get_revision("0024_broker_simulation")
    assert revision is not None
    assert revision.down_revision == "0023_evolution_authority"


async def _table_names(database_url: str) -> set[str]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            result = await connection.execute(
                text(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema='public'"
                )
            )
            return {str(row.table_name) for row in result}
    finally:
        await engine.dispose()


async def _insert_minimal_binding(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO broker_account_bindings "
                    "(id,provider,environment,account_type,redacted_label,"
                    "external_account_id_digest,snapshot_identity_hash,capability_subject,"
                    "capability_expires_at,reconciliation_status,locked,version) VALUES "
                    "('00000000-0000-0000-0000-000000000024','fosun','simulation',"
                    "'simulation','Fixture SIM',repeat('a',64),repeat('b',64),'fixture-v1',"
                    "'2026-08-28T15:00:00Z','matched',false,1)"
                )
            )
    finally:
        await engine.dispose()


def test_0024_empty_schema_round_trips(
    monkeypatch: pytest.MonkeyPatch, temporary_database_url: str
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0023_evolution_authority")
    assert BROKER_TABLES.isdisjoint(asyncio.run(_table_names(temporary_database_url)))

    command.upgrade(config, "0024_broker_simulation")
    assert BROKER_TABLES <= asyncio.run(_table_names(temporary_database_url))
    command.downgrade(config, "0023_evolution_authority")
    assert BROKER_TABLES.isdisjoint(asyncio.run(_table_names(temporary_database_url)))
    command.upgrade(config, "0024_broker_simulation")
    assert BROKER_TABLES <= asyncio.run(_table_names(temporary_database_url))


def test_0024_populated_downgrade_fails_closed(
    monkeypatch: pytest.MonkeyPatch, temporary_database_url: str
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config("engine/alembic.ini")
    command.upgrade(config, "0024_broker_simulation")
    asyncio.run(_insert_minimal_binding(temporary_database_url))

    with pytest.raises(DBAPIError, match="broker simulation facts prevent downgrade") as error:
        command.downgrade(config, "0023_evolution_authority")

    assert error.value.orig.sqlstate == "P3D01"
