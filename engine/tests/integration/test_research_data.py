"""Migration-level assertions for immutable research evidence storage."""

from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from helpers.migration_helpers import _table_names
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine


def test_0010_research_data_round_trips_from_0009_with_one_head(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))
    research_tables = {
        "research_evidence_refs",
        "filings",
        "financial_facts",
        "corporate_actions",
    }

    assert ScriptDirectory.from_config(config).get_heads() == ["0010_research_data"]
    command.upgrade(config, "0009_optimization")
    assert research_tables.isdisjoint(_table_names(temporary_database_url))
    command.upgrade(config, "0010_research_data")
    assert research_tables.issubset(_table_names(temporary_database_url))
    command.downgrade(config, "0009_optimization")
    assert research_tables.isdisjoint(_table_names(temporary_database_url))
    command.upgrade(config, "0010_research_data")
    assert research_tables.issubset(_table_names(temporary_database_url))


@pytest.mark.asyncio
async def test_research_evidence_reference_is_append_only_and_macro_series_is_constrained(
    migrated_database_url: str,
) -> None:
    engine = create_async_engine(migrated_database_url)
    evidence_id = uuid4()
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO research_evidence_refs "
                    "(id, source, provider_object_id, revision, available_at, location, "
                    "content_hash, byte_length) "
                    "VALUES (:id, 'fixture', 'object', '1', :available_at, "
                    "'memory://evidence/object.json', :content_hash, 7)"
                ),
                {
                    "id": evidence_id,
                    "available_at": datetime(2026, 8, 24, tzinfo=UTC),
                    "content_hash": "a" * 64,
                },
            )
        async with engine.begin() as connection:
            with pytest.raises(DBAPIError, match="research evidence is append-only"):
                await connection.execute(
                    text("UPDATE research_evidence_refs SET revision = '2' WHERE id = :id"),
                    {"id": evidence_id},
                )
        async with engine.begin() as connection:
            with pytest.raises(DBAPIError, match="series_allowed"):
                await connection.execute(
                    text(
                        "INSERT INTO macro_observations "
                        "(id, source, series, observed_at, available_at, revision, value, "
                        "unit, display_sanity_only) VALUES (:id, 'fixture', 'EURUSD', "
                        ":observed_at, :available_at, '1', 1, 'ratio', false)"
                    ),
                    {
                        "id": uuid4(),
                        "observed_at": datetime(2026, 8, 24, tzinfo=UTC),
                        "available_at": datetime(2026, 8, 24, tzinfo=UTC),
                    },
                )
    finally:
        await engine.dispose()
