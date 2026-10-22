"""New ownership branch leaves the existing current-identity guard unchanged."""

import asyncio
from pathlib import Path

from alembic import command
from alembic.config import Config
from integration.test_research_identity_migration import definition


def test_observation_ownership_migration_roundtrip(temporary_database_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    command.upgrade(config, "0082_synthesis_failure_facts")
    before = asyncio.run(definition(temporary_database_url))
    command.upgrade(config, "head")
    after = asyncio.run(definition(temporary_database_url))
    assert "research_observation_identity" in after
    assert (
        "research_current_identity_session_v1" in before
        and "research_current_identity_session_v1" in after
    )
    assert "research_historical_observation_only" in after
    command.downgrade(config, "0082_synthesis_failure_facts")
    assert asyncio.run(definition(temporary_database_url)) == before
