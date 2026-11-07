"""Native formal binding is additive and reverses before it has persisted users."""

import asyncio
from pathlib import Path

from alembic import command
from alembic.config import Config
from integration.test_isolated_v9_output_migration import definition


def test_empty_sol_binding_roundtrip_keeps_entire_previous_guard(migrated_database_url):
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    upgraded = asyncio.run(definition(migrated_database_url))
    assert "nexus_sol_formal_output_binding_valid" in upgraded
    command.downgrade(config, "0119_compact_budget_obligations")
    previous = asyncio.run(definition(migrated_database_url))
    added = """               public.nexus_sol_formal_output_binding_valid(
                 NEW.strategy_decision_id, NEW.agent_research_output_id) OR
"""
    assert upgraded.replace(added, "") == previous
    command.upgrade(config, "head")
    assert asyncio.run(definition(migrated_database_url)) == upgraded
