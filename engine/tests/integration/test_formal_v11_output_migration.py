import asyncio
from pathlib import Path

from alembic import command
from alembic.config import Config
from integration.test_isolated_v9_output_migration import definition
from nexus.agents.codex_bounded_budget import bounded_stage_config_hash


def test_v11_empty_roundtrip_preserves_old_formal_and_excludes_diagnostic(migrated_database_url):
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    upgraded = asyncio.run(definition(migrated_database_url))
    assert "codex-sdk-chatgpt-v11-bounded-stage-budget" in upgraded
    assert bounded_stage_config_hash() in upgraded
    assert "full-input-timing-diagnostic-v1" not in upgraded
    command.downgrade(config, "0078_formal_v10_output_binding")
    old = asyncio.run(definition(migrated_database_url))
    assert "codex-sdk-chatgpt-v11-bounded-stage-budget" not in old
    assert "codex-sdk-chatgpt-v10-flexible-stage-budget" in old
    command.upgrade(config, "head")
    assert asyncio.run(definition(migrated_database_url)) == upgraded
