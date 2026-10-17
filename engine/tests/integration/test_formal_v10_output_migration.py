import asyncio
from pathlib import Path

from alembic import command
from alembic.config import Config
from integration.test_isolated_v9_output_migration import definition
from nexus.agents.codex_flexible_budget import flexible_stage_config_hash


def test_formal_v10_empty_roundtrip_preserves_frozen_v9_and_diagnostic_exclusion(
    migrated_database_url,
):
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    upgraded = asyncio.run(definition(migrated_database_url))
    assert (
        "run_version_ref IN ('codex-sdk-chatgpt-v8-stage-capacity', "
        "'codex-sdk-chatgpt-v9-full-input-capacity', "
        "'codex-sdk-chatgpt-v10-flexible-stage-budget'"
    ) in upgraded
    assert flexible_stage_config_hash() in upgraded
    assert "full-input-timing-diagnostic-v1" not in upgraded
    command.downgrade(config, "0077_formal_v9_output_binding")
    old = asyncio.run(definition(migrated_database_url))
    assert "codex-sdk-chatgpt-v10-flexible-stage-budget" not in old
    assert "codex-sdk-chatgpt-v9-full-input-capacity" in old
    command.upgrade(config, "head")
    assert asyncio.run(definition(migrated_database_url)) == upgraded
