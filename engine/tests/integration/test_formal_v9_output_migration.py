"""Formal v9 SQL changes are reversible before new facts; isolated guards survive."""

import asyncio
from pathlib import Path

from alembic import command
from alembic.config import Config
from integration.test_isolated_v9_output_migration import definition


def test_formal_v9_empty_migration_roundtrip_preserves_other_modes(migrated_database_url):
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    command.downgrade(config, "0077_formal_v9_output_binding")
    upgraded = asyncio.run(definition(migrated_database_url))
    formal_clause = (
        "run_version_ref IN ('codex-sdk-chatgpt-v8-stage-capacity', "
        "'codex-sdk-chatgpt-v9-full-input-capacity')"
    )
    assert formal_clause in upgraded
    command.downgrade(config, "0076_isolated_v9_output_binding")
    old = asyncio.run(definition(migrated_database_url))
    assert formal_clause not in old
    assert "run_version_ref = 'codex-sdk-chatgpt-v8-stage-capacity'" in old
    assert (
        "run_version_ref IN ('codex-sdk-chatgpt-v7-stage-capacity-candidate', "
        "'codex-sdk-chatgpt-v9-full-input-capacity')" in old
    )
    assert "current_database() ~ '^nexus_(test|diag)_'" in old
    command.upgrade(config, "0077_formal_v9_output_binding")
    assert asyncio.run(definition(migrated_database_url)) == upgraded
    command.upgrade(config, "head")
