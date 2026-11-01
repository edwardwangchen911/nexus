from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus import worker
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD


def test_runtime_authority_matches_installed_migration_chain():
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    assert ScriptDirectory.from_config(config).get_heads() == [CURRENT_MIGRATION_HEAD]
    assert worker._MIGRATION_HEAD == CURRENT_MIGRATION_HEAD


def test_third_qualification_revision_preserves_restore_lookup_predecessor():
    assert CURRENT_MIGRATION_HEAD == "0109_platform_third_round"
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    revision = ScriptDirectory.from_config(config).get_revision(CURRENT_MIGRATION_HEAD)
    assert revision.down_revision == "0108_platform_restore_lookup"
