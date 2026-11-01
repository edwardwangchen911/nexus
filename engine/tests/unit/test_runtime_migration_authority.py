from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus import worker
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD


def test_runtime_authority_matches_installed_migration_chain():
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    assert ScriptDirectory.from_config(config).get_heads() == [CURRENT_MIGRATION_HEAD]
    assert worker._MIGRATION_HEAD == CURRENT_MIGRATION_HEAD


def test_platform_price_renewal_revision_is_current_authority():
    assert CURRENT_MIGRATION_HEAD == "0107_platform_price_renewal"
