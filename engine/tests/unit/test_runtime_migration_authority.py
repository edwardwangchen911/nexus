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
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    revision = ScriptDirectory.from_config(config).get_revision("0109_platform_third_round")
    assert revision.revision == "0109_platform_third_round"
    assert revision.down_revision == "0108_platform_restore_lookup"


def test_structured_schema_revision_preserves_third_qualification_predecessor():
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    revision = ScriptDirectory.from_config(config).get_revision("0110_platform_structured_schema")
    assert revision.revision == "0110_platform_structured_schema"
    assert revision.down_revision == "0109_platform_third_round"


def test_fourth_round_preserves_structured_schema_predecessor():
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    revision = ScriptDirectory.from_config(config).get_revision("0111_platform_round_registry")
    assert revision.revision == "0111_platform_round_registry"
    assert revision.down_revision == "0110_platform_structured_schema"


def test_medium_profile_preserves_stable_round_predecessor():
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    revision = ScriptDirectory.from_config(config).get_revision(CURRENT_MIGRATION_HEAD)
    assert revision.revision == "0112_platform_medium_profile"
    assert revision.down_revision == "0111_platform_round_registry"
