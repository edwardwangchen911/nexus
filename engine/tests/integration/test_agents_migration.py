"""Alembic boundary tests for append-only Agent research persistence."""

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory


def test_0011_agents_is_the_single_head_with_the_research_data_parent() -> None:
    """A split head or wrong parent could silently omit immutable Agent audit records."""
    engine_root = Path(__file__).resolve().parents[2]
    scripts = ScriptDirectory.from_config(Config(str(engine_root / "alembic.ini")))
    revision = scripts.get_revision("0011_agents")
    assert scripts.get_heads() == ["0011_agents"]
    assert revision is not None
    assert revision.down_revision == "0010_research_data"
