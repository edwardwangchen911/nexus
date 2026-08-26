"""Release migration checks for immutable optimization storage."""

from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from helpers.migration_helpers import _table_names


def test_0009_round_trips_from_0008_with_one_head(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    """The single 0009 successor must downgrade to 0008 and upgrade deterministically again."""
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))
    optimization_tables = {
        "cost_policies",
        "optimization_admission_authorities",
        "optimization_policies",
        "optimization_proposal_groups",
        "optimization_proposals",
        "proposal_comparisons",
        "proposal_positions",
    }

    assert ScriptDirectory.from_config(config).get_heads() == ["0016_paper_execution_atomic"]
    command.upgrade(config, "0008_diagnostics")
    assert optimization_tables.isdisjoint(_table_names(temporary_database_url))
    command.upgrade(config, "0009_optimization")
    assert optimization_tables.issubset(_table_names(temporary_database_url))
    command.downgrade(config, "0008_diagnostics")
    assert optimization_tables.isdisjoint(_table_names(temporary_database_url))
    command.upgrade(config, "0009_optimization")
    assert optimization_tables.issubset(_table_names(temporary_database_url))
