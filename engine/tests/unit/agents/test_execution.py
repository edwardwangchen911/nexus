"""Durable fixture execution must append the full Agent lifecycle."""

from nexus.agents.execution import LIFECYCLE


def test_execution_lifecycle_is_exact_and_append_only() -> None:
    """Skipping an audit phase would make completed research unauditable."""
    assert LIFECYCLE == (
        "prepared",
        "running",
        "validated",
        "audited",
        "synthesized",
        "completed",
    )
