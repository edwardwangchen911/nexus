"""Deterministic lifecycle constants used by the durable research worker seam."""

from typing import Final

LIFECYCLE: Final[tuple[str, ...]] = (
    "prepared",
    "running",
    "validated",
    "audited",
    "synthesized",
    "completed",
)
TERMINAL: Final[frozenset[str]] = frozenset({"completed", "failed", "abstained"})
