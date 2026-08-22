"""Internal identifier primitives."""

from uuid import UUID

from uuid6 import uuid7


def new_uuid7() -> UUID:
    """Create an internal UUIDv7 with time-sortable ordering."""
    return uuid7()
