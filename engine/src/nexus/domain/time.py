"""Timestamp invariants for persisted and wire-visible data."""

from datetime import UTC, datetime


def require_utc(value: datetime) -> datetime:
    """Reject naive datetimes and canonicalize aware values to UTC."""
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)
