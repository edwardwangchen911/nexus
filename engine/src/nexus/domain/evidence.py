"""Traceable references to source material used by domain decisions."""

from datetime import datetime

from pydantic import BaseModel, field_validator

from nexus.domain.time import require_utc


class EvidenceRef(BaseModel):
    """A point-in-time reference to immutable source content."""

    source: str
    object_id: str
    available_at: datetime
    location: str
    content_hash: str

    @field_validator("available_at")
    @classmethod
    def validate_available_at(cls, value: datetime) -> datetime:
        """Store only canonical UTC availability timestamps."""
        return require_utc(value)
