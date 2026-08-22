"""Traceable references to source material used by domain decisions."""

from datetime import datetime

from pydantic import BaseModel, Field, field_validator

from nexus.domain.time import require_utc


class EvidenceRef(BaseModel):
    """A point-in-time reference to immutable source content."""

    source: str
    object_id: str
    available_at: datetime
    location: str
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("available_at")
    @classmethod
    def validate_available_at(cls, value: datetime) -> datetime:
        """Store only canonical UTC availability timestamps."""
        return require_utc(value)
