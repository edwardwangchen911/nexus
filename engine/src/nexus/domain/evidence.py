"""Traceable references to source material used by domain decisions."""

from datetime import datetime

from pydantic import BaseModel, ConfigDict, Field, field_validator

from nexus.domain.time import require_utc


class EvidenceRef(BaseModel):
    """A point-in-time reference to immutable source content."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    source: str = Field(min_length=1, max_length=1000)
    object_id: str = Field(min_length=1, max_length=1000)
    available_at: datetime
    location: str = Field(min_length=1, max_length=1000)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("available_at")
    @classmethod
    def validate_available_at(cls, value: datetime) -> datetime:
        """Store only canonical UTC availability timestamps."""
        return require_utc(value)
