"""Public, transport-neutral models for the read-only research committee."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator

from nexus.domain.time import require_utc
from nexus.strategy.intents import ResearchIntent


class ResearchRequest(BaseModel):
    """Frozen point-in-time input for one bounded research decision."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: UUID
    data_snapshot_id: UUID
    decision_time: datetime

    @field_validator("decision_time")
    @classmethod
    def validate_decision_time(cls, value: datetime) -> datetime:
        """Reject naive and non-canonical timestamps before a model sees them."""
        return require_utc(value)


class ResearchRunResult(BaseModel):
    """The only model-derived outcome the rest of the Engine may consume."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    status: Literal["completed", "failed", "abstained"]
    intent: ResearchIntent | None = None
