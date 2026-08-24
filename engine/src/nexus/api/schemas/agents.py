"""Strict public shapes for bounded research-run state and evidence summaries."""

from datetime import UTC, date, datetime
from typing import Literal
from uuid import UUID

from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.domain.decimal import DecimalString
from nexus.domain.time import require_utc
from nexus.strategy.intents import ResearchIntent
from pydantic import BaseModel, ConfigDict, field_validator


class _StrictAgentSchema(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


type AgentRunStatus = Literal[
    "prepared",
    "running",
    "validated",
    "audited",
    "synthesized",
    "completed",
    "failed",
    "abstained",
]


class ResearchRunCreateRequest(_StrictAgentSchema):
    instrument_id: UUID
    data_snapshot_id: UUID
    decision_time: datetime

    @field_validator("decision_time")
    @classmethod
    def require_utc_time(cls, value: datetime) -> datetime:
        return require_utc(value)


class ResearchRunResponse(_StrictAgentSchema):
    run_id: UUID
    instrument_id: UUID
    data_snapshot_id: UUID
    decision_time: datetime
    status: AgentRunStatus
    retry_count: int
    reserved_cost_usd: DecimalString
    settled_cost_usd: DecimalString | None
    intent: ResearchIntent | None


class ResearchRunEnvelope(_StrictAgentSchema):
    data: ResearchRunResponse
    meta: SuccessMetaResponse


class DailyBriefResponse(_StrictAgentSchema):
    session_date: date
    generated_at: datetime
    runs: tuple[ResearchRunResponse, ...]

    @field_validator("generated_at")
    @classmethod
    def require_generated_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class DailyBriefEnvelope(_StrictAgentSchema):
    data: DailyBriefResponse
    meta: SuccessMetaResponse


def utc_now() -> datetime:
    """One UTC source for read response metadata."""
    return datetime.now(UTC)
