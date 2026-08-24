"""Strict public shapes for bounded research-run state and evidence summaries."""

from datetime import UTC, date, datetime
from typing import Literal
from uuid import UUID

from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.domain.decimal import DecimalString
from nexus.domain.evidence import EvidenceRef
from nexus.domain.time import require_utc
from nexus.strategy.intents import ResearchCondition, ResearchIntent, RoleVote
from pydantic import BaseModel, ConfigDict, Field, field_validator


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
    evidence: tuple[EvidenceRef, ...] = Field(max_length=100)
    role_votes: tuple[RoleVote, ...] = Field(max_length=8)
    disagreement: str | None = Field(default=None, max_length=500)
    invalidation_conditions: tuple[ResearchCondition, ...] = Field(max_length=10)
    quality_status: Literal["complete", "partial"] | None
    budget_reason: Literal["budget_exhausted"] | None
    abstention_reason: str | None = Field(default=None, max_length=100)


class ResearchRunEnvelope(_StrictAgentSchema):
    data: ResearchRunResponse
    meta: SuccessMetaResponse


class DailyBriefItem(_StrictAgentSchema):
    run_id: UUID
    instrument_id: UUID
    status: AgentRunStatus
    stance: Literal["buy_candidate", "reduce_candidate", "hold", "watch"] | None
    reason: str | None = Field(default=None, max_length=100)


class EvidenceCompletenessSummary(_StrictAgentSchema):
    total_runs: int = Field(ge=0, le=100)
    complete: int = Field(ge=0, le=100)
    partial: int = Field(ge=0, le=100)
    failed: int = Field(ge=0, le=100)


class BriefDisagreementSummary(_StrictAgentSchema):
    run_id: UUID
    instrument_id: UUID
    summary: str = Field(min_length=1, max_length=500)


class BriefInvalidationSummary(_StrictAgentSchema):
    run_id: UUID
    instrument_id: UUID
    conditions: tuple[ResearchCondition, ...] = Field(min_length=1, max_length=10)


class DailyBriefResponse(_StrictAgentSchema):
    session_date: date
    generated_at: datetime
    candidates: tuple[DailyBriefItem, ...] = Field(max_length=20)
    holding_risks: tuple[DailyBriefItem, ...] = Field(max_length=20)
    rejected: tuple[DailyBriefItem, ...] = Field(max_length=20)
    budget_exhausted_watch: tuple[DailyBriefItem, ...] = Field(max_length=20)
    evidence_completeness: EvidenceCompletenessSummary
    disagreements: tuple[BriefDisagreementSummary, ...] = Field(max_length=20)
    invalidations: tuple[BriefInvalidationSummary, ...] = Field(max_length=50)
    truncated: bool

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
