"""Strict public shapes for bounded research-run state and evidence summaries."""

from datetime import UTC, date, datetime
from typing import Literal
from uuid import UUID

from nexus.agents.portfolio_context import PortfolioResearchContext
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
    billing_mode: Literal["chatgpt_subscription"] | None = None
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
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


class ResearchInputFiling(_StrictAgentSchema):
    evidence_ref_id: UUID
    source: Literal["sec"] = "sec"
    form: str = Field(max_length=32)
    accession_number: str = Field(max_length=32)
    accepted_at: datetime
    received_at: datetime
    revision: str = Field(max_length=255)
    content_hash: str = Field(pattern="^[0-9a-f]{64}$")
    disclosure_excerpt: str | None = Field(default=None, max_length=12000)
    disclosure_truncated: bool = False
    source_url: str | None = Field(
        default=None,
        max_length=512,
        pattern=r"^https://www\.sec\.gov/Archives/edgar/data/[0-9]+/[0-9]{18}/[A-Za-z0-9][A-Za-z0-9._-]*\.htm[l]?$",
    )


class ResearchInputFact(_StrictAgentSchema):
    id: UUID
    evidence_ref_id: UUID
    fact: str = Field(max_length=64)
    raw_tag: str = Field(max_length=255)
    value: DecimalString
    unit: str = Field(max_length=100)
    period_kind: Literal["instant", "duration"]
    period_start: date | None
    period_end: date


class ResearchInputsSummary(_StrictAgentSchema):
    display_symbol: str | None = Field(max_length=100)
    filings: tuple[ResearchInputFiling, ...] = Field(max_length=20)
    facts: tuple[ResearchInputFact, ...] = Field(max_length=100)
    total_facts: int = Field(ge=0)
    truncated: bool
    portfolio_context: PortfolioResearchContext | None = None


class ResearchAdmissionResponse(_StrictAgentSchema):
    eligible: bool
    reason: (
        Literal[
            "instrument_not_found",
            "decision_time_in_future",
            "data_snapshot_unavailable",
            "prompt_authority_unavailable",
            "model_pricing_unavailable",
        ]
        | None
    )
    instrument_id: UUID
    data_snapshot_id: UUID | None
    decision_time: datetime
    inputs: ResearchInputsSummary | None = None

    @field_validator("decision_time")
    @classmethod
    def require_decision_utc(cls, value: datetime) -> datetime:
        return require_utc(value)


class ResearchAdmissionEnvelope(_StrictAgentSchema):
    data: ResearchAdmissionResponse
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
