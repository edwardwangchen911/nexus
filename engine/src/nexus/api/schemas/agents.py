"""Strict public shapes for bounded research-run state and evidence summaries."""

from datetime import UTC, date, datetime
from typing import Literal, cast
from uuid import UUID

from nexus.agents.committee import AgentRole, StageInputIdentity
from nexus.agents.portfolio_context import PortfolioResearchContext
from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.domain.decimal import DecimalString
from nexus.domain.evidence import EvidenceRef
from nexus.domain.time import require_utc
from nexus.strategy.intents import (
    ResearchClaim,
    ResearchCondition,
    ResearchIntent,
    RoleVote,
    SynthesisAssessment,
)
from pydantic import (
    AwareDatetime,
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    field_validator,
    model_serializer,
)


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


class ResearchStageRole(_StrictAgentSchema):
    synthesis_assessment: SynthesisAssessment | None = None

    @model_serializer(mode="wrap")
    # A return annotation would replace the closed model serialization schema.
    def _legacy_payload(self, handler: SerializerFunctionWrapHandler):  # type: ignore[no-untyped-def]
        payload = cast(dict[str, object], handler(self))
        if self.synthesis_assessment is None:
            payload.pop("synthesis_assessment", None)
        return payload

    role: AgentRole
    claims: tuple[ResearchClaim, ...] = Field(min_length=1, max_length=2)
    reviewed_claim_ids: tuple[str, ...] = Field(max_length=16)


class ResearchStageSummary(_StrictAgentSchema):
    stage: Literal["research", "critique", "synthesis"]
    status: Literal["complete", "partial", "unavailable"]
    roles: tuple[ResearchStageRole, ...] = Field(max_length=5)
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    input_hash: str | None = Field(default=None, pattern="^[0-9a-f]{64}$")
    frozen_inputs_hash: str | None = Field(default=None, pattern="^[0-9a-f]{64}$")


class ResearchStageTrace(_StrictAgentSchema):
    evidence_hash: str = Field(pattern="^[0-9a-f]{64}$")
    status: Literal["verified", "invalid"]
    stages: tuple[ResearchStageSummary, ...] = Field(max_length=3)


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
    stage_trace: ResearchStageTrace | None = None
    failed_stage_input: StageInputIdentity | None = None
    inputs: "ResearchInputsSummary | None" = None


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


class ResearchInputIssuerDistribution(_StrictAgentSchema):
    """Latest bound issuer observation; amounts are USD per fund share."""

    evidence_ref_id: UUID
    source: Literal["ssga_distribution"]
    revision: str = Field(max_length=255)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    ex_date: date
    record_date: date
    payable_date: date
    currency: Literal["USD"]
    amount_basis: Literal["per_share"]
    dividend_per_share: DecimalString
    short_term_capital_gain: DecimalString | None
    long_term_capital_gain: DecimalString | None
    available_at: AwareDatetime
    received_at: AwareDatetime
    source_url: str = Field(max_length=512)
    historical_backtest_eligible: Literal[False]


class ResearchInputFundIndustry(_StrictAgentSchema):
    label: str = Field(min_length=1, max_length=512)
    weight_percent: DecimalString


class ResearchInputFundHolding(_StrictAgentSchema):
    name: str = Field(min_length=1, max_length=512)
    ticker: str | None = Field(max_length=100)
    classification: str | None = Field(max_length=512)
    weight_percent: DecimalString
    local_currency: str | None = Field(max_length=100)


class ResearchInputFundObservation(_StrictAgentSchema):
    """Bounded original issuer classifications, never inferred sector-risk exposure."""

    evidence_ref_id: UUID
    source: Literal["ssga_fund_observation"]
    revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    as_of: date
    classification_level: Literal["industry"]
    weight_unit: Literal["percent"]
    industries: tuple[ResearchInputFundIndustry, ...] = Field(min_length=1, max_length=50)
    holdings: tuple[ResearchInputFundHolding, ...] = Field(min_length=1, max_length=100)
    total_industries: int = Field(ge=1)
    total_holdings: int = Field(ge=1)
    truncated: bool
    html_url: str = Field(
        max_length=512, pattern=r"^https://www\.ssga\.com/us/en/individual/etfs/[^?#%]+$"
    )
    workbook_url: str = Field(
        max_length=512,
        pattern=r"^https://www\.ssga\.com/library-content/products/fund-data/etfs/us/[^?#%]+$",
    )
    html_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    workbook_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    html_received_at: AwareDatetime
    workbook_received_at: AwareDatetime
    available_at: AwareDatetime
    source_published_at: None
    valid_until: None
    denominator: None
    residual_classification: None
    historical_backtest_eligible: Literal[False]
    sector_risk_eligible: Literal[False]


class ResearchInputNavObservation(_StrictAgentSchema):
    date: date
    nav: DecimalString
    shares_outstanding: DecimalString | None
    total_net_assets: DecimalString | None


class ResearchInputNavHistory(_StrictAgentSchema):
    """Issuer per-share NAV observations, not executable market-price authority."""

    evidence_ref_id: UUID
    source: Literal["ssga_nav_history"]
    revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    currency: Literal["USD"]
    currency_basis: Literal["issuer_page_base_currency"]
    nav_unit: Literal["currency_per_share"]
    observations: tuple[ResearchInputNavObservation, ...] = Field(min_length=1, max_length=32)
    total_observations: int = Field(ge=1)
    truncated: bool
    coverage_start: date
    coverage_end: date
    date_semantics: Literal["issuer_observation_date_not_verified_trading_session"]
    html_url: str = Field(
        max_length=512, pattern=r"^https://www\.ssga\.com/us/en/individual/etfs/[^?#%]+$"
    )
    workbook_url: str = Field(
        max_length=512,
        pattern=r"^https://www\.ssga\.com/library-content/products/fund-data/etfs/us/[^?#%]+$",
    )
    html_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    workbook_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    html_received_at: AwareDatetime
    workbook_received_at: AwareDatetime
    available_at: AwareDatetime
    source_published_at: None
    valid_until: None
    revision_method: None
    split_adjustment: None
    reinvestment_method: None
    historical_backtest_eligible: Literal[False]
    market_price_eligible: Literal[False]
    total_return_eligible: Literal[False]


class ResearchInputActionEvent(_StrictAgentSchema):
    category: str = Field(min_length=1, max_length=255)
    provider_event_id: str = Field(min_length=1, max_length=255)
    page_number: int = Field(ge=1, le=100)
    index: int = Field(ge=0, le=999)
    symbol: str | None = Field(max_length=255)
    old_symbol: str | None = Field(max_length=255)
    new_symbol: str | None = Field(max_length=255)
    process_date: date | None
    ex_date: date | None
    record_date: date | None
    payable_date: date | None
    declaration_date: date | None
    rate: DecimalString | None
    old_rate: DecimalString | None
    new_rate: DecimalString | None
    currency: str | None = Field(max_length=32)
    unavailable_numeric_fields: tuple[Literal["rate", "old_rate", "new_rate"], ...] = Field(
        max_length=3
    )


class ResearchInputActionObservation(_StrictAgentSchema):
    evidence_ref_id: UUID
    source: Literal["alpaca_action_observation"]
    revision: str = Field(pattern="^[0-9a-f]{64}$")
    content_hash: str = Field(pattern="^[0-9a-f]{64}$")
    symbol: str = Field(min_length=1, max_length=255)
    query_start: date
    query_end: date
    available_at: AwareDatetime
    manifest_content_hash: str = Field(pattern="^[0-9a-f]{64}$")
    page_count: int = Field(ge=1, le=100)
    total_event_count: int = Field(ge=0, le=10000)
    bound_event_count: int = Field(ge=0, le=10000)
    unbound_event_count: int = Field(ge=0, le=10000)
    truncated: bool
    events: tuple[ResearchInputActionEvent, ...] = Field(max_length=32)
    pagination_exhausted: Literal[True]
    source_completeness: Literal["unknown"]
    revision_semantics: Literal["local_content_hash"]
    historical_backtest_eligible: Literal[False]
    ledger_eligible: Literal[False]
    execution_locked: Literal[True]
    available_at_semantics: Literal["maximum_page_receipt_not_historical_availability"]


class ResearchInputCompanyProfile(_StrictAgentSchema):
    evidence_ref_id: UUID
    source: Literal["fmp_company_profile"]
    revision: str = Field(pattern=r"^[0-9a-f]{64}$")
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    raw_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    receipt_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    symbol: str = Field(pattern=r"^[A-Z][A-Z0-9.-]{0,14}$")
    company_name: str | None = Field(max_length=1000)
    description: str | None = Field(max_length=16000)
    cik: str | None = Field(max_length=1000)
    isin: str | None = Field(max_length=1000)
    cusip: str | None = Field(max_length=1000)
    industry: str | None = Field(max_length=1000)
    sector: str | None = Field(max_length=1000)
    source_url: str = Field(
        pattern=r"^https://financialmodelingprep\.com/stable/profile\?symbol=[A-Z][A-Z0-9.-]{0,14}$"
    )
    received_at: AwareDatetime
    available_at: AwareDatetime
    source_published_at: None
    valid_until: None
    sector_taxonomy: None
    revision_semantics: Literal["local_content_hash"]
    sector_risk_eligible: Literal[False]
    financial_fact_eligible: Literal[False]
    price_authority_eligible: Literal[False]
    historical_backtest_eligible: Literal[False]
    execution_locked: Literal[True]


class ResearchInputMacroObservation(_StrictAgentSchema):
    observation_id: UUID
    evidence_ref_id: UUID
    calendar_evidence_ref_id: UUID
    source: Literal["fred"]
    revision: str = Field(max_length=255)
    content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    raw_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    series: str = Field(max_length=100)
    observed_on: date
    vintage: date
    value: DecimalString
    unit: str = Field(max_length=255)
    received_at: AwareDatetime
    available_at: AwareDatetime
    revision_semantics: Literal["local_content_hash"]
    historical_backtest_eligible: Literal[False]
    execution_locked: Literal[True]


class ResearchInputsSummary(_StrictAgentSchema):
    display_symbol: str | None = Field(max_length=100)
    filings: tuple[ResearchInputFiling, ...] = Field(max_length=20)
    facts: tuple[ResearchInputFact, ...] = Field(max_length=100)
    total_facts: int = Field(ge=0)
    truncated: bool
    portfolio_context: PortfolioResearchContext | None = None
    issuer_distributions: tuple[ResearchInputIssuerDistribution, ...] = Field(
        default=(), max_length=1
    )
    issuer_fund_observations: tuple[ResearchInputFundObservation, ...] = Field(
        default=(), max_length=1
    )
    issuer_nav_histories: tuple[ResearchInputNavHistory, ...] = Field(default=(), max_length=1)
    corporate_action_observations: tuple[ResearchInputActionObservation, ...] = Field(
        default=(), max_length=1
    )
    company_profiles: tuple[ResearchInputCompanyProfile, ...] = Field(default=(), max_length=1)
    macro_observations: tuple[ResearchInputMacroObservation, ...] = Field(default=(), max_length=8)


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
