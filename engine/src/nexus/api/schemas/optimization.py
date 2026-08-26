"""Typed request and success envelopes for narrow optimization APIs."""

from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.domain.decimal import DecimalString
from nexus.optimizer.models import ProposalGroupStatus, ProposalVariant
from nexus.optimizer.what_if import WhatIfEvaluation
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _StrictOptimizationSchema(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class WhatIfPositionRequest(_StrictOptimizationSchema):
    instrument_id: UUID
    weight: DecimalString

    @field_validator("weight")
    @classmethod
    def _weight_range(cls, value: str) -> str:
        decimal = Decimal(value)
        if not decimal.is_finite() or decimal < 0 or decimal > 1:
            raise ValueError("weight must be within zero and one")
        return value


class WhatIfRequest(_StrictOptimizationSchema):
    proposal_group_id: UUID
    proposal_id: UUID
    policy_version: Literal["optimizer_v1"]
    positions: tuple[WhatIfPositionRequest, ...] = Field(min_length=1, max_length=30)

    @field_validator("positions")
    @classmethod
    def _unique_instruments(
        cls, value: tuple[WhatIfPositionRequest, ...]
    ) -> tuple[WhatIfPositionRequest, ...]:
        if len({position.instrument_id for position in value}) != len(value):
            raise ValueError("instrument ids must be unique")
        return value


class ProposalAdmissionRequest(_StrictOptimizationSchema):
    portfolio_snapshot_id: UUID
    data_snapshot_id: UUID
    cost_policy_id: UUID


class OptimizationLifecycleRequest(_StrictOptimizationSchema):
    version: int = Field(ge=0)


class OptimizationJobData(_StrictOptimizationSchema):
    job_id: UUID
    status: Literal["QUEUED"]


class OptimizationJobEnvelope(_StrictOptimizationSchema):
    data: OptimizationJobData
    meta: SuccessMetaResponse


class OptimizationLifecycleData(_StrictOptimizationSchema):
    proposal_group_id: UUID
    status: ProposalGroupStatus
    version: int = Field(ge=0)


class OptimizationLifecycleEnvelope(_StrictOptimizationSchema):
    data: OptimizationLifecycleData
    meta: SuccessMetaResponse


class ShadowStartData(_StrictOptimizationSchema):
    proposal_group_id: UUID
    shadow_group_id: UUID
    status: Literal["shadowing"]
    version: int = Field(ge=0)
    start_session: date
    evaluation_end_session: date
    grace_end_session: date

    @model_validator(mode="after")
    def _ordered_window(self) -> "ShadowStartData":
        if not self.start_session <= self.evaluation_end_session < self.grace_end_session:
            raise ValueError("Shadow session window must be ordered")
        return self


class ShadowStartEnvelope(_StrictOptimizationSchema):
    data: ShadowStartData
    meta: SuccessMetaResponse


class ShadowCoverageResponse(_StrictOptimizationSchema):
    completed: int = Field(ge=0, le=63)
    due: int = Field(ge=0, le=63)
    required: Literal[63]
    next_expected_session: date | None
    missing_sessions: tuple[date, ...] = Field(max_length=63)

    @model_validator(mode="after")
    def _coverage_is_consistent(self) -> "ShadowCoverageResponse":
        if tuple(sorted(set(self.missing_sessions))) != self.missing_sessions:
            raise ValueError("missing Shadow sessions must be unique and ordered")
        if self.completed == self.required and self.next_expected_session is not None:
            raise ValueError("complete Shadow coverage has no next session")
        if self.completed < self.required and self.next_expected_session is None:
            raise ValueError("incomplete Shadow coverage requires the next session")
        return self


class ShadowFreshnessResponse(_StrictOptimizationSchema):
    status: Literal["current", "stale", "unknown"]
    latest_session: date | None
    available_at: datetime | None
    last_attempt_at: datetime | None
    database_time: datetime
    reason: str | None

    @field_validator("available_at", "last_attempt_at", "database_time")
    @classmethod
    def _utc_datetime(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("Shadow timestamps must be timezone-aware")
        return None if value is None else value.astimezone(UTC)

    @model_validator(mode="after")
    def _freshness_reason(self) -> "ShadowFreshnessResponse":
        if (self.status == "current") != (self.reason is None):
            raise ValueError("only current Shadow freshness omits a reason")
        if (self.latest_session is None) != (self.available_at is None):
            raise ValueError("latest Shadow session and availability must agree")
        return self


class ShadowSourceResponse(_StrictOptimizationSchema):
    source: str = Field(min_length=1, max_length=100)
    kind: Literal["price_bars", "corporate_actions"]
    delivery_mode: Literal["real_time", "delayed", "fixture", "unknown"]
    latest_session: date
    available_at: datetime

    @field_validator("available_at")
    @classmethod
    def _utc_available_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Shadow source timestamps must be timezone-aware")
        return value.astimezone(UTC)


class ShadowNavPointResponse(_StrictOptimizationSchema):
    session: date
    nav: DecimalString


class ShadowFillResponse(_StrictOptimizationSchema):
    side: Literal["buy", "sell"]
    quantity: DecimalString
    price: DecimalString
    notional: DecimalString
    cost: DecimalString


class ShadowOutcomeResponse(_StrictOptimizationSchema):
    instrument_id: UUID
    side: Literal["buy", "sell", "rebalance"]
    status: Literal["filled", "missing_bar", "untradable"]
    reason: str = Field(min_length=1, max_length=255)
    fill: ShadowFillResponse | None

    @model_validator(mode="after")
    def _fill_matches_status(self) -> "ShadowOutcomeResponse":
        if (self.status == "filled") != (self.fill is not None):
            raise ValueError("only filled Shadow outcomes contain a fill")
        return self


class ShadowPortfolioResponse(_StrictOptimizationSchema):
    shadow_portfolio_id: UUID
    variant: ProposalVariant
    nav: tuple[ShadowNavPointResponse, ...] = Field(max_length=63)
    initial_outcomes: tuple[ShadowOutcomeResponse, ...] = Field(max_length=30)

    @model_validator(mode="after")
    def _ordered_nav(self) -> "ShadowPortfolioResponse":
        sessions = tuple(point.session for point in self.nav)
        if tuple(sorted(set(sessions))) != sessions:
            raise ValueError("Shadow NAV sessions must be unique and ordered")
        return self


class ShadowMetricValueResponse(_StrictOptimizationSchema):
    status: Literal["known", "unknown"]
    value: DecimalString | None
    reason: str | None

    @model_validator(mode="after")
    def _known_or_unknown(self) -> "ShadowMetricValueResponse":
        if self.status == "known" and (self.value is None or self.reason is not None):
            raise ValueError("known Shadow metric requires only a value")
        if self.status == "unknown" and (self.value is not None or not self.reason):
            raise ValueError("unknown Shadow metric requires only a reason")
        return self


class ShadowWorstSessionResponse(_StrictOptimizationSchema):
    session: date
    return_value: DecimalString


class ShadowMetricsResponse(_StrictOptimizationSchema):
    net_cumulative_return: ShadowMetricValueResponse
    return_delta_vs_hold: ShadowMetricValueResponse
    max_drawdown: ShadowMetricValueResponse
    drawdown_improvement: ShadowMetricValueResponse
    annualized_volatility: ShadowMetricValueResponse
    volatility_improvement: ShadowMetricValueResponse
    worst_sessions: tuple[ShadowWorstSessionResponse, ...] = Field(max_length=5)
    cumulative_turnover: ShadowMetricValueResponse
    fee_drag_index_points: ShadowMetricValueResponse

    @model_validator(mode="after")
    def _metric_states_agree(self) -> "ShadowMetricsResponse":
        values = (
            self.net_cumulative_return,
            self.return_delta_vs_hold,
            self.max_drawdown,
            self.drawdown_improvement,
            self.annualized_volatility,
            self.volatility_improvement,
            self.cumulative_turnover,
            self.fee_drag_index_points,
        )
        if len({item.status for item in values}) != 1:
            raise ValueError("Shadow checkpoint metrics must share one availability state")
        if values[0].status == "unknown" and self.worst_sessions:
            raise ValueError("unknown Shadow metrics cannot contain worst sessions")
        sessions = tuple(item.session for item in self.worst_sessions)
        if len(sessions) != len(set(sessions)):
            raise ValueError("Shadow worst sessions must be unique")
        return self


class ShadowVariantMetricsResponse(_StrictOptimizationSchema):
    variant: ProposalVariant
    metrics: ShadowMetricsResponse


class ShadowCheckpointResponse(_StrictOptimizationSchema):
    ordinal: Literal[5, 21, 63]
    session: date
    status: Literal["complete", "incomplete"]
    coverage_count: int = Field(ge=0, le=63)
    reason: str | None
    metrics: tuple[
        ShadowVariantMetricsResponse,
        ShadowVariantMetricsResponse,
        ShadowVariantMetricsResponse,
    ]

    @model_validator(mode="after")
    def _checkpoint_is_consistent(self) -> "ShadowCheckpointResponse":
        variants = tuple(item.variant for item in self.metrics)
        if variants != ("hold_current", "minimum_change", "strategy_target"):
            raise ValueError("Shadow checkpoint variants must use the fixed order")
        metric_statuses = {item.metrics.net_cumulative_return.status for item in self.metrics}
        if self.status == "complete":
            if self.reason is not None or metric_statuses != {"known"}:
                raise ValueError("complete Shadow checkpoint requires known metrics")
        elif not self.reason or metric_statuses != {"unknown"}:
            raise ValueError("incomplete Shadow checkpoint requires an unknown reason")
        return self


class ShadowAttemptResponse(_StrictOptimizationSchema):
    requested_session: date
    expected_session: date | None
    result: Literal["data_partial", "data_stale", "source_conflict", "session_gap"]
    reason: str = Field(min_length=1, max_length=100)
    observed_at: datetime
    created_at: datetime

    @field_validator("observed_at", "created_at")
    @classmethod
    def _utc_attempt_time(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Shadow attempt timestamps must be timezone-aware")
        return value.astimezone(UTC)


class ShadowSafetyResponse(_StrictOptimizationSchema):
    creates_orders: Literal[False]
    trading_approval: Literal[False]


class ShadowGroupResponse(_StrictOptimizationSchema):
    proposal_group_id: UUID
    shadow_group_id: UUID
    proposal_status: ProposalGroupStatus
    version: int = Field(ge=0)
    start_session: date
    evaluation_end_session: date
    grace_end_session: date
    coverage: ShadowCoverageResponse
    freshness: ShadowFreshnessResponse
    sources: tuple[ShadowSourceResponse, ...] = Field(max_length=32)
    portfolios: tuple[
        ShadowPortfolioResponse,
        ShadowPortfolioResponse,
        ShadowPortfolioResponse,
    ]
    checkpoints: tuple[ShadowCheckpointResponse, ...] = Field(max_length=3)
    last_failed_attempt: ShadowAttemptResponse | None
    sealed: bool
    seal_reason: Literal["validated", "rejected", "expired"] | None
    safety: ShadowSafetyResponse

    @model_validator(mode="after")
    def _verified_projection_shape(self) -> "ShadowGroupResponse":
        if not self.start_session <= self.evaluation_end_session < self.grace_end_session:
            raise ValueError("Shadow session window must be ordered")
        if tuple(item.variant for item in self.portfolios) != (
            "hold_current",
            "minimum_change",
            "strategy_target",
        ):
            raise ValueError("Shadow portfolios must use the fixed variant order")
        nav_sessions = tuple(tuple(point.session for point in item.nav) for item in self.portfolios)
        if len(set(nav_sessions)) != 1 or len(nav_sessions[0]) != self.coverage.completed:
            raise ValueError("Shadow portfolio coverage must be identical")
        checkpoint_ordinals = tuple(item.ordinal for item in self.checkpoints)
        if tuple(sorted(set(checkpoint_ordinals))) != checkpoint_ordinals:
            raise ValueError("Shadow checkpoints must be unique and ordered")
        if self.sealed:
            if self.seal_reason is None or self.proposal_status != self.seal_reason:
                raise ValueError("sealed Shadow status must match its reason")
        elif self.seal_reason is not None or self.proposal_status != "shadowing":
            raise ValueError("active Shadow must be unsealed and shadowing")
        return self


class ShadowGroupEnvelope(_StrictOptimizationSchema):
    data: ShadowGroupResponse
    meta: SuccessMetaResponse


class ComparisonMetricResponse(_StrictOptimizationSchema):
    status: Literal["known", "unknown"]
    value: DecimalString | None
    reason: str | None

    @model_validator(mode="after")
    def _state_matches_payload(self) -> "ComparisonMetricResponse":
        if self.status == "known" and (self.value is None or self.reason is not None):
            raise ValueError("known metric requires only a value")
        if self.status == "unknown" and (self.value is not None or not self.reason):
            raise ValueError("unknown metric requires only a reason")
        return self


class ProposalComparisonDimensionsResponse(_StrictOptimizationSchema):
    hard_violations: int = Field(ge=0)
    top1: ComparisonMetricResponse
    top3: ComparisonMetricResponse
    hhi: ComparisonMetricResponse
    sector_exposure: ComparisonMetricResponse
    currency_exposure: ComparisonMetricResponse
    volatility: ComparisonMetricResponse
    drawdown: ComparisonMetricResponse
    correlation: ComparisonMetricResponse
    liquidity: ComparisonMetricResponse
    turnover: ComparisonMetricResponse
    cost: ComparisonMetricResponse
    tax_impact: ComparisonMetricResponse


class ProposalComparisonAuthorityResponse(_StrictOptimizationSchema):
    diagnostic_report_id: UUID
    data_snapshot_id: UUID
    source_policy_id: UUID
    optimizer_policy_id: UUID
    cost_policy_id: UUID


class ProposalComparisonMetricsResponse(_StrictOptimizationSchema):
    dimensions: ProposalComparisonDimensionsResponse
    ranking_eligible: bool
    constraint_complete: bool
    eligibility_complete: bool
    evidence_complete: bool
    stale: bool
    unknown_metrics: tuple[str, ...]
    authority: ProposalComparisonAuthorityResponse


class ProposalComparisonResponse(_StrictOptimizationSchema):
    metrics: ProposalComparisonMetricsResponse
    tax_impact: Literal["unknown"]
    requires_fx: bool
    non_executable: bool
    cost_complete: bool


class OptimizationProposalPositionResponse(_StrictOptimizationSchema):
    instrument_id: UUID
    current_weight: DecimalString
    target_weight: DecimalString
    action: Literal["none", "reduce", "maintain", "increase"]
    reason_code: str


class OptimizationProposalResponse(_StrictOptimizationSchema):
    proposal_id: UUID
    variant: ProposalVariant
    analysis_only: bool
    strategy_evidence_complete: bool
    portfolio_scope: Literal["real_read_only", "internal_paper", "fosun_simulation"]
    positions: tuple[OptimizationProposalPositionResponse, ...]
    comparison: ProposalComparisonResponse


class ShadowEligibilityResponse(_StrictOptimizationSchema):
    eligible: bool
    reasons: tuple[str, ...]


class OptimizationProposalGroupResponse(_StrictOptimizationSchema):
    proposal_group_id: UUID
    status: ProposalGroupStatus
    version: int = Field(ge=0)
    baseline_portfolio_snapshot_id: UUID
    portfolio_scope: Literal["real_read_only", "internal_paper", "fosun_simulation"]
    diagnostic_input_hash: str
    data_snapshot_id: UUID
    source_policy_id: UUID
    cost_policy_id: UUID
    proposals: tuple[
        OptimizationProposalResponse,
        OptimizationProposalResponse,
        OptimizationProposalResponse,
    ]
    shadow_eligibility: ShadowEligibilityResponse

    @model_validator(mode="after")
    def _three_fixed_variants(self) -> "OptimizationProposalGroupResponse":
        if tuple(proposal.variant for proposal in self.proposals) != (
            "hold_current",
            "minimum_change",
            "strategy_target",
        ):
            raise ValueError("proposal group requires exactly ordered unique variants")
        if len({proposal.proposal_id for proposal in self.proposals}) != 3:
            raise ValueError("proposal group requires unique proposal ids")
        return self


class OptimizationProposalGroupEnvelope(_StrictOptimizationSchema):
    data: OptimizationProposalGroupResponse
    meta: SuccessMetaResponse


class WhatIfResponse(_StrictOptimizationSchema):
    proposal_group_id: UUID
    proposal_id: UUID
    policy_version: Literal["optimizer_v1"]
    instrument_count: int = Field(ge=0, le=30)
    result: WhatIfEvaluation


class WhatIfEnvelope(_StrictOptimizationSchema):
    data: WhatIfResponse
    meta: SuccessMetaResponse
