"""Typed request and success envelopes for narrow optimization APIs."""

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
