"""Strict public contracts for bounded portfolio decision runs."""

from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from nexus.agents.portfolio_decision_contracts import PortfolioResearchIntentV1
from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.domain.decimal import DecimalString
from nexus.domain.evidence import EvidenceRef
from nexus.domain.time import require_utc
from nexus.optimizer.scenario_contracts import PortfolioScenarioRequestV1
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    field_validator,
    model_validator,
)

Hash64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
PortfolioDecisionStatus = Literal["prepared", "running", "completed", "abstained", "failed"]
PortfolioDecisionErrorCode = Literal[
    "PORTFOLIO_DECISION_TOOL_REQUIRED",
    "PORTFOLIO_DECISION_TOOL_LIMIT_EXCEEDED",
    "PORTFOLIO_DECISION_TOOL_RESULT_MISMATCH",
    "PORTFOLIO_DECISION_OUTPUT_INVALID",
    "PORTFOLIO_DECISION_MODEL_FAILED",
    "PORTFOLIO_DECISION_PERSISTENCE_UNCERTAIN",
    "PORTFOLIO_DECISION_AUTHORITY_INVALID",
]


class _StrictSchema(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)


class PortfolioDecisionCreateRequest(_StrictSchema):
    schema_version: Literal["portfolio_decision_create_v1"]
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    expected_scenario_result_hash: Hash64
    scenario_request: PortfolioScenarioRequestV1

    @model_validator(mode="after")
    def _same_scope(self) -> "PortfolioDecisionCreateRequest":
        if (
            self.portfolio_snapshot_id != self.scenario_request.portfolio_snapshot_id
            or self.diagnostic_report_id != self.scenario_request.diagnostic_report_id
        ):
            raise ValueError("portfolio decision request scope does not match scenario request")
        return self


class PortfolioDecisionCitationResponse(_StrictSchema):
    citation_id: Hash64
    evidence: EvidenceRef


class PortfolioDecisionOutputResponse(_StrictSchema):
    status: Literal["completed", "abstained", "failed"]
    scenario_result_hash: Hash64 | None
    intent: PortfolioResearchIntentV1 | None
    error_code: PortfolioDecisionErrorCode | None


class PortfolioDecisionRunResponse(_StrictSchema):
    schema_version: Literal["portfolio_decision_run_v1"] = "portfolio_decision_run_v1"
    run_id: UUID
    portfolio_id: UUID
    portfolio_snapshot_id: UUID
    diagnostic_report_id: UUID
    decision_time: datetime
    scenario_request_hash: Hash64
    expected_scenario_result_hash: Hash64
    objective: Literal["reduce_security_concentration"]
    status: PortfolioDecisionStatus
    reserved_cost_usd: DecimalString
    settled_cost_usd: DecimalString | None
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    output: PortfolioDecisionOutputResponse | None
    citations: tuple[PortfolioDecisionCitationResponse, ...] = Field(max_length=100)
    execution_locked: Literal[True] = True

    @field_validator("decision_time")
    @classmethod
    def _utc_time(cls, value: datetime) -> datetime:
        return require_utc(value)


class PortfolioDecisionRunEnvelope(_StrictSchema):
    data: PortfolioDecisionRunResponse
    meta: SuccessMetaResponse
