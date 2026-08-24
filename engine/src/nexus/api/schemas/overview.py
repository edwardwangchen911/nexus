"""Closed read-only overview response contracts."""

from datetime import date, datetime
from typing import Literal
from uuid import UUID

from nexus.api.schemas.diagnostics import PortfolioDiagnosticResponse
from nexus.api.schemas.integrations import SuccessMetaResponse
from pydantic import BaseModel, ConfigDict, Field, model_validator


class _StrictOverviewSchema(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class OverviewProposalGroupSummaryResponse(_StrictOverviewSchema):
    proposal_group_id: UUID
    status: Literal["draft", "compared", "shadowing", "validated", "rejected", "expired"]
    version: int = Field(ge=0)


class ProposalAdmissionResponse(_StrictOverviewSchema):
    portfolio_snapshot_id: UUID
    data_snapshot_id: UUID
    cost_policy_id: UUID


class OverviewResponse(_StrictOverviewSchema):
    portfolio_id: UUID
    portfolio_snapshot_id: UUID
    snapshot_as_of: datetime
    trading_day: date | None
    diagnostic_report: PortfolioDiagnosticResponse
    proposal_group: OverviewProposalGroupSummaryResponse | None
    proposal_admission: ProposalAdmissionResponse | None
    proposal_admission_reason: Literal["OPTIMIZATION_AUTHORITY_MISSING"] | None

    @model_validator(mode="after")
    def _admission_state_is_unambiguous(self) -> "OverviewResponse":
        if (self.proposal_admission is None) != (self.proposal_admission_reason is not None):
            raise ValueError("proposal admission and reason must be complementary")
        return self


class OverviewEnvelope(_StrictOverviewSchema):
    data: OverviewResponse
    meta: SuccessMetaResponse
