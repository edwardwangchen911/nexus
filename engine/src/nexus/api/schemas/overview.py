"""Closed read-only overview response contracts."""

from datetime import date, datetime
from typing import Literal
from uuid import UUID

from nexus.api.schemas.diagnostics import PortfolioDiagnosticResponse
from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.api.schemas.portfolio import PortfolioSnapshotResponse
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
    overview_mode: Literal["DIAGNOSTIC", "PORTFOLIO_FACTS_ONLY"]
    portfolio_id: UUID
    portfolio_name: str | None = Field(min_length=1, max_length=100)
    provider: Literal["fosun"] | None
    portfolio_snapshot_id: UUID
    portfolio_snapshot: PortfolioSnapshotResponse | None
    snapshot_as_of: datetime
    trading_day: date | None
    diagnostic_report: PortfolioDiagnosticResponse | None
    proposal_group: OverviewProposalGroupSummaryResponse | None
    proposal_admission: ProposalAdmissionResponse | None
    proposal_admission_reason: Literal["OPTIMIZATION_AUTHORITY_MISSING"] | None

    @model_validator(mode="after")
    def _admission_state_is_unambiguous(self) -> "OverviewResponse":
        if self.overview_mode == "PORTFOLIO_FACTS_ONLY":
            if (
                self.portfolio_name is None
                or self.provider != "fosun"
                or self.portfolio_snapshot is None
                or self.diagnostic_report is not None
                or self.trading_day is not None
                or self.proposal_group is not None
                or self.proposal_admission is not None
                or self.proposal_admission_reason is not None
            ):
                raise ValueError(
                    "portfolio facts overview must contain only persisted broker facts"
                )
            if (
                self.portfolio_snapshot.portfolio_id != self.portfolio_id
                or self.portfolio_snapshot.portfolio_snapshot_id != self.portfolio_snapshot_id
                or self.portfolio_snapshot.as_of != self.snapshot_as_of
            ):
                raise ValueError("portfolio facts overview snapshot identity must match")
        else:
            if self.diagnostic_report is None or self.portfolio_snapshot is not None:
                raise ValueError("diagnostic overview must contain one diagnostic report")
            if self.diagnostic_report.portfolio_snapshot_id != self.portfolio_snapshot_id:
                raise ValueError("diagnostic overview snapshot identity must match")
            if (self.proposal_admission is None) != (
                self.proposal_admission_reason is not None
            ):
                raise ValueError("proposal admission and reason must be complementary")
        return self


class OverviewEnvelope(_StrictOverviewSchema):
    data: OverviewResponse
    meta: SuccessMetaResponse
