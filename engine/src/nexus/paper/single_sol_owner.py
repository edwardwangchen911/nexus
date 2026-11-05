"""Frozen identity for one explicitly selected research-only Sol request."""

from datetime import date, datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from nexus.jobs.models import job_input_hash
from nexus.persistence.models.manual_paper import ManualPaperRequest


class SingleSolBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    instrument_id: UUID
    selection_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    budget_day: date
    calendar_id: UUID
    deadline: datetime
    history_policy: Literal["single_sol_original_acceptances_v1"] = (
        "single_sol_original_acceptances_v1"
    )
    preparation_proof: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")] | None
    scheduled_proof: Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")] | None

    @property
    def intent_hash(self) -> str:
        return job_input_hash(
            {"contract": "single_sol_request_intent_v1", **self.model_dump(mode="json")}
        )


class BudgetedSingleSolBinding(SingleSolBinding):
    admission_policy: Literal["shared_budget_v1"] = "shared_budget_v1"
    protection_context_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @property
    def intent_hash(self) -> str:
        return job_input_hash(
            {"contract": "budgeted_single_sol_intent_v1", **self.model_dump(mode="json")}
        )


class SingleSolOffer(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    binding: BudgetedSingleSolBinding | SingleSolBinding

    intent_hash: str


def binding_for(row: ManualPaperRequest) -> SingleSolBinding:
    if (
        row.contract_version not in {4, 5}
        or row.mode != "research_only"
        or row.research_profile is None
    ):
        raise ValueError("SINGLE_SOL_REQUEST_BINDING_INVALID")
    try:
        model = BudgetedSingleSolBinding if row.contract_version == 5 else SingleSolBinding
        binding = model.model_validate(row.research_profile["single_sol"])
        if binding.deadline.utcoffset() is None or binding.deadline.date() != binding.budget_day:
            raise ValueError
        return binding
    except (ValueError, KeyError):
        raise ValueError("SINGLE_SOL_REQUEST_BINDING_INVALID") from None


def research_identity(version: int) -> tuple[str, str]:
    if version == 5:
        return "local-budgeted-single-sol-v1", "budgeted-single-sol"
    if version == 4:
        return "local-single-sol-research-v1", "single-sol"
    if version == 3:
        return "local-independent-monitor-research-v1", "independent-monitor"
    if version == 2:
        return "local-research-only-v2", "research-only"
    raise ValueError("MANUAL_REQUEST_INVALID")
