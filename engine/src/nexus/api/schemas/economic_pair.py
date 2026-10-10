"""Redacted persisted pair references and exact execution results."""

from datetime import date
from typing import Literal
from uuid import UUID

from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.api.schemas.paper import (
    AgentPaperManagementCycleResponse,
    PaperPlanActualResponse,
    PaperPortfolioResponse,
)
from nexus.paper.agent_cycle_models import CycleStatus
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EconomicCycleRefResponse(_Strict):
    lane: Literal["incumbent", "challenger"]
    cycle_id: UUID | None
    cycle_status: CycleStatus | None


class EconomicExecutionLaneResponse(_Strict):
    lane: Literal["incumbent", "challenger"]
    status: Literal[
        "unknown", "unavailable", "partial", "risk_rejected", "filled", "not_required", "no_trade"
    ]
    order_count: int = Field(ge=0)


class EconomicExecutionOrderResponse(_Strict):
    lane: Literal["incumbent", "challenger"]
    order_id: UUID
    status: str = Field(min_length=1, max_length=64, pattern=r"^[a-z_]+$")
    action: Literal["filled", "partially_filled", "unknown", "unavailable", "not_attempted"]
    code: str = Field(min_length=1, max_length=100, pattern=r"^[A-Z0-9_]+$")


class EconomicExecutionResponse(_Strict):
    status: Literal["not_started", "unknown", "unavailable", "partial", "completed"]
    window_id: UUID | None
    lanes: tuple[EconomicExecutionLaneResponse, ...] = Field(min_length=2, max_length=2)
    orders: tuple[EconomicExecutionOrderResponse, ...]
    comparison_status: Literal["not_evaluated", "incomplete", "incomparable"]


class EconomicPairResponse(_Strict):
    pair_slot_id: UUID
    evaluation_id: UUID
    ordinal: int = Field(ge=1, le=24)
    planned_slots: Literal[24]
    decision_session: date
    decision_at: AwareDatetime
    execution_locked: Literal[True]
    economic_status: Literal["not_evaluated"]
    lanes: tuple[EconomicCycleRefResponse, ...] = Field(min_length=2, max_length=2)
    execution: EconomicExecutionResponse | None
    gap_code: Literal["ECONOMIC_WINDOW_REQUIRED"] | None


class EconomicPairEnvelope(_Strict):
    data: EconomicPairResponse
    meta: SuccessMetaResponse


class EconomicLaneResponse(_Strict):
    pair: EconomicPairResponse
    lane: Literal["incumbent", "challenger"]
    detail: AgentPaperManagementCycleResponse | None
    actual: PaperPlanActualResponse
    portfolio: PaperPortfolioResponse


class EconomicLaneEnvelope(_Strict):
    data: EconomicLaneResponse
    meta: SuccessMetaResponse
