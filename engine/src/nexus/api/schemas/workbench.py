"""Strict redacted contracts for the internal-paper workbench."""

from __future__ import annotations

from typing import Literal
from uuid import UUID

from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.api.schemas.paper import (
    AgentPaperCycleResponse,
    AgentPaperSessionSummaryResponse,
    PaperOrderResponse,
)
from nexus.domain.decimal import DecimalString
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator


class _StrictSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")


class WorkbenchLoopResponse(_StrictSchema):
    status: Literal["online", "offline", "unknown"]
    reason_code: (
        Literal[
            "HEARTBEAT_MISSING", "HEARTBEAT_STALE", "HEARTBEAT_INCOMPATIBLE", "HEARTBEAT_INVALID"
        ]
        | None
    )
    started_at: AwareDatetime | None
    last_seen_at: AwareDatetime | None
    stale_after_seconds: Literal[30]


class WorkbenchAttentionResponse(_StrictSchema):
    kind: Literal["data", "model", "risk", "execution", "service"]
    severity: Literal["info", "warning", "critical"]
    title_code: str = Field(min_length=1, max_length=100)
    reason_code: str = Field(min_length=1, max_length=100)
    safe_action: Literal["view_evidence", "view_decision", "verify_original_result", "wait"]
    href: str = Field(min_length=1, max_length=500, pattern=r"^/")
    cycle_id: UUID | None = None
    order_id: UUID | None = None

    @field_validator("href")
    @classmethod
    def _href_is_local(cls, value: str) -> str:
        if not value.startswith("/") or value.startswith("//"):
            raise ValueError("href must be a local path")
        return value


class WorkbenchAccountSummaryResponse(_StrictSchema):
    portfolio_id: UUID
    currency: Literal["USD"]
    initial_capital: DecimalString
    data_state: Literal["EMPTY", "READY", "PARTIAL", "STALE"]
    as_of: AwareDatetime | None
    nav: DecimalString | None
    available_cash: DecimalString | None
    reserved_cash: DecimalString | None
    market_value: DecimalString | None
    realized_pnl: DecimalString | None
    unrealized_pnl: DecimalString | None


WorkbenchBlocker = Literal[
    "none",
    "data_missing",
    "stale",
    "unbound",
    "research_not_actionable",
    "risk_rejected",
    "result_unknown",
    "incompatible",
    "unknown",
]
WorkbenchStage = Literal[
    "window",
    "data",
    "agent",
    "risk",
    "execution",
    "recovery",
    "completed",
    "unknown",
]


class WorkbenchReadinessResponse(_StrictSchema):
    stage: WorkbenchStage
    blocker: WorkbenchBlocker
    reason_code: str = Field(min_length=1, max_length=100)
    previous_cycle_id: UUID | None
    previous_result_code: str | None = Field(default=None, min_length=1, max_length=100)
    changes: tuple[Literal["stage", "blocker", "result", "risk", "orders"], ...] = Field(
        max_length=5
    )


class WorkbenchCompatibilityResponse(_StrictSchema):
    status: Literal["compatible", "incompatible", "unknown"]
    expected_process_version: str = Field(max_length=100)
    expected_migration_head: str = Field(max_length=100)
    reported_process_version: str | None = Field(max_length=100)
    reported_migration_head: str | None = Field(max_length=100)
    database_migration_heads: tuple[str, ...] = Field(max_length=10)


class WorkbenchResponse(_StrictSchema):
    mode: Literal["internal_paper"]
    execution_locked: Literal[True]
    loop: WorkbenchLoopResponse
    readiness: WorkbenchReadinessResponse | None = None
    compatibility: WorkbenchCompatibilityResponse | None = None
    session: AgentPaperSessionSummaryResponse
    latest_cycle: AgentPaperCycleResponse | None
    active_orders: tuple[PaperOrderResponse, ...] = Field(max_length=20)
    account: WorkbenchAccountSummaryResponse | None
    attention: tuple[WorkbenchAttentionResponse, ...] = Field(max_length=10)


class WorkbenchEnvelope(_StrictSchema):
    data: WorkbenchResponse
    meta: SuccessMetaResponse
