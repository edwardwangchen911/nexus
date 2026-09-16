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
    reason_code: Literal["HEARTBEAT_MISSING", "HEARTBEAT_STALE", "HEARTBEAT_INCOMPATIBLE"] | None
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


class WorkbenchResponse(_StrictSchema):
    mode: Literal["internal_paper"]
    execution_locked: Literal[True]
    loop: WorkbenchLoopResponse
    session: AgentPaperSessionSummaryResponse
    latest_cycle: AgentPaperCycleResponse | None
    active_orders: tuple[PaperOrderResponse, ...] = Field(max_length=20)
    account: WorkbenchAccountSummaryResponse | None
    attention: tuple[WorkbenchAttentionResponse, ...] = Field(max_length=10)


class WorkbenchEnvelope(_StrictSchema):
    data: WorkbenchResponse
    meta: SuccessMetaResponse
