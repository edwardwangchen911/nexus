"""Additive, bounded read model; never an admission result."""

from datetime import date
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field

from nexus.workbench.research_summary import WorkbenchCandidateResponse


class WorkbenchActivityScopeResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mode: Literal["research_and_paper", "research_only", "research_and_monitor"]
    today_registered: int | None = Field(default=None, ge=0)
    today_started: int | None = Field(default=None, ge=0)
    registration_date: date
    decision_session: date | None
    selected_decision_session: date | None = None
    selection: Literal[
        "active", "historical_unresolved", "today", "historical", "none", "ambiguous", "unavailable"
    ]
    request_id: UUID | None = None
    cycle_id: UUID | None = None
    requested_at: AwareDatetime | None = None
    status: str | None = Field(default=None, max_length=40)
    reason_code: str | None = Field(default=None, max_length=100)
    trigger: Literal["manual", "scheduled"] | None = None
    historical: bool = False
    occupancy: Literal["unresolved", "released", "not_indicated", "unverified"] = "not_indicated"
    portfolio_status: (
        Literal[
            "not_applicable",
            "awaiting_cycle",
            "pending",
            "deferred",
            "no_order_required",
            "recorded",
            "unavailable",
        ]
        | None
    ) = None
    portfolio_reason_code: str | None = Field(default=None, max_length=100)
    href: str | None = None
    candidates: tuple[WorkbenchCandidateResponse, ...] = Field(default=(), max_length=5)


class WorkbenchActivityResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")
    read_at: AwareDatetime
    read_status: Literal["complete", "partial"]
    scopes: tuple[WorkbenchActivityScopeResponse, ...] = Field(min_length=2, max_length=3)
