"""Saved durable discovery facts; no research or execution admission inference."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from nexus.api.schemas.research_monitoring import MonitoringViewModel
from pydantic import Field


class DurableSecCheckView(MonitoringViewModel):
    scheduled_at: datetime
    checked_at: datetime
    result: Literal["true", "false", "unknown"]
    coverage: Literal["complete", "partial", "unknown"]
    interval_start: datetime | None = None
    interval_end: datetime | None = None
    reason: Literal["evidence_unverified"] | None = None


class DurableSecEventView(MonitoringViewModel):
    event_key: str
    form: Literal["8-K", "8-K/A"]
    accepted_at: datetime
    first_seen_at: datetime
    metadata_status: Literal["recorded", "conflict"]


class DurableSecView(MonitoringViewModel):
    observed_at: datetime
    rule_id: UUID
    state: Literal["active", "paused", "revoked", "unknown"]
    effective_at: datetime | None = None
    forms: tuple[Literal["8-K", "8-K/A"], ...] = ()
    latest_check: DurableSecCheckView | None = None
    event_count: int = Field(default=0, ge=0)
    events: tuple[DurableSecEventView, ...] = Field(default=(), max_length=5)
    execution_locked: Literal[True] = True
