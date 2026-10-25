"""Read-only monitoring observations, separate from the frozen research conclusion."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from nexus.monitoring.contracts import DisclosureReviewCondition, PriceReviewCondition
from pydantic import BaseModel, ConfigDict, Field

type MonitoringState = Literal[
    "not_installed", "valid", "expired", "superseded", "withdrawn", "unknown"
]
type CoverageReason = Literal[
    "audit_rejected",
    "source_invalid",
    "no_condition",
    "run_failed",
    "usage_unknown",
    "awaiting_reconciliation",
    "events_unresolved",
    "review_unverified",
    "replacement_rejected",
]
type BlockedReason = Literal["budget", "account_occupied", "inputs", "overflow", "unknown"]
type UsageStatus = Literal["not_started", "in_progress", "known", "unknown"]


class MonitoringViewModel(BaseModel):
    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        hide_input_in_errors=True,
        json_schema_serialization_defaults_required=True,
    )


class ResearchMonitorCheck(MonitoringViewModel):
    result: Literal["true", "false", "unknown"]
    scheduled_at: datetime
    checked_at: datetime
    reason: (
        Literal["observation_missing", "authority_mismatch", "time_unproven", "price_missing"]
        | None
    )


class ResearchMonitorTrigger(MonitoringViewModel):
    trigger_id: UUID
    cause: Literal["initial_match", "crossed"]
    checked_at: datetime
    status: Literal["pending", "blocked", "admitted", "completed", "failed", "unknown", "covered"]
    blocked_reason: BlockedReason | None
    run_id: UUID | None
    usage_status: UsageStatus
    notification_status: Literal["queued", "delivering", "published", "dead_letter", "unknown"]


class ResearchMonitoringView(MonitoringViewModel):
    observed_at: datetime
    state: MonitoringState
    plan_id: UUID | None = None
    condition: PriceReviewCondition | None = None
    valid_until: datetime | None = None
    review_due_at: datetime | None = None
    replacement_run_id: UUID | None = None
    coverage_reason: CoverageReason | None = None
    latest_check: ResearchMonitorCheck | None = None
    triggers: tuple[ResearchMonitorTrigger, ...] = Field(default=(), max_length=5)
    trigger_count: int = Field(default=0, ge=0)
    execution_locked: Literal[True] = True


class ResearchDisclosureMonitorCheck(MonitoringViewModel):
    result: Literal["true", "false", "unknown"]
    scheduled_at: datetime
    checked_at: datetime
    reason: Literal["observation_unverified"] | None


class ResearchDisclosureMonitorTrigger(MonitoringViewModel):
    trigger_id: UUID
    batch_id: UUID | None
    event_key: str
    checked_at: datetime
    status: Literal["pending", "blocked", "admitted", "completed", "failed", "unknown", "covered"]
    blocked_reason: BlockedReason | None
    run_id: UUID | None
    usage_status: UsageStatus
    notification_status: Literal["queued", "delivering", "published", "dead_letter", "unknown"]
    event_review: Literal["addressed", "unresolved", "unverified", "not_performed"]


class ResearchDisclosureMonitoringView(MonitoringViewModel):
    observed_at: datetime
    state: MonitoringState
    coverage_reason: CoverageReason | None = None
    plan_id: UUID | None = None
    condition: DisclosureReviewCondition | None = None
    baseline_as_of: datetime | None = None
    review_due_at: datetime | None = None
    valid_until: datetime | None = None
    replacement_run_id: UUID | None = None
    latest_check: ResearchDisclosureMonitorCheck | None = None
    trigger_count: int = Field(default=0, ge=0)
    triggers: tuple[ResearchDisclosureMonitorTrigger, ...] = Field(default=(), max_length=5)
    execution_locked: Literal[True] = True
