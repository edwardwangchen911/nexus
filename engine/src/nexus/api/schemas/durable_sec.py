"""Saved durable discovery facts; no research or execution admission inference."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from nexus.api.schemas.research_monitoring import MonitoringViewModel
from nexus.outbox.failure import FailureReason
from pydantic import ConfigDict, Field


class DurableSecCoverageDetails(MonitoringViewModel):
    unread_history_files: int | None = Field(default=None, ge=0, le=10000, strict=True)
    invalid_rows: int = Field(ge=0, le=10000, strict=True)
    coverage_start_known: bool = Field(strict=True)


class DurableSecCheckView(MonitoringViewModel):
    scheduled_at: datetime
    checked_at: datetime
    result: Literal["true", "false", "unknown"]
    coverage: Literal["complete", "partial", "unknown"]
    interval_start: datetime | None = None
    interval_end: datetime | None = None
    reason: (
        Literal[
            "evidence_unverified",
            "coverage_incomplete",
            "coverage_conflict",
            "source_stale",
            "source_invalid",
            "time_invalid",
            "history_invalid",
            "rule_inactive",
        ]
        | None
    ) = None
    coverage_details: DurableSecCoverageDetails | None = None


class DurableSecNoticeView(MonitoringViewModel):
    outbox_id: UUID
    status: Literal["queued", "delivering", "published", "dead_letter"]
    attempt_count: Literal[0, 1]
    attempted_at: datetime | None = None
    finished_at: datetime | None = None
    failure_reason: FailureReason | None = None
    server_result: Literal["accepted", "http_error", "unconfirmed"] = "unconfirmed"
    device_result: Literal["unknown"] = "unknown"


DurableSecResearchStatus = Literal[
    "no_receipt",
    "bound",
    "increment_failed",
    "addressed_increment",
    "addressed_daily",
    "unverified",
]


class DurableSecEventView(MonitoringViewModel):
    # Older API processes omit the optional readback fields.
    model_config = ConfigDict(json_schema_serialization_defaults_required=False)

    event_key: str
    form: Literal["8-K", "8-K/A"]
    accepted_at: datetime
    first_seen_at: datetime
    metadata_status: Literal["recorded", "conflict"]
    notice: DurableSecNoticeView | None = None
    research_status: DurableSecResearchStatus | None = None


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
