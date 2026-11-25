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
    # Earlier API versions omit the new readback fields.
    model_config = ConfigDict(json_schema_serialization_defaults_required=False)

    scheduled_at: datetime
    checked_at: datetime
    result: Literal["true", "false", "unknown"]
    coverage: Literal["complete", "partial", "unknown"]
    interval_start: datetime | None
    interval_end: datetime | None
    evaluation_version: Literal["accepted_cutoff_v1"] | None = None
    source_received_at: datetime | None = None
    accepted_complete_through_at: datetime | None = None
    reason: (
        Literal[
            "evidence_unverified",
            "coverage_incomplete",
            "accepted_cutoff_unproven",
            "coverage_conflict",
            "source_stale",
            "source_invalid",
            "collection_failed",
            "time_invalid",
            "history_invalid",
            "rule_inactive",
        ]
        | None
    )
    coverage_details: DurableSecCoverageDetails | None


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
    "observation_only",
    "no_receipt",
    "bound",
    "increment_failed",
    "addressed_increment",
    "addressed_daily",
    "unverified",
]


class DurableSecPreparationRejection(MonitoringViewModel):
    rejected_at: datetime
    reason: Literal["ineligible", "account_unavailable"]


class DurableSecEventView(MonitoringViewModel):
    # Older API processes omit the optional readback fields.
    model_config = ConfigDict(json_schema_serialization_defaults_required=False)

    event_key: str
    form: Literal["8-K", "8-K/A"]
    accepted_at: datetime
    first_seen_at: datetime
    metadata_status: Literal["recorded", "conflict"]
    event_id: UUID | None = None
    notice: DurableSecNoticeView | None = None
    research_status: DurableSecResearchStatus | None = None
    preparation_rejection: DurableSecPreparationRejection | None = None


class DurableSecCollectionView(MonitoringViewModel):
    scheduled_at: datetime
    claimed_at: datetime
    finished_at: datetime | None
    status: Literal["archived", "failed", "unknown"]


class DurableSecView(MonitoringViewModel):
    # Collection readback is negotiated independently of the existing discovery fields.
    model_config = ConfigDict(json_schema_serialization_defaults_required=False)

    observed_at: datetime
    rule_id: UUID
    state: Literal["active", "paused", "revoked", "unknown"]
    effective_at: datetime | None
    forms: tuple[Literal["8-K", "8-K/A"], ...]
    latest_check: DurableSecCheckView | None
    event_count: int = Field(ge=0)
    events: tuple[DurableSecEventView, ...] = Field(max_length=5)
    execution_locked: Literal[True]
    latest_collection: DurableSecCollectionView | None = None
