"""Strict redacted contracts for readiness and the risk-audit console."""

from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from nexus.api.schemas.integrations import SuccessMetaResponse
from pydantic import BaseModel, ConfigDict, Field, field_validator


class _StrictSchema(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ReadinessComponents(_StrictSchema):
    database: Literal["ready", "unavailable", "incompatible", "stale"]
    migration: Literal["ready", "unavailable", "incompatible", "stale"]
    object_store: Literal["ready", "unavailable", "incompatible", "stale"]
    worker: Literal["ready", "unavailable", "incompatible", "stale"]


class ReadinessResponse(_StrictSchema):
    status: Literal["ready", "unready"]
    components: ReadinessComponents


class ReadinessEnvelope(_StrictSchema):
    data: ReadinessResponse
    meta: SuccessMetaResponse


class RiskAuditEventResponse(_StrictSchema):
    event_seq: int = Field(ge=1)
    occurred_at: datetime
    kind: Literal["quality", "risk", "agent_cost", "job", "reconciliation", "audit"]
    actor_kind: Literal["user", "system", "service"]
    action: str = Field(min_length=1, max_length=255)
    schema_version: str = Field(min_length=1, max_length=32)
    outcome: dict[str, str | bool | int]
    previous_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    event_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("occurred_at")
    @classmethod
    def _time_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("occurred_at must be timezone-aware")
        return value.astimezone(UTC)


class RetryableJobSummary(_StrictSchema):
    job_id: UUID
    job_type: str = Field(min_length=1, max_length=50)
    status: Literal["failed", "dead_letter"]
    version: int = Field(ge=1)
    manual_retry_count: int = Field(ge=0, le=2)
    last_error_code: str | None = Field(default=None, max_length=100)
    finished_at: datetime


class FailedJobSummary(_StrictSchema):
    count: int = Field(ge=0)
    retryable: list[RetryableJobSummary]


class DrawdownLockSummary(_StrictSchema):
    state: Literal["unlocked", "locked", "recovery_eligible", "unknown"]
    event_seq: int | None = Field(default=None, ge=1)
    acknowledge_allowed: bool


class RiskAuditSummary(_StrictSchema):
    data_quality: Literal["PASS", "PARTIAL", "FAIL", "UNKNOWN"]
    drawdown_lock: DrawdownLockSummary
    failed_jobs: FailedJobSummary


class RiskAuditPageResponse(_StrictSchema):
    summary: RiskAuditSummary
    items: list[RiskAuditEventResponse]
    next_cursor: str | None


class RiskAuditPageEnvelope(_StrictSchema):
    data: RiskAuditPageResponse
    meta: SuccessMetaResponse


class EmptyActionRequest(_StrictSchema):
    """Reject accidental mutable payloads on Job retry."""


class JobRetryResponse(_StrictSchema):
    job_id: UUID
    status: Literal["queued"]
    version: int = Field(ge=1)
    manual_retry_count: int = Field(ge=1, le=2)
    replayed: bool


class JobRetryEnvelope(_StrictSchema):
    data: JobRetryResponse
    meta: SuccessMetaResponse


class DrawdownAcknowledgeRequest(_StrictSchema):
    expected_event_seq: int = Field(ge=1)


class DrawdownAcknowledgeResponse(_StrictSchema):
    state: Literal["unlocked"]
    event_seq: int = Field(ge=2)
    replayed: bool


class DrawdownAcknowledgeEnvelope(_StrictSchema):
    data: DrawdownAcknowledgeResponse
    meta: SuccessMetaResponse
