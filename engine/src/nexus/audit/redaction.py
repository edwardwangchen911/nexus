"""Closed, non-identifying projection for risk and audit exports."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

Hash64 = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
AuditKind = Literal["quality", "risk", "agent_cost", "job", "reconciliation", "audit"]

_SAFE_OUTCOME_FIELDS = frozenset(
    {
        "completeness",
        "error_code",
        "outcome",
        "quality_status",
        "reason_code",
        "result",
        "retryable",
        "status",
    }
)


class AuditExportSource(BaseModel):
    """Private source row accepted by the allowlist-only export projection."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    event_seq: int = Field(ge=1)
    occurred_at: datetime
    actor_id: str = Field(min_length=1, max_length=255)
    action: str = Field(min_length=1, max_length=255)
    schema_version: str = Field(min_length=1, max_length=32)
    payload: dict[str, object]
    previous_hash: Hash64
    event_hash: Hash64

    @field_validator("occurred_at")
    @classmethod
    def _occurred_at_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
            raise ValueError("audit export time must use UTC")
        return value


def classify_audit_action(action: str) -> AuditKind:
    """Map a private action name into one public, closed category."""
    if action.startswith(("quality.", "source.")):
        return "quality"
    if action.startswith(("risk.", "paper.risk.", "drawdown.")):
        return "risk"
    if action.startswith(("agent.budget.", "agent.cost.")):
        return "agent_cost"
    if action.startswith(("job.", "outbox.")):
        return "job"
    if action.startswith(("reconciliation.", "broker.reconciliation.")):
        return "reconciliation"
    return "audit"


def _actor_kind(actor_id: str) -> Literal["user", "system", "service"]:
    if actor_id.startswith("user:"):
        return "user"
    if actor_id.startswith("system:"):
        return "system"
    return "service"


def redact_audit_event(source: AuditExportSource) -> dict[str, object]:
    """Project one source event without copying any non-allowlisted payload key."""
    outcome = {
        key: source.payload[key]
        for key in sorted(_SAFE_OUTCOME_FIELDS)
        if key in source.payload and isinstance(source.payload[key], str | bool | int)
    }
    return {
        "event_seq": source.event_seq,
        "occurred_at": source.occurred_at.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "kind": classify_audit_action(source.action),
        "actor_kind": _actor_kind(source.actor_id),
        "action": source.action,
        "schema_version": source.schema_version,
        "outcome": outcome,
        "previous_hash": source.previous_hash,
        "event_hash": source.event_hash,
    }
