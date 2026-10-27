"""Allowlisted delivery diagnostics, separate from retry and delivery authority."""

from enum import StrEnum
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.runtime import AuditEvent

FAILURE_ACTION = "outbox.delivery.failure"
FAILURE_SCHEMA = "outbox_delivery_failure_v1"


class FailureReason(StrEnum):
    UNKNOWN = "UNKNOWN"
    PAYLOAD_INVALID = "PAYLOAD_INVALID"
    EVENT_UNSUPPORTED = "EVENT_UNSUPPORTED"
    ENCRYPTION_INVALID = "ENCRYPTION_INVALID"
    ENCRYPTION_FAILED = "ENCRYPTION_FAILED"
    CONNECT_TIMEOUT = "CONNECT_TIMEOUT"
    READ_TIMEOUT = "READ_TIMEOUT"
    WRITE_TIMEOUT = "WRITE_TIMEOUT"
    POOL_TIMEOUT = "POOL_TIMEOUT"
    TRANSPORT_ERROR = "TRANSPORT_ERROR"
    HTTP_REJECTED = "HTTP_REJECTED"
    RESPONSE_DECODE = "RESPONSE_DECODE"
    PROVIDER_REJECTED = "PROVIDER_REJECTED"


_STAGES = {
    FailureReason.UNKNOWN: "unknown",
    FailureReason.PAYLOAD_INVALID: "payload",
    FailureReason.EVENT_UNSUPPORTED: "payload",
    FailureReason.ENCRYPTION_INVALID: "encryption",
    FailureReason.ENCRYPTION_FAILED: "encryption",
    FailureReason.CONNECT_TIMEOUT: "transport",
    FailureReason.READ_TIMEOUT: "transport",
    FailureReason.WRITE_TIMEOUT: "transport",
    FailureReason.POOL_TIMEOUT: "transport",
    FailureReason.TRANSPORT_ERROR: "transport",
    FailureReason.HTTP_REJECTED: "response",
    FailureReason.RESPONSE_DECODE: "response",
    FailureReason.PROVIDER_REJECTED: "response",
}


class PublisherFailure(RuntimeError):
    """Exception text is never evidence; only this closed reason set is persisted."""

    def __init__(self, message: str, *, reason: FailureReason = FailureReason.UNKNOWN) -> None:
        super().__init__(message)
        self.reason = reason

    def failure_details(self) -> dict[str, str]:
        reason = self.reason if type(self.reason) is FailureReason else FailureReason.UNKNOWN
        return {"stage": _STAGES[reason], "reason": reason.value}


async def read_delivery_failure(
    session: AsyncSession, *, event_id: UUID, attempt_no: int
) -> dict[str, str] | None:
    rows = list(
        await session.scalars(
            select(AuditEvent.payload)
            .where(
                AuditEvent.action == FAILURE_ACTION,
                AuditEvent.schema_version == FAILURE_SCHEMA,
                AuditEvent.actor_id == "outbox-dispatcher",
                AuditEvent.payload["event_id"].as_string() == str(event_id),
                AuditEvent.payload["attempt_no"].as_string() == str(attempt_no),
            )
            .limit(2)
        )
    )
    if not rows:
        return None  # Legacy failures have no original detailed evidence.
    unknown = {"stage": "unknown", "reason": "UNKNOWN"}
    if len(rows) != 1:
        return unknown
    payload = rows[0]
    raw_reason = payload.get("reason")
    if not isinstance(raw_reason, str):
        return unknown
    try:
        reason = FailureReason(raw_reason)
    except (KeyError, ValueError, TypeError):
        return unknown
    if (
        set(payload) != {"event_id", "attempt_no", "stage", "reason"}
        or payload["event_id"] != str(event_id)
        or type(payload["attempt_no"]) is not int
        or payload["attempt_no"] != attempt_no
        or payload["stage"] != _STAGES[reason]
    ):
        return unknown
    return {"stage": _STAGES[reason], "reason": reason.value}
