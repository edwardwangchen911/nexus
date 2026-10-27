"""One explicitly requested communication test; never a business event."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, field_validator
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.outbox.failure import read_delivery_failure
from nexus.persistence.models.runtime import OutboxEvent

DIAGNOSTIC_EVENT = ("diagnostic.notification.test", "diagnostic_notification_v1")
DIAGNOSTIC_TITLE = "NEXUS 测试通知｜非投资预警"


class DiagnosticNotificationRejected(ValueError):
    """A fixed, explicitly pre-submission rejection; never a transport outcome."""


class DiagnosticNotification(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    schema_version: Literal["diagnostic_notification_v1"]
    test_id: UUID
    occurred_at: datetime

    @field_validator("occurred_at", mode="before")
    @classmethod
    def timestamp_text(cls, value: object) -> object:
        if not isinstance(value, str):
            raise ValueError("DIAGNOSTIC_NOTIFICATION_INVALID")
        return value

    @field_validator("occurred_at")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    def render(self) -> dict[str, str]:
        return {
            "title": DIAGNOSTIC_TITLE,
            "body": DIAGNOSTIC_TITLE + "\n仅验证通知通信，不代表市场触发、研究结论或交易许可。",
            "group": "NEXUS 测试",
            "id": f"diagnostic-test:{self.test_id}",
            "message_id": str(self.test_id),
        }


def _receipt(row: OutboxEvent) -> dict[str, object]:
    payload = DiagnosticNotification.model_validate(row.payload)
    if (
        (row.event_type, row.schema_version) != DIAGNOSTIC_EVENT
        or row.id != row.aggregate_id
        or payload.test_id != row.id
        or row.max_attempts != 1
        or payload.occurred_at != row.created_at
    ):
        raise ValueError("DIAGNOSTIC_NOTIFICATION_INVALID")
    # A dead letter may follow a network timeout or acknowledgement loss.
    # Neither is evidence that the provider did not accept the request.
    status = {
        "queued": "queued",
        "delivering": "unknown",
        "published": "provider_accepted",
        "dead_letter": "unknown",
    }[row.status]
    return {
        "test_id": str(row.id),
        "status": status,
        "outbox_status": row.status,
        "attempt_count": row.attempt_count,
        "created_at": row.created_at.isoformat(),
        "provider_accepted_at": None if row.published_at is None else row.published_at.isoformat(),
        "device_delivery": "unverified",
        "execution_locked": True,
        "failure": None,
    }


async def read_diagnostic_notification(
    session: AsyncSession, *, test_id: UUID
) -> dict[str, object]:
    row = await session.get(OutboxEvent, test_id, populate_existing=True)
    if row is None:
        return {"test_id": str(test_id), "status": "not_found", "device_delivery": "unverified"}
    receipt = _receipt(row)
    receipt["failure"] = await read_delivery_failure(
        session, event_id=row.id, attempt_no=row.attempt_count
    )
    return receipt


async def enqueue_diagnostic_notification(
    session: AsyncSession, *, test_id: UUID, activated_at: datetime
) -> dict[str, object]:
    """Caller owns the transaction; one UUID, one audit, at most one external attempt."""
    require_utc(activated_at)
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
        {"key": f"nexus:diagnostic-notification:{test_id}"},
    )
    previous = await session.get(OutboxEvent, test_id, populate_existing=True)
    if previous is not None:
        return await read_diagnostic_notification(session, test_id=test_id)
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or now < activated_at:
        raise DiagnosticNotificationRejected("DIAGNOSTIC_NOTIFICATION_NOT_ACTIVE")
    payload: dict[str, object] = {
        "schema_version": DIAGNOSTIC_EVENT[1],
        "test_id": str(test_id),
        "occurred_at": now.isoformat(),
    }
    DiagnosticNotification.model_validate(payload)
    row = OutboxEvent(
        id=test_id,
        aggregate_id=test_id,
        event_type=DIAGNOSTIC_EVENT[0],
        schema_version=DIAGNOSTIC_EVENT[1],
        payload=payload,
        max_attempts=1,
        attempt_count=0,
        status="queued",
        created_at=now,
        available_at=now,
    )
    session.add(row)
    await append_audit_event(
        session,
        actor_id="local-diagnostic-notification",
        action=DIAGNOSTIC_EVENT[0],
        schema_version=DIAGNOSTIC_EVENT[1],
        payload=payload,
        occurred_at=now,
    )
    await session.flush()
    return _receipt(row)
