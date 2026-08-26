"""Fail-closed user acknowledgement for the internal-paper drawdown lock."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast

from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.schemas.risk_audit import DrawdownAcknowledgeResponse
from nexus.audit.canonical import canonical_json
from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.paper.projection import DrawdownLockEvent, fold_drawdown_events
from nexus.persistence.idempotency import IdempotencyClaimError, claim_idempotency
from nexus.persistence.models.paper import (
    PaperAccountRecord,
    PaperDrawdownLockEventRecord,
    PaperPerformanceSnapshotRecord,
)
from nexus.persistence.models.runtime import OutboxEvent
from nexus.risk.models import DrawdownState

_ROUTE = "/internal/v1/risk/drawdown-lock/acknowledge"


class DrawdownAcknowledgeError(RuntimeError):
    """A stable closed reason for refusing an acknowledgement."""


async def acknowledge_drawdown_lock(
    session: AsyncSession,
    *,
    actor_id: str,
    expected_event_seq: int,
    idempotency_key: str,
    now: datetime,
) -> DrawdownAcknowledgeResponse:
    """Append the sole recovery acknowledgement without creating an order."""
    if now.tzinfo is None or now.utcoffset() != UTC.utcoffset(now):
        raise ValueError("drawdown acknowledgement time must use UTC")
    if expected_event_seq < 1 or not 1 <= len(idempotency_key) <= 255:
        raise ValueError("drawdown acknowledgement precondition is invalid")
    request_hash = hashlib.sha256(
        canonical_json(
            {
                "expected_event_seq": expected_event_seq,
                "schema": "drawdown_ack_request_v1",
            }
        ).encode()
    ).hexdigest()
    try:
        claim, created = await claim_idempotency(
            session,
            actor_id=actor_id,
            route=_ROUTE,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            schema_version="drawdown_ack_response_v1",
        )
    except IdempotencyClaimError as error:
        raise DrawdownAcknowledgeError(str(error)) from error
    if not created:
        if not claim.response_payload:
            raise DrawdownAcknowledgeError("IDEMPOTENCY_INCOMPLETE")
        return DrawdownAcknowledgeResponse.model_validate(claim.response_payload).model_copy(
            update={"replayed": True}
        )
    accounts = tuple(
        (
            await session.scalars(
                select(PaperAccountRecord)
                .where(PaperAccountRecord.capability == "simulation_write_internal")
                .order_by(PaperAccountRecord.id)
                .with_for_update()
            )
        ).all()
    )
    if len(accounts) != 1:
        raise DrawdownAcknowledgeError("INTERNAL_ACCOUNT_AMBIGUOUS")
    account = accounts[0]
    rows = tuple(
        (
            await session.scalars(
                select(PaperDrawdownLockEventRecord)
                .where(PaperDrawdownLockEventRecord.account_id == account.id)
                .order_by(PaperDrawdownLockEventRecord.event_seq)
                .with_for_update()
            )
        ).all()
    )
    if not rows:
        raise DrawdownAcknowledgeError("DRAWDOWN_STATE_UNKNOWN")
    events = tuple(
        DrawdownLockEvent(
            event_id=row.id,
            event_seq=row.event_seq,
            from_state=cast(DrawdownState | None, row.from_state),
            to_state=cast(DrawdownState, row.to_state),
            drawdown_ratio=row.drawdown_ratio,
            user_acknowledged=row.user_acknowledged,
            reason_code=row.reason_code,
            occurred_at=row.occurred_at,
            payload_hash=row.payload_hash,
        )
        for row in rows
    )
    try:
        current_state = fold_drawdown_events(events)
    except ValueError as error:
        raise DrawdownAcknowledgeError("DRAWDOWN_STATE_INVALID") from error
    latest = rows[-1]
    if latest.event_seq != expected_event_seq:
        raise DrawdownAcknowledgeError("VERSION_CONFLICT")
    if now < latest.occurred_at:
        raise DrawdownAcknowledgeError("EVENT_TIME_CONFLICT")
    if current_state != "recovery_eligible":
        raise DrawdownAcknowledgeError("DRAWDOWN_NOT_RECOVERY_ELIGIBLE")
    performance = await session.scalar(
        select(PaperPerformanceSnapshotRecord)
        .where(PaperPerformanceSnapshotRecord.account_id == account.id)
        .order_by(
            desc(PaperPerformanceSnapshotRecord.as_of),
            desc(PaperPerformanceSnapshotRecord.id),
        )
        .limit(1)
    )
    if performance is None:
        raise DrawdownAcknowledgeError("PERFORMANCE_UNKNOWN")
    if performance.drawdown_ratio >= Decimal("0.06"):
        raise DrawdownAcknowledgeError("DRAWDOWN_THRESHOLD_NOT_MET")
    event_seq = latest.event_seq + 1
    payload = {
        "drawdown_ratio": format(performance.drawdown_ratio, ".12f"),
        "event_seq": event_seq,
        "from_state": "recovery_eligible",
        "reason_code": "USER_ACK_RECOVERY",
        "to_state": "unlocked",
        "user_acknowledged": True,
    }
    payload_hash = hashlib.sha256(
        canonical_json(
            {
                "kind": "paper_drawdown_lock_event",
                "payload": payload,
                "schema": "paper_fact_payload_sha256_v1",
            }
        ).encode()
    ).hexdigest()
    event = PaperDrawdownLockEventRecord(
        id=new_uuid7(),
        account_id=account.id,
        event_seq=event_seq,
        from_state="recovery_eligible",
        to_state="unlocked",
        drawdown_ratio=performance.drawdown_ratio,
        user_acknowledged=True,
        reason_code="USER_ACK_RECOVERY",
        occurred_at=now,
        payload_hash=payload_hash,
    )
    session.add(event)
    audit_payload = {
        "event_seq": event_seq,
        "outcome": "unlocked",
        "reason_code": "USER_ACK_RECOVERY",
        "status": "unlocked",
    }
    await append_audit_event(
        session,
        actor_id=f"user:{actor_id}",
        action="drawdown.lock.acknowledged",
        schema_version="paper_drawdown_v1",
        payload=audit_payload,
        occurred_at=now,
    )
    session.add(
        OutboxEvent(
            id=new_uuid7(),
            event_type="paper.drawdown_lock.acknowledged",
            aggregate_id=account.id,
            schema_version="paper_drawdown_v1",
            payload=audit_payload,
            published_at=None,
        )
    )
    result = DrawdownAcknowledgeResponse(state="unlocked", event_seq=event_seq, replayed=False)
    claim.response_payload = result.model_dump(mode="json")
    await session.flush()
    return result
