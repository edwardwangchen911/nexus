"""Short-transaction, at-least-once outbox dispatcher."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.domain.ids import new_uuid7
from nexus.outbox.models import OutboxMessage
from nexus.outbox.registry import PublisherRegistry
from nexus.persistence.models.runtime import OutboxDeliveryAttempt, OutboxEvent

Clock = Callable[[], datetime]
AfterPublish = Callable[[OutboxMessage], Awaitable[None]]


class OutboxLeaseLostError(RuntimeError):
    """The delivery attempt is no longer the current unexpired authority."""


@dataclass(frozen=True)
class _ClaimedOutbox:
    event_id: UUID
    event_type: str
    aggregate_id: UUID
    schema_version: str
    payload: dict[str, object]
    attempt_no: int
    worker_id: str
    lease_token: UUID
    expires_at: datetime


def _utc(value: datetime, *, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != UTC.utcoffset(value):
        raise ValueError(f"{field} must use UTC")
    return value.astimezone(UTC)


def _worker_id(value: str) -> str:
    if not 1 <= len(value) <= 255 or value.strip() != value:
        raise ValueError("worker_id is invalid")
    return value


async def _reclaim_expired(
    session: AsyncSession,
    *,
    now: datetime,
    limit: int = 100,
) -> int:
    events = tuple(
        await session.scalars(
            select(OutboxEvent)
            .where(
                OutboxEvent.status == "delivering",
                OutboxEvent.available_at <= now,
            )
            .order_by(OutboxEvent.available_at, OutboxEvent.id)
            .with_for_update(skip_locked=True)
            .limit(limit)
        )
    )
    for event in events:
        attempt = await session.scalar(
            select(OutboxDeliveryAttempt)
            .where(
                OutboxDeliveryAttempt.outbox_event_id == event.id,
                OutboxDeliveryAttempt.attempt_no == event.attempt_count,
                OutboxDeliveryAttempt.outcome == "delivering",
            )
            .with_for_update()
        )
        if attempt is None:
            raise RuntimeError("delivering outbox event has no current attempt")
        attempt.outcome = "lost"
        attempt.finished_at = now
        attempt.error_code = "LEASE_EXPIRED"
        event.last_error_code = "LEASE_EXPIRED"
        if event.attempt_count < event.max_attempts:
            event.status = "queued"
            event.available_at = now
        else:
            event.status = "dead_letter"
            event.dead_lettered_at = now
    await session.flush()
    return len(events)


async def _claim(
    session: AsyncSession,
    *,
    worker_id: str,
    now: datetime,
    lease_seconds: int,
    event_keys: Sequence[tuple[str, str]] | None,
) -> _ClaimedOutbox | None:
    conditions = [
        OutboxEvent.status == "queued",
        OutboxEvent.available_at <= now,
        OutboxEvent.attempt_count < OutboxEvent.max_attempts,
    ]
    if event_keys is not None:
        if not event_keys:
            return None
        conditions.append(
            or_(
                *(
                    (OutboxEvent.event_type == event_type)
                    & (OutboxEvent.schema_version == schema_version)
                    for event_type, schema_version in event_keys
                )
            )
        )
    event = await session.scalar(
        select(OutboxEvent)
        .where(*conditions)
        .order_by(OutboxEvent.available_at, OutboxEvent.created_at, OutboxEvent.id)
        .with_for_update(skip_locked=True)
        .limit(1)
    )
    if event is None:
        return None
    lease_token = new_uuid7()
    event.status = "delivering"
    event.attempt_count += 1
    event.available_at = now + timedelta(seconds=lease_seconds)
    event.last_error_code = None
    attempt = OutboxDeliveryAttempt(
        id=new_uuid7(),
        outbox_event_id=event.id,
        attempt_no=event.attempt_count,
        worker_id=worker_id,
        lease_token=lease_token,
        started_at=now,
        finished_at=None,
        outcome="delivering",
        error_code=None,
    )
    session.add(attempt)
    await session.flush()
    return _ClaimedOutbox(
        event_id=event.id,
        event_type=event.event_type,
        aggregate_id=event.aggregate_id,
        schema_version=event.schema_version,
        payload=dict(event.payload),
        attempt_no=event.attempt_count,
        worker_id=worker_id,
        lease_token=lease_token,
        expires_at=event.available_at,
    )


async def _lock_claim(
    session: AsyncSession,
    *,
    claim: _ClaimedOutbox,
    now: datetime,
) -> tuple[OutboxEvent, OutboxDeliveryAttempt]:
    event = await session.scalar(
        select(OutboxEvent).where(OutboxEvent.id == claim.event_id).with_for_update()
    )
    attempt = await session.scalar(
        select(OutboxDeliveryAttempt)
        .where(
            OutboxDeliveryAttempt.outbox_event_id == claim.event_id,
            OutboxDeliveryAttempt.attempt_no == claim.attempt_no,
        )
        .with_for_update()
    )
    if (
        event is None
        or event.status != "delivering"
        or event.attempt_count != claim.attempt_no
        or event.available_at <= now
        or attempt is None
        or attempt.lease_token != claim.lease_token
        or attempt.worker_id != claim.worker_id
        or attempt.outcome != "delivering"
    ):
        raise OutboxLeaseLostError("outbox delivery lease is stale or expired")
    return event, attempt


async def _complete(
    session: AsyncSession,
    *,
    claim: _ClaimedOutbox,
    now: datetime,
) -> None:
    event, attempt = await _lock_claim(session, claim=claim, now=now)
    attempt.outcome = "published"
    attempt.finished_at = now
    attempt.error_code = None
    event.status = "published"
    event.published_at = now
    event.last_error_code = None
    await session.flush()


async def _fail(
    session: AsyncSession,
    *,
    claim: _ClaimedOutbox,
    now: datetime,
    error_code: str,
    retryable: bool,
) -> None:
    event, attempt = await _lock_claim(session, claim=claim, now=now)
    attempt.outcome = "failed"
    attempt.finished_at = now
    attempt.error_code = error_code
    event.last_error_code = error_code
    if retryable and event.attempt_count < event.max_attempts:
        event.status = "queued"
        event.available_at = now + timedelta(seconds=min(2**event.attempt_count, 60))
    else:
        event.status = "dead_letter"
        event.dead_lettered_at = now
    await session.flush()


async def dispatch_outbox_once(
    factory: async_sessionmaker[AsyncSession],
    *,
    registry: PublisherRegistry,
    worker_id: str,
    clock: Clock,
    lease_seconds: int = 60,
    after_publish: AfterPublish | None = None,
    claim_registered_only: bool = False,
    raise_publisher_errors: bool = False,
) -> bool:
    """Dispatch at most one event and durably record its delivery attempt."""
    worker_id = _worker_id(worker_id)
    if not 1 <= lease_seconds <= 60:
        raise ValueError("lease_seconds must be between 1 and 60")
    claim_time = _utc(clock(), field="clock")
    async with factory() as session, session.begin():
        await _reclaim_expired(session, now=claim_time)
        claim = await _claim(
            session,
            worker_id=worker_id,
            now=claim_time,
            lease_seconds=lease_seconds,
            event_keys=(
                tuple(sorted(registry.keys)) if claim_registered_only else None
            ),
        )
    if claim is None:
        return False
    spec = registry.resolve(claim.event_type, claim.schema_version)
    if spec is None:
        async with factory() as session, session.begin():
            await _fail(
                session,
                claim=claim,
                now=_utc(clock(), field="clock"),
                error_code="PUBLISHER_UNREGISTERED",
                retryable=False,
            )
        return True
    message = OutboxMessage(
        event_id=claim.event_id,
        event_type=claim.event_type,
        aggregate_id=claim.aggregate_id,
        schema_version=claim.schema_version,
        payload=claim.payload,
    )
    try:
        await spec.publisher(message)
    except Exception as error:
        try:
            async with factory() as session, session.begin():
                await _fail(
                    session,
                    claim=claim,
                    now=_utc(clock(), field="clock"),
                    error_code="PUBLISHER_FAILED",
                    retryable=True,
                )
        except OutboxLeaseLostError:
            pass
        if raise_publisher_errors:
            raise error
        return True
    if after_publish is not None:
        await after_publish(message)
    try:
        async with factory() as session, session.begin():
            await _complete(
                session,
                claim=claim,
                now=_utc(clock(), field="clock"),
            )
    except OutboxLeaseLostError:
        pass
    return True
