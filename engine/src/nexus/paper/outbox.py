"""At-least-once dispatcher for internal-paper outbox events."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.runtime import OutboxEvent


@dataclass(frozen=True)
class PaperOutboxMessage:
    """Stable delivery envelope; consumers deduplicate by ``event_id``."""

    event_id: UUID
    event_type: str
    aggregate_id: UUID
    schema_version: str
    payload: Mapping[str, object]


PaperOutboxPublisher = Callable[[PaperOutboxMessage], Awaitable[None]]


async def dispatch_paper_outbox(
    session: AsyncSession,
    *,
    publisher: PaperOutboxPublisher,
    published_at: datetime,
    limit: int = 100,
) -> int:
    """Deliver one locked page and mark it published in the caller transaction.

    Delivery is deliberately at-least-once: a crash after publication but before
    commit can redeliver the same globally unique event ID.
    """
    if published_at.tzinfo is None or published_at.utcoffset() != UTC.utcoffset(published_at):
        raise ValueError("published_at must use UTC")
    if not 1 <= limit <= 1_000:
        raise ValueError("limit must be between 1 and 1000")
    events = tuple(
        (
            await session.scalars(
                select(OutboxEvent)
                .where(
                    or_(
                        OutboxEvent.event_type.like("paper.%"),
                        OutboxEvent.event_type == "strategy_decision.persisted",
                    ),
                    OutboxEvent.status == "queued",
                    OutboxEvent.published_at.is_(None),
                )
                .order_by(OutboxEvent.created_at, OutboxEvent.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
        ).all()
    )
    for event in events:
        await publisher(
            PaperOutboxMessage(
                event_id=event.id,
                event_type=event.event_type,
                aggregate_id=event.aggregate_id,
                schema_version=event.schema_version,
                payload=dict(event.payload),
            )
        )
        event.status = "published"
        event.attempt_count += 1
        event.published_at = published_at
    await session.flush()
    return len(events)
