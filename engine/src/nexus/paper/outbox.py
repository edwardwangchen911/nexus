"""Compatibility wrapper over the recoverable generic outbox dispatcher."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.domain.ids import new_uuid7
from nexus.outbox.dispatcher import dispatch_outbox_once
from nexus.outbox.models import OutboxMessage, PublisherSpec
from nexus.outbox.registry import PublisherRegistry

PaperOutboxMessage = OutboxMessage
PaperOutboxPublisher = Callable[[PaperOutboxMessage], Awaitable[None]]

_PAPER_EVENT_SCHEMAS = (
    ("strategy_decision.persisted", "paper_event_v1"),
    ("paper.order_batch.evaluated", "paper_event_v1"),
    ("paper.order.risk_rejected", "paper_event_v1"),
    ("paper.order.submitted", "paper_event_v1"),
    ("paper.reservation.created", "paper_event_v1"),
    ("paper.ledger_transaction.posted", "paper_event_v1"),
    ("paper.execution.advance.partially_filled", "paper_execution_v1"),
    ("paper.execution.advance.filled", "paper_execution_v1"),
    ("paper.execution.advance.cancelled", "paper_execution_v1"),
    ("paper.execution.cancelled", "paper_execution_v1"),
)


def _registry(publisher: PaperOutboxPublisher) -> PublisherRegistry:
    return PublisherRegistry(
        PublisherSpec(
            event_type=event_type,
            schema_version=schema_version,
            consumer_idempotent=True,
            publisher=publisher,
        )
        for event_type, schema_version in _PAPER_EVENT_SCHEMAS
    )


async def dispatch_paper_outbox(
    session: AsyncSession,
    *,
    publisher: PaperOutboxPublisher,
    published_at: datetime,
    limit: int = 100,
) -> int:
    """Deliver a bounded Paper page through recoverable short transactions."""
    if published_at.tzinfo is None or published_at.utcoffset() != UTC.utcoffset(published_at):
        raise ValueError("published_at must use UTC")
    if not 1 <= limit <= 1_000:
        raise ValueError("limit must be between 1 and 1000")
    if session.bind is None:
        raise RuntimeError("Paper outbox session has no database bind")
    factory = async_sessionmaker(session.bind, expire_on_commit=False)
    registry = _registry(publisher)
    worker_id = f"paper-outbox-{new_uuid7()}"
    delivered = 0
    while delivered < limit:
        worked = await dispatch_outbox_once(
            factory,
            registry=registry,
            worker_id=worker_id,
            clock=lambda: published_at,
            claim_registered_only=True,
            raise_publisher_errors=True,
        )
        if not worked:
            break
        delivered += 1
    return delivered
