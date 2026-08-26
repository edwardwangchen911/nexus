"""Recoverable at-least-once outbox delivery contracts."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.outbox.dispatcher import dispatch_outbox_once
from nexus.outbox.models import OutboxMessage, PublisherSpec
from nexus.outbox.registry import PublisherRegistry
from nexus.persistence.models.runtime import OutboxDeliveryAttempt, OutboxEvent
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

_NOW = datetime(2026, 8, 27, 3, 0, tzinfo=UTC)
_AGGREGATE_ID = UUID("0198d500-1000-7000-8000-000000002201")


async def _seed(
    factory: async_sessionmaker[AsyncSession],
    *,
    event_type: str = "job.test",
    schema_version: str = "outbox_test_v1",
    max_attempts: int = 3,
) -> UUID:
    async with factory() as session, session.begin():
        event = OutboxEvent(
            event_type=event_type,
            aggregate_id=_AGGREGATE_ID,
            schema_version=schema_version,
            payload={"outcome": "ready"},
            published_at=None,
            max_attempts=max_attempts,
            available_at=_NOW,
        )
        session.add(event)
        await session.flush()
    return event.id


def _registry(publisher, *, event_type: str = "job.test") -> PublisherRegistry:
    return PublisherRegistry(
        (
            PublisherSpec(
                event_type=event_type,
                schema_version="outbox_test_v1",
                consumer_idempotent=True,
                publisher=publisher,
            ),
        )
    )


def test_registry_rejects_duplicate_or_non_idempotent_publisher() -> None:
    async def publisher(message: OutboxMessage) -> None:
        del message

    with pytest.raises(ValueError, match="idempotent"):
        PublisherSpec(
            event_type="job.test",
            schema_version="outbox_test_v1",
            consumer_idempotent=False,
            publisher=publisher,
        )
    registry = _registry(publisher)
    with pytest.raises(ValueError, match="duplicate"):
        registry.register(_registry(publisher).resolve("job.test", "outbox_test_v1"))


@pytest.mark.asyncio
async def test_concurrent_dispatchers_never_publish_one_claim_twice(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    event_id = await _seed(factory)
    publishing = asyncio.Event()
    release = asyncio.Event()
    seen: list[UUID] = []

    async def publisher(message: OutboxMessage) -> None:
        seen.append(message.event_id)
        publishing.set()
        await release.wait()

    first = asyncio.create_task(
        dispatch_outbox_once(
            factory,
            registry=_registry(publisher),
            worker_id="outbox-one",
            clock=lambda: _NOW + timedelta(seconds=1),
        )
    )
    await asyncio.wait_for(publishing.wait(), timeout=2)
    second = await dispatch_outbox_once(
        factory,
        registry=_registry(publisher),
        worker_id="outbox-two",
        clock=lambda: _NOW + timedelta(seconds=1),
    )
    release.set()
    assert await first is True
    assert second is False
    assert seen == [event_id]


@pytest.mark.asyncio
async def test_success_before_confirmation_redelivers_same_event_id_after_reclaim(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    event_id = await _seed(factory)
    seen: list[UUID] = []

    async def publisher(message: OutboxMessage) -> None:
        seen.append(message.event_id)

    async def crash_after_publish(message: OutboxMessage) -> None:
        assert message.event_id == event_id
        raise RuntimeError("injected crash before confirmation")

    with pytest.raises(RuntimeError, match="injected crash"):
        await dispatch_outbox_once(
            factory,
            registry=_registry(publisher),
            worker_id="outbox-crash",
            clock=lambda: _NOW + timedelta(seconds=1),
            after_publish=crash_after_publish,
        )
    async with factory() as session:
        unconfirmed = await session.get(OutboxEvent, event_id)
    assert unconfirmed is not None and unconfirmed.status == "delivering"
    assert await dispatch_outbox_once(
        factory,
        registry=_registry(publisher),
        worker_id="outbox-recover",
        clock=lambda: _NOW + timedelta(seconds=62),
    )
    async with factory() as session:
        event = await session.get(OutboxEvent, event_id)
        attempts = tuple(
            await session.scalars(
                select(OutboxDeliveryAttempt)
                .where(OutboxDeliveryAttempt.outbox_event_id == event_id)
                .order_by(OutboxDeliveryAttempt.attempt_no)
            )
        )
    assert seen == [event_id, event_id]
    assert event is not None and event.status == "published"
    assert [(item.attempt_no, item.outcome) for item in attempts] == [
        (1, "lost"),
        (2, "published"),
    ]


@pytest.mark.asyncio
async def test_retry_uses_deterministic_backoff_then_dead_letters_at_limit(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    event_id = await _seed(factory, max_attempts=2)

    async def unavailable(message: OutboxMessage) -> None:
        del message
        raise RuntimeError("private publisher detail")

    assert await dispatch_outbox_once(
        factory,
        registry=_registry(unavailable),
        worker_id="outbox-fail-one",
        clock=lambda: _NOW + timedelta(seconds=1),
    )
    async with factory() as session:
        first = await session.get(OutboxEvent, event_id)
    assert first is not None
    assert (first.status, first.available_at, first.last_error_code) == (
        "queued",
        _NOW + timedelta(seconds=3),
        "PUBLISHER_FAILED",
    )
    assert not await dispatch_outbox_once(
        factory,
        registry=_registry(unavailable),
        worker_id="outbox-too-early",
        clock=lambda: _NOW + timedelta(seconds=2),
    )
    assert await dispatch_outbox_once(
        factory,
        registry=_registry(unavailable),
        worker_id="outbox-fail-two",
        clock=lambda: _NOW + timedelta(seconds=3),
    )
    async with factory() as session:
        exhausted = await session.get(OutboxEvent, event_id)
    assert exhausted is not None
    assert (exhausted.status, exhausted.attempt_count, exhausted.last_error_code) == (
        "dead_letter",
        2,
        "PUBLISHER_FAILED",
    )


@pytest.mark.asyncio
async def test_unknown_schema_dead_letters_without_calling_any_publisher(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    event_id = await _seed(factory, schema_version="unknown_v9")
    called = False

    async def publisher(message: OutboxMessage) -> None:
        nonlocal called
        del message
        called = True

    assert await dispatch_outbox_once(
        factory,
        registry=_registry(publisher),
        worker_id="outbox-unknown",
        clock=lambda: _NOW + timedelta(seconds=1),
    )
    async with factory() as session:
        event = await session.get(OutboxEvent, event_id)
    assert called is False
    assert event is not None
    assert (event.status, event.last_error_code) == (
        "dead_letter",
        "PUBLISHER_UNREGISTERED",
    )
