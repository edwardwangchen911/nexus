"""Transactional audit-chain writer integration contracts."""

import asyncio
from datetime import UTC, datetime

import pytest
from nexus.audit.canonical import AuditChainInput, audit_event_hash, canonical_audit_event
from nexus.audit.service import append_audit_event
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select


@pytest.mark.asyncio
async def test_concurrent_appends_form_one_verified_global_chain(
    migrated_database_url: str,
) -> None:
    factory = session_factory(
        EngineSettings(NEXUS_ENV="test", DATABASE_URL=migrated_database_url)
    )

    async def append(actor_id: str, action: str, second: int) -> None:
        async with factory() as session, session.begin():
            await append_audit_event(
                session,
                actor_id=actor_id,
                action=action,
                schema_version="audit_test_v1",
                payload={"outcome": "ok", "second": second},
                occurred_at=datetime(2026, 8, 27, 1, 0, second, tzinfo=UTC),
            )

    await asyncio.gather(
        append("system:first", "audit.first", 1),
        append("system:second", "audit.second", 2),
    )

    async with factory() as session:
        events = tuple(
            await session.scalars(select(AuditEvent).order_by(AuditEvent.event_seq))
        )

    assert [event.event_seq for event in events] == [1, 2]
    previous_hash = "0" * 64
    for event in events:
        expected_canonical = canonical_audit_event(
            AuditChainInput(
                event_seq=event.event_seq,
                occurred_at=event.occurred_at,
                actor_id=event.actor_id,
                action=event.action,
                schema_version=event.schema_version,
                payload=event.payload,
            )
        )
        assert event.previous_hash == previous_hash
        assert event.canonical_event == expected_canonical
        assert event.event_hash == audit_event_hash(previous_hash, expected_canonical)
        previous_hash = event.event_hash
