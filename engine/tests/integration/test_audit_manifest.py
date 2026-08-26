"""Content-addressed daily audit manifest contracts."""

import asyncio
import gzip
import hashlib
from datetime import UTC, date, datetime, timedelta

import pytest
from nexus.audit.manifest import (
    AuditManifestObjectStore,
    NoAuditEventsError,
    publish_audit_chain_manifest,
)
from nexus.audit.service import append_audit_event
from nexus.persistence.models.runtime import AuditChainManifest
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

_NOW = datetime(2026, 8, 27, 22, 0, tzinfo=UTC)


class _MemoryStore(AuditManifestObjectStore):
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}
        self.put_count = 0

    async def put_if_absent(self, *, key: str, content: bytes) -> str:
        self.put_count += 1
        existing = self.objects.setdefault(key, content)
        if existing != content:
            raise AssertionError("content-address collision")
        return f"memory://audit-fixture/{key}"


async def _append(database_url: str, *, second: int, secret: str) -> None:
    factory = session_factory(EngineSettings(database_url=database_url))
    async with factory() as session, session.begin():
        await append_audit_event(
            session,
            actor_id=f"system:private-{secret}",
            action="audit.manifest.test",
            schema_version="audit_test_v1",
            payload={"status": "ok", "account_id": secret},
            occurred_at=_NOW + timedelta(seconds=second),
        )


@pytest.mark.asyncio
async def test_manifest_is_content_addressed_redacted_contiguous_and_idempotent(
    migrated_database_url: str,
) -> None:
    await _append(migrated_database_url, second=1, secret="account-one")
    await _append(migrated_database_url, second=2, secret="account-two")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = _MemoryStore()
    async with factory() as session, session.begin():
        first = await publish_audit_chain_manifest(
            session,
            session_date=date(2026, 8, 27),
            object_store=store,
        )
    async with factory() as session, session.begin():
        replay = await publish_audit_chain_manifest(
            session,
            session_date=date(2026, 8, 27),
            object_store=store,
        )
    assert replay.id == first.id
    assert (first.first_event_seq, first.last_event_seq) == (1, 2)
    assert store.put_count == 1
    assert len(store.objects) == 1
    key, content = next(iter(store.objects.items()))
    assert key.endswith(f"/{first.object_sha256}.ndjson.gz")
    assert hashlib.sha256(content).hexdigest() == first.object_sha256
    assert len(content) == first.byte_size
    decoded = gzip.decompress(content).decode("utf-8")
    assert "account-one" not in decoded
    assert "account-two" not in decoded

    await _append(migrated_database_url, second=3, secret="account-three")
    async with factory() as session, session.begin():
        second = await publish_audit_chain_manifest(
            session,
            session_date=date(2026, 8, 28),
            object_store=store,
        )
    assert (second.first_event_seq, second.last_event_seq) == (3, 3)
    assert second.head_hash != first.head_hash


@pytest.mark.asyncio
async def test_concurrent_manifest_publish_converges_and_rows_are_immutable(
    migrated_database_url: str,
) -> None:
    await _append(migrated_database_url, second=1, secret="account-private")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = _MemoryStore()

    async def publish() -> AuditChainManifest:
        async with factory() as session, session.begin():
            return await publish_audit_chain_manifest(
                session,
                session_date=date(2026, 8, 27),
                object_store=store,
            )

    first, second = await asyncio.gather(publish(), publish())
    assert first.id == second.id
    assert store.put_count == 1
    with pytest.raises(DBAPIError, match="runtime authority fact is immutable"):
        async with factory() as session, session.begin():
            await session.execute(
                text(
                    "UPDATE audit_chain_manifests SET byte_size = byte_size + 1 "
                    "WHERE id = :id"
                ),
                {"id": first.id},
            )
    async with factory() as session:
        rows = tuple(await session.scalars(select(AuditChainManifest)))
    assert len(rows) == 1


@pytest.mark.asyncio
async def test_manifest_without_new_events_fails_closed(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    with pytest.raises(NoAuditEventsError):
        async with factory() as session, session.begin():
            await publish_audit_chain_manifest(
                session,
                session_date=date(2026, 8, 27),
                object_store=_MemoryStore(),
            )
