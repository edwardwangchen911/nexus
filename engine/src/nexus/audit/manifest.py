"""Content-addressed, redacted snapshots of contiguous audit-chain segments."""

from __future__ import annotations

import hashlib
from datetime import date
from typing import Protocol

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.export import render_redacted_audit_gzip
from nexus.domain.ids import new_uuid7
from nexus.persistence.models.runtime import AuditChainManifest, AuditEvent


class NoAuditEventsError(RuntimeError):
    """No new chain segment exists for the requested daily manifest."""


class AuditManifestObjectStore(Protocol):
    """Minimal idempotent content-addressed object-store boundary."""

    async def put_if_absent(self, *, key: str, content: bytes) -> str: ...


async def publish_audit_chain_manifest(
    session: AsyncSession,
    *,
    session_date: date,
    object_store: AuditManifestObjectStore,
) -> AuditChainManifest:
    """Publish exactly the next contiguous redacted chain segment once."""
    if type(session_date) is not date:
        raise ValueError("manifest session must be a date")
    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended('nexus:audit-chain', 2020))"
        )
    )
    existing = await session.scalar(
        select(AuditChainManifest).where(
            AuditChainManifest.session == session_date
        )
    )
    if existing is not None:
        return existing
    previous = await session.scalar(
        select(AuditChainManifest)
        .order_by(AuditChainManifest.last_event_seq.desc())
        .limit(1)
    )
    if previous is not None and session_date <= previous.session:
        raise ValueError("manifest sessions must increase monotonically")
    first_event_seq = 1 if previous is None else previous.last_event_seq + 1
    head = await session.scalar(
        select(AuditEvent).order_by(AuditEvent.event_seq.desc()).limit(1)
    )
    if head is None or head.event_seq < first_event_seq:
        raise NoAuditEventsError("no new audit events are available")
    events = tuple(
        (
            await session.scalars(
                select(AuditEvent)
                .where(
                    AuditEvent.event_seq >= first_event_seq,
                    AuditEvent.event_seq <= head.event_seq,
                )
                .order_by(AuditEvent.event_seq)
            )
        ).all()
    )
    expected_sequences = tuple(range(first_event_seq, head.event_seq + 1))
    if tuple(event.event_seq for event in events) != expected_sequences:
        raise RuntimeError("audit manifest chain segment is not contiguous")
    content = render_redacted_audit_gzip(events)
    object_sha256 = hashlib.sha256(content).hexdigest()
    key = (
        f"audit/manifests/{session_date.isoformat()}/"
        f"{object_sha256}.ndjson.gz"
    )
    object_uri = await object_store.put_if_absent(key=key, content=content)
    if (
        not 1 <= len(object_uri) <= 2_048
        or not object_uri.endswith(key)
        or "\x00" in object_uri
    ):
        raise ValueError("audit manifest object URI is not content addressed")
    manifest = AuditChainManifest(
        id=new_uuid7(),
        session=session_date,
        first_event_seq=first_event_seq,
        last_event_seq=head.event_seq,
        head_hash=head.event_hash,
        object_uri=object_uri,
        object_sha256=object_sha256,
        byte_size=len(content),
    )
    session.add(manifest)
    await session.flush()
    return manifest
