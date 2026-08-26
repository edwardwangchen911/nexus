"""Transactional writer for the single global NEXUS audit chain."""

from __future__ import annotations

from datetime import datetime

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import (
    AuditChainInput,
    audit_event_hash,
    canonical_audit_event,
)
from nexus.domain.ids import new_uuid7
from nexus.persistence.models.runtime import AuditEvent

_ZERO_HASH = "0" * 64
_CHAIN_VERSION = "audit_jcs_sha256_v1"


async def append_audit_event(
    session: AsyncSession,
    *,
    actor_id: str,
    action: str,
    schema_version: str,
    payload: dict[str, object],
    occurred_at: datetime,
) -> AuditEvent:
    """Append and flush one event while holding the global transaction lock."""
    await session.execute(
        text(
            "SELECT pg_advisory_xact_lock("
            "hashtextextended('nexus:audit-chain', 2020))"
        )
    )
    previous = await session.scalar(
        select(AuditEvent).order_by(AuditEvent.event_seq.desc()).limit(1)
    )
    event_seq = 1 if previous is None else previous.event_seq + 1
    previous_hash = _ZERO_HASH if previous is None else previous.event_hash
    chain_input = AuditChainInput(
        event_seq=event_seq,
        occurred_at=occurred_at,
        actor_id=actor_id,
        action=action,
        schema_version=schema_version,
        payload=payload,
    )
    canonical_event = canonical_audit_event(chain_input)
    event = AuditEvent(
        id=new_uuid7(),
        actor_id=actor_id,
        action=action,
        schema_version=schema_version,
        payload=payload,
        event_seq=event_seq,
        occurred_at=occurred_at,
        previous_hash=previous_hash,
        canonical_event=canonical_event,
        event_hash=audit_event_hash(previous_hash, canonical_event),
        chain_version=_CHAIN_VERSION,
    )
    session.add(event)
    await session.flush()
    return event
