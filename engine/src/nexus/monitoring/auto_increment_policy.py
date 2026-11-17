"""Register a session limit scope, independently of SEC discovery and paid authority."""

import json
from hashlib import sha256
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.paper.candidate_batch import load_agent_paper_candidate_batch
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.models.data import DataSnapshot

SOURCE = "nexus_auto_increment_policy"
SCHEMA = "auto_increment_scope_v1"


async def enable_scope(
    session: AsyncSession, *, account_id: UUID, candidate_batch_id: UUID
) -> DataSnapshot:
    await lock_portfolio_account(session, account_id)
    candidate = await load_agent_paper_candidate_batch(session, batch_id=candidate_batch_id)
    at = await session.scalar(text("SELECT pg_catalog.clock_timestamp()"))
    if candidate is None or candidate.account_id != account_id or candidate.created_at > at:
        raise ValueError("AUTO_INCREMENT_SCOPE_INVALID")
    old = await session.scalar(
        select(DataSnapshot).where(
            DataSnapshot.source == SOURCE, DataSnapshot.provider_object_id == str(account_id)
        )
    )
    if old is not None:
        if old.payload.get("candidate_batch_id") != str(candidate_batch_id):
            raise ValueError("AUTO_INCREMENT_SCOPE_CONFLICT")
        await session.scalar(
            text("SELECT sec_auto_increment_scoped(:account,:instrument)"),
            {"account": account_id, "instrument": candidate.instrument_ids[0]},
        )
        return old
    payload = dict(
        schema_version=SCHEMA,
        account_id=str(account_id),
        candidate_batch_id=str(candidate_batch_id),
        candidate_hash=candidate.batch_hash,
        instrument_ids=[str(v) for v in candidate.instrument_ids],
        effective_at=at.isoformat(),
        max_per_session=1,
        session_timezone="America/New_York",
    )
    digest = sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    row = DataSnapshot(
        id=new_uuid7(),
        created_at=at,
        source=SOURCE,
        provider_object_id=str(account_id),
        schema_version=SCHEMA,
        revision=digest,
        observed_at=at,
        available_at=at,
        received_at=at,
        payload=payload,
    )
    session.add(row)
    await session.flush()
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.auto_increment_scope",
        schema_version=SCHEMA,
        occurred_at=at,
        payload={"snapshot_id": str(row.id), "scope_hash": digest},
    )
    return row
