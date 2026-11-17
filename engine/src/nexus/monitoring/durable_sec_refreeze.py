"""Supersede only unsubmitted batches; original checks, materials and notices stay frozen."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import exists, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.persistence.models.monitoring import MonitorIncrement, MonitorTrigger
from nexus.persistence.models.sec_refreeze import SecResearchBatchSupersession

SUPERSEDED = "DURABLE_SEC_RESEARCH_BATCH_SUPERSEDED"
UNSAFE = "DURABLE_SEC_RESEARCH_REFREEZE_UNSAFE"
ACTION = "research.monitor.sec_research_batch_superseded"
SCHEMA = "sec_research_refreeze_v1"


def not_superseded_pending() -> ColumnElement[bool]:
    """Queue filtering only; never filter original submitted or unknown ownership."""
    return or_(
        MonitorTrigger.batch_id.is_(None),
        ~exists(
            select(SecResearchBatchSupersession.id).where(
                SecResearchBatchSupersession.old_batch_id == MonitorTrigger.batch_id
            )
        ),
    )


async def require_current_batch(session: AsyncSession, batch_id: UUID) -> None:
    if (
        await session.scalar(
            select(SecResearchBatchSupersession.id).where(
                SecResearchBatchSupersession.old_batch_id == batch_id
            )
        )
        is not None
    ):
        raise ValueError(SUPERSEDED)


async def batch_unsubmitted(session: AsyncSession, batch_id: UUID) -> bool:
    if (
        await session.scalar(
            select(MonitorIncrement.id).where(MonitorIncrement.batch_id == batch_id)
        )
        is not None
    ):
        return False
    triggers = tuple(
        await session.scalars(select(MonitorTrigger).where(MonitorTrigger.batch_id == batch_id))
    )
    return bool(triggers) and all(
        t.status in {"pending", "blocked"} and t.research_run_id is None for t in triggers
    )


async def append_supersession(
    session: AsyncSession, *, old_batch_id: UUID, new_batch_id: UUID, now: datetime
) -> SecResearchBatchSupersession:
    if not await batch_unsubmitted(session, old_batch_id):
        raise ValueError(UNSAFE)
    row = SecResearchBatchSupersession(
        id=new_uuid7(),
        created_at=now,
        old_batch_id=old_batch_id,
        new_batch_id=new_batch_id,
        reason="current_evidence_refreeze",
    )
    session.add(row)
    await session.flush()
    binding = await session.scalar(
        text(
            "SELECT to_jsonb(x)-'created_at' FROM sec_research_batch_supersessions x WHERE id=:id"
        ),
        {"id": row.id},
    )
    if not isinstance(binding, dict):
        raise ValueError(UNSAFE)
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action=ACTION,
        schema_version=SCHEMA,
        occurred_at=now,
        payload={"row_id": str(row.id), "binding": binding},
    )
    return row
