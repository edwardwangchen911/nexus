"""Persist only conflicts established by the caller's verified current raw capture."""

from datetime import datetime
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.persistence.models.sec_conflicts import SecDisclosureKeyConflict
from nexus.persistence.models.sec_monitor import SecMonitorRule


async def record_key_conflict(
    session: AsyncSession,
    *,
    rule: SecMonitorRule,
    capture_id: UUID,
    event_key: str,
    original_event_id: UUID | None,
    legacy_trigger_id: UUID | None,
    entry: dict[str, object],
    metadata_hash: str,
    now: datetime,
) -> None:
    existing = await session.scalar(
        select(SecDisclosureKeyConflict.id).where(
            SecDisclosureKeyConflict.account_id == rule.account_id,
            SecDisclosureKeyConflict.instrument_id == rule.instrument_id,
            SecDisclosureKeyConflict.event_key == event_key,
            SecDisclosureKeyConflict.metadata_hash == metadata_hash,
        )
    )
    if existing is not None:
        return
    row = SecDisclosureKeyConflict(
        id=new_uuid7(),
        created_at=now,
        account_id=rule.account_id,
        instrument_id=rule.instrument_id,
        event_key=event_key,
        rule_id=rule.id,
        capture_id=capture_id,
        original_event_id=original_event_id,
        legacy_trigger_id=legacy_trigger_id,
        entry=entry,
        metadata_hash=metadata_hash,
        observed_at=now,
    )
    session.add(row)
    await session.flush()
    binding = await session.scalar(
        text("SELECT to_jsonb(c)-'created_at' FROM sec_disclosure_key_conflicts c WHERE id=:id"),
        {"id": row.id},
    )
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.sec_key_conflict",
        schema_version="sec_key_conflict_v1",
        payload={"table": row.__tablename__, "row_id": str(row.id), "binding": binding},
        occurred_at=now,
    )
