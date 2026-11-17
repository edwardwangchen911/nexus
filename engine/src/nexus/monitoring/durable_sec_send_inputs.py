"""Revalidate the current SEC originals immediately before a protected new send."""

from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.monitoring.batches import disclosure_source_current
from nexus.monitoring.sec_index_reader import read_monitor_index
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorPlan
from nexus.research.durable_sec_index_storage import SOURCE, read_durable_index_capture
from nexus.research.sec_disclosure_index import SecIndexEntry
from nexus.research.sec_exhibit_storage import configured_exhibit_store

INVALID = "SEC_RESEARCH_PERMISSION_INVALID"

_SCOPES = text("""
WITH frozen AS (
 SELECT s.payload FROM agent_research_runs r JOIN data_snapshots s ON s.id=r.data_snapshot_id
 WHERE r.id=:run
), scopes AS (
 SELECT t.account_id,t.instrument_id,c.plan_id,t.event_key,c.observation->'entry' AS entry
 FROM research_monitor_increments m
 LEFT JOIN research_monitor_batches b ON b.id=m.batch_id
 JOIN research_monitor_triggers t ON t.account_id=m.account_id AND t.instrument_id=m.instrument_id
 JOIN research_monitor_checks c ON c.id=t.check_id
 WHERE m.run_id=:run AND t.kind='disclosure'
 AND (t.id=m.trigger_id OR (t.batch_id=m.batch_id AND b.trigger_ids ? t.id::text))
 UNION
 SELECT t.account_id,t.instrument_id,NULL::uuid,t.event_key,c.observation->'entry'
 FROM frozen f CROSS JOIN LATERAL jsonb_array_elements(CASE
  WHEN jsonb_typeof(f.payload#>'{agent_paper_context,pending_monitoring_v2,trigger_checks}')='array'
  THEN f.payload#>'{agent_paper_context,pending_monitoring_v2,trigger_checks}'
  ELSE '[]'::jsonb END) item
 JOIN research_monitor_triggers t ON t.id::text=item->>'trigger_id'
 JOIN research_monitor_checks c ON c.id=t.check_id
 WHERE item->>'kind'='disclosure'
 UNION
 SELECT e.account_id,e.instrument_id,NULL::uuid,e.event_key,e.entry
 FROM frozen f CROSS JOIN LATERAL jsonb_array_elements(CASE
  WHEN jsonb_typeof(f.payload#>'{agent_paper_context,pending_durable_sec,events}')='array'
  THEN f.payload#>'{agent_paper_context,pending_durable_sec,events}' ELSE '[]'::jsonb END) item
 JOIN sec_discovery_events e ON e.id::text=item->>'event_id'
)
SELECT DISTINCT s.account_id,s.instrument_id,s.plan_id,s.event_key,s.entry FROM scopes s
JOIN sec_monitor_rules r ON r.account_id=s.account_id AND r.instrument_id=s.instrument_id
ORDER BY s.account_id,s.instrument_id,s.plan_id
""")

_LATEST = text("""
SELECT id FROM data_snapshots
WHERE source IN (
 'sec_disclosure_index','sec_disclosure_listed_index','sec_durable_disclosure_index')
AND (provider_object_id LIKE CAST(:instrument AS text)||':%'
 OR payload#>>'{index,instrument_id}'=CAST(:instrument AS text))
AND greatest(created_at,observed_at,available_at,received_at)<=:at
ORDER BY observed_at DESC,available_at DESC,received_at DESC,created_at DESC,id DESC LIMIT 1
""")


async def verify_send_inputs(session: AsyncSession, run_id: UUID) -> None:
    """SQL first proves the exact frozen relations and holds their security locks."""
    scopes = tuple((await session.execute(_SCOPES, {"run": run_id})).mappings())
    instruments = sorted({scope["instrument_id"] for scope in scopes}, key=str)
    # All normal capture publishers share this lock. New source material cannot
    # replace the one whose own originals we verify while the headers fence holds.
    for instrument in instruments:
        if not await session.scalar(
            text("SELECT pg_try_advisory_xact_lock(hashtextextended(:key,0))"),
            {"key": f"sec-index-current:{instrument}"},
        ):
            raise ValueError("SEC_RESEARCH_PERMISSION_BUSY")
    verified: dict[UUID, UUID] = {}
    received: list[datetime] = []
    for instrument in instruments:
        at = await session.scalar(select(text("clock_timestamp()")))
        if not isinstance(at, datetime):
            raise ValueError(INVALID)
        snapshot_id = await session.scalar(_LATEST, {"instrument": str(instrument), "at": at})
        if not isinstance(snapshot_id, UUID):
            raise ValueError(INVALID)
        try:
            store = configured_exhibit_store(session)
            row = await session.get(DataSnapshot, snapshot_id)
            if row is not None and row.source == SOURCE:
                _, capture = await read_durable_index_capture(
                    session, store, snapshot_id=snapshot_id, as_of=at
                )
                index, issues = capture.index, capture.issues
            else:
                _, index = await read_monitor_index(
                    session, store, snapshot_id=snapshot_id, as_of=at
                )
                issues = ()
            received.append(index.received_at)
            # Validate the frozen entries against this very capture, rather than
            # relying on a different capture selected by an earlier SQL reader.
            for scope in scopes:
                if scope["instrument_id"] != instrument:
                    continue
                entry = SecIndexEntry.model_validate(scope["entry"])
                if (
                    index.instrument_id != instrument
                    or scope["event_key"] != f"sec:{index.issuer_cik}:{entry.accession_number}"
                    or tuple(
                        e.model_dump(mode="json")
                        for e in index.entries
                        if e.accession_number == entry.accession_number
                    )
                    != (entry.model_dump(mode="json"),)
                    or any(i.accession_number == entry.accession_number for i in issues)
                ):
                    raise ValueError(INVALID)
        except ValueError:
            raise ValueError(INVALID) from None
        verified[instrument] = snapshot_id
    # Original reads are asynchronous: take a fresh clock and recheck permissions
    # and freshness afterwards, without replaying a changed or failed read.
    await session.execute(
        text("SELECT public.sec_research_require_send_run(:run)"), {"run": run_id}
    )
    at = await session.scalar(select(text("clock_timestamp()")))
    if not isinstance(at, datetime):
        raise ValueError(INVALID)
    for instrument, snapshot_id in verified.items():
        if await session.scalar(_LATEST, {"instrument": str(instrument), "at": at}) != snapshot_id:
            raise ValueError(INVALID)
    plans: list[MonitorPlan] = []
    for scope in scopes:
        if scope["plan_id"] is not None:
            plan = await session.get(MonitorPlan, scope["plan_id"])
            if plan is None or not await disclosure_source_current(
                session, plan, at, exclude_run_id=run_id
            ):
                raise ValueError(INVALID)
            plans.append(plan)
    # The last source query can itself wait. Nothing asynchronous follows this
    # final clock check before the transport also checks its work deadline.
    at = await session.scalar(select(text("clock_timestamp()")))
    if (
        not isinstance(at, datetime)
        or any(not timedelta(0) <= at - value <= timedelta(seconds=300) for value in received)
        or any(not plan.effective_at <= at < plan.valid_until for plan in plans)
    ):
        raise ValueError(INVALID)
