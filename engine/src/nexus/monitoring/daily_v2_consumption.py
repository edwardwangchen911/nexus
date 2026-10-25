"""Append new daily proof without changing the original event or research ownership."""

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import exists, select, text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from nexus.monitoring.mixed_contracts import canonical_hash
from nexus.persistence.models.monitoring import MonitorTrigger

if TYPE_CHECKING:
    from nexus.monitoring.source_qualification import QualifiedSource


def not_daily_event_consumed() -> ColumnElement[bool]:
    from nexus.persistence.models.monitoring import MonitorDailyEventReceipt

    # Presence fences new work even if a damaged proof is later rejected by readback.
    return ~exists(
        select(MonitorDailyEventReceipt.id).where(
            MonitorDailyEventReceipt.trigger_id == MonitorTrigger.id
        )
    )


async def validate_daily_event_receipt(session: AsyncSession, receipt_id: UUID) -> bool:
    try:
        async with session.begin_nested():
            await session.execute(
                text("SELECT nexus_validate_daily_event_receipt(:id)"), {"id": receipt_id}
            )
        return True
    except DBAPIError:
        return False


async def record_daily_event_consumption(
    session: AsyncSession, *, qualified: "QualifiedSource", account_id: UUID, now: datetime
) -> int:
    from nexus.agents.paper_context import AgentPaperResearchContext
    from nexus.audit.service import append_audit_event
    from nexus.monitoring.daily_contracts import compile_daily_pending_proposal
    from nexus.persistence.models.monitoring import (
        MonitorDailyEventReceipt,
        MonitorDailyTriggerReceipt,
        MonitorMixedEventReceipt,
    )

    paper = qualified.context
    if not isinstance(paper, AgentPaperResearchContext) or paper.pending_monitoring_v2 is None:
        return 0
    context = paper.pending_monitoring_v2
    proposal = qualified.intent.daily_pending_proposal
    if proposal is None:
        return 0
    roles = {row.role: row for row in qualified.role_outputs}
    if roles["orchestrator"].daily_pending_proposal != proposal:
        raise ValueError("MONITOR_DAILY_EVENT_ADOPTION_INVALID")
    reviews = {
        str(role): row.daily_pending_review
        for role, row in roles.items()
        if role in {"bear", "evidence_audit"} and row.daily_pending_review is not None
    }
    adopted = compile_daily_pending_proposal(
        proposal,
        reviews,
        context_hash=context.content_hash,
        trigger_ids=tuple(row.trigger_id for row in context.trigger_checks),
        evidence_refs=qualified.evidence,
        audit_vote=roles["evidence_audit"].vote,
        adoption_hash=roles["portfolio"].daily_pending_adoption_hash,
    )
    review_hash = canonical_hash({key: row.model_dump(mode="json") for key, row in reviews.items()})
    members = {row.trigger_id: row for row in context.trigger_checks}
    recorded = 0
    for item in sorted(adopted.items, key=lambda row: str(row.trigger_id)):
        if item.status != "addressed":
            continue
        original = members[item.trigger_id]
        trigger = await session.get(MonitorTrigger, item.trigger_id, with_for_update=True)
        if (
            trigger is None
            or trigger.account_id != account_id
            or trigger.instrument_id != qualified.run.instrument_id
            or trigger.research_run_id != original.original_run_id
            or trigger.batch_id != original.batch_id
            or trigger.kind != original.kind
            or trigger.event_key != original.event_key
        ):
            raise ValueError("MONITOR_DAILY_EVENT_BINDING_INVALID")
        prior = await session.scalar(
            select(MonitorDailyEventReceipt).where(
                MonitorDailyEventReceipt.trigger_id == trigger.id
            )
        )
        if prior is not None:
            if (
                prior.run_id != qualified.run.id
                or prior.output_id != qualified.output.id
                or prior.context_hash != context.content_hash
                or prior.proposal_hash != adopted.content_hash
                or not await validate_daily_event_receipt(session, prior.id)
            ):
                raise ValueError("MONITOR_DAILY_EVENT_ALREADY_COVERED")
            continue
        for model in (MonitorDailyTriggerReceipt, MonitorMixedEventReceipt):
            if await session.scalar(select(model.id).where(model.trigger_id == trigger.id)):
                raise ValueError("MONITOR_DAILY_EVENT_ALREADY_COVERED")
        receipt = MonitorDailyEventReceipt(
            account_id=account_id,
            instrument_id=trigger.instrument_id,
            trigger_id=trigger.id,
            kind=original.kind,
            event_key=original.event_key,
            original_run_id=original.original_run_id,
            original_output_id=original.original_output_id,
            batch_id=original.batch_id,
            materials_snapshot_id=original.materials_snapshot_id,
            cycle_id=paper.cycle_id,
            run_id=qualified.run.id,
            output_id=qualified.output.id,
            snapshot_id=qualified.run.data_snapshot_id,
            context_hash=context.content_hash,
            proposal_hash=adopted.content_hash,
            review_hash=review_hash,
            observed_at=now,
        )
        session.add(receipt)
        await session.flush()
        await append_audit_event(
            session,
            actor_id="research-monitor",
            action="research.monitor.daily_event_covered",
            schema_version="daily_monitor_consumption_v2",
            occurred_at=now,
            payload={
                "receipt_id": str(receipt.id),
                "trigger_id": str(trigger.id),
                "run_id": str(qualified.run.id),
                "original_run_id": None
                if original.original_run_id is None
                else str(original.original_run_id),
                "context_hash": context.content_hash,
                "proposal_hash": adopted.content_hash,
            },
        )
        recorded += 1
    return recorded
