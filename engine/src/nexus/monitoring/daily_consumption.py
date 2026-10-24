"""Append proof of reviewed daily consumption; never rewrite original triggers."""

from datetime import datetime
from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from nexus.agents.committee import AgentRole
from nexus.agents.paper_context import AgentPaperResearchContext
from nexus.monitoring.daily_contracts import DailyPendingReview
from nexus.persistence.models.monitoring import MonitorDailyTriggerReceipt, MonitorTrigger

if TYPE_CHECKING:
    from nexus.monitoring.source_qualification import QualifiedSource


def not_daily_covered() -> ColumnElement[bool]:
    return ~exists(
        select(MonitorDailyTriggerReceipt.id).where(
            MonitorDailyTriggerReceipt.trigger_id == MonitorTrigger.id
        )
    )


async def daily_receipt(
    session: AsyncSession, trigger_id: UUID
) -> MonitorDailyTriggerReceipt | None:
    row: MonitorDailyTriggerReceipt | None = await session.scalar(
        select(MonitorDailyTriggerReceipt).where(
            MonitorDailyTriggerReceipt.trigger_id == trigger_id
        )
    )
    return row


async def record_daily_consumption(
    session: AsyncSession, *, qualified: "QualifiedSource", account_id: UUID, now: datetime
) -> int:
    from nexus.audit.service import append_audit_event
    from nexus.monitoring.daily_contracts import compile_daily_pending_proposal

    context = qualified.context
    if not isinstance(context, AgentPaperResearchContext) or context.pending_monitoring is None:
        return 0
    pending = context.pending_monitoring
    proposal = qualified.intent.daily_pending_proposal
    if proposal is None:
        return 0
    by_role = {output.role: output for output in qualified.role_outputs}
    origin = by_role["orchestrator"].daily_pending_proposal
    if origin is None or origin != proposal or pending.account_id != account_id:
        raise ValueError("DAILY_PENDING_ADOPTION_INVALID")
    reviewers: tuple[AgentRole, ...] = ("bear", "evidence_audit")
    reviews: dict[str, DailyPendingReview] = {
        role: review
        for role in reviewers
        if (review := by_role[role].daily_pending_review) is not None
    }
    adopted = compile_daily_pending_proposal(
        proposal,
        reviews,
        context_hash=pending.content_hash,
        trigger_ids=tuple(item.trigger_id for item in pending.trigger_checks),
        evidence_refs=qualified.evidence,
        audit_vote=by_role["evidence_audit"].vote,
        adoption_hash=by_role["portfolio"].daily_pending_adoption_hash,
    )
    recorded = 0
    for item in adopted.items:
        if item.status != "addressed":
            continue
        if await daily_receipt(session, item.trigger_id) is not None:
            continue
        trigger = await session.get(MonitorTrigger, item.trigger_id, with_for_update=True)
        if (
            trigger is None
            or trigger.kind != "price"
            or trigger.account_id != account_id
            or trigger.instrument_id != qualified.run.instrument_id
            or trigger.research_run_id is not None
            or trigger.status not in {"pending", "blocked"}
        ):
            raise ValueError("DAILY_MONITOR_TRIGGER_OCCUPIED")
        receipt = MonitorDailyTriggerReceipt(
            trigger_id=trigger.id,
            account_id=account_id,
            instrument_id=trigger.instrument_id,
            cycle_id=context.cycle_id,
            run_id=qualified.run.id,
            output_id=qualified.output.id,
            snapshot_id=qualified.run.data_snapshot_id,
            context_hash=pending.content_hash,
            review_bundle_hash=adopted.content_hash,
            observed_at=now,
        )
        session.add(receipt)
        await session.flush()
        await append_audit_event(
            session,
            actor_id="research-monitor",
            action="research.monitor.daily_trigger_covered",
            schema_version="daily_monitor_coverage_v1",
            occurred_at=now,
            payload={
                "receipt_id": str(receipt.id),
                "trigger_id": str(trigger.id),
                "run_id": str(qualified.run.id),
                "context_hash": pending.content_hash,
                "review_bundle_hash": adopted.content_hash,
            },
        )
        recorded += 1
    return recorded
