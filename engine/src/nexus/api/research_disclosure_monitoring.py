"""Saved SEC coverage and per-event review, without collecting or admitting work."""

from datetime import datetime
from typing import Literal
from uuid import UUID

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.research_monitoring import _REASONS
from nexus.api.schemas.research_monitoring import (
    ResearchDisclosureMonitorCheck,
    ResearchDisclosureMonitoringView,
    ResearchDisclosureMonitorTrigger,
    UsageStatus,
)
from nexus.domain.time import require_utc
from nexus.monitoring.batches import disclosure_source_current
from nexus.monitoring.contracts import DisclosureReviewCondition
from nexus.monitoring.daily_v2_consumption import validate_daily_event_receipt
from nexus.monitoring.disclosure_contracts import DisclosureMonitoringProposal
from nexus.monitoring.mixed_context import MixedMonitorContext
from nexus.monitoring.mixed_contracts import canonical_hash, compile_mixed_proposal
from nexus.monitoring.source_qualification import QualifiedSource, load_qualified_source
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import (
    MonitorCheck,
    MonitorCoverageReceipt,
    MonitorDailyEventReceipt,
    MonitorIncrement,
    MonitorMixedEventReceipt,
    MonitorObservationBatch,
    MonitorPlan,
    MonitorTrigger,
)
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, OutboxEvent

type EventReview = Literal["addressed", "unresolved", "unverified", "not_performed"]


async def _usage(
    session: AsyncSession, run: AgentResearchRunRecord | None, now: datetime
) -> UsageStatus:
    if run is None or run.created_at > now or run.status == "prepared":
        return "not_started"
    if run.status in {"completed", "failed", "abstained"}:
        job = await session.scalar(select(Job).where(Job.payload == {"run_id": str(run.id)}))
        if job is None or job.finished_at is None or job.finished_at > now:
            return "unknown"
    if (
        run.settled_cost_usd is not None
        and run.input_tokens is not None
        and run.output_tokens is not None
    ):
        return "known"
    return "unknown" if run.status in {"completed", "failed", "abstained"} else "in_progress"


async def _account(
    session: AsyncSession, run: AgentResearchRunRecord, now: datetime
) -> UUID | None:
    snapshot = await session.get(DataSnapshot, run.data_snapshot_id)
    if (
        snapshot is None
        or max(
            snapshot.created_at, snapshot.observed_at, snapshot.available_at, snapshot.received_at
        )
        > now
    ):
        return None
    member = await session.scalar(
        select(MonitorIncrement).where(
            MonitorIncrement.run_id == run.id,
            MonitorIncrement.instrument_id == run.instrument_id,
            MonitorIncrement.snapshot_id == run.data_snapshot_id,
            MonitorIncrement.created_at <= now,
        )
    )
    if member is not None:
        return member.account_id
    from nexus.monitoring.independent_context import is_independent_snapshot
    from nexus.monitoring.independent_storage import require_independent_run

    if is_independent_snapshot(snapshot):
        try:
            return (await require_independent_run(session, run)).account_id
        except ValueError:
            return None
    paper = snapshot.payload.get("agent_paper_context")
    if not isinstance(paper, dict) or paper.get("instrument_id") != str(run.instrument_id):
        return None
    try:
        cycle_id = UUID(str(paper["cycle_id"]))
    except (KeyError, ValueError):
        return None
    cycle = await session.get(AgentPaperCycleRecord, cycle_id)
    return (
        None
        if cycle is None or cycle.created_at > now or cycle.decision_time != run.decision_time
        else cycle.account_id
    )


async def _mixed_review(
    session: AsyncSession,
    trigger: MonitorTrigger,
    child: AgentResearchRunRecord | None,
    proof: MonitorMixedEventReceipt | None,
    now: datetime,
    cache: dict[tuple[UUID, UUID], QualifiedSource],
) -> EventReview:
    if child is None or child.created_at > now or child.status in {"prepared", "running"}:
        return "unverified" if proof is not None else "not_performed"
    if child.status != "completed":
        return "unverified"
    try:
        output = await session.scalar(
            select(AgentResearchOutputRecord).where(
                AgentResearchOutputRecord.run_id == child.id,
                AgentResearchOutputRecord.role.is_(None),
                AgentResearchOutputRecord.status == "completed",
                AgentResearchOutputRecord.created_at <= now,
            )
        )
        job = await session.scalar(select(Job).where(Job.payload == {"run_id": str(child.id)}))
        if output is None or job is None or job.finished_at is None or job.finished_at > now:
            return "unverified"
        key = (child.id, output.id)
        source = cache.get(key)
        if source is None:
            source = await load_qualified_source(
                session, account_id=trigger.account_id, output_id=output.id, now=job.finished_at
            )
            if not isinstance(source.context, MixedMonitorContext):
                return "unverified"
            cache[key] = source
        context = source.context
        if not isinstance(context, MixedMonitorContext) or context.batch_id != trigger.batch_id:
            return "unverified"
        roles = {row.role: row for row in source.role_outputs}
        candidate = roles["market_state"].mixed_event_proposal
        if candidate is None:
            return "unverified"
        reviews = {
            str(role): row.mixed_event_review
            for role, row in roles.items()
            if role in {"bear", "evidence_audit"} and row.mixed_event_review is not None
        }
        proposal = compile_mixed_proposal(
            candidate,
            reviews,
            context=context.model_dump(mode="json"),
            evidence_refs=source.evidence,
            audit_vote=roles["evidence_audit"].vote,
            adoption_hash=roles["portfolio"].mixed_event_adoption_hash,
        )
        item = next((row for row in proposal.items if row.trigger_id == trigger.id), None)
        if item is None or item.kind != "disclosure" or item.event_key != trigger.event_key:
            return "unverified"
        if proof is None:
            return "unresolved" if item.status == "unresolved" else "unverified"
        if (
            proof.account_id != trigger.account_id
            or proof.instrument_id != trigger.instrument_id
            or proof.batch_id != trigger.batch_id
            or proof.run_id != child.id
            or proof.output_id != output.id
            or proof.materials_snapshot_id != context.materials_snapshot_id
            or proof.context_hash != proposal.context_hash
            or proof.proposal_hash != proposal.content_hash
            or proof.review_hash
            != canonical_hash({key: row.model_dump(mode="json") for key, row in reviews.items()})
            or proof.observed_at < job.finished_at
            or item.status != "addressed"
        ):
            return "unverified"
        coverage = tuple(
            await session.scalars(
                select(MonitorCoverageReceipt).where(
                    MonitorCoverageReceipt.run_id == child.id,
                    MonitorCoverageReceipt.output_id == output.id,
                    MonitorCoverageReceipt.account_id == trigger.account_id,
                    MonitorCoverageReceipt.instrument_id == trigger.instrument_id,
                    MonitorCoverageReceipt.research_as_of == child.decision_time,
                    MonitorCoverageReceipt.observed_at == proof.observed_at,
                    MonitorCoverageReceipt.created_at <= now,
                    MonitorCoverageReceipt.status.in_(
                        ("installed", "retained", "gap", "superseded")
                    ),
                )
            )
        )
        if (
            trigger.status != "completed"
            or len(coverage) != 2
            or {row.kind for row in coverage} != {"price", "disclosure"}
        ):
            return "unverified"
        return "addressed"
    except (ValueError, KeyError, TypeError):
        return "unverified"


async def read_disclosure_monitoring_view(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    *,
    now: datetime,
) -> ResearchDisclosureMonitoringView:
    now = require_utc(now)
    account_id = await _account(session, run, now)
    unknown = ResearchDisclosureMonitoringView(
        observed_at=now, state="unknown", coverage_reason="source_invalid"
    )
    if account_id is None or run.created_at > now or run.decision_time > now:
        return unknown
    scope = (
        MonitorPlan.kind == "disclosure",
        MonitorPlan.account_id == account_id,
        MonitorPlan.instrument_id == run.instrument_id,
        MonitorPlan.created_at <= now,
        MonitorPlan.effective_at <= now,
    )
    plans = tuple(
        await session.scalars(
            select(MonitorPlan).where(*scope, MonitorPlan.source_run_id == run.id).limit(2)
        )
    )
    own = tuple(
        await session.scalars(
            select(MonitorCoverageReceipt)
            .where(
                MonitorCoverageReceipt.kind == "disclosure",
                MonitorCoverageReceipt.account_id == account_id,
                MonitorCoverageReceipt.instrument_id == run.instrument_id,
                MonitorCoverageReceipt.run_id == run.id,
                MonitorCoverageReceipt.created_at <= now,
                MonitorCoverageReceipt.observed_at <= now,
            )
            .limit(2)
        )
    )
    if len(plans) > 1 or len(own) > 1:
        return unknown
    original = plans[0] if plans else None
    receipt = own[0] if own else None
    reason = None if receipt is None else _REASONS.get(receipt.reason)
    if original is None:
        return ResearchDisclosureMonitoringView(
            observed_at=now,
            state="not_installed"
            if receipt is None
            else "unknown"
            if receipt.status == "unknown"
            else "withdrawn",
            coverage_reason=reason,
        )
    try:
        condition = DisclosureReviewCondition.model_validate(original.condition)
        output = await session.get(AgentResearchOutputRecord, original.source_output_id)
        if (
            output is None
            or output.run_id != run.id
            or output.created_at > now
            or output.intent is None
            or output.role is not None
            or output.status != "completed"
            or original.data_snapshot_id != run.data_snapshot_id
        ):
            return unknown
        proposal = DisclosureMonitoringProposal.model_validate(
            output.intent.get("disclosure_proposal")
        )
        if (
            condition.semantic_hash != original.condition_hash
            or condition != proposal.condition
            or original.baseline_snapshot_id != proposal.baseline_snapshot_id
            or original.baseline_as_of != proposal.baseline_as_of
            or original.review_due_at != proposal.review_due_at
            or original.valid_until != proposal.valid_until
            or original.source_hash != output.content_hash
            or output.content_hash
            != canonical_hash(
                {
                    "status": output.status,
                    "role": output.role,
                    "payload": output.payload,
                    "intent": output.intent,
                }
            )
        ):
            return unknown
    except (ValueError, TypeError):
        return unknown
    latest = await session.scalar(
        select(MonitorPlan).where(*scope).order_by(MonitorPlan.generation.desc()).limit(1)
    )
    receipt = await session.scalar(
        select(MonitorCoverageReceipt)
        .where(
            MonitorCoverageReceipt.kind == "disclosure",
            MonitorCoverageReceipt.account_id == account_id,
            MonitorCoverageReceipt.instrument_id == run.instrument_id,
            MonitorCoverageReceipt.created_at <= now,
            MonitorCoverageReceipt.observed_at <= now,
            MonitorCoverageReceipt.status != "superseded",
        )
        .order_by(
            MonitorCoverageReceipt.research_as_of.desc(), MonitorCoverageReceipt.created_at.desc()
        )
        .limit(1)
    )
    state, replacement = "valid", None
    if latest is not None and latest.id != original.id:
        state, replacement = "superseded", latest.source_run_id
    elif now >= original.valid_until:
        state = "expired"
    elif (
        receipt is not None
        and receipt.research_as_of >= run.decision_time
        and receipt.active_plan_id != original.id
    ):
        state = "unknown" if receipt.status == "unknown" else "withdrawn"
        reason = _REASONS.get(receipt.reason)
    elif not await disclosure_source_current(session, original, now):
        state, reason = "unknown", "awaiting_reconciliation"
    check_scope = (
        MonitorCheck.kind == "disclosure",
        MonitorCheck.plan_id == original.id,
        MonitorCheck.account_id == account_id,
        MonitorCheck.instrument_id == run.instrument_id,
        MonitorCheck.created_at <= now,
        MonitorCheck.checked_at <= now,
        MonitorCheck.scheduled_at <= now,
    )
    check = await session.scalar(
        select(MonitorCheck)
        .where(*check_scope)
        .order_by(MonitorCheck.checked_at.desc(), MonitorCheck.id.desc())
        .limit(1)
    )
    trigger_scope = (
        *check_scope,
        MonitorTrigger.kind == "disclosure",
        MonitorTrigger.account_id == account_id,
        MonitorTrigger.instrument_id == run.instrument_id,
        MonitorTrigger.event_key == MonitorCheck.event_key,
        MonitorTrigger.created_at <= now,
    )
    rows = tuple(
        (
            await session.execute(
                select(MonitorTrigger, MonitorCheck)
                .join(MonitorCheck, MonitorCheck.id == MonitorTrigger.check_id)
                .where(*trigger_scope)
                .order_by(MonitorCheck.checked_at.desc(), MonitorTrigger.id.desc())
                .limit(5)
            )
        ).all()
    )
    count = await session.scalar(
        select(func.count())
        .select_from(MonitorTrigger)
        .join(MonitorCheck, MonitorCheck.id == MonitorTrigger.check_id)
        .where(*trigger_scope)
    )
    triggers = []
    cache: dict[tuple[UUID, UUID], QualifiedSource] = {}
    for trigger, checked in rows:
        child = (
            None
            if trigger.research_run_id is None
            else await session.get(AgentResearchRunRecord, trigger.research_run_id)
        )
        hidden_child = child is not None and (
            child.instrument_id != trigger.instrument_id or child.created_at > now
        )
        if hidden_child:
            child = None
        notification = await session.get(OutboxEvent, trigger.outbox_id)
        batch = (
            None
            if trigger.batch_id is None
            else await session.get(MonitorObservationBatch, trigger.batch_id)
        )
        valid_batch = (
            batch is not None
            and batch.account_id == account_id
            and batch.instrument_id == run.instrument_id
            and batch.created_at <= now
            and batch.sealed_at <= now
            and batch.checked_at == checked.checked_at
            and batch.outbox_id == trigger.outbox_id
            and str(trigger.id) in batch.trigger_ids
            and str(checked.id) in batch.check_ids
            and str(original.id) in batch.plan_ids
        )
        daily = await session.scalar(
            select(MonitorDailyEventReceipt).where(
                MonitorDailyEventReceipt.trigger_id == trigger.id,
                MonitorDailyEventReceipt.created_at <= now,
                MonitorDailyEventReceipt.observed_at <= now,
            )
        )
        mixed = await session.scalar(
            select(MonitorMixedEventReceipt).where(
                MonitorMixedEventReceipt.trigger_id == trigger.id,
                MonitorMixedEventReceipt.created_at <= now,
                MonitorMixedEventReceipt.observed_at <= now,
            )
        )
        covered, invalid = False, not valid_batch or hidden_child
        review: EventReview = "unverified" if invalid else "not_performed"
        if daily is not None:
            covered = (
                not invalid
                and daily.kind == "disclosure"
                and daily.account_id == account_id
                and daily.instrument_id == run.instrument_id
                and await validate_daily_event_receipt(session, daily.id)
            )
            invalid = not covered
            review = "addressed" if covered else "unverified"
            if covered:
                child = await session.get(AgentResearchRunRecord, daily.run_id)
        elif not invalid:
            review = await _mixed_review(session, trigger, child, mixed, now, cache)
            invalid = mixed is not None and review != "addressed"
        usage = await _usage(session, child, now)
        if child is not None and child.status == "completed" and usage != "known":
            invalid, review = True, "unverified"
        blocked = {
            "MONITOR_BUDGET_UNAVAILABLE": "budget",
            "MONITOR_DAILY_LIMIT_REACHED": "budget",
            "MONITOR_ACCOUNT_OCCUPIED": "account_occupied",
            "MONITOR_INPUTS_UNAVAILABLE": "inputs",
            "MONITOR_PENDING_OVERFLOW": "overflow",
        }
        triggers.append(
            ResearchDisclosureMonitorTrigger.model_validate(
                {
                    "trigger_id": trigger.id,
                    "batch_id": trigger.batch_id,
                    "event_key": trigger.event_key,
                    "checked_at": checked.checked_at,
                    "status": "unknown" if invalid else "covered" if covered else trigger.status,
                    "blocked_reason": blocked.get(trigger.reason, "unknown")
                    if trigger.status == "blocked" and not covered
                    else None,
                    "run_id": daily.run_id
                    if covered and daily is not None
                    else None
                    if hidden_child
                    else trigger.research_run_id,
                    "usage_status": usage,
                    "notification_status": notification.status
                    if notification is not None
                    and notification.created_at <= now
                    and (notification.published_at is None or notification.published_at <= now)
                    and (
                        notification.dead_lettered_at is None
                        or notification.dead_lettered_at <= now
                    )
                    else "unknown",
                    "event_review": review,
                }
            )
        )
    return ResearchDisclosureMonitoringView.model_validate(
        {
            "observed_at": now,
            "state": state,
            "coverage_reason": reason,
            "plan_id": original.id,
            "condition": condition,
            "baseline_as_of": original.baseline_as_of,
            "review_due_at": original.review_due_at,
            "valid_until": original.valid_until,
            "replacement_run_id": replacement,
            "trigger_count": count or 0,
            "triggers": tuple(triggers),
            "latest_check": None
            if check is None
            else ResearchDisclosureMonitorCheck.model_validate(
                {
                    "result": check.result,
                    "scheduled_at": check.scheduled_at,
                    "checked_at": check.checked_at,
                    "reason": "observation_unverified" if check.reason is not None else None,
                }
            ),
        }
    )
