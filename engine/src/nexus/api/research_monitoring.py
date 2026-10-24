"""Bounded reads of saved price monitoring; never reconciles, collects or admits work."""

from datetime import datetime

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.schemas.research_monitoring import (
    BlockedReason,
    CoverageReason,
    MonitoringState,
    ResearchMonitorCheck,
    ResearchMonitoringView,
    ResearchMonitorTrigger,
    UsageStatus,
)
from nexus.domain.time import require_utc
from nexus.monitoring.contracts import PriceReviewCondition
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.monitoring import (
    MonitorCheck,
    MonitorCoverageReceipt,
    MonitorDailyTriggerReceipt,
    MonitorPlan,
    MonitorTrigger,
)
from nexus.persistence.models.runtime import OutboxEvent

_REASONS: dict[str, CoverageReason] = {
    "MONITOR_AUDIT_REJECTED": "audit_rejected",
    "MONITOR_SOURCE_INVALID": "source_invalid",
    "MONITOR_SOURCE_UNVERIFIED": "source_invalid",
    "MONITOR_NO_APPROVED_CONDITION": "no_condition",
    "MONITOR_RUN_FAILED": "run_failed",
    "MONITOR_UNKNOWN_USAGE": "usage_unknown",
}


async def read_monitoring_view(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    *,
    now: datetime,
) -> ResearchMonitoringView:
    now = require_utc(now)
    plans = tuple(
        await session.scalars(
            select(MonitorPlan)
            .where(
                MonitorPlan.kind == "price",
                MonitorPlan.source_run_id == run.id,
                MonitorPlan.instrument_id == run.instrument_id,
                MonitorPlan.created_at <= now,
                MonitorPlan.effective_at <= now,
            )
            .limit(2)
        )
    )
    receipts = tuple(
        await session.scalars(
            select(MonitorCoverageReceipt)
            .where(
                MonitorCoverageReceipt.kind == "price",
                MonitorCoverageReceipt.run_id == run.id,
                MonitorCoverageReceipt.observed_at <= now,
                MonitorCoverageReceipt.created_at <= now,
            )
            .limit(2)
        )
    )
    if len(plans) > 1 or len(receipts) > 1:
        return ResearchMonitoringView(
            observed_at=now, state="unknown", coverage_reason="source_invalid"
        )
    original = plans[0] if plans else None
    own_receipt = receipts[0] if receipts else None
    reason = _REASONS.get(own_receipt.reason) if own_receipt is not None else None
    if original is None:
        return ResearchMonitoringView(
            observed_at=now,
            state="unknown"
            if own_receipt is not None and own_receipt.status == "unknown"
            else "withdrawn"
            if own_receipt is not None
            else "not_installed",
            coverage_reason=reason,
        )
    latest = await session.scalar(
        select(MonitorPlan)
        .where(
            MonitorPlan.kind == "price",
            MonitorPlan.account_id == original.account_id,
            MonitorPlan.instrument_id == original.instrument_id,
            MonitorPlan.created_at <= now,
            MonitorPlan.effective_at <= now,
        )
        .order_by(MonitorPlan.generation.desc())
        .limit(1)
    )
    receipt = await session.scalar(
        select(MonitorCoverageReceipt)
        .where(
            MonitorCoverageReceipt.kind == "price",
            MonitorCoverageReceipt.account_id == original.account_id,
            MonitorCoverageReceipt.instrument_id == original.instrument_id,
            MonitorCoverageReceipt.observed_at <= now,
            MonitorCoverageReceipt.created_at <= now,
            MonitorCoverageReceipt.status != "superseded",
        )
        .order_by(
            MonitorCoverageReceipt.research_as_of.desc(), MonitorCoverageReceipt.created_at.desc()
        )
        .limit(1)
    )
    state: MonitoringState = "valid"
    replacement = None
    if latest is not None and latest.id != original.id:
        state, replacement = "superseded", latest.source_run_id
    elif now >= original.valid_until:
        state = "expired"
    elif (
        receipt is not None
        and receipt.research_as_of >= run.decision_time
        and (receipt.active_plan_id != original.id)
    ):
        state = "unknown" if receipt.status == "unknown" else "withdrawn"
        reason = _REASONS.get(receipt.reason)
    # A completed newer result must be reconciled before claiming its prior
    # coverage survives; this read has no authority to perform reconciliation.
    from nexus.monitoring.reconciliation import plan_covered

    if state == "valid" and not await plan_covered(session, plan=original, now=now):
        state, reason = "unknown", "awaiting_reconciliation"
    check = await session.scalar(
        select(MonitorCheck)
        .where(
            MonitorCheck.kind == "price",
            MonitorCheck.plan_id == original.id,
            MonitorCheck.checked_at <= now,
            MonitorCheck.created_at <= now,
        )
        .order_by(MonitorCheck.checked_at.desc(), MonitorCheck.id.desc())
        .limit(1)
    )
    rows = (
        await session.execute(
            select(
                MonitorTrigger,
                MonitorCheck,
                AgentResearchRunRecord,
                OutboxEvent,
                MonitorDailyTriggerReceipt,
            )
            .join(MonitorCheck, MonitorCheck.id == MonitorTrigger.check_id)
            .outerjoin(
                MonitorDailyTriggerReceipt,
                MonitorDailyTriggerReceipt.trigger_id == MonitorTrigger.id,
            )
            .outerjoin(
                AgentResearchRunRecord,
                AgentResearchRunRecord.id
                == func.coalesce(MonitorTrigger.research_run_id, MonitorDailyTriggerReceipt.run_id),
            )
            .outerjoin(OutboxEvent, OutboxEvent.id == MonitorTrigger.outbox_id)
            .where(
                MonitorCheck.kind == "price",
                MonitorCheck.plan_id == original.id,
                MonitorTrigger.kind == "price",
                MonitorTrigger.account_id == original.account_id,
                MonitorTrigger.instrument_id == run.instrument_id,
                MonitorTrigger.created_at <= now,
                MonitorCheck.created_at <= now,
                MonitorCheck.checked_at <= now,
            )
            .order_by(MonitorCheck.checked_at.desc(), MonitorTrigger.id.desc())
            .limit(5)
        )
    ).all()
    count = await session.scalar(
        select(func.count())
        .select_from(MonitorTrigger)
        .join(MonitorCheck, MonitorCheck.id == MonitorTrigger.check_id)
        .where(
            MonitorCheck.kind == "price",
            MonitorCheck.plan_id == original.id,
            MonitorTrigger.kind == "price",
            MonitorTrigger.account_id == original.account_id,
            MonitorTrigger.instrument_id == run.instrument_id,
            MonitorTrigger.created_at <= now,
            MonitorCheck.checked_at <= now,
            MonitorCheck.created_at <= now,
        )
    )
    triggers = []
    for trigger, checked, child, notification, daily in rows:
        covered = daily is not None and max(daily.created_at, daily.observed_at) <= now
        usage: UsageStatus = (
            "not_started"
            if child is None or child.created_at > now or child.status == "prepared"
            else (
                "known"
                if child.settled_cost_usd is not None
                and child.input_tokens is not None
                and child.output_tokens is not None
                else "unknown"
                if child.status in {"completed", "failed", "abstained"}
                else "in_progress"
            )
        )
        blocked_codes: dict[str, BlockedReason] = {
            "MONITOR_BUDGET_UNAVAILABLE": "budget",
            "MONITOR_DAILY_LIMIT_REACHED": "budget",
            "MONITOR_ACCOUNT_OCCUPIED": "account_occupied",
            "MONITOR_INPUTS_UNAVAILABLE": "inputs",
            "MONITOR_PENDING_OVERFLOW": "overflow",
        }
        blocked = (
            blocked_codes.get(trigger.reason, "unknown")
            if not covered and trigger.status == "blocked"
            else None
        )
        triggers.append(
            ResearchMonitorTrigger(
                trigger_id=trigger.id,
                cause=trigger.cause,
                checked_at=checked.checked_at,
                status="covered" if covered else trigger.status,
                blocked_reason=blocked,
                run_id=(daily.run_id if covered else trigger.research_run_id),
                usage_status=usage,
                notification_status=notification.status
                if notification is not None and notification.created_at <= now
                else "unknown",
            )
        )
    return ResearchMonitoringView(
        observed_at=now,
        state=state,
        coverage_reason=reason,
        plan_id=original.id,
        condition=PriceReviewCondition.model_validate(original.condition),
        valid_until=original.valid_until,
        review_due_at=original.review_due_at,
        replacement_run_id=replacement,
        latest_check=None
        if check is None
        else ResearchMonitorCheck.model_validate(
            {
                "result": check.result,
                "scheduled_at": check.scheduled_at,
                "checked_at": check.checked_at,
                "reason": check.reason,
            }
        ),
        triggers=tuple(triggers),
        trigger_count=count or 0,
    )
