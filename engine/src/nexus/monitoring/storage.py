"""Transactional checks and trigger capture, before independent research admission."""

from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.domain.time import require_utc
from nexus.monitoring.contracts import PriceObservation, PriceReviewCondition
from nexus.monitoring.evaluation import EpisodeState, advance_episode, evaluate_price
from nexus.persistence.models.monitoring import (
    MonitorCheck,
    MonitorEpisode,
    MonitorPlan,
    MonitorTrigger,
)
from nexus.persistence.models.runtime import OutboxEvent


async def lock_security(session: AsyncSession, *, account_id: UUID, instrument_id: UUID) -> None:
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 0))"),
        {"scope": f"research-monitor:{account_id}:{instrument_id}"},
    )


async def plan_recorded_at(session: AsyncSession, *, requested_at: datetime) -> datetime:
    """Read insertion visibility only after holding the security transaction lock."""
    value = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(value, datetime) or value.utcoffset() is None or value < requested_at:
        raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
    return require_utc(value)


async def record_check(
    session: AsyncSession,
    *,
    plan_id: UUID,
    scheduled_at: datetime,
    checked_at: datetime,
    observation: PriceObservation | None,
    batch_id: UUID | None = None,
    batch_outbox_id: UUID | None = None,
) -> MonitorCheck:
    """Persist one current tick using a verified loader projection, never enqueue here.

    The caller commits this transaction before trying normal research admission, so
    exhausted budget or unknown occupancy cannot erase a real trigger.
    """
    if (batch_id is None) != (batch_outbox_id is None):
        raise ValueError("MONITOR_BATCH_IDENTITY_INVALID")
    scheduled_at, checked_at = require_utc(scheduled_at), require_utc(checked_at)
    if (
        scheduled_at.second
        or scheduled_at.microsecond
        or scheduled_at.minute % 5
        or not scheduled_at <= checked_at < scheduled_at + timedelta(minutes=5)
    ):
        raise ValueError("MONITOR_TICK_TIME_INVALID")
    plan = await session.get(MonitorPlan, plan_id)
    if plan is None:
        raise ValueError("MONITOR_PLAN_UNAVAILABLE")
    if plan.kind != "price":
        raise ValueError("MONITOR_KIND_UNSUPPORTED")
    await lock_security(session, account_id=plan.account_id, instrument_id=plan.instrument_id)
    existing = await session.scalar(
        select(MonitorCheck).where(
            MonitorCheck.kind == "price",
            MonitorCheck.account_id == plan.account_id,
            MonitorCheck.instrument_id == plan.instrument_id,
            MonitorCheck.scheduled_at == scheduled_at,
        )
    )
    if existing is not None:
        # Return the original observation/result, not a replacement quote or plan.
        return existing
    latest_plan = await session.scalar(
        select(MonitorPlan.id)
        .where(
            MonitorPlan.kind == "price",
            MonitorPlan.account_id == plan.account_id,
            MonitorPlan.instrument_id == plan.instrument_id,
        )
        .order_by(MonitorPlan.generation.desc())
        .limit(1)
    )
    if latest_plan != plan.id or not plan.effective_at <= checked_at < plan.valid_until:
        raise ValueError("MONITOR_PLAN_NOT_CURRENT")
    condition = PriceReviewCondition.model_validate(plan.condition)
    if condition.semantic_hash != plan.condition_hash:
        raise ValueError("MONITOR_PLAN_IDENTITY_INVALID")
    state = await session.get(
        MonitorEpisode, (plan.account_id, plan.instrument_id, plan.condition_hash)
    )
    if state is not None and state.checked_at > checked_at:
        raise ValueError("MONITOR_LATE_CHECK")
    previous = (
        EpisodeState()
        if state is None
        else EpisodeState(
            sequence=state.sequence,
            latched=state.latched,
            observed_false=state.observed_false,
        )
    )
    evaluation = evaluate_price(
        condition, observation, instrument_id=plan.instrument_id, now=checked_at
    )
    updated, cause = advance_episode(previous, evaluation.result)
    check = MonitorCheck(
        account_id=plan.account_id,
        instrument_id=plan.instrument_id,
        plan_id=plan.id,
        scheduled_at=scheduled_at,
        checked_at=checked_at,
        result=evaluation.result,
        reason=evaluation.reason,
        observation=None if observation is None else observation.model_dump(mode="json"),
    )
    session.add(check)
    if state is None:
        state = MonitorEpisode(
            account_id=plan.account_id,
            instrument_id=plan.instrument_id,
            condition_hash=plan.condition_hash,
            checked_at=checked_at,
        )
        session.add(state)
    state.sequence, state.latched = updated.sequence, updated.latched
    state.observed_false, state.checked_at = updated.observed_false, checked_at
    await session.flush()
    if cause is not None:
        trigger_id, outbox_id = new_uuid7(), batch_outbox_id or new_uuid7()
        if batch_id is None:
            event = OutboxEvent(
                id=outbox_id,
                aggregate_id=trigger_id,
                event_type="research.monitor.triggered",
                schema_version="research_price_trigger_v1",
                payload={
                    "schema_version": "research_price_trigger_v1",
                    "trigger_id": str(trigger_id),
                    "plan_id": str(plan.id),
                    "source_run_id": str(plan.source_run_id),
                    "instrument_id": str(plan.instrument_id),
                    "cause": cause,
                    "occurred_at": checked_at.isoformat(),
                },
                available_at=checked_at,
            )
            session.add(event)
            await session.flush()
        session.add(
            MonitorTrigger(
                id=trigger_id,
                batch_id=batch_id,
                account_id=plan.account_id,
                instrument_id=plan.instrument_id,
                condition_hash=plan.condition_hash,
                episode=updated.sequence,
                check_id=check.id,
                cause=cause,
                status="pending",
                reason=None,
                research_run_id=None,
                outbox_id=outbox_id,
            )
        )
        await session.flush()
    return check


async def mark_trigger_blocked(
    session: AsyncSession,
    *,
    trigger_id: UUID,
    reason: str,
    now: datetime,
) -> MonitorTrigger:
    from nexus.audit.service import append_audit_event

    now = require_utc(now)
    if reason not in {
        "MONITOR_DAILY_LIMIT_REACHED",
        "MONITOR_BUDGET_UNAVAILABLE",
        "MONITOR_ACCOUNT_OCCUPIED",
        "MONITOR_UNKNOWN_USAGE",
        "MONITOR_INPUTS_UNAVAILABLE",
    }:
        raise ValueError("MONITOR_BLOCK_REASON_INVALID")
    trigger = await session.get(MonitorTrigger, trigger_id)
    if trigger is None:
        raise ValueError("MONITOR_TRIGGER_UNAVAILABLE")
    if trigger.kind != "price":
        raise ValueError("MONITOR_KIND_UNSUPPORTED")
    from nexus.paper.target_service import lock_portfolio_account

    await lock_portfolio_account(session, trigger.account_id)
    await lock_security(session, account_id=trigger.account_id, instrument_id=trigger.instrument_id)
    await session.refresh(trigger)
    if trigger.status == "blocked" and trigger.reason == reason:
        return trigger
    if trigger.status not in {"pending", "blocked"} or trigger.research_run_id is not None:
        raise ValueError("MONITOR_TRIGGER_ALREADY_DISPATCHED")
    trigger.status, trigger.reason = "blocked", reason
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.admission_blocked",
        schema_version="monitor_admission_v1",
        payload={"trigger_id": str(trigger.id), "reason": reason},
        occurred_at=now,
    )
    await session.flush()
    return trigger
