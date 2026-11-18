"""Fresh custody for an untouched, already-admitted monitor increment."""

from datetime import datetime, timedelta
from uuid import UUID

from sqlalchemy import exists, func, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased

from nexus.monitoring.durable_sec_host import latest_capture
from nexus.monitoring.durable_sec_research import verify_event_evidence
from nexus.monitoring.durable_sec_storage import rule_state
from nexus.paper.host_probe_state import require_host_admitting
from nexus.persistence.models.monitoring import MonitorIncrement, MonitorPlan
from nexus.persistence.models.runtime import AuditEvent, Job
from nexus.persistence.models.sec_conflicts import SecDisclosureKeyConflict
from nexus.persistence.models.sec_monitor import (
    SecDiscoveryEvent,
    SecDiscoveryEventRevision,
    SecMonitorRule,
)
from nexus.persistence.models.sec_research import SecEventResearchBinding
from nexus.research.durable_sec_index_storage import read_durable_index_capture
from nexus.research.sec_exhibit_storage import configured_exhibit_store
from nexus.settings import AgentRuntimeMode, EngineSettings

WORKER_ACTION = "research.monitor.worker_dispatched"
SEC_DEFERRED_ACTION = "research.monitor.sec_first_claim_deferred"
_SEC_DEFERRED_SCHEMA = "monitor_sec_preclaim_wait_v1"
_SEC_DEFERRED_REASONS = frozenset(
    {
        "SEC_RESEARCH_PERMISSION_INVALID",
        "SEC_RESEARCH_PERMISSION_BUSY",
        "MONITOR_SEC_PRECLAIM_REFRESH_UNAVAILABLE",
        "MONITOR_SEC_PRECLAIM_WINDOW_CLOSED",
    }
)


async def deferred_receipt(
    session: AsyncSession, member: MonitorIncrement, *, since: datetime, now: datetime
) -> AuditEvent | None:
    row = await session.scalar(
        select(AuditEvent)
        .where(
            AuditEvent.action == SEC_DEFERRED_ACTION,
            AuditEvent.actor_id == "research-monitor",
            AuditEvent.schema_version == _SEC_DEFERRED_SCHEMA,
            AuditEvent.payload["job_id"].astext == str(member.job_id),
            AuditEvent.occurred_at >= since,
            AuditEvent.occurred_at <= now,
        )
        .order_by(AuditEvent.event_seq.desc())
        .limit(1)
    )
    if row is None:
        return None
    reason = row.payload.get("reason")
    confirmed = row.payload.get("source_confirmed")
    if (
        not isinstance(reason, str)
        or reason not in _SEC_DEFERRED_REASONS
        or not isinstance(confirmed, bool)
        or row.payload != _deferred_payload(member, reason, confirmed)
    ):
        raise ValueError("MONITOR_SEC_PRECLAIM_RECEIPT_INVALID")
    return row


def _deferred_payload(
    member: MonitorIncrement, reason: str, source_confirmed: bool
) -> dict[str, object]:
    return {
        "trigger_id": str(member.trigger_id),
        "run_id": str(member.run_id),
        "job_id": str(member.job_id),
        "reason": reason,
        "source_confirmed": source_confirmed,
    }


async def _defer_sec_preclaim(
    session: AsyncSession,
    member: MonitorIncrement,
    *,
    now: datetime,
    reason: str,
    source_confirmed: bool,
) -> str:
    from nexus.audit.service import append_audit_event

    await append_audit_event(
        session,
        actor_id="research-monitor",
        action=SEC_DEFERRED_ACTION,
        schema_version=_SEC_DEFERRED_SCHEMA,
        payload=_deferred_payload(member, reason, source_confirmed),
        occurred_at=now,
    )
    return reason


async def expired_dispatch_capture(
    session: AsyncSession, member: MonitorIncrement, *, now: datetime
) -> bool:
    """Prove the last exact source is stale, only to authorize a new official read."""
    rule = await session.scalar(
        select(SecMonitorRule).where(
            SecMonitorRule.account_id == member.account_id,
            SecMonitorRule.instrument_id == member.instrument_id,
        )
    )
    if rule is None or await rule_state(session, rule) != "active":
        return False
    latest = await latest_capture(session, rule, now)
    if latest is None or now - latest.received_at <= timedelta(seconds=300):
        return False

    binding = SecEventResearchBinding
    event = SecDiscoveryEvent
    plan = MonitorPlan
    newer = aliased(MonitorPlan)
    rows = tuple(
        await session.execute(
            select(
                event,
                plan,
                exists().where(SecDiscoveryEventRevision.event_id == event.id),
                exists().where(
                    SecDisclosureKeyConflict.account_id == event.account_id,
                    SecDisclosureKeyConflict.instrument_id == event.instrument_id,
                    SecDisclosureKeyConflict.event_key == event.event_key,
                ),
                exists().where(
                    newer.account_id == plan.account_id,
                    newer.instrument_id == plan.instrument_id,
                    newer.kind == plan.kind,
                    newer.generation > plan.generation,
                ),
            )
            .select_from(binding)
            .join(event, binding.event_id == event.id)
            .join(plan, binding.plan_id == plan.id)
            .where(binding.batch_id == member.batch_id)
            .limit(33)
        )
    )
    if not 1 <= len(rows) <= 32 or any(
        row.rule_id != rule.id
        or (row.account_id, row.instrument_id) != (member.account_id, member.instrument_id)
        or (parent.account_id, parent.instrument_id, parent.kind)
        != (member.account_id, member.instrument_id, "disclosure")
        or not parent.effective_at <= now < parent.valid_until
        or revised
        or conflict
        or superseded
        for row, parent, revised, conflict, superseded in rows
    ):
        return False
    try:
        _, capture = await read_durable_index_capture(
            session, configured_exhibit_store(session), snapshot_id=latest.id, as_of=now
        )
        for row, *_ in rows:
            # Use the recorded receipt instant to verify its original content.
            # The real clock below still requires this receipt to be expired.
            verify_event_evidence(row, capture, now=capture.index.received_at)
    except (ValueError, OSError):
        return False
    return now - capture.index.received_at > timedelta(seconds=300)


class WorkerDispatchRefused(ValueError):
    """The original native job cannot be dispatched on the current evidence."""


async def record_worker_dispatch(
    session: AsyncSession,
    *,
    settings: EngineSettings,
    account_id: UUID,
    trigger_id: UUID,
    job_id: UUID,
    can_refresh: bool,
    refresh_not_after: datetime | None,
    after_refresh: bool = False,
    defer_source_confirmed: bool | None = None,
) -> tuple[UUID, datetime] | str | None:
    from nexus.audit.service import append_audit_event
    from nexus.monitoring.durable_sec_permission import durable_batch, require_new_send
    from nexus.ops.monitor_disclosure_refresh import TOTAL_TIMEOUT_SECONDS, _bucket
    from nexus.paper.target_service import lock_portfolio_account

    await session.execute(text("SET LOCAL statement_timeout='5s'"))
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    await lock_portfolio_account(session, account_id)
    await require_host_admitting(session, account_id)
    job = await session.get(Job, job_id, with_for_update=True)
    member = await session.scalar(
        select(MonitorIncrement).where(
            MonitorIncrement.job_id == job_id,
            MonitorIncrement.account_id == account_id,
            MonitorIncrement.trigger_id == trigger_id,
        )
    )
    if (
        job is None
        or member is None
        or job.status != "queued"
        or job.attempt_count != 0
        or job.manual_retry_count != 0
        or job.payload != {"run_id": str(member.run_id)}
        or await session.scalar(
            select(AuditEvent.id)
            .where(
                AuditEvent.action == WORKER_ACTION,
                AuditEvent.payload["job_id"].astext == str(job_id),
            )
            .limit(1)
        )
        is not None
    ):
        raise WorkerDispatchRefused("MONITOR_WORKER_DISPATCH_UNCONFIRMED")

    native_durable = (
        settings.agent_runtime_mode is AgentRuntimeMode.RESPONSES
        and await durable_batch(session, member.run_id) is not None
    )
    if after_refresh and not native_durable:
        raise WorkerDispatchRefused("MONITOR_WORKER_DISPATCH_UNCONFIRMED")
    if native_durable:
        from nexus.agents.research_stages.routes import selected_route

        route = await selected_route(session, job_id)
        if (
            route is None
            or route["kind"] != "increment"
            or route["scope_id"] != member.id
            or route["run_id"] != member.run_id
            or route.get("platform_binding") is None
            or await session.scalar(
                text("""
                    SELECT stopped
                       OR EXISTS(SELECT FROM job_leases WHERE job_id=:job)
                       OR EXISTS(SELECT FROM job_attempts WHERE job_id=:job)
                    FROM research_stage_scope WHERE id=:scope
                    """),
                {"job": job_id, "scope": member.id},
            )
            is not False
        ):
            raise WorkerDispatchRefused("MONITOR_WORKER_DISPATCH_UNCONFIRMED")

        now = await session.scalar(select(func.clock_timestamp()))
        if not isinstance(now, datetime):
            raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
        prior = await deferred_receipt(session, member, since=_bucket(now), now=now)
        if prior is not None:
            return str(prior.payload["reason"])
        if defer_source_confirmed is not None:
            return await _defer_sec_preclaim(
                session,
                member,
                now=now,
                reason="MONITOR_SEC_PRECLAIM_REFRESH_UNAVAILABLE",
                source_confirmed=defer_source_confirmed,
            )

        try:
            # The SQL gate may reject and abort its transaction. Retain no
            # authority from a rejected check, and inspect the source separately.
            async with session.begin_nested():
                ready = await require_new_send(session, member.run_id)
                if not ready:
                    raise ValueError("SEC_RESEARCH_PERMISSION_INVALID")
        except ValueError as error:
            if str(error) not in {
                "SEC_RESEARCH_PERMISSION_INVALID",
                "SEC_RESEARCH_PERMISSION_BUSY",
            }:
                raise
            if can_refresh and str(error) == "SEC_RESEARCH_PERMISSION_INVALID":
                now = await session.scalar(select(func.clock_timestamp()))
                if (
                    isinstance(now, datetime)
                    and (
                        refresh_not_after is None
                        or now + timedelta(seconds=TOTAL_TIMEOUT_SECONDS) <= refresh_not_after
                    )
                    and await expired_dispatch_capture(session, member, now=now)
                ):
                    checked_at = await session.scalar(select(func.clock_timestamp()))
                    if (
                        isinstance(checked_at, datetime)
                        and _bucket(checked_at) == _bucket(now)
                        and (
                            refresh_not_after is None
                            or checked_at + timedelta(seconds=TOTAL_TIMEOUT_SECONDS)
                            <= refresh_not_after
                        )
                    ):
                        return member.instrument_id, _bucket(checked_at)
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
            return await _defer_sec_preclaim(
                session, member, now=now, reason=str(error), source_confirmed=True
            )

    async def append_dispatch(at: datetime) -> None:
        await append_audit_event(
            session,
            actor_id="research-monitor",
            action=WORKER_ACTION,
            schema_version="monitor_dispatch_v1",
            payload={"trigger_id": str(trigger_id), "job_id": str(job_id)},
            occurred_at=at,
        )

    sent_at = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(sent_at, datetime):
        raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
    if not after_refresh or refresh_not_after is None:
        await append_dispatch(sent_at)
        return None
    try:
        async with session.begin_nested():
            if sent_at >= refresh_not_after:
                raise WorkerDispatchRefused("MONITOR_SEC_PRECLAIM_WINDOW_CLOSED")
            await append_dispatch(sent_at)
            # The global audit lock can delay the append beyond the last
            # sample. Undo the marker before returning a known defer.
            recorded_at = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(recorded_at, datetime):
                raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
            if recorded_at >= refresh_not_after:
                sent_at = recorded_at
                raise WorkerDispatchRefused("MONITOR_SEC_PRECLAIM_WINDOW_CLOSED")
    except WorkerDispatchRefused as error:
        return await _defer_sec_preclaim(
            session, member, now=sent_at, reason=str(error), source_confirmed=True
        )
    return None


async def confirm_deferred_stop(
    session: AsyncSession,
    *,
    account_id: UUID,
    run_id: UUID,
    action_started_at: datetime,
    now: datetime,
) -> bool:
    """Confirm only this action's witnessed defer and the original untouched Job."""
    from nexus.monitoring.durable_sec_permission import durable_batch
    from nexus.paper.target_service import lock_portfolio_account

    await lock_portfolio_account(session, account_id)
    member = await session.scalar(
        select(MonitorIncrement).where(
            MonitorIncrement.run_id == run_id,
            MonitorIncrement.account_id == account_id,
        )
    )
    if member is None or await durable_batch(session, member.run_id) is None:
        return False
    proof = await deferred_receipt(session, member, since=action_started_at, now=now)
    if proof is None or proof.payload["source_confirmed"] is not True:
        return False
    return (
        await session.scalar(
            text("""
            SELECT j.status = 'queued' AND j.attempt_count = 0
               AND j.manual_retry_count = 0 AND r.status = 'prepared'
               AND NOT EXISTS(SELECT FROM job_attempts WHERE job_id=j.id)
               AND NOT EXISTS(SELECT FROM job_leases WHERE job_id=j.id)
               AND NOT EXISTS(
                   SELECT FROM research_stage_authorization WHERE run_id=r.id
               )
               AND NOT EXISTS(SELECT FROM audit_events a WHERE a.action=:dispatch
                              AND a.payload->>'job_id'=j.id::text)
            FROM jobs j JOIN agent_research_runs r ON r.id=:run
            WHERE j.id=:job AND j.payload=jsonb_build_object('run_id',r.id::text)
            """),
            {"run": member.run_id, "job": member.job_id, "dispatch": WORKER_ACTION},
        )
        is True
    )
