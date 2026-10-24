"""Single-host incremental queue, using original admission and fenced Worker."""

import argparse
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID

from sqlalchemy import Text, cast, exists, func, select, text
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import ColumnElement

from nexus.monitoring.daily_consumption import daily_receipt, not_daily_covered
from nexus.ops.agent_paper_loop import (
    AgentPaperLoopAction,
    AgentPaperLoopSnapshot,
    choose_loop_action,
)
from nexus.persistence.models.monitoring import MonitorIncrement, MonitorTrigger
from nexus.persistence.models.runtime import AuditEvent, Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings

IDLE_ACTIONS = frozenset(
    {AgentPaperLoopAction.WAIT_FOR_SLOT, AgentPaperLoopAction.WAIT_FOR_SESSION}
)
DISPATCH_ACTION = "research.monitor.dispatch_checked"
WORKER_ACTION = "research.monitor.worker_dispatched"


def unchecked(now: datetime) -> ColumnElement[bool]:
    bucket = now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0)
    peer = aliased(MonitorTrigger)
    return ~exists(
        select(AuditEvent.id)
        .join(peer, AuditEvent.payload["trigger_id"].astext == cast(peer.id, Text))
        .where(
            AuditEvent.action == DISPATCH_ACTION,
            peer.kind == MonitorTrigger.kind,
            peer.account_id == MonitorTrigger.account_id,
            peer.instrument_id == MonitorTrigger.instrument_id,
            AuditEvent.occurred_at >= bucket,
        )
    )


async def inspect_monitor_queue(
    settings: EngineSettings, *, account_id: UUID, snapshot: AgentPaperLoopSnapshot
) -> AgentPaperLoopSnapshot:
    """Read pending scope; daily/manual/evaluation actions retain priority."""
    if (
        not settings.agent_paper_cycle_enabled
        or not snapshot.enabled
        or settings.agent_paper_formal_model_config_id is None
    ):
        # Legacy hosts have no approved monitoring model; preserve their existing route.
        return snapshot
    ordinary = choose_loop_action(snapshot)
    if ordinary not in IDLE_ACTIONS | {
        AgentPaperLoopAction.TRIGGER,
        AgentPaperLoopAction.PREPARE_INPUTS,
    }:
        return snapshot
    if account_id != settings.agent_paper_account_id:
        raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
    from nexus.paper.candidate_batch import load_agent_paper_candidate_batch

    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            active = tuple(
                await session.scalars(
                    select(MonitorTrigger.research_run_id)
                    .where(
                        MonitorTrigger.account_id == account_id,
                        MonitorTrigger.status.in_(("admitted", "unknown")),
                    )
                    .distinct()
                    .limit(2)
                )
            )
            if len(active) > 1:
                raise ValueError("MONITOR_ACCOUNT_BUSY_OR_UNKNOWN")
            if active:
                member = await session.scalar(
                    select(MonitorIncrement).where(MonitorIncrement.run_id == active[0])
                )
                if member is None:
                    raise ValueError("MONITOR_ACCOUNT_BUSY_OR_UNKNOWN")
                trigger = await session.get(MonitorTrigger, member.trigger_id)
                from nexus.monitoring.admission import require_increment_binding
                from nexus.persistence.models.agents import AgentResearchRunRecord

                original = await session.get(AgentResearchRunRecord, member.run_id)
                if trigger is None or original is None:
                    raise ValueError("MONITOR_ACCOUNT_BUSY_OR_UNKNOWN")
                await require_increment_binding(session, original)
                job = None if member is None else await session.get(Job, member.job_id)
                sent = (
                    None
                    if job is None
                    else await session.scalar(
                        select(AuditEvent.id)
                        .where(
                            AuditEvent.action == WORKER_ACTION,
                            AuditEvent.payload["job_id"].astext == str(job.id),
                        )
                        .limit(1)
                    )
                )
                status = (
                    "unknown"
                    if trigger.status == "unknown"
                    or job is None
                    or job.status == "running"
                    or (job.status == "queued" and sent is not None)
                    else "admitted"
                )
                return replace(
                    snapshot, monitor_trigger_id=trigger.id, monitor_trigger_status=status
                )
            if (
                settings.agent_paper_candidate_batch_id is None
                or ordinary == AgentPaperLoopAction.TRIGGER
            ):
                return snapshot
            batch = await load_agent_paper_candidate_batch(
                session, batch_id=settings.agent_paper_candidate_batch_id
            )
            if batch is None or batch.account_id != account_id or batch.created_at > snapshot.now:
                raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
            pending = await session.scalar(
                select(MonitorTrigger)
                .where(
                    MonitorTrigger.kind == "price",
                    MonitorTrigger.account_id == account_id,
                    MonitorTrigger.instrument_id.in_(batch.instrument_ids),
                    MonitorTrigger.status.in_(("pending", "blocked")),
                    MonitorTrigger.research_run_id.is_(None),
                    unchecked(snapshot.now),
                    not_daily_covered(),
                )
                .order_by(MonitorTrigger.created_at, MonitorTrigger.id)
                .limit(1)
            )
            return (
                snapshot
                if pending is None
                else replace(
                    snapshot, monitor_trigger_id=pending.id, monitor_trigger_status="pending"
                )
            )
    finally:
        await factory.kw["bind"].dispose()


async def dispatch_configured_once(
    settings: EngineSettings, *, trigger_id: UUID, calendar_authority_id: UUID
) -> dict[str, object]:
    """Persist a bounded eligibility check, then consume only the original native Job.

    Retrying a blocked eligibility check on a later tick does not retry research.
    Any existing Run identity, including unknown/failed, is never replaced.
    """
    from nexus.audit.service import append_audit_event
    from nexus.monitoring.admission import require_increment_binding
    from nexus.ops.agent_paper_once import _validate_local_database
    from nexus.ops.research_monitor import run
    from nexus.paper.target_service import lock_portfolio_account
    from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD, SCHEMA_ADMISSION_LOCK_KEY
    from nexus.persistence.models.agents import AgentResearchRunRecord
    from nexus.worker import run_agent_worker_once

    if not settings.agent_paper_cycle_enabled or settings.agent_paper_account_id is None:
        raise ValueError("MONITOR_HOST_DISABLED")
    _validate_local_database(settings.database_url)
    factory = session_factory(settings)
    job_id = None
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            await session.execute(text("SET LOCAL lock_timeout='2s'"))
            await session.execute(
                text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key, 0))"),
                {"key": SCHEMA_ADMISSION_LOCK_KEY},
            )
            if (
                await session.scalar(text("SELECT version_num FROM alembic_version"))
                != CURRENT_MIGRATION_HEAD
            ):
                raise ValueError("MONITOR_SCHEMA_UNAVAILABLE")
            await lock_portfolio_account(session, settings.agent_paper_account_id)
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
            trigger = await session.get(MonitorTrigger, trigger_id)
            if (
                trigger is None
                or trigger.kind != "price"
                or trigger.account_id != settings.agent_paper_account_id
            ):
                raise ValueError("MONITOR_TRIGGER_UNAVAILABLE")
            covered = await daily_receipt(session, trigger.id)
            if covered is not None:
                return {
                    "status": "covered",
                    "trigger_id": str(trigger.id),
                    "run_id": str(covered.run_id),
                    "output_id": str(covered.output_id),
                    "execution_locked": True,
                    "model_requests": 0,
                    "provider_requests": 0,
                }
            if trigger.research_run_id is not None:
                original = await session.get(AgentResearchRunRecord, trigger.research_run_id)
                if original is None:
                    raise ValueError("MONITOR_INCREMENT_BINDING_INVALID")
                member = await require_increment_binding(session, original)
                trigger_id = member.trigger_id
                job = await session.get(Job, member.job_id)
                assert job is not None
                if trigger.status in {"completed", "failed", "unknown"}:
                    return {
                        "status": trigger.status,
                        "trigger_id": str(member.trigger_id),
                        "run_id": str(original.id),
                        "execution_locked": True,
                    }
                if (
                    job.status == "queued"
                    and job.attempt_count == 0
                    and job.manual_retry_count == 0
                ):
                    from nexus.paper.candidate_batch import load_agent_paper_candidate_batch

                    batch = (
                        None
                        if settings.agent_paper_candidate_batch_id is None
                        else await load_agent_paper_candidate_batch(
                            session, batch_id=settings.agent_paper_candidate_batch_id
                        )
                    )
                    if (
                        original.model_config_id != settings.agent_paper_formal_model_config_id
                        or batch is None
                        or batch.account_id != settings.agent_paper_account_id
                        or batch.created_at > now
                        or original.instrument_id not in batch.instrument_ids
                    ):
                        raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
                    job_id = job.id
                elif job.status not in {"succeeded", "failed"}:
                    return {"status": "unconfirmed", "execution_locked": True}
            else:
                if not await session.scalar(
                    select(MonitorTrigger.id).where(MonitorTrigger.id == trigger.id, unchecked(now))
                ):
                    return {"status": "checked", "execution_locked": True}
                await append_audit_event(
                    session,
                    actor_id="research-monitor",
                    action=DISPATCH_ACTION,
                    schema_version="monitor_dispatch_v1",
                    payload={"trigger_id": str(trigger.id)},
                    occurred_at=now,
                )
        if trigger.research_run_id is None:
            admitted = await run(
                settings,
                argparse.Namespace(
                    action="admit",
                    trigger_id=trigger_id,
                    calendar_authority_id=calendar_authority_id,
                ),
            )
            if admitted["status"] != "admitted":
                return admitted
            job_id = UUID(str(admitted["job_id"]))
            trigger_id = UUID(str(admitted["trigger_id"]))
        if job_id is not None:
            # Native claim, one attempt, exact Job and capacity/lease checks. No fallback key.
            async with factory() as session, session.begin():
                await lock_portfolio_account(session, settings.agent_paper_account_id)
                sent = await session.scalar(
                    select(AuditEvent.id)
                    .where(
                        AuditEvent.action == WORKER_ACTION,
                        AuditEvent.payload["job_id"].astext == str(job_id),
                    )
                    .limit(1)
                )
                if sent is not None:
                    return {"status": "unconfirmed", "execution_locked": True}
                sent_at = await session.scalar(select(func.clock_timestamp()))
                if not isinstance(sent_at, datetime):
                    raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
                await append_audit_event(
                    session,
                    actor_id="research-monitor",
                    action=WORKER_ACTION,
                    schema_version="monitor_dispatch_v1",
                    payload={"trigger_id": str(trigger_id), "job_id": str(job_id)},
                    occurred_at=sent_at,
                )
            from nexus.ops.agent_capacity_rejection import CAPACITY_REJECTION_CODES

            try:
                await run_agent_worker_once(settings, job_ids=(job_id,), formal_stage_capacity=True)
            except ValueError as error:
                if str(error) not in CAPACITY_REJECTION_CODES:
                    raise
                from nexus.monitoring.preclaim import close_capacity_rejected_increment_once

                rejected = await close_capacity_rejected_increment_once(
                    settings,
                    account_id=settings.agent_paper_account_id,
                    trigger_id=trigger_id,
                    job_id=job_id,
                    code=str(error),
                )
                await run(settings, argparse.Namespace(action="reconcile"))
                return rejected
        finished = await run(settings, argparse.Namespace(action="finish", trigger_id=trigger_id))
        # Reconciliation only reads the terminal native result and original audit.
        await run(settings, argparse.Namespace(action="reconcile"))
        if finished["status"] == "admitted":
            return {"status": "unconfirmed", "execution_locked": True}
        return finished
    finally:
        await factory.kw["bind"].dispose()


async def dispatch_host_once(
    settings: EngineSettings,
    *,
    account_id: UUID,
    trigger_id: UUID,
    calendar_authority_id: UUID,
    repo_root: Path,
    env_file: Path | None = None,
    prepared_market_open_at: datetime | None = None,
) -> dict[str, object]:
    from nexus.ops.research_monitor import load_host_settings

    current = await load_host_settings(
        settings,
        account_id=account_id,
        calendar_authority_id=calendar_authority_id,
        repo_root=repo_root,
        env_file=env_file,
    )
    if not current.agent_paper_cycle_enabled:
        raise ValueError("MONITOR_HOST_DISABLED")
    from nexus.ops.agent_paper_formal import require_formal_host_configuration_match
    from nexus.ops.agent_paper_loop import (
        inspect_agent_paper_loop,
        inspect_agent_version_evaluation_loop,
    )
    from nexus.ops.manual_paper_host import inspect_manual_close, inspect_manual_queue

    require_formal_host_configuration_match(settings, current)
    if settings.agent_paper_candidate_batch_id != current.agent_paper_candidate_batch_id:
        raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
    # Recheck due normal work immediately before spending; inspection cannot grant a stale slot.
    evaluation = await inspect_agent_version_evaluation_loop(
        current, expected_calendar_authority_id=calendar_authority_id
    )
    from nexus.monitoring.host import INCREMENT_ACTION_HEADROOM_SECONDS

    if evaluation is not None and (
        evaluation.cycle_status != "evaluation_waiting"
        or evaluation.evaluation_slot_at is None
        or evaluation.evaluation_slot_at
        <= evaluation.now + timedelta(seconds=INCREMENT_ACTION_HEADROOM_SECONDS)
    ):
        return {"status": "deferred", "execution_locked": True}
    normal = await inspect_agent_paper_loop(
        current, account_id=account_id, expected_calendar_authority_id=calendar_authority_id
    )
    normal = await inspect_manual_close(current, account_id=account_id, snapshot=normal)
    normal = await inspect_manual_queue(current, account_id=account_id, snapshot=normal)
    selected = await inspect_monitor_queue(current, account_id=account_id, snapshot=normal)
    if (
        selected.monitor_trigger_id != trigger_id
        or choose_loop_action(selected, prepared_market_open_at=prepared_market_open_at)
        != AgentPaperLoopAction.RUN_MONITOR_INCREMENT
    ):
        return {"status": "deferred", "execution_locked": True}
    return await dispatch_configured_once(
        current, trigger_id=trigger_id, calendar_authority_id=calendar_authority_id
    )


async def confirm_monitor_shutdown(
    settings: EngineSettings, *, account_id: UUID, trigger_id: UUID
) -> bool:
    """Read exact native exit, known settlement and durable reconciliation only."""
    from nexus.monitoring.admission import require_increment_binding
    from nexus.monitoring.reconciliation import _native_finished
    from nexus.ops.agent_paper_once import _validate_local_database
    from nexus.persistence.models.agents import (
        AgentDailyBudgetReservationRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.monitoring import MonitorCoverageReceipt

    if settings.agent_paper_account_id != account_id:
        return False
    _validate_local_database(settings.database_url)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            now = await session.scalar(select(func.clock_timestamp()))
            trigger = await session.get(MonitorTrigger, trigger_id)
            if (
                not isinstance(now, datetime)
                or trigger is None
                or trigger.kind != "price"
                or trigger.account_id != account_id
                or trigger.status not in {"completed", "failed"}
                or trigger.research_run_id is None
            ):
                return False
            run = await session.get(AgentResearchRunRecord, trigger.research_run_id)
            if run is None or run.status not in {"completed", "failed", "abstained"}:
                return False
            member = await require_increment_binding(session, run)
            if member.account_id != account_id or not await _native_finished(session, run, now):
                return False
            reservation = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run.id
                )
            )
            coverage = await session.scalar(
                select(MonitorCoverageReceipt.id).where(
                    MonitorCoverageReceipt.kind == "price",
                    MonitorCoverageReceipt.run_id == run.id,
                    MonitorCoverageReceipt.account_id == account_id,
                )
            )
            from nexus.monitoring.preclaim import preclaim_rejection_qualified

            if coverage is not None and await preclaim_rejection_qualified(
                session, run=run, now=now
            ):
                return True
            return bool(
                coverage is not None
                and reservation is not None
                and reservation.state == "settled"
                and run.settled_cost_usd is not None
                and reservation.settled_usd == run.settled_cost_usd
                and run.input_tokens is not None
                and run.output_tokens is not None
            )
    finally:
        await factory.kw["bind"].dispose()
