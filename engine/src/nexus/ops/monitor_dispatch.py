"""Single-host incremental queue, using original admission and fenced Worker."""

import argparse
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID

from sqlalchemy import Text, cast, exists, func, or_, select, text
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import ColumnElement

from nexus.monitoring.daily_consumption import daily_receipt, not_daily_covered
from nexus.monitoring.durable_sec_refreeze import not_superseded_pending
from nexus.monitoring.mixed_readback import not_mixed_consumed
from nexus.monitoring.original_occupancy import accepted, active_monitor_runs
from nexus.ops.agent_paper_loop import (
    AgentPaperLoopAction,
    AgentPaperLoopSnapshot,
    choose_loop_action,
)
from nexus.ops.monitor_dispatch_sec import (
    SEC_DEFERRED_ACTION as SEC_DEFERRED_ACTION,
)
from nexus.ops.monitor_dispatch_sec import (
    WORKER_ACTION,
    WorkerDispatchRefused,
    confirm_deferred_stop,
    deferred_receipt,
    record_worker_dispatch,
)
from nexus.paper.host_probe_state import require_host_admitting
from nexus.persistence.models.monitoring import MonitorIncrement, MonitorTrigger
from nexus.persistence.models.runtime import AuditEvent, Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings

IDLE_ACTIONS = frozenset(
    {AgentPaperLoopAction.WAIT_FOR_SLOT, AgentPaperLoopAction.WAIT_FOR_SESSION}
)
DISPATCH_ACTION = "research.monitor.dispatch_checked"


def unchecked(now: datetime) -> ColumnElement[bool]:
    bucket = now.replace(minute=now.minute // 5 * 5, second=0, microsecond=0)
    peer = aliased(MonitorTrigger)
    return ~exists(
        select(AuditEvent.id)
        .join(peer, AuditEvent.payload["trigger_id"].astext == cast(peer.id, Text))
        .where(
            AuditEvent.action == DISPATCH_ACTION,
            or_(
                MonitorTrigger.batch_id.is_not(None),
                peer.batch_id.is_not(None),
                peer.kind == MonitorTrigger.kind,
            ),
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
            active = await active_monitor_runs(session, account_id=account_id, now=snapshot.now)
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
                if (
                    job is not None
                    and trigger.status == "admitted"
                    and job.status == "queued"
                    and job.attempt_count == 0
                    and sent is None
                ):
                    from nexus.ops.monitor_disclosure_refresh import _bucket

                    if (
                        await deferred_receipt(
                            session, member, since=_bucket(snapshot.now), now=snapshot.now
                        )
                        is not None
                    ):
                        # This queued Run still owns its original reservation.
                        return replace(snapshot, monitor_preclaim_deferred=True)
                status = (
                    "unknown"
                    if trigger.status == "unknown"
                    or job is None
                    or job.status == "running"
                    or (job.status == "queued" and sent is not None)
                    else "admitted"
                )
                return replace(
                    snapshot,
                    monitor_trigger_id=trigger.id,
                    monitor_trigger_status=status,
                    monitor_batch_id=member.batch_id,
                )
            if settings.agent_paper_candidate_batch_id is None or ordinary not in IDLE_ACTIONS:
                return snapshot
            batch = await load_agent_paper_candidate_batch(
                session, batch_id=settings.agent_paper_candidate_batch_id
            )
            if batch is None or batch.account_id != account_id or batch.created_at > snapshot.now:
                raise ValueError("MONITOR_HOST_SCOPE_CHANGED")
            pending = tuple(
                await session.scalars(
                    select(MonitorTrigger)
                    .where(
                        MonitorTrigger.account_id == account_id,
                        MonitorTrigger.instrument_id.in_(batch.instrument_ids),
                        MonitorTrigger.status.in_(("pending", "blocked")),
                        MonitorTrigger.research_run_id.is_(None),
                        not_superseded_pending(),
                        unchecked(snapshot.now),
                        not_daily_covered(),
                        not_mixed_consumed(),
                    )
                    .order_by(MonitorTrigger.created_at, MonitorTrigger.id)
                )
            )
            from nexus.monitoring.mixed_readback import unadmitted_stock_count

            seen_batches: set[UUID] = set()
            overflow: dict[UUID, bool] = {}
            for trigger in pending:
                if trigger.instrument_id not in overflow:
                    overflow[trigger.instrument_id] = (
                        await unadmitted_stock_count(
                            session, account_id=account_id, instrument_id=trigger.instrument_id
                        )
                        > 32
                    )
                if overflow[trigger.instrument_id]:
                    continue
                if trigger.batch_id is not None:
                    if trigger.batch_id in seen_batches:
                        continue
                    seen_batches.add(trigger.batch_id)
                    from nexus.ops.monitor_batch_prepare import inspect_batch_preparation
                    from nexus.persistence.models.monitoring import MonitorObservationBatch

                    sealed = await session.get(MonitorObservationBatch, trigger.batch_id)
                    if (
                        sealed is None
                        or sealed.account_id != account_id
                        or sealed.instrument_id != trigger.instrument_id
                        or max(sealed.created_at, sealed.sealed_at) > snapshot.now
                        or not 1 <= len(sealed.trigger_ids) <= 32
                    ):
                        continue
                    group = tuple(
                        await session.scalars(
                            select(MonitorTrigger)
                            .where(MonitorTrigger.batch_id == sealed.id)
                            .order_by(MonitorTrigger.id)
                        )
                    )
                    if tuple(str(row.id) for row in group) != tuple(sealed.trigger_ids) or any(
                        row.status not in {"pending", "blocked"} or row.research_run_id is not None
                        for row in group
                    ):
                        continue
                    from nexus.ops.monitor_market_prepare import inspect_batch_market_preparation

                    market_state = await inspect_batch_market_preparation(session, sealed.id)
                    if market_state in {"blocked", "unknown"}:
                        continue
                    state = await inspect_batch_preparation(session, sealed.id)
                    if state in {"blocked", "unknown"}:
                        continue
                    return replace(
                        snapshot,
                        monitor_trigger_id=group[0].id,
                        monitor_trigger_status="pending",
                        monitor_batch_id=sealed.id,
                        monitor_materials_status=state,
                    )
                if trigger.kind == "price":
                    return replace(
                        snapshot, monitor_trigger_id=trigger.id, monitor_trigger_status="pending"
                    )
            return snapshot
    finally:
        await factory.kw["bind"].dispose()


async def dispatch_configured_once(
    settings: EngineSettings,
    *,
    trigger_id: UUID,
    calendar_authority_id: UUID,
    repo_root: Path | None = None,
    sec_contact: str | None = None,
    environment: dict[str, str] | None = None,
    prepare_materials_only: bool = False,
    refresh_not_after: datetime | None = None,
) -> dict[str, object]:
    """Persist a bounded eligibility check, then consume only the original native Job.

    Retrying a blocked eligibility check on a later tick does not retry research.
    Any existing Run identity, including unknown/failed, is never replaced.
    """
    from nexus.agents.research_maintenance import require_research_open

    require_research_open()
    from nexus.audit.service import append_audit_event
    from nexus.monitoring.admission import require_increment_binding
    from nexus.ops.agent_paper_once import _validate_local_database
    from nexus.ops.research_monitor import run
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
            await require_host_admitting(session, settings.agent_paper_account_id)
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
            trigger = await session.get(MonitorTrigger, trigger_id)
            if (
                trigger is None
                or (trigger.kind != "price" and trigger.batch_id is None)
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
            if prepare_materials_only and (
                trigger.batch_id is None or trigger.research_run_id is not None
            ):
                return {"status": "deferred", "execution_locked": True}
            if trigger.research_run_id is not None:
                original = await session.get(AgentResearchRunRecord, trigger.research_run_id)
                if original is None:
                    raise ValueError("MONITOR_INCREMENT_BINDING_INVALID")
                if await accepted(
                    session, run_id=original.id, account_id=trigger.account_id, now=now
                ):
                    return {
                        "status": "historical_occupancy_disposed",
                        "trigger_id": str(trigger.id),
                        "run_id": str(original.id),
                        "execution_locked": True,
                        "model_requests": 0,
                        "provider_requests": 0,
                    }
                member = await require_increment_binding(session, original)
                trigger_id = member.trigger_id
                job = await session.get(Job, member.job_id)
                assert job is not None
                if trigger.status in {"completed", "failed", "unknown"} and member.batch_id is None:
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
                elif job.status == "skipped" and member.batch_id is not None:
                    from nexus.monitoring.preclaim import preclaim_rejection_qualified

                    if not await preclaim_rejection_qualified(session, run=original, now=now):
                        return {"status": "unconfirmed", "execution_locked": True}
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
            if trigger.batch_id is not None:
                prepared = await run(
                    settings,
                    argparse.Namespace(
                        action="prepare-pending",
                        batch_id=trigger.batch_id,
                        repo_root=Path.cwd() if repo_root is None else repo_root,
                        _sec_contact=sec_contact,
                    ),
                )
                if prepared["status"] != "prepared" or prepare_materials_only:
                    return {**prepared, "trigger_id": str(trigger.id)}
                from nexus.ops.monitor_market_prepare import prepare_batch_market_inputs

                market = await prepare_batch_market_inputs(
                    settings,
                    account_id=settings.agent_paper_account_id,
                    batch_id=trigger.batch_id,
                    calendar_authority_id=calendar_authority_id,
                    environment=environment or {},
                    repo_root=repo_root,
                )
                if market["status"] != "prepared":
                    return {**market, "trigger_id": str(trigger.id)}
                admitted = await run(
                    settings,
                    argparse.Namespace(
                        action="admit-batch",
                        batch_id=trigger.batch_id,
                        materials_snapshot_id=UUID(str(prepared["snapshot_id"])),
                        calendar_authority_id=calendar_authority_id,
                    ),
                )
            else:
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
            if "trigger_id" in admitted:
                trigger_id = UUID(str(admitted["trigger_id"]))
        if job_id is not None:
            from nexus.ops.monitor_disclosure_refresh import refresh_disclosure_observations
            from nexus.paper.account_transaction_turn import account_transaction_turn

            account_id = settings.agent_paper_account_id

            async def record_dispatch(
                selected_job: UUID,
                *,
                can_refresh: bool,
                after_refresh: bool = False,
                defer_source_confirmed: bool | None = None,
            ) -> tuple[UUID, datetime] | str | None:
                async with account_transaction_turn(settings), factory.begin() as session:
                    return await record_worker_dispatch(
                        session,
                        settings=settings,
                        account_id=account_id,
                        trigger_id=trigger_id,
                        job_id=selected_job,
                        refresh_not_after=refresh_not_after,
                        can_refresh=can_refresh,
                        after_refresh=after_refresh,
                        defer_source_confirmed=defer_source_confirmed,
                    )

            try:
                stale = await record_dispatch(
                    job_id,
                    can_refresh=repo_root is not None
                    and bool((environment or {}).get("SEC_CONTACT")),
                )
                if isinstance(stale, tuple):
                    instrument_id, scheduled_at = stale
                    assert repo_root is not None
                    observation = await refresh_disclosure_observations(
                        settings,
                        account_id=account_id,
                        calendar_authority_id=calendar_authority_id,
                        scheduled_at=scheduled_at,
                        environment=environment or {},
                        repo_root=repo_root,
                        only_instrument=instrument_id,
                    )
                    status = observation["status"]
                    stale = await record_dispatch(
                        job_id,
                        can_refresh=False,
                        after_refresh=True,
                        defer_source_confirmed=None
                        if status in {"refreshed", "unchanged"}
                        else status != "unknown"
                        and type(observation.get("provider_requests")) is int,
                    )
                if isinstance(stale, str):
                    return {"status": "deferred", "reason": stale, "execution_locked": True}
            except WorkerDispatchRefused as error:
                return {"status": "unconfirmed", "reason": str(error), "execution_locked": True}
            from nexus.ops.agent_capacity_rejection import dispatch_preclaim_code

            try:
                await run_agent_worker_once(settings, job_ids=(job_id,), formal_stage_capacity=True)
            except ValueError as error:
                code = dispatch_preclaim_code(error, settings)
                if code is None:
                    raise
                from nexus.monitoring.preclaim import close_capacity_rejected_increment_once

                rejected = await close_capacity_rejected_increment_once(
                    settings,
                    account_id=settings.agent_paper_account_id,
                    trigger_id=trigger_id,
                    job_id=job_id,
                    code=code,
                )
                await run(settings, argparse.Namespace(action="reconcile"))
                return rejected
        finished = await run(settings, argparse.Namespace(action="finish", trigger_id=trigger_id))
        # Reconciliation only reads the terminal native result and original audit.
        await run(settings, argparse.Namespace(action="reconcile"))
        if finished["status"] == "admitted":
            return {"status": "unconfirmed", "execution_locked": True}
        if "unresolved_event_count" in finished:
            # Only reread immutable receipts after reconciliation; no worker/source call.
            finished = await run(
                settings, argparse.Namespace(action="finish", trigger_id=trigger_id)
            )
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
    prepare_materials_only: bool = False,
) -> dict[str, object]:
    from nexus.agents.research_maintenance import require_research_open

    require_research_open()
    from nexus.ops.research_monitor import load_host_settings

    private_context: dict[str, str] = {}
    current = await load_host_settings(
        settings,
        account_id=account_id,
        calendar_authority_id=calendar_authority_id,
        repo_root=repo_root,
        env_file=env_file,
        private_context=private_context,
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
    if prepared_market_open_at is not None:
        normal = replace(normal, host_prepared_market_open_at=prepared_market_open_at)
    selected = await inspect_monitor_queue(current, account_id=account_id, snapshot=normal)
    if selected.monitor_trigger_id != trigger_id or choose_loop_action(
        selected, prepared_market_open_at=prepared_market_open_at
    ) != (
        AgentPaperLoopAction.PREPARE_MONITOR_MATERIALS
        if prepare_materials_only
        else AgentPaperLoopAction.RUN_MONITOR_INCREMENT
    ):
        return {"status": "deferred", "execution_locked": True}
    if (
        selected.monitor_batch_id is not None
        and selected.monitor_trigger_status == "pending"
        and evaluation is not None
        and not prepare_materials_only
    ):
        from nexus.monitoring.host import BATCH_PREPARATION_HEADROOM_SECONDS

        if (
            evaluation.evaluation_slot_at is None
            or evaluation.evaluation_slot_at
            <= evaluation.now
            + timedelta(
                seconds=INCREMENT_ACTION_HEADROOM_SECONDS + BATCH_PREPARATION_HEADROOM_SECONDS
            )
        ):
            return {"status": "deferred", "execution_locked": True}
    from nexus.ops.agent_paper_loop import _portfolio_slot

    cutoffs: list[datetime] = []
    if normal.decision_window_start_at is not None:
        daily = _portfolio_slot(normal.decision_window_start_at)
        if normal.now < daily or choose_loop_action(normal) in {
            AgentPaperLoopAction.TRIGGER,
            AgentPaperLoopAction.PREPARE_INPUTS,
        }:
            cutoffs.append(daily)
    if evaluation is not None and evaluation.evaluation_slot_at is not None:
        cutoffs.append(evaluation.evaluation_slot_at)
    refresh_not_after = (
        min(cutoffs) - timedelta(seconds=INCREMENT_ACTION_HEADROOM_SECONDS) if cutoffs else None
    )
    return await dispatch_configured_once(
        current,
        trigger_id=trigger_id,
        calendar_authority_id=calendar_authority_id,
        repo_root=repo_root,
        sec_contact=private_context.get("SEC_CONTACT"),
        environment=private_context,
        prepare_materials_only=prepare_materials_only,
        refresh_not_after=refresh_not_after,
    )


async def confirm_monitor_materials_shutdown(
    settings: EngineSettings, *, account_id: UUID, trigger_id: UUID, batch_id: UUID
) -> bool:
    """Confirm only this exact collection action; no market or child proof is claimed."""
    if settings.agent_paper_account_id != account_id:
        return False
    from nexus.ops.agent_paper_once import _validate_local_database
    from nexus.ops.monitor_batch_prepare import confirm_batch_preparation

    _validate_local_database(settings.database_url)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            await session.execute(text("SET LOCAL statement_timeout='5s'"))
            trigger = await session.get(MonitorTrigger, trigger_id)
            if (
                trigger is None
                or trigger.account_id != account_id
                or trigger.batch_id != batch_id
                or trigger.research_run_id is not None
                or trigger.status not in {"pending", "blocked"}
            ):
                return False
            return await confirm_batch_preparation(
                settings, account_id=account_id, batch_id=batch_id
            )
    finally:
        await factory.kw["bind"].dispose()


async def confirm_monitor_shutdown(
    settings: EngineSettings,
    *,
    account_id: UUID,
    trigger_id: UUID,
    action_started_at: datetime | None = None,
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
                or (trigger.kind != "price" and trigger.batch_id is None)
                or trigger.account_id != account_id
            ):
                return False
            if trigger.research_run_id is None:
                if trigger.batch_id is None:
                    return False
                from nexus.ops.monitor_batch_prepare import confirm_batch_preparation
                from nexus.ops.monitor_market_prepare import confirm_batch_market_preparation

                return await confirm_batch_preparation(
                    settings, account_id=account_id, batch_id=trigger.batch_id
                ) and await confirm_batch_market_preparation(
                    settings, account_id=account_id, batch_id=trigger.batch_id
                )
            if trigger.status == "admitted" and action_started_at is not None:
                return await confirm_deferred_stop(
                    session,
                    account_id=account_id,
                    run_id=trigger.research_run_id,
                    action_started_at=action_started_at,
                    now=now,
                )
            if trigger.status not in {"completed", "failed"}:
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
            coverage_rows = tuple(
                await session.scalars(
                    select(MonitorCoverageReceipt).where(
                        MonitorCoverageReceipt.run_id == run.id,
                        MonitorCoverageReceipt.account_id == account_id,
                    )
                )
            )
            coverage = (
                {row.kind for row in coverage_rows} == {"price", "disclosure"}
                and all(
                    row.status in {"installed", "retained", "gap", "superseded"}
                    for row in coverage_rows
                )
                if member.batch_id is not None
                else any(row.kind == "price" for row in coverage_rows)
            )
            from nexus.monitoring.preclaim import preclaim_rejection_qualified

            if coverage and await preclaim_rejection_qualified(session, run=run, now=now):
                return True
            return bool(
                coverage
                and reservation is not None
                and reservation.state == "settled"
                and run.settled_cost_usd is not None
                and reservation.settled_usd == run.settled_cost_usd
                and run.input_tokens is not None
                and run.output_tokens is not None
            )
    finally:
        await factory.kw["bind"].dispose()
