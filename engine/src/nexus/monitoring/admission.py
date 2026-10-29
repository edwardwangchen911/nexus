"""Normal single-security monitor admission; no model calls or execution authority."""

from datetime import datetime
from decimal import Decimal
from typing import Literal, cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.jobs.models import job_input_hash
from nexus.monitoring.context import MonitorResearchContext, read_monitor_context
from nexus.monitoring.mixed_context import MixedMonitorContext
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import MonitorIncrement, MonitorTrigger
from nexus.persistence.models.runtime import Job, JobLease


def increment_manifest_hash(member: MonitorIncrement, run: AgentResearchRunRecord, job: Job) -> str:
    return job_input_hash(
        {
            "purpose": "monitor_increment_v2"
            if member.batch_id is not None
            else "price_monitor_increment_v1",
            "account_id": str(member.account_id),
            "instrument_id": str(member.instrument_id),
            "trigger_id": str(member.trigger_id),
            "plan_id": str(member.plan_id),
            "run_id": str(run.id),
            "job_id": str(job.id),
            "snapshot_id": str(member.snapshot_id),
            "research_as_of": member.research_as_of.isoformat(),
            "session_date": member.session_date.isoformat(),
            "reserved_sdk_calls": member.reserved_sdk_calls,
            "daily_sdk_call_limit": member.daily_sdk_call_limit,
            "context_hash": member.context_hash,
            "job_input_hash": job.input_hash,
            "model_hash": run.model_hash,
            "prompt_hash": run.prompt_hash,
            "evidence_hash": run.evidence_hash,
            "input_hash": run.input_hash,
            "schema_hash": run.schema_hash,
            "tool_hash": run.tool_hash,
        }
    )


async def require_increment_binding(
    session: AsyncSession, run: AgentResearchRunRecord
) -> MonitorIncrement:
    from nexus.agents.codex_formal_stage import _formal_stage_job_hash

    invalid = "MONITOR_INCREMENT_BINDING_INVALID"
    member = await session.scalar(select(MonitorIncrement).where(MonitorIncrement.run_id == run.id))
    if member is None:
        raise ValueError(invalid)
    job = await session.get(Job, member.job_id)
    trigger = await session.get(MonitorTrigger, member.trigger_id)
    bound = await read_monitor_context(
        session,
        snapshot_id=run.data_snapshot_id,
        instrument_id=run.instrument_id,
        as_of=run.decision_time,
    )
    if (
        job is None
        or trigger is None
        or (trigger.kind != "price" and member.batch_id is None)
        or bound is None
        or member.snapshot_id != run.data_snapshot_id
        or member.instrument_id != run.instrument_id
        or member.research_as_of != run.decision_time
        or member.account_id != bound[0].account_id
        or member.plan_id != bound[0].plan_id
        or member.context_hash != bound[1].content_hash
        or member.trigger_id != bound[0].trigger_checks[0].trigger_id
        or member.reserved_sdk_calls != 3
        or not 3 <= member.daily_sdk_call_limit <= 90
        or trigger.research_run_id != run.id
        or trigger.status not in {"admitted", "completed", "failed", "unknown"}
        or job.payload != {"run_id": str(run.id)}
        or job.job_type != "agent.paper.research"
        or job.schema_version != "agent_research_job_v1"
        or job.max_attempts != 1
        or job.manual_retry_count != 0
        or job.scope_id != str(run.instrument_id)
        or job.scope_kind != "instrument"
        or job.effective_at != run.decision_time
        or job.input_hash != _formal_stage_job_hash(run)
        or member.manifest_hash != increment_manifest_hash(member, run, job)
    ):
        raise ValueError(invalid)
    if isinstance(bound[0], MixedMonitorContext):
        if member.batch_id != bound[0].batch_id:
            raise ValueError(invalid)
    elif member.batch_id is not None:
        raise ValueError(invalid)
    frozen_ids = [item.trigger_id for item in bound[0].trigger_checks]
    rows = tuple(
        await session.scalars(
            select(MonitorTrigger).where(MonitorTrigger.research_run_id == run.id)
        )
    )
    if (
        len(set(frozen_ids)) != len(frozen_ids)
        or {row.id for row in rows} != set(frozen_ids)
        or any(
            (row.kind != "price" and member.batch_id is None)
            or row.account_id != member.account_id
            or row.instrument_id != member.instrument_id
            or (row.status, row.reason) != (trigger.status, trigger.reason)
            for row in rows
        )
    ):
        raise ValueError(invalid)
    return member


async def require_monitor_account_available(
    session: AsyncSession, *, account_id: UUID, now: datetime
) -> None:
    """Shared by manual, scheduled, and monitor admission; unknown remains occupied."""
    from nexus.paper.target_service import lock_portfolio_account

    await lock_portfolio_account(session, account_id)
    members = tuple(
        await session.scalars(
            select(MonitorIncrement).where(MonitorIncrement.account_id == account_id)
        )
    )
    for member in members:
        await _require_monitor_member_available(session, member=member, now=now)


async def _legacy_require_monitor_member_available(
    session: AsyncSession, *, member: MonitorIncrement, now: datetime
) -> None:
    """One native member; callers retain the account lock and whole-account iteration."""
    trigger = await session.get(MonitorTrigger, member.trigger_id)
    if trigger is None or trigger.status == "unknown":
        raise ValueError("MONITOR_ACCOUNT_BUSY_OR_UNKNOWN")
    run = await session.get(AgentResearchRunRecord, member.run_id)
    job = await session.get(Job, member.job_id)
    reservation = await session.scalar(
        select(AgentDailyBudgetReservationRecord).where(
            AgentDailyBudgetReservationRecord.run_id == member.run_id
        )
    )
    if run is not None and job is not None and job.status == "skipped":
        from nexus.monitoring.preclaim import preclaim_rejection_qualified

        if await preclaim_rejection_qualified(session, run=run, now=now):
            return
    if (
        run is None
        or job is None
        or reservation is None
        or run.status not in {"completed", "failed", "abstained"}
        or job.status not in {"succeeded", "failed"}
        or job.finished_at is None
        or job.finished_at > now
        or run.settled_cost_usd is None
        or run.input_tokens is None
        or run.output_tokens is None
        or reservation.state != "settled"
        or reservation.settled_usd != run.settled_cost_usd
    ):
        raise ValueError("MONITOR_ACCOUNT_BUSY_OR_UNKNOWN")
    leases = tuple(
        await session.scalars(
            select(JobLease).where(JobLease.job_id == job.id, JobLease.expires_at > now)
        )
    )
    for lease in leases:
        from nexus.jobs.repository import job_lease_consumed

        if (
            not await job_lease_consumed(session, job=job, lease=lease, now=now)
            or job.attempt_count != 1
            or job.manual_retry_count != 0
        ):
            raise ValueError("MONITOR_ACCOUNT_BUSY_OR_UNKNOWN")


async def admit_trigger(
    session: AsyncSession,
    *,
    account_id: UUID,
    trigger_id: UUID,
    candidate_batch_id: UUID,
    calendar_authority_id: UUID,
    model_config_id: UUID,
    daily_limit_usd: Decimal,
    daily_sdk_call_limit: int,
    now: datetime,
) -> MonitorIncrement:
    """One new identity only; failed eligibility rolls back all admission writes."""
    async with session.begin_nested():
        return await _admit_trigger(
            session,
            account_id=account_id,
            trigger_id=trigger_id,
            candidate_batch_id=candidate_batch_id,
            calendar_authority_id=calendar_authority_id,
            model_config_id=model_config_id,
            daily_limit_usd=daily_limit_usd,
            daily_sdk_call_limit=daily_sdk_call_limit,
            now=now,
        )


async def _admit_trigger(
    session: AsyncSession,
    *,
    account_id: UUID,
    trigger_id: UUID,
    candidate_batch_id: UUID,
    calendar_authority_id: UUID,
    model_config_id: UUID,
    daily_limit_usd: Decimal,
    daily_sdk_call_limit: int,
    now: datetime,
) -> MonitorIncrement:
    import hashlib
    from decimal import Decimal, localcontext

    from sqlalchemy import func

    from nexus.agents.models import ResearchRequest
    from nexus.agents.paper_context import AgentPaperPortfolioContext
    from nexus.agents.persistence import admit_research_run
    from nexus.domain.time import require_utc
    from nexus.jobs.models import JobType
    from nexus.monitoring.context import MonitorResearchContext, TriggerCheck, content
    from nexus.monitoring.contracts import PriceObservation, PriceReviewCondition
    from nexus.monitoring.evaluation import evaluate_price
    from nexus.monitoring.observations import read_latest_price
    from nexus.monitoring.storage import lock_security
    from nexus.paper.agent_cycle_authority import (
        load_agent_paper_service_calendar_authority,
        load_quasi_realtime_cycle_authority,
        prepare_candidates,
    )
    from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch
    from nexus.paper.manual_request import require_manual_account_available
    from nexus.paper.projection import load_paper_projection
    from nexus.paper.target_service import lock_portfolio_account
    from nexus.persistence.models.agents import AgentResearchOutputRecord
    from nexus.persistence.models.monitoring import MonitorPlan
    from nexus.strategy.intents import ResearchIntent

    now = require_utc(now)
    if type(daily_sdk_call_limit) is not int or not 3 <= daily_sdk_call_limit <= 90:
        raise ValueError("MONITOR_CALL_LIMIT_INVALID")
    await lock_portfolio_account(session, account_id)
    frozen_now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(frozen_now, datetime):
        raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
    now = require_utc(frozen_now)
    trigger = await session.get(MonitorTrigger, trigger_id)
    if trigger is None or trigger.kind != "price" or trigger.account_id != account_id:
        raise ValueError("MONITOR_TRIGGER_UNAVAILABLE")
    if trigger.batch_id is not None:
        raise ValueError("MONITOR_TYPED_CONTEXT_REQUIRED")
    # A concurrent request may have populated this session before the account lock.
    await session.refresh(trigger)
    from nexus.monitoring.daily_consumption import daily_receipt

    if await daily_receipt(session, trigger.id) is not None:
        raise ValueError("MONITOR_TRIGGER_ALREADY_COVERED")
    if trigger.research_run_id is not None:
        run = await session.get(AgentResearchRunRecord, trigger.research_run_id)
        if run is None:
            raise ValueError("MONITOR_INCREMENT_BINDING_INVALID")
        return await require_increment_binding(session, run)
    # A monitor continues the scheduled loop after its deterministic execution
    # proof; the history validator still checks every Job, fee and live lease.
    await require_manual_account_available(
        session, account_id=account_id, now=now, allow_scheduled_completion=True
    )
    await lock_security(session, account_id=account_id, instrument_id=trigger.instrument_id)
    await session.refresh(trigger, with_for_update=True)
    from nexus.monitoring.mixed_readback import unadmitted_stock_count
    from nexus.monitoring.pending import compatible_check, pending_stock, same_condition_lineage

    if (
        await unadmitted_stock_count(
            session, account_id=account_id, instrument_id=trigger.instrument_id
        )
        > 32
    ):
        raise ValueError("MONITOR_PENDING_OVERFLOW")

    pending = await pending_stock(
        session, account_id=account_id, instrument_id=trigger.instrument_id, now=now
    )
    if trigger.id not in {item.id for item, _ in pending}:
        raise ValueError("MONITOR_TRIGGER_UNAVAILABLE")
    trigger, check = pending[0]
    plan = await session.scalar(
        select(MonitorPlan)
        .where(
            MonitorPlan.kind == "price",
            MonitorPlan.account_id == account_id,
            MonitorPlan.instrument_id == trigger.instrument_id,
        )
        .order_by(MonitorPlan.generation.desc())
        .limit(1)
    )
    if plan is None or not plan.effective_at <= now < plan.valid_until:
        raise ValueError("MONITOR_TRIGGER_SUPERSEDED_OR_EXPIRED")
    lineage = await same_condition_lineage(session, plan)
    if any(not compatible_check(item, checked, plan, lineage) for item, checked in pending):
        raise ValueError("MONITOR_PENDING_CONDITION_UNRESOLVED")
    calendar = await load_agent_paper_service_calendar_authority(
        session, now=now, expected_calendar_authority_id=calendar_authority_id
    )
    if not calendar.market_open_at <= now < calendar.market_close_at:
        raise ValueError("MONITOR_MARKET_CLOSED")
    from nexus.monitoring.reconciliation import plan_covered

    if not await plan_covered(session, plan=plan, now=now):
        raise ValueError("MONITOR_TRIGGER_SUPERSEDED_OR_EXPIRED")
    batch, _identities = await validate_agent_paper_candidate_batch(
        session, batch_id=candidate_batch_id, account_id=account_id, as_of=now
    )
    if trigger.instrument_id not in batch.instrument_ids:
        raise ValueError("MONITOR_SCOPE_INVALID")
    authority = await load_quasi_realtime_cycle_authority(
        session,
        account_id=account_id,
        now=now,
        expected_calendar_authority_id=calendar_authority_id,
        expected_universe_snapshot_id=batch.universe_snapshot_id,
    )
    candidates = await prepare_candidates(
        session,
        authority=authority,
        requested_instrument_ids=(trigger.instrument_id,),
        max_instruments=1,
    )
    quote = await read_latest_price(session, instrument_id=trigger.instrument_id, now=now)
    if (
        len(candidates) != 1
        or quote is None
        or quote.quote_id != candidates[0].decision_quote_id
        or evaluate_price(
            PriceReviewCondition.model_validate(plan.condition),
            quote,
            instrument_id=trigger.instrument_id,
            now=now,
        ).result
        == "unknown"
    ):
        raise ValueError("MONITOR_CURRENT_INPUTS_UNAVAILABLE")
    # Separate finite subscription calls from USD budget; unknown never refunds calls.
    used, prior_limit = (
        await session.execute(
            select(
                func.coalesce(func.sum(MonitorIncrement.reserved_sdk_calls), 0),
                func.min(MonitorIncrement.daily_sdk_call_limit),
            ).where(
                MonitorIncrement.account_id == account_id,
                MonitorIncrement.session_date == authority.decision_session,
            )
        )
    ).one()
    limit = daily_sdk_call_limit if prior_limit is None else min(daily_sdk_call_limit, prior_limit)
    if used + 3 > limit:
        raise ValueError("MONITOR_DAILY_CALL_LIMIT")
    parent = await session.get(AgentResearchRunRecord, plan.source_run_id)
    output = await session.get(AgentResearchOutputRecord, plan.source_output_id)
    if (
        parent is None
        or output is None
        or output.intent is None
        or parent.model_config_id != model_config_id
    ):
        raise ValueError("MONITOR_SOURCE_INVALID")
    projection = await load_paper_projection(
        session, account_id=account_id, as_of=now, derived_snapshots_known_at=now
    )
    position = next(
        (p for p in projection.positions if p.instrument_id == trigger.instrument_id), None
    )
    if projection.nav <= 0:
        raise ValueError("MONITOR_CURRENT_INPUTS_UNAVAILABLE")
    with localcontext() as arithmetic:
        arithmetic.prec = 38
        weight = (
            Decimal(0)
            if position is None
            else position.quantity * position.reference_close / projection.nav
        )
        cash = (projection.available_cash + projection.reserved_cash) / projection.nav
    context = MonitorResearchContext(
        account_id=account_id,
        instrument_id=trigger.instrument_id,
        parent_run_id=parent.id,
        parent_output_id=output.id,
        parent_output_hash=output.content_hash,
        parent_intent=ResearchIntent.model_validate(output.intent),
        plan_id=plan.id,
        plan_generation=plan.generation,
        decision_time=now,
        decision_quote=quote,
        candidate=candidates[0],
        authority=authority,
        trigger_checks=tuple(
            TriggerCheck(
                trigger_id=item.id,
                check_id=checked.id,
                cause=cast(Literal["initial_match", "crossed"], item.cause),
                checked_at=checked.checked_at,
                observation=PriceObservation.model_validate(checked.observation),
            )
            for item, checked in pending
        ),
        paper_portfolio=AgentPaperPortfolioContext(
            candidate_position_weight=str(weight),
            cash_weight=str(cash),
            projection_hash=projection.projection_hash,
        ),
    )
    snapshot = await _freeze_increment_snapshot(session, context)
    request_hash = job_input_hash(
        {
            "purpose": context.purpose,
            "trigger_id": str(trigger.id),
            "snapshot_id": str(snapshot.id),
            "context_hash": hashlib.sha256(content(context)).hexdigest(),
        }
    )
    admitted = await admit_research_run(
        session,
        actor_id=f"monitor:{account_id}",
        route="research-monitor/admit",
        idempotency_key=f"monitor:{trigger.id}",
        request_hash=request_hash,
        request=ResearchRequest(
            instrument_id=trigger.instrument_id, data_snapshot_id=snapshot.id, decision_time=now
        ),
        prompt_version_id=parent.prompt_version_id,
        model_config_id=parent.model_config_id,
        daily_limit_usd=daily_limit_usd,
        now=now,
        job_type=JobType.AGENT_PAPER_RESEARCH,
        formal_stage_capacity=True,
        monitor_increment=True,
    )
    if admitted.replayed or admitted.run.prompt_hash != parent.prompt_hash:
        raise ValueError("MONITOR_VERSION_BINDING_INVALID")
    job = await session.scalar(
        select(Job).where(Job.payload["run_id"].astext == str(admitted.run.id))
    )
    if job is None:
        raise ValueError("MONITOR_INCREMENT_BINDING_INVALID")
    member = MonitorIncrement(
        account_id=account_id,
        instrument_id=trigger.instrument_id,
        trigger_id=trigger.id,
        plan_id=plan.id,
        run_id=admitted.run.id,
        job_id=job.id,
        snapshot_id=snapshot.id,
        research_as_of=now,
        session_date=authority.decision_session,
        reserved_sdk_calls=3,
        daily_sdk_call_limit=limit,
        context_hash=hashlib.sha256(content(context)).hexdigest(),
        manifest_hash="0" * 64,
    )
    member.manifest_hash = increment_manifest_hash(member, admitted.run, job)
    session.add(member)
    for item, _ in pending:
        item.status, item.reason, item.research_run_id = "admitted", None, admitted.run.id
    await session.flush()
    await require_increment_binding(session, admitted.run)
    from nexus.audit.service import append_audit_event

    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.increment_admitted",
        schema_version="monitor_increment_admission_v1",
        occurred_at=now,
        payload={
            "trigger_id": str(trigger.id),
            "run_id": str(member.run_id),
            "manifest_hash": member.manifest_hash,
            "reserved_sdk_calls": 3,
        },
    )
    from nexus.agents.research_stages.routes import register_new_prepared_runs

    await register_new_prepared_runs(session, (member.run_id,))
    return member


async def _freeze_increment_snapshot(
    session: AsyncSession, context: MonitorResearchContext | MixedMonitorContext
) -> DataSnapshot:
    import hashlib
    import json

    from nexus.monitoring.context import SOURCE, content
    from nexus.monitoring.mixed_context import SOURCE as MIXED_SOURCE

    source = MIXED_SOURCE if isinstance(context, MixedMonitorContext) else SOURCE
    purpose = context.purpose

    from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
    from nexus.persistence.models.data import DataSnapshot
    from nexus.persistence.models.research import ResearchEvidenceRef

    base = await session.get(DataSnapshot, context.candidate.research_snapshot_id)
    if base is None:
        raise ValueError("MONITOR_CURRENT_INPUTS_UNAVAILABLE")
    rows = tuple(
        await session.scalars(
            select(AgentDataSnapshotEvidenceRecord).where(
                AgentDataSnapshotEvidenceRecord.data_snapshot_id == base.id,
                AgentDataSnapshotEvidenceRecord.instrument_id == context.instrument_id,
                AgentDataSnapshotEvidenceRecord.authority_kind == "research",
                AgentDataSnapshotEvidenceRecord.available_at <= context.decision_time,
            )
        )
    )
    if not rows or len(rows) >= 100:
        raise ValueError("MONITOR_EVIDENCE_SCOPE_INVALID")
    raw = content(context)
    digest = hashlib.sha256(raw).hexdigest()
    evidence = ResearchEvidenceRef(
        source=source,
        provider_object_id=str(context.trigger_checks[0].trigger_id),
        revision=digest,
        available_at=context.decision_time,
        content_hash=digest,
        byte_length=len(raw),
        location=f"memory://sha256/{digest}",
    )
    session.add(evidence)
    await session.flush()
    payload = {
        k: v
        for k, v in base.payload.items()
        if k not in {"portfolio_context", "agent_paper_context"}
    }
    payload.update(
        purpose=purpose,
        monitor_context=context.model_dump(mode="json"),
        evidence_ids=[str(row.evidence_ref_id) for row in rows] + [str(evidence.id)],
    )
    snapshot = DataSnapshot(
        source=source,
        provider_object_id=str(context.instrument_id),
        revision=hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest(),
        observed_at=context.decision_time,
        available_at=context.decision_time,
        received_at=context.decision_time,
        schema_version=purpose,
        payload=payload,
    )
    session.add(snapshot)
    await session.flush()
    for row in rows:
        session.add(
            AgentDataSnapshotEvidenceRecord(
                data_snapshot_id=snapshot.id,
                instrument_id=context.instrument_id,
                evidence_ref_id=row.evidence_ref_id,
                authority_kind="research",
                selected_at=None,
                source=row.source,
                provider_object_id=row.provider_object_id,
                revision=row.revision,
                available_at=row.available_at,
                location=row.location,
                content_hash=row.content_hash,
            )
        )
    session.add(
        AgentDataSnapshotEvidenceRecord(
            data_snapshot_id=snapshot.id,
            instrument_id=context.instrument_id,
            evidence_ref_id=evidence.id,
            authority_kind="research",
            selected_at=None,
            source=evidence.source,
            provider_object_id=evidence.provider_object_id,
            revision=evidence.revision,
            available_at=evidence.available_at,
            location=evidence.location,
            content_hash=evidence.content_hash,
        )
    )
    await session.flush()
    return snapshot


async def _legacy_monitor_job_finished(
    session: AsyncSession,
    *,
    run: AgentResearchRunRecord,
    job: Job | None,
    leases: tuple[JobLease, ...],
    now: datetime,
    rejected_before_claim: bool,
) -> bool:
    """Native completion classification, separate from usage and audit recording."""
    from nexus.jobs.repository import job_lease_consumed

    return not (
        job is None
        or run.status not in {"completed", "failed", "abstained"}
        or (job.status not in {"succeeded", "failed"} and not rejected_before_claim)
        or len(leases) != 1
        or not await job_lease_consumed(session, job=job, lease=leases[0], now=now)
    )


async def finish_increment(
    session: AsyncSession, *, account_id: UUID, trigger_id: UUID, now: datetime
) -> MonitorTrigger:
    """Record original native completion only; unknown never releases any reservation."""
    from nexus.audit.service import append_audit_event
    from nexus.domain.time import require_utc
    from nexus.monitoring.storage import lock_security
    from nexus.paper.target_service import lock_portfolio_account
    from nexus.persistence.models.agents import AgentResearchOutputRecord

    now = require_utc(now)
    await lock_portfolio_account(session, account_id)
    trigger = await session.get(MonitorTrigger, trigger_id)
    if trigger is None or trigger.account_id != account_id:
        raise ValueError("MONITOR_TRIGGER_UNAVAILABLE")
    await lock_security(session, account_id=account_id, instrument_id=trigger.instrument_id)
    await session.refresh(trigger, with_for_update=True)
    if trigger.research_run_id is None:
        raise ValueError("MONITOR_INCREMENT_UNAVAILABLE")
    run = await session.get(AgentResearchRunRecord, trigger.research_run_id)
    if run is None:
        raise ValueError("MONITOR_INCREMENT_BINDING_INVALID")
    member = await require_increment_binding(session, run)
    trigger = await session.get(MonitorTrigger, member.trigger_id)
    assert trigger is not None
    if trigger.status in {"completed", "failed", "unknown"}:
        return trigger
    job = await session.get(Job, member.job_id)
    leases = tuple(await session.scalars(select(JobLease).where(JobLease.job_id == member.job_id)))
    from nexus.monitoring.preclaim import preclaim_rejection_qualified

    rejected_before_claim = (
        job is not None
        and job.status == "skipped"
        and await preclaim_rejection_qualified(session, run=run, now=now)
    )
    if not await _monitor_job_finished(
        session,
        run=run,
        job=job,
        leases=leases,
        now=now,
        rejected_before_claim=rejected_before_claim,
    ):
        return trigger
    assert job is not None
    reservation = await session.scalar(
        select(AgentDailyBudgetReservationRecord).where(
            AgentDailyBudgetReservationRecord.run_id == run.id
        )
    )
    known = rejected_before_claim or (
        reservation is not None
        and reservation.state == "settled"
        and run.settled_cost_usd is not None
        and reservation.settled_usd == run.settled_cost_usd
        and run.input_tokens is not None
        and run.output_tokens is not None
    )
    if not known:
        trigger.status, trigger.reason = "unknown", "MONITOR_UNKNOWN_USAGE"
    elif run.status == "completed" and job.status == "succeeded":
        output = await session.scalar(
            select(AgentResearchOutputRecord).where(
                AgentResearchOutputRecord.run_id == run.id,
                AgentResearchOutputRecord.status == "completed",
            )
        )
        if output is None:
            raise ValueError("MONITOR_OUTPUT_UNAVAILABLE")
        trigger.status, trigger.reason = (
            "completed",
            ("MONITOR_EVENT_REVIEW_UNRESOLVED" if member.batch_id is not None else None),
        )
    else:
        trigger.status, trigger.reason = (
            "failed",
            ("MONITOR_CAPACITY_REJECTED" if rejected_before_claim else "MONITOR_RESEARCH_FAILED"),
        )
    # One native outcome applies to exactly the frozen group, atomically.
    with session.no_autoflush:
        members = tuple(
            await session.scalars(
                select(MonitorTrigger).where(MonitorTrigger.research_run_id == run.id)
            )
        )
        for item in members:
            item.status, item.reason = trigger.status, trigger.reason
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action="research.monitor.increment_finished",
        schema_version="monitor_increment_finish_v1",
        occurred_at=now,
        payload={
            "trigger_id": str(trigger.id),
            "run_id": str(run.id),
            "status": trigger.status,
            "reason": trigger.reason,
        },
    )
    await session.flush()
    return trigger


async def _require_monitor_member_available(
    session: AsyncSession, *, member: MonitorIncrement, now: datetime
) -> None:
    from nexus.agents.research_stages.public import _member_available

    await _member_available(session, member=member, now=now)


async def _monitor_job_finished(
    session: AsyncSession,
    *,
    run: AgentResearchRunRecord,
    job: Job | None,
    leases: tuple[JobLease, ...],
    now: datetime,
    rejected_before_claim: bool,
) -> bool:
    from typing import cast

    from nexus.agents.research_stages.public import _finished

    return cast(
        bool,
        await _finished(
            session,
            run=run,
            job=job,
            leases=leases,
            now=now,
            rejected_before_claim=rejected_before_claim,
        ),
    )
