"""Close only a proved zero-call capacity rejection through native cancellation."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_versions import (
    BOUNDED_RESEARCH_LEASE_SECONDS,
    BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
)
from nexus.agents.execution import _terminal_without_usage
from nexus.agents.persistence import require_reserved_daily_budget, try_lock_budget_day
from nexus.audit.service import append_audit_event
from nexus.domain.time import require_utc
from nexus.jobs.models import QueueName
from nexus.jobs.repository import claim_next_job, finalize_job, job_lease_consumed
from nexus.ops.agent_capacity_rejection import CAPACITY_REJECTION_CODES
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchEventRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.monitoring import MonitorIncrement, MonitorTrigger
from nexus.persistence.models.runtime import AuditEvent, Job, JobAttempt, JobLease
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings

ACTION = "research.monitor.capacity_rejected"
RESULT = "MONITOR_CAPACITY_REJECTED"
REASON = "monitor_capacity_preclaim_rejected"
INVALID = "MONITOR_PRECLAIM_CLOSE_UNCONFIRMED"


def _payload(member: MonitorIncrement, code: str) -> dict[str, object]:
    return {
        "trigger_id": str(member.trigger_id),
        "run_id": str(member.run_id),
        "job_id": str(member.job_id),
        "manifest_hash": member.manifest_hash,
        "code": code,
        "initial_run_status": "prepared",
        "initial_job_status": "queued",
        "initial_attempt_count": 0,
        "initial_lease_count": 0,
        "initial_output_count": 0,
        "initial_usage": None,
        "initial_reservation_state": "reserved",
        "reserved_sdk_calls": 3,
    }


async def preclaim_rejection_qualified(
    session: AsyncSession, *, run: AgentResearchRunRecord, now: datetime
) -> bool:
    """Recognize only this exact native skipped cancellation; ordinary skipped is not proof."""
    from nexus.agents.actionable_research import _hash
    from nexus.monitoring.admission import require_increment_binding

    now = require_utc(now)
    if run.status != "abstained" or (run.settled_cost_usd, run.input_tokens, run.output_tokens) != (
        Decimal(0),
        0,
        0,
    ):
        return False
    try:
        member = await require_increment_binding(session, run)
    except ValueError:
        return False
    job = await session.get(Job, member.job_id)
    leases = tuple(
        await session.scalars(select(JobLease).where(JobLease.job_id == member.job_id).limit(2))
    )
    attempts = tuple(
        await session.scalars(select(JobAttempt).where(JobAttempt.job_id == member.job_id).limit(2))
    )
    reservation = await session.scalar(
        select(AgentDailyBudgetReservationRecord).where(
            AgentDailyBudgetReservationRecord.run_id == run.id
        )
    )
    outputs = tuple(
        await session.scalars(
            select(AgentResearchOutputRecord)
            .where(AgentResearchOutputRecord.run_id == run.id)
            .limit(2)
        )
    )
    proofs = tuple(
        await session.scalars(
            select(AuditEvent)
            .where(AuditEvent.action == ACTION, AuditEvent.payload["run_id"].astext == str(run.id))
            .limit(2)
        )
    )
    if (
        job is None
        or job.status != "skipped"
        or job.result_code != RESULT
        or job.attempt_count != 1
        or len(leases) != 1
        or len(attempts) != 1
        or leases[0].worker_id != f"monitor-preclaim:{member.trigger_id}"
        or not await job_lease_consumed(session, job=job, lease=leases[0], now=now)
        or reservation is None
        or reservation.state != "released"
        or reservation.settled_usd is not None
        or reservation.pricing_version != run.pricing_version
        or reservation.reserved_usd != run.reserved_cost_usd
        or len(outputs) != 1
        or len(proofs) != 1
    ):
        return False
    output, proof = outputs[0], proofs[0]
    code = proof.payload.get("code")
    return bool(
        isinstance(code, str)
        and code in CAPACITY_REJECTION_CODES
        and proof.actor_id == "research-monitor"
        and proof.schema_version == "monitor_preclaim_v1"
        and proof.payload == _payload(member, code)
        and proof.occurred_at == job.finished_at
        and run.actor_id == f"monitor:{member.account_id}"
        and run.idempotency_key == f"monitor:{member.trigger_id}"
        and output.role is None
        and output.status == "abstained"
        and output.intent is None
        and output.payload == {"reason": REASON}
        and output.content_hash
        == _hash(
            {
                "status": output.status,
                "role": output.role,
                "payload": output.payload,
                "intent": output.intent,
            }
        )
    )


async def close_capacity_rejected_increment_once(
    settings: EngineSettings, *, account_id: UUID, trigger_id: UUID, job_id: UUID, code: str
) -> dict[str, object]:
    """Called only for this dispatch's deterministic preclaim capacity error."""
    from nexus.agents.codex_formal_stage import formal_stage_job_qualified
    from nexus.monitoring.admission import finish_increment, require_increment_binding
    from nexus.monitoring.storage import lock_security
    from nexus.ops.agent_paper_once import _validate_local_database
    from nexus.paper.target_service import lock_portfolio_account
    from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD, SCHEMA_ADMISSION_LOCK_KEY

    if (
        code not in CAPACITY_REJECTION_CODES
        or account_id != settings.agent_paper_account_id
        or settings.agent_paper_formal_model_config_id is None
    ):
        raise ValueError(INVALID)
    _validate_local_database(settings.database_url)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET LOCAL lock_timeout='2s'"))
            await session.execute(
                text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key, 0))"),
                {"key": SCHEMA_ADMISSION_LOCK_KEY},
            )
            if (
                await session.scalar(text("SELECT version_num FROM alembic_version"))
                != CURRENT_MIGRATION_HEAD
            ):
                raise ValueError(INVALID)
            await lock_portfolio_account(session, account_id)
            trigger = await session.get(MonitorTrigger, trigger_id)
            if (
                trigger is None
                or trigger.kind != "price"
                or trigger.account_id != account_id
                or trigger.research_run_id is None
            ):
                raise ValueError(INVALID)
            await lock_security(session, account_id=account_id, instrument_id=trigger.instrument_id)
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise ValueError(INVALID)
            run = await session.get(AgentResearchRunRecord, trigger.research_run_id)
            if run is None:
                raise ValueError(INVALID)
            member = await require_increment_binding(session, run)
            if (
                member.job_id != job_id
                or run.model_config_id != settings.agent_paper_formal_model_config_id
            ):
                raise ValueError(INVALID)
            if not await preclaim_rejection_qualified(session, run=run, now=now):
                reservation = await session.scalar(
                    select(AgentDailyBudgetReservationRecord).where(
                        AgentDailyBudgetReservationRecord.run_id == run.id
                    )
                )
                if reservation is None or not await try_lock_budget_day(
                    session, reservation.business_date.isoformat()
                ):
                    raise ValueError(INVALID)
                locked = await session.scalar(
                    select(AgentResearchRunRecord)
                    .where(AgentResearchRunRecord.id == run.id)
                    .with_for_update(skip_locked=True)
                    .execution_options(populate_existing=True)
                )
                job = await session.scalar(
                    select(Job)
                    .where(Job.id == job_id)
                    .with_for_update(skip_locked=True)
                    .execution_options(populate_existing=True)
                )
                if (
                    locked is None
                    or job is None
                    or trigger.status != "admitted"
                    or run.status != "prepared"
                    or run.retry_count != 0
                    or any(
                        x is not None
                        for x in (run.settled_cost_usd, run.input_tokens, run.output_tokens)
                    )
                    or job.status != "queued"
                    or job.attempt_count != 0
                    or job.manual_retry_count != 0
                    or run.actor_id != f"monitor:{account_id}"
                    or run.idempotency_key != f"monitor:{trigger_id}"
                    or not await formal_stage_job_qualified(session, job)
                ):
                    raise ValueError(INVALID)
                for model, field, value in (
                    (JobLease, JobLease.job_id, job.id),
                    (JobAttempt, JobAttempt.job_id, job.id),
                    (AgentResearchOutputRecord, AgentResearchOutputRecord.run_id, run.id),
                ):
                    if await session.scalar(select(model.id).where(field == value).limit(1)):
                        raise ValueError(INVALID)
                events = tuple(
                    await session.scalars(
                        select(AgentResearchEventRecord).where(
                            AgentResearchEventRecord.run_id == run.id
                        )
                    )
                )
                dispatches = tuple(
                    await session.scalars(
                        select(AuditEvent)
                        .where(
                            AuditEvent.action == "research.monitor.worker_dispatched",
                            AuditEvent.payload["job_id"].astext == str(job.id),
                        )
                        .limit(2)
                    )
                )
                if (
                    any(event.state != "prepared" for event in events)
                    or len(dispatches) != 1
                    or dispatches[0].actor_id != "research-monitor"
                    or dispatches[0].schema_version != "monitor_dispatch_v1"
                    or dispatches[0].payload
                    != {"trigger_id": str(trigger_id), "job_id": str(job_id)}
                    or not run.decision_time <= dispatches[0].occurred_at <= now
                ):
                    raise ValueError(INVALID)
                reservation = await require_reserved_daily_budget(session, run_id=run.id)
                if (
                    reservation.settled_usd is not None
                    or reservation.reserved_usd != run.reserved_cost_usd
                    or reservation.pricing_version != run.pricing_version
                ):
                    raise ValueError(INVALID)
                claim = await claim_next_job(
                    session,
                    queue=QueueName.GENERAL,
                    worker_id=f"monitor-preclaim:{trigger_id}",
                    now=now,
                    lease_seconds=BOUNDED_RESEARCH_LEASE_SECONDS
                    if run.pricing_version == BOUNDED_STAGE_BUDGET_CONFIG_VERSION
                    else 210,
                    job_ids=(job.id,),
                    job_types=("agent.paper.research",),
                    locked_job_guard=formal_stage_job_qualified,
                    research_budget_profile=run.pricing_version,
                )
                if claim is None:
                    raise ValueError(INVALID)
                await _terminal_without_usage(
                    session,
                    run,
                    output_sequence=1,
                    event_sequence=len(events) + 1,
                    status="abstained",
                    reason=REASON,
                )
                await finalize_job(
                    session, claim=claim, now=now, outcome="skipped", result_code=RESULT
                )
                await append_audit_event(
                    session,
                    actor_id="research-monitor",
                    action=ACTION,
                    schema_version="monitor_preclaim_v1",
                    payload=_payload(member, code),
                    occurred_at=now,
                )
            proof = await session.scalar(
                select(AuditEvent).where(
                    AuditEvent.action == ACTION, AuditEvent.payload["run_id"].astext == str(run.id)
                )
            )
            if (
                proof is None
                or proof.payload.get("code") != code
                or not await preclaim_rejection_qualified(session, run=run, now=now)
            ):
                raise ValueError(INVALID)
            finished = await finish_increment(
                session, account_id=account_id, trigger_id=trigger_id, now=now
            )
            if finished.status != "failed" or finished.reason != RESULT:
                raise ValueError(INVALID)
            return {
                "status": "failed",
                "reason": RESULT,
                "trigger_id": str(trigger_id),
                "run_id": str(run.id),
                "job_id": str(job_id),
                "execution_locked": True,
            }
    finally:
        await factory.kw["bind"].dispose()
