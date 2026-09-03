"""Attach native research to a completed, explicitly frozen manual diagnosis."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import ResearchAdmission, admit_research_run
from nexus.agents.portfolio_context import read_portfolio_context
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.jobs.daily_manual import (
    ManualDailyPayload,
    ManualDailyRequest,
    _root,
    manual_daily_job_ids,
)
from nexus.jobs.models import JobType, QueueName, job_input_hash
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.runtime import Job, JobDependency

_ACTOR = "local-daily-research"
_ROUTE = "manual-daily-research-v1"
_BINDING_ERROR = "MANUAL_DAILY_RESEARCH_BINDING_INVALID"


async def _diagnosis(
    session: AsyncSession,
    root_id: UUID,
    as_of: datetime,
) -> tuple[ManualDailyPayload, UUID, UUID]:
    ids = await manual_daily_job_ids(session, root_id=root_id)
    _, payload = await _root(session, root_id)
    if len(ids) != 3 or payload.cutoff_at > as_of:
        raise ValueError("MANUAL_DAILY_RESEARCH_PARENT_NOT_READY")
    for job_id in ids:
        job = await session.get(Job, job_id)
        if (
            job is None
            or job.status != "succeeded"
            or job.finished_at is None
            or job.finished_at > as_of
        ):
            raise ValueError("MANUAL_DAILY_RESEARCH_PARENT_NOT_READY")
    report = await session.scalar(
        select(PortfolioDiagnosticReportRecord).where(
            PortfolioDiagnosticReportRecord.portfolio_snapshot_id == payload.snapshot_id,
            PortfolioDiagnosticReportRecord.portfolio_id == payload.portfolio_id,
            PortfolioDiagnosticReportRecord.snapshot_as_of == payload.snapshot_as_of,
            PortfolioDiagnosticReportRecord.policy_id == REAL_ADVISORY_POLICY_ID,
            PortfolioDiagnosticReportRecord.created_at <= as_of,
            PortfolioDiagnosticReportRecord.optimization_need != "BLOCKED",
        )
    )
    if report is None:
        raise ValueError("MANUAL_DAILY_RESEARCH_DIAGNOSIS_UNAVAILABLE")
    return payload, ids[-1], report.id


def _request_hash(
    root_id: UUID,
    diagnostic_id: UUID,
    instrument_id: UUID,
    as_of: datetime,
    model_config_id: UUID,
) -> str:
    return job_input_hash(
        {
            "schema": _ROUTE,
            "root_id": str(root_id),
            "diagnostic_job_id": str(diagnostic_id),
            "instrument_id": str(instrument_id),
            "research_as_of": as_of.isoformat(),
            "model_config_id": str(model_config_id),
        }
    )


async def _native_job(session: AsyncSession, run_id: UUID) -> Job:
    jobs = tuple(
        await session.scalars(
            select(Job)
            .where(
                Job.job_type == JobType.AGENT_RESEARCH.value,
                Job.payload["run_id"].as_string() == str(run_id),
            )
            .limit(2)
        )
    )
    if (
        len(jobs) != 1
        or jobs[0].schema_version != "agent_research_job_v1"
        or jobs[0].queue != QueueName.GENERAL.value
        or jobs[0].payload != {"run_id": str(run_id)}
    ):
        raise ValueError(_BINDING_ERROR)
    return jobs[0]


async def resolve_daily_research_job(
    session: AsyncSession,
    *,
    root_id: UUID,
    run_id: UUID,
) -> Job:
    """Resolve one exact native job without claiming, repairing, or changing it."""
    run = await session.get(AgentResearchRunRecord, run_id)
    if run is None or run.actor_id != _ACTOR:
        raise ValueError(_BINDING_ERROR)
    payload, diagnostic_id, report_id = await _diagnosis(session, root_id, run.decision_time)
    expected_hash = _request_hash(
        root_id,
        diagnostic_id,
        run.instrument_id,
        run.decision_time,
        run.model_config_id,
    )
    context = await read_portfolio_context(
        session,
        run.data_snapshot_id,
        run.instrument_id,
        run.decision_time,
    )
    job = await _native_job(session, run_id)
    dependencies = tuple(
        (
            await session.execute(
                select(
                    JobDependency.depends_on_job_id,
                    JobDependency.required_outcome,
                ).where(JobDependency.job_id == job.id)
            )
        ).all()
    )
    # The run stores the caller request hash; the native job additionally binds
    # its immutable data/model/prompt/tool authority. They are not the same hash.
    durable_hash = job_input_hash(
        {
            "instrument_id": str(run.instrument_id),
            "data_snapshot_id": str(run.data_snapshot_id),
            "decision_time": run.decision_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "request_hash": run.input_hash,
            "schema_hash": run.schema_hash,
            "model_hash": run.model_hash,
            "prompt_hash": run.prompt_hash,
            "evidence_hash": run.evidence_hash,
            "tool_hash": run.tool_hash,
            "pricing_version": run.pricing_version,
        }
    )
    if (
        run.idempotency_key != expected_hash
        or run.input_hash != expected_hash
        or context is None
        or context.portfolio_snapshot_id != payload.snapshot_id
        or context.snapshot_as_of != payload.snapshot_as_of
        or context.research_as_of != run.decision_time
        or context.diagnostic_run_id != report_id
        or job.scope_kind != "instrument"
        or job.scope_id != str(run.instrument_id)
        or job.effective_at != run.decision_time
        or job.input_hash != durable_hash
        or dependencies != ((diagnostic_id, "succeeded"),)
    ):
        raise ValueError(_BINDING_ERROR)
    return job


async def admit_daily_research(
    session: AsyncSession,
    *,
    root_id: UUID,
    instrument_id: UUID,
    research_as_of: datetime,
    model_config_id: UUID,
    daily_limit_usd: Decimal,
    now: datetime,
) -> ResearchAdmission:
    """One transaction: exact portfolio context, native admission, and dependency.

    This neither calls a model nor admits work before diagnosis succeeds. Failures
    roll back newly prepared authority even if the caller catches the exception.
    Replays require the original dependency; historical jobs are never patched.
    """
    try:
        ManualDailyRequest._utc(research_as_of)
        ManualDailyRequest._utc(now)
    except ValueError as error:
        raise ValueError("MANUAL_DAILY_RESEARCH_TIME_INVALID") from error
    if research_as_of > now:
        raise ValueError("MANUAL_DAILY_RESEARCH_TIME_INVALID")
    payload, diagnostic_id, report_id = await _diagnosis(session, root_id, research_as_of)
    request_hash = _request_hash(
        root_id,
        diagnostic_id,
        instrument_id,
        research_as_of,
        model_config_id,
    )
    async with session.begin_nested():
        snapshot = await prepare_research_snapshot(
            session,
            instrument_id,
            as_of=research_as_of,
            portfolio_snapshot_id=payload.snapshot_id,
        )
        if snapshot is None:
            raise ValueError("MANUAL_DAILY_RESEARCH_EVIDENCE_UNAVAILABLE")
        context = await read_portfolio_context(session, snapshot.id, instrument_id, research_as_of)
        if context is None or context.diagnostic_run_id != report_id:
            raise ValueError("MANUAL_DAILY_RESEARCH_DIAGNOSIS_UNAVAILABLE")
        admission = await admit_research_run(
            session,
            actor_id=_ACTOR,
            route=_ROUTE,
            idempotency_key=request_hash,
            request_hash=request_hash,
            request=ResearchRequest(
                instrument_id=instrument_id,
                data_snapshot_id=snapshot.id,
                decision_time=research_as_of,
            ),
            prompt_version_id=None,
            model_config_id=model_config_id,
            daily_limit_usd=daily_limit_usd,
            now=now,
        )
        if not admission.replayed:
            job = await _native_job(session, admission.run_id)
            session.add(
                JobDependency(
                    job_id=job.id,
                    depends_on_job_id=diagnostic_id,
                    required_outcome="succeeded",
                )
            )
            await session.flush()
        await resolve_daily_research_job(session, root_id=root_id, run_id=admission.run_id)
        return admission
