"""Attach native research to a completed, explicitly frozen manual diagnosis."""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSIONS, PAPER_SCOPE_CONFIG_VERSIONS
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
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.runtime import Job, JobDependency

_ACTOR = "local-daily-research"
_ROUTE = "manual-daily-research-v1"
_BINDING_ERROR = "MANUAL_DAILY_RESEARCH_BINDING_INVALID"
_AGENT_PAPER_ACTOR = "local-agent-paper-cycle"
_AGENT_PAPER_ROUTE = "agent-paper-cycle-v1"
AGENT_VERSION_INCUMBENT_ACTOR = "local-agent-version-incumbent"
AGENT_VERSION_CHALLENGER_ACTOR = "local-agent-version-challenger"
_AGENT_VERSION_ROUTE = "agent-version-evaluation-v1"


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


async def admit_agent_paper_research(
    session: AsyncSession,
    *,
    cycle_id: UUID,
    instrument_id: UUID,
    research_snapshot_id: UUID,
    research_as_of: datetime,
    daily_limit_usd: Decimal,
    now: datetime,
    actor_id: str = _AGENT_PAPER_ACTOR,
    admission_idempotency_key: str | None = None,
    version_source_run_id: UUID | None = None,
    prompt_version_id: UUID | None = None,
    model_config_id: UUID | None = None,
) -> ResearchAdmission:
    """Admit one already-frozen candidate through the native research Job path."""
    evaluation_actor = actor_id in {
        AGENT_VERSION_INCUMBENT_ACTOR,
        AGENT_VERSION_CHALLENGER_ACTOR,
    }
    explicit_version = prompt_version_id is not None or model_config_id is not None
    if evaluation_actor:
        if (
            admission_idempotency_key is None
            or (prompt_version_id is None) != (model_config_id is None)
            or (version_source_run_id is None) == (not explicit_version)
        ):
            raise ValueError("Agent version authority is ambiguous")
    elif (
        actor_id != _AGENT_PAPER_ACTOR
        or admission_idempotency_key is not None
        or version_source_run_id is not None
        or explicit_version
    ):
        raise ValueError("Agent version authority is ambiguous")

    route = _AGENT_VERSION_ROUTE if evaluation_actor else _AGENT_PAPER_ROUTE
    idempotency_key = admission_idempotency_key or f"agent-paper:{cycle_id}:{instrument_id}"
    request_payload: dict[str, object] = {
        "schema": route,
        "cycle_id": str(cycle_id),
        "instrument_id": str(instrument_id),
        "research_snapshot_id": str(research_snapshot_id),
        "research_as_of": research_as_of.isoformat(),
    }
    if evaluation_actor:
        request_payload.update(
            {
                "actor_id": actor_id,
                "version_source_run_id": (
                    str(version_source_run_id) if version_source_run_id is not None else None
                ),
                "prompt_version_id": (
                    str(prompt_version_id) if prompt_version_id is not None else None
                ),
                "model_config_id": (str(model_config_id) if model_config_id is not None else None),
            }
        )
    request_hash = job_input_hash(request_payload)
    reuse_latest_actor_agent_version = not evaluation_actor
    reuse_latest_actor_prompts_only = False
    if not evaluation_actor:
        from nexus.paper.agent_version_evaluation import (
            _model_is_agent_paper_compatible,
            load_selected_internal_agent_version,
        )

        selected = await load_selected_internal_agent_version(session, now=now)
        if selected is None:
            model_config_id = await session.scalar(
                select(AgentModelConfigRecord.id)
                .where(
                    AgentModelConfigRecord.version.in_(ACTIONABLE_CONFIG_VERSIONS),
                    AgentModelConfigRecord.pricing_available_at <= now,
                    AgentModelConfigRecord.pricing_expires_at > now,
                )
                .order_by(
                    AgentModelConfigRecord.created_at.desc(),
                    AgentModelConfigRecord.id.desc(),
                )
                .limit(1)
            )
            model = (
                await session.get(AgentModelConfigRecord, model_config_id)
                if model_config_id is not None
                else None
            )
            reuse_latest_actor_prompts_only = (
                model is not None
                and model.version in PAPER_SCOPE_CONFIG_VERSIONS
                and _model_is_agent_paper_compatible(model)
                and model.max_tool_calls == 0
            )
        else:
            reuse_latest_actor_agent_version = False
            version_source_run_id = selected.source_run_id
            model_config_id = None
    return await admit_research_run(
        session,
        actor_id=actor_id,
        route=route,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        request=ResearchRequest(
            instrument_id=instrument_id,
            data_snapshot_id=research_snapshot_id,
            decision_time=research_as_of,
        ),
        prompt_version_id=prompt_version_id,
        model_config_id=model_config_id,
        daily_limit_usd=daily_limit_usd,
        now=now,
        job_type=JobType.AGENT_PAPER_RESEARCH,
        reuse_latest_actor_agent_version=reuse_latest_actor_agent_version,
        reuse_latest_actor_prompts_only=reuse_latest_actor_prompts_only,
        version_source_run_id=version_source_run_id,
    )
