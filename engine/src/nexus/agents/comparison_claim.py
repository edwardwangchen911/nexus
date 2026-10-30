"""Exact native claim for the finite comparison; not an inference entry point."""

from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.agents.persistence import require_reserved_daily_budget
from nexus.agents.research_maintenance import require_research_open
from nexus.agents.research_stages.proof import read_terminal_proof
from nexus.agents.research_stages.protocol import ProtocolRejected, StagedProtocol
from nexus.agents.research_stages.routes import selected_route
from nexus.jobs.repository import ClaimedJob, _claim_selected_job, _worker_id
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import Job, JobAttempt, JobDependency, JobLease


async def claim_comparison_position(
    factory: async_sessionmaker[AsyncSession], *, experiment_id: UUID, ordinal: int, worker_id: str
) -> ClaimedJob:
    """Return only the first committed native claim, never recover send rights.

    A claim is not a stage ticket. The runner must check legitimate transport
    authorization before calling and still use the native stage protocol before
    every send. This function does not obtain credentials or invoke a provider.
    """
    require_research_open()
    worker_id = _worker_id(worker_id)
    if type(ordinal) is not int or not 1 <= ordinal <= 12:
        raise ValueError("COMPARISON_POSITION_INVALID")
    protocol = StagedProtocol(factory)
    async with factory() as session:
        await session.begin()
        if (
            await session.scalar(text("SELECT to_regclass('research_comparison_chain_stop')"))
            is None
        ):
            raise ValueError("COMPARISON_CHAIN_STOP_SCHEMA_REQUIRED")
        scope = (
            (
                await session.execute(
                    text("""SELECT comparison_id,stopped
          FROM research_stage_scope WHERE id=:id FOR UPDATE"""),
                    {"id": experiment_id},
                )
            )
            .mappings()
            .one_or_none()
        )
        if scope is None or scope["comparison_id"] != experiment_id:
            raise ValueError("COMPARISON_REGISTRATION_REQUIRED")
        if scope["stopped"]:
            raise ValueError("COMPARISON_STOPPED")
        rows = (
            (
                await session.execute(
                    text("""SELECT ordinal,run_id,job_id
          FROM research_comparison_chain WHERE experiment_id=:id AND ordinal<=:ordinal
          ORDER BY ordinal"""),
                    {"id": experiment_id, "ordinal": ordinal},
                )
            )
            .mappings()
            .all()
        )
        if [r["ordinal"] for r in rows] != list(range(1, ordinal + 1)):
            raise ValueError("COMPARISON_PREDECESSOR_UNCONFIRMED")
        now = await session.scalar(text("SELECT clock_timestamp()"))
        for prior in rows[:-1]:
            try:
                proof = await read_terminal_proof(session, run_id=prior["run_id"], now=now)
            except ProtocolRejected:
                raise ValueError("COMPARISON_PREDECESSOR_UNCONFIRMED") from None
            if proof is None or not proof.known or not proof.cleanup_confirmed or proof.orphan:
                raise ValueError("COMPARISON_PREDECESSOR_UNCONFIRMED")
        job = await session.get(Job, rows[-1]["job_id"], with_for_update=True)
        if job is None or job.status != "queued" or job.attempt_count != 0:
            raise ValueError("COMPARISON_POSITION_CONSUMED")
        route = await selected_route(session, job.id)
        if route is None or route["kind"] != "comparison" or route["scope_id"] != experiment_id:
            raise ValueError("COMPARISON_NATIVE_BINDING_INVALID")
        run = await session.get(AgentResearchRunRecord, rows[-1]["run_id"])
        if run is None or run.status != "prepared" or job.priority != 0:
            raise ValueError("COMPARISON_NATIVE_BINDING_INVALID")
        model = await session.get(AgentModelConfigRecord, run.model_config_id)
        budget = await require_reserved_daily_budget(session, run_id=run.id)
        for record in (JobAttempt, JobLease, JobDependency):
            if await session.scalar(select(record.id).where(record.job_id == job.id)) is not None:
                raise ValueError("COMPARISON_POSITION_CONSUMED")
        now = await session.scalar(text("SELECT clock_timestamp()"))
        if (
            model is None
            or not model.pricing_available_at <= now < model.pricing_expires_at
            or budget.business_date != now.date()
            or job.available_at > now
        ):
            raise ValueError("COMPARISON_CLAIM_WINDOW_INVALID")
        await session.execute(
            text("SELECT set_config('nexus.stage_protocol','research-stages-v1',true)")
        )
        await session.execute(
            text("SELECT set_config('nexus.comparison_position',:position,true)"),
            {"position": f"{experiment_id}:{ordinal}"},
        )
        claim = await _claim_selected_job(
            session, job=job, worker_id=worker_id, now=now, lease_seconds=330
        )
        await protocol._commit(session, experiment_id)
    return claim
