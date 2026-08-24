"""Bounded database-backed research worker composition root."""

import asyncio
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.committee import (
    AgentRole,
    CommitteeModel,
    CommitteeRoleOutput,
    FixtureCommittee,
    PromptAsset,
)
from nexus.agents.execution import TERMINAL, execute_research_run
from nexus.agents.models import ResearchRequest
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import MacroObservation
from nexus.persistence.models.research import CorporateActionRecord, Filing, ResearchEvidenceRef
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.settings import AgentRuntimeMode, EngineSettings

_JOB_SCHEMA = "agent_research_job_v1"


async def run_agent_worker_once(settings: EngineSettings) -> bool:
    """Claim and terminalize at most one queued Agent job outside any HTTP request."""
    factory = session_factory(settings)
    async with factory() as session, session.begin():
        job = await session.scalar(
            select(Job)
            .where(Job.schema_version == _JOB_SCHEMA, Job.status == "QUEUED")
            .order_by(Job.created_at, Job.id)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        if job is None:
            return False
        job.status = "RUNNING"
        raw_run_id = job.payload.get("run_id")
        if not isinstance(raw_run_id, str):
            job.status = "FAILED"
            return True
        try:
            run_id = UUID(raw_run_id)
        except ValueError:
            job.status = "FAILED"
            return True
        run = await session.get(AgentResearchRunRecord, run_id, with_for_update=True)
        if run is None:
            job.status = "FAILED"
            return True
        if run.status in TERMINAL:
            job.status = run.status.upper()
            return True
        if run.status != "prepared":
            job.status = "FAILED"
            return True
        evidence_ids = await _evidence_ids(session, run, settings.agent_runtime_mode)
        result = await execute_research_run(
            session,
            run_id=run.id,
            committee=_committee(settings.agent_runtime_mode),
            evidence_ref_ids=evidence_ids,
        )
        job.status = result.status.upper()
        return True


async def run_worker(
    settings: EngineSettings | None = None, *, run_forever: bool = False
) -> None:
    """Validate fixture startup in tests, or poll durable jobs in the worker process."""
    configured = settings or EngineSettings()
    if not run_forever:
        await asyncio.sleep(0)
        return
    while True:
        worked = await run_agent_worker_once(configured)
        if not worked:
            await asyncio.sleep(0.5)


async def _evidence_ids(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    mode: AgentRuntimeMode,
) -> tuple[UUID, ...]:
    if mode is AgentRuntimeMode.FIXTURE:
        return tuple(
            await session.scalars(
                select(ResearchEvidenceRef.id)
                .where(
                    ResearchEvidenceRef.source == "fixture",
                    ResearchEvidenceRef.available_at <= run.decision_time,
                )
                .order_by(ResearchEvidenceRef.available_at.desc(), ResearchEvidenceRef.id)
                .limit(100)
            )
        )
    owned: set[UUID] = set(
        await session.scalars(
            select(Filing.evidence_ref_id)
            .where(Filing.instrument_id == run.instrument_id)
            .limit(100)
        )
    )
    owned.update(
        await session.scalars(
            select(CorporateActionRecord.evidence_ref_id)
            .where(CorporateActionRecord.instrument_id == run.instrument_id)
            .limit(100)
        )
    )
    owned.update(
        evidence_id
        for evidence_id in await session.scalars(
            select(MacroObservation.evidence_ref_id)
            .where(
                MacroObservation.authority_version == "research_macro_v1",
                MacroObservation.evidence_ref_id.is_not(None),
            )
            .limit(100)
        )
        if evidence_id is not None
    )
    if not owned:
        return ()
    return tuple(
        await session.scalars(
            select(ResearchEvidenceRef.id)
            .where(
                ResearchEvidenceRef.id.in_(owned),
                ResearchEvidenceRef.available_at <= run.decision_time,
            )
            .order_by(ResearchEvidenceRef.available_at.desc(), ResearchEvidenceRef.id)
            .limit(100)
        )
    )


def _committee(mode: AgentRuntimeMode) -> CommitteeModel:
    if mode is AgentRuntimeMode.FIXTURE:
        return FixtureCommittee()
    return _FailClosedResponsesCommittee()


class _FailClosedResponsesCommittee:
    """Keep configured non-fixture jobs terminal and closed without widening permissions."""

    async def analyze(
        self,
        role: AgentRole,
        request: ResearchRequest,
        evidence_refs: tuple[EvidenceRef, ...],
        prompt: PromptAsset,
    ) -> CommitteeRoleOutput:
        del role, request, evidence_refs, prompt
        raise RuntimeError("configured Responses committee is unavailable")


def main() -> None:
    """Run the durable worker process entry point."""
    asyncio.run(run_worker(run_forever=True))
