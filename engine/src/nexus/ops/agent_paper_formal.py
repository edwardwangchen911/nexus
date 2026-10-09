"""Read-only host binding of explicit configuration to frozen formal cycles."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.agents.codex_formal_stage import require_formal_stage_model_profile
from nexus.jobs.models import JobType
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.paper.agent_cycle import _require_cycle_candidate_profile
from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord, AgentResearchRunRecord
from nexus.persistence.models.paper import AgentPaperCycleEventRecord, AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job
from nexus.settings import AgentRuntimeMode, EngineSettings

_CODE = "CODEX_FORMAL_HOST_PROFILE_INVALID"


def require_formal_host_configuration_match(
    original: EngineSettings, loaded: EngineSettings
) -> None:
    if (
        original.agent_paper_formal_model_config_id is not None
        or loaded.agent_paper_formal_model_config_id is not None
    ) and (
        original.agent_paper_formal_model_config_id != loaded.agent_paper_formal_model_config_id
        or original.database_url != loaded.database_url
        or original.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK
        or loaded.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK
    ):
        raise ValueError(_CODE)


@asynccontextmanager
async def _read_session(settings: EngineSettings) -> AsyncIterator[AsyncSession]:
    _validate_local_database(settings.database_url)
    assert settings.database_url is not None
    engine = create_async_engine(settings.database_url)
    try:
        async with async_sessionmaker(engine)() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            if (
                await session.scalar(select(func.current_database()))
                != make_url(settings.database_url).database
            ):
                raise ValueError(_CODE)
            yield session
    finally:
        await engine.dispose()


async def require_formal_host_trigger_profile(settings: EngineSettings, *, now: datetime) -> None:
    model_id = settings.agent_paper_formal_model_config_id
    if model_id is None:
        return
    if settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK:
        raise ValueError(_CODE)
    async with _read_session(settings) as session:
        await require_formal_stage_model_profile(session, model_config_id=model_id, now=now)


async def resolve_formal_host_cycle_profile(
    settings: EngineSettings,
    *,
    account_id: UUID,
    cycle_id: UUID,
    research_job_id: UUID | None = None,
) -> bool:
    async with _read_session(settings) as session:
        cycle = await session.get(AgentPaperCycleRecord, cycle_id)
        first = await session.scalar(
            select(AgentPaperCycleEventRecord).where(
                AgentPaperCycleEventRecord.cycle_id == cycle_id,
                AgentPaperCycleEventRecord.sequence == 1,
            )
        )
        if cycle is None or cycle.account_id != account_id or first is None:
            raise ValueError(_CODE)
        if not {
            "formal_stage_capacity",
            "formal_model_config_id",
            "formal_model_hash",
        }.intersection(first.fact_ids):
            return False
        model_id = settings.agent_paper_formal_model_config_id
        if model_id is None or settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK:
            raise ValueError(_CODE)
        await _require_cycle_candidate_profile(
            session,
            cycle=cycle,
            candidate_stage_capacity=False,
            formal_stage_capacity=True,
            expected_formal_model_id=model_id,
            now=cycle.decision_time,
        )
        if research_job_id is not None:
            job = await session.get(Job, research_job_id)
            try:
                run_id = UUID(str(job.payload["run_id"])) if job is not None else None
            except (KeyError, TypeError, ValueError):
                raise ValueError(_CODE) from None
            if job is None or run_id is None:
                raise ValueError(_CODE)
            if job.job_type == JobType.AGENT_PAPER_RESEARCH.value:
                research = await session.get(AgentResearchRunRecord, run_id)
                if (
                    research is None
                    or research.model_config_id != model_id
                    or research.idempotency_key
                    != f"agent-paper:{cycle.id}:{research.instrument_id}"
                ):
                    raise ValueError(_CODE)
            elif job.job_type == JobType.AGENT_PORTFOLIO_SYNTHESIS.value:
                synthesis = await session.get(AgentPortfolioSynthesisRunRecord, run_id)
                if (
                    synthesis is None
                    or synthesis.cycle_id != cycle.id
                    or synthesis.model_config_id != model_id
                ):
                    raise ValueError(_CODE)
            else:
                raise ValueError(_CODE)
        return True
