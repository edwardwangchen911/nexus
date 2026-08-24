"""Bounded database-backed research worker composition root."""

import asyncio
from decimal import Decimal
from typing import cast
from uuid import UUID

from openai import AsyncOpenAI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.committee import (
    AgentRole,
    CommitteeModel,
    CommitteeRoleOutput,
    FixtureCommittee,
    PromptAsset,
)
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.execution import TERMINAL, execute_research_run
from nexus.agents.models import ResearchRequest
from nexus.agents.responses import ResponsesClient, ResponsesCommittee, ResponsesModelConfig
from nexus.agents.tools import ReadOnlyToolRegistry, ToolInputError
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.settings import AgentRuntimeMode, EngineSettings, Environment

_JOB_SCHEMA = "agent_research_job_v1"


async def run_agent_worker_once(
    settings: EngineSettings,
    *,
    responses_client: object | None = None,
    tools: ReadOnlyToolRegistry | None = None,
) -> bool:
    """Claim and terminalize at most one queued Agent job outside any HTTP request."""
    _require_worker_runtime(settings)
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
        try:
            committee = await _committee(
                session,
                settings,
                run,
                responses_client=responses_client,
                tools=tools,
            )
        except ToolInputError:
            committee = _UnavailableResponsesCommittee()
        result = await execute_research_run(
            session,
            run_id=run.id,
            committee=committee,
            allow_fixture_evidence=settings.environment is not Environment.PRODUCTION,
        )
        job.status = result.status.upper()
        return True


async def run_worker(settings: EngineSettings | None = None, *, run_forever: bool = False) -> None:
    """Validate fixture startup in tests, or poll durable jobs in the worker process."""
    configured = settings or EngineSettings()
    _require_worker_runtime(configured)
    if not run_forever:
        await asyncio.sleep(0)
        return
    while True:
        worked = await run_agent_worker_once(configured)
        if not worked:
            await asyncio.sleep(0.5)


async def _committee(
    session: AsyncSession,
    settings: EngineSettings,
    run: AgentResearchRunRecord,
    *,
    responses_client: object | None,
    tools: ReadOnlyToolRegistry | None,
) -> CommitteeModel:
    if settings.agent_runtime_mode is AgentRuntimeMode.FIXTURE:
        return FixtureCommittee()
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    if (
        model is None
        or settings.openai_reasoning_model != model.model_id
        or settings.openai_api_key is None
    ):
        return _UnavailableResponsesCommittee()
    client = responses_client
    if client is None:
        client = AsyncOpenAI(api_key=settings.openai_api_key.get_secret_value())
    configured_tools = tools or await database_read_only_tools(
        session,
        run,
        allow_fixture_evidence=settings.environment is not Environment.PRODUCTION,
    )
    return ResponsesCommittee(
        client=cast(ResponsesClient, client),
        config=ResponsesModelConfig(
            model_id=model.model_id,
            pricing_version=model.version,
            max_input_tokens=model.max_input_tokens,
            max_output_tokens=model.max_output_tokens,
            max_tool_calls=model.max_tool_calls,
            absolute_timeout_seconds=Decimal("30"),
            input_usd_per_million=model.input_usd_per_million,
            output_usd_per_million=model.output_usd_per_million,
        ),
        tools=configured_tools,
    )


class _UnavailableResponsesCommittee:
    """Terminalize a job when its frozen model authority cannot be composed."""

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


def _require_worker_runtime(settings: EngineSettings) -> AgentRuntimeMode:
    mode = settings.agent_runtime_mode
    if mode is None or (
        settings.environment is Environment.PRODUCTION and mode is not AgentRuntimeMode.RESPONSES
    ):
        raise RuntimeError("production Agent worker requires explicit responses runtime")
    return mode
