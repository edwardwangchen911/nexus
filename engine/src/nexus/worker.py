"""Bounded database-backed research worker composition root."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from uuid import UUID

from openai import AsyncOpenAI
from pydantic import BaseModel, ConfigDict, Field
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
from nexus.agents.execution_authority import load_frozen_evidence
from nexus.agents.models import ResearchRequest
from nexus.agents.price_authority import (
    FrozenPriceAuthorityStatus,
    revalidate_frozen_price_authority,
)
from nexus.agents.responses import ResponsesClient, ResponsesCommittee, ResponsesModelConfig
from nexus.agents.tools import ReadOnlyToolRegistry, ToolInputError
from nexus.audit.manifest import S3AuditManifestObjectStore
from nexus.domain.evidence import EvidenceRef
from nexus.domain.ids import new_uuid7
from nexus.jobs.daily_local import (
    LOCAL_SAFE_DAILY_PROFILE,
    build_local_daily_pipeline_registry,
)
from nexus.jobs.daily_pipeline import (
    assert_daily_pipeline_schema_ready,
    schedule_due_daily_pipeline,
)
from nexus.jobs.models import JobType, QueueName, RetryPolicy
from nexus.jobs.registry import (
    JobHandlerContext,
    JobHandlerResult,
    JobHandlerSpec,
    JobRegistry,
)
from nexus.jobs.worker import UnsafeJobError, run_job_worker_once
from nexus.persistence.migrations import (
    CURRENT_GENERAL_WORKER_PROCESS_VERSION,
    CURRENT_MIGRATION_HEAD,
)
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import AgentRuntimeMode, EngineSettings, Environment

_JOB_SCHEMA = "agent_research_job_v1"
_RUN_TO_JOB_STATUS = {
    "completed": "succeeded",
    "abstained": "skipped",
    "failed": "failed",
}
_PROCESS_VERSION = CURRENT_GENERAL_WORKER_PROCESS_VERSION
_MIGRATION_HEAD = CURRENT_MIGRATION_HEAD
_ONCE_WORKER_ID = f"agent-once-{new_uuid7()}"
_ONCE_STARTED_AT = datetime.now(UTC)


class _AgentResearchJobPayload(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    run_id: str = Field(
        pattern=(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
            r"[0-9a-f]{4}-[0-9a-f]{12}$"
        )
    )


async def run_agent_worker_once(
    settings: EngineSettings,
    *,
    responses_client: object | None = None,
    tools: ReadOnlyToolRegistry | None = None,
) -> bool:
    """Claim and terminalize at most one queued Agent job outside any HTTP request."""
    _require_worker_runtime(settings)
    factory = session_factory(settings)
    return await run_job_worker_once(
        factory,
        registry=_agent_registry(
            settings,
            responses_client=responses_client,
            tools=tools,
        ),
        queue=QueueName.GENERAL,
        worker_id=_ONCE_WORKER_ID,
        worker_started_at=_ONCE_STARTED_AT,
        process_version=_PROCESS_VERSION,
        migration_head=_MIGRATION_HEAD,
        clock=lambda: datetime.now(UTC),
        claim_registered_only=True,
    )


async def run_worker(settings: EngineSettings | None = None, *, run_forever: bool = False) -> None:
    """Validate fixture startup in tests, or poll durable jobs in the worker process."""
    configured = settings or EngineSettings()
    _require_worker_runtime(configured)
    if not run_forever:
        await asyncio.sleep(0)
        return
    factory = session_factory(configured)
    registry = _general_registry(
        configured,
        responses_client=None,
        tools=None,
    )
    worker_id = f"agent-general-{new_uuid7()}"
    started_at = datetime.now(UTC)
    while True:
        now = datetime.now(UTC)
        async with factory() as session, session.begin():
            await schedule_due_daily_pipeline(
                session,
                now=now,
                profile_id=LOCAL_SAFE_DAILY_PROFILE,
                market_source=configured.market_provider,
            )
        worked = await run_job_worker_once(
            factory,
            registry=registry,
            queue=QueueName.GENERAL,
            worker_id=worker_id,
            worker_started_at=started_at,
            process_version=_PROCESS_VERSION,
            migration_head=_MIGRATION_HEAD,
            clock=lambda: datetime.now(UTC),
            claim_registered_only=True,
            preclaim_guard=assert_daily_pipeline_schema_ready,
        )
        if not worked:
            await asyncio.sleep(0.5)


def _general_registry(
    settings: EngineSettings,
    *,
    responses_client: object | None,
    tools: ReadOnlyToolRegistry | None,
) -> JobRegistry:
    agent = _agent_registry(
        settings,
        responses_client=responses_client,
        tools=tools,
    )
    daily = build_local_daily_pipeline_registry(
        audit_object_store=S3AuditManifestObjectStore(settings),
        include_provider_stages=settings.provider_jobs_enabled,
    )
    return JobRegistry((*agent.specs, *daily.specs))


def _agent_registry(
    settings: EngineSettings,
    *,
    responses_client: object | None,
    tools: ReadOnlyToolRegistry | None,
) -> JobRegistry:
    runtime_mode = _require_worker_runtime(settings)
    allow_fixture_evidence = settings.environment is not Environment.PRODUCTION
    allow_fixture_price_authority = runtime_mode is AgentRuntimeMode.FIXTURE

    async def handle(context: JobHandlerContext) -> JobHandlerResult:
        payload = cast(_AgentResearchJobPayload, context.payload)
        run = await context.session.get(
            AgentResearchRunRecord,
            UUID(payload.run_id),
            with_for_update=True,
        )
        if run is None:
            raise UnsafeJobError("RUN_NOT_FOUND")
        if run.status in TERMINAL:
            return _agent_result(run.status)
        if run.status != "prepared":
            raise UnsafeJobError("RUN_STATE_INVALID")
        try:
            committee = await _committee(
                context.session,
                settings,
                run,
                responses_client=responses_client,
                tools=tools,
            )
        except ToolInputError:
            committee = _UnavailableResponsesCommittee()
        result = await execute_research_run(
            context.session,
            run_id=run.id,
            committee=committee,
            allow_fixture_evidence=allow_fixture_evidence,
            allow_fixture_price_authority=allow_fixture_price_authority,
        )
        return _agent_result(result.status)

    return JobRegistry(
        (
            JobHandlerSpec(
                job_type=JobType.AGENT_RESEARCH,
                schema_version=_JOB_SCHEMA,
                payload_model=_AgentResearchJobPayload,
                queue=QueueName.GENERAL,
                timeout_seconds=45,
                retry_policy=RetryPolicy.AUTOMATIC_SAFE,
                max_attempts=3,
                idempotency_namespace="agent.research",
                handler=handle,
            ),
        )
    )


def _agent_result(status: str) -> JobHandlerResult:
    mapped = _RUN_TO_JOB_STATUS.get(status)
    if mapped == "succeeded":
        return JobHandlerResult(outcome="succeeded", result_code="AGENT_COMPLETED")
    if mapped == "skipped":
        return JobHandlerResult(outcome="skipped", result_code="AGENT_ABSTAINED")
    if mapped == "failed":
        return JobHandlerResult(
            outcome="failed",
            result_code="AGENT_FAILED",
            error_code="AGENT_FAILED",
        )
    raise UnsafeJobError("RUN_STATE_INVALID")


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
    allow_fixture_evidence = settings.environment is not Environment.PRODUCTION
    evidence_records = await load_frozen_evidence(
        session,
        run,
        allow_fixture_evidence=allow_fixture_evidence,
    )
    if not evidence_records:
        return _UnavailableResponsesCommittee()
    price_authority_status = await revalidate_frozen_price_authority(
        session,
        run=run,
        evidence_records=evidence_records,
        allow_fixture=False,
    )
    if price_authority_status is FrozenPriceAuthorityStatus.INVALID:
        raise ToolInputError("frozen price authority is unavailable")
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    if (
        model is None
        or settings.openai_reasoning_model != model.model_id
        or settings.openai_api_key is None
    ):
        return _UnavailableResponsesCommittee()
    client = responses_client
    if client is None:
        client = AsyncOpenAI(
            api_key=settings.openai_api_key.get_secret_value(),
            base_url="https://api.openai.com/v1",
            max_retries=0,
            timeout=30,
        )
    configured_tools = tools or await database_read_only_tools(
        session,
        run,
        allow_fixture_evidence=allow_fixture_evidence,
        allow_fixture_price_evidence=(settings.agent_runtime_mode is AgentRuntimeMode.FIXTURE),
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
