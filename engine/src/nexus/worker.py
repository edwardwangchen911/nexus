"""Bounded database-backed research worker composition root."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from uuid import UUID

from openai import AsyncOpenAI
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_versions import (
    ACTIONABLE_CONFIG_VERSION,
    INLINE_CITATION_CONFIG_VERSION,
    PORTFOLIO_DECISION_ABSOLUTE_TIMEOUT_SECONDS,
    SDK_CONFIG_VERSIONS,
    STAGED_CONFIG_VERSIONS,
    STRUCTURED_CONFIG_VERSIONS,
    aggregate_input_limit,
    wire_byte_limit,
)
from nexus.agents.committee import (
    AgentRole,
    CommitteeModel,
    CommitteeRoleOutput,
    FixtureCommittee,
    PromptAsset,
)
from nexus.agents.database_tools import database_read_only_tools
from nexus.agents.execution import TERMINAL, execute_research_run
from nexus.agents.execution_authority import load_frozen_evidence, load_frozen_prompts
from nexus.agents.models import ResearchRequest
from nexus.agents.portfolio_decision_model import (
    BoundPortfolioResponsesClient,
    CodexPortfolioResponsesClient,
)
from nexus.agents.portfolio_decision_worker import (
    run_portfolio_decision_worker_once as _run_portfolio_decision_worker_once,
)
from nexus.agents.price_authority import (
    FrozenPriceAuthorityStatus,
    revalidate_frozen_price_authority,
)
from nexus.agents.responses import ResponsesClient, ResponsesCommittee, ResponsesModelConfig
from nexus.agents.tools import ReadOnlyToolRegistry, ToolInputError
from nexus.audit.manifest import S3AuditManifestObjectStore
from nexus.diagnostics.artifacts import ArtifactByteReader
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
from nexus.jobs.worker import PreclaimGuard, UnsafeJobError, run_job_worker_once
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.optimizer.jobs import build_optimization_registry
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
_PORTFOLIO_ONCE_WORKER_ID = f"portfolio-decision-once-{new_uuid7()}"
_PORTFOLIO_ONCE_STARTED_AT = datetime.now(UTC)


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
    job_ids: tuple[UUID, ...] | None = None,
    preclaim_guard: PreclaimGuard | None = None,
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
        job_ids=job_ids,
        preclaim_guard=preclaim_guard,
    )


def _openai_portfolio_responses_client(
    settings: EngineSettings,
) -> BoundPortfolioResponsesClient:
    """Bind the paid Responses transport to the exact locally registered authority."""
    from nexus.ops.openai_local import _CONFIG as OPENAI_LOCAL_CONFIG

    if settings.openai_api_key is None:
        raise ValueError("OPENAI_API_KEY_MISSING")
    native_client = AsyncOpenAI(
        api_key=settings.openai_api_key.get_secret_value(),
        base_url="https://api.openai.com/v1",
        max_retries=0,
        timeout=45,
    )
    return BoundPortfolioResponsesClient(
        client=native_client,
        config=ResponsesModelConfig(
            model_id=str(OPENAI_LOCAL_CONFIG["model_id"]),
            pricing_version=str(OPENAI_LOCAL_CONFIG["version"]),
            max_input_tokens=int(OPENAI_LOCAL_CONFIG["max_input_tokens"]),
            max_output_tokens=int(OPENAI_LOCAL_CONFIG["max_output_tokens"]),
            max_tool_calls=1,
            absolute_timeout_seconds=PORTFOLIO_DECISION_ABSOLUTE_TIMEOUT_SECONDS,
            input_usd_per_million=Decimal(
                str(OPENAI_LOCAL_CONFIG["input_usd_per_million"])
            ),
            output_usd_per_million=Decimal(
                str(OPENAI_LOCAL_CONFIG["output_usd_per_million"])
            ),
        ),
    )


async def run_portfolio_decision_worker_once(
    settings: EngineSettings,
    *,
    responses_client: object | None = None,
    job_ids: tuple[UUID, ...] | None = None,
) -> bool:
    """Run the dedicated no-retry portfolio decision worker once."""
    runtime_mode = _require_worker_runtime(settings)
    client = responses_client
    if client is None:
        if runtime_mode is AgentRuntimeMode.CODEX_SDK:
            from nexus.agents.codex_sdk import MODEL_ID
            from nexus.agents.codex_versions import PORTFOLIO_DECISION_CONFIG_VERSION

            client = CodexPortfolioResponsesClient(
                model_id=MODEL_ID,
                pricing_version=PORTFOLIO_DECISION_CONFIG_VERSION,
            )
        elif runtime_mode is AgentRuntimeMode.RESPONSES and settings.openai_api_key is not None:
            client = _openai_portfolio_responses_client(settings)
        else:
            return False
    return await _run_portfolio_decision_worker_once(
        session_factory(settings),
        responses_client=cast(ResponsesClient, client),
        worker_id=_PORTFOLIO_ONCE_WORKER_ID,
        worker_started_at=_PORTFOLIO_ONCE_STARTED_AT,
        process_version=_PROCESS_VERSION,
        migration_head=_MIGRATION_HEAD,
        clock=lambda: datetime.now(UTC),
        scenario_evidence_store=S3ProviderEvidenceStore(settings),
        job_ids=job_ids,
    )


async def run_worker(settings: EngineSettings | None = None, *, run_forever: bool = False) -> None:
    """Validate fixture startup in tests, or poll durable jobs in the worker process."""
    configured = settings or EngineSettings()
    _require_worker_runtime(configured)
    if run_forever and configured.agent_runtime_mode is AgentRuntimeMode.CODEX_SDK:
        raise ValueError("Codex SDK research requires an explicit run-once invocation")
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
    portfolio_worker_id = f"portfolio-decision-{new_uuid7()}"
    started_at = datetime.now(UTC)
    portfolio_client: ResponsesClient | None = None
    scenario_evidence_store = S3ProviderEvidenceStore(configured)
    if (
        configured.agent_runtime_mode is AgentRuntimeMode.RESPONSES
        and configured.openai_api_key is not None
    ):
        portfolio_client = _openai_portfolio_responses_client(configured)
    while True:
        now = datetime.now(UTC)
        async with factory() as session, session.begin():
            await schedule_due_daily_pipeline(
                session,
                now=now,
                profile_id=LOCAL_SAFE_DAILY_PROFILE,
                market_source=configured.market_provider,
            )
        portfolio_worked = False
        if portfolio_client is not None:
            portfolio_worked = await _run_portfolio_decision_worker_once(
                factory,
                responses_client=portfolio_client,
                worker_id=portfolio_worker_id,
                worker_started_at=started_at,
                process_version=_PROCESS_VERSION,
                migration_head=_MIGRATION_HEAD,
                clock=lambda: datetime.now(UTC),
                scenario_evidence_store=scenario_evidence_store,
            )
        general_worked = await run_job_worker_once(
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
        if not portfolio_worked and not general_worked:
            await asyncio.sleep(0.5)


def _general_registry(
    settings: EngineSettings,
    *,
    responses_client: object | None,
    tools: ReadOnlyToolRegistry | None,
    artifact_reader: ArtifactByteReader | None = None,
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
    optimization = build_optimization_registry(
        artifact_reader=(
            artifact_reader
            if artifact_reader is not None
            else S3ProviderEvidenceStore(settings).read_normalized
        ),
    )
    return JobRegistry((*agent.specs, *daily.specs, *optimization.specs))


def _agent_registry(
    settings: EngineSettings,
    *,
    responses_client: object | None,
    tools: ReadOnlyToolRegistry | None,
) -> JobRegistry:
    runtime_mode = _require_worker_runtime(settings)
    allow_fixture_evidence = (
        settings.environment is not Environment.PRODUCTION
        and runtime_mode is not AgentRuntimeMode.CODEX_SDK
    )
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
                deadline=context.monotonic_deadline(),
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
                timeout_seconds=120 if runtime_mode is AgentRuntimeMode.CODEX_SDK else 45,
                retry_policy=RetryPolicy.AUTOMATIC_SAFE,
                max_attempts=1 if runtime_mode is AgentRuntimeMode.CODEX_SDK else 3,
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
    deadline: float | None = None,
) -> CommitteeModel:
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    if model is not None and model.version == "codex-sdk-strategy-rnd-v1":
        return _UnavailableResponsesCommittee()
    if (
        model is not None
        and model.version in SDK_CONFIG_VERSIONS
        and settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK
    ):
        return _UnavailableResponsesCommittee()
    if settings.agent_runtime_mode is AgentRuntimeMode.FIXTURE:
        return FixtureCommittee()
    allow_fixture_evidence = (
        settings.environment is not Environment.PRODUCTION
        and settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK
    )
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
    if settings.agent_runtime_mode is AgentRuntimeMode.CODEX_SDK:
        from nexus.agents.codex_sdk import (
            MODEL_ID,
            STAGED_CONFIG_VERSION,
            CodexCommittee,
        )
        from nexus.agents.codex_staged import StagedCodexCommittee, StructuredStagedCodexCommittee

        prompts = await load_frozen_prompts(session, run)
        if (
            model is None
            or model.model_id != MODEL_ID
            or model.version not in SDK_CONFIG_VERSIONS
            or model.version != run.pricing_version
            or model.config_hash != run.model_hash
            or model.max_input_tokens != aggregate_input_limit(model.version)
            or model.max_output_tokens != 8192
            or model.input_usd_per_million != 0
            or model.output_usd_per_million != 0
            or prompts is None
        ):
            return _UnavailableResponsesCommittee()
        committee_type = (
            StructuredStagedCodexCommittee
            if model.version in STRUCTURED_CONFIG_VERSIONS
            else StagedCodexCommittee
            if model.version == STAGED_CONFIG_VERSION
            else CodexCommittee
        )
        return committee_type(
            prompts={
                role: PromptAsset(
                    role=role,
                    version=item.version,
                    content=item.content,
                    content_hash=item.content_hash,
                )
                for role, item in prompts.items()
            },
            tools=tools
            or await database_read_only_tools(
                session,
                run,
                allow_fixture_evidence=False,
                allow_fixture_price_evidence=False,
            ),
            **(
                {"inline_citations": True}
                if model.version in {INLINE_CITATION_CONFIG_VERSION, ACTIONABLE_CONFIG_VERSION}
                else {}
            ),
            **(
                {
                    "deadline": deadline,
                    "aggregate_input_tokens": model.max_input_tokens,
                    "max_wire_bytes": wire_byte_limit(model.version),
                }
                if model.version in STAGED_CONFIG_VERSIONS
                else {}
            ),
        )
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
