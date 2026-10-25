"""Bounded database-backed research worker composition root."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from typing import cast
from uuid import UUID

from openai import AsyncOpenAI
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.codex_versions import (
    ACTIONABLE_CONFIG_VERSIONS,
    BOUNDED_RESEARCH_HANDLER_TIMEOUT_SECONDS,
    BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
    CANDIDATE_BUDGET_CONFIG_VERSION,
    CANDIDATE_RESEARCH_HANDLER_TIMEOUT_SECONDS,
    CANDIDATE_RESEARCH_JOB_TIMEOUT_SECONDS,
    CAPACITY_PRECLAIM_TIMEOUT_SECONDS,
    DEADLINE_BUDGET_CONFIG_VERSION,
    FORMAL_RESEARCH_HANDLER_TIMEOUT_SECONDS,
    FORMAL_RESEARCH_JOB_TIMEOUT_SECONDS,
    FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
    FULL_INPUT_CAPACITY_CONFIG_VERSION,
    FULL_INPUT_CAPACITY_MAX_WIRE_BYTES,
    INLINE_CITATION_CONFIG_VERSION,
    ISOLATED_CAPACITY_CONFIG_VERSIONS,
    PAPER_SCOPE_CONFIG_VERSIONS,
    PORTFOLIO_DECISION_ABSOLUTE_TIMEOUT_SECONDS,
    RESEARCH_SDK_HANDLER_TIMEOUT_SECONDS,
    RESEARCH_STAGE_JOB_TIMEOUT_SECONDS,
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
from nexus.jobs.repository import LockedJobGuard
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
from nexus.persistence.models.runtime import Job
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


async def run_portfolio_synthesis_worker_once(
    settings: EngineSettings,
    *,
    job_ids: tuple[UUID, ...] = (),
    candidate_stage_capacity: bool = False,
    formal_stage_capacity: bool = False,
) -> bool:
    """Explicit one-shot synthesis using frozen Codex authority and no retry."""
    from nexus.agents.portfolio_synthesis_worker import (
        CodexSynthesisTransport,
        run_synthesis_job_once,
    )

    if type(formal_stage_capacity) is not bool or (
        formal_stage_capacity
        and (
            candidate_stage_capacity is not False
            or not isinstance(job_ids, tuple)
            or len(job_ids) != 1
            or not isinstance(job_ids[0], UUID)
            or settings.database_url is None
            or _require_worker_runtime(settings) is not AgentRuntimeMode.CODEX_SDK
            or make_url(settings.database_url).host not in {"127.0.0.1", "localhost", "::1"}
            or bool(
                set(make_url(settings.database_url).query)
                & {"host", "hostaddr", "port", "dsn", "database", "dbname"}
            )
        )
    ):
        raise ValueError("CODEX_FORMAL_WORKER_ARGUMENT_INVALID")
    if (
        type(candidate_stage_capacity) is not bool
        or candidate_stage_capacity
        and (len(job_ids) != 1 or not isinstance(job_ids[0], UUID) or settings.database_url is None)
    ):
        raise ValueError("CODEX_CANDIDATE_WORKER_ARGUMENT_INVALID")
    if _require_worker_runtime(settings) is not AgentRuntimeMode.CODEX_SDK:
        raise ValueError("PORTFOLIO_SYNTHESIS_TRANSPORT_UNSUPPORTED")
    factory = session_factory(settings)
    if candidate_stage_capacity:
        from nexus.agents.codex_candidate import require_candidate_database

        async with factory() as session:
            await require_candidate_database(
                session, configured_database=make_url(cast(str, settings.database_url)).database
            )

    async def formal_database_guard() -> None:
        async with factory() as session:
            actual = await session.scalar(select(func.current_database()))
            if actual != make_url(cast(str, settings.database_url)).database:
                raise ValueError("CODEX_FORMAL_WORKER_DATABASE_INVALID")

    return await run_synthesis_job_once(
        factory,
        transport=CodexSynthesisTransport(formal_stage_capacity=True)
        if formal_stage_capacity
        else CodexSynthesisTransport(candidate_stage_capacity=True)
        if candidate_stage_capacity
        else CodexSynthesisTransport(),
        worker_id=f"portfolio-synthesis-{new_uuid7()}",
        clock=lambda: datetime.now(UTC),
        job_ids=job_ids,
        candidate_stage_capacity=candidate_stage_capacity,
        formal_stage_capacity=formal_stage_capacity,
        preclaim=formal_database_guard if formal_stage_capacity else None,
    )


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
    candidate_stage_capacity: bool = False,
    formal_stage_capacity: bool = False,
    full_input_capacity: bool = False,
) -> bool:
    """Claim and terminalize at most one queued Agent job outside any HTTP request."""
    runtime_mode = _require_worker_runtime(settings)
    job_ids = None if job_ids is None else tuple(job_ids)
    if (
        type(full_input_capacity) is not bool
        or full_input_capacity
        and (
            candidate_stage_capacity
            or formal_stage_capacity
            or runtime_mode is not AgentRuntimeMode.CODEX_SDK
            or job_ids is None
            or len(job_ids) != 1
            or not isinstance(job_ids[0], UUID)
            or settings.database_url is None
            or tools is not None
            or responses_client is not None
        )
    ):
        raise ValueError("CODEX_CAPACITY_WORKER_ARGUMENT_INVALID")
    if (
        type(candidate_stage_capacity) is not bool
        or candidate_stage_capacity
        and (
            runtime_mode is not AgentRuntimeMode.CODEX_SDK
            or job_ids is None
            or len(job_ids) != 1
            or not isinstance(job_ids[0], UUID)
            or settings.database_url is None
        )
    ):
        raise ValueError("CODEX_CANDIDATE_WORKER_ARGUMENT_INVALID")
    if (
        type(formal_stage_capacity) is not bool
        or formal_stage_capacity
        and (
            candidate_stage_capacity
            or runtime_mode is not AgentRuntimeMode.CODEX_SDK
            or job_ids is None
            or len(job_ids) != 1
            or not isinstance(job_ids[0], UUID)
            or settings.database_url is None
            or make_url(settings.database_url).host not in {"127.0.0.1", "localhost", "::1"}
            or bool(
                set(make_url(settings.database_url).query)
                & {"host", "hostaddr", "port", "dsn", "database", "dbname"}
            )
        )
    ):
        raise ValueError("CODEX_FORMAL_WORKER_ARGUMENT_INVALID")
    factory = session_factory(settings)
    locked_guard: LockedJobGuard | None = None
    formal_full_input_capacity = False
    capacity_profile_version = FULL_INPUT_CAPACITY_CONFIG_VERSION
    if full_input_capacity:
        from nexus.agents.codex_candidate import require_candidate_database
        from nexus.agents.codex_capacity import capacity_job_qualified

        async with factory() as session:
            await require_candidate_database(
                session, configured_database=make_url(cast(str, settings.database_url)).database
            )
        async with factory() as session:
            selected_job = await session.get(Job, cast(tuple[UUID, ...], job_ids)[0])
            if selected_job is not None and type(selected_job.payload.get("run_id")) is str:
                selected_run = await session.get(
                    AgentResearchRunRecord, UUID(cast(str, selected_job.payload["run_id"]))
                )
                if selected_run is not None:
                    capacity_profile_version = selected_run.pricing_version
        locked_guard = capacity_job_qualified
        prior_guard = preclaim_guard

        async def guard_complete_pool(session: AsyncSession) -> None:
            if prior_guard is not None:
                await prior_guard(session)
            job = await session.get(Job, cast(tuple[UUID, ...], job_ids)[0], populate_existing=True)
            if job is None or job.status != "queued":
                return
            if not await capacity_job_qualified(session, job):
                raise ValueError("CODEX_CAPACITY_AUTHORITY_INVALID")
            run = await session.get(AgentResearchRunRecord, UUID(cast(str, job.payload["run_id"])))
            if run is None:
                raise ValueError("CODEX_CAPACITY_AUTHORITY_INVALID")
            await _prepare_preclaim_capacity_pool(session, settings, run)

        preclaim_guard = guard_complete_pool
    if candidate_stage_capacity:
        from nexus.agents.codex_candidate import candidate_job_qualified, require_candidate_database

        async with factory() as session:
            await require_candidate_database(
                session,
                configured_database=make_url(cast(str, settings.database_url)).database,
            )
        locked_guard = candidate_job_qualified
    if formal_stage_capacity:
        from nexus.agents.codex_formal_stage import formal_stage_job_qualified

        async with factory() as session:
            actual = await session.scalar(select(func.current_database()))
            if actual != make_url(cast(str, settings.database_url)).database:
                raise ValueError("CODEX_FORMAL_WORKER_DATABASE_INVALID")
            selected = await session.get(Job, cast(tuple[UUID, ...], job_ids)[0])
            if selected is not None and type(selected.payload.get("run_id")) is str:
                selected_run = await session.get(
                    AgentResearchRunRecord, UUID(cast(str, selected.payload["run_id"]))
                )
                formal_full_input_capacity = (
                    selected_run is not None
                    and selected_run.pricing_version in ISOLATED_CAPACITY_CONFIG_VERSIONS
                )
                if formal_full_input_capacity:
                    assert selected_run is not None
                    capacity_profile_version = selected_run.pricing_version
        locked_guard = formal_stage_job_qualified
        if formal_full_input_capacity:
            if tools is not None or responses_client is not None:
                raise ValueError("CODEX_FORMAL_WORKER_ARGUMENT_INVALID")
            prior_formal_guard = preclaim_guard

            async def guard_formal_pool(session: AsyncSession) -> None:
                if prior_formal_guard is not None:
                    await prior_formal_guard(session)
                job = await session.get(
                    Job, cast(tuple[UUID, ...], job_ids)[0], populate_existing=True
                )
                if job is None or job.status != "queued":
                    return
                if not await formal_stage_job_qualified(session, job):
                    raise ValueError("CODEX_FORMAL_AUTHORITY_INVALID")
                run = await session.get(
                    AgentResearchRunRecord, UUID(cast(str, job.payload["run_id"]))
                )
                if run is None:
                    raise ValueError("CODEX_FORMAL_AUTHORITY_INVALID")
                await _prepare_preclaim_capacity_pool(
                    session, settings, run, formal_stage_capacity=True
                )

            preclaim_guard = guard_formal_pool
    return await run_job_worker_once(
        factory,
        registry=_agent_registry(
            settings,
            responses_client=responses_client,
            tools=tools,
            include_agent_paper=job_ids is not None,
            candidate_stage_capacity=candidate_stage_capacity,
            formal_stage_capacity=formal_stage_capacity,
            full_input_capacity=full_input_capacity,
            formal_full_input_capacity=formal_full_input_capacity,
            capacity_profile_version=capacity_profile_version,
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
        locked_job_guard=locked_guard,
    )


class CapacityPreclaimTimeout(TimeoutError):
    """Only the bounded input-only preclaim allowance expired; no claim was made."""


async def _prepare_preclaim_capacity_pool(
    session: AsyncSession,
    settings: EngineSettings,
    run: AgentResearchRunRecord,
    *,
    formal_stage_capacity: bool = False,
) -> None:
    """Bound v11 input-only preparation before any Job claim or SDK call."""
    from nexus.agents.capacity_cycle import prepare_cycle_capacity_pool

    loop = asyncio.get_running_loop()
    deadline = loop.time() + CAPACITY_PRECLAIM_TIMEOUT_SECONDS
    if run.pricing_version != BOUNDED_STAGE_BUDGET_CONFIG_VERSION:
        await prepare_cycle_capacity_pool(
            session,
            settings,
            run,
            deadline=deadline,
            formal_stage_capacity=formal_stage_capacity,
        )
        return
    allowance = asyncio.timeout_at(deadline)
    try:
        async with allowance:
            await prepare_cycle_capacity_pool(
                session,
                settings,
                run,
                deadline=deadline,
                formal_stage_capacity=formal_stage_capacity,
            )
            # A synchronous tokenizer can exhaust the allowance without yielding.
            if loop.time() >= deadline:
                raise CapacityPreclaimTimeout("CODEX_CAPACITY_PRECLAIM_TIMEOUT")
    except TimeoutError as error:
        if allowance.expired():
            raise CapacityPreclaimTimeout("CODEX_CAPACITY_PRECLAIM_TIMEOUT") from error
        raise


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
            input_usd_per_million=Decimal(str(OPENAI_LOCAL_CONFIG["input_usd_per_million"])),
            output_usd_per_million=Decimal(str(OPENAI_LOCAL_CONFIG["output_usd_per_million"])),
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
    include_agent_paper: bool = False,
    candidate_stage_capacity: bool = False,
    formal_stage_capacity: bool = False,
    full_input_capacity: bool = False,
    formal_full_input_capacity: bool = False,
    capacity_profile_version: str = FULL_INPUT_CAPACITY_CONFIG_VERSION,
) -> JobRegistry:
    if capacity_profile_version not in ISOLATED_CAPACITY_CONFIG_VERSIONS or (
        not (full_input_capacity or formal_full_input_capacity)
        and capacity_profile_version != FULL_INPUT_CAPACITY_CONFIG_VERSION
    ):
        raise ValueError("CODEX_CAPACITY_WORKER_ARGUMENT_INVALID")
    runtime_mode = _require_worker_runtime(settings)
    if (
        type(formal_full_input_capacity) is not bool
        or formal_full_input_capacity
        and (
            not formal_stage_capacity
            or full_input_capacity
            or tools is not None
            or responses_client is not None
        )
    ):
        raise ValueError("CODEX_FORMAL_WORKER_ARGUMENT_INVALID")
    if (
        type(full_input_capacity) is not bool
        or full_input_capacity
        and (
            candidate_stage_capacity
            or formal_stage_capacity
            or runtime_mode is not AgentRuntimeMode.CODEX_SDK
            or not include_agent_paper
            or tools is not None
            or responses_client is not None
        )
    ):
        raise ValueError("CODEX_CAPACITY_WORKER_ARGUMENT_INVALID")
    if (
        type(candidate_stage_capacity) is not bool
        or candidate_stage_capacity
        and (runtime_mode is not AgentRuntimeMode.CODEX_SDK or not include_agent_paper)
    ):
        raise ValueError("CODEX_CANDIDATE_WORKER_ARGUMENT_INVALID")
    if (
        type(formal_stage_capacity) is not bool
        or formal_stage_capacity
        and (
            candidate_stage_capacity
            or runtime_mode is not AgentRuntimeMode.CODEX_SDK
            or not include_agent_paper
        )
    ):
        raise ValueError("CODEX_FORMAL_WORKER_ARGUMENT_INVALID")
    allow_fixture_evidence = (
        settings.environment is not Environment.PRODUCTION
        and runtime_mode is not AgentRuntimeMode.CODEX_SDK
    )
    allow_fixture_price_authority = runtime_mode is AgentRuntimeMode.FIXTURE

    async def handle(context: JobHandlerContext) -> JobHandlerResult:
        payload = cast(_AgentResearchJobPayload, context.payload)
        from nexus.agents.research_dispatch import current_dispatch

        dispatch = current_dispatch(UUID(payload.run_id))
        if dispatch is not None:
            dispatch.bind_claim(UUID(payload.run_id), context.claim)
        run = await context.session.get(
            AgentResearchRunRecord,
            UUID(payload.run_id),
            with_for_update=True,
        )
        if run is None:
            raise UnsafeJobError("RUN_NOT_FOUND")
        if candidate_stage_capacity:
            from nexus.agents.codex_candidate import candidate_job_qualified

            selected = await context.session.get(Job, context.claim.job_id, populate_existing=True)
            if selected is None or not await candidate_job_qualified(
                context.session,
                selected,
                claimed=True,
            ):
                raise UnsafeJobError("CODEX_CANDIDATE_AUTHORITY_INVALID")
        if formal_stage_capacity:
            from nexus.agents.codex_formal_stage import formal_stage_job_qualified

            selected = await context.session.get(Job, context.claim.job_id, populate_existing=True)
            if selected is None or not await formal_stage_job_qualified(
                context.session,
                selected,
                claimed=True,
            ):
                raise UnsafeJobError("CODEX_FORMAL_AUTHORITY_INVALID")
        if full_input_capacity:
            from nexus.agents.codex_capacity import capacity_job_qualified

            selected = await context.session.get(Job, context.claim.job_id, populate_existing=True)
            if selected is None or not await capacity_job_qualified(
                context.session, selected, claimed=True
            ):
                raise UnsafeJobError("CODEX_CAPACITY_AUTHORITY_INVALID")
        if run.status in TERMINAL:
            return _agent_result(run.status)
        if run.status != "prepared":
            raise UnsafeJobError("RUN_STATE_INVALID")
        from nexus.agents.codex_sdk import CodexInvocationError

        committee: CommitteeModel
        try:
            if full_input_capacity or formal_full_input_capacity:
                from nexus.agents.capacity_cycle import prepare_cycle_capacity_pool

                deadline = context.monotonic_deadline()
                if deadline is None:
                    raise ValueError("CODEX_CAPACITY_WORKER_ARGUMENT_INVALID")
                committee = await prepare_cycle_capacity_pool(
                    context.session,
                    settings,
                    run,
                    deadline=deadline,
                    formal_stage_capacity=formal_full_input_capacity,
                )
            else:
                committee = await _committee(
                    context.session,
                    settings,
                    run,
                    responses_client=responses_client,
                    tools=tools,
                    deadline=context.monotonic_deadline(),
                    candidate_stage_capacity=candidate_stage_capacity,
                    formal_stage_capacity=formal_stage_capacity,
                )
        except CodexInvocationError as error:
            if (
                dispatch is None
                or error.reason != "CODEX_RESEARCH_STOPPED"
                or error.provider_call_started is not False
            ):
                raise
            # Capacity preparation is pure and precedes committee execution.
            # A claimed member stopped here has made no provider invocation.
            from nexus.agents.execution import stop_uncalled_research

            context.require_dispatch_lease()
            stopped = await stop_uncalled_research(context.session, run_id=run.id)
            return _agent_result(stopped.status)
        except ToolInputError:
            committee = _UnavailableResponsesCommittee()
        from nexus.agents.codex_sdk import CodexCommittee

        if isinstance(committee, CodexCommittee):

            def require_dispatch() -> None:
                context.require_dispatch_lease()
                if dispatch is not None:
                    dispatch.guard()

            committee.dispatch_guard = require_dispatch
        result = await execute_research_run(
            context.session,
            run_id=run.id,
            committee=committee,
            allow_fixture_evidence=allow_fixture_evidence,
            allow_fixture_price_authority=allow_fixture_price_authority,
        )
        return _agent_result(result.status)

    job_types = (
        (JobType.AGENT_PAPER_RESEARCH,)
        if candidate_stage_capacity or formal_stage_capacity or full_input_capacity
        else (JobType.AGENT_RESEARCH, JobType.AGENT_PAPER_RESEARCH)
        if include_agent_paper
        else (JobType.AGENT_RESEARCH,)
    )
    return JobRegistry(
        tuple(
            JobHandlerSpec(
                job_type=job_type,
                schema_version=_JOB_SCHEMA,
                payload_model=_AgentResearchJobPayload,
                queue=QueueName.GENERAL,
                timeout_seconds=BOUNDED_RESEARCH_HANDLER_TIMEOUT_SECONDS
                if capacity_profile_version == BOUNDED_STAGE_BUDGET_CONFIG_VERSION
                and (formal_full_input_capacity or full_input_capacity)
                else FORMAL_RESEARCH_HANDLER_TIMEOUT_SECONDS
                if formal_stage_capacity or full_input_capacity
                else CANDIDATE_RESEARCH_HANDLER_TIMEOUT_SECONDS
                if candidate_stage_capacity
                else RESEARCH_SDK_HANDLER_TIMEOUT_SECONDS
                if runtime_mode is AgentRuntimeMode.CODEX_SDK
                else 45,
                retry_policy=RetryPolicy.AUTOMATIC_SAFE,
                max_attempts=1 if runtime_mode is AgentRuntimeMode.CODEX_SDK else 3,
                idempotency_namespace="agent.research",
                handler=handle,
                research_budget_profile=capacity_profile_version
                if full_input_capacity or formal_full_input_capacity
                else FORMAL_STAGE_CAPACITY_CONFIG_VERSION
                if formal_stage_capacity
                else CANDIDATE_BUDGET_CONFIG_VERSION
                if candidate_stage_capacity
                else None,
            )
            for job_type in job_types
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
    candidate_stage_capacity: bool = False,
    formal_stage_capacity: bool = False,
    full_input_capacity: bool = False,
) -> CommitteeModel:
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    full_input_model = model is not None and model.version in ISOLATED_CAPACITY_CONFIG_VERSIONS
    if (
        type(full_input_capacity) is not bool
        or full_input_capacity
        and formal_stage_capacity
        or full_input_model != (full_input_capacity or formal_stage_capacity and full_input_model)
    ):
        return _UnavailableResponsesCommittee()
    if full_input_capacity:
        from nexus.agents.codex_candidate import require_candidate_database
        from nexus.agents.codex_capacity import isolated_capacity_model_matches

        # Only explicit isolated composition. No public Worker/registration flag
        # routes this candidate into the business database or claims its Jobs.
        await require_candidate_database(session)
        if (
            candidate_stage_capacity
            or formal_stage_capacity
            or settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK
            or model is None
            or not isolated_capacity_model_matches(model)
            or deadline is None
        ):
            return _UnavailableResponsesCommittee()
    formal_model = model is not None and (
        model.version == FORMAL_STAGE_CAPACITY_CONFIG_VERSION
        or full_input_model
        and formal_stage_capacity
    )
    if type(formal_stage_capacity) is not bool or formal_model != formal_stage_capacity:
        return _UnavailableResponsesCommittee()
    if formal_stage_capacity:
        from nexus.agents.codex_formal_stage import formal_preparation_model_matches

        if (
            candidate_stage_capacity
            or settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK
            or model is None
            or not formal_preparation_model_matches(model)
            or deadline is None
        ):
            return _UnavailableResponsesCommittee()
    capacity_profile = candidate_stage_capacity or formal_stage_capacity or full_input_capacity
    candidate_model = model is not None and model.version == CANDIDATE_BUDGET_CONFIG_VERSION
    if candidate_model != candidate_stage_capacity:
        return _UnavailableResponsesCommittee()
    if candidate_stage_capacity:
        from nexus.agents.codex_candidate import candidate_model_matches, require_candidate_database

        await require_candidate_database(session)
        if (
            settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK
            or model is None
            or not candidate_model_matches(model)
            or deadline is None
        ):
            return _UnavailableResponsesCommittee()
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
            or (model.version not in SDK_CONFIG_VERSIONS and not capacity_profile)
            or model.version != run.pricing_version
            or model.config_hash != run.model_hash
            or model.max_input_tokens
            != (96000 if capacity_profile else aggregate_input_limit(model.version))
            or model.max_output_tokens != 8192
            or model.input_usd_per_million != 0
            or model.output_usd_per_million != 0
            or prompts is None
        ):
            return _UnavailableResponsesCommittee()
        committee_type = (
            StructuredStagedCodexCommittee
            if model.version in STRUCTURED_CONFIG_VERSIONS or capacity_profile
            else StagedCodexCommittee
            if model.version == STAGED_CONFIG_VERSION
            else CodexCommittee
        )
        if model.version == DEADLINE_BUDGET_CONFIG_VERSION and deadline is not None:
            # The worker deadline includes transaction/handler grace. Anchor v6
            # to the original worker start, never reset time after DB preparation.
            deadline -= RESEARCH_SDK_HANDLER_TIMEOUT_SECONDS - RESEARCH_STAGE_JOB_TIMEOUT_SECONDS
        if (formal_stage_capacity or full_input_capacity) and deadline is not None:
            deadline -= (
                FORMAL_RESEARCH_HANDLER_TIMEOUT_SECONDS - FORMAL_RESEARCH_JOB_TIMEOUT_SECONDS
            )
        if candidate_stage_capacity and deadline is not None:
            deadline -= (
                CANDIDATE_RESEARCH_HANDLER_TIMEOUT_SECONDS - CANDIDATE_RESEARCH_JOB_TIMEOUT_SECONDS
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
                {
                    "stage_time_budget": True,
                    "stage_budget_version": model.version,
                }
                if full_input_capacity
                else {
                    "stage_time_budget": True,
                    "stage_budget_version": model.version,
                }
                if formal_stage_capacity
                else {
                    "stage_time_budget": True,
                    "stage_budget_version": CANDIDATE_BUDGET_CONFIG_VERSION,
                }
                if candidate_stage_capacity
                else {"stage_time_budget": True}
                if model.version == DEADLINE_BUDGET_CONFIG_VERSION
                else {}
            ),
            **(
                {"paper_stage_scope": True}
                if model.version in PAPER_SCOPE_CONFIG_VERSIONS or capacity_profile
                else {}
            ),
            **(
                {"inline_citations": True}
                if model.version in ({INLINE_CITATION_CONFIG_VERSION} | ACTIONABLE_CONFIG_VERSIONS)
                or capacity_profile
                else {}
            ),
            **(
                {
                    "deadline": deadline,
                    "aggregate_input_tokens": model.max_input_tokens,
                    "max_wire_bytes": FULL_INPUT_CAPACITY_MAX_WIRE_BYTES
                    if full_input_model
                    else 64000
                    if capacity_profile
                    else wire_byte_limit(model.version),
                }
                if model.version in STAGED_CONFIG_VERSIONS or capacity_profile
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
