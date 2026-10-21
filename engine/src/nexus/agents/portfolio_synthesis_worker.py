"""One fenced, non-retryable portfolio comparison outside database transactions."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace
from datetime import datetime
from hashlib import sha256
from typing import Literal, Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.agents.codex_candidate import (
    isolated_candidate_model_matches,
    require_candidate_consumer_profile,
)
from nexus.agents.codex_formal_stage import formal_preparation_model_matches
from nexus.agents.codex_input_view import project_portfolio_payload
from nexus.agents.codex_sdk import (
    MAX_INPUT_TOKENS,
    MAX_OUTPUT_TOKENS,
    MODEL_ID,
    CodexInvocationError,
    invoke_codex_portfolio,
)
from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSIONS
from nexus.agents.persistence import load_actionable_research_output
from nexus.agents.portfolio_synthesis_contracts import (
    SYNTHESIS_PROMPT_VERSION,
    SYNTHESIS_TOOL_HASH,
    PortfolioSynthesisIntentV1,
    canonical_portfolio_synthesis_hash,
    validate_portfolio_synthesis_intent,
)
from nexus.agents.portfolio_synthesis_diagnostics import (
    FailureObservation,
    SynthesisDiagnosticConflict,
    freeze_failure_observation,
)
from nexus.agents.portfolio_synthesis_persistence import (
    load_frozen_synthesis_authority,
    persist_portfolio_synthesis_terminal,
)
from nexus.agents.responses_budget import UsageSnapshot
from nexus.agents.synthesis_lifecycle import (
    BOUNDED_SYNTHESIS_JOB,
    SYNTHESIS_JOB_VERSIONS,
    SYNTHESIS_LEASE_SECONDS,
    SynthesisDeadline,
)
from nexus.jobs.models import JOB_PRIORITIES, JobType, QueueName
from nexus.jobs.repository import (
    ClaimedJob,
    JobCompletion,
    LeaseLostError,
    _not_candidate_agent_job,
    checkpoint_job,
    claim_next_job,
    dead_letter_job,
    execute_and_finalize_job,
    reclaim_expired_jobs,
)
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPortfolioSynthesisBudgetReservationRecord,
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentPromptVersionRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, JobAttempt


class SynthesisHandlerResultUnknown(ValueError):
    """An interrupted handler needs persisted-state readback, never replay."""

    def __init__(self) -> None:
        super().__init__("PORTFOLIO_SYNTHESIS_HANDLER_RESULT_UNKNOWN")


class SynthesisTransport(Protocol):
    def supports(self, model: AgentModelConfigRecord) -> bool: ...

    async def invoke(
        self,
        payload: Mapping[str, object],
        *,
        lifecycle: SynthesisDeadline | None = None,
        on_usage: Callable[[UsageSnapshot], None] | None = None,
    ) -> tuple[str, UsageSnapshot]: ...


class CodexSynthesisTransport:
    """The registered actionable SDK model, with the existing isolated one-turn adapter."""

    def __init__(
        self, *, candidate_stage_capacity: bool = False, formal_stage_capacity: bool = False
    ) -> None:
        if type(candidate_stage_capacity) is not bool:
            raise ValueError("CODEX_CANDIDATE_ISOLATION_REQUIRED")
        if type(formal_stage_capacity) is not bool or (
            formal_stage_capacity and candidate_stage_capacity is not False
        ):
            raise ValueError("CODEX_FORMAL_WORKER_ARGUMENT_INVALID")
        self._formal_stage_capacity = formal_stage_capacity
        self._candidate_stage_capacity = candidate_stage_capacity

    def supports(self, model: AgentModelConfigRecord) -> bool:
        if self._formal_stage_capacity:
            return formal_preparation_model_matches(model)
        if self._candidate_stage_capacity:
            return isolated_candidate_model_matches(model)
        return (
            model.model_id == MODEL_ID
            and model.version in ACTIONABLE_CONFIG_VERSIONS
            and model.max_input_tokens == 96000
            and model.max_output_tokens == MAX_OUTPUT_TOKENS
            and model.max_tool_calls == 0
            and model.input_usd_per_million == model.output_usd_per_million == 0
        )

    async def invoke(
        self,
        payload: Mapping[str, object],
        *,
        lifecycle: SynthesisDeadline | None = None,
        on_usage: Callable[[UsageSnapshot], None] | None = None,
    ) -> tuple[str, UsageSnapshot]:
        if lifecycle is not None:
            if not self._formal_stage_capacity:
                raise ValueError("CODEX_FORMAL_WORKER_ARGUMENT_INVALID")
            return await invoke_codex_portfolio(
                project_portfolio_payload(payload),
                output_model=PortfolioSynthesisIntentV1,
                max_wire_bytes=64000,
                lifecycle_version=BOUNDED_SYNTHESIS_JOB,
                deadline_at=lifecycle.work_at,
                cleanup_deadline_at=lifecycle.cleanup_at,
                on_usage=on_usage,
            )
        return await invoke_codex_portfolio(
            project_portfolio_payload(payload),
            output_model=PortfolioSynthesisIntentV1,
            max_wire_bytes=64000,
        )


async def candidate_synthesis_job_qualified(
    session: AsyncSession, job: Job, *, now: datetime, claimed: bool = False
) -> bool:
    """Verify frozen candidate synthesis authority under the selected Job lock."""
    await require_candidate_consumer_profile(session, candidate_stage_capacity=True)
    if (
        type(claimed) is not bool
        or job.job_type != JobType.AGENT_PORTFOLIO_SYNTHESIS.value
        or job.queue != QueueName.GENERAL.value
        or job.schema_version != "portfolio_synthesis_job_v1"
        or job.priority != JOB_PRIORITIES[JobType.AGENT_PORTFOLIO_SYNTHESIS]
        or job.retry_policy != "never"
        or job.max_attempts != 1
        or job.manual_retry_count != 0
        or job.status != ("running" if claimed else "queued")
        or job.attempt_count != (1 if claimed else 0)
        or set(job.payload) != {"run_id"}
        or type(job.payload.get("run_id")) is not str
    ):
        return False
    try:
        run_id = UUID(str(job.payload["run_id"]))
    except (ValueError, TypeError):
        return False
    run = await session.scalar(
        select(AgentPortfolioSynthesisRunRecord)
        .where(AgentPortfolioSynthesisRunRecord.id == run_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if run is None or run.status != "prepared":
        return False
    model = await session.scalar(
        select(AgentModelConfigRecord)
        .where(AgentModelConfigRecord.id == run.model_config_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        model is None
        or not isolated_candidate_model_matches(model)
        or run.model_hash != model.config_hash
        or run.pricing_version != model.version
        or model.pricing_available_at > now
        or model.pricing_expires_at <= now
    ):
        return False
    bound_jobs = tuple(
        await session.scalars(
            select(Job.id).where(
                Job.job_type == JobType.AGENT_PORTFOLIO_SYNTHESIS.value,
                Job.payload["run_id"].as_string() == str(run.id),
            )
        )
    )
    if bound_jobs != (job.id,):
        return False
    try:
        frozen = await load_frozen_synthesis_authority(session, run)
        if (
            job.input_hash != run.input_hash
            or job.scope_kind != "portfolio"
            or job.scope_id != str(frozen.portfolio_id)
            or job.effective_at != frozen.decision_time
            or now >= frozen.expires_at
        ):
            return False
        for source in frozen.security_outputs:
            output = await session.get(
                AgentResearchOutputRecord, source.research_output_id, populate_existing=True
            )
            research = (
                None
                if output is None
                else await session.get(
                    AgentResearchRunRecord, output.run_id, populate_existing=True
                )
            )
            if (
                output is None
                or research is None
                or output.content_hash != source.content_hash
                or research.instrument_id != source.instrument_id
                or research.decision_time != frozen.decision_time
                or research.idempotency_key
                != f"agent-paper:{frozen.cycle_id}:{source.instrument_id}"
            ):
                return False
            action = await load_actionable_research_output(
                session,
                run_id=research.id,
                decision_time=now,
                allow_safe_no_trade=True,
                candidate_stage_capacity=True,
            )
            if (
                action.output_id != source.research_output_id
                or action.intent_hash != source.intent_hash
                or action.evidence_ref_ids != source.evidence_ref_ids
            ):
                return False
    except ValueError:
        return False
    return True


async def formal_synthesis_job_qualified(
    session: AsyncSession,
    job: Job,
    *,
    now: datetime,
    claimed: bool = False,
    expired_recovery: bool = False,
) -> bool:
    """Verify frozen formal synthesis authority under the selected Job lock."""
    if (
        type(claimed) is not bool
        or type(expired_recovery) is not bool
        or expired_recovery
        and not claimed
        or job.job_type != JobType.AGENT_PORTFOLIO_SYNTHESIS.value
        or job.queue != QueueName.GENERAL.value
        or job.schema_version not in SYNTHESIS_JOB_VERSIONS
        or job.priority != JOB_PRIORITIES[JobType.AGENT_PORTFOLIO_SYNTHESIS]
        or job.retry_policy != "never"
        or job.max_attempts != 1
        or job.manual_retry_count != 0
        or job.status != ("running" if claimed else "queued")
        or job.attempt_count != (1 if claimed else 0)
        or set(job.payload) != {"run_id"}
        or type(job.payload.get("run_id")) is not str
    ):
        return False
    try:
        run_id = UUID(str(job.payload["run_id"]))
    except (ValueError, TypeError):
        return False
    run = await session.scalar(
        select(AgentPortfolioSynthesisRunRecord)
        .where(AgentPortfolioSynthesisRunRecord.id == run_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if run is None or run.status != ("running" if expired_recovery else "prepared"):
        return False
    if expired_recovery:
        # Historical identity proof only; the caller separately fences actual lease expiry.
        # This timestamp must never grant a new execution permission.
        try:
            now = (await load_frozen_synthesis_authority(session, run)).admitted_at
        except ValueError:
            return False
    model = await session.scalar(
        select(AgentModelConfigRecord)
        .where(AgentModelConfigRecord.id == run.model_config_id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        model is None
        or not formal_preparation_model_matches(model)
        or run.model_hash != model.config_hash
        or run.pricing_version != model.version
        or model.pricing_available_at > now
        or model.pricing_expires_at <= now
    ):
        return False
    bound_jobs = tuple(
        await session.scalars(
            select(Job.id).where(
                Job.job_type == JobType.AGENT_PORTFOLIO_SYNTHESIS.value,
                Job.payload["run_id"].as_string() == str(run.id),
            )
        )
    )
    if bound_jobs != (job.id,):
        return False
    try:
        frozen = await load_frozen_synthesis_authority(session, run)
        from nexus.paper.agent_cycle import _require_cycle_candidate_profile

        cycle = await session.get(AgentPaperCycleRecord, frozen.cycle_id, populate_existing=True)
        if (
            cycle is None
            or frozen.account_id != cycle.account_id
            or frozen.portfolio_id != cycle.portfolio_id
            or frozen.candidate_batch_id != cycle.candidate_batch_id
            or frozen.decision_time != cycle.decision_time
            or model.created_at > frozen.admitted_at
        ):
            return False
        model_id = await _require_cycle_candidate_profile(
            session,
            cycle=cycle,
            candidate_stage_capacity=False,
            formal_stage_capacity=True,
            now=now,
        )
        if model_id != model.id:
            return False
        if (
            job.input_hash != run.input_hash
            or job.scope_kind != "portfolio"
            or job.scope_id != str(frozen.portfolio_id)
            or job.effective_at != frozen.decision_time
            or now >= frozen.expires_at
        ):
            return False
        for source in frozen.security_outputs:
            output = await session.get(
                AgentResearchOutputRecord, source.research_output_id, populate_existing=True
            )
            research = (
                None
                if output is None
                else await session.get(
                    AgentResearchRunRecord, output.run_id, populate_existing=True
                )
            )
            if (
                output is None
                or research is None
                or output.content_hash != source.content_hash
                or research.model_config_id != model.id
                or research.model_hash != model.config_hash
                or research.pricing_version != model.version
                or research.actor_id != "local-agent-paper-cycle"
                or research.instrument_id != source.instrument_id
                or research.decision_time != frozen.decision_time
                or research.idempotency_key
                != f"agent-paper:{frozen.cycle_id}:{source.instrument_id}"
            ):
                return False
            action = await load_actionable_research_output(
                session,
                run_id=research.id,
                decision_time=now,
                allow_safe_no_trade=True,
                formal_stage_capacity=True,
            )
            if (
                action.output_id != source.research_output_id
                or action.intent_hash != source.intent_hash
                or action.evidence_ref_ids != source.evidence_ref_ids
            ):
                return False
    except ValueError:
        return False
    return True


async def read_synthesis_job(session: AsyncSession, *, cycle_id: UUID) -> Job | None:
    """Resolve only the unique run belonging to this cycle, never the latest Job."""
    run = await session.scalar(
        select(AgentPortfolioSynthesisRunRecord).where(
            AgentPortfolioSynthesisRunRecord.cycle_id == cycle_id
        )
    )
    if run is None:
        return None
    jobs = tuple(
        await session.scalars(
            select(Job).where(
                Job.job_type == JobType.AGENT_PORTFOLIO_SYNTHESIS.value,
                Job.payload["run_id"].as_string() == str(run.id),
            )
        )
    )
    if len(jobs) != 1:
        raise ValueError("PORTFOLIO_SYNTHESIS_JOB_INVALID")
    return jobs[0]


async def reconcile_synthesis_orphans(
    session: AsyncSession, *, now: datetime, job_ids: tuple[UUID, ...]
) -> None:
    jobs = tuple(
        await session.scalars(
            select(Job).where(
                Job.id.in_(job_ids),
                Job.job_type == JobType.AGENT_PORTFOLIO_SYNTHESIS.value,
                Job.status.in_(("failed", "dead_letter", "skipped", "succeeded")),
                _not_candidate_agent_job(),
            )
        )
    )
    for job in jobs:
        try:
            run_id = UUID(str(job.payload["run_id"]))
        except (KeyError, TypeError, ValueError):
            continue
        run = await session.get(AgentPortfolioSynthesisRunRecord, run_id)
        if run is None or run.status not in {"prepared", "running"}:
            continue
        output = await session.scalar(
            select(AgentPortfolioSynthesisOutputRecord).where(
                AgentPortfolioSynthesisOutputRecord.run_id == run.id
            )
        )
        if output is None:
            attempt = await session.scalar(
                select(JobAttempt).where(
                    JobAttempt.job_id == job.id,
                    JobAttempt.attempt_no == job.attempt_count,
                )
            )
            observation = FailureObservation(
                local="orphan_prepared" if run.status == "prepared" else "orphan_running"
            )
            await persist_portfolio_synthesis_terminal(
                session,
                run_id=run.id,
                input_hash=run.input_hash,
                status="failed",
                intent=None,
                input_tokens=None,
                output_tokens=None,
                now=now,
                failure_observation=observation,
                diagnostic_job_id=job.id,
                diagnostic_attempt_no=None if attempt is None else attempt.attempt_no,
                diagnostic_fence_token=None if attempt is None else attempt.fence_token,
            )


async def run_synthesis_job_once(
    factory: async_sessionmaker[AsyncSession],
    *,
    transport: SynthesisTransport,
    worker_id: str,
    clock: Callable[[], datetime],
    job_ids: tuple[UUID, ...] = (),
    candidate_stage_capacity: bool = False,
    formal_stage_capacity: bool = False,
    preclaim: Callable[[], Awaitable[None]] | None = None,
) -> bool:
    """Bound the formal handler from before claim through terminal commit.

    A timeout can race COMMIT. Never infer rollback or write a replacement terminal;
    the caller receives UNKNOWN and normal exact-job readback/recovery resolves it.
    """
    lifecycle = SynthesisDeadline(asyncio.get_running_loop().time())
    try:
        async with asyncio.timeout_at(lifecycle.handler_at if formal_stage_capacity else None):
            if preclaim is not None:
                await preclaim()
            return await _run_synthesis_job_once(
                factory,
                transport=transport,
                worker_id=worker_id,
                clock=clock,
                job_ids=job_ids,
                candidate_stage_capacity=candidate_stage_capacity,
                formal_stage_capacity=formal_stage_capacity,
                lifecycle=lifecycle,
            )
    except TimeoutError as error:
        raise SynthesisHandlerResultUnknown() from error


async def _run_synthesis_job_once(
    factory: async_sessionmaker[AsyncSession],
    *,
    transport: SynthesisTransport,
    worker_id: str,
    clock: Callable[[], datetime],
    job_ids: tuple[UUID, ...] = (),
    candidate_stage_capacity: bool = False,
    formal_stage_capacity: bool = False,
    lifecycle: SynthesisDeadline | None = None,
) -> bool:
    """Dispatch only explicitly selected Jobs; persist running before any model I/O."""
    if type(formal_stage_capacity) is not bool or (
        formal_stage_capacity
        and (
            candidate_stage_capacity is not False
            or not isinstance(job_ids, tuple)
            or len(job_ids) != 1
            or not isinstance(job_ids[0], UUID)
        )
    ):
        raise ValueError("CODEX_FORMAL_WORKER_ARGUMENT_INVALID")
    if (
        type(candidate_stage_capacity) is not bool
        or candidate_stage_capacity
        and (len(job_ids) != 1 or not isinstance(job_ids[0], UUID))
    ):
        raise ValueError("CODEX_CANDIDATE_WORKER_ARGUMENT_INVALID")
    capacity_profile = candidate_stage_capacity or formal_stage_capacity
    if not job_ids:
        return False

    async def candidate_guard(session: AsyncSession, job: Job) -> bool:
        return await candidate_synthesis_job_qualified(session, job, now=clock())

    async def formal_guard(session: AsyncSession, job: Job) -> bool:
        return await formal_synthesis_job_qualified(session, job, now=clock())

    async with factory.begin() as session:
        await require_candidate_consumer_profile(
            session, candidate_stage_capacity=candidate_stage_capacity
        )
        scope_conditions = [] if capacity_profile else [_not_candidate_agent_job()]
        job_ids = tuple(
            await session.scalars(
                select(Job.id).where(
                    Job.id.in_(job_ids),
                    Job.job_type == JobType.AGENT_PORTFOLIO_SYNTHESIS.value,
                    *scope_conditions,
                )
            )
        )
        if not job_ids:
            return False
        if not capacity_profile:
            await reclaim_expired_jobs(
                session, queue=QueueName.GENERAL, now=clock(), job_ids=job_ids
            )
            await reconcile_synthesis_orphans(session, now=clock(), job_ids=job_ids)
        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id=worker_id,
            now=clock(),
            lease_seconds=SYNTHESIS_LEASE_SECONDS,
            job_types=(JobType.AGENT_PORTFOLIO_SYNTHESIS.value,),
            job_ids=job_ids,
            locked_job_guard=(
                formal_guard
                if formal_stage_capacity
                else candidate_guard
                if candidate_stage_capacity
                else None
            ),
            candidate_synthesis_profile=candidate_stage_capacity,
            formal_synthesis_profile=formal_stage_capacity,
        )
    if claim is None:
        return False
    try:
        run_id = UUID(str(claim.payload["run_id"]))
    except (KeyError, TypeError, ValueError):
        async with factory.begin() as session:
            await dead_letter_job(
                session, claim=claim, now=clock(), error_code="HANDLER_PAYLOAD_INVALID"
            )
        return True
    payload: dict[str, object] = {}
    input_hash = ""
    limits = (0, 0)
    expires_at: datetime | None = None

    async def start(session: AsyncSession) -> None:
        nonlocal payload, input_hash, limits, expires_at
        if capacity_profile:
            selected_job = await session.get(
                Job, claim.job_id, with_for_update=True, populate_existing=True
            )
            qualifier = (
                formal_synthesis_job_qualified
                if formal_stage_capacity
                else candidate_synthesis_job_qualified
            )
            if selected_job is None or not await qualifier(
                session, selected_job, now=clock(), claimed=True
            ):
                raise ValueError("PORTFOLIO_SYNTHESIS_AUTHORITY_INVALID")
        run = await session.get(AgentPortfolioSynthesisRunRecord, run_id, with_for_update=True)
        if run is None:
            raise ValueError("PORTFOLIO_SYNTHESIS_RUN_INVALID")
        from nexus.paper.synthesis_usage_acceptance import require_synthesis_not_accepted

        await require_synthesis_not_accepted(session, run_id)
        frozen = await load_frozen_synthesis_authority(session, run)
        from nexus.paper.economic_cycle import read_economic_cycle

        economic = await read_economic_cycle(session, cycle_id=run.cycle_id)
        expires_at = frozen.expires_at
        input_hash = run.input_hash
        job = await session.get(Job, claim.job_id)
        prompt = await session.get(AgentPromptVersionRecord, run.prompt_version_id)
        model = await session.get(AgentModelConfigRecord, run.model_config_id)
        reserve = await session.scalar(
            select(AgentPortfolioSynthesisBudgetReservationRecord).where(
                AgentPortfolioSynthesisBudgetReservationRecord.run_id == run.id
            )
        )
        if (
            run.status != "prepared"
            or claim.attempt_no != 1
            or claim.max_attempts != 1
            or claim.retry_policy != "never"
            or claim.queue != QueueName.GENERAL.value
            or claim.schema_version not in SYNTHESIS_JOB_VERSIONS
            or claim.priority != JOB_PRIORITIES[JobType.AGENT_PORTFOLIO_SYNTHESIS]
            or not claim.dependencies_satisfied
            or claim.dependency_result_code is not None
            or set(claim.payload) != {"run_id"}
            or job is None
            or job.manual_retry_count != 0
            or job.input_hash != run.input_hash
            or job.scope_kind != "portfolio"
            or job.scope_id != str(frozen.portfolio_id)
            or job.effective_at != frozen.decision_time
            or prompt is None
            or prompt.role != "portfolio_decision"
            or prompt.version != frozen.prompt_version
            or economic is None
            and prompt.version != SYNTHESIS_PROMPT_VERSION
            or prompt.content_hash != run.prompt_hash
            or sha256(prompt.content.encode()).hexdigest() != run.prompt_hash
            or prompt.created_at > frozen.admitted_at
            or model is None
            or model.config_hash != run.model_hash
            or model.version != run.pricing_version
            or not transport.supports(model)
            or model.created_at > frozen.admitted_at
            or model.pricing_available_at > clock()
            or model.pricing_expires_at <= clock()
            or clock() >= frozen.expires_at
            or frozen.tool_hash != SYNTHESIS_TOOL_HASH
            or frozen.intent_schema_hash
            != canonical_portfolio_synthesis_hash(PortfolioSynthesisIntentV1.model_json_schema())
            or reserve is None
            or reserve.state != "reserved"
            or reserve.pricing_version != model.version
            or reserve.reserved_usd != model.worst_case_cost_usd
        ):
            raise ValueError("PORTFOLIO_SYNTHESIS_AUTHORITY_INVALID")
        limits = (min(model.max_input_tokens, MAX_INPUT_TOKENS), model.max_output_tokens)
        payload = {
            "instructions": prompt.content,
            "input_hash": run.input_hash,
            "frozen_input": frozen.model_dump(mode="json"),
        }
        run.status = "running"
        await session.flush()

    try:
        async with factory.begin() as session:
            lease_expires_at = await checkpoint_job(session, claim=claim, now=clock(), apply=start)
        terminal_claim = replace(claim, expires_at=lease_expires_at)
    except ValueError:
        async with factory.begin() as session:
            run = await session.get(AgentPortfolioSynthesisRunRecord, run_id)
            if run is None:
                await dead_letter_job(
                    session,
                    claim=claim,
                    now=clock(),
                    error_code="PORTFOLIO_SYNTHESIS_AUTHORITY_INVALID",
                )
                return True
            # Never overwrite a terminal fact or replay a previous dispatch.
            if run.status not in {"prepared", "running"}:
                await dead_letter_job(
                    session,
                    claim=claim,
                    now=clock(),
                    error_code="PORTFOLIO_SYNTHESIS_AUTHORITY_INVALID",
                )
                return True
            input_hash = run.input_hash
        await _finish(
            factory,
            claim,
            run_id,
            input_hash,
            "failed",
            None,
            None,
            clock,
            failure_observation=FailureObservation(local="authority_rejected"),
        )
        return True
    usage: UsageSnapshot | None = None
    cancelled = False
    failure_observation: FailureObservation | None = None

    def retain_usage(observed: UsageSnapshot) -> None:
        nonlocal usage
        usage = observed

    intent = None
    status: Literal["completed", "abstained", "failed"] = "failed"
    pre_dispatch = False
    error_code = "PORTFOLIO_SYNTHESIS_FAILED"
    try:
        if claim.schema_version == BOUNDED_SYNTHESIS_JOB:
            if not formal_stage_capacity or lifecycle is None:
                raise ValueError("PORTFOLIO_SYNTHESIS_AUTHORITY_INVALID")
            if asyncio.get_running_loop().time() >= lifecycle.work_at:
                raise CodexInvocationError(
                    "CODEX_TIMEOUT", phase="preflight", provider_call_started=False
                )
            raw, usage = await transport.invoke(payload, lifecycle=lifecycle, on_usage=retain_usage)
        else:
            raw, usage = await transport.invoke(payload)
        if usage.input_tokens > limits[0] or usage.output_tokens > limits[1]:
            usage = None
            error_code = "PORTFOLIO_SYNTHESIS_TOKEN_LIMIT_EXCEEDED"
            raise ValueError("PORTFOLIO_SYNTHESIS_TOKEN_LIMIT")
        intent = PortfolioSynthesisIntentV1.model_validate_json(raw)
        async with factory() as session:
            run = await session.get(AgentPortfolioSynthesisRunRecord, run_id)
            assert run is not None
            frozen = await load_frozen_synthesis_authority(session, run)
            validate_portfolio_synthesis_intent(intent, frozen)
            if clock() >= frozen.expires_at:
                intent = None
                error_code = "PORTFOLIO_SYNTHESIS_SESSION_EXPIRED"
                raise ValueError(error_code)
        status = "completed"
    except asyncio.CancelledError:
        if (
            claim.schema_version != BOUNDED_SYNTHESIS_JOB
            or lifecycle is None
            or asyncio.get_running_loop().time() >= lifecycle.handler_at
        ):
            raise
        # The SDK has already bounded child cleanup and delivered any trusted usage.
        # This remains under the original handler deadline; a second cancellation
        # rolls back the pending write, leaving exact-job orphan recovery to read it.
        cancelled = True
        failure_observation = FailureObservation(local="cancelled")
        intent = None
        error_code = "PORTFOLIO_SYNTHESIS_CALL_UNCERTAIN"
    except CodexInvocationError as error:
        failure_observation = freeze_failure_observation(
            {
                "invocation_phase": error.phase,
                "invocation_kind": error.kind,
                "timeout_source": error.timeout_source,
                "failure_facts": error.failure_facts,
            }
        )
        usage = error.usage if error.usage is not None else usage
        pre_dispatch = usage is None and error.provider_call_started is False
        error_code = (
            "PORTFOLIO_SYNTHESIS_PRE_DISPATCH_" + error.phase.upper()
            if pre_dispatch
            else "PORTFOLIO_SYNTHESIS_CALL_UNCERTAIN"
        )
    except ValueError:
        failure_observation = FailureObservation(local="validation")
        status = (
            "abstained"
            if usage is not None and error_code != "PORTFOLIO_SYNTHESIS_SESSION_EXPIRED"
            else "failed"
        )
        intent = None
    except Exception:
        # Provider errors carry no trusted raw text and never trigger another call.
        failure_observation = FailureObservation(local="exception")
        intent = None
    if usage is not None and (usage.input_tokens > limits[0] or usage.output_tokens > limits[1]):
        status, intent, usage = "failed", None, None
        error_code = "PORTFOLIO_SYNTHESIS_TOKEN_LIMIT_EXCEEDED"
    if not pre_dispatch and expires_at is not None and clock() >= expires_at:
        status, intent = "failed", None
        error_code = "PORTFOLIO_SYNTHESIS_SESSION_EXPIRED"
        if failure_observation is None:
            failure_observation = FailureObservation(local="evidence_expired")
    await _finish(
        factory,
        terminal_claim,
        run_id,
        input_hash,
        status,
        intent,
        usage,
        clock,
        expires_at=expires_at,
        pre_dispatch=pre_dispatch,
        error_code=error_code,
        failure_observation=failure_observation,
    )
    if cancelled:
        raise asyncio.CancelledError
    return True


async def _finish(
    factory: async_sessionmaker[AsyncSession],
    claim: ClaimedJob,
    run_id: UUID,
    input_hash: str,
    status: Literal["completed", "abstained", "failed"],
    intent: PortfolioSynthesisIntentV1 | None,
    usage: UsageSnapshot | None,
    clock: Callable[[], datetime],
    *,
    expires_at: datetime | None = None,
    pre_dispatch: bool = False,
    error_code: str = "PORTFOLIO_SYNTHESIS_FAILED",
    failure_observation: FailureObservation | None = None,
) -> None:
    if failure_observation is not None:
        failure_observation = freeze_failure_observation(failure_observation)

    class EvidenceExpired(Exception):
        """Abort a tentative terminal; never escape this local finalization."""

    def require_evidence(at: datetime) -> None:
        if status != "failed" and expires_at is not None and at >= expires_at:
            raise EvidenceExpired

    async def complete(session: AsyncSession) -> JobCompletion:
        started_at = clock()
        require_evidence(started_at)
        try:
            await persist_portfolio_synthesis_terminal(
                session,
                run_id=run_id,
                input_hash=input_hash,
                status=status,
                intent=intent,
                input_tokens=None if usage is None else usage.input_tokens,
                output_tokens=None if usage is None else usage.output_tokens,
                now=started_at,
                provider_call_started=False if pre_dispatch else None,
                failure_observation=failure_observation,
                diagnostic_job_id=claim.job_id,
                diagnostic_attempt_no=claim.attempt_no,
                diagnostic_fence_token=claim.fence_token,
            )
        except SynthesisDiagnosticConflict:
            # Business/hash/usage already matched; reject only the new diagnostic.
            # This fixed signal must not escape into any generic job retry path.
            import logging

            logging.getLogger(__name__).warning("PORTFOLIO_SYNTHESIS_DIAGNOSTIC_CONFLICT")
        finished_at = clock()
        require_evidence(finished_at)
        outcome: Literal["succeeded", "skipped", "failed"] = (
            "succeeded"
            if status == "completed"
            else "skipped"
            if status == "abstained"
            else "failed"
        )
        return JobCompletion(
            outcome=outcome,
            result_code="PORTFOLIO_SYNTHESIS_" + status.upper(),
            # Recheck the lease after terminal locks/writes, not with entry time.
            finished_at=finished_at,
            error_code=error_code if status == "failed" else None,
        )

    async def finalize() -> None:
        async with factory.begin() as session:
            await execute_and_finalize_job(
                session, claim=claim, checked_at=clock(), execute=complete
            )
            # Include the final Job/attempt flush before the transaction commits.
            boundary_at = clock()
            if boundary_at >= claim.expires_at:
                raise LeaseLostError("job lease expired during synthesis terminal flush")
            require_evidence(boundary_at)

    try:
        await finalize()
    except EvidenceExpired:
        # The entire tentative terminal (including settlement/Job) rolled back.
        # Save one failure with the same trusted usage under the original fence;
        # loss of that lease still rolls everything back. No provider redispatch.
        status, intent = "failed", None
        error_code = "PORTFOLIO_SYNTHESIS_SESSION_EXPIRED"
        if failure_observation is None:
            failure_observation = FailureObservation(local="evidence_expired")
        await finalize()
