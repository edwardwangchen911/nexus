"""One fenced, non-retryable portfolio comparison outside database transactions."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from hashlib import sha256
from typing import Literal, Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from nexus.agents.codex_sdk import (
    MAX_INPUT_TOKENS,
    MAX_OUTPUT_TOKENS,
    MODEL_ID,
    CodexInvocationError,
    invoke_codex_portfolio,
)
from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSION
from nexus.agents.portfolio_synthesis_contracts import (
    SYNTHESIS_PROMPT_VERSION,
    SYNTHESIS_TOOL_HASH,
    PortfolioSynthesisIntentV1,
    canonical_portfolio_synthesis_hash,
    validate_portfolio_synthesis_intent,
)
from nexus.agents.portfolio_synthesis_persistence import (
    _frozen_run,
    persist_portfolio_synthesis_terminal,
)
from nexus.agents.responses_budget import UsageSnapshot
from nexus.jobs.models import JOB_PRIORITIES, JobType, QueueName
from nexus.jobs.repository import (
    ClaimedJob,
    JobCompletion,
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
)
from nexus.persistence.models.runtime import Job


class SynthesisTransport(Protocol):
    def supports(self, model: AgentModelConfigRecord) -> bool: ...

    async def invoke(self, payload: Mapping[str, object]) -> tuple[str, UsageSnapshot]: ...


class CodexSynthesisTransport:
    """The registered actionable SDK model, with the existing isolated one-turn adapter."""

    def supports(self, model: AgentModelConfigRecord) -> bool:
        return (
            model.model_id == MODEL_ID
            and model.version == ACTIONABLE_CONFIG_VERSION
            and model.max_input_tokens == 96000
            and model.max_output_tokens == MAX_OUTPUT_TOKENS
            and model.max_tool_calls == 0
            and model.input_usd_per_million == model.output_usd_per_million == 0
        )

    async def invoke(self, payload: Mapping[str, object]) -> tuple[str, UsageSnapshot]:
        return await invoke_codex_portfolio(
            payload, output_model=PortfolioSynthesisIntentV1, max_wire_bytes=64000
        )


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
            await persist_portfolio_synthesis_terminal(
                session,
                run_id=run.id,
                input_hash=run.input_hash,
                status="failed",
                intent=None,
                input_tokens=None,
                output_tokens=None,
                now=now,
            )


async def run_synthesis_job_once(
    factory: async_sessionmaker[AsyncSession],
    *,
    transport: SynthesisTransport,
    worker_id: str,
    clock: Callable[[], datetime],
    job_ids: tuple[UUID, ...] = (),
) -> bool:
    """Dispatch only explicitly selected Jobs; persist running before any model I/O."""
    if not job_ids:
        return False
    async with factory.begin() as session:
        job_ids = tuple(
            await session.scalars(
                select(Job.id).where(
                    Job.id.in_(job_ids),
                    Job.job_type == JobType.AGENT_PORTFOLIO_SYNTHESIS.value,
                )
            )
        )
        if not job_ids:
            return False
        await reclaim_expired_jobs(session, queue=QueueName.GENERAL, now=clock(), job_ids=job_ids)
        await reconcile_synthesis_orphans(session, now=clock(), job_ids=job_ids)
        claim = await claim_next_job(
            session,
            queue=QueueName.GENERAL,
            worker_id=worker_id,
            now=clock(),
            lease_seconds=120,
            job_types=(JobType.AGENT_PORTFOLIO_SYNTHESIS.value,),
            job_ids=job_ids,
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
        run = await session.get(AgentPortfolioSynthesisRunRecord, run_id, with_for_update=True)
        if run is None:
            raise ValueError("PORTFOLIO_SYNTHESIS_RUN_INVALID")
        frozen = _frozen_run(run)
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
            or claim.schema_version != "portfolio_synthesis_job_v1"
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
            or prompt.version != SYNTHESIS_PROMPT_VERSION
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
            await checkpoint_job(session, claim=claim, now=clock(), apply=start)
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
        await _finish(factory, claim, run_id, input_hash, "failed", None, None, clock())
        return True
    usage = None
    intent = None
    status: Literal["completed", "abstained", "failed"] = "failed"
    pre_dispatch = False
    error_code = "PORTFOLIO_SYNTHESIS_FAILED"
    try:
        raw, usage = await transport.invoke(payload)
        if usage.input_tokens > limits[0] or usage.output_tokens > limits[1]:
            usage = None
            error_code = "PORTFOLIO_SYNTHESIS_TOKEN_LIMIT_EXCEEDED"
            raise ValueError("PORTFOLIO_SYNTHESIS_TOKEN_LIMIT")
        intent = PortfolioSynthesisIntentV1.model_validate_json(raw)
        async with factory() as session:
            run = await session.get(AgentPortfolioSynthesisRunRecord, run_id)
            assert run is not None
            frozen = _frozen_run(run)
            validate_portfolio_synthesis_intent(intent, frozen)
            if clock() >= frozen.expires_at:
                intent = None
                error_code = "PORTFOLIO_SYNTHESIS_SESSION_EXPIRED"
                raise ValueError(error_code)
        status = "completed"
    except CodexInvocationError as error:
        usage = error.usage
        pre_dispatch = usage is None and error.phase in {
            "preflight",
            "client_init",
            "account",
            "config_read",
            "thread_start",
        }
        error_code = (
            "PORTFOLIO_SYNTHESIS_PRE_DISPATCH_" + error.phase.upper()
            if pre_dispatch
            else "PORTFOLIO_SYNTHESIS_CALL_UNCERTAIN"
        )
    except ValueError:
        status = (
            "abstained"
            if usage is not None and error_code != "PORTFOLIO_SYNTHESIS_SESSION_EXPIRED"
            else "failed"
        )
        intent = None
    except Exception:
        # Provider errors carry no trusted raw text and never trigger another call.
        intent = None
    if usage is not None and (usage.input_tokens > limits[0] or usage.output_tokens > limits[1]):
        status, intent, usage = "failed", None, None
        error_code = "PORTFOLIO_SYNTHESIS_TOKEN_LIMIT_EXCEEDED"
    if not pre_dispatch and expires_at is not None and clock() >= expires_at:
        status, intent = "failed", None
        error_code = "PORTFOLIO_SYNTHESIS_SESSION_EXPIRED"
    await _finish(
        factory,
        claim,
        run_id,
        input_hash,
        status,
        intent,
        usage,
        clock(),
        pre_dispatch=pre_dispatch,
        error_code=error_code,
    )
    return True


async def _finish(
    factory: async_sessionmaker[AsyncSession],
    claim: ClaimedJob,
    run_id: UUID,
    input_hash: str,
    status: Literal["completed", "abstained", "failed"],
    intent: PortfolioSynthesisIntentV1 | None,
    usage: UsageSnapshot | None,
    now: datetime,
    *,
    pre_dispatch: bool = False,
    error_code: str = "PORTFOLIO_SYNTHESIS_FAILED",
) -> None:
    async def complete(session: AsyncSession) -> JobCompletion:
        await persist_portfolio_synthesis_terminal(
            session,
            run_id=run_id,
            input_hash=input_hash,
            status=status,
            intent=intent,
            input_tokens=None if usage is None else usage.input_tokens,
            output_tokens=None if usage is None else usage.output_tokens,
            now=now,
            provider_call_started=False if pre_dispatch else None,
        )
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
            finished_at=now,
            error_code=error_code if status == "failed" else None,
        )

    async with factory.begin() as session:
        await execute_and_finalize_job(session, claim=claim, checked_at=now, execute=complete)
