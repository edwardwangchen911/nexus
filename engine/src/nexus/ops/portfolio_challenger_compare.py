"""Once-only prospective comparison of a published portfolio prompt, without orders."""

from __future__ import annotations

import argparse
import asyncio
import json
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexus.agents.portfolio_synthesis_contracts import (
    PortfolioSynthesisIntentV1,
    validate_portfolio_synthesis_intent,
)
from nexus.agents.portfolio_synthesis_persistence import _frozen_run
from nexus.agents.portfolio_synthesis_worker import CodexSynthesisTransport, SynthesisTransport
from nexus.audit.service import append_audit_event
from nexus.jobs.models import JobKeyV1, JobStatus, JobType, QueueName, RetryPolicy, job_input_hash
from nexus.jobs.repository import claim_next_job, enqueue_job, fail_job, finalize_job
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.fosun_local import load_local_environment
from nexus.paper.agent_challenger import (
    PORTFOLIO_CHALLENGER_PUBLICATION_ACTION,
    AgentChallengerError,
    PortfolioChallengerPublicationV1,
)
from nexus.paper.portfolio_challenger_case import (
    compare_portfolio_challenger_decisions,
    freeze_prospective_portfolio_challenger_case,
)
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPortfolioSynthesisOutputRecord,
    AgentPortfolioSynthesisRunRecord,
    AgentPromptVersionRecord,
)
from nexus.persistence.models.runtime import AuditEvent, Job, JobLease
from nexus.settings import AgentRuntimeMode, EngineSettings

_JOB_SCHEMA = "portfolio_challenger_cmp_v1"
_OUTPUT_SCHEMA = "portfolio_challenger_result_v1"
_OUTPUT_ACTION = "portfolio.challenger.compared"
_SAFE_ERROR_CODES = frozenset(
    {
        "PORTFOLIO_CHALLENGER_SOURCE_INVALID",
        "PORTFOLIO_CHALLENGER_PUBLICATION_INVALID",
        "PORTFOLIO_CHALLENGER_CASE_NOT_PROSPECTIVE",
        "PORTFOLIO_CHALLENGER_MODEL_AUTHORITY_CHANGED",
        "PORTFOLIO_CHALLENGER_IDEMPOTENCY_CONFLICT",
        "PORTFOLIO_CHALLENGER_RUNTIME_UNSUPPORTED",
        "PORTFOLIO_CHALLENGER_RESULT_UNKNOWN",
    }
)


class CompareRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    cycle_id: UUID
    proposal_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class ComparisonOutput(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    schema_version: Literal["portfolio_challenger_result_v1"]
    job_id: UUID
    source_run_id: UUID
    proposal_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_input_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    challenger_input_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    challenger_prompt_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    challenger_prompt_version: str = Field(min_length=1, max_length=100)
    intent: PortfolioSynthesisIntentV1
    input_tokens: int = Field(ge=0)
    output_tokens: int = Field(ge=0)
    provider_calls: Literal[1]
    tool_calls: Literal[0]


class ComparisonStatus(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    status: Literal["not_run", "pending", "result_unknown", "incumbent_pending", "compared"]
    job_id: UUID | None = None
    selection_changed: bool | None = None
    reasoning_changed: bool | None = None
    changed_candidate_count: int | None = Field(default=None, ge=0, le=5)
    economic_result: Literal["not_evaluated"] = "not_evaluated"


async def _read_output(session, job: Job) -> ComparisonOutput:  # type: ignore[no-untyped-def]
    events = tuple(
        await session.scalars(
            select(AuditEvent).where(
                AuditEvent.action == _OUTPUT_ACTION,
                AuditEvent.schema_version == _OUTPUT_SCHEMA,
                AuditEvent.payload["job_id"].as_string() == str(job.id),
            )
        )
    )
    if len(events) != 1:
        raise AgentChallengerError("PORTFOLIO_CHALLENGER_RESULT_UNKNOWN")
    output = ComparisonOutput.model_validate(events[0].payload)
    if (
        job.status != JobStatus.SUCCEEDED.value
        or job.job_type != JobType.AGENT_PORTFOLIO_CHALLENGER_COMPARE.value
        or job.schema_version != _JOB_SCHEMA
        or job.result_code != "PORTFOLIO_CHALLENGER_COMPARED"
        or job.retry_policy != RetryPolicy.NEVER.value
        or job.max_attempts != 1
        or job.attempt_count != 1
        or job.input_hash != job_input_hash(job.payload)
        or output.job_id != job.id
        or job.payload.get("source_run_id") != str(output.source_run_id)
        or job.payload.get("proposal_hash") != output.proposal_hash
        or job.payload.get("source_input_hash") != output.source_input_hash
        or job.payload.get("challenger_input_hash") != output.challenger_input_hash
        or job.payload.get("challenger_prompt_hash") != output.challenger_prompt_hash
        or job.payload.get("challenger_prompt_version") != output.challenger_prompt_version
    ):
        raise AgentChallengerError("PORTFOLIO_CHALLENGER_RESULT_UNKNOWN")
    return output


async def inspect_compare(settings: EngineSettings, request: CompareRequest) -> ComparisonStatus:
    """Read persisted comparison and incumbent decision without model or paper writes."""
    _validate_local_database(settings.database_url)
    assert settings.database_url is not None
    engine = create_async_engine(settings.database_url)
    try:
        async with async_sessionmaker(engine)() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            run = await session.scalar(
                select(AgentPortfolioSynthesisRunRecord).where(
                    AgentPortfolioSynthesisRunRecord.cycle_id == request.cycle_id
                )
            )
            if run is None:
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_SOURCE_INVALID")
            jobs = tuple(
                await session.scalars(
                    select(Job).where(
                        Job.job_type == JobType.AGENT_PORTFOLIO_CHALLENGER_COMPARE.value,
                        Job.payload["source_run_id"].as_string() == str(run.id),
                        Job.payload["proposal_hash"].as_string() == request.proposal_hash,
                    )
                )
            )
            if not jobs:
                return ComparisonStatus(status="not_run")
            if len(jobs) != 1:
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_RESULT_UNKNOWN")
            job = jobs[0]
            if job.status == JobStatus.QUEUED.value:
                return ComparisonStatus(status="pending", job_id=job.id)
            if job.status == JobStatus.RUNNING.value:
                lease = await session.scalar(
                    select(JobLease).where(JobLease.job_id == job.id)
                )
                database_now = await session.scalar(select(func.clock_timestamp()))
                if lease is None or database_now is None or database_now >= lease.expires_at:
                    return ComparisonStatus(status="result_unknown", job_id=job.id)
                return ComparisonStatus(status="pending", job_id=job.id)
            if job.status != JobStatus.SUCCEEDED.value:
                return ComparisonStatus(status="result_unknown", job_id=job.id)
            candidate_output = await _read_output(session, job)
            incumbent_output = await session.scalar(
                select(AgentPortfolioSynthesisOutputRecord).where(
                    AgentPortfolioSynthesisOutputRecord.run_id == run.id
                )
            )
            if incumbent_output is None or incumbent_output.status != "completed":
                return ComparisonStatus(status="incumbent_pending", job_id=job.id)
            publication_id = job.payload.get("publication_event_id")
            publication_event = (
                await session.get(AuditEvent, UUID(publication_id))
                if isinstance(publication_id, str)
                else None
            )
            if (
                publication_event is None
                or publication_event.action != PORTFOLIO_CHALLENGER_PUBLICATION_ACTION
                or publication_event.schema_version != "portfolio_challenger_pub_v1"
                or publication_event.created_at > run.created_at
            ):
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_RESULT_UNKNOWN")
            publication = PortfolioChallengerPublicationV1.model_validate(
                publication_event.payload
            )
            incumbent_prompt = await session.get(
                AgentPromptVersionRecord, publication.incumbent_prompt_version_id
            )
            challenger_prompt = await session.get(
                AgentPromptVersionRecord, publication.challenger_prompt_version_id
            )
            if incumbent_prompt is None or challenger_prompt is None:
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_RESULT_UNKNOWN")
            incumbent_input = _frozen_run(run)
            case = freeze_prospective_portfolio_challenger_case(
                run=run,
                incumbent_prompt=incumbent_prompt,
                challenger_prompt=challenger_prompt,
                publication=publication,
                published_at=publication_event.occurred_at,
                now=incumbent_input.admitted_at,
            )
            if (
                candidate_output.source_input_hash != run.input_hash
                or candidate_output.challenger_input_hash != case.challenger_input_hash
                or incumbent_output.intent is None
            ):
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_RESULT_UNKNOWN")
            difference = compare_portfolio_challenger_decisions(
                incumbent_input=incumbent_input,
                incumbent_intent=PortfolioSynthesisIntentV1.model_validate(
                    incumbent_output.intent
                ),
                challenger_input=case.frozen_input,
                challenger_intent=candidate_output.intent,
            )
            return ComparisonStatus(
                status="compared",
                job_id=job.id,
                selection_changed=difference.selection_changed,
                reasoning_changed=difference.reasoning_changed,
                changed_candidate_count=len(difference.changed_instrument_ids),
            )
    finally:
        await engine.dispose()


async def execute_compare(
    settings: EngineSettings,
    request: CompareRequest,
    *,
    transport: SynthesisTransport | None = None,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> ComparisonOutput:
    """Invoke exactly one challenger model call on a future frozen case."""
    _validate_local_database(settings.database_url)
    if settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK and transport is None:
        raise AgentChallengerError("PORTFOLIO_CHALLENGER_RUNTIME_UNSUPPORTED")
    assert settings.database_url is not None
    engine = create_async_engine(settings.database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    case = None
    model = None
    job = None
    try:
        async with factory.begin() as session:
            await session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1423))"),
                {
                    "scope": (
                        f"portfolio-challenger-compare:{request.cycle_id}:"
                        f"{request.proposal_hash}"
                    )
                },
            )
            run = await session.scalar(
                select(AgentPortfolioSynthesisRunRecord).where(
                    AgentPortfolioSynthesisRunRecord.cycle_id == request.cycle_id
                )
            )
            if run is None:
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_SOURCE_INVALID")
            existing = tuple(
                await session.scalars(
                    select(Job).where(
                        Job.job_type == JobType.AGENT_PORTFOLIO_CHALLENGER_COMPARE.value,
                        Job.payload["source_run_id"].as_string() == str(run.id),
                        Job.payload["proposal_hash"].as_string() == request.proposal_hash,
                    )
                )
            )
            if len(existing) > 1:
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_RESULT_UNKNOWN")
            if existing:
                job = existing[0]
                if job.status == JobStatus.SUCCEEDED.value:
                    return await _read_output(session, job)
                if job.status != JobStatus.QUEUED.value:
                    raise AgentChallengerError("PORTFOLIO_CHALLENGER_RESULT_UNKNOWN")
                # A queued attempt may only continue with its original frozen authority.
            publications = tuple(
                await session.scalars(
                    select(AuditEvent).where(
                        AuditEvent.action == PORTFOLIO_CHALLENGER_PUBLICATION_ACTION,
                        AuditEvent.schema_version == "portfolio_challenger_pub_v1",
                        AuditEvent.payload["proposal_hash"].as_string() == request.proposal_hash,
                    )
                )
            )
            if len(publications) != 1:
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_PUBLICATION_INVALID")
            publication_event = publications[0]
            if publication_event.created_at > run.created_at:
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_CASE_NOT_PROSPECTIVE")
            publication = PortfolioChallengerPublicationV1.model_validate(
                publication_event.payload
            )
            incumbent = await session.get(
                AgentPromptVersionRecord, publication.incumbent_prompt_version_id
            )
            challenger = await session.get(
                AgentPromptVersionRecord, publication.challenger_prompt_version_id
            )
            model = await session.get(
                AgentModelConfigRecord, publication.challenger_model_config_id
            )
            if incumbent is None or challenger is None or model is None:
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_PUBLICATION_INVALID")
            case = freeze_prospective_portfolio_challenger_case(
                run=run,
                incumbent_prompt=incumbent,
                challenger_prompt=challenger,
                publication=publication,
                published_at=publication_event.occurred_at,
                now=clock(),
            )
            configured_transport = transport or CodexSynthesisTransport()
            observed_at = clock()
            if (
                model.config_hash != run.model_hash
                or model.created_at > case.frozen_input.admitted_at
                or not model.pricing_available_at <= observed_at < model.pricing_expires_at
                or not configured_transport.supports(model)
            ):
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_MODEL_AUTHORITY_CHANGED")
            payload: dict[str, object] = {
                "schema_version": _JOB_SCHEMA,
                "source_run_id": str(run.id),
                "proposal_hash": request.proposal_hash,
                "source_input_hash": case.source_input_hash,
                "challenger_input_hash": case.challenger_input_hash,
                "challenger_prompt_hash": case.challenger_prompt_hash,
                "challenger_prompt_version": case.challenger_prompt_version,
                "publication_event_id": str(publication_event.id),
            }
            if job is None:
                job = await enqueue_job(
                    session,
                    key=JobKeyV1(
                        job_type=JobType.AGENT_PORTFOLIO_CHALLENGER_COMPARE,
                        scope_kind="portfolio",
                        scope_id=str(case.frozen_input.portfolio_id),
                        effective_at=case.frozen_input.decision_time,
                        input_hash=job_input_hash(payload),
                    ),
                    schema_version=_JOB_SCHEMA,
                    payload=payload,
                    queue=QueueName.GENERAL,
                    retry_policy=RetryPolicy.NEVER,
                    max_attempts=1,
                )
            elif job.payload != payload or job.input_hash != job_input_hash(payload):
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_IDEMPOTENCY_CONFLICT")
        assert job is not None and case is not None and model is not None
        async with factory.begin() as session:
            claim = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id=f"portfolio-challenger-compare-{job.id}",
                now=clock(),
                lease_seconds=135,
                job_ids=(job.id,),
                job_types=(JobType.AGENT_PORTFOLIO_CHALLENGER_COMPARE.value,),
            )
            if claim is None:
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_RESULT_UNKNOWN")
        try:
            raw, usage = await configured_transport.invoke(
                {
                    "instructions": case.instructions,
                    "input_hash": case.challenger_input_hash,
                    "frozen_input": case.frozen_input.model_dump(mode="json"),
                }
            )
            intent = PortfolioSynthesisIntentV1.model_validate_json(raw)
            validate_portfolio_synthesis_intent(intent, case.frozen_input)
            if (
                clock() >= case.frozen_input.expires_at
                or usage.provider_calls != 1
                or usage.tool_calls != 0
                or usage.input_tokens > model.max_input_tokens
                or usage.output_tokens > model.max_output_tokens
            ):
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_RESULT_INVALID")
            output = ComparisonOutput(
                schema_version="portfolio_challenger_result_v1",
                job_id=job.id,
                source_run_id=case.source_run_id,
                proposal_hash=request.proposal_hash,
                source_input_hash=case.source_input_hash,
                challenger_input_hash=case.challenger_input_hash,
                challenger_prompt_hash=case.challenger_prompt_hash,
                challenger_prompt_version=case.challenger_prompt_version,
                intent=intent,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                provider_calls=1,
                tool_calls=0,
            )
        except Exception as error:
            async with factory.begin() as session:
                await fail_job(
                    session,
                    claim=claim,
                    now=clock(),
                    error_code="PORTFOLIO_CHALLENGER_COMPARE_UNAVAILABLE",
                )
            raise AgentChallengerError("PORTFOLIO_CHALLENGER_RESULT_UNKNOWN") from error

        async def persist_output(session) -> None:  # type: ignore[no-untyped-def]
            await append_audit_event(
                session,
                actor_id="local-portfolio-challenger",
                action=_OUTPUT_ACTION,
                schema_version=_OUTPUT_SCHEMA,
                payload=output.model_dump(mode="json"),
                occurred_at=clock(),
            )

        async with factory.begin() as session:
            await finalize_job(
                session,
                claim=claim,
                now=clock(),
                outcome="succeeded",
                result_code="PORTFOLIO_CHALLENGER_COMPARED",
                apply=persist_output,
            )
        return output
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="nexus-engine-portfolio-challenger-compare")
    parser.add_argument("--cycle-id", required=True)
    parser.add_argument("--proposal-hash", required=True)
    parser.add_argument("--status", action="store_true", help="只读核对已保存的对照结果")
    try:
        values = vars(parser.parse_args(argv))
        status_only = values.pop("status")
        request = CompareRequest.model_validate(values)
        settings = EngineSettings.model_validate(load_local_environment(Path.cwd(), {}))
        if status_only:
            status = asyncio.run(inspect_compare(settings, request))
        else:
            result = asyncio.run(execute_compare(settings, request))
    except Exception as error:
        code = str(error)
        if code not in _SAFE_ERROR_CODES:
            code = "PORTFOLIO_CHALLENGER_RESULT_UNKNOWN"
        print(json.dumps({"error_code": code}, sort_keys=True))
        return 1
    if status_only:
        print(json.dumps(status.model_dump(mode="json"), sort_keys=True))
        return 0
    print(
        json.dumps(
            {
                "status": "completed",
                "job_id": str(result.job_id),
                "result_code": "PORTFOLIO_CHALLENGER_COMPARED",
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
