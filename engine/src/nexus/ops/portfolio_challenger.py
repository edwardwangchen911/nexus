"""Local, once-only proposal entry for the portfolio-decision prompt."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from nexus.audit.service import append_audit_event
from nexus.jobs.models import (
    JobKeyV1,
    JobStatus,
    JobType,
    QueueName,
    RetryPolicy,
    job_input_hash,
)
from nexus.jobs.repository import claim_next_job, enqueue_job, fail_job, finalize_job
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.fosun_local import load_local_environment
from nexus.paper.agent_challenger import (
    PORTFOLIO_CHALLENGER_PROPOSAL_ACTION,
    PORTFOLIO_PROPOSER_PROMPT_HASH,
    PORTFOLIO_PROPOSER_PROMPT_VERSION,
    PORTFOLIO_PROVIDER_PROJECTION_VERSION,
    AgentChallengerError,
    PortfolioChallengerJobOutputV1,
    PortfolioChallengerPublicationV1,
    PortfolioChallengerRetrospectiveV1,
    collect_portfolio_challenger_retrospective,
    freeze_portfolio_challenger_proposal_v1,
    publish_portfolio_challenger,
)
from nexus.paper.agent_challenger_runtime import (
    CodexPortfolioChallengerRuntime,
    PortfolioChallengerRuntime,
)
from nexus.persistence.models.agents import AgentModelConfigRecord
from nexus.persistence.models.runtime import AuditEvent, Job
from nexus.settings import AgentRuntimeMode, EngineSettings

_ACTOR = "local-portfolio-challenger"


class ProposeRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    action: Literal["propose"]
    account_id: UUID
    history_limit: int = Field(default=20, ge=1, le=20)
    idempotency_key: str = Field(min_length=1, max_length=255, pattern=r"\S")


class PublishRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)

    action: Literal["publish"]
    proposal_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="nexus-engine-portfolio-challenger")
    commands = root.add_subparsers(dest="action", required=True)
    propose = commands.add_parser("propose")
    propose.add_argument("--account-id", required=True)
    propose.add_argument("--history-limit", type=int, default=20)
    propose.add_argument("--idempotency-key", required=True)
    publish = commands.add_parser("publish")
    publish.add_argument("--proposal-hash", required=True)
    return root


async def execute_propose(
    settings: EngineSettings,
    request: ProposeRequest,
    *,
    runtime: PortfolioChallengerRuntime | None = None,
) -> PortfolioChallengerJobOutputV1:
    _validate_local_database(settings.database_url)
    if settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK and runtime is None:
        raise AgentChallengerError("PORTFOLIO_CHALLENGER_RUNTIME_UNSUPPORTED")
    assert settings.database_url is not None
    engine = create_async_engine(settings.database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    prepared_at = datetime.now(UTC)
    request_hash = job_input_hash(
        {
            "schema_version": "portfolio_challenger_request_v1",
            "account_id": str(request.account_id),
            "history_limit": request.history_limit,
        }
    )
    try:
        async with factory.begin() as session:
            await session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1420))"),
                {"scope": f"portfolio-challenger:{request.idempotency_key}"},
            )
            job = await session.scalar(
                select(Job).where(
                    Job.job_type == JobType.AGENT_CHALLENGER_PROPOSE.value,
                    Job.payload["idempotency_key"].as_string() == request.idempotency_key,
                )
            )
            if job is None:
                retrospective = await collect_portfolio_challenger_retrospective(
                    session,
                    account_id=request.account_id,
                    as_of=prepared_at,
                    limit=request.history_limit,
                )
                payload: dict[str, object] = {
                    "schema_version": "portfolio_challenger_job_v1",
                    "idempotency_key": request.idempotency_key,
                    "request_hash": request_hash,
                    "history_limit": request.history_limit,
                    "retrospective": retrospective.model_dump(mode="json"),
                    "proposer_prompt_version": PORTFOLIO_PROPOSER_PROMPT_VERSION,
                    "proposer_prompt_hash": PORTFOLIO_PROPOSER_PROMPT_HASH,
                    "provider_projection_version": PORTFOLIO_PROVIDER_PROJECTION_VERSION,
                }
                job = await enqueue_job(
                    session,
                    key=JobKeyV1(
                        job_type=JobType.AGENT_CHALLENGER_PROPOSE,
                        scope_kind="portfolio",
                        scope_id=str(retrospective.portfolio_id),
                        effective_at=max(item.decision_slot_at for item in retrospective.cycles),
                        input_hash=job_input_hash(payload),
                    ),
                    schema_version="portfolio_challenger_job_v1",
                    payload=payload,
                    queue=QueueName.GENERAL,
                    retry_policy=RetryPolicy.NEVER,
                    max_attempts=1,
                )
            elif (
                job.payload.get("schema_version") != "portfolio_challenger_job_v1"
                or job.payload.get("idempotency_key") != request.idempotency_key
                or job.payload.get("request_hash") != request_hash
                or job.payload.get("history_limit") != request.history_limit
            ):
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_IDEMPOTENCY_CONFLICT")

        if job.status == JobStatus.SUCCEEDED.value:
            async with factory() as session:
                return await _load_job_output(session, job.id)
        if job.status != JobStatus.QUEUED.value:
            raise AgentChallengerError("PORTFOLIO_CHALLENGER_RESULT_UNKNOWN")
        if (
            job.payload.get("proposer_prompt_version") != PORTFOLIO_PROPOSER_PROMPT_VERSION
            or job.payload.get("proposer_prompt_hash") != PORTFOLIO_PROPOSER_PROMPT_HASH
            or job.payload.get("provider_projection_version")
            != PORTFOLIO_PROVIDER_PROJECTION_VERSION
        ):
            raise AgentChallengerError("PORTFOLIO_CHALLENGER_PROMPT_AUTHORITY_CHANGED")
        try:
            retrospective = PortfolioChallengerRetrospectiveV1.model_validate(
                job.payload["retrospective"]
            )
        except (KeyError, TypeError, ValueError) as error:
            raise AgentChallengerError("PORTFOLIO_CHALLENGER_JOB_AUTHORITY_INVALID") from error
        if (
            job.input_hash != job_input_hash(job.payload)
            or job.payload.get("request_hash") != request_hash
            or retrospective.account_id != request.account_id
        ):
            raise AgentChallengerError("PORTFOLIO_CHALLENGER_JOB_AUTHORITY_INVALID")
        async with factory.begin() as session:
            model = await session.get(
                AgentModelConfigRecord, retrospective.incumbent_model_config_id
            )
            if model is None or model.config_hash != retrospective.incumbent_model_hash:
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_MODEL_AUTHORITY_CHANGED")
            claim = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id=f"portfolio-challenger-{job.id}",
                now=prepared_at,
                lease_seconds=135,
                job_ids=(job.id,),
                job_types=(JobType.AGENT_CHALLENGER_PROPOSE.value,),
            )
            if claim is None:
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_RESULT_UNKNOWN")
        try:
            configured_runtime = runtime or CodexPortfolioChallengerRuntime(model)
            suggestion, usage = await configured_runtime.generate(retrospective)
            if (
                usage.provider_calls != 1
                or usage.tool_calls != 0
                or usage.input_tokens < 0
                or usage.output_tokens < 0
            ):
                raise AgentChallengerError("PORTFOLIO_CHALLENGER_USAGE_INVALID")
            proposal = freeze_portfolio_challenger_proposal_v1(retrospective, suggestion)
            output = PortfolioChallengerJobOutputV1(
                schema_version="portfolio_challenger_output_v1",
                job_id=job.id,
                proposer_prompt_version=cast(
                    Literal["portfolio-challenger-review-v1"], PORTFOLIO_PROPOSER_PROMPT_VERSION
                ),
                proposer_prompt_hash=PORTFOLIO_PROPOSER_PROMPT_HASH,
                proposal=proposal,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                provider_calls=1,
                tool_calls=0,
            )
        except Exception as error:
            safe_error = str(error)
            if safe_error not in {
                "PORTFOLIO_CHALLENGER_INPUT_TOO_LARGE",
                "PORTFOLIO_CHALLENGER_SOURCE_MISMATCH",
                "PORTFOLIO_CHALLENGER_USAGE_INVALID",
                "CODEX_CHATGPT_LOGIN_REQUIRED",
                "CODEX_INPUT_TOO_LARGE",
                "CODEX_OUTPUT_TOO_LARGE",
                "CODEX_RESPONSE_INCOMPLETE",
                "CODEX_TIMEOUT",
                "CODEX_UNEXPECTED_TOOL",
                "CODEX_USAGE_INVALID",
                "CODEX_USAGE_MISSING",
            }:
                safe_error = "PORTFOLIO_CHALLENGER_RESULT_UNKNOWN"
            async with factory.begin() as session:
                await fail_job(session, claim=claim, now=datetime.now(UTC), error_code=safe_error)
            raise AgentChallengerError("PORTFOLIO_CHALLENGER_RESULT_UNKNOWN") from error

        completed_at = datetime.now(UTC)

        async def persist_output(session) -> None:  # type: ignore[no-untyped-def]
            await append_audit_event(
                session,
                actor_id=_ACTOR,
                action=PORTFOLIO_CHALLENGER_PROPOSAL_ACTION,
                schema_version=output.schema_version,
                payload=output.model_dump(mode="json"),
                occurred_at=completed_at,
            )

        async with factory.begin() as session:
            await finalize_job(
                session,
                claim=claim,
                now=completed_at,
                outcome="succeeded",
                result_code="PORTFOLIO_CHALLENGER_PROPOSED",
                apply=persist_output,
            )
        return output
    finally:
        await engine.dispose()


async def _load_job_output(session, job_id: UUID) -> PortfolioChallengerJobOutputV1:  # type: ignore[no-untyped-def]
    event = await session.scalar(
        select(AuditEvent).where(
            AuditEvent.action == PORTFOLIO_CHALLENGER_PROPOSAL_ACTION,
            AuditEvent.schema_version == "portfolio_challenger_output_v1",
            AuditEvent.payload["job_id"].as_string() == str(job_id),
        )
    )
    if event is None:
        raise AgentChallengerError("PORTFOLIO_CHALLENGER_OUTPUT_MISSING")
    return PortfolioChallengerJobOutputV1.model_validate(event.payload)


async def execute_publish(
    settings: EngineSettings, request: PublishRequest
) -> PortfolioChallengerPublicationV1:
    _validate_local_database(settings.database_url)
    assert settings.database_url is not None
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory.begin() as session:
            return await publish_portfolio_challenger(
                session,
                proposal_hash=request.proposal_hash,
                actor_id="human-local",
                now=datetime.now(UTC),
            )
    finally:
        await engine.dispose()


def main(argv: list[str] | None = None) -> int:
    try:
        values = vars(parser().parse_args(argv))
        request: ProposeRequest | PublishRequest = (
            ProposeRequest.model_validate(values)
            if values.get("action") == "propose"
            else PublishRequest.model_validate(values)
        )
        settings = EngineSettings.model_validate(load_local_environment(Path.cwd(), {}))
        result = asyncio.run(
            execute_propose(settings, request)
            if isinstance(request, ProposeRequest)
            else execute_publish(settings, request)
        )
    except Exception as error:
        code = str(error)
        if not code.startswith("PORTFOLIO_CHALLENGER_") and code not in {
            "AGENT_PAPER_DATABASE_NOT_LOCAL",
            "AGENT_PAPER_DATABASE_UNCONFIGURED",
        }:
            code = "PORTFOLIO_CHALLENGER_RESULT_UNKNOWN"
        print(json.dumps({"error_code": code}, separators=(",", ":"), sort_keys=True))
        return 1
    print(json.dumps(result.model_dump(mode="json"), separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
