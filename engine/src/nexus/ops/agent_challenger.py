"""Local CLI for evidence-backed Agent challenger proposal and human publication."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal
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
    AGENT_CHALLENGER_PROPOSAL_ACTION,
    PROPOSER_PROMPT_HASH,
    PROPOSER_PROMPT_VERSION,
    PROVIDER_PROJECTION_VERSION,
    AgentChallengerError,
    AgentChallengerJobOutputV4,
    AgentChallengerPublicationV4,
    AgentChallengerRetrospectiveV4,
    collect_agent_challenger_retrospective,
    freeze_agent_challenger_proposal_v4,
    publish_agent_challenger,
    verify_agent_challenger_retrospective_v4,
)
from nexus.paper.agent_challenger_runtime import AgentChallengerRuntime, CodexAgentChallengerRuntime
from nexus.persistence.models.agents import AgentModelConfigRecord
from nexus.persistence.models.runtime import AuditEvent, Job
from nexus.settings import AgentRuntimeMode, EngineSettings

_ACTOR = "local-agent-challenger"


class _Request(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", hide_input_in_errors=True)


class ProposeRequest(_Request):
    action: Literal["propose"]
    instrument_id: UUID
    history_limit: int = Field(default=20, ge=4, le=20)
    idempotency_key: str = Field(min_length=1, max_length=255, pattern=r"\S")


class PublishRequest(_Request):
    action: Literal["publish"]
    proposal_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(prog="nexus-engine-agent-challenger")
    commands = root.add_subparsers(dest="action", required=True)
    propose = commands.add_parser("propose")
    propose.add_argument("--instrument-id", required=True)
    propose.add_argument("--history-limit", type=int, default=20)
    propose.add_argument("--idempotency-key", required=True)
    publish = commands.add_parser("publish")
    publish.add_argument("--proposal-hash", required=True)
    return root


async def execute_propose(
    settings: EngineSettings,
    request: ProposeRequest,
    *,
    runtime: AgentChallengerRuntime | None = None,
) -> AgentChallengerJobOutputV4:
    _validate_local_database(settings.database_url)
    if settings.agent_runtime_mode is not AgentRuntimeMode.CODEX_SDK and runtime is None:
        raise AgentChallengerError("AGENT_CHALLENGER_RUNTIME_UNSUPPORTED")
    assert settings.database_url is not None
    engine = create_async_engine(settings.database_url)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    prepared_at = datetime.now(UTC)
    request_hash = job_input_hash(
        {
            "schema_version": "agent_challenger_request_v4",
            "instrument_id": str(request.instrument_id),
            "history_limit": request.history_limit,
        }
    )
    try:
        async with factory.begin() as session:
            await session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:scope, 1419))"),
                {"scope": f"agent-challenger:{request.idempotency_key}"},
            )
            job = await session.scalar(
                select(Job).where(
                    Job.job_type == JobType.AGENT_CHALLENGER_PROPOSE.value,
                    Job.payload["idempotency_key"].as_string() == request.idempotency_key,
                )
            )
            if job is None:
                retrospective = await collect_agent_challenger_retrospective(
                    session,
                    instrument_id=request.instrument_id,
                    as_of=prepared_at,
                    limit=request.history_limit,
                )
                payload: dict[str, object] = {
                    "schema_version": "agent_challenger_job_v4",
                    "idempotency_key": request.idempotency_key,
                    "request_hash": request_hash,
                    "history_limit": request.history_limit,
                    "retrospective": retrospective.model_dump(mode="json"),
                    "proposer_prompt_version": PROPOSER_PROMPT_VERSION,
                    "proposer_prompt_hash": PROPOSER_PROMPT_HASH,
                    "provider_projection_version": PROVIDER_PROJECTION_VERSION,
                }
                job = await enqueue_job(
                    session,
                    key=JobKeyV1(
                        job_type=JobType.AGENT_CHALLENGER_PROPOSE,
                        scope_kind="instrument",
                        scope_id=str(request.instrument_id),
                        effective_at=max(item.decision_slot_at for item in retrospective.cycles),
                        input_hash=job_input_hash(payload),
                    ),
                    schema_version="agent_challenger_job_v4",
                    payload=payload,
                    queue=QueueName.GENERAL,
                    retry_policy=RetryPolicy.NEVER,
                    max_attempts=1,
                )
            elif (
                job.payload.get("schema_version") != "agent_challenger_job_v4"
                or job.payload.get("idempotency_key") != request.idempotency_key
                or job.payload.get("request_hash") != request_hash
                or job.payload.get("history_limit") != request.history_limit
            ):
                raise AgentChallengerError("AGENT_CHALLENGER_IDEMPOTENCY_CONFLICT")

        if job.status == JobStatus.SUCCEEDED.value:
            async with factory() as session:
                return await _load_job_output(session, job.id)
        if job.status != JobStatus.QUEUED.value:
            raise AgentChallengerError("AGENT_CHALLENGER_RESULT_UNKNOWN")
        if (
            job.payload.get("proposer_prompt_version") != PROPOSER_PROMPT_VERSION
            or job.payload.get("proposer_prompt_hash") != PROPOSER_PROMPT_HASH
            or job.payload.get("provider_projection_version") != PROVIDER_PROJECTION_VERSION
        ):
            raise AgentChallengerError("AGENT_CHALLENGER_PROMPT_AUTHORITY_CHANGED")
        retrospective = AgentChallengerRetrospectiveV4.model_validate(job.payload["retrospective"])
        try:
            verify_agent_challenger_retrospective_v4(retrospective)
        except ValueError as error:
            raise AgentChallengerError("AGENT_CHALLENGER_JOB_AUTHORITY_INVALID") from error
        if (
            job.input_hash != job_input_hash(job.payload)
            or job.payload.get("request_hash") != request_hash
            or retrospective.instrument_id != request.instrument_id
        ):
            raise AgentChallengerError("AGENT_CHALLENGER_JOB_AUTHORITY_INVALID")
        async with factory.begin() as session:
            model = await session.get(
                AgentModelConfigRecord, retrospective.incumbent_model_config_id
            )
            if model is None or model.config_hash != retrospective.incumbent_model_hash:
                raise AgentChallengerError("AGENT_CHALLENGER_MODEL_AUTHORITY_CHANGED")
            claim = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id=f"agent-challenger-{job.id}",
                now=prepared_at,
                lease_seconds=135,
                job_ids=(job.id,),
                job_types=(JobType.AGENT_CHALLENGER_PROPOSE.value,),
            )
            if claim is None:
                raise AgentChallengerError("AGENT_CHALLENGER_RESULT_UNKNOWN")
        configured_runtime = runtime or CodexAgentChallengerRuntime(model)
        try:
            suggestion, usage = await configured_runtime.generate(retrospective)
            proposal = freeze_agent_challenger_proposal_v4(retrospective, suggestion)
            output = AgentChallengerJobOutputV4(
                schema_version="agent_challenger_job_output_v4",
                job_id=job.id,
                proposer_prompt_version=PROPOSER_PROMPT_VERSION,
                proposer_prompt_hash=PROPOSER_PROMPT_HASH,
                proposal=proposal,
                input_tokens=usage.input_tokens,
                output_tokens=usage.output_tokens,
                provider_calls=1,
                tool_calls=0,
            )
        except Exception as error:
            failed_at = datetime.now(UTC)
            safe_error = str(error)
            if safe_error not in {
                "AGENT_CHALLENGER_INPUT_TOO_LARGE",
                "AGENT_CHALLENGER_INPUT_TOKENS_EXCEEDED",
                "AGENT_CHALLENGER_SOURCE_MISMATCH",
                "AGENT_CHALLENGER_USAGE_INVALID",
                "CODEX_CHATGPT_LOGIN_REQUIRED",
                "CODEX_INPUT_TOO_LARGE",
                "CODEX_OUTPUT_TOO_LARGE",
                "CODEX_RESPONSE_INCOMPLETE",
                "CODEX_TIMEOUT",
                "CODEX_UNEXPECTED_TOOL",
                "CODEX_USAGE_INVALID",
                "CODEX_USAGE_MISSING",
            }:
                safe_error = "AGENT_CHALLENGER_RESULT_UNKNOWN"
            async with factory.begin() as session:
                await fail_job(
                    session,
                    claim=claim,
                    now=failed_at,
                    error_code=safe_error,
                )
            raise AgentChallengerError("AGENT_CHALLENGER_RESULT_UNKNOWN") from error

        completed_at = datetime.now(UTC)

        async def persist_output(session) -> None:  # type: ignore[no-untyped-def]
            await append_audit_event(
                session,
                actor_id=_ACTOR,
                action=AGENT_CHALLENGER_PROPOSAL_ACTION,
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
                result_code="AGENT_CHALLENGER_PROPOSED",
                apply=persist_output,
            )
        return output
    finally:
        await engine.dispose()


async def execute_publish(
    settings: EngineSettings, request: PublishRequest
) -> AgentChallengerPublicationV4:
    _validate_local_database(settings.database_url)
    assert settings.database_url is not None
    engine = create_async_engine(settings.database_url)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory.begin() as session:
            return await publish_agent_challenger(
                session,
                proposal_hash=request.proposal_hash,
                actor_id="human-local",
                now=datetime.now(UTC),
            )
    finally:
        await engine.dispose()


async def _load_job_output(session, job_id: UUID) -> AgentChallengerJobOutputV4:  # type: ignore[no-untyped-def]
    event = await session.scalar(
        select(AuditEvent).where(
            AuditEvent.action == AGENT_CHALLENGER_PROPOSAL_ACTION,
            AuditEvent.schema_version == "agent_challenger_job_output_v4",
            AuditEvent.payload["job_id"].as_string() == str(job_id),
        )
    )
    if event is None:
        raise AgentChallengerError("AGENT_CHALLENGER_OUTPUT_MISSING")
    return AgentChallengerJobOutputV4.model_validate(event.payload)


def main(argv: list[str] | None = None) -> int:
    try:
        values = vars(parser().parse_args(argv))
        request: ProposeRequest | PublishRequest
        request = (
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
        if not code.startswith("AGENT_CHALLENGER_") and code not in {
            "AGENT_PAPER_DATABASE_NOT_LOCAL",
            "AGENT_PAPER_DATABASE_UNCONFIGURED",
        }:
            code = "AGENT_CHALLENGER_RESULT_UNKNOWN"
        print(json.dumps({"error_code": code}, separators=(",", ":"), sort_keys=True))
        return 1
    print(json.dumps(result.model_dump(mode="json"), separators=(",", ":"), sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
