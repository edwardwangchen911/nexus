"""Explicit local daily admission or at most three scoped worker ticks."""

import argparse
import asyncio
import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Literal, Never
from uuid import UUID, uuid4

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.jobs.daily_manual import (
    ManualDailyRequest,
    build_manual_daily_registry,
    enqueue_manual_daily_root,
    manual_daily_job_ids,
)
from nexus.jobs.daily_pipeline import assert_daily_pipeline_schema_ready
from nexus.jobs.daily_research import admit_daily_research, resolve_daily_research_job
from nexus.jobs.models import JobType, QueueName
from nexus.jobs.worker import run_job_worker_once
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.optimizer.formal_producer import FormalOptimizationRequest, admit_formal_optimization
from nexus.optimizer.initial_draft import InitialDraftUnavailable, create_initial_draft
from nexus.optimizer.jobs import OptimizationConstructionPayload, build_optimization_registry
from nexus.optimizer.research_context import (
    DailyResearchContextRequest,
    freeze_daily_research_context,
)
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import Job
from nexus.settings import AgentRuntimeMode, EngineSettings
from nexus.worker import run_agent_worker_once

_SAFE_CODES = {
    "DAILY_ONCE_REQUEST_INVALID",
    "DAILY_ONCE_DATABASE_UNCONFIGURED",
    "DAILY_ONCE_DATABASE_NOT_LOCAL",
    "DAILY_ONCE_ARTIFACT_READER_UNAVAILABLE",
    "DAILY_ONCE_RESULT_UNKNOWN",
    "DAILY_PIPELINE_DATABASE_HEAD_MISMATCH",
    "MANUAL_DAILY_TIME_INVALID",
    "MANUAL_DAILY_TIME_IN_FUTURE",
    "MANUAL_DAILY_SNAPSHOT_UNAVAILABLE",
    "MANUAL_DAILY_CALENDAR_UNAVAILABLE",
    "MANUAL_DAILY_ROOT_UNAVAILABLE",
    "MANUAL_DAILY_ROOT_INVALID",
    "MANUAL_DAILY_CHILD_INVALID",
    "MANUAL_DAILY_CHILD_MISSING",
    "DAILY_ONCE_RESEARCH_RUNTIME_UNAVAILABLE",
    "DAILY_ONCE_RESEARCH_MODEL_UNAVAILABLE",
    "MANUAL_DAILY_RESEARCH_TIME_INVALID",
    "MANUAL_DAILY_RESEARCH_PARENT_NOT_READY",
    "MANUAL_DAILY_RESEARCH_DIAGNOSIS_UNAVAILABLE",
    "MANUAL_DAILY_RESEARCH_EVIDENCE_UNAVAILABLE",
    "MANUAL_DAILY_RESEARCH_BINDING_INVALID",
    "DAILY_ONCE_DRAFT_INPUT_UNAVAILABLE",
    "DAILY_ONCE_FORMAL_INPUT_UNAVAILABLE",
    "DAILY_ONCE_FORMAL_JOB_UNAVAILABLE",
}


class DailyOnceError(ValueError):
    """Fixed errors only; no request, account data, DSN, or dependency text."""


class DailyOnceSettings(EngineSettings):
    """Remember actual input before EngineSettings fills development fixtures."""

    explicit_artifact_config: bool = Field(default=False, exclude=True)

    @model_validator(mode="before")
    @classmethod
    def _explicit_storage(cls, values: object) -> object:
        if not isinstance(values, dict):
            return values
        fields = (
            "object_store_endpoint",
            "object_store_region",
            "object_store_bucket",
            "object_store_access_key_id",
            "object_store_secret_access_key",
        )
        inputs = [values.get(name, values.get(name.upper())) for name in fields]
        return {
            **values,
            "explicit_artifact_config": all(
                bool((value.get_secret_value() if isinstance(value, SecretStr) else value).strip())
                if isinstance(value, str | SecretStr)
                else False
                for value in inputs
            ),
        }


class DailyAdmitRequest(ManualDailyRequest):
    action: Literal["admit"]


class DailyAdvanceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    action: Literal["advance"]
    root_id: UUID


class DailyResearchAdmitRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    action: Literal["research-admit"]
    root_id: UUID
    instrument_id: UUID
    research_as_of: datetime
    model_config_id: UUID

    @field_validator("research_as_of")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return ManualDailyRequest._utc(value)


class DailyResearchRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    action: Literal["research-run"]
    root_id: UUID
    run_id: UUID


class DailyProposalBuildRequest(DailyResearchContextRequest):
    action: Literal["proposal-build"]
    idempotency_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$")


class DailyFormalProposalAdmitRequest(FormalOptimizationRequest):
    action: Literal["formal-proposal-admit"]
    idempotency_key: str = Field(min_length=1, max_length=128, pattern=r"^[A-Za-z0-9._:-]+$")


class DailyFormalProposalRunRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, hide_input_in_errors=True)

    action: Literal["formal-proposal-run"]
    job_id: UUID


DailyRequest = (
    DailyAdmitRequest
    | DailyAdvanceRequest
    | DailyResearchAdmitRequest
    | DailyResearchRunRequest
    | DailyProposalBuildRequest
    | DailyFormalProposalAdmitRequest
    | DailyFormalProposalRunRequest
)


def _require_research_runtime(settings: EngineSettings) -> None:
    if settings.agent_runtime_mode not in {AgentRuntimeMode.CODEX_SDK, AgentRuntimeMode.RESPONSES}:
        raise DailyOnceError("DAILY_ONCE_RESEARCH_RUNTIME_UNAVAILABLE")
    if not isinstance(settings, DailyOnceSettings) or not settings.explicit_artifact_config:
        raise DailyOnceError("DAILY_ONCE_ARTIFACT_READER_UNAVAILABLE")


async def _require_research_model(
    session: AsyncSession,
    settings: EngineSettings,
    model_id: UUID,
) -> None:
    model = await session.get(AgentModelConfigRecord, model_id)
    if settings.agent_runtime_mode is AgentRuntimeMode.CODEX_SDK:
        from nexus.agents.codex_sdk import (
            CONFIG_VERSION,
            MODEL_ID,
            STAGED_CONFIG_VERSION,
            STRUCTURED_CONFIG_VERSION,
        )

        valid = (
            model is not None
            and model.model_id == MODEL_ID
            and model.version in {CONFIG_VERSION, STAGED_CONFIG_VERSION, STRUCTURED_CONFIG_VERSION}
            and model.input_usd_per_million == 0
            and model.output_usd_per_million == 0
        )
    else:
        valid = (
            model is not None
            and model.model_id == settings.openai_reasoning_model
            and not model.version.startswith("codex-sdk-")
            and settings.openai_api_key is not None
            and bool(settings.openai_api_key.get_secret_value().strip())
        )
    if not valid:
        raise DailyOnceError("DAILY_ONCE_RESEARCH_MODEL_UNAVAILABLE")


async def _research(
    factory: async_sessionmaker[AsyncSession],
    settings: EngineSettings,
    request: DailyResearchAdmitRequest | DailyResearchRunRequest,
) -> dict[str, object]:
    if isinstance(request, DailyResearchAdmitRequest):
        async with factory() as session, session.begin():
            await assert_daily_pipeline_schema_ready(session)
            await _require_research_model(session, settings, request.model_config_id)
            admission = await admit_daily_research(
                session,
                root_id=request.root_id,
                instrument_id=request.instrument_id,
                research_as_of=request.research_as_of,
                model_config_id=request.model_config_id,
                daily_limit_usd=settings.agent_daily_budget_usd,
                now=datetime.now(UTC),
            )
            output: dict[str, object] = {
                "status": admission.run.status,
                "root_id": str(request.root_id),
                "run_id": str(admission.run_id),
                "replayed": admission.replayed,
            }
        return output
    async with factory() as session:
        await assert_daily_pipeline_schema_ready(session)
        job = await resolve_daily_research_job(
            session, root_id=request.root_id, run_id=request.run_id
        )
        run = await session.get(AgentResearchRunRecord, request.run_id)
        assert run is not None
        await _require_research_model(session, settings, run.model_config_id)
        job_id = job.id
    ran = await run_agent_worker_once(
        settings,
        job_ids=(job_id,),
        preclaim_guard=assert_daily_pipeline_schema_ready,
    )
    async with factory() as session:
        job = await resolve_daily_research_job(
            session, root_id=request.root_id, run_id=request.run_id
        )
        run = await session.get(AgentResearchRunRecord, request.run_id)
        assert run is not None
        status = (
            "pending"
            if job.status in {"queued", "running"}
            else "completed"
            if job.status == "succeeded" and run.status == "completed"
            else "blocked"
        )
        return {
            "status": status,
            "root_id": str(request.root_id),
            "run_id": str(request.run_id),
            "processed": int(ran),
            "run_status": run.status,
            "job_status": job.status,
            "result": job.result_code,
        }


async def _advance(
    factory: async_sessionmaker[AsyncSession],
    settings: EngineSettings,
    root_id: UUID,
) -> dict[str, object]:
    # Construct only a read capability; admission does not need object access.
    if not isinstance(settings, DailyOnceSettings) or not settings.explicit_artifact_config:
        raise DailyOnceError("DAILY_ONCE_ARTIFACT_READER_UNAVAILABLE")
    try:
        reader = S3ProviderEvidenceStore(settings).read_normalized
    except Exception as error:
        raise DailyOnceError("DAILY_ONCE_ARTIFACT_READER_UNAVAILABLE") from error
    registry = build_manual_daily_registry(artifact_reader=reader)
    started = datetime.now(UTC)
    worker_id = f"daily-manual-{uuid4().hex}"
    processed = 0
    for _ in range(3):
        async with factory() as session:
            job_ids = await manual_daily_job_ids(session, root_id=root_id)
        ran = await run_job_worker_once(
            factory,
            registry=registry,
            queue=QueueName.GENERAL,
            worker_id=worker_id,
            worker_started_at=started,
            process_version="nexus-daily-manual-v1",
            migration_head=CURRENT_MIGRATION_HEAD,
            clock=lambda: datetime.now(UTC),
            claim_registered_only=True,
            job_ids=job_ids,
            preclaim_guard=assert_daily_pipeline_schema_ready,
        )
        if not ran:
            break
        processed += 1
    async with factory() as session:
        job_ids = await manual_daily_job_ids(session, root_id=root_id)
        stages = []
        statuses = []
        for job_id in job_ids:
            job = await session.get(Job, job_id)
            if job is None:
                raise DailyOnceError("MANUAL_DAILY_CHILD_MISSING")
            statuses.append(job.status)
            stages.append({"stage": job.job_type, "status": job.status, "result": job.result_code})
    status = (
        "pending"
        if any(value in {"queued", "running"} for value in statuses)
        else "completed"
        if len(statuses) == 3 and all(value == "succeeded" for value in statuses)
        else "blocked"
    )
    return {"status": status, "root_id": str(root_id), "processed": processed, "stages": stages}


async def _proposal(
    factory: async_sessionmaker[AsyncSession],
    settings: EngineSettings,
    request: DailyProposalBuildRequest,
) -> dict[str, object]:
    try:
        reader = S3ProviderEvidenceStore(settings).read_normalized
    except Exception as error:
        raise DailyOnceError("DAILY_ONCE_ARTIFACT_READER_UNAVAILABLE") from error
    reference = DailyResearchContextRequest(
        root_id=request.root_id,
        run_id=request.run_id,
        context_as_of=request.context_as_of,
    )
    digest = hashlib.sha256(request.model_dump_json().encode()).hexdigest()
    async with factory() as session, session.begin():
        await assert_daily_pipeline_schema_ready(session)
        try:
            frozen = await freeze_daily_research_context(session, **reference.model_dump())
            group = await create_initial_draft(
                session,
                portfolio_snapshot_id=frozen.portfolio_snapshot_id,
                diagnostic_report_id=frozen.diagnostic_report_id,
                actor_id="local-daily-proposal",
                idempotency_key=request.idempotency_key,
                request_hash=digest,
                now=datetime.now(UTC),
                artifact_reader=reader,
                research_context=reference,
            )
        except (InitialDraftUnavailable, ValueError) as error:
            raise DailyOnceError("DAILY_ONCE_DRAFT_INPUT_UNAVAILABLE") from error
        output: dict[str, object] = {
            "status": "draft_created",
            "proposal_group_id": str(group.group_id),
            "root_id": str(request.root_id),
            "run_id": str(request.run_id),
            "mode": "context_only",
            "execution_locked": True,
        }
    return output


async def _formal_admit(
    factory: async_sessionmaker[AsyncSession], request: DailyFormalProposalAdmitRequest,
) -> dict[str, object]:
    async with factory() as session, session.begin():
        await assert_daily_pipeline_schema_ready(session)
        try:
            result = await admit_formal_optimization(
                session,
                request=FormalOptimizationRequest.model_validate(
                    request.model_dump(exclude={"action", "idempotency_key"}),
                ),
                actor_id="local-daily-formal-proposal", idempotency_key=request.idempotency_key,
            )
        except ValueError as error:
            raise DailyOnceError("DAILY_ONCE_FORMAL_INPUT_UNAVAILABLE") from error
        return {
            "status": "admitted", "job_id": str(result.job_id),
            "data_snapshot_id": str(result.data_snapshot_id), "replayed": result.replayed,
            "mode": "formal_bound_read_only", "execution_locked": True,
        }


async def _formal_run(
    factory: async_sessionmaker[AsyncSession], settings: EngineSettings,
    request: DailyFormalProposalRunRequest,
) -> dict[str, object]:
    async with factory() as session:
        await assert_daily_pipeline_schema_ready(session)
        job = await session.get(Job, request.job_id)
        if (
            job is None or job.job_type != JobType.OPTIMIZATION_PROPOSE.value
            or job.schema_version != "optimization_job_v1" or job.queue != QueueName.GENERAL.value
        ):
            raise DailyOnceError("DAILY_ONCE_FORMAL_JOB_UNAVAILABLE")
        try:
            payload = OptimizationConstructionPayload.model_validate(job.payload)
        except ValueError as error:
            raise DailyOnceError("DAILY_ONCE_FORMAL_JOB_UNAVAILABLE") from error
        if payload.formal_binding is None:
            raise DailyOnceError("DAILY_ONCE_FORMAL_JOB_UNAVAILABLE")
    try:
        reader = S3ProviderEvidenceStore(settings).read_normalized
    except Exception as error:
        raise DailyOnceError("DAILY_ONCE_ARTIFACT_READER_UNAVAILABLE") from error
    processed = await run_job_worker_once(
        factory, registry=build_optimization_registry(artifact_reader=reader),
        queue=QueueName.GENERAL, worker_id=f"daily-formal-{uuid4().hex}",
        worker_started_at=datetime.now(UTC), process_version="nexus-daily-formal-v1",
        migration_head=CURRENT_MIGRATION_HEAD, clock=lambda: datetime.now(UTC),
        claim_registered_only=True, job_ids=(request.job_id,),
        preclaim_guard=assert_daily_pipeline_schema_ready,
    )
    async with factory() as session:
        job = await session.get(Job, request.job_id)
        if job is None:
            raise DailyOnceError("DAILY_ONCE_FORMAL_JOB_UNAVAILABLE")
        return {
            "status": "completed" if job.status == "succeeded" else
            "pending" if job.status in {"queued", "running"} else "blocked",
            "job_id": str(job.id), "job_status": job.status, "processed": int(processed),
            "mode": "formal_bound_read_only", "execution_locked": True,
        }


async def run_once(
    settings: EngineSettings,
    request: DailyRequest,
) -> dict[str, object]:
    """Only research-run invokes one model job; no collection, scheduling or orders."""
    if not settings.database_url:
        raise DailyOnceError("DAILY_ONCE_DATABASE_UNCONFIGURED")
    try:
        url = make_url(settings.database_url)
    except Exception as error:
        raise DailyOnceError("DAILY_ONCE_DATABASE_NOT_LOCAL") from error
    if (
        url.drivername != "postgresql+asyncpg"
        or url.host not in {"localhost", "127.0.0.1", "::1"}
        or bool(set(url.query) & {"host", "hostaddr", "port", "dsn", "database", "dbname"})
    ):
        raise DailyOnceError("DAILY_ONCE_DATABASE_NOT_LOCAL")
    try:
        if isinstance(request, DailyResearchAdmitRequest | DailyResearchRunRequest):
            _require_research_runtime(settings)
        if isinstance(request, DailyProposalBuildRequest | DailyFormalProposalRunRequest) and (
            not isinstance(settings, DailyOnceSettings) or not settings.explicit_artifact_config
        ):
            raise DailyOnceError("DAILY_ONCE_ARTIFACT_READER_UNAVAILABLE")
        engine = create_async_engine(settings.database_url)
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            if isinstance(request, DailyAdvanceRequest):
                return await _advance(factory, settings, request.root_id)
            if isinstance(request, DailyResearchAdmitRequest | DailyResearchRunRequest):
                return await _research(factory, settings, request)
            if isinstance(request, DailyProposalBuildRequest):
                return await _proposal(factory, settings, request)
            if isinstance(request, DailyFormalProposalAdmitRequest):
                return await _formal_admit(factory, request)
            if isinstance(request, DailyFormalProposalRunRequest):
                return await _formal_run(factory, settings, request)
            async with factory() as session, session.begin():
                root = await enqueue_manual_daily_root(
                    session,
                    request=request,
                    now=datetime.now(UTC),
                )
                output: dict[str, object] = {"status": root.status, "root_id": str(root.id)}
            return output
        finally:
            await engine.dispose()
    except DailyOnceError:
        raise
    except Exception as error:
        # A failed observation/commit acknowledgement is not proof of rollback.
        code = str(error) if str(error) in _SAFE_CODES else "DAILY_ONCE_RESULT_UNKNOWN"
        raise DailyOnceError(code) from error


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise DailyOnceError("DAILY_ONCE_REQUEST_INVALID")


def main() -> None:
    parser = _Parser(
        description="显式本地诊断与单次研究；仅research-run调用模型，不采集/调度/下单。"
    )
    commands = parser.add_subparsers(dest="action", required=True)
    admit = commands.add_parser("admit", help="绑定已有快照与真实手动截止")
    for field in ("snapshot-id", "calendar-authority-id", "session", "cutoff-at"):
        admit.add_argument(f"--{field}", required=True)
    advance = commands.add_parser("advance", help="最多推进所选日根的三个本地阶段")
    advance.add_argument("--root-id", required=True)
    research_admit = commands.add_parser(
        "research-admit", help="诊断成功后准入原生研究，不调用模型"
    )
    for field in ("root-id", "instrument-id", "research-as-of", "model-config-id"):
        research_admit.add_argument(f"--{field}", required=True)
    research_run = commands.add_parser("research-run", help="仅运行明确绑定的一项原生研究")
    research_run.add_argument("--root-id", required=True)
    research_run.add_argument("--run-id", required=True)
    proposal = commands.add_parser("proposal-build", help="将已保存研究绑定到不可执行草稿")
    for field in ("root-id", "run-id", "context-as-of", "idempotency-key"):
        proposal.add_argument(f"--{field}", required=True)
    formal_admit = commands.add_parser(
        "formal-proposal-admit", help="精确绑定研究和策略资料，仅登记只读方案Job",
    )
    formal_admit.add_argument("--request-file", required=True)
    formal_run = commands.add_parser(
        "formal-proposal-run", help="最多消费一个明确的只读正式绑定方案Job，不调用模型",
    )
    formal_run.add_argument("--job-id", required=True)
    try:
        args = vars(parser.parse_args())
        if args["action"] == "formal-proposal-admit":
            with Path(args["request_file"]).open("rb") as request_file:
                content = request_file.read(1_048_577)
            if len(content) > 1_048_576:
                raise DailyOnceError("DAILY_ONCE_REQUEST_INVALID")
            loaded = json.loads(content)
            if not isinstance(loaded, dict) or "action" in loaded:
                raise DailyOnceError("DAILY_ONCE_REQUEST_INVALID")
            args = {"action": "formal-proposal-admit", **loaded}
        request_types: dict[
            str,
            type[DailyAdmitRequest]
            | type[DailyAdvanceRequest]
            | type[DailyResearchAdmitRequest]
            | type[DailyResearchRunRequest]
            | type[DailyProposalBuildRequest]
            | type[DailyFormalProposalAdmitRequest]
            | type[DailyFormalProposalRunRequest],
        ] = {
            "admit": DailyAdmitRequest,
            "advance": DailyAdvanceRequest,
            "research-admit": DailyResearchAdmitRequest,
            "research-run": DailyResearchRunRequest,
            "proposal-build": DailyProposalBuildRequest,
            "formal-proposal-admit": DailyFormalProposalAdmitRequest,
            "formal-proposal-run": DailyFormalProposalRunRequest,
        }
        request = request_types[args["action"]].model_validate(args)
    except (ValueError, OSError, RecursionError):
        sys.stderr.write(
            json.dumps({"status": "failed", "code": "DAILY_ONCE_REQUEST_INVALID"}) + "\n"
        )
        raise SystemExit(1) from None
    try:
        result = asyncio.run(run_once(DailyOnceSettings(), request))
    except Exception as error:
        code = str(error) if isinstance(error, DailyOnceError) else "DAILY_ONCE_RESULT_UNKNOWN"
        if code not in _SAFE_CODES:
            code = "DAILY_ONCE_RESULT_UNKNOWN"
        status = "unknown" if code == "DAILY_ONCE_RESULT_UNKNOWN" else "failed"
        sys.stderr.write(json.dumps({"status": status, "code": code}) + "\n")
        raise SystemExit(1) from None
    sys.stdout.write(json.dumps(result, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
