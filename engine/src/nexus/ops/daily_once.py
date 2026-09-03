"""Explicit local daily admission or at most three scoped worker ticks."""

import argparse
import asyncio
import json
import sys
from datetime import UTC, datetime
from typing import Literal, Never
from uuid import UUID, uuid4

from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError, model_validator
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from nexus.jobs.daily_manual import (
    ManualDailyRequest,
    build_manual_daily_registry,
    enqueue_manual_daily_root,
    manual_daily_job_ids,
)
from nexus.jobs.daily_pipeline import assert_daily_pipeline_schema_ready
from nexus.jobs.models import QueueName
from nexus.jobs.worker import run_job_worker_once
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.runtime import Job
from nexus.settings import EngineSettings

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


async def run_once(
    settings: EngineSettings,
    request: DailyAdmitRequest | DailyAdvanceRequest,
) -> dict[str, object]:
    """No scheduling, collection, model, paper, automatic polling, or account writes."""
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
        engine = create_async_engine(settings.database_url)
        try:
            factory = async_sessionmaker(engine, expire_on_commit=False)
            if isinstance(request, DailyAdvanceRequest):
                return await _advance(factory, settings, request.root_id)
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
    parser = _Parser(description="显式运行本地快照诊断；不采集、不调用模型、不调度、不下单。")
    commands = parser.add_subparsers(dest="action", required=True)
    admit = commands.add_parser("admit", help="绑定已有快照与真实手动截止")
    for field in ("snapshot-id", "calendar-authority-id", "session", "cutoff-at"):
        admit.add_argument(f"--{field}", required=True)
    advance = commands.add_parser("advance", help="最多推进所选日根的三个本地阶段")
    advance.add_argument("--root-id", required=True)
    try:
        args = vars(parser.parse_args())
        request = (
            DailyAdmitRequest.model_validate(args)
            if args["action"] == "admit"
            else DailyAdvanceRequest.model_validate(args)
        )
    except (DailyOnceError, ValidationError):
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
