"""Fail-closed process readiness without exposing operational configuration."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol

import boto3  # type: ignore[import-untyped]
from botocore.config import Config  # type: ignore[import-untyped]
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.migrations import (
    CURRENT_GENERAL_WORKER_PROCESS_VERSION,
    CURRENT_MIGRATION_HEAD,
)
from nexus.persistence.models.runtime import WorkerHeartbeat
from nexus.settings import EngineSettings

ComponentStatus = Literal["ready", "unavailable", "incompatible", "stale"]


class ReadinessObjectStore(Protocol):
    """The only object-store capability needed by readiness."""

    async def probe_bucket(self) -> None: ...


class S3ReadinessObjectStore:
    """Bounded S3-compatible HEAD-bucket probe with no object listing."""

    def __init__(self, settings: EngineSettings) -> None:
        endpoint = settings.object_store_endpoint
        region = settings.object_store_region
        bucket = settings.object_store_bucket
        access_key = settings.object_store_access_key_id
        secret_key = settings.object_store_secret_access_key
        if any(
            value is None
            for value in (
                endpoint,
                region,
                bucket,
                access_key,
                secret_key,
            )
        ):
            raise ValueError("object-store settings are incomplete")
        assert endpoint is not None
        assert region is not None
        assert bucket is not None
        assert access_key is not None
        assert secret_key is not None
        self._bucket = bucket
        self._client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            region_name=region,
            aws_access_key_id=access_key.get_secret_value(),
            aws_secret_access_key=secret_key.get_secret_value(),
            config=Config(
                connect_timeout=2,
                read_timeout=2,
                retries={"max_attempts": 0},
                s3={"addressing_style": "path"},
            ),
        )

    async def probe_bucket(self) -> None:
        await asyncio.to_thread(self._client.head_bucket, Bucket=self._bucket)


async def probe_database(
    session: AsyncSession,
) -> ComponentStatus:
    """Check only PostgreSQL reachability."""
    await session.execute(text("SELECT 1"))
    return "ready"


async def probe_migration(session: AsyncSession) -> ComponentStatus:
    """Require the database to match the sole code-side Alembic head."""
    version = await session.scalar(text("SELECT version_num FROM alembic_version"))
    return "ready" if version == CURRENT_MIGRATION_HEAD else "incompatible"


async def probe_general_worker(
    session: AsyncSession,
    *,
    now: datetime | None = None,
    clock: Callable[[], datetime] | None = None,
) -> ComponentStatus:
    """Require a compatible general worker heartbeat no older than 30 seconds."""
    heartbeat = await session.scalar(
        select(WorkerHeartbeat)
        .where(
            WorkerHeartbeat.queue == "general",
            WorkerHeartbeat.process_version == CURRENT_GENERAL_WORKER_PROCESS_VERSION,
            WorkerHeartbeat.migration_head == CURRENT_MIGRATION_HEAD,
        )
        .order_by(WorkerHeartbeat.seen_at.desc(), WorkerHeartbeat.id.desc())
        .limit(1)
    )
    if heartbeat is None:
        return "unavailable"
    observed_at = now if now is not None else (clock or _utc_now)()
    if observed_at.tzinfo is None or observed_at.utcoffset() != UTC.utcoffset(observed_at):
        raise ValueError("readiness time must use UTC")
    if (
        heartbeat.seen_at > observed_at
        or observed_at - heartbeat.seen_at > timedelta(seconds=30)
    ):
        return "stale"
    return "ready"


def _utc_now() -> datetime:
    return datetime.now(UTC)


async def object_store_readiness(
    probe: ReadinessObjectStore | None,
) -> ComponentStatus:
    """Collapse every object-store failure to one non-sensitive status."""
    if probe is None:
        return "unavailable"
    try:
        await probe.probe_bucket()
    except Exception:
        return "unavailable"
    return "ready"


def overall_readiness(components: Mapping[str, ComponentStatus]) -> bool:
    return set(components) == {"database", "migration", "object_store", "worker"} and all(
        status == "ready" for status in components.values()
    )
