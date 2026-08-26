from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from helpers.hmac import (
    TEST_HMAC_KEY_ID,
    TEST_HMAC_SECRET,
    build_test_hmac_headers,
)
from nexus.domain.ids import new_uuid7
from nexus.main import create_engine_app
from nexus.persistence.migrations import (
    CURRENT_GENERAL_WORKER_PROCESS_VERSION,
    CURRENT_MIGRATION_HEAD,
)
from nexus.persistence.models.runtime import IntegrationStatus, WorkerHeartbeat
from nexus.settings import EngineSettings
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

_NOW = datetime(2026, 8, 27, 12, tzinfo=UTC)


class _BucketProbe:
    def __init__(self) -> None:
        self.available = True
        self.calls = 0

    async def probe_bucket(self) -> None:
        self.calls += 1
        if not self.available:
            raise RuntimeError("private endpoint and bucket must never escape")


def _settings(database_url: str) -> EngineSettings:
    return EngineSettings(
        NEXUS_ENV="test",
        DATABASE_URL=database_url,
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID=TEST_HMAC_KEY_ID,
        NEXUS_ENGINE_HMAC_CURRENT_SECRET=TEST_HMAC_SECRET,
    )


def _headers() -> dict[str, str]:
    return build_test_hmac_headers("GET", "/health/ready", b"", nonce=str(new_uuid7()))


@pytest.mark.asyncio
async def test_readiness_is_hmac_protected_and_each_dependency_fails_closed(
    migrated_database_url: str,
) -> None:
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    probe = _BucketProbe()
    app = create_engine_app(_settings(migrated_database_url), readiness_object_store=probe)
    app.state.utc_clock = lambda: _NOW
    try:
        async with sessions.begin() as session:
            session.add(
                WorkerHeartbeat(
                    id=new_uuid7(),
                    queue="general",
                    worker_id="incompatible-worker",
                    process_version="incompatible-worker-v0",
                    migration_head=CURRENT_MIGRATION_HEAD,
                    started_at=_NOW - timedelta(minutes=5),
                    seen_at=_NOW,
                )
            )
            session.add(
                IntegrationStatus(
                    id=new_uuid7(),
                    provider="fosun",
                    status="provider_outage",
                    last_success_at=None,
                    version=1,
                    mode_changed_at=_NOW,
                    capability_invalidated_at=None,
                    admitted_contract_hash=None,
                )
            )

        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            unauthenticated = await client.get("/health/ready")
            incompatible_worker = await client.get("/health/ready", headers=_headers())
            assert incompatible_worker.status_code == 503
            assert incompatible_worker.json()["data"]["components"]["worker"] == (
                "unavailable"
            )
            async with sessions.begin() as session:
                session.add(
                    WorkerHeartbeat(
                        id=new_uuid7(),
                        queue="general",
                        worker_id="compatible-worker",
                        process_version=CURRENT_GENERAL_WORKER_PROCESS_VERSION,
                        migration_head=CURRENT_MIGRATION_HEAD,
                        started_at=_NOW - timedelta(minutes=5),
                        seen_at=_NOW,
                    )
                )
            ready = await client.get("/health/ready", headers=_headers())
            assert unauthenticated.status_code == 401
            assert ready.status_code == 200
            assert ready.json()["data"] == {
                "status": "ready",
                "components": {
                    "database": "ready",
                    "migration": "ready",
                    "object_store": "ready",
                    "worker": "ready",
                },
            }

            probe.available = False
            object_store_down = await client.get("/health/ready", headers=_headers())
            assert object_store_down.status_code == 503
            assert object_store_down.json()["data"]["components"]["object_store"] == ("unavailable")
            probe.available = True

            async with sessions.begin() as session:
                await session.execute(
                    text("UPDATE alembic_version SET version_num='0019_paper_performance_root'")
                )
            migration_mismatch = await client.get("/health/ready", headers=_headers())
            assert migration_mismatch.status_code == 503
            assert migration_mismatch.json()["data"]["components"]["migration"] == ("incompatible")
            async with sessions.begin() as session:
                await session.execute(
                    text("UPDATE alembic_version SET version_num=:head"),
                    {"head": CURRENT_MIGRATION_HEAD},
                )

            async with sessions.begin() as session:
                heartbeat = await session.get(
                    WorkerHeartbeat,
                    await session.scalar(
                        select(WorkerHeartbeat.id).where(
                            WorkerHeartbeat.process_version
                            == CURRENT_GENERAL_WORKER_PROCESS_VERSION
                        )
                    ),
                )
                assert heartbeat is not None
                heartbeat.seen_at = _NOW + timedelta(seconds=31)
            future_worker = await client.get("/health/ready", headers=_headers())
            assert future_worker.status_code == 503
            assert future_worker.json()["data"]["components"]["worker"] == "stale"

        serialized = json.dumps(object_store_down.json(), sort_keys=True)
        assert "private endpoint" not in serialized
        assert "bucket" not in serialized
        assert "compatible-worker" not in serialized
        assert "provider_outage" not in serialized
        assert probe.calls >= 5
    finally:
        await app.state.session_factory.kw["bind"].dispose()
        await engine.dispose()


@pytest.mark.asyncio
async def test_readiness_returns_sanitized_503_when_database_cannot_open(
    migrated_database_url: str,
) -> None:
    probe = _BucketProbe()
    app = create_engine_app(_settings(migrated_database_url), readiness_object_store=probe)
    app.state.utc_clock = lambda: _NOW
    original_engine = app.state.session_factory.kw["bind"]
    unavailable_engine = create_async_engine(
        "postgresql+asyncpg://nexus:private@127.0.0.1:1/nexus",
        connect_args={"timeout": 0.1},
    )
    app.state.session_factory = async_sessionmaker(
        unavailable_engine, class_=AsyncSession, expire_on_commit=False
    )
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            response = await client.get("/health/ready", headers=_headers())
        assert response.status_code == 503
        assert response.json()["data"]["components"]["database"] == "unavailable"
        serialized = json.dumps(response.json(), sort_keys=True)
        assert "127.0.0.1" not in serialized
        assert "private" not in serialized
    finally:
        await unavailable_engine.dispose()
        await original_engine.dispose()
