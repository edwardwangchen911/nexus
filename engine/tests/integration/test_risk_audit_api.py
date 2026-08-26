from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from urllib.parse import urlencode

import httpx
import pytest
from helpers.hmac import (
    TEST_HMAC_KEY_ID,
    TEST_HMAC_SECRET,
    build_test_hmac_headers,
)
from nexus.audit.service import append_audit_event
from nexus.domain.ids import new_uuid7
from nexus.jobs.models import QueueName
from nexus.jobs.repository import claim_next_job
from nexus.main import create_engine_app
from nexus.persistence.models.runtime import AuditEvent, IdempotencyRecord, Job
from nexus.settings import EngineSettings
from sqlalchemy import func, select, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

_NOW = datetime(2026, 8, 27, 15, tzinfo=UTC)


def _settings(database_url: str) -> EngineSettings:
    return EngineSettings(
        NEXUS_ENV="test",
        DATABASE_URL=database_url,
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID=TEST_HMAC_KEY_ID,
        NEXUS_ENGINE_HMAC_CURRENT_SECRET=TEST_HMAC_SECRET,
    )


def _headers(method: str, path: str, body: bytes = b"") -> dict[str, str]:
    headers = build_test_hmac_headers(method, path, body, nonce=str(new_uuid7()))
    if body:
        headers["Content-Type"] = "application/json"
    return headers


def test_risk_audit_openapi_declares_binary_export_and_write_preconditions() -> None:
    document = create_engine_app(EngineSettings.for_test()).openapi()
    export = document["paths"]["/internal/v1/risk-audit/export"]["get"]
    assert set(export["responses"]["200"]["content"]) == {"application/x-ndjson"}
    retry = document["paths"]["/internal/v1/jobs/{job_id}/retry"]["post"]
    required_headers = {
        parameter["name"]
        for parameter in retry["parameters"]
        if parameter.get("required") is True and parameter["in"] == "header"
    }
    assert {"Idempotency-Key", "If-Match"}.issubset(required_headers)


@pytest.mark.asyncio
async def test_risk_audit_is_redacted_filter_bound_and_stably_paginated(
    migrated_database_url: str,
) -> None:
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with sessions.begin() as session:
        await append_audit_event(
            session,
            actor_id="service:quality",
            action="quality.freeze.completed",
            schema_version="quality_v1",
            payload={
                "quality_status": "PASS",
                "secret": "must-never-cross-the-redaction-boundary",
            },
            occurred_at=_NOW - timedelta(minutes=2),
        )
        await append_audit_event(
            session,
            actor_id="system:worker",
            action="job.paper.performance.succeeded",
            schema_version="job_v1",
            payload={"status": "succeeded", "private_scope": "portfolio-secret"},
            occurred_at=_NOW - timedelta(minutes=1),
        )
    app = create_engine_app(_settings(migrated_database_url))
    app.state.utc_clock = lambda: _NOW
    try:
        first_path = "/internal/v1/risk-audit?limit=1"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            first = await client.get(first_path, headers=_headers("GET", first_path))
            assert first.status_code == 200
            first_data = first.json()["data"]
            assert first_data["summary"]["data_quality"] == "PASS"
            assert len(first_data["items"]) == 1
            assert "payload" not in first_data["items"][0]
            assert "private_scope" not in first.text
            assert "must-never" not in first.text
            cursor = first_data["next_cursor"]
            assert cursor is not None

            second_path = "/internal/v1/risk-audit?" + urlencode({"limit": 1, "cursor": cursor})
            second = await client.get(second_path, headers=_headers("GET", second_path))
            assert second.status_code == 200
            assert second.json()["data"]["items"][0]["kind"] == "quality"
            assert second.json()["data"]["next_cursor"] is None

            drift_path = "/internal/v1/risk-audit?" + urlencode(
                {"kind": "risk", "limit": 1, "cursor": cursor}
            )
            drift = await client.get(drift_path, headers=_headers("GET", drift_path))
            assert drift.status_code == 422
            assert drift.json()["error"]["code"] == "VALIDATION_ERROR"

            from_at = (_NOW - timedelta(days=1)).isoformat().replace("+00:00", "Z")
            to_at = _NOW.isoformat().replace("+00:00", "Z")
            export_path = "/internal/v1/risk-audit/export?" + urlencode(
                {"from": from_at, "to": to_at, "kind": "quality"}
            )
            exported = await client.get(export_path, headers=_headers("GET", export_path))
            assert exported.status_code == 200
            assert exported.headers["content-type"].startswith("application/x-ndjson")
            assert exported.headers["content-encoding"] == "gzip"
            assert b"must-never" not in exported.content
    finally:
        await app.state.session_factory.kw["bind"].dispose()
        await engine.dispose()


@pytest.mark.asyncio
async def test_manual_safe_dead_letter_retry_is_cas_idempotent_and_claimable(
    migrated_database_url: str,
) -> None:
    job_id = new_uuid7()
    unsafe_job_id = new_uuid7()
    engine = create_async_engine(migrated_database_url)
    sessions = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
    async with sessions.begin() as session:
        for current_id, policy in (
            (job_id, "manual_safe"),
            (unsafe_job_id, "never"),
        ):
            session.add(
                Job(
                    id=current_id,
                    job_key=f"test:{current_id}",
                    input_hash="a" * 64,
                    status="dead_letter" if policy == "manual_safe" else "failed",
                    schema_version="test_v1",
                    payload={"immutable": True},
                    job_type="paper.performance",
                    queue="general",
                    scope_kind="global",
                    scope_id="global",
                    effective_at=_NOW - timedelta(minutes=5),
                    priority=220,
                    retry_policy=policy,
                    max_attempts=1,
                    attempt_count=1,
                    manual_retry_count=0,
                    available_at=_NOW - timedelta(minutes=5),
                    started_at=_NOW - timedelta(minutes=2),
                    finished_at=_NOW - timedelta(minutes=1),
                    last_error_code="SAFE_FIXTURE_FAILURE",
                    result_code=None,
                    version=1,
                )
            )
    with pytest.raises(DBAPIError, match="manual retry authority"):
        async with sessions.begin() as session:
            await session.execute(
                update(Job)
                .where(Job.id == job_id)
                .values(
                    status="queued",
                    attempt_count=2,
                    manual_retry_count=1,
                    started_at=None,
                    finished_at=None,
                    version=2,
                )
            )
    app = create_engine_app(_settings(migrated_database_url))
    app.state.utc_clock = lambda: _NOW
    body = b"{}"
    path = f"/internal/v1/jobs/{job_id}/retry"
    action_headers = {
        **_headers("POST", path, body),
        "Idempotency-Key": "retry-once",
        "If-Match": '"1"',
    }
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            replay_headers = {
                **_headers("POST", path, body),
                "Idempotency-Key": "retry-once",
                "If-Match": '"1"',
            }
            first, replay = await asyncio.gather(
                client.post(path, content=body, headers=action_headers),
                client.post(path, content=body, headers=replay_headers),
            )
            unsafe_path = f"/internal/v1/jobs/{unsafe_job_id}/retry"
            unsafe = await client.post(
                unsafe_path,
                content=body,
                headers={
                    **_headers("POST", unsafe_path, body),
                    "Idempotency-Key": "unsafe-retry",
                    "If-Match": '"1"',
                },
            )
        assert first.status_code == replay.status_code == 200
        assert first.json()["data"]["manual_retry_count"] == 1
        assert replay.json()["data"]["manual_retry_count"] == 1
        assert sorted(
            (first.json()["data"]["replayed"], replay.json()["data"]["replayed"])
        ) == [False, True]
        assert unsafe.status_code == 403
        assert unsafe.json()["error"]["code"] == "FORBIDDEN"

        async with sessions.begin() as session:
            claimed = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="manual-retry-worker",
                now=_NOW + timedelta(seconds=1),
            )
            assert claimed is not None
            assert claimed.job_id == job_id
            assert claimed.attempt_no == 2
        async with sessions() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(IdempotencyRecord)
                    .where(IdempotencyRecord.idempotency_key == "retry-once")
                )
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == "job.manual_retry.queued")
                )
                == 1
            )
    finally:
        await app.state.session_factory.kw["bind"].dispose()
        await engine.dispose()
