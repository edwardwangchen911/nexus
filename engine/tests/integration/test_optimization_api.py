# ruff: noqa: E501
"""Authenticated optimization what-if API contracts."""

import asyncio
import hashlib
import json
from datetime import UTC, date, datetime
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import app, headers, settings
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.session import session_factory
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_what_if_writes_only_redacted_audit_summary(migrated_database_url: str) -> None:
    """Persisting requested weights or proposal rows would leak and mutate business state."""
    payload = {
        "policy_version": "optimizer_v1",
        "positions": [{"instrument_id": "0198d500-1000-7000-8000-000000000601", "weight": "1"}],
    }
    body = json.dumps(payload, separators=(",", ":")).encode()
    path = "/internal/v1/what-if/evaluate"
    engine_app = app(migrated_database_url)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://testserver"
    ) as client:
        response = await client.post(path, content=body, headers=headers("POST", path, body))
    assert response.status_code == 200
    assert response.json()["data"]["policy_version"] == "optimizer_v1"
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session:
        rows = (
            (
                await session.execute(
                    select(AuditEvent).where(AuditEvent.action == "what_if_evaluated")
                )
            )
            .scalars()
            .all()
        )
        assert len(rows) == 1
        assert rows[0].payload == {
            "policy_version": "optimizer_v1",
            "instrument_count": 1,
            "request_hash": hashlib.sha256(body).hexdigest(),
        }
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 1


@pytest.mark.asyncio
async def test_what_if_rejects_oversized_and_more_than_thirty_instruments(
    migrated_database_url: str,
) -> None:
    """Parsing oversized bodies or permitting 31 instruments breaks the bounded what-if contract."""
    path = "/internal/v1/what-if/evaluate"
    too_many = json.dumps(
        {
            "policy_version": "optimizer_v1",
            "positions": [
                {"instrument_id": str(UUID(int=index + 1)), "weight": "0"} for index in range(31)
            ],
        }
    ).encode()
    oversized = b" " * (64 * 1024 + 1)
    engine_app = app(migrated_database_url)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://testserver"
    ) as client:
        for body in (too_many, oversized):
            response = await client.post(path, content=body, headers=headers("POST", path, body))
            assert response.status_code == 422
            assert response.json()["error"]["code"] == "VALIDATION_ERROR"


@pytest.mark.asyncio
async def test_proposal_admission_fails_closed_without_persisted_authority(
    migrated_database_url: str,
) -> None:
    """Creating a job from a fabricated data/cost authority must be rejected before persistence."""
    payload = json.dumps(
        {
            "portfolio_snapshot_id": "0198d500-1000-7000-8000-000000000041",
            "data_snapshot_id": "0198d500-1000-7000-8000-000000000699",
            "cost_model_version": "cost_model_v1",
        },
        separators=(",", ":"),
    ).encode()
    path = "/internal/v1/optimization-proposals"
    engine_app = app(migrated_database_url)
    request_headers = headers("POST", path, payload)
    request_headers["Idempotency-Key"] = "proposal-missing-authority"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://testserver"
    ) as client:
        response = await client.post(path, content=payload, headers=request_headers)
    assert response.status_code == 422
    assert response.json()["error"]["code"] == "DATA_PARTIAL"


@pytest.mark.asyncio
async def test_proposal_admission_returns_one_queued_job_for_idempotent_replay(
    migrated_database_url: str,
) -> None:
    """Creating two jobs for one key/body or accepting a changed replay breaks admission idempotency."""
    from helpers.portfolio_api import PORTFOLIOS, seed_portfolio_list
    from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID

    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    data_snapshot_id = UUID("0198d500-1000-7000-8000-000000000711")
    async with factory() as session, session.begin():
        session.add(
            DataSnapshot(
                id=data_snapshot_id,
                source="fixture",
                provider_object_id="optimization-711",
                revision="1",
                observed_at=datetime(2026, 8, 24, tzinfo=UTC),
                available_at=datetime(2026, 8, 24, tzinfo=UTC),
                received_at=datetime(2026, 8, 24, tzinfo=UTC),
                schema_version="1",
                payload={},
            )
        )
        session.add(
            PortfolioDiagnosticReportRecord(
                portfolio_id=PORTFOLIOS[0],
                portfolio_snapshot_id=UUID("0198d500-1000-7000-8000-000000000041"),
                snapshot_as_of=datetime(2026, 8, 24, tzinfo=UTC),
                trading_day=date(2026, 8, 24),
                policy_id=REAL_ADVISORY_POLICY_ID,
                policy_version="real_advisory_policy_v1",
                optimization_need="OPTIMIZATION_CANDIDATE",
                diagnostic_input_hash="d" * 64,
            )
        )
    body = json.dumps(
        {
            "portfolio_snapshot_id": "0198d500-1000-7000-8000-000000000041",
            "data_snapshot_id": str(data_snapshot_id),
            "cost_model_version": "cost_model_v1",
        },
        separators=(",", ":"),
    ).encode()
    path = "/internal/v1/optimization-proposals"
    engine_app = app(migrated_database_url)
    request_headers = headers("POST", path, body)
    request_headers["Idempotency-Key"] = "proposal-key-711"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://testserver"
    ) as client:
        first = await client.post(path, content=body, headers=request_headers)
        replay_headers = headers("POST", path, body)
        replay_headers["Idempotency-Key"] = "proposal-key-711"
        replay = await client.post(path, content=body, headers=replay_headers)
        changed = await client.post(
            path,
            content=body + b" ",
            headers={
                **headers("POST", path, body + b" "),
                "Idempotency-Key": "proposal-key-711",
            },
        )
        concurrent_headers = []
        for _ in range(2):
            header = headers("POST", path, body)
            header["Idempotency-Key"] = "proposal-key-concurrent-711"
            concurrent_headers.append(header)
        concurrent = await asyncio.gather(
            *(client.post(path, content=body, headers=header) for header in concurrent_headers)
        )
    assert first.status_code == replay.status_code == 202
    assert first.json()["data"]["job_id"] == replay.json()["data"]["job_id"]
    assert changed.status_code == 409
    assert changed.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"
    assert {response.status_code for response in concurrent} == {202}
    assert len({response.json()["data"]["job_id"] for response in concurrent}) == 1
