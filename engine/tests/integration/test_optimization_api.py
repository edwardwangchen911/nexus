"""Authenticated optimization what-if API contracts."""

import hashlib
import json
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import app, headers, settings
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
