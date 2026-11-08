"""One workbench projection reuses immutable inputs, never results across reads."""

import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from nexus.persistence.models.data import DataArtifact
from nexus.research import identity_sources
from nexus.research.artifact_read_scope import read_artifact
from nexus.workbench import activity_read
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "error", "cancel"])
async def test_activity_reuses_source_parsing_and_registrations_only_within_read(
    monkeypatch, failure
):
    raw = json.dumps(
        {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [[123456, "Example", "AAPL", "Nasdaq"]],
        }
    ).encode()
    parsed = []
    native = identity_sources.parse_identity_source

    def parse(value):
        parsed.append(value)
        return native(value)

    monkeypatch.setattr(identity_sources, "parse_identity_source", parse)
    now = datetime.now(UTC)
    row = DataArtifact(
        id=uuid4(),
        sha256="a" * 64,
        uri="s3://synthetic/a?versionId=fixed",
        byte_size=8,
        mime_type="application/json",
        schema_version="test",
        source="sec",
        observed_at=now,
        available_at=now,
        received_at=now,
        created_at=now,
    )
    session = AsyncSession()
    monkeypatch.setattr(session, "scalar", AsyncMock(return_value=row))
    bodies = []

    async def body(*args, **kwargs):
        bodies.append(len(bodies))
        for _ in range(2):
            assert identity_sources.directory_binding_from_raw(raw, "AAPL") == (
                "0000123456",
                "XNAS",
            )
            assert (await read_artifact(session, "a" * 64)).uri == row.uri
        if failure == "error":
            raise ValueError("READ_REJECTED")
        if failure == "cancel":
            raise asyncio.CancelledError
        return bodies[-1]

    monkeypatch.setattr(activity_read, "_read_activity", body)
    try:
        if failure:
            with pytest.raises(ValueError if failure == "error" else asyncio.CancelledError):
                await activity_read.read_activity(session, read_at=now, decision_session=None)
        else:
            assert (
                await activity_read.read_activity(session, read_at=now, decision_session=None) == 0
            )
        assert len(parsed) == 1 and session.scalar.await_count == 1
        assert session.info == {} and identity_sources._PARSE_SCOPE.get() is None
        failure = None
        assert await activity_read.read_activity(session, read_at=now, decision_session=None) == 1
        assert len(parsed) == 2 and session.scalar.await_count == 2 and len(bodies) == 2
    finally:
        await session.close()
