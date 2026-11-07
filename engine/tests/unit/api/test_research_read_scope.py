"""One research GET shares exact originals, never authority or cross-request results."""

import asyncio
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from nexus.api.routes import research
from nexus.research.sec_exhibit_storage import configured_exhibit_store
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "error", "cancel"])
async def test_research_hydration_reads_one_version_and_rechecks_next_request(monkeypatch, failure):
    content = b"frozen original"
    digest = sha256(content).hexdigest()
    reads = []
    corrupt = False

    class Store:
        async def read_raw(self, key, *, uri):
            reads.append((key, uri))
            return b"changed original" if corrupt else content

    store = Store()
    session = AsyncSession(info={"sec_exhibit_store": store})

    async def bound_original(*_args, **_kwargs):
        raw = await configured_exhibit_store(session).read_raw(
            "raw/identity", uri="s3://bucket/raw/identity?versionId=original-version"
        )
        if sha256(raw).hexdigest() != digest:
            raise ValueError("ORIGINAL_HASH_MISMATCH")
        return True

    async def context(*args, **kwargs):
        await bound_original()
        if failure == "error":
            raise ValueError("CONTEXT_REJECTED")
        if failure == "cancel":
            raise asyncio.CancelledError
        return None

    monkeypatch.setattr(research, "load_frozen_evidence", bound_original)
    monkeypatch.setattr(research, "read_portfolio_context", context)
    monkeypatch.setattr(research, "_declares_current_trends", lambda _: True)
    monkeypatch.setattr(research, "_declares_monitoring", lambda _: False)
    monkeypatch.setattr(research, "_declares_disclosure_monitoring", lambda _: False)
    monkeypatch.setattr(research, "read_stage_trace", AsyncMock(return_value=None))
    monkeypatch.setattr(research, "read_research_inputs", AsyncMock(return_value=None))
    monkeypatch.setattr(
        research,
        "_response",
        lambda *args, **kwargs: SimpleNamespace(model_copy=lambda **updates: updates),
    )
    run = SimpleNamespace(
        data_snapshot_id=None,
        instrument_id=None,
        decision_time=None,
        pricing_version="codex-sdk-chatgpt-v11-bounded-stage-budget",
        status="prepared",
    )
    reader = SimpleNamespace(verified=bound_original, read_at=None)
    try:
        if failure:
            error = asyncio.CancelledError if failure == "cancel" else ValueError
            with pytest.raises(error):
                await research._hydrate_response(session, run, None, observation_reader=reader)
        else:
            await research._hydrate_response(session, run, None, observation_reader=reader)
        assert len(reads) == 1
        assert session.info["sec_exhibit_store"] is store
        assert "nexus_immutable_artifact_read_scope" not in session.info
        corrupt = True
        with pytest.raises(ValueError, match="ORIGINAL_HASH_MISMATCH"):
            await research._hydrate_response(session, run, None, observation_reader=reader)
        assert len(reads) == 2
        assert session.info["sec_exhibit_store"] is store
    finally:
        await session.close()
