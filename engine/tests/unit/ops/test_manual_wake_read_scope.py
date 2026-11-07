"""Wakeup reuses fixed bytes per read, never a queue/eligibility conclusion."""

import asyncio
from contextlib import asynccontextmanager
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from nexus.ops import manual_paper_host as host
from nexus.paper import formal_intraday_intent
from nexus.research.sec_exhibit_storage import configured_exhibit_store
from nexus.settings import EngineSettings
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "error", "cancel"])
async def test_wakeup_read_scope_is_local_and_rechecks_dynamic_work(monkeypatch, failure):
    raw = b"fixed original"
    reads = []
    checks = []
    corrupt = pending = False

    class Store:
        async def read_raw(self, key, *, uri):
            reads.append((key, uri))
            return b"changed original" if corrupt else raw

    store = Store()
    session = AsyncSession(info={"sec_exhibit_store": store})
    monkeypatch.setattr(session, "execute", AsyncMock())

    @asynccontextmanager
    async def begin():
        yield session

    factory = SimpleNamespace(begin=begin, kw={"bind": SimpleNamespace(dispose=AsyncMock())})
    monkeypatch.setattr(host, "session_factory", lambda _: factory)

    async def original():
        data = await configured_exhibit_store(session).read_raw(
            "raw/fixed", uri="s3://synthetic/raw/fixed?versionId=frozen-version"
        )
        if sha256(data).digest() != sha256(raw).digest():
            raise ValueError("ORIGINAL_CHANGED")

    async def peek(*args, **kwargs):
        checks.append("peek")
        await original()
        await original()
        if failure == "error":
            raise RuntimeError("QUEUE_READ_REJECTED")
        if failure == "cancel":
            raise asyncio.CancelledError
        return SimpleNamespace() if pending else None

    async def due(*args, **kwargs):
        checks.append("due")
        await original()
        return None

    monkeypatch.setattr(host, "peek_manual_request", peek)
    monkeypatch.setattr(formal_intraday_intent, "due_intent", due)
    account = uuid4()
    settings = EngineSettings.for_test().model_copy(
        update={"agent_paper_manual_enabled": True, "agent_paper_account_id": account}
    )
    try:
        if failure:
            with pytest.raises(RuntimeError if failure == "error" else asyncio.CancelledError):
                await host.manual_work_pending(settings, account_id=account)
        else:
            assert await host.manual_work_pending(settings, account_id=account) is False
        assert len(reads) == 1
        assert session.info["sec_exhibit_store"] is store
        assert "nexus_immutable_artifact_read_scope" not in session.info
        if failure:
            failure = None
        pending = True
        assert await host.manual_work_pending(settings, account_id=account) is True
        assert len(reads) == 2 and checks.count("peek") == 2
        corrupt = True
        with pytest.raises(ValueError, match="ORIGINAL_CHANGED"):
            await host.manual_work_pending(settings, account_id=account)
        assert len(reads) == 3 and checks.count("peek") == 3
        assert session.info["sec_exhibit_store"] is store
    finally:
        await session.close()
