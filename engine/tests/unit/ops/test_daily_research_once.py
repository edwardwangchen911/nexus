"""Explicit research commands never broaden a single native worker's scope."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.ops.daily_once import (
    DailyOnceError,
    DailyOnceSettings,
    DailyResearchAdmitRequest,
    DailyResearchRunRequest,
    main,
    run_once,
)
from nexus.settings import EngineSettings
from nexus.worker import run_agent_worker_once


@pytest.mark.asyncio
@pytest.mark.parametrize("ids", [None, (), (UUID(int=1),)])
async def test_agent_once_forwards_exact_optional_scope(monkeypatch, ids):
    worker = AsyncMock(return_value=False)
    monkeypatch.setattr("nexus.worker.run_job_worker_once", worker)
    assert not await run_agent_worker_once(EngineSettings.for_test(), job_ids=ids)
    assert worker.await_count == 1
    assert worker.call_args.kwargs["job_ids"] == ids
    assert worker.call_args.kwargs["claim_registered_only"] is True


@pytest.mark.asyncio
async def test_agent_once_forwards_guard_to_actual_claim_transaction(monkeypatch):
    worker = AsyncMock(return_value=False)
    guard = AsyncMock()
    monkeypatch.setattr("nexus.worker.run_job_worker_once", worker)
    assert not await run_agent_worker_once(
        EngineSettings.for_test(),
        job_ids=(UUID(int=1),),
        preclaim_guard=guard,
    )
    assert worker.call_args.kwargs["preclaim_guard"] is guard


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["research-admit", "research-run"])
async def test_research_commands_reject_implicit_fixture_before_database(monkeypatch, action):
    def forbidden(*args, **kwargs):
        pytest.fail("fixture must fail before database or worker")

    monkeypatch.setattr("nexus.ops.daily_once.create_async_engine", forbidden)
    request = (
        DailyResearchRunRequest(action=action, root_id=UUID(int=1), run_id=UUID(int=2))
        if action == "research-run"
        else DailyResearchAdmitRequest(
            action=action,
            root_id=UUID(int=1),
            instrument_id=UUID(int=2),
            model_config_id=UUID(int=3),
            research_as_of=datetime.now(UTC),
        )
    )
    with pytest.raises(DailyOnceError, match="DAILY_ONCE_RESEARCH_RUNTIME_UNAVAILABLE"):
        await run_once(
            DailyOnceSettings(
                database_url="postgresql+asyncpg://user:secret@127.0.0.1/db",
                agent_runtime_mode="fixture",
            ),
            request,
        )


@pytest.mark.asyncio
async def test_research_cannot_use_implicit_fixture_storage(monkeypatch):
    for suffix in ("ENDPOINT", "REGION", "BUCKET", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY"):
        monkeypatch.delenv(f"OBJECT_STORE_{suffix}", raising=False)

    def forbidden(*args, **kwargs):
        pytest.fail("missing store must fail before database or worker")

    monkeypatch.setattr("nexus.ops.daily_once.create_async_engine", forbidden)
    with pytest.raises(DailyOnceError, match="DAILY_ONCE_ARTIFACT_READER_UNAVAILABLE"):
        await run_once(
            DailyOnceSettings(
                database_url="postgresql+asyncpg://user:secret@127.0.0.1/db",
                agent_runtime_mode="codex_sdk",
            ),
            DailyResearchRunRequest(action="research-run", root_id=UUID(int=1), run_id=UUID(int=2)),
        )


def test_research_cli_invalid_input_is_redacted(monkeypatch, capsys):
    monkeypatch.setattr(
        "sys.argv",
        ["daily_once", "research-run", "--root-id", "private-canary", "--run-id", str(UUID(int=1))],
    )
    with pytest.raises(SystemExit):
        main()
    result = capsys.readouterr()
    assert "DAILY_ONCE_REQUEST_INVALID" in result.err
    assert "private-canary" not in result.err and not result.out
