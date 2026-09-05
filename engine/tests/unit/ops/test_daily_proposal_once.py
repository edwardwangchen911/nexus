"""Proposal composition is explicit, local, and never executes research."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.ops.daily_once import (
    DailyOnceError,
    DailyOnceSettings,
    DailyProposalBuildRequest,
    _require_research_model,
    main,
    run_once,
)
from nexus.persistence.models.agents import AgentModelConfigRecord


def request():
    return DailyProposalBuildRequest(
        action="proposal-build",
        root_id=UUID(int=1),
        run_id=UUID(int=2),
        context_as_of=datetime(2026, 9, 4, tzinfo=UTC),
        idempotency_key="test-draft",
    )


@pytest.mark.asyncio
async def test_proposal_requires_explicit_storage_before_database(monkeypatch):
    for name in ("ENDPOINT", "REGION", "BUCKET", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY"):
        monkeypatch.delenv(f"OBJECT_STORE_{name}", raising=False)

    def forbidden(*args, **kwargs):
        pytest.fail("must not access database or model with implicit storage")

    monkeypatch.setattr("nexus.ops.daily_once.create_async_engine", forbidden)
    with pytest.raises(DailyOnceError, match="DAILY_ONCE_ARTIFACT_READER_UNAVAILABLE"):
        await run_once(
            DailyOnceSettings(database_url="postgresql+asyncpg://u:p@127.0.0.1/db"), request()
        )


def test_explicit_proposal_cli_parses_without_echoing_inputs(monkeypatch, capsys):
    command = request()
    runner = AsyncMock(return_value={"status": "draft_created"})
    monkeypatch.setattr("nexus.ops.daily_once.run_once", runner)
    monkeypatch.setattr(
        "sys.argv",
        [
            "daily_once",
            "proposal-build",
            "--root-id",
            str(command.root_id),
            "--run-id",
            str(command.run_id),
            "--context-as-of",
            command.context_as_of.isoformat(),
            "--idempotency-key",
            command.idempotency_key,
        ],
    )
    main()
    assert runner.await_args.args[1] == command
    assert capsys.readouterr().out == '{"status": "draft_created"}\n'


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "version,limit", [("codex-sdk-chatgpt-v3", 64000), ("codex-sdk-chatgpt-v3-input96k", 96000)]
)
async def test_explicit_structured_model_is_recognized_without_selection_or_execution(
    version, limit
):
    model = AgentModelConfigRecord(
        model_id="gpt-5.4",
        version=version,
        max_input_tokens=limit,
        max_output_tokens=8192,
        input_usd_per_million=0,
        output_usd_per_million=0,
    )
    session = AsyncMock()
    session.get.return_value = model
    from nexus.agents.codex_sdk import MODEL_ID

    model.model_id = MODEL_ID
    await _require_research_model(
        session, DailyOnceSettings(agent_runtime_mode="codex_sdk"), UUID(int=1)
    )
