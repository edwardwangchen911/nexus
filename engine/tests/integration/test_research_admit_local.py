"""Normal local CLI admits existing v3 once, with full selection idempotency."""

import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from integration.test_agent_persistence_pg import (
    _INSTRUMENT_ID,
    _NOW,
    _SNAPSHOT_ID,
    _seed_authority,
)
from nexus.ops import openai_local
from nexus.ops.research_admit_local import admit_local_research
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.runtime import Job
from nexus.persistence.session import session_factory
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from nexus.settings import EngineSettings
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_local_cli_v3_admission_is_explicit_bounded_and_idempotent(
    migrated_database_url, tmp_path, monkeypatch, capsys
):
    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    factory = session_factory(settings)
    await _seed_authority(factory)
    await openai_local.prepare_database(settings, structured_synthesis=True)
    await openai_local.prepare_database(settings, staged=True)
    async with factory() as session:
        configs = {r.version: r.id for r in await session.scalars(select(AgentModelConfigRecord))}
    payload = {
        "request": {
            "instrument_id": str(_INSTRUMENT_ID),
            "data_snapshot_id": str(_SNAPSHOT_ID),
            "decision_time": _NOW.isoformat(),
        },
        "model_config_id": str(configs["codex-sdk-chatgpt-v3"]),
        "idempotency_key": "local-v3-once",
    }
    path = tmp_path / "request.json"
    path.write_text(json.dumps(payload))
    monkeypatch.setattr(openai_local, "local_settings", lambda *a, **kw: settings)
    monkeypatch.setattr(
        "sys.argv", ["nexus-openai-local", "admit-research", "--research-request", str(path)]
    )
    # main owns its normal asyncio runner, so call it outside this test's event loop.
    import asyncio

    assert await asyncio.to_thread(openai_local.main) == 0
    first = json.loads(capsys.readouterr().out)
    assert first["model_requests"] == 0 and not first["replayed"]
    replay = await admit_local_research(settings, path)
    assert replay["run_id"] == first["run_id"] and replay["replayed"]
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, UUID(first["run_id"]))
        assert run.model_config_id == configs["codex-sdk-chatgpt-v3"]
        assert run.status == "prepared" and run.input_tokens is None
        jobs = list(await session.scalars(select(Job)))
        assert len(jobs) == 1 and jobs[0].max_attempts == 1
    for changes in (
        {"model_config_id": str(configs["codex-sdk-chatgpt-v2"])},
        {"request": {**payload["request"], "data_snapshot_id": str(UUID(int=999))}},
    ):
        path.write_text(json.dumps({**payload, **changes}))
        with pytest.raises(IdempotencyConflict):
            await admit_local_research(settings, path)
    for field, value in (
        ("data_snapshot_id", str(UUID(int=999))),
        ("decision_time", (datetime.now(UTC) + timedelta(days=1)).isoformat()),
    ):
        path.write_text(
            json.dumps(
                {
                    **payload,
                    "idempotency_key": "invalid-" + field,
                    "request": {**payload["request"], field: value},
                }
            )
        )
        with pytest.raises(ValueError):
            await admit_local_research(settings, path)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 1
        assert await session.scalar(select(func.count()).select_from(Job)) == 1
    await factory.kw["bind"].dispose()
