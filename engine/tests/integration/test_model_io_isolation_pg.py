"""Borrowed tests and a real child Worker cannot fall through to live SDK I/O."""

import json
import os
import subprocess
import sys
from unittest.mock import AsyncMock

import pytest
from integration import test_candidate_multistock_path as cycle
from nexus.agents import codex_sdk
from openai_codex import AsyncCodex


@pytest.mark.asyncio
async def test_borrowed_scenario_without_autouse_blocks_before_live_sdk(
    migrated_database_url, monkeypatch
):
    monkeypatch.setattr(codex_sdk, "AsyncCodex", AsyncCodex)
    account = AsyncMock(side_effect=AssertionError("LIVE_MODEL_BOUNDARY_REACHED"))
    monkeypatch.setattr(AsyncCodex, "account", account)
    guards = []
    original_guard = codex_sdk._require_offline_sdk_transport

    def observe_guard():
        guards.append(1)
        return original_guard()

    monkeypatch.setattr(codex_sdk, "_require_offline_sdk_transport", observe_guard)
    with pytest.raises((AssertionError, ValueError)):
        await cycle.test_fresh_multistock_candidate_cycle_freezes_native_jobs(
            migrated_database_url, monkeypatch, 3, False, "formal_v11"
        )
    assert guards == [1]
    assert account.await_count == 0


@pytest.mark.asyncio
async def test_child_worker_without_pytest_or_fixture_blocks_actual_sdk_edge(
    migrated_database_url, monkeypatch, tmp_path
):
    captured = {}

    class Prepared(Exception):
        pass

    async def stop_at_worker(settings, **kwargs):
        captured.update(settings=settings.model_dump(mode="json"), job_id=str(kwargs["job_ids"][0]))
        raise Prepared

    with monkeypatch.context() as preparation:
        preparation.setattr(cycle, "run_agent_worker_once", stop_at_worker)
        with pytest.raises(Prepared):
            await cycle.test_fresh_multistock_candidate_cycle_freezes_native_jobs(
                migrated_database_url, preparation, 3, False, "formal_v11"
            )
    path = tmp_path / "child.json"
    path.write_text(json.dumps(captured))
    path.chmod(0o600)
    source = r"""
import asyncio,json,sys
from datetime import datetime,UTC
from uuid import UUID
from sqlalchemy.ext.asyncio import AsyncSession
from nexus import worker
from nexus.agents import codex_sdk
from nexus.settings import EngineSettings
from openai_codex import AsyncCodex
assert "pytest" not in sys.modules
value=json.load(open(sys.argv[1]));settings=EngineSettings.model_validate(value['settings'])
assert 'nexus_test_' in settings.database_url
at=datetime(2026,10,9,14,0,10,tzinfo=UTC)
class Clock(datetime):
    @classmethod
    def now(cls,tz=None):return at if tz is None else at.astimezone(tz)
worker.datetime=Clock;worker._ONCE_STARTED_AT=at
original_scalar=AsyncSession.scalar
async def scalar(self,statement,*a,**kw):
    if str(statement)=="SELECT clock_timestamp() AS clock_timestamp_1":return at
    return await original_scalar(self,statement,*a,**kw)
AsyncSession.scalar=scalar
counts={'guard':0,'account':0}
original_guard=codex_sdk._require_offline_sdk_transport
async def account(self,**kw):
    counts['account']+=1
    raise AssertionError('LIVE_MODEL_BOUNDARY_REACHED')
def guard():
    counts['guard']+=1
    return original_guard()
AsyncCodex.account=account;codex_sdk._require_offline_sdk_transport=guard
async def main():
    await worker.run_agent_worker_once(
        settings,job_ids=(UUID(value['job_id']),),formal_stage_capacity=True
    )
    assert counts=={'guard':1,'account':0},counts
    print(json.dumps(counts))
asyncio.run(main())
"""
    env = dict(os.environ)
    env.pop("PYTEST_CURRENT_TEST", None)
    result = subprocess.run(
        [sys.executable, "-c", source, str(path)],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout.splitlines()[-1]) == {"guard": 1, "account": 0}
