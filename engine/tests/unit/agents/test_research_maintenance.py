"""The rollback image rejects execution before any claim, authorization or SDK work."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import pytest


@pytest.mark.asyncio
async def test_maintenance_refuses_worker_before_any_session(monkeypatch):
    from nexus import worker
    from nexus.agents import research_maintenance
    from nexus.settings import EngineSettings

    monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
    factory = Mock(side_effect=AssertionError("must not open a worker session"))
    monkeypatch.setattr(worker, "session_factory", factory)
    with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
        await worker.run_agent_worker_once(EngineSettings.for_test(), job_ids=(uuid4(),))
    factory.assert_not_called()


@pytest.mark.asyncio
async def test_maintenance_refuses_direct_execution_and_authorization(monkeypatch):
    from nexus.agents import research_maintenance
    from nexus.agents.execution import execute_research_run
    from nexus.agents.research_stages.protocol import StagedProtocol

    monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
    with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
        await execute_research_run(None, run_id=uuid4(), committee=None)
    with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
        await StagedProtocol(None).authorize(None, stage=1, now=datetime.now(UTC), wire=b"no send")


@pytest.mark.asyncio
async def test_maintenance_identity_reads_existing_without_insert(monkeypatch):
    from nexus.agents import research_maintenance
    from nexus.persistence.models.runtime import IdempotencyRecord

    monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
    old = IdempotencyRecord(response_payload={"run_id": str(uuid4())})
    session = AsyncMock()
    session.scalar.return_value = old
    result, inserted = await research_maintenance.claim_research_identity(
        session, "actor", "route", "key", "a" * 64, "schema"
    )
    assert result is old and inserted is False
    session.execute.assert_not_called()
    session.scalar.return_value = None
    with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
        await research_maintenance.claim_research_identity(
            session, "actor", "route", "new", "a" * 64, "schema"
        )
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_maintenance_refuses_cycle_and_generic_queue_before_work(monkeypatch):
    from types import SimpleNamespace

    from nexus.agents import research_maintenance
    from nexus.jobs.models import JobType, QueueName
    from nexus.jobs.worker import run_job_worker_once
    from nexus.paper.agent_cycle import _admit_cycle_research, _start_cycle

    monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
    with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
        await _start_cycle(
            None,
            authority=None,
            decision_slot_at=None,
            daily_limit_usd=None,
            requested_instrument_ids=(),
        )
    with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
        await _admit_cycle_research(
            None, cycle=None, authority=None, projection=None, daily_limit_usd=None
        )
    with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
        await run_job_worker_once(
            None,
            registry=SimpleNamespace(
                specs=(SimpleNamespace(job_type=JobType.AGENT_PAPER_RESEARCH),)
            ),
            queue=QueueName.GENERAL,
            worker_id="no-claim",
            worker_started_at=None,
            process_version="no-start",
            migration_head="0099_research_stages",
            clock=None,
        )


@pytest.mark.asyncio
async def test_maintenance_new_admission_refuses_before_authority_callback(monkeypatch):
    from nexus.agents import research_maintenance
    from nexus.agents.persistence import _admit_research_run

    monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
    session = AsyncMock()
    session.scalar.return_value = None
    callback = AsyncMock(side_effect=AssertionError("new authority must not run"))
    with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
        await _admit_research_run(
            session,
            actor_id="actor",
            route="research",
            idempotency_key="new",
            request_hash="a" * 64,
            request=None,
            prompt_version_id=None,
            model_config_id=None,
            daily_limit_usd=None,
            now=None,
            after_authority=callback,
        )
    callback.assert_not_called()
    session.execute.assert_not_called()


def test_runtime_environment_cannot_disable_image_maintenance(monkeypatch):
    from nexus.agents import research_maintenance

    monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
    monkeypatch.setenv("NEXUS_RESEARCH_MAINTENANCE", "false")
    with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
        research_maintenance.require_research_open()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["queued", "active"])
async def test_manual_claim_preserves_replay_but_cannot_start_preparation(monkeypatch, status):
    from types import SimpleNamespace

    from nexus.agents import research_maintenance
    from nexus.paper import manual_request

    monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
    row = SimpleNamespace(id=uuid4(), account_id=uuid4())
    view = SimpleNamespace(status=status)
    session = AsyncMock()
    session.get.return_value = session.scalar.return_value = row
    monkeypatch.setattr(manual_request, "lock_portfolio_account", AsyncMock())
    monkeypatch.setattr(manual_request, "read_manual_request", AsyncMock(return_value=view))
    if status == "queued":
        with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
            await manual_request.claim_manual_request(session, request_id=row.id)
    else:
        assert await manual_request.claim_manual_request(session, request_id=row.id) == (
            view,
            False,
        )
    session.add.assert_not_called()
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_unprepared_independent_request_cannot_create_manifest(monkeypatch):
    from types import SimpleNamespace

    from nexus.agents import research_maintenance
    from nexus.paper import research_request

    monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
    row = SimpleNamespace(id=uuid4(), account_id=uuid4(), contract_version=3)
    settings = SimpleNamespace(agent_paper_account_id=row.account_id)
    session = AsyncMock()
    session.get.return_value = row

    # The v3 replay guard reads isolation and existing acceptance tombstones
    # before preparation. Model these database facts without bypassing the guard.
    async def scalar(statement):
        assert str(statement) == "SHOW transaction_isolation"
        return "read committed"

    session.scalar.side_effect = scalar
    session.scalars.return_value = []
    monkeypatch.setattr(research_request, "lock_portfolio_account", AsyncMock())
    monkeypatch.setattr(
        research_request,
        "read_manual_request",
        AsyncMock(return_value=SimpleNamespace(status="preparing")),
    )
    with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
        await research_request.prepare_research_request(session, settings, request_id=row.id)
    session.add.assert_not_called()
    session.execute.assert_not_called()


@pytest.mark.asyncio
async def test_maintenance_stops_host_loop_before_inspection_or_collection(monkeypatch):
    from pathlib import Path

    from nexus.agents import research_maintenance
    from nexus.ops.agent_paper_loop import run_foreground_loop, run_local_agent_paper_loop
    from nexus.settings import EngineSettings

    monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
    inspect = AsyncMock(side_effect=AssertionError("host must not inspect or dispatch"))
    execute = AsyncMock(side_effect=AssertionError("host must not collect or dispatch"))
    with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
        await run_foreground_loop(inspect=inspect, execute=execute, sleep=None, heartbeat=None)
    with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
        await run_local_agent_paper_loop(
            EngineSettings.for_test(), account_id=uuid4(), repo_root=Path(".")
        )
    inspect.assert_not_called()
    execute.assert_not_called()
