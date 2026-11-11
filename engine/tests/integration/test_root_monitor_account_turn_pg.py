"""A normal root reservation must queue monitor work before either opens its DB turn."""

import asyncio
from types import SimpleNamespace
from uuid import UUID

import pytest
from nexus.ops import agent_paper_once as once
from nexus.paper.account_transaction_turn import account_transaction_turn
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import text


@pytest.mark.asyncio
async def test_root_reservation_does_not_expose_monitor_to_account_lock_timeout(
    temporary_database_url, monkeypatch, tmp_path
):
    settings = EngineSettings.for_test().model_copy(
        update={"database_url": temporary_database_url, "agent_paper_account_id": UUID(int=1)}
    )
    factory = session_factory(settings)
    held = asyncio.Event()
    release = asyncio.Event()
    monitor_started = asyncio.Event()
    monitor_opened = asyncio.Event()
    result = object()
    reservations = []

    async def reserve(session, **kwargs):
        reservations.append(kwargs["client_idempotency_key"])
        await lock_portfolio_account(session, kwargs["account_id"])
        held.set()
        await release.wait()
        return SimpleNamespace(replay=result)

    def no_provider(*args, **kwargs):
        raise AssertionError("reservation readback must not collect or send")

    monkeypatch.setattr(once, "_validate_trigger_dependencies", lambda *_: None)
    monkeypatch.setattr(once, "reserve_schwab_agent_paper_trigger", reserve)
    monkeypatch.setattr(once, "S3ProviderEvidenceStore", no_provider)
    request = once.AgentPaperTriggerRequest(
        action="trigger",
        account_id=UUID(int=1),
        candidate_batch_id=UUID(int=2),
        idempotency_key="original-root",
    )

    async def monitor():
        monitor_started.set()
        async with account_transaction_turn(settings), factory() as session, session.begin():
            monitor_opened.set()
            await session.execute(text("SET LOCAL lock_timeout = '2s'"))
            await lock_portfolio_account(session, request.account_id)
            return await session.scalar(text("SELECT 1"))

    root = asyncio.create_task(once.trigger_once(settings, request, repo_root=tmp_path))
    scan = None
    try:
        await asyncio.wait_for(held.wait(), 5)
        scan = asyncio.create_task(monitor())
        await monitor_started.wait()
        # Reproduce the real, unchanged PostgreSQL lock-timeout interval.
        # Root holds the account past that interval; the monitor must wait
        # without a DB transaction rather than timing out inside PostgreSQL.
        await asyncio.sleep(2.2)
        opened_while_root_holds = monitor_opened.is_set()
        release.set()
        assert await root is result
        assert await scan == 1
        assert not opened_while_root_holds
        assert reservations == ["original-root"]
    finally:
        release.set()
        await asyncio.gather(root, *([scan] if scan is not None else []), return_exceptions=True)
        await factory.kw["bind"].dispose()
