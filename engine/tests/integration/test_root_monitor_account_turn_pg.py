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


@pytest.mark.asyncio
@pytest.mark.parametrize("cross_bucket", [False, True])
async def test_nonreplay_root_provider_wait_does_not_consume_scan_deadline(
    migrated_database_url, monkeypatch, tmp_path, cross_bucket
):
    from contextlib import asynccontextmanager
    from datetime import timedelta
    from unittest.mock import AsyncMock

    from helpers.paper_batch import CALENDAR_ID
    from integration.test_candidate_multistock_path import _set_test_insert_clock
    from integration.test_monitor_batches_pg import installed
    from nexus.monitoring.host import PriceMonitorCoordinator
    from nexus.ops import research_monitor

    factory, settings, _, captured, _ = await installed(
        migrated_database_url, monkeypatch, tmp_path, added=0, kinds=("price",)
    )
    settings = settings.model_copy(update={"agent_paper_cycle_enabled": True})
    account = settings.agent_paper_account_id
    from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord

    async with factory.begin() as session:
        monday = captured["clock"][0] + timedelta(days=3)
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=monday.date(),
                decision_at=monday.replace(hour=21),
                market_open_at=monday.replace(hour=13, minute=30),
                market_close_at=monday.replace(hour=20),
            )
        )
    monkeypatch.setattr(research_monitor, "load_host_settings", AsyncMock(return_value=settings))
    held, release, prepared = asyncio.Event(), asyncio.Event(), asyncio.Event()
    scans, sends, finishes = [], [], []
    reservation = SimpleNamespace(replay=None)

    @asynccontextmanager
    async def client(**kwargs):
        yield object()

    async def reserve(*args, **kwargs):
        return reservation

    async def provider(session, *args, **kwargs):
        await lock_portfolio_account(session, account)
        sends.append("one-original-capture")
        held.set()
        await release.wait()
        await session.execute(text("CREATE TABLE root_atomic(value integer)"))
        await session.execute(text("INSERT INTO root_atomic VALUES (1)"))
        return "root-result"

    async def finish(*args, **kwargs):
        finishes.append(kwargs)

    async def prepare(bucket):
        prepared.set()
        return {"status": "refreshed"}

    scan_errors = []

    async def scan():
        try:
            return await normal_scan()
        except Exception as error:
            scan_errors.append(repr(error))
            raise

    async def normal_scan():
        result = await research_monitor.scan_host_once(
            settings,
            account_id=account,
            calendar_authority_id=CALENDAR_ID,
            repo_root=tmp_path,
            scheduled_at=pinned[0],
        )
        scans.append(result)
        return result

    monkeypatch.setattr(once, "_validate_trigger_dependencies", lambda *_: None)
    monkeypatch.setattr(once, "reserve_schwab_agent_paper_trigger", reserve)
    monkeypatch.setattr(once, "trigger_in_session", provider)
    monkeypatch.setattr(once, "finish_schwab_agent_paper_trigger", finish)
    monkeypatch.setattr(once, "schwab_http_client", client)
    monkeypatch.setattr(once, "S3ProviderEvidenceStore", lambda *a, **kw: object())
    request = once.AgentPaperTriggerRequest(
        action="trigger",
        account_id=account,
        candidate_batch_id=UUID(int=2),
        idempotency_key="original-nonreplay",
    )
    root = asyncio.create_task(once.trigger_once(settings, request, repo_root=tmp_path))
    monitor = PriceMonitorCoordinator(
        scan, prepare=prepare, scan_turn=lambda: account_transaction_turn(settings)
    )
    at = captured["clock"][0].replace(
        minute=captured["clock"][0].minute // 5 * 5, second=0, microsecond=0
    )
    pinned = [at]
    try:
        await asyncio.wait_for(held.wait(), 5)
        assert monitor.kick(at)
        await prepared.wait()
        # Exceed the unchanged real eight-second scan deadline while the
        # original provider capture owns the root transaction.
        await asyncio.sleep(8.2)
        assert monitor.busy and not scans
        async with factory() as observer:
            assert await observer.scalar(text("SELECT to_regclass('root_atomic')")) is None
        if cross_bucket:
            captured["clock"][0] = at + timedelta(minutes=5, seconds=1)
            await _set_test_insert_clock(factory, captured["clock"][0])
        release.set()
        assert await root == "root-result"
        await monitor._task
        assert monitor.last_diagnostic == (
            "MONITOR_SCAN_MISSED_BUCKET" if cross_bucket else None
        ), scan_errors
        assert len(scans) == 1
        async with factory() as observer:
            old_count = await observer.scalar(
                text("SELECT count(*) FROM research_monitor_checks WHERE scheduled_at=:at"),
                {"at": at},
            )
            new_count = await observer.scalar(
                text("SELECT count(*) FROM research_monitor_checks WHERE scheduled_at=:at"),
                {"at": at + timedelta(minutes=5)},
            )
        assert (old_count == 0 if cross_bucket else old_count > 0) and new_count == 0
        if cross_bucket:
            assert scans[0]["status"] == "missed_bucket"
        assert not monitor.kick(at)
        pinned[0] = at + timedelta(minutes=5)
        captured["clock"][0] = pinned[0] + timedelta(seconds=1)
        await _set_test_insert_clock(factory, captured["clock"][0])
        assert monitor.kick(pinned[0])
        await monitor._task
        async with factory() as observer:
            assert (
                await observer.scalar(
                    text("SELECT count(*) FROM research_monitor_checks WHERE scheduled_at=:at"),
                    {"at": pinned[0]},
                )
                > 0
            )
            assert await observer.scalar(text("SELECT count(*) FROM root_atomic")) == 1
        assert sends == ["one-original-capture"] and len(finishes) == 1
    finally:
        release.set()
        await asyncio.gather(root, return_exceptions=True)
        await monitor.close()
        await factory.kw["bind"].dispose()
