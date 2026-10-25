"""Exact installed issuer through normal mocked HTTP collector and durable once-only claims."""

import asyncio
from datetime import datetime, timedelta

import httpx
import pytest
from helpers.disclosure_observations import append_capture
from helpers.paper_batch import CALENDAR_ID
from integration.test_candidate_multistock_path import AT, _set_test_insert_clock
from integration.test_monitor_batches_pg import installed
from nexus.ops import monitor_disclosure_refresh as refresh
from nexus.ops.sec_index_local import collect_index
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.runtime import AuditEvent, Job
from sqlalchemy import func, select


def settings_for_import(factory):
    from nexus.settings import EngineSettings

    return EngineSettings(
        database_url=factory.kw["bind"].url.render_as_string(hide_password=False),
        object_store_endpoint="http://127.0.0.1:9000",
    )


async def setup(database_url, monkeypatch, root):
    factory, settings, store, captured, pointer = await installed(
        database_url, monkeypatch, root, kinds=("disclosure",)
    )
    async with factory() as session:
        run = await session.get(AgentResearchRunRecord, pointer.source_run_id)
        model_id = run.model_config_id
    settings = settings.model_copy(
        update={"agent_paper_cycle_enabled": True, "agent_paper_formal_model_config_id": model_id}
    )
    monkeypatch.setattr(refresh, "configured_exhibit_store", lambda session: store)
    async with factory() as session, session.begin():
        later = AT + timedelta(days=3)
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=later.date(),
                decision_at=later.replace(hour=21),
                market_open_at=later.replace(hour=13, minute=30),
                market_close_at=later.replace(hour=20),
                created_at=AT - timedelta(hours=1),
            )
        )
    calls = []

    async def request(request):
        calls.append(str(request.url))
        name = "directory" if request.url.host == "www.sec.gov" else "AAPL-submissions"
        return httpx.Response(200, content=(root / "baseline" / f"{name}.json").read_bytes())

    client = httpx.AsyncClient(
        transport=httpx.MockTransport(request),
    )
    monkeypatch.setattr(
        refresh,
        "_client",
        lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(request),
        ),
    )

    async def collect(settings, **kwargs):
        return await collect_index(settings, **kwargs, store=store)

    monkeypatch.setattr(refresh, "collect_index", collect)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return captured["clock"][0].astimezone(tz)

    monkeypatch.setattr("nexus.ops.research_identity_local.datetime", Clock)

    async def invoke(environment=None, scheduled_at=None):
        return await refresh.refresh_disclosure_observations(
            settings,
            account_id=pointer.account_id,
            calendar_authority_id=CALENDAR_ID,
            scheduled_at=scheduled_at or refresh._bucket(captured["clock"][0]),
            environment={"SEC_CONTACT": "Synthetic test test@example.invalid"}
            if environment is None
            else environment,
            repo_root=root,
        )

    return factory, store, captured, pointer, calls, client, invoke


@pytest.mark.asyncio
async def test_fresh_then_concurrent_stale_refresh_is_exact_once(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, store, captured, pointer, calls, client, invoke = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    try:
        fresh = await invoke({})
        assert fresh["provider_requests"] == 0 and not calls, fresh
        captured["clock"][0] = AT + timedelta(minutes=10)
        await _set_test_insert_clock(factory, captured["clock"][0])
        await append_capture(
            # Import time is current, but the real network receipt is still old.
            settings_for_import(factory),
            store,
            captured,
            factory,
            tmp_path,
            "late-stale",
            received=AT + timedelta(seconds=2),
            imported_at=captured["clock"][0],
            added=4,
        )
        async with factory() as session:
            jobs = await session.scalar(select(func.count()).select_from(Job))
        results = await asyncio.gather(invoke(), invoke())
        assert len(calls) == 2, results
        assert sorted(r["provider_requests"] for r in results) == [0, 2], results
        assert any(i["status"] == "refreshed" for r in results for i in r["items"]), results
        async with factory() as session:
            audits = tuple(
                await session.scalars(
                    select(AuditEvent).where(AuditEvent.action.in_((refresh.CLAIM, refresh.RESULT)))
                )
            )
            assert len(audits) == 2
            assert audits[0].payload["identity_snapshot_id"] == str(
                captured["identity"].snapshot_id
            )
            assert await session.scalar(select(func.count()).select_from(Job)) == jobs
        assert (await invoke())["provider_requests"] == 0
        assert len(calls) == 2
        missed = await invoke(scheduled_at=AT + timedelta(minutes=5))
        assert missed["status"] == "missed_bucket" and missed["provider_requests"] == 0
        captured["clock"][0] = AT.replace(hour=21)
        closed = await invoke()
        assert closed["status"] == "market_closed" and closed["provider_requests"] == 0
        assert len(calls) == 2
    finally:
        await client.aclose()
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_failed_claim_never_replayed_but_new_bucket_is_independent(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, store, captured, pointer, calls, client, invoke = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    entered = []

    async def failure(settings, **kwargs):
        entered.append(kwargs)
        raise ValueError("private provider detail")

    monkeypatch.setattr(refresh, "collect_index", failure)
    try:
        captured["clock"][0] = AT + timedelta(minutes=10)
        await _set_test_insert_clock(factory, captured["clock"][0])
        missing = await invoke({})
        assert missing["provider_requests"] == 0 and not entered, missing
        first = await invoke()
        assert first["provider_requests"] is None, first
        assert "private provider detail" not in str(first)
        second = await invoke()
        assert len(entered) == 1 and second["items"][0]["status"] == "already_claimed", second
        captured["clock"][0] += timedelta(minutes=5)
        await _set_test_insert_clock(factory, captured["clock"][0])
        await invoke()
        assert len(entered) == 2
    finally:
        await client.aclose()
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_cancelled_collection_leaves_claim_without_replay(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, store, captured, pointer, calls, client, invoke = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    entered, never = asyncio.Event(), asyncio.Event()
    attempts = []

    async def suspended(settings, **kwargs):
        attempts.append(kwargs)
        entered.set()
        await never.wait()

    monkeypatch.setattr(refresh, "collect_index", suspended)
    try:
        captured["clock"][0] = AT + timedelta(minutes=10)
        await _set_test_insert_clock(factory, captured["clock"][0])
        task = asyncio.create_task(invoke())
        await asyncio.wait_for(entered.wait(), timeout=10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        result = await invoke()
        assert result["items"] == [{"status": "already_claimed"}], result
        assert len(attempts) == 1
        async with factory() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == refresh.CLAIM)
                )
                == 1
            )
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == refresh.RESULT)
                )
                == 0
            )
    finally:
        await client.aclose()
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_identity_drift_not_imported_and_stale_source_never_becomes_false(
    migrated_database_url, monkeypatch, tmp_path
):
    import json

    from nexus.persistence.models.data import DataSnapshot

    factory, store, captured, pointer, calls, client, invoke = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    drift_calls = []

    async def drift(request):
        drift_calls.append(str(request.url))
        if request.url.host == "www.sec.gov":
            data = json.loads((tmp_path / "baseline" / "directory.json").read_bytes())
            data["data"][0][0] = 999999
        else:
            data = json.loads((tmp_path / "baseline" / "AAPL-submissions.json").read_bytes())
            data["cik"] = 999999
        return httpx.Response(200, json=data)

    drift_client = httpx.AsyncClient(transport=httpx.MockTransport(drift))

    async def collect(settings, **kwargs):
        return await collect_index(settings, **kwargs, store=store)

    monkeypatch.setattr(refresh, "collect_index", collect)
    monkeypatch.setattr(
        refresh,
        "_client",
        lambda: httpx.AsyncClient(
            transport=httpx.MockTransport(drift),
        ),
    )
    try:
        captured["clock"][0] = AT + timedelta(minutes=10)
        await _set_test_insert_clock(factory, captured["clock"][0])
        async with factory() as session:
            before = await session.scalar(
                select(func.count())
                .select_from(DataSnapshot)
                .where(DataSnapshot.source == "sec_disclosure_index")
            )
        result = await invoke()
        assert result["items"][0]["status"] == "failed", result
        assert result["provider_requests"] == len(drift_calls) == 2
        async with factory() as session:
            after = await session.scalar(
                select(func.count())
                .select_from(DataSnapshot)
                .where(DataSnapshot.source == "sec_disclosure_index")
            )
            assert before == after
            observed = await refresh.read_disclosure_observation(
                session, plan=pointer, now=captured["clock"][0]
            )
            assert observed.result == "unknown" and observed.reason == "SEC_INDEX_STALE"
        assert (await invoke())["provider_requests"] == 0
        assert len(drift_calls) == 2
    finally:
        await drift_client.aclose()
        await client.aclose()
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_valid_capture_does_not_override_later_source_gap(
    migrated_database_url, monkeypatch, tmp_path
):
    from nexus.monitoring import batches
    from nexus.persistence.models.monitoring import MonitorCheck, MonitorTrigger

    factory, store, captured, pointer, calls, client, invoke = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    original = refresh.collect_index

    async def no_current_source(*args, **kwargs):
        return False

    async def source_changed(settings, **kwargs):
        result = await original(settings, **kwargs)
        # A newer daily result without coverage invalidates this old installed source.
        # Exercise the scanner's real coverage gate after source import, before D.
        monkeypatch.setattr(batches, "disclosure_source_current", no_current_source)
        return result

    monkeypatch.setattr(refresh, "collect_index", source_changed)
    try:
        captured["clock"][0] = AT + timedelta(minutes=10)
        await _set_test_insert_clock(factory, captured["clock"][0])
        result = await invoke()
        assert result["provider_requests"] == 2 and result["items"][0]["status"] == "refreshed", (
            result
        )
        async with factory() as session, session.begin():
            scan = await batches.scan_stock_batch(
                session,
                account_id=pointer.account_id,
                instrument_id=pointer.instrument_id,
                now=captured["clock"][0],
            )
            assert scan["new_triggers"] == 0 and scan["unknown"] == 1, scan
        async with factory() as session:
            check = await session.scalar(
                select(MonitorCheck).where(MonitorCheck.instrument_id == pointer.instrument_id)
            )
            assert check.result == "unknown" and check.reason == "MONITOR_AWAITING_RECONCILIATION"
            assert await session.scalar(select(func.count()).select_from(MonitorTrigger)) == 0
    finally:
        await client.aclose()
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_slow_original_readback_cannot_claim_a_missed_bucket(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, store, captured, pointer, calls, client, invoke = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    original = refresh._capture

    async def late(*args, **kwargs):
        value = await original(*args, **kwargs)
        captured["clock"][0] = AT + timedelta(minutes=15)
        return value

    monkeypatch.setattr(refresh, "_capture", late)
    try:
        captured["clock"][0] = AT + timedelta(minutes=10)
        await _set_test_insert_clock(factory, captured["clock"][0])
        result = await invoke()
        assert result["provider_requests"] == 0 and not calls, result
        async with factory() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == refresh.CLAIM)
                )
                == 0
            )
    finally:
        await client.aclose()
        await factory.kw["bind"].dispose()
