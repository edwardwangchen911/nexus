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


async def setup(database_url, monkeypatch, root, *, listed_history=False):
    factory, settings, store, captured, pointer = await installed(
        database_url, monkeypatch, root, kinds=("disclosure",), listed_history=listed_history
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
    if listed_history:
        monkeypatch.setattr(
            "nexus.monitoring.disclosure_forward.configured_exhibit_store", lambda session: store
        )
    calls = []

    async def request(request):
        calls.append(str(request.url))
        if request.url.path.endswith("-submissions-001.json"):
            return httpx.Response(
                200,
                json=dict(
                    accessionNumber=["0000123456-20-000001"],
                    form=["8-K/A"],
                    acceptanceDateTime=["2020-01-05T19:00:00Z"],
                    filingDate=["2020-01-05"],
                    primaryDocument=["old.txt"],
                ),
            )
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
    monkeypatch.setattr("nexus.ops.sec_listed_local.datetime", Clock)

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
@pytest.mark.parametrize("server_committed", [False, True])
async def test_forward_refresh_commit_ack_loss_preserves_claim_and_never_recollects(
    migrated_database_url, monkeypatch, tmp_path, server_committed
):
    from nexus.monitoring import disclosure_forward
    from nexus.persistence.models.data import DataSnapshot
    from sqlalchemy.exc import OperationalError
    from sqlalchemy.orm import SessionTransaction

    factory, store, captured, pointer, calls, client, invoke = await setup(
        migrated_database_url, monkeypatch, tmp_path, listed_history=True
    )
    real_establish = disclosure_forward.establish_origin
    real_commit = SessionTransaction.commit
    injected = []

    async def tagged(*args, **kwargs):
        row = await real_establish(*args, **kwargs)
        args[0].info["test_forward_commit"] = row.id
        return row

    def commit(transaction, *args, **kwargs):
        if transaction.parent is None and transaction.session.info.pop("test_forward_commit", None):
            injected.append(server_committed)
            if server_committed:
                real_commit(transaction, *args, **kwargs)
            raise OperationalError("synthetic lost commit acknowledgement", None, RuntimeError())
        return real_commit(transaction, *args, **kwargs)

    try:
        with monkeypatch.context() as fault:
            fault.setattr(disclosure_forward, "establish_origin", tagged)
            fault.setattr(SessionTransaction, "commit", commit)
            uncertain = await invoke()
            assert uncertain["status"] == "unknown", uncertain
            assert uncertain["reason"] == "SEC_INDEX_REFRESH_UNAVAILABLE"
            assert uncertain["provider_requests"] is None
        assert injected == [server_committed] and len(calls) == 4
        async with factory() as session:
            origins = tuple(
                await session.scalars(
                    select(DataSnapshot).where(DataSnapshot.source == disclosure_forward.SOURCE)
                )
            )
            assert len(origins) == int(server_committed)
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == refresh.CLAIM)
                )
                == 1
            )
            assert await session.scalar(
                select(func.count())
                .select_from(AuditEvent)
                .where(AuditEvent.action == refresh.RESULT)
            ) == int(server_committed)
            original_ids = [row.id for row in origins]
        # Same normal bucket reads the committed original or occupied claim;
        # neither lost confirmation grants another HTTP collection.
        result = await invoke()
        assert result["provider_requests"] == 0 and len(calls) == 4
        async with factory() as session:
            assert (
                list(
                    await session.scalars(
                        select(DataSnapshot.id).where(
                            DataSnapshot.source == disclosure_forward.SOURCE
                        )
                    )
                )
                == original_ids
            )
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


@pytest.mark.asyncio
async def test_normal_refresh_publishes_forward_origin_once_without_old_events(
    migrated_database_url, monkeypatch, tmp_path
):
    from nexus.monitoring.disclosure_forward import SOURCE, ForwardObservation
    from nexus.monitoring.disclosure_observations import read_disclosure_observation
    from nexus.persistence.models.data import DataSnapshot
    from nexus.persistence.models.monitoring import MonitorPlan, MonitorTrigger

    factory, store, captured, pointer, calls, client, invoke = await setup(
        migrated_database_url, monkeypatch, tmp_path, listed_history=True
    )
    try:
        results = await asyncio.gather(invoke(), invoke())
        assert len(calls) == 4, results
        assert any(r["status"] == "refreshed" for r in results), results
        async with factory() as session:
            plan = await session.scalar(select(MonitorPlan).where(MonitorPlan.kind == "disclosure"))
            observation = await read_disclosure_observation(
                session, plan=plan, now=captured["clock"][0]
            )
            assert isinstance(observation, ForwardObservation)
            assert observation.reason == "SEC_INDEX_FORWARD_BASELINE_ESTABLISHED"
            assert not observation.added
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(DataSnapshot)
                    .where(DataSnapshot.source == SOURCE)
                )
                == 1
            )
            assert await session.scalar(select(func.count()).select_from(MonitorTrigger)) == 0
        assert (await invoke())["provider_requests"] == 0
        assert len(calls) == 4
        captured["clock"][0] = AT + timedelta(minutes=10)
        await _set_test_insert_clock(factory, captured["clock"][0])
        assert (await invoke())["status"] == "refreshed"
        assert len(calls) == 8
        async with factory() as session:
            observation = await read_disclosure_observation(
                session, plan=plan, now=captured["clock"][0]
            )
            assert observation.result == "false" and observation.old_history_gap == "unknown"
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(DataSnapshot)
                    .where(DataSnapshot.source == SOURCE)
                )
                == 1
            )
    finally:
        await client.aclose()
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_forward_route_survives_complete_legacy_baseline_projection(
    migrated_database_url, monkeypatch, tmp_path
):
    factory, store, captured, pointer, calls, client, invoke = await setup(
        migrated_database_url, monkeypatch, tmp_path, listed_history=True
    )
    try:
        assert (await invoke())["status"] == "refreshed"
        original = refresh._capture

        async def complete_baseline(*args, **kwargs):
            # Trusted-reader projection isolates routing from archive parsing.
            # A successor's v1 baseline completeness cannot reset its inherited v2 route.
            row, index = await original(*args, **kwargs)
            return row, index.model_copy(update={"history_complete": True})

        monkeypatch.setattr(refresh, "_capture", complete_baseline)
        captured["clock"][0] = AT + timedelta(minutes=10)
        await _set_test_insert_clock(factory, captured["clock"][0])
        result = await invoke()
        assert result["status"] == "refreshed", result
        assert len(calls) == 8
        async with factory() as session:
            from nexus.monitoring.disclosure_forward import ForwardObservation
            from nexus.persistence.models.monitoring import MonitorPlan

            plan = await session.scalar(select(MonitorPlan).where(MonitorPlan.kind == "disclosure"))
            observed = await refresh.read_disclosure_observation(
                session, plan=plan, now=captured["clock"][0]
            )
            assert isinstance(observed, ForwardObservation) and observed.result == "false"
    finally:
        await client.aclose()
        await factory.kw["bind"].dispose()
