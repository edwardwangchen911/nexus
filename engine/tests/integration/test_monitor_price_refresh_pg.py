"""Real durable source ticks, with the normal L1 capture boundary isolated."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from nexus.ops import monitor_price_refresh as refresh
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.runtime import AuditEvent, Job, JobLease
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from pydantic import SecretStr
from sqlalchemy import func, select


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode",
    ["normal", "multi", "fresh", "config", "timeout", "cancel", "changed", "expired", "envelope"],
)
async def test_tick_claim_native_capture_and_no_replay(
    migrated_database_url, monkeypatch, tmp_path, mode
):
    from nexus.market import object_store
    from nexus.ops import schwab_market_stream as native
    from nexus.persistence.models.data import DataSnapshot
    from nexus.providers.market import schwab_oauth

    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    account, calendar, instrument, candidate, universe, model, plan = [uuid4() for _ in range(7)]
    identity = SimpleNamespace(
        instrument_id=instrument,
        symbol="SYNTHETIC",
        model_dump=lambda **kw: {"instrument_id": str(instrument), "symbol": "SYNTHETIC"},
    )
    quote_id, artifact_id = uuid4(), uuid4()
    refs = (
        {
            "instrument_id": str(instrument),
            "quote_id": str(quote_id),
            "artifact_id": str(artifact_id),
            "content_hash": "c" * 64,
            "observed_at": now.isoformat(),
        },
    )
    scope = refresh.PriceScope(
        account,
        calendar,
        candidate,
        universe,
        model,
        "a" * 64,
        ((str(plan), "b" * 64),),
        (identity,),
        (identity,),
        (),
    )
    if mode == "multi":
        second = uuid4()
        identity2 = SimpleNamespace(
            instrument_id=second,
            symbol="SECOND",
            model_dump=lambda **kw: {"instrument_id": str(second), "symbol": "SECOND"},
        )
        refs += (
            {
                "instrument_id": str(second),
                "quote_id": str(uuid4()),
                "artifact_id": str(artifact_id),
                "content_hash": "d" * 64,
                "observed_at": now.isoformat(),
            },
        )
        scope = replace(
            scope,
            identities=(identity, identity2),
            needed=(identity, identity2),
            plans=scope.plans + ((str(uuid4()), "d" * 64),),
        )
    state = {
        "scope": replace(scope, needed=(), observations=refs) if mode == "fresh" else scope,
        "calls": 0,
        "now": now,
    }
    settings = EngineSettings(
        database_url=migrated_database_url,
        object_store_endpoint="http://127.0.0.1:9999",
        object_store_region="us-east-1",
        object_store_bucket="synthetic",
        object_store_prefix="synthetic",
        object_store_access_key_id="synthetic",
        object_store_secret_access_key="synthetic",
        agent_paper_cycle_enabled=True,
        agent_paper_account_id=account,
        agent_paper_formal_model_config_id=model,
        agent_paper_candidate_batch_id=candidate,
    )
    environment = {
        "NEXUS_SCHWAB_APP_KEY": "synthetic-key",
        "NEXUS_SCHWAB_APP_SECRET": "synthetic-secret",
        "NEXUS_SCHWAB_CALLBACK_URL": "https://127.0.0.1",
    }

    async def clock(session):
        return state["now"]

    async def scope_for(session, supplied, **kwargs):
        assert kwargs["account_id"] == account and kwargs["calendar_authority_id"] == calendar
        return state["scope"]

    monkeypatch.setattr(refresh, "_clock", clock)
    monkeypatch.setattr(refresh, "_select_scope", scope_for)

    class Store:
        def __init__(self, *args, **kwargs):
            pass

    monkeypatch.setattr(object_store, "S3ProviderEvidenceStore", Store)

    @asynccontextmanager
    async def client(**kwargs):
        assert kwargs == {"timeout": 30}
        yield object()

    monkeypatch.setattr(schwab_oauth, "schwab_http_client", client)
    entered, release = asyncio.Event(), asyncio.Event()

    async def capture(session, store, **kwargs):
        state["calls"] += 1
        assert kwargs["bindings"] == {row.symbol: row.instrument_id for row in scope.needed}
        assert kwargs["max_instruments"] == 5
        assert kwargs["process_environment"] == environment
        assert kwargs["repo_root"] == tmp_path
        assert "checked_at" not in kwargs  # Never stamp the tick time onto source observations.
        entered.set()
        if mode == "cancel":
            await release.wait()
        if mode in {"timeout", "envelope"}:
            if mode == "envelope":
                assert state["envelope"] == 90
            raise TimeoutError("synthetic transport uncertainty")
        state["scope"] = replace(scope, needed=(), observations=refs)
        if mode == "changed":
            session.add(
                DataSnapshot(
                    source="synthetic-price-import",
                    provider_object_id="original",
                    revision="1",
                    observed_at=now,
                    available_at=now,
                    received_at=now,
                    schema_version="synthetic_v1",
                    payload={},
                )
            )
            await session.flush()
            state["scope"] = replace(state["scope"], plans=((str(uuid4()), "b" * 64),))
        return SimpleNamespace(
            quote_ids={
                row.symbol: UUID(ref["quote_id"])
                for row, ref in zip(scope.needed, refs, strict=True)
            },
            artifact_id=artifact_id,
        )

    monkeypatch.setattr(native, "capture_and_import_schwab_level_one_batch", capture)
    if mode == "envelope":

        @asynccontextmanager
        async def timeout(seconds):
            assert seconds in {90, 5}
            if seconds == 90:
                state["envelope"] = seconds
            yield

        monkeypatch.setattr(
            refresh,
            "asyncio",
            SimpleNamespace(
                timeout=timeout, shield=asyncio.shield, CancelledError=asyncio.CancelledError
            ),
        )
    kwargs = dict(
        account_id=account,
        calendar_authority_id=calendar,
        scheduled_at=now,
        environment={} if mode == "config" else environment,
        repo_root=tmp_path,
    )
    if mode != "config":
        refresh._configured(settings, environment, tmp_path)
    factory = session_factory(settings)
    try:
        if mode == "cancel":
            task = asyncio.create_task(refresh.refresh_price_observations(settings, **kwargs))
            await asyncio.wait_for(entered.wait(), 10)
            concurrent = await refresh.refresh_price_observations(settings, **kwargs)
            assert concurrent["status"] == "unknown" and concurrent["provider_requests"] is None
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            result = await refresh.refresh_price_observations(settings, **kwargs)
        else:
            result = await refresh.refresh_price_observations(settings, **kwargs)
        expected = (
            "blocked"
            if mode == "config"
            else "unknown"
            if mode in {"timeout", "cancel", "changed", "envelope"}
            else "prepared"
        )
        assert result["status"] == expected, result
        assert (
            result["model_requests"] == 0
            and result["job_created"] is False
            and result["execution_locked"] is True
        )
        if mode == "expired":
            state["scope"] = scope
            result = await refresh.refresh_price_observations(settings, **kwargs)
            assert result["status"] == "blocked"
        repeat = await refresh.refresh_price_observations(settings, **kwargs)
        assert repeat["status"] == result["status"]
        assert state["calls"] == (0 if mode in {"fresh", "config"} else 1)
        async with factory() as session:
            rows = tuple(await session.scalars(select(AuditEvent)))
            assert len(rows) == (0 if mode in {"fresh", "config"} else 2)
            assert "synthetic-secret" not in repr([e.payload for e in rows])
            if mode == "changed":
                assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 0
            for table in (Job, AgentResearchRunRecord, AgentDailyBudgetReservationRecord):
                assert await session.scalar(select(func.count()).select_from(table)) == 0
        if mode == "normal":
            state["now"] += timedelta(minutes=5)
            state["scope"] = scope
            result = await refresh.refresh_price_observations(
                settings, **{**kwargs, "scheduled_at": state["now"]}
            )
            assert result["status"] == "prepared" and state["calls"] == 2
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("price", [True, False])
async def test_native_installed_plan_fresh_then_real_l1_import(
    migrated_database_url, monkeypatch, tmp_path, price
):
    """Only the socket is synthetic: normal archive parser/import and plan readers run."""
    import json

    import httpx
    from helpers.paper_batch import CALENDAR_ID
    from integration.test_candidate_multistock_path import _set_test_insert_clock
    from integration.test_monitor_batches_pg import installed
    from integration.test_schwab_market_stream_import import CREDENTIALS, Socket, Store, _frames
    from nexus.market import object_store
    from nexus.ops import schwab_market_stream as native
    from nexus.persistence.models.data import DataArtifact
    from nexus.persistence.models.market import LatestQuote
    from nexus.providers.market import schwab_oauth

    factory, settings, store, captured, plan = await installed(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        kinds=("price", "disclosure") if price else ("disclosure",),
    )

    async def research_facts(session):
        return tuple(
            [
                tuple(
                    dict(row)
                    for row in (
                        await session.execute(select(table.__table__).order_by(table.id))
                    ).mappings()
                )
                for table in (
                    Job,
                    JobLease,
                    AgentResearchRunRecord,
                    AgentDailyBudgetReservationRecord,
                )
            ]
        )

    async with factory() as session:
        before_facts = await research_facts(session)
        parent = await session.get(AgentResearchRunRecord, plan.source_run_id)
        before = tuple(
            [
                await session.scalar(select(func.count()).select_from(table))
                for table in (Job, AgentResearchRunRecord, AgentDailyBudgetReservationRecord)
            ]
        )
    settings = settings.model_copy(
        update={
            "agent_paper_cycle_enabled": True,
            "agent_paper_formal_model_config_id": parent.model_config_id,
            "object_store_region": "us-east-1",
            "object_store_bucket": "synthetic",
            "object_store_prefix": "synthetic",
            "object_store_access_key_id": SecretStr("synthetic"),
            "object_store_secret_access_key": SecretStr("synthetic"),
        }
    )

    from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord

    async with factory() as session, session.begin():
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

    async def clock(session):
        return captured["clock"][0]

    monkeypatch.setattr(refresh, "_clock", clock)
    kwargs = dict(
        account_id=plan.account_id,
        calendar_authority_id=CALENDAR_ID,
        scheduled_at=refresh._bucket(captured["clock"][0]),
        environment={},
        repo_root=tmp_path,
    )
    calls = []
    original = native.capture_and_import_schwab_level_one_batch

    class EvidenceStore(Store):
        def __init__(self, *args, **kwargs):
            super().__init__()

    evidence = EvidenceStore()
    monkeypatch.setattr(object_store, "S3ProviderEvidenceStore", lambda *a, **kw: evidence)

    @asynccontextmanager
    async def client(**kwargs):
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: (_ for _ in ()).throw(AssertionError("no HTTP expected"))
            )
        ) as connection:
            yield connection

    monkeypatch.setattr(schwab_oauth, "schwab_http_client", client)

    async def capture(arguments, client, **options):
        calls.append(arguments.symbol)
        assert arguments.symbol == ["AAPL"] and arguments.duration == 30.0
        at = captured["clock"][0]
        result = await native.collect_schwab_stream(
            symbols=("AAPL",),
            directory=arguments.output_dir,
            streamer_url="wss://streamer-api.schwab.com/ws",
            credentials=CREDENTIALS,
            max_frames=3,
            connector=lambda *a, **kw: Socket(_frames(at - timedelta(seconds=1))),
        )
        path = arguments.output_dir / "receipt.json"
        receipt = json.loads(path.read_text())
        receipt["started_at"] = (at - timedelta(seconds=2)).isoformat()
        receipt["ended_at"] = at.isoformat()
        for frame in receipt["frames"]:
            frame["received_at"] = at.isoformat()
        path.write_text(json.dumps(receipt, sort_keys=True))
        return result

    async def normal_capture(*args, **options):
        return await original(*args, **options, capture=capture)

    monkeypatch.setattr(native, "capture_and_import_schwab_level_one_batch", normal_capture)
    try:
        if not price:
            missing = await refresh.refresh_price_observations(settings, **kwargs)
            assert (
                missing["status"] == "skipped"
                and missing["reason"] == "MONITOR_NO_ELIGIBLE_PRICE_PLAN"
            ), missing
            assert missing["provider_requests"] == 0 and not calls
            return
        async with factory() as session, session.begin():
            await refresh._locks(session, plan.account_id)
            verified = await refresh._select_scope(
                session,
                settings,
                account_id=plan.account_id,
                calendar_authority_id=CALENDAR_ID,
                now=captured["clock"][0],
            )
            assert not verified.needed
        fresh = await refresh.refresh_price_observations(settings, **kwargs)
        assert fresh["status"] == "prepared" and fresh["provider_requests"] == 0, fresh
        assert not calls
        captured["clock"][0] += timedelta(seconds=61)
        await _set_test_insert_clock(factory, captured["clock"][0])
        environment = {
            "NEXUS_SCHWAB_APP_KEY": "synthetic-key",
            "NEXUS_SCHWAB_APP_SECRET": "synthetic-secret",
            "NEXUS_SCHWAB_CALLBACK_URL": "https://127.0.0.1",
        }
        result = await refresh.refresh_price_observations(
            settings, **{**kwargs, "environment": environment}
        )
        assert result["status"] == "prepared" and result["capture_count"] == 1, result
        assert calls == [["AAPL"]]
        assert len(evidence.objects) == 1
        async with factory() as session:
            assert await research_facts(session) == before_facts
            reference = result["observations"][0]
            quote = await session.get(LatestQuote, UUID(reference["quote_id"]))
            artifact = await session.get(DataArtifact, quote.data_artifact_id)
            assert quote.observed_at == captured["clock"][0] - timedelta(seconds=1)
            assert (
                quote.available_at
                == quote.received_at
                == artifact.available_at
                == captured["clock"][0]
            )
            assert (
                tuple(
                    [
                        await session.scalar(select(func.count()).select_from(table))
                        for table in (
                            Job,
                            AgentResearchRunRecord,
                            AgentDailyBudgetReservationRecord,
                        )
                    ]
                )
                == before
            )
        repeated = await refresh.refresh_price_observations(
            settings, **{**kwargs, "environment": environment}
        )
        assert repeated["status"] == "prepared" and calls == [["AAPL"]]
        captured["clock"][0] = captured["clock"][0].replace(hour=21)
        closed = await refresh.refresh_price_observations(
            settings,
            **{
                **kwargs,
                "scheduled_at": refresh._bucket(captured["clock"][0]),
                "environment": environment,
            },
        )
        assert closed["status"] == "skipped" and closed["reason"] == "MONITOR_MARKET_CLOSED", closed
        assert closed["provider_requests"] == 0 and calls == [["AAPL"]]
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["timeout", "cancel"])
async def test_cancellation_during_close_has_no_background_writer(
    migrated_database_url, monkeypatch, tmp_path, first
):
    account, calendar, instrument, candidate, model = [uuid4() for _ in range(5)]
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    identity = SimpleNamespace(
        instrument_id=instrument,
        symbol="SYNTHETIC",
        model_dump=lambda **kw: {"instrument_id": str(instrument)},
    )
    scope = refresh.PriceScope(
        account,
        calendar,
        candidate,
        uuid4(),
        model,
        "a" * 64,
        ((str(uuid4()), "b" * 64),),
        (identity,),
        (identity,),
        (),
    )
    settings = EngineSettings(
        database_url=migrated_database_url,
        agent_paper_cycle_enabled=True,
        agent_paper_account_id=account,
        agent_paper_formal_model_config_id=model,
    )
    capture_entered, close_entered, release = asyncio.Event(), asyncio.Event(), asyncio.Event()
    state = {"lock_calls": 0, "active_close": 0, "source_calls": 0, "scan_calls": 0}
    original_locks = refresh._locks

    async def locks(session, owner):
        state["lock_calls"] += 1
        if state["lock_calls"] == 2:
            state["cleanup_task"] = asyncio.current_task()
            state["active_close"] += 1
            close_entered.set()
            try:
                await release.wait()
            finally:
                state["active_close"] -= 1
        await original_locks(session, owner)

    async def clock(session):
        return now

    async def select_scope(*args, **kwargs):
        return scope

    async def produce(*args, **kwargs):
        state["source_calls"] += 1
        capture_entered.set()
        if first == "timeout":
            raise TimeoutError("controlled source timeout")
        await asyncio.Event().wait()

    monkeypatch.setattr(refresh, "_locks", locks)
    monkeypatch.setattr(refresh, "_clock", clock)
    monkeypatch.setattr(refresh, "_select_scope", select_scope)
    monkeypatch.setattr(refresh, "_configured", lambda *args: None)
    monkeypatch.setattr(refresh, "_produce", produce)
    kwargs = dict(
        account_id=account,
        calendar_authority_id=calendar,
        scheduled_at=now,
        environment={},
        repo_root=tmp_path,
    )

    async def coordinator():
        await refresh.refresh_price_observations(settings, **kwargs)
        state["scan_calls"] += 1

    task = asyncio.create_task(coordinator())
    factory = session_factory(settings)
    try:
        await asyncio.wait_for(capture_entered.wait(), 10)
        if first == "cancel":
            task.cancel()
        await asyncio.wait_for(close_entered.wait(), 10)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert state["active_close"] == state["scan_calls"] == 0
        assert state["cleanup_task"] is task
        async with factory() as session:
            events = tuple(await session.scalars(select(AuditEvent)))
            assert len(events) == 1 and events[0].payload["phase"] == "claimed"
            for table in (Job, JobLease, AgentResearchRunRecord, AgentDailyBudgetReservationRecord):
                assert await session.scalar(select(func.count()).select_from(table)) == 0
        repeated = await refresh.refresh_price_observations(settings, **kwargs)
        assert repeated["status"] == "unknown" and repeated["provider_requests"] is None
        assert state["source_calls"] == 1 and state["scan_calls"] == 0
    finally:
        release.set()
        cleanup = state.get("cleanup_task")
        if cleanup is not None and cleanup is not task:
            await asyncio.gather(cleanup, return_exceptions=True)
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await factory.kw["bind"].dispose()
