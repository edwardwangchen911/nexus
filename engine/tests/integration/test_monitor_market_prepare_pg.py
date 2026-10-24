"""One automatic market attempt is fenced before normal source I/O."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from nexus.ops import monitor_market_prepare as market
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.monitoring import MonitorObservationBatch
from nexus.persistence.models.runtime import AuditEvent, Job
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["normal", "fresh", "config", "timeout", "cancel", "expired", "envelope"]
)
async def test_once_market_claim_cancellation_and_exact_normal_service(
    migrated_database_url, monkeypatch, tmp_path, mode
):
    from nexus.ops import agent_paper_once as native

    now = datetime.now(UTC)
    account, batch_id, instrument, calendar = [uuid4() for _ in range(4)]
    batch = MonitorObservationBatch(
        id=batch_id, account_id=account, instrument_id=instrument, scheduled_at=now
    )
    identity = SimpleNamespace(instrument_id=instrument, symbol="SYNTHETIC")
    quote = {
        "quote_id": str(uuid4()),
        "data_artifact_id": str(uuid4()),
        "risk_authority_ids": [str(uuid4())] * 3,
        "decision_time": now.isoformat(),
    }
    scope = market.MarketScope(
        batch,
        identity,
        uuid4(),
        uuid4(),
        calendar,
        uuid4(),
        "a" * 64,
        uuid4(),
        "b" * 64,
        now.date(),
        quote if mode == "fresh" else None,
    )
    settings = EngineSettings(
        database_url=migrated_database_url,
        object_store_endpoint="http://127.0.0.1:9999",
        agent_paper_account_id=account,
    )
    environment = {
        "NEXUS_SCHWAB_APP_KEY": "synthetic-key",
        "NEXUS_SCHWAB_APP_SECRET": "synthetic-secret",
        "NEXUS_SCHWAB_CALLBACK_URL": "https://127.0.0.1",
        "ALPACA_API_KEY_ID": "synthetic",
        "ALPACA_API_SECRET_KEY": "synthetic",
    }
    state = {"scope": scope, "calls": 0}
    if mode == "envelope":

        @asynccontextmanager
        async def envelope(seconds):
            assert seconds == 300
            yield
            raise TimeoutError("controlled outer envelope")

        monkeypatch.setattr(
            market,
            "asyncio",
            SimpleNamespace(
                timeout=envelope, shield=asyncio.shield, CancelledError=asyncio.CancelledError
            ),
        )
    entered = asyncio.Event()
    hold = asyncio.Event()

    async def preflight(session, supplied, **kwargs):
        assert supplied.database_url == migrated_database_url
        assert (kwargs["account_id"], kwargs["batch_id"], kwargs["calendar_authority_id"]) == (
            account,
            batch_id,
            calendar,
        )
        return state["scope"]

    monkeypatch.setattr(market, "_preflight", preflight)
    monkeypatch.setattr(native, "S3ProviderEvidenceStore", lambda *args, **kwargs: object())

    @asynccontextmanager
    async def client(**kwargs):
        assert kwargs == {"timeout": 30}
        yield object()

    monkeypatch.setattr(native, "schwab_http_client", client)

    async def producer(session, store, configured, **kwargs):
        state["calls"] += 1
        assert kwargs["identities"] == (identity,)
        assert kwargs["expand_execution_scope"] is False and kwargs["manual_market_window"] is True
        assert kwargs["expected_calendar_authority_id"] == calendar
        assert kwargs["expected_universe_snapshot_id"] == scope.universe_id
        assert kwargs["process_environment"] == environment
        assert kwargs["repo_root"] == tmp_path
        entered.set()
        if mode == "cancel":
            await hold.wait()
        if mode == "timeout":
            raise TimeoutError("synthetic absolute envelope")
        state["scope"] = replace(scope, fresh=quote)

    monkeypatch.setattr(native, "prepare_schwab_decision_authority", producer)
    kwargs = dict(
        account_id=account,
        batch_id=batch_id,
        calendar_authority_id=calendar,
        environment={} if mode == "config" else environment,
        repo_root=tmp_path,
    )
    factory = session_factory(settings)
    try:
        assert await market.confirm_batch_market_preparation(
            settings, account_id=account, batch_id=batch_id
        )
        if mode == "cancel":
            task = asyncio.create_task(market.prepare_batch_market_inputs(settings, **kwargs))
            await asyncio.wait_for(entered.wait(), 10)
            assert not await market.confirm_batch_market_preparation(
                settings, account_id=account, batch_id=batch_id
            )
            concurrent = await market.prepare_batch_market_inputs(settings, **kwargs)
            assert concurrent["status"] == "unknown" and state["calls"] == 1
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            result = await market.prepare_batch_market_inputs(settings, **kwargs)
        else:
            result = await market.prepare_batch_market_inputs(settings, **kwargs)
        assert result["status"] == (
            "blocked"
            if mode == "config"
            else "unknown"
            if mode in {"cancel", "timeout", "envelope"}
            else "prepared"
        )
        assert (
            result["model_requests"] == 0
            and result["job_created"] is False
            and result["execution_locked"] is True
        )
        if mode == "expired":
            state["scope"] = scope
            assert (await market.prepare_batch_market_inputs(settings, **kwargs))[
                "status"
            ] == "blocked"
            assert (await market.prepare_batch_market_inputs(settings, **kwargs))[
                "status"
            ] == "blocked"
        elif mode != "config":
            assert (await market.prepare_batch_market_inputs(settings, **kwargs))[
                "status"
            ] == result["status"]
        assert state["calls"] == (0 if mode in {"config", "fresh"} else 1)
        async with factory() as session:
            events = tuple(await session.scalars(select(AuditEvent)))
            assert "synthetic-secret" not in repr([e.payload for e in events])
            assert len(events) == (
                0 if mode in {"config", "fresh"} else 3 if mode == "expired" else 2
            )
            for model in (Job, AgentResearchRunRecord, AgentDailyBudgetReservationRecord):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
            status = await market.inspect_batch_market_preparation(session, batch_id)
            assert status == (
                "pending"
                if mode in {"config", "fresh"}
                else "blocked"
                if mode == "expired"
                else "unknown"
                if mode in {"cancel", "timeout", "envelope"}
                else "prepared"
            )
        assert await market.confirm_batch_market_preparation(
            settings, account_id=account, batch_id=batch_id
        ) == (mode not in {"cancel", "timeout", "envelope"})
        if events:
            assert not await market.confirm_batch_market_preparation(
                settings, account_id=uuid4(), batch_id=batch_id
            )
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_real_materials_and_native_fresh_inputs_skip_provider(
    migrated_database_url, monkeypatch, tmp_path
):
    from helpers.monitor_materials import prepared_materials
    from helpers.paper_batch import CALENDAR_ID
    from nexus.ops import agent_paper_once as native

    factory, settings, store, captured, pointer, batch_id, materials = await prepared_materials(
        migrated_database_url, monkeypatch, tmp_path
    )
    try:
        async with factory() as session:
            parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
        from datetime import timedelta

        from integration.test_candidate_multistock_path import _set_test_insert_clock
        from integration.test_monitor_increment_pg import append_fresh_inputs
        from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
        from nexus.persistence.models.runtime import JobLease

        async with factory() as session:
            later = await session.scalar(select(func.max(JobLease.expires_at))) + timedelta(
                seconds=1
            )
        captured["clock"][0] = later
        await _set_test_insert_clock(factory, later)
        await append_fresh_inputs(factory, pointer.instrument_id, later)
        async with factory() as session, session.begin():
            monday = later + timedelta(days=3)
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=CALENDAR_ID,
                    session=monday.date(),
                    decision_at=monday.replace(hour=21),
                    market_open_at=monday.replace(hour=13, minute=30),
                    market_close_at=monday.replace(hour=20),
                )
            )
        settings = settings.model_copy(
            update={
                "agent_paper_cycle_enabled": True,
                "agent_paper_account_id": pointer.account_id,
                "agent_paper_formal_model_config_id": parent.model_config_id,
            }
        )

        async def fixed_clock(session):
            return captured["clock"][0]

        monkeypatch.setattr(market, "_clock", fixed_clock)

        async def forbidden(*args, **kwargs):
            raise AssertionError("fresh native inputs must not call source")

        monkeypatch.setattr(native, "prepare_schwab_decision_authority", forbidden)
        async with factory() as session, session.begin():
            verified = await market._preflight(
                session,
                settings,
                account_id=pointer.account_id,
                batch_id=batch_id,
                calendar_authority_id=CALENDAR_ID,
                now=captured["clock"][0],
            )
            assert verified.fresh is not None
        result = await market.prepare_batch_market_inputs(
            settings,
            account_id=pointer.account_id,
            batch_id=batch_id,
            calendar_authority_id=CALENDAR_ID,
            environment={},
            repo_root=tmp_path,
        )
        assert result["status"] == "prepared", result
        assert result["provider_requests"] == 0
    finally:
        await factory.kw["bind"].dispose()
