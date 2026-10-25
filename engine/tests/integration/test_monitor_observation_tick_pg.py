"""Normal current-source CLI, real imports, deterministic scan and one sealed outbox."""

import argparse
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import httpx
import pytest
from helpers.paper_batch import CALENDAR_ID
from integration.test_candidate_multistock_path import _set_test_insert_clock
from integration.test_monitor_batches_pg import installed
from integration.test_schwab_market_stream_import import CREDENTIALS, Socket, Store, _frames
from nexus.ops import research_monitor
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.monitoring import MonitorCheck, MonitorPlan, MonitorTrigger
from nexus.persistence.models.runtime import AuditEvent, Job, JobLease, OutboxEvent
from pydantic import SecretStr
from sqlalchemy import func, select


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["trigger", "no_trigger", "source_failure", "new_sec_after_install"]
)
async def test_observe_cli_to_real_persisted_checks_preserves_research_and_deduplicates(
    migrated_database_url, monkeypatch, tmp_path, mode
):
    from nexus.market import object_store
    from nexus.ops import (
        monitor_disclosure_refresh,
        research_identity_local,
        schwab_market_stream,
        sec_index_local,
    )
    from nexus.providers.market import schwab_oauth

    factory, settings, sec_store, captured, pointer = await installed(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        # Ordinary daily reconciliation may install the audited SEC proposal.
        # A no-trigger source must therefore contain no new SEC facts either.
        added=0 if mode == "no_trigger" else 2,
        kinds=("price",)
        if mode in {"no_trigger", "new_sec_after_install"}
        else ("price", "disclosure"),
    )
    async with factory() as session:
        parent = await session.get(AgentResearchRunRecord, pointer.source_run_id)
        plan = await session.scalar(
            select(MonitorPlan).where(
                MonitorPlan.kind == "price",
                MonitorPlan.account_id == pointer.account_id,
                MonitorPlan.instrument_id == pointer.instrument_id,
            )
        )
        threshold = Decimal(plan.condition["threshold"])
        midpoint = (
            threshold
            if mode == "trigger"
            else threshold / 2
            if plan.condition["operator"] == "gte"
            else threshold * 2
        )
        before = tuple(
            [
                await session.scalar(select(func.count()).select_from(model))
                for model in (
                    AgentResearchRunRecord,
                    Job,
                    JobLease,
                    AgentDailyBudgetReservationRecord,
                )
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
    now = captured["clock"][0] + timedelta(minutes=6)
    captured["clock"][0] = now
    await _set_test_insert_clock(factory, now)
    async with factory() as session, session.begin():
        monday = now + timedelta(days=3)
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=monday.date(),
                decision_at=monday.replace(hour=21),
                market_open_at=monday.replace(hour=13, minute=30),
                market_close_at=monday.replace(hour=20),
            )
        )

    class ClockMeta(type):
        def __instancecheck__(cls, value):
            return isinstance(value, datetime)

    class Clock(datetime, metaclass=ClockMeta):
        @classmethod
        def now(cls, tz=None):
            return captured["clock"][0].astimezone(tz or UTC)

    monkeypatch.setattr(research_identity_local, "datetime", Clock)
    real_http = httpx.AsyncClient
    sec_calls = []
    price_calls = []

    def source_http(request):
        sec_calls.append(str(request.url))
        if mode == "source_failure":
            return httpx.Response(503)
        name = "directory.json" if request.url.host == "www.sec.gov" else "AAPL-submissions.json"
        folder = "baseline" if name == "directory.json" else "current"
        return httpx.Response(200, content=(tmp_path / folder / name).read_bytes())

    monkeypatch.setattr(
        monitor_disclosure_refresh, "configured_exhibit_store", lambda session: sec_store
    )
    monkeypatch.setattr(
        monitor_disclosure_refresh,
        "_client",
        lambda: real_http(transport=httpx.MockTransport(source_http), follow_redirects=False),
    )
    real_collect = sec_index_local.collect_index

    async def collect(settings, **kwargs):
        return await real_collect(settings, **kwargs, store=sec_store)

    monkeypatch.setattr(monitor_disclosure_refresh, "collect_index", collect)

    class Evidence(Store):
        def __init__(self, *a, **kw):
            super().__init__()

    evidence = Evidence()

    class SharedEvidence(Evidence):
        def __new__(cls, *a, **kw):
            return evidence

    monkeypatch.setattr(object_store, "S3ProviderEvidenceStore", SharedEvidence)

    @asynccontextmanager
    async def broker_client(**kwargs):
        async with real_http(
            transport=httpx.MockTransport(
                lambda request: (_ for _ in ()).throw(AssertionError("unexpected broker HTTP"))
            )
        ) as client:
            yield client

    monkeypatch.setattr(schwab_oauth, "schwab_http_client", broker_client)
    real_capture = schwab_market_stream.capture_and_import_schwab_level_one_batch

    async def capture(arguments, client, **kwargs):
        price_calls.append(tuple(arguments.symbol))
        if mode == "source_failure":
            raise ValueError("controlled source unavailable")
        frames = _frames(captured["clock"][0] - timedelta(seconds=1))
        for packet in frames:
            for data in packet.get("data", []):
                for content in data.get("content", []):
                    content["1"] = float(midpoint)
                    content["2"] = float(midpoint)
        result = await schwab_market_stream.collect_schwab_stream(
            symbols=("AAPL",),
            directory=arguments.output_dir,
            streamer_url="wss://streamer-api.schwab.com/ws",
            credentials=CREDENTIALS,
            max_frames=3,
            connector=lambda *a, **kw: Socket(frames),
        )
        path = arguments.output_dir / "receipt.json"
        receipt = json.loads(path.read_text())
        receipt["started_at"] = (now - timedelta(seconds=2)).isoformat()
        receipt["ended_at"] = now.isoformat()
        for frame in receipt["frames"]:
            frame["received_at"] = now.isoformat()
        path.write_text(json.dumps(receipt, sort_keys=True))
        return result

    async def native(*args, **kwargs):
        return await real_capture(*args, **kwargs, capture=capture)

    monkeypatch.setattr(schwab_market_stream, "capture_and_import_schwab_level_one_batch", native)
    environment = {
        "SEC_CONTACT": "nexus-test@example.test",
        "NEXUS_SCHWAB_APP_KEY": "synthetic-key",
        "NEXUS_SCHWAB_APP_SECRET": "synthetic-secret",
        "NEXUS_SCHWAB_CALLBACK_URL": "https://127.0.0.1",
    }
    args = argparse.Namespace(
        action="observe",
        calendar_authority_id=CALENDAR_ID,
        repo_root=tmp_path,
        _observation_environment=environment,
    )
    observed = await research_monitor.run(settings, args)
    assert observed["model_requests"] == 0 and observed["job_created"] is False
    assert observed["status"] == ("partial" if mode == "source_failure" else "refreshed"), (
        json.dumps(observed, default=str)
    )
    if mode == "new_sec_after_install":
        async with factory() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(MonitorPlan)
                    .where(MonitorPlan.kind == "disclosure")
                )
                == 0
            )
    scanned = await research_monitor.run(
        settings, argparse.Namespace(action="scan", calendar_authority_id=CALENDAR_ID)
    )
    assert scanned.get("new_triggers", 0) == (3 if mode == "trigger" else 0), scanned
    assert scanned.get("notifications", 0) == (1 if mode == "trigger" else 0)
    assert scanned["model_requests"] == 0
    if mode == "source_failure":
        assert scanned["unknown"] > 0 and scanned["false"] == 0
    if mode == "no_trigger":
        assert scanned["false"] == 1 and not sec_calls
    if mode == "new_sec_after_install":
        assert scanned["false"] == 1 and not sec_calls
        async with factory() as session:
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(MonitorPlan)
                    .where(MonitorPlan.kind == "disclosure")
                )
                == 1
            )
            stale = await session.scalar(
                select(MonitorCheck).where(MonitorCheck.kind == "disclosure")
            )
            assert stale.result == "unknown" and stale.reason == "SEC_INDEX_STALE"
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == "research.monitor.disclosure_refresh_claimed")
                )
                == 0
            )
    assert len(price_calls) == 1
    if mode == "new_sec_after_install":
        assert not sec_calls
    else:
        assert len(sec_calls) == (
            0 if mode == "no_trigger" else 1 if mode == "source_failure" else 2
        )
    repeated = await research_monitor.run(settings, args)
    again = await research_monitor.run(
        settings, argparse.Namespace(action="scan", calendar_authority_id=CALENDAR_ID)
    )
    if mode == "new_sec_after_install":
        assert again["new_triggers"] == 2 and again["notifications"] == 1
        assert len(price_calls) == 1 and len(sec_calls) == 2
        await research_monitor.run(settings, args)
        final = await research_monitor.run(
            settings, argparse.Namespace(action="scan", calendar_authority_id=CALENDAR_ID)
        )
        assert final["new_triggers"] == final["notifications"] == 0
        assert len(price_calls) == 1 and len(sec_calls) == 2
    else:
        assert again.get("new_triggers", 0) == 0 and len(price_calls) == 1
    assert repeated["model_requests"] == 0
    async with factory() as session:
        assert (
            tuple(
                [
                    await session.scalar(select(func.count()).select_from(model))
                    for model in (
                        AgentResearchRunRecord,
                        Job,
                        JobLease,
                        AgentDailyBudgetReservationRecord,
                    )
                ]
            )
            == before
        )
        if mode == "new_sec_after_install":
            triggers = tuple(await session.scalars(select(MonitorTrigger)))
            assert len(triggers) == 2
            assert all(
                t.kind == "disclosure"
                and t.cause == "disclosure_added"
                and t.instrument_id == pointer.instrument_id
                for t in triggers
            )
            current = json.loads((tmp_path / "current/AAPL-submissions.json").read_bytes())
            baseline = json.loads((tmp_path / "baseline/AAPL-submissions.json").read_bytes())
            added = set(current["filings"]["recent"]["accessionNumber"]) - set(
                baseline["filings"]["recent"]["accessionNumber"]
            )
            assert {t.event_key for t in triggers} == {
                f"sec:{int(current['cik']):010d}:{accession}" for accession in added
            }
            assert len({t.batch_id for t in triggers}) == len({t.outbox_id for t in triggers}) == 1
            assert (
                await session.scalar(
                    select(func.count())
                    .select_from(AuditEvent)
                    .where(AuditEvent.action == "research.monitor.disclosure_refresh_claimed")
                )
                == 1
            )
        else:
            assert await session.scalar(select(func.count()).select_from(MonitorTrigger)) == (
                3 if mode == "trigger" else 0
            )
        count = await session.scalar(
            select(func.count())
            .select_from(OutboxEvent)
            .where(
                OutboxEvent.event_type.in_(
                    ("research.monitor.batch_triggered", "research.monitor.triggered")
                )
            )
        )
        if mode == "new_sec_after_install":
            assert count == 1
        else:
            assert count == (1 if mode == "trigger" else 0)
    await factory.kw["bind"].dispose()
