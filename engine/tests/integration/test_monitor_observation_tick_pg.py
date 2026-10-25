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
from nexus.monitoring.host import PriceMonitorCoordinator
from nexus.ops import research_monitor
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.monitoring import MonitorCheck, MonitorPlan, MonitorTrigger
from nexus.persistence.models.runtime import AuditEvent, Job, JobLease, OutboxEvent
from pydantic import SecretStr
from sqlalchemy import func, select, text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode",
    ["trigger", "no_trigger", "source_failure", "new_sec_after_install", "restart_next_session"],
)
async def test_observe_cli_to_real_persisted_checks_preserves_research_and_deduplicates(
    migrated_database_url, monkeypatch, tmp_path, mode
):
    restart_next_session = mode == "restart_next_session"
    if restart_next_session:
        mode = "trigger"
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

    async def coordinated_tick(at, *, diagnostic=None):
        results = {}

        async def prepare(bucket):
            results["observed"] = await research_monitor.run(settings, args)
            return results["observed"]

        async def scan():
            results["scanned"] = await research_monitor.run(
                settings, argparse.Namespace(action="scan", calendar_authority_id=CALENDAR_ID)
            )
            return results["scanned"]

        coordinator = PriceMonitorCoordinator(scan, prepare=prepare)
        assert coordinator.last_bucket is None
        assert coordinator.kick(at)
        await coordinator.close()
        assert not coordinator.busy
        assert coordinator.last_diagnostic == diagnostic, json.dumps(results, default=str)
        assert not coordinator.kick(at)
        return results

    if restart_next_session:
        initial = await coordinated_tick(now)
        observed = initial["observed"]
    else:
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
    scanned = (
        initial["scanned"]
        if restart_next_session
        else await research_monitor.run(
            settings, argparse.Namespace(action="scan", calendar_authority_id=CALENDAR_ID)
        )
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

    if restart_next_session:
        # Lose the coordinator's bucket memory and the DB connection pool. The
        # normal callbacks must deduplicate from durable claims/episodes instead.
        tables = (
            "research_monitor_plans",
            "research_monitor_checks",
            "research_monitor_episodes",
            "research_monitor_triggers",
            "research_monitor_batches",
            "research_monitor_increments",
            "research_monitor_coverage",
            "outbox_events",
            "outbox_delivery_attempts",
            "agent_research_runs",
            "agent_research_outputs",
            "agent_daily_budget_reservations",
            "formal_budget_bindings",
            "agent_portfolio_synthesis_budget_reservations",
            "portfolio_decision_budget_reservations",
            "jobs",
            "job_leases",
            "data_snapshots",
        )

        async def persisted():
            async with factory() as session:
                rows = {
                    table: tuple(
                        await session.scalars(
                            text(f'SELECT to_jsonb(t)::text AS row FROM "{table}" t ORDER BY row')
                        )
                    )
                    for table in tables
                }
                rows["source_claims"] = tuple(
                    await session.scalars(
                        text(
                            "SELECT to_jsonb(t)::text AS row FROM audit_events t "
                            "WHERE action IN ('research.monitor.price_refresh', "
                            "'research.monitor.disclosure_refresh_claimed') ORDER BY row"
                        )
                    )
                )
                return rows

        saved = await persisted()
        assert saved["research_monitor_episodes"] and saved["research_monitor_triggers"]
        requests = (tuple(price_calls), tuple(sec_calls))
        await factory.kw["bind"].dispose()
        restarted = await coordinated_tick(now, diagnostic="MONITOR_OBSERVATIONS_INCOMPLETE")
        assert restarted["observed"]["status"] == "partial"
        assert restarted["observed"]["provider_requests"] == 0
        assert (
            restarted["observed"]["sources"]["price"]["reason"] == "MONITOR_PRICE_REFRESH_EXPIRED"
        ), json.dumps(restarted, default=str)
        assert restarted["observed"]["sources"]["disclosure"]["status"] == "unchanged"
        assert restarted["observed"]["sources"]["disclosure"]["provider_requests"] == 0
        assert restarted["observed"]["sources"]["disclosure"]["items"] == []
        assert restarted["scanned"]["new_triggers"] == 0
        assert restarted["scanned"]["notifications"] == 0
        assert await persisted() == saved
        assert (tuple(price_calls), tuple(sec_calls)) == requests

        # This normal producer's Friday plans expire before the next Monday.
        # Do not extend them to manufacture cross-day monitoring coverage.
        next_session = monday.replace(hour=13, minute=36, second=0, microsecond=0)
        async with factory() as session:
            plans = list(await session.scalars(select(MonitorPlan)))
            assert plans and all(plan.valid_until < next_session for plan in plans)
        captured["clock"][0] = next_session
        await _set_test_insert_clock(factory, next_session)
        async with factory() as session, session.begin():
            tuesday = next_session + timedelta(days=1)
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=CALENDAR_ID,
                    session=tuesday.date(),
                    decision_at=tuesday.replace(hour=21),
                    market_open_at=tuesday.replace(hour=13, minute=30),
                    market_close_at=tuesday.replace(hour=20),
                )
            )
        await factory.kw["bind"].dispose()
        following = await coordinated_tick(next_session)
        assert following["observed"]["status"] == "refreshed"
        assert following["observed"]["provider_requests"] == 0
        assert following["observed"]["sources"]["price"]["status"] == "skipped"
        assert (
            following["observed"]["sources"]["price"]["reason"] == "MONITOR_NO_ELIGIBLE_PRICE_PLAN"
        )
        assert following["observed"]["sources"]["disclosure"]["status"] == "unchanged"
        assert following["observed"]["sources"]["disclosure"]["items"] == []
        assert following["scanned"]["unknown"] == following["scanned"]["expired"] == 2
        assert following["scanned"]["true"] == following["scanned"]["false"] == 0
        assert following["scanned"]["new_triggers"] == 0
        assert following["scanned"]["notifications"] == 0
        following_rows = await persisted()
        for table, old_rows in saved.items():
            if table not in {"research_monitor_checks", "research_monitor_batches"}:
                assert following_rows[table] == old_rows, table
                continue
            assert set(old_rows) <= set(following_rows[table]), table
            added = [json.loads(row) for row in set(following_rows[table]) - set(old_rows)]
            assert added, table
            bucket = next_session.replace(minute=35)
            assert all(datetime.fromisoformat(row["scheduled_at"]) == bucket for row in added)
            if table == "research_monitor_checks":
                assert len(added) == 2
                assert all(
                    row["result"] == "unknown"
                    and row["reason"] == "MONITOR_PLAN_EXPIRED_OR_NOT_EFFECTIVE"
                    for row in added
                )
            else:
                assert len(added) == 1
                assert all(row["trigger_ids"] == [] and row["outbox_id"] is None for row in added)
        assert (tuple(price_calls), tuple(sec_calls)) == requests
    await factory.kw["bind"].dispose()
