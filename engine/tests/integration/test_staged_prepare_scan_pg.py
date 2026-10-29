"""Joint native observation/price publication/scan around one staged Worker.

Only network transports are controlled. No live provider, host or notification.
"""

import argparse
import asyncio
import json
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import httpx
import pytest
from helpers.paper_batch import CALENDAR_ID
from integration.test_schwab_market_stream_import import CREDENTIALS, Socket, Store, _frames
from integration.test_staged_worker_pg import exercise
from nexus.market.price_bar_authority import finalize_price_bar_set, prepare_price_bar_set
from nexus.monitoring.host import PriceMonitorCoordinator
from nexus.ops import research_monitor
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord, AgentResearchRunRecord
from nexus.persistence.models.data import PriceBarRecord, PriceBarSetAuthority, PriceSeriesManifest
from nexus.persistence.models.monitoring import MonitorCheck, MonitorIncrement, MonitorPlan
from nexus.persistence.models.runtime import OutboxEvent
from sqlalchemy import select, text


def install_sources(monkeypatch, root, store, captured, prices):
    from nexus.market import object_store
    from nexus.ops import (
        monitor_disclosure_refresh,
        research_identity_local,
        schwab_market_stream,
        sec_index_local,
    )
    from nexus.providers.market import schwab_oauth

    class ClockMeta(type):
        def __instancecheck__(cls, value):
            return isinstance(value, datetime)

    class Clock(datetime, metaclass=ClockMeta):
        @classmethod
        def now(cls, tz=None):
            return captured["clock"][0].astimezone(tz or UTC)

    monkeypatch.setattr(research_identity_local, "datetime", Clock)
    real_http, calls = httpx.AsyncClient, {"sec": [], "price": []}

    def source(request):
        calls["sec"].append(str(request.url))
        name = "directory.json" if request.url.host == "www.sec.gov" else "AAPL-submissions.json"
        folder = "baseline" if name == "directory.json" else "current"
        return httpx.Response(200, content=(root / folder / name).read_bytes())

    monkeypatch.setattr(
        monitor_disclosure_refresh, "configured_exhibit_store", lambda session: store
    )
    monkeypatch.setattr(
        monitor_disclosure_refresh,
        "_client",
        lambda: real_http(transport=httpx.MockTransport(source), follow_redirects=False),
    )
    native_collect = sec_index_local.collect_index

    async def collect(settings, **kwargs):
        return await native_collect(settings, **kwargs, store=store)

    monkeypatch.setattr(monitor_disclosure_refresh, "collect_index", collect)
    evidence = Store()
    monkeypatch.setattr(object_store, "S3ProviderEvidenceStore", lambda *a, **kw: evidence)

    @asynccontextmanager
    async def broker_client(**kwargs):
        async with real_http(
            transport=httpx.MockTransport(
                lambda request: (_ for _ in ()).throw(AssertionError("unexpected broker HTTP"))
            )
        ) as client:
            yield client

    monkeypatch.setattr(schwab_oauth, "schwab_http_client", broker_client)
    native_capture = schwab_market_stream.capture_and_import_schwab_level_one_batch

    async def capture(arguments, client, **kwargs):
        calls["price"].append(tuple(arguments.symbol))
        frames = _frames(captured["clock"][0] - timedelta(seconds=1))
        for packet in frames:
            for data in packet.get("data", []):
                original = data["content"][0]
                data["content"] = [
                    {
                        **original,
                        "key": symbol,
                        "1": float(prices[symbol]),
                        "2": float(prices[symbol]),
                    }
                    for symbol in arguments.symbol
                ]
        result = await schwab_market_stream.collect_schwab_stream(
            symbols=tuple(arguments.symbol),
            directory=arguments.output_dir,
            streamer_url="wss://streamer-api.schwab.com/ws",
            credentials=CREDENTIALS,
            max_frames=3,
            connector=lambda *a, **kw: Socket(frames),
        )
        path = arguments.output_dir / "receipt.json"
        receipt = json.loads(path.read_text())
        now = captured["clock"][0]
        receipt["started_at"], receipt["ended_at"] = (
            (now - timedelta(seconds=2)).isoformat(),
            now.isoformat(),
        )
        for frame in receipt["frames"]:
            frame["received_at"] = now.isoformat()
        path.write_text(json.dumps(receipt, sort_keys=True))
        return result

    async def native(*args, **kwargs):
        return await native_capture(*args, **kwargs, capture=capture)

    monkeypatch.setattr(schwab_market_stream, "capture_and_import_schwab_level_one_batch", native)
    return calls, {
        "SEC_CONTACT": "nexus-test@example.test",
        "NEXUS_SCHWAB_APP_KEY": "synthetic-key",
        "NEXUS_SCHWAB_APP_SECRET": "synthetic-secret",
        "NEXUS_SCHWAB_CALLBACK_URL": "https://127.0.0.1",
    }


async def wait_blocked(factory, pid):
    # Observe real lock ownership; this is a watchdog, not a business timing assertion.
    async with asyncio.timeout(10):
        while True:
            async with factory() as observer:
                blocked = await observer.scalar(
                    text("SELECT cardinality(pg_blocking_pids(:pid))"), {"pid": pid}
                )
            if blocked:
                return
            await asyncio.sleep(0)


@pytest.mark.asyncio
@pytest.mark.parametrize("commit_writer", [False, True])
async def test_normal_prepare_legal_publication_and_five_minute_scan_during_model(
    migrated_database_url, monkeypatch, tmp_path, commit_writer
):
    fixture, results = {}, {}
    from integration import test_monitor_mixed_increment_pg as mixed

    original_materials = mixed.prepared_materials

    async def materials(*args, **kwargs):
        value = await original_materials(*args, **kwargs)
        fixture["value"] = value
        # Install all already-completed daily results through the normal entry
        # before this incremental Job is admitted. No model or source refresh.
        await research_monitor.run(
            value[1], argparse.Namespace(action="reconcile", calendar_authority_id=CALENDAR_ID)
        )
        original_append = mixed.append_fresh_inputs

        async def fresh_nontrigger(factory, instrument_id, at, **options):
            async with factory() as session:
                plan = await session.scalar(
                    select(MonitorPlan)
                    .where(MonitorPlan.instrument_id == instrument_id, MonitorPlan.kind == "price")
                    .order_by(MonitorPlan.generation.desc())
                    .limit(1)
                )
                safe = Decimal(plan.condition["threshold"]) * (
                    Decimal("0.5") if plan.condition["operator"] == "gte" else 2
                )
            return await original_append(
                factory, instrument_id, at, **(options | {"midpoint": safe})
            )

        # A new synthetic observation can resolve the old latched price condition;
        # the sealed trigger and research snapshot remain the original facts.
        monkeypatch.setattr(mixed, "append_fresh_inputs", fresh_nontrigger)
        return value

    monkeypatch.setattr(mixed, "prepared_materials", materials)

    async def phase(phase, factory, settings):
        if phase != "model":
            return
        _, _, store, captured, pointer, _, _ = fixture["value"]
        async with factory() as session:
            member = await session.scalar(select(MonitorIncrement))
            run = await session.get(AgentResearchRunRecord, member.run_id)
            frozen = tuple(
                await session.scalars(
                    select(AgentDataSnapshotEvidenceRecord).where(
                        AgentDataSnapshotEvidenceRecord.data_snapshot_id == run.data_snapshot_id
                    )
                )
            )
            original_links = {
                (row.evidence_ref_id, row.authority_kind): (row.content_hash, row.selected_at)
                for row in frozen
            }
            authority = await session.scalar(
                select(PriceBarSetAuthority).where(
                    PriceBarSetAuthority.evidence_ref_id.in_(
                        [row.evidence_ref_id for row in frozen]
                    )
                )
            )
            assert authority is not None
            old_manifest = await session.get(
                PriceSeriesManifest, authority.price_series_manifest_id
            )
            bars = tuple(
                await session.scalars(
                    select(PriceBarRecord).where(
                        PriceBarRecord.price_series_manifest_id == old_manifest.id
                    )
                )
            )
            assert bars and old_manifest.daily_calendar_authority_id is not None
            plans = tuple(
                await session.scalars(select(MonitorPlan).where(MonitorPlan.kind == "price"))
            )
            from nexus.persistence.models.market import InstrumentAlias

            prices = {}
            for plan in plans:
                symbol = await session.scalar(
                    select(InstrumentAlias.symbol)
                    .where(
                        InstrumentAlias.instrument_id == plan.instrument_id,
                        InstrumentAlias.valid_from <= run.decision_time.date(),
                    )
                    .order_by(InstrumentAlias.valid_from.desc())
                    .limit(1)
                )
                prices[symbol] = Decimal(plan.condition["threshold"]) * (
                    Decimal("0.5") if plan.condition["operator"] == "gte" else 2
                )
            before_checks = set(await session.scalars(select(MonitorCheck.id)))
            before_outbox = set(await session.scalars(select(OutboxEvent.id)))
            before_budgets = await mixed.budget_rows(session)
        calls, environment = install_sources(monkeypatch, tmp_path, store, captured, prices)
        # Same source lineage, a new manifest identity: old frozen rows are never edited.
        new_id = uuid4()
        writer_pid = asyncio.get_running_loop().create_future()
        published, release_writer = asyncio.Event(), asyncio.Event()
        publication = {}

        async def writer():
            async with factory() as session:
                await session.begin()
                writer_pid.set_result(await session.scalar(text("SELECT pg_backend_pid()")))
                values = {
                    c.name: getattr(old_manifest, c.name)
                    for c in old_manifest.__table__.columns
                    if c.name not in {"id", "evidence_ref_id", "created_at"}
                }
                new = PriceSeriesManifest(id=new_id, **values)
                session.add(new)
                await session.flush()
                for bar in bars:
                    values = {
                        c.name: getattr(bar, c.name)
                        for c in bar.__table__.columns
                        if c.name not in {"id", "price_series_manifest_id", "created_at"}
                    }
                    session.add(PriceBarRecord(price_series_manifest_id=new_id, **values))
                await session.flush()
                finalized = await finalize_price_bar_set(session, new)
                assert finalized is not None
                publication["hash"] = finalized[0].content_hash
                publication["bars"] = finalized[0].bar_count
                published.set()
                await release_writer.wait()
                if commit_writer:
                    await session.commit()
                else:
                    await session.rollback()

        async def prepare(bucket):
            results["prepared"] = await research_monitor.run(
                settings,
                argparse.Namespace(
                    action="observe",
                    calendar_authority_id=CALENDAR_ID,
                    repo_root=tmp_path,
                    _observation_environment=environment,
                ),
            )
            return results["prepared"]

        async def scan():
            results["scanned"] = await research_monitor.scan_configured_once(
                settings, calendar_authority_id=CALENDAR_ID
            )
            return results["scanned"]

        task = None
        coordinator = PriceMonitorCoordinator(scan, prepare=prepare)
        try:
            async with factory.begin() as reader:
                prior = await prepare_price_bar_set(
                    reader, old_manifest.id, as_of=run.decision_time
                )
                assert prior is not None
                task = asyncio.create_task(writer())
                await wait_blocked(factory, await writer_pid)
                assert not published.is_set()
            await asyncio.wait_for(published.wait(), 10)
            async with factory() as observer:
                assert await observer.get(PriceSeriesManifest, new_id) is None
                assert (
                    await observer.scalar(text("SELECT count(*) FROM stage_proto_authorization"))
                    == 1
                )
                assert (
                    await observer.scalar(text("SELECT count(*) FROM stage_proto_observation")) == 0
                )
            # Start the normal preparation while a lawful publication is in-flight.
            assert coordinator.kick(captured["clock"][0])
            async with asyncio.timeout(10):
                while True:
                    async with factory() as observer:
                        waiting = await observer.scalar(
                            text(
                                "SELECT count(*) FROM pg_stat_activity "
                                "WHERE datname=current_database() "
                                "AND :writer=ANY(pg_blocking_pids(pid))"
                            ),
                            {"writer": await writer_pid},
                        )
                    if waiting:
                        break
                    if not coordinator.busy:
                        # The scanner may use only the prior frozen version and
                        # complete independently of a different manifest's lock.
                        break
                    await asyncio.sleep(0)
            release_writer.set()
            await task
            await coordinator.close()
            assert results["prepared"]["status"] == "refreshed", json.dumps(results, default=str)
            assert results["prepared"]["model_requests"] == 0
            assert results["prepared"]["sources"]["price"]["status"] == "prepared", json.dumps(
                results, default=str
            )
            assert results["prepared"]["sources"]["price"]["reason"] == "MONITOR_PRICE_REFRESHED"
            assert results["prepared"]["sources"]["price"]["capture_count"] == 1
            assert len(calls["price"]) == 1
            assert results["scanned"]["new_triggers"] == results["scanned"]["notifications"] == 0
            assert coordinator.last_diagnostic is None
            assert not coordinator.kick(captured["clock"][0])
            counts = {key: len(value) for key, value in calls.items()}
            again = await research_monitor.run(
                settings,
                argparse.Namespace(
                    action="observe",
                    calendar_authority_id=CALENDAR_ID,
                    repo_root=tmp_path,
                    _observation_environment=environment,
                ),
            )
            assert again["model_requests"] == 0
            assert {key: len(value) for key, value in calls.items()} == counts
            repeated = await scan()
            assert repeated["new_triggers"] == repeated["notifications"] == 0
            async with factory() as session:
                assert bool(await session.get(PriceSeriesManifest, new_id)) == commit_writer
                new_authority = await session.scalar(
                    select(PriceBarSetAuthority).where(
                        PriceBarSetAuthority.price_series_manifest_id == new_id
                    )
                )
                assert bool(new_authority) == commit_writer
                if commit_writer:
                    prepared_new = await prepare_price_bar_set(
                        session, new_id, as_of=run.decision_time
                    )
                    assert prepared_new is not None
                    assert (
                        prepared_new.content_hash
                        == new_authority.content_hash
                        == publication["hash"]
                    )
                    assert prepared_new.bar_count == new_authority.bar_count == publication["bars"]
                assert await mixed.budget_rows(session) == before_budgets
                assert (
                    await prepare_price_bar_set(session, old_manifest.id, as_of=run.decision_time)
                    == prior
                )
                links = tuple(
                    await session.scalars(
                        select(AgentDataSnapshotEvidenceRecord).where(
                            AgentDataSnapshotEvidenceRecord.data_snapshot_id == run.data_snapshot_id
                        )
                    )
                )
                assert {
                    (row.evidence_ref_id, row.authority_kind): (row.content_hash, row.selected_at)
                    for row in links
                } == original_links
                checks = tuple(await session.scalars(select(MonitorCheck)))
                fresh_checks = [
                    row for row in checks if row.id not in before_checks and row.kind == "price"
                ]
                assert {row.plan_id for row in fresh_checks} == {plan.id for plan in plans}
                from nexus.monitoring.observations import read_latest_price

                for checked in fresh_checks:
                    assert checked.plan_id in {plan.id for plan in plans}
                    assert checked.result == "false" and checked.reason is None, (
                        checked.result,
                        checked.reason,
                    )
                    observation = await read_latest_price(
                        session, instrument_id=checked.instrument_id, now=checked.checked_at
                    )
                    assert observation is not None
                    assert checked.observation == observation.model_dump(mode="json")
                    assert (
                        checked.observation
                        in results["prepared"]["sources"]["price"]["observations"]
                    )
                assert all(
                    row.scheduled_at.minute % 5 == 0 and row.scheduled_at.second == 0
                    for row in checks
                )
                assert set(await session.scalars(select(OutboxEvent.id))) == before_outbox
        finally:
            release_writer.set()
            if task is not None:
                await asyncio.gather(task, return_exceptions=True)
            await coordinator.close()

    errors = []

    async def checked_phase(*args):
        try:
            return await phase(*args)
        except Exception as error:
            errors.append(error)
            raise

    try:
        await exercise(
            migrated_database_url, monkeypatch, tmp_path, "unresolved", phase_hook=checked_phase
        )
    finally:
        if errors:
            raise errors[0]
    assert results["prepared"]["execution_locked"] is True
