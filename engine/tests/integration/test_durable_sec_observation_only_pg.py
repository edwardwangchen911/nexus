"""Restricted discovery keeps the audit facts and has no downstream authority."""

import pytest
from sqlalchemy import text


@pytest.mark.asyncio
async def test_observation_only_has_database_fences(migrated_database_url):
    from sqlalchemy.ext.asyncio import create_async_engine

    engine = create_async_engine(migrated_database_url)
    try:
        async with engine.connect() as connection:
            tables = await connection.scalars(
                text("""
                SELECT c.relname FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
                WHERE t.tgname='a_sec_observation_downstream_guard' ORDER BY c.relname
                """)
            )
            assert tables.all() == [
                "data_snapshots",
                "outbox_events",
                "sec_durable_daily_event_receipts",
                "sec_event_consumption_receipts",
                "sec_event_research_bindings",
                "sec_notification_claims",
            ]
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("source", ["new", "failed", "no_proven_cutoff", "closed"])
async def test_restricted_refresh_publishes_only_its_own_check_and_observation(
    migrated_database_url, monkeypatch, tmp_path, source
):
    import json
    from datetime import datetime, timedelta
    from uuid import uuid4
    from zoneinfo import ZoneInfo

    import httpx
    from helpers.durable_test_clock import DisposableClock
    from integration.test_monitor_disclosure_refresh_pg import setup
    from nexus.api.durable_sec import read_durable_sec_view
    from nexus.monitoring import durable_sec_host as host
    from nexus.monitoring import durable_sec_storage as storage
    from nexus.monitoring.durable_sec_claims import claim_notice
    from nexus.monitoring.durable_sec_daily import freeze_durable_sec_pending
    from nexus.ops import durable_sec_index_local
    from nexus.ops import monitor_disclosure_refresh as refresh
    from nexus.paper import host_probe_state as probe
    from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
    from nexus.persistence.models.agents import AgentResearchRunRecord
    from nexus.persistence.models.backtest import (
        ExchangeCalendarAuthorityRecord,
        ExchangeCalendarSessionRecord,
    )
    from nexus.persistence.models.monitoring import MonitorPlan
    from nexus.persistence.models.paper import AgentPaperCycleRecord
    from nexus.persistence.models.runtime import AuditEvent, Job, OutboxEvent, WorkerHeartbeat
    from nexus.persistence.models.sec_monitor import (
        SecDiscoveryCheck,
        SecDiscoveryEvent,
        SecNotificationClaim,
    )
    from sqlalchemy import func, select
    from sqlalchemy.exc import IntegrityError
    from sqlalchemy.ext.asyncio import AsyncSession

    real_scalar = AsyncSession.scalar
    factory, store, captured, pointer, calls, client, invoke = await setup(
        migrated_database_url, monkeypatch, tmp_path
    )
    monkeypatch.setattr(AsyncSession, "scalar", real_scalar)
    for module in (storage, host):
        monkeypatch.setattr(module, "configured_exhibit_store", lambda session: store)
    collect_durable = durable_sec_index_local.collect_durable_index

    async def collect(settings, **kwargs):
        return await collect_durable(settings, **kwargs, store=store)

    monkeypatch.setattr(durable_sec_index_local, "collect_durable_index", collect)
    operation = uuid4()
    worker = f"agent-paper-loop-{operation}:manual:test"
    calendar_id = uuid4()
    try:
        async with factory.begin() as session:
            plan = await session.scalar(select(MonitorPlan).where(MonitorPlan.kind == "disclosure"))
            assert plan is not None
            rule = await storage.enable_rule(
                session, account_id=pointer.account_id, plan_id=plan.id
            )
        async with factory.begin() as session:
            now = await session.scalar(select(func.clock_timestamp()))
            bucket = refresh._bucket(now)
            day = now.astimezone(ZoneInfo("America/New_York")).date()
            old = bucket - timedelta(days=1)
            end = bucket + timedelta(minutes=5)
            start = bucket - timedelta(minutes=5)
            session.add(
                ExchangeCalendarAuthorityRecord(
                    id=calendar_id,
                    exchange="XNYS",
                    source="isolated synthetic calendar",
                    version="observation-test-v1",
                    range_start=day,
                    range_end=day + timedelta(days=1),
                    query_parameters={},
                    completeness_status="complete",
                    content_hash="a" * 64,
                    published_at=old,
                    created_at=old,
                )
            )
            await session.flush()
            for offset in (0, 1):
                next_open = (end if source == "closed" else start) + timedelta(days=offset)
                session.add(
                    ExchangeCalendarSessionRecord(
                        calendar_authority_id=calendar_id,
                        session=day + timedelta(days=offset),
                        market_open_at=next_open,
                        market_close_at=next_open + timedelta(minutes=20),
                        decision_at=next_open + timedelta(hours=1),
                        created_at=old,
                    )
                )
            session.add(
                WorkerHeartbeat(
                    worker_id=worker,
                    queue="agent_paper",
                    started_at=now,
                    seen_at=now,
                    process_version="candidate-test",
                    migration_head=CURRENT_MIGRATION_HEAD,
                )
            )
            await session.flush()
            # Seed the approved restricted state, while the actual capture entry
            # still validates its persisted identity and singleton heartbeat.
            state = probe.MonitorOnlyState(
                worker=worker,
                started=now.isoformat(),
                version="candidate-test",
                consecutive=1,
                total=1,
                reason=probe.MONITOR_REASON,
                stage="monitor_only_startup",
                stage_ms=0,
                elapsed_ms=0,
                operation_id=str(operation),
                recorded_at=now.isoformat(),
                admitting=False,
            )
            await probe._store_state(
                session, pointer.account_id, state, probe.MONITOR_SCHEMA, same=False, expected=None
            )
            job_count = await session.scalar(select(func.count()).select_from(Job))
        captured["clock"][0] = now

        current_source = source

        def serve(request):
            calls.append(str(request.url))
            if request.url.host == "www.sec.gov":
                if current_source == "failed":
                    raise httpx.ReadTimeout("synthetic", request=request)
                return httpx.Response(
                    200, content=(tmp_path / "baseline/directory.json").read_bytes()
                )
            document = json.loads((tmp_path / "baseline/AAPL-submissions.json").read_bytes())
            document["filings"]["files"] = []
            if current_source == "new":
                recent = document["filings"]["recent"]
                for key, value in {
                    "accessionNumber": "0000123456-26-000099",
                    "form": "8-K",
                    "acceptanceDateTime": now.isoformat(),
                    "filingDate": now.date().isoformat(),
                    "primaryDocument": "independent-new.htm",
                }.items():
                    recent[key].append(value)
            return httpx.Response(200, json=document)

        monkeypatch.setattr(
            refresh, "_client", lambda: httpx.AsyncClient(transport=httpx.MockTransport(serve))
        )
        result = await invoke(
            scheduled_at=bucket, calendar_id=calendar_id, restricted_worker=worker
        )
        if source == "closed":
            assert result["status"] == "market_closed"
            assert not calls
            async with factory() as session:
                assert await session.scalar(select(SecDiscoveryCheck)) is None
            return
        assert result["status"] == ("blocked" if source == "failed" else "refreshed"), result
        assert "check_id" in result["items"][0], result
        captured_requests = len(calls)
        repeat = await invoke(
            scheduled_at=bucket, calendar_id=calendar_id, restricted_worker=worker
        )
        assert repeat["items"] == [{"status": "already_claimed"}]
        assert len(calls) == captured_requests

        async with factory.begin() as session:
            check = (await session.scalars(select(SecDiscoveryCheck))).one()
            event = await session.scalar(select(SecDiscoveryEvent))
            run = await session.get(AgentResearchRunRecord, pointer.source_run_id)
            receipt = (
                await session.scalars(select(AuditEvent).where(AuditEvent.action == refresh.RESULT))
            ).one()
            assert check.observation["observation_receipt_id"] == str(receipt.id)
            assert await session.scalar(select(func.count()).select_from(Job)) == job_count
            assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0
            assert await session.scalar(select(func.count()).select_from(SecNotificationClaim)) == 0
            view = await read_durable_sec_view(
                session,
                run,
                now=await storage._time(session),
                include_notice=True,
                include_research=True,
            )
            assert view is not None
            assert view.latest_check is not None
            if source != "new":
                assert (check.result, check.coverage) == (
                    "unknown",
                    "unknown" if source == "failed" else "partial",
                )
                assert view.latest_check.reason == (
                    "collection_failed" if source == "failed" else "accepted_cutoff_unproven"
                )
                assert event is None and not view.events
            else:
                cycle = await session.scalar(
                    select(AgentPaperCycleRecord).where(
                        AgentPaperCycleRecord.account_id == pointer.account_id
                    )
                )
                assert await freeze_durable_sec_pending(
                    session,
                    cycle_id=cycle.id,
                    instrument_id=rule.instrument_id,
                    as_of=await storage._time(session),
                ) == (None, None)

        if source == "failed":
            # A fresh later bucket may recover without reusing or amending
            # the failed bucket's original claim, receipt, or outcome.
            await factory.kw["bind"].dispose()
            clock = DisposableClock(migrated_database_url)
            later = bucket + timedelta(minutes=5, seconds=2)
            async with factory.begin() as session:
                now = await clock.set(session, later)
                heartbeat = await session.scalar(
                    select(WorkerHeartbeat).where(WorkerHeartbeat.worker_id == worker)
                )
                heartbeat.seen_at = now

            class TestClock(datetime):
                @classmethod
                def now(cls, tz=None):
                    return clock.now().astimezone(tz)

            monkeypatch.setattr("nexus.research.sec_disclosure_index.datetime", TestClock)
            monkeypatch.setattr("nexus.ops.research_identity_local.datetime", TestClock)
            captured["clock"][0] = now
            current_source = "new"
            recovered = await invoke(
                scheduled_at=refresh._bucket(now),
                calendar_id=calendar_id,
                restricted_worker=worker,
            )
            assert recovered["status"] == "refreshed" and "check_id" in recovered["items"][0], (
                recovered
            )
            async with factory() as session:
                checks = (
                    await session.scalars(
                        select(SecDiscoveryCheck).order_by(SecDiscoveryCheck.scheduled_at)
                    )
                ).all()
                assert [row.result for row in checks] == ["unknown", "true"]
                assert checks[0].id == check.id and checks[0].reason == "SEC_INDEX_REFRESH_FAILED"
                assert (
                    await session.scalar(select(func.count()).select_from(SecDiscoveryEvent)) == 1
                )
                assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0
            return
        if source != "new":
            return
        async with factory.begin() as session:
            assert (check.result, check.coverage) == ("true", "partial")
            assert event is not None and event.observation_receipt_id == receipt.id
            assert view.event_count == 1
            assert view.events[0].research_status == "observation_only"
            assert view.events[0].notice is None
            assert not await claim_notice(
                session,
                account_id=rule.account_id,
                instrument_id=rule.instrument_id,
                event_key=event.event_key,
                outbox_id=uuid4(),
                legacy_trigger_id=None,
                now=now,
            )
            event_id = event.id
        with pytest.raises(IntegrityError, match="SEC_OBSERVATION_DOWNSTREAM_DENIED"):
            async with factory.begin() as session:
                session.add(
                    OutboxEvent(
                        event_type="research.sec.discovered",
                        schema_version="durable_sec_notice_v1",
                        aggregate_id=event_id,
                        payload={},
                        available_at=now,
                    )
                )
        for table in (
            "sec_event_research_bindings",
            "sec_event_consumption_receipts",
            "sec_durable_daily_event_receipts",
        ):
            with pytest.raises(IntegrityError, match="SEC_OBSERVATION_DOWNSTREAM_DENIED"):
                async with factory.begin() as session:
                    await session.execute(
                        text(f"INSERT INTO {table}(id,event_id) VALUES (:id,:event)"),
                        {"id": uuid4(), "event": event_id},
                    )
        with pytest.raises(IntegrityError, match="SEC_OBSERVATION_DOWNSTREAM_DENIED"):
            async with factory.begin() as session:
                # A direct daily input writer is fenced before admission.
                await session.execute(
                    text("""
                    INSERT INTO data_snapshots(id,payload)
                    VALUES (:id,jsonb_build_object('agent_paper_context',
                      jsonb_build_object('pending_durable_sec',jsonb_build_object(
                        'events', jsonb_build_array(
                          jsonb_build_object('event_id', CAST(:event_id AS text)))
                      ))))
                    """),
                    {"id": uuid4(), "event_id": str(event_id)},
                )
        with pytest.raises(IntegrityError, match="SEC_DURABLE_IMMUTABLE"):
            async with factory.begin() as session:
                await session.execute(
                    text(
                        "UPDATE sec_discovery_events SET observation_receipt_id=NULL WHERE id=:id"
                    ),
                    {"id": event_id},
                )
    finally:
        await client.aclose()
        await factory.kw["bind"].dispose()
