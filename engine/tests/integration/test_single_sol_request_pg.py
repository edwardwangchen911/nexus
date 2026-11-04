import asyncio
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from integration import test_sol_native_preparation_pg as base
from integration.test_sol_native_stage_pg import install_sol_http
from nexus import worker
from nexus.ops import manual_paper
from nexus.ops.manual_paper_host import claim_configured_manual_request
from nexus.paper.manual_entry import manual_host_scope
from nexus.paper.research_request import finish_research_request, prepare_research_request
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.models.runtime import WorkerHeartbeat
from sqlalchemy import func, select, text


async def preopen_fixture_clock(url, monkeypatch):
    """One synthetic morning for archives, DB inserts, claims and model-local clocks."""
    from helpers import current_identity
    from integration import test_research_identity_context
    from integration import test_research_request_pg as pure
    from nexus.agents import snapshots
    from nexus.agents.codex_candidate import require_candidate_database
    from nexus.ops import instrument_identity_local
    from nexus.persistence.session import session_factory
    from nexus.research import current_identity_archive
    from nexus.settings import EngineSettings
    from sqlalchemy.engine import make_url

    factory = session_factory(EngineSettings(database_url=url))
    async with factory.begin() as db:
        await require_candidate_database(db, configured_database=make_url(url).database)
    account = await pure.seed_paper_batch(url)

    async def seeded_account(_url):
        return account

    monkeypatch.setattr(pure, "seed_paper_batch", seeded_account)

    async with factory.begin() as db:
        name = await db.scalar(text("SELECT current_database()"))
        assert name.isidentifier() and name != "postgres"
        wall = await db.scalar(text("SELECT pg_catalog.clock_timestamp()"))
        target = wall.astimezone(ZoneInfo("America/New_York")).replace(
            hour=8, minute=0, second=0, microsecond=0
        )
        while target >= wall or target.weekday() >= 5:
            target -= timedelta(days=1)
        target = target.astimezone(UTC)
        delta = int((wall - target).total_seconds())
        await db.execute(text("CREATE SCHEMA fixture_clock"))
        await db.execute(
            text(f"""CREATE FUNCTION fixture_clock.clock_timestamp() RETURNS timestamptz
            LANGUAGE sql VOLATILE AS $$
            SELECT pg_catalog.clock_timestamp() - interval '{delta} seconds' $$""")
        )
        await db.execute(
            text(f"""CREATE FUNCTION fixture_clock.now() RETURNS timestamptz
            LANGUAGE sql STABLE AS $$
            SELECT pg_catalog.transaction_timestamp() - interval '{delta} seconds' $$""")
        )
        await db.execute(
            text(f'ALTER DATABASE "{name}" SET search_path TO fixture_clock,public,pg_catalog')
        )
        await db.execute(
            text("""DO $$ DECLARE obj record; BEGIN
            FOR obj IN SELECT table_name FROM information_schema.columns
            WHERE table_schema='public' AND column_name='created_at' AND column_default IS NOT NULL
            LOOP EXECUTE format('ALTER TABLE public.%I ALTER COLUMN created_at '
                'SET DEFAULT fixture_clock.now()', obj.table_name);
            END LOOP; END $$""")
        )
    await factory.kw["bind"].dispose()

    class FixtureTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return (datetime.now(UTC) - timedelta(seconds=delta)).astimezone(tz)

    monkeypatch.setattr(worker, "_ONCE_STARTED_AT", target)
    for module in (
        current_identity,
        test_research_identity_context,
        instrument_identity_local,
        current_identity_archive,
        snapshots,
        worker,
    ):
        monkeypatch.setattr(module, "datetime", FixtureTime)


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["complete", "late_history", "host"])
async def test_single_owner(migrated_database_url, tmp_path, monkeypatch, path):
    await preopen_fixture_clock(migrated_database_url, monkeypatch)
    original = base.current_record

    async def price(factory):
        record = await original(factory)
        record["expires_at"] = (
            datetime.fromisoformat(record["verified_at"]) + timedelta(days=7)
        ).isoformat()
        return record

    monkeypatch.setattr(base, "current_record", price)
    factory, settings, store, ids = await base.scope(migrated_database_url, tmp_path)
    settings = settings.model_copy(
        update={"agent_daily_budget_usd": Decimal(13), "release_revision": "a" * 40}
    )
    cal = uuid4()
    async with factory.begin() as db:
        now = await db.scalar(select(func.clock_timestamp()))
        day = now.date()
        db.add(
            ExchangeCalendarAuthorityRecord(
                id=cal,
                exchange="XNYS",
                version="single-v4-fixture",
                source="fixture",
                range_start=day,
                range_end=day + timedelta(days=8),
                query_parameters={},
                completeness_status="complete",
                content_hash="7" * 64,
                published_at=now - timedelta(seconds=1),
            )
        )
        await db.flush()
        for off in range(7):
            dt = day + timedelta(days=off)
            if dt.weekday() > 4:
                continue
            opening = datetime.combine(dt, time(9, 30), ZoneInfo("America/New_York")).astimezone(
                UTC
            )
            close = opening + timedelta(hours=6, minutes=30)
            db.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=cal,
                    session=dt,
                    market_open_at=opening,
                    market_close_at=close,
                    decision_at=close,
                )
            )
        db.add(
            WorkerHeartbeat(
                queue="agent_paper",
                worker_id=f"agent-paper-loop-{uuid4()}:manual:{manual_host_scope(settings)}",
                process_version="nexus-manual-paper-v1+" + "a" * 40,
                migration_head=CURRENT_MIGRATION_HEAD,
                started_at=now,
                seen_at=now,
            )
        )

    async def call(*args):
        return await manual_paper.run(settings, manual_paper.parser().parse_args(list(args)))

    offer = await call("inspect-single-sol-intent", "--instrument-id", str(ids[0]))
    args = [
        "start-single-sol",
        "--instrument-id",
        str(ids[0]),
        "--idempotency-key",
        "one-v4",
        "--expected-intent-hash",
        offer.intent_hash,
    ]
    # Contending callers may get BUSY, but cannot both own a key with different inputs.
    from nexus.agents.budget import ResearchAccountingBusy

    wrong_hash = offer.binding.model_copy(
        update={"deadline": offer.binding.deadline - timedelta(seconds=1)}
    ).intent_hash
    competitors = await asyncio.gather(
        call(*args), call(*(args[:-1] + [wrong_hash])), return_exceptions=True
    )
    assert isinstance(competitors[1], ValueError | ResearchAccountingBusy)
    assert str(competitors[1]) in {"MANUAL_IDEMPOTENCY_CONFLICT", "RESEARCH_ACCOUNTING_BUSY"}
    assert not isinstance(competitors[0], BaseException) or isinstance(
        competitors[0], ResearchAccountingBusy
    )
    receipt = await call(*args)
    assert (await call(*args)).request_id == receipt.request_id
    with pytest.raises(ValueError, match="MANUAL_IDEMPOTENCY_CONFLICT"):
        await call(*(args[:-1] + ["a" * 64]))
    with pytest.raises(ValueError, match="MANUAL_IDEMPOTENCY_CONFLICT"):
        await call("start", "--mode", "research_only", "--idempotency-key", "one-v4")
    for change in (
        {"agent_paper_candidate_batch_id": uuid4()},
        {"agent_paper_formal_model_config_id": uuid4()},
    ):
        with pytest.raises(ValueError, match="MANUAL_IDEMPOTENCY_CONFLICT"):
            await manual_paper.run(
                settings.model_copy(update=change), manual_paper.parser().parse_args(args)
            )
    different = offer.binding.model_copy(
        update={"deadline": offer.binding.deadline - timedelta(seconds=1)}
    )
    with pytest.raises(ValueError, match="MANUAL_IDEMPOTENCY_CONFLICT"):
        await call(*(args[:-1] + [different.intent_hash]))
    with pytest.raises(ValueError, match="MANUAL_IDEMPOTENCY_CONFLICT"):
        await call(args[0], args[1], str(ids[1]), *args[3:])
    from nexus.agents import research_maintenance

    with monkeypatch.context() as m:
        m.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
        assert (await call(*args)).request_id == receipt.request_id
        with pytest.raises(ValueError, match="RESEARCH_MAINTENANCE"):
            await call(*[("new-v4" if item == "one-v4" else item) for item in args])
    async with factory.begin() as db:
        assert await db.scalar(text("SELECT count(*) FROM manual_paper_requests")) == 1
        assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_scopes")) == 0
    from nexus.monitoring.host import INCREMENT_ACTION_HEADROOM_SECONDS
    from nexus.paper import single_sol_research as authority
    from nexus.paper.single_sol_obligations import original_slot

    for late_time in (
        offer.binding.deadline - timedelta(seconds=INCREMENT_ACTION_HEADROOM_SECONDS),
        original_slot(offer.binding.budget_day) + timedelta(minutes=1),
    ):

        async def late(_db):
            return late_time

        with monkeypatch.context() as scoped:
            scoped.setattr(authority, "database_now", late)
            async with factory.begin() as db:
                row = await db.get(ManualPaperRequest, receipt.request_id)
                with pytest.raises(ValueError, match="SINGLE_SOL_NATURAL_WINDOW_PROTECTED"):
                    await authority.recheck_single_sol(db, settings, row)
    if path == "host":
        from nexus.ops.manual_paper_host import dispatch_pure_research_request
        from nexus.paper.manual_request import read_manual_request

        sent, clients = install_sol_http(monkeypatch, store)
        try:
            assert await dispatch_pure_research_request(
                settings, request_id=receipt.request_id, calendar_authority_id=cal
            )
            async with factory.begin() as db:
                row = await db.get(ManualPaperRequest, receipt.request_id)
                view = await read_manual_request(db, row)
                assert view.status == "completed" and view.research.members[0].usage_known
                assert (
                    len(view.research.members) == 1
                    and view.research.members[0].instrument_id == ids[0]
                )
                assert sent == ["research", "critique", "synthesis"] and all(
                    client.is_closed for client in clients
                )
            return
        finally:
            await factory.kw["bind"].dispose()
    async with factory.begin() as db:
        view, claimed = await claim_configured_manual_request(
            db, settings, request_id=receipt.request_id, calendar_authority_id=cal
        )
        assert claimed, view
    async with factory.begin() as db:
        db.info["sec_exhibit_store"] = store
        view = await prepare_research_request(db, settings, request_id=receipt.request_id)
        assert view.research_contract == "single_sol_research_v1"
        (member,) = view.research.members
        assert member.instrument_id == ids[0]

    async def add_late_history(payload):
        if path == "late_history" and payload["stage"] == "research":
            from nexus.audit.service import append_audit_event
            from nexus.paper import preparation_unknown_ack as legacy

            async with factory.begin() as db:
                await append_audit_event(
                    db,
                    actor_id=legacy.ACTOR,
                    action=legacy.ACTION,
                    schema_version=legacy.SCHEMA,
                    payload={"account_id": str(settings.agent_paper_account_id)},
                    occurred_at=await db.scalar(select(func.clock_timestamp())),
                )

    requests, clients = install_sol_http(monkeypatch, store, on_request=add_late_history)
    try:
        assert await worker.run_agent_worker_once(
            settings, job_ids=(member.job_id,), formal_stage_capacity=True
        )
        async with factory.begin() as db:
            row = await db.get(ManualPaperRequest, receipt.request_id)
            facts = (
                await db.execute(
                    text(
                        "select state,details from agent_research_events "
                        "where run_id=:run order by sequence"
                    ),
                    {"run": member.run_id},
                )
            ).all()
            if path == "late_history":
                assert requests == ["research"], facts[-2:]
                assert (
                    await db.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 1
                )
                assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_scopes")) == 1
                return
            assert requests == ["research", "critique", "synthesis"], facts[-2:]
            done = await finish_research_request(db, row=row, cancel_unused=False)
            assert done.status == "completed" and done.research.members[0].usage_known
            assert await db.scalar(text("select count(*) from agent_paper_cycles")) == 0
            from nexus.monitoring.reconciliation import reconcile_account_results

            await reconcile_account_results(
                db,
                account_id=row.account_id,
                instrument_ids=tuple(ids),
                now=await db.scalar(select(func.clock_timestamp())),
            )
            assert await db.scalar(text("select count(*) from research_monitor_plans")) == 0
            assert await db.scalar(text("select count(*) from research_monitor_coverage")) == 0
            assert await db.scalar(text("select count(*) from research_stage_authorization")) == 3
    finally:
        await factory.kw["bind"].dispose()
