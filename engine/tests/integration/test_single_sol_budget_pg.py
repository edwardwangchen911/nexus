"""Shared-day allocation races. Only synthetic native reservations, never a transport."""

import asyncio
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from helpers.paper_batch import CALENDAR_ID
from integration import test_sol_native_preparation_pg as native
from integration.test_formal_budget_protection_pg import controlled_clock
from nexus.agents.budget import BudgetExhausted
from nexus.agents.persistence import (
    _shared_daily_budget_used,
    reserve_daily_budget,
)
from nexus.paper.formal_budget_protection import apply_protection
from nexus.paper.research_request import prepare_research_request
from nexus.paper.single_sol_budget import freeze_single_sol_budget, has_guard
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError

LIMIT = Decimal("13")
OTHER = Decimal("0.30")


async def ledger_fixture(url, tmp_path, monkeypatch):
    original_price = native.current_record

    async def extended_price(factory):
        result = await original_price(factory)
        result["expires_at"] = (
            datetime.fromisoformat(result["verified_at"]) + timedelta(days=7)
        ).isoformat()
        return result

    monkeypatch.setattr(native, "current_record", extended_price)
    factory, settings, store, _ = await native.scope(url, tmp_path)
    root = await native.admit(factory, settings, store)
    async with factory.begin() as db:
        db.info["sec_exhibit_store"] = store
        result = await prepare_research_request(db, settings, request_id=root)
        # The three fixture runs belong to the previous accounting day.
        old = await db.get(AgentResearchRunRecord, result.research.members[0].run_id)
        instrument = old.instrument_id
        maximum = old.reserved_cost_usd
        day = old.decision_time.date() + timedelta(days=1)
        while day.weekday() >= 5:
            day += timedelta(days=1)
        now = datetime.combine(day, time(9), ZoneInfo("America/New_York")).astimezone(UTC)
        current = await db.get(ExchangeCalendarAuthorityRecord, CALENDAR_ID)
        cid = uuid4()
        db.add(
            ExchangeCalendarAuthorityRecord(
                id=cid,
                exchange="XNYS",
                version="single-budget-fixture",
                source="fixture",
                range_start=day,
                range_end=day + timedelta(days=4),
                query_parameters={},
                completeness_status="complete",
                content_hash="7" * 64,
                published_at=current.published_at,
            )
        )
        await db.flush()
        next_day = day + timedelta(days=1)
        while next_day.weekday() >= 5:
            next_day += timedelta(days=1)
        for session_day in (day, next_day):
            elapsed = session_day - day
            db.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=cid,
                    session=session_day,
                    market_open_at=now + elapsed + timedelta(minutes=30),
                    market_close_at=now + elapsed + timedelta(hours=7),
                    decision_at=now + elapsed + timedelta(hours=7),
                )
            )
        base = {column.name: getattr(old, column.name) for column in old.__table__.columns}
        ids = (uuid4(), uuid4())
        for identity, actor in zip(
            ids, ("local-single-sol-research-v1", "ordinary-fixture"), strict=True
        ):
            db.add(
                AgentResearchRunRecord(
                    **(
                        base
                        | dict(
                            id=identity,
                            actor_id=actor,
                            idempotency_key=str(identity),
                            decision_time=now,
                            created_at=now,
                            input_hash=identity.hex * 2,
                        )
                    )
                )
            )
    return (
        factory,
        settings.model_copy(update={"agent_daily_budget_usd": LIMIT}),
        now,
        instrument,
        maximum,
        ids,
        cid,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("first", "opponent"),
    [("single", "other"), ("other", "other"), ("single", "economic"), ("economic", "economic")],
)
async def test_shared_allocator_serializes_single_scope(
    migrated_database_url, tmp_path, monkeypatch, first, opponent
):
    factory, settings, now, instrument, maximum, ids, calendar_id = await ledger_fixture(
        migrated_database_url, tmp_path, monkeypatch
    )

    async def allocate(db, kind):
        if kind == "economic":
            return await apply_protection(
                db,
                account_id=settings.agent_paper_account_id,
                batch_id=settings.agent_paper_candidate_batch_id,
                enabled=True,
                daily_limit_usd=LIMIT,
                configuration_hash="8" * 64,
                idempotency_key="economic-bind",
                formal_model_config_id=settings.agent_paper_formal_model_config_id,
                now=now,
            )
        source = kind == "single"
        run = await db.get(AgentResearchRunRecord, ids[0 if source else 1])
        await reserve_daily_budget(
            db,
            run_id=run.id,
            now=now,
            daily_limit_usd=LIMIT,
            worst_case_usd=maximum if source else OTHER,
            pricing_version=run.pricing_version,
            pricing_fresh=True,
        )
        if source:
            await freeze_single_sol_budget(
                db,
                settings,
                run_id=run.id,
                instrument_id=instrument,
                calendar_authority_id=calendar_id,
            )

    try:
        second = opponent if first == "single" else "single"
        async with factory() as a, factory() as b, factory() as observer:
            controlled_clock(a, monkeypatch, now)
            controlled_clock(b, monkeypatch, now)
            async with a.begin():
                await allocate(a, first)
                a_pid = await a.scalar(text("select pg_backend_pid()"))
                b_pid = asyncio.get_running_loop().create_future()

                async def contender():
                    async with b.begin():
                        b_pid.set_result(await b.scalar(text("select pg_backend_pid()")))
                        await allocate(b, second)

                pending = asyncio.create_task(contender())
                try:
                    pid = await b_pid
                    async with asyncio.timeout(20):
                        while a_pid not in (
                            await observer.scalar(
                                text("select pg_blocking_pids(:pid)"), {"pid": pid}
                            )
                        ):
                            if pending.done():
                                await pending
                                pytest.fail("contender bypassed the shared budget lock")
                            await asyncio.sleep(0.02)
                except BaseException:
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
                    raise
            error = (
                "SINGLE_SOL_ECONOMIC_SCOPE_CONFLICT"
                if opponent == "economic"
                else "SINGLE_SOL_NATURAL_BUDGET_PROTECTED"
            )
            with pytest.raises((ValueError, BudgetExhausted), match=error):
                await asyncio.wait_for(pending, 20)
        async with factory() as db:
            assert await has_guard(db, now.date()) == (first == "single")
            assert await db.scalar(
                text("SELECT count(*) FROM formal_budget_bindings WHERE business_date=:day"),
                {"day": now.date()},
            ) == int(first == "economic")
            assert await db.scalar(
                text(
                    "SELECT count(*) FROM agent_daily_budget_reservations WHERE business_date=:day"
                ),
                {"day": now.date()},
            ) == int(first != "economic")
            expected = maximum if first == "single" else OTHER if first == "other" else 0
            assert await _shared_daily_budget_used(db, business_date=now.date()) == expected
            assert await db.scalar(text("select count(*) from single_sol_budget_credits")) == 0
    finally:
        await factory.kw["bind"].dispose()


async def seed_source(session, ordinary_reserve, kwargs, old, source_snapshot=None):
    base = {c.name: getattr(old, c.name) for c in old.__table__.columns}
    if source_snapshot is not None:
        base["data_snapshot_id"] = source_snapshot
    identity = uuid4()
    session.add(
        AgentResearchRunRecord(
            **(
                base
                | dict(
                    id=identity,
                    actor_id="local-single-sol-research-v1",
                    idempotency_key=str(identity),
                    input_hash=identity.hex * 2,
                )
            )
        )
    )
    await session.flush()
    await ordinary_reserve(session, **(kwargs | dict(run_id=identity)))
    return identity


async def seed_funded_scope(
    session, ordinary_reserve, kwargs, old, account, batch, slot, *, source_snapshot=None
):
    identity = await seed_source(session, ordinary_reserve, kwargs, old, source_snapshot)
    # The bilateral tests exercise the real installer separately. Inject its
    # immutable source fixture before the first natural reservation here.
    await session.execute(
        text("""INSERT INTO single_sol_budget_scopes VALUES
       (:day,:run,:account,:batch,:model,:selected,:calendar,
        :slot,:created,:cap,:maximum)"""),
        dict(
            day=kwargs["now"].date(),
            run=identity,
            account=account,
            batch=batch,
            model=old.model_config_id,
            selected=old.instrument_id,
            calendar=CALENDAR_ID,
            slot=slot,
            created=slot - timedelta(minutes=15),
            cap=LIMIT,
            maximum=old.reserved_cost_usd,
        ),
    )


@pytest.mark.asyncio
async def test_natural_research_and_synthesis_convert_their_own_native_reservations(
    migrated_database_url, monkeypatch, tmp_path
):
    from integration import test_candidate_multistock_path as candidates
    from nexus.agents import persistence
    from nexus.paper import agent_cycle, single_sol_budget
    from nexus.persistence.models.agents import AgentDailyBudgetReservationRecord

    start = candidates.start_schwab_agent_paper_cycle
    ordinary_reserve = persistence.reserve_daily_budget
    installed = False

    async def scheduled(session, **kwargs):
        at = kwargs["decision_slot_at"]

        async def observed(_db):
            return at

        monkeypatch.setattr(single_sol_budget, "database_now", observed)
        ticket = await agent_cycle.reserve_schwab_agent_paper_trigger(
            session,
            account_id=kwargs["authority"].account_id,
            client_idempotency_key="single-budget-natural",
            now=at,
            candidate_batch_id=kwargs["candidate_batch_id"],
            formal_stage_capacity=True,
            formal_model_config_id=kwargs["formal_model_config_id"],
        )
        result = await start(session, **(kwargs | {"daily_limit_usd": LIMIT}))
        await agent_cycle.finish_schwab_agent_paper_trigger(
            session, reservation=ticket, result=result
        )
        return result

    async def funded(session, **kwargs):
        nonlocal installed
        if not installed:
            installed = True
            old = await session.get(AgentResearchRunRecord, kwargs["run_id"])
            account, batch, slot = (
                await session.execute(
                    text(
                        "select account_id,candidate_batch_id,decision_slot_at "
                        "from agent_paper_cycles"
                    )
                )
            ).one()
            await seed_funded_scope(session, ordinary_reserve, kwargs, old, account, batch, slot)
        return await ordinary_reserve(session, **kwargs)

    monkeypatch.setattr(candidates, "start_schwab_agent_paper_cycle", scheduled)
    monkeypatch.setattr(persistence, "reserve_daily_budget", funded)

    async def verify(factory, settings, cycle):
        async with factory() as db:
            credits = (
                await db.execute(
                    text("""SELECT c.task,c.run_id,b.reserved_usd,r.instrument_id,
                rt.scope_id FROM single_sol_budget_credits c JOIN agent_daily_budget_reservations b
                USING(run_id) JOIN agent_research_runs r ON r.id=c.run_id
                JOIN research_stage_route rt ON rt.run_id=c.run_id""")
                )
            ).all()
            assert len(credits) == 3 and len({item.run_id for item in credits}) == 3
            for item in credits:
                assert item.task == "research:" + str(item.instrument_id) and item.scope_id == cycle
            scope = (
                (await db.execute(text("select * from single_sol_budget_scopes"))).mappings().one()
            )
            assert (
                await _shared_daily_budget_used(db, business_date=scope["budget_day"])
                == 4 * scope["maximum"]
            )
            assert (
                await db.scalar(
                    select(AgentDailyBudgetReservationRecord.id).where(
                        AgentDailyBudgetReservationRecord.run_id == scope["source_run_id"]
                    )
                )
                is not None
            )

        # A real, unused source release stops covering its original obligation.
        # Roll this isolated alternative back before running the normal portfolio.
        from nexus.agents.persistence import release_daily_budget
        from nexus.paper.single_sol_budget import require_single_sol_capacity

        async with factory() as db:
            await release_daily_budget(db, run_id=credits[0].run_id)
            used = await _shared_daily_budget_used(db, business_date=scope["budget_day"])
            # Two unconverted obligations (portfolio and increment), plus the released stock.
            available = LIMIT - used - 3 * scope["maximum"]
            params = dict(
                kind="dispatch",
                run_id=scope["source_run_id"],
                now=scope["slot"],
                daily_limit_usd=LIMIT,
            )
            assert await require_single_sol_capacity(db, **params, amount=available)
            with pytest.raises(BudgetExhausted, match="SINGLE_SOL_NATURAL_BUDGET_PROTECTED"):
                await require_single_sol_capacity(
                    db, **params, amount=available + Decimal("0.000001")
                )
            await db.rollback()

    from integration import test_sol_native_stage_pg as stages

    original_prepare = native.test_formal_root_preserves_cycle_owner_and_freezes_native_sol_routes

    async def prepare(*args, after_prepared, **kwargs):
        async def run_checks(factory, settings, cycle):
            await verify(factory, settings, cycle)
            await after_prepared(
                factory, settings.model_copy(update={"agent_daily_budget_usd": LIMIT}), cycle
            )

        await original_prepare(*args, **kwargs, after_prepared=run_checks, natural_time=True)

    async def after_portfolio(factory, host, portfolio, job, at, patch, case, root, env_file):
        async with factory() as db:
            rows = (
                await db.execute(
                    text("SELECT task,run_id FROM single_sol_budget_credits ORDER BY task")
                )
            ).all()
            assert len(rows) == 4 and ("synthesis", portfolio.id) in rows
            assert len({r.run_id for r in rows}) == 4
            budget = (
                await db.execute(
                    text(
                        "SELECT b.business_date, b.reserved_usd, s.budget_day,s.maximum "
                        "FROM agent_portfolio_synthesis_budget_reservations b "
                        "JOIN single_sol_budget_scopes s ON s.budget_day=b.business_date "
                        "WHERE b.run_id=:run"
                    ),
                    {"run": portfolio.id},
                )
            ).one()
            assert budget.business_date == budget.budget_day
            assert budget.reserved_usd == budget.maximum

        from integration.test_sol_portfolio_responses_pg import _run_native_portfolio_http
        from nexus.paper.single_sol_obligations import funded_obligations

        async with factory() as db:
            guard = (
                (await db.execute(text("SELECT * FROM single_sol_budget_scopes"))).mappings().one()
            )
            members = await db.scalar(
                text(
                    "SELECT candidate_instrument_ids FROM agent_paper_candidate_batches "
                    "WHERE id=:batch"
                ),
                {"batch": guard["batch_id"]},
            )
            lookup = dict(
                now=guard["slot"] + timedelta(minutes=5),
                account=guard["account_id"],
                batch=guard["batch_id"],
                model=guard["model_id"],
                calendar=guard["calendar_id"],
                members=tuple(members),
                maximum=guard["maximum"],
            )
            with pytest.raises(ValueError, match="SINGLE_SOL_NATURAL_WINDOW_PROTECTED"):
                await funded_obligations(db, **lookup)
        await _run_native_portfolio_http(
            factory, host, portfolio, job, at, patch, "complete", root, env_file
        )
        async with factory() as db:
            credited = await funded_obligations(db, **lookup)
            assert set(credited) == {"research:" + str(m) for m in members} | {"synthesis"}
            assert credited["synthesis"] == portfolio.id and len(set(credited.values())) == 4
            with pytest.raises(ValueError, match="SINGLE_SOL_ORIGINAL_SETTLEMENT_UNPROVEN"):
                await funded_obligations(db, **(lookup | {"calendar": uuid4()}))

    monkeypatch.setattr(
        native, "test_formal_root_preserves_cycle_owner_and_freezes_native_sol_routes", prepare
    )
    await stages.test_normal_sol_formal_research_reconciles_monitor_plans_once(
        migrated_database_url, monkeypatch, tmp_path, after_admission=after_portfolio
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["release", "allocate"])
async def test_real_increment_converts_one_obligation_from_an_existing_plan(
    migrated_database_url, monkeypatch, first
):
    from nexus.agents import persistence
    from nexus.paper import single_sol_budget
    from nexus.paper.portfolio_policy import PORTFOLIO_ALLOCATION_V1

    original = persistence.reserve_daily_budget

    async def fund_before_increment(db, **kwargs):
        row = await db.get(AgentResearchRunRecord, kwargs["run_id"])
        if row.actor_id.startswith("monitor:"):
            account, batch = (
                await db.execute(
                    text("SELECT account_id,candidate_batch_id FROM agent_paper_cycles")
                )
            ).one()
            now = kwargs["now"]
            slot = datetime.combine(
                now.date(),
                PORTFOLIO_ALLOCATION_V1.decision_local_time,
                ZoneInfo(PORTFOLIO_ALLOCATION_V1.decision_timezone),
            ).astimezone(UTC)

            async def observed(_db):
                return now

            monkeypatch.setattr(single_sol_budget, "database_now", observed)
            parent_snapshot = await db.scalar(
                select(AgentResearchRunRecord.data_snapshot_id).where(
                    AgentResearchRunRecord.actor_id == "local-agent-paper-cycle",
                    AgentResearchRunRecord.instrument_id == row.instrument_id,
                )
            )
            assert parent_snapshot is not None and parent_snapshot != row.data_snapshot_id
            await seed_funded_scope(
                db, original, kwargs, row, account, batch, slot, source_snapshot=parent_snapshot
            )
        return await original(db, **kwargs)

    async def verify(factory, settings, member, trigger, parent):
        async with factory() as db:
            credits = (
                await db.execute(text("SELECT task,run_id FROM single_sol_budget_credits"))
            ).all()
            assert credits == [("increment", member.run_id)]
            assert member.trigger_id == trigger.id and member.run_id != parent.id

        # The new writer has a distinct native identity. Both transactions use the
        # public budget writers; wait for the PostgreSQL blocking edge, not a timer.
        async with factory.begin() as db:
            old = await db.get(AgentResearchRunRecord, member.run_id)
            now = member.research_as_of
            maximum, price = old.reserved_cost_usd, old.pricing_version
            initial = await _shared_daily_budget_used(db, business_date=now.date())
            remaining = LIMIT - initial - 4 * maximum
            assert remaining > 0
            new_id = uuid4()
            values = {c.name: getattr(old, c.name) for c in old.__table__.columns}
            db.add(
                AgentResearchRunRecord(
                    **(
                        values
                        | dict(
                            id=new_id,
                            actor_id="other-research-fixture",
                            data_snapshot_id=parent.data_snapshot_id,
                            idempotency_key=str(new_id),
                            input_hash=new_id.hex * 2,
                        )
                    )
                )
            )

        async def write(db, action, amount):
            if action == "release":
                await persistence.release_daily_budget(db, run_id=member.run_id)
            else:
                await original(
                    db,
                    run_id=new_id,
                    now=now,
                    daily_limit_usd=LIMIT,
                    worst_case_usd=amount,
                    pricing_version=price,
                    pricing_fresh=True,
                )

        extra = Decimal("0.000001")
        async with factory() as a, factory() as b, factory() as observer:
            async with a.begin():
                await write(a, first, remaining)
                holder = await a.scalar(text("SELECT pg_backend_pid()"))
                pid = asyncio.get_running_loop().create_future()

                async def other():
                    async with b.begin():
                        pid.set_result(await b.scalar(text("SELECT pg_backend_pid()")))
                        await write(
                            b, "allocate" if first == "release" else "release", remaining + extra
                        )

                pending = asyncio.create_task(other())
                try:
                    waiting = await pid
                    async with asyncio.timeout(20):
                        while holder not in await observer.scalar(
                            text("SELECT pg_blocking_pids(:pid)"), {"pid": waiting}
                        ):
                            if pending.done():
                                await pending
                                pytest.fail("writers did not serialize")
                            await asyncio.sleep(0.02)
                except BaseException:
                    pending.cancel()
                    await asyncio.gather(pending, return_exceptions=True)
                    raise
            if first == "release":
                with pytest.raises(BudgetExhausted, match="SINGLE_SOL_NATURAL_BUDGET_PROTECTED"):
                    await asyncio.wait_for(pending, 20)
                async with b.begin():
                    await write(b, "allocate", remaining)
            else:
                await asyncio.wait_for(pending, 20)
        async with factory() as db:
            assert (
                await db.scalar(
                    text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:id"),
                    {"id": member.run_id},
                )
                == "released"
            )
            assert (
                await db.scalar(
                    text("SELECT run_id FROM single_sol_budget_credits WHERE task='increment'")
                )
                == member.run_id
            )
            used = await _shared_daily_budget_used(db, business_date=now.date())
            assert used == initial - maximum + remaining
            assert used + 5 * maximum == LIMIT

    monkeypatch.setattr(persistence, "reserve_daily_budget", fund_before_increment)
    await native.test_normal_increment_selects_sol_and_retains_exact_sdk_predecessor(
        migrated_database_url, monkeypatch, after_admitted=verify, daily_limit=LIMIT
    )


@pytest.mark.asyncio
async def test_install_counts_a_previous_native_increment_once(migrated_database_url, monkeypatch):
    async def inspect(factory, settings, member, trigger, parent):
        async with factory.begin() as db:
            run = await db.get(AgentResearchRunRecord, member.run_id)
            now = member.research_as_of + timedelta(seconds=1)
            controlled_clock(db, monkeypatch, now)
            identity = await seed_source(
                db,
                reserve_daily_budget,
                dict(
                    now=now,
                    daily_limit_usd=LIMIT,
                    worst_case_usd=run.reserved_cost_usd,
                    pricing_version=run.pricing_version,
                    pricing_fresh=True,
                ),
                run,
                parent.data_snapshot_id,
            )
            await freeze_single_sol_budget(
                db,
                settings,
                run_id=identity,
                instrument_id=run.instrument_id,
                calendar_authority_id=CALENDAR_ID,
            )
        async with factory() as db:
            assert (
                await db.execute(text("SELECT task, run_id FROM single_sol_budget_credits"))
            ).all() == [("increment", member.run_id)]
            assert (
                await _shared_daily_budget_used(db, business_date=now.date())
                == 2 * run.reserved_cost_usd
            )

    await native.test_normal_increment_selects_sol_and_retains_exact_sdk_predecessor(
        migrated_database_url,
        monkeypatch,
        after_admitted=inspect,
        daily_limit=LIMIT,
        before_slot=True,
    )


@pytest.mark.asyncio
async def test_a_credit_requires_its_native_owner_and_the_original_budget_day(
    migrated_database_url, tmp_path, monkeypatch
):
    factory, settings, now, instrument, maximum, ids, calendar_id = await ledger_fixture(
        migrated_database_url, tmp_path, monkeypatch
    )
    try:
        async with factory.begin() as db:
            controlled_clock(db, monkeypatch, now)
            source = await db.get(AgentResearchRunRecord, ids[0])
            inputs = dict(
                daily_limit_usd=LIMIT, pricing_version=source.pricing_version, pricing_fresh=True
            )
            await reserve_daily_budget(
                db, run_id=source.id, now=now, worst_case_usd=maximum, **inputs
            )
            await freeze_single_sol_budget(
                db,
                settings,
                run_id=source.id,
                instrument_id=instrument,
                calendar_authority_id=calendar_id,
            )
        with pytest.raises(ValueError, match="SINGLE_SOL_BUDGET_STALE_SNAPSHOT"):
            async with factory.begin() as db:
                await db.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"))
                controlled_clock(db, monkeypatch, now)
                await reserve_daily_budget(
                    db, run_id=ids[1], now=now, worst_case_usd=OTHER, **inputs
                )
        for task in ("increment", "synthesis", f"research:{instrument}"):
            with pytest.raises(DBAPIError, match="SINGLE_SOL_BUDGET_CREDIT_UNPROVEN"):
                async with factory.begin() as db:
                    await db.execute(
                        text("INSERT INTO single_sol_budget_credits VALUES (:d,:t,:r)"),
                        {"d": now.date(), "t": task, "r": ids[0]},
                    )
        from pathlib import Path

        from alembic import command
        from alembic.config import Config

        monkeypatch.setenv("DATABASE_URL", migrated_database_url)
        with pytest.raises(DBAPIError, match="SINGLE_SOL_BUDGET_DOWNGRADE_FORBIDDEN"):
            await asyncio.to_thread(
                command.downgrade,
                Config(str(Path(__file__).resolve().parents[2] / "alembic.ini")),
                "0114_formal_intraday_intent",
            )
        for requested, seen in ((now, now + timedelta(days=1)), (now - timedelta(days=1), now)):
            with pytest.raises(ValueError, match="SINGLE_SOL_BUDGET_WINDOW_CLOSED"):
                async with factory.begin() as db:
                    controlled_clock(db, monkeypatch, seen)
                    await reserve_daily_budget(
                        db, run_id=ids[1], now=requested, worst_case_usd=OTHER, **inputs
                    )
        async with factory() as db:
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_credits")) == 0
            assert await _shared_daily_budget_used(db, business_date=now.date()) == maximum
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_old_reservation_cannot_authorize_a_new_scope(
    migrated_database_url, tmp_path, monkeypatch
):
    factory, settings, now, instrument, maximum, (run_id, _), calendar_id = await ledger_fixture(
        migrated_database_url, tmp_path, monkeypatch
    )
    try:
        async with factory.begin() as db:
            controlled_clock(db, monkeypatch, now)
            source = await db.get(AgentResearchRunRecord, run_id)
            await reserve_daily_budget(
                db,
                run_id=run_id,
                now=now,
                daily_limit_usd=LIMIT,
                worst_case_usd=maximum,
                pricing_version=source.pricing_version,
                pricing_fresh=True,
            )
        with pytest.raises(ValueError, match="SINGLE_SOL_BUDGET_SCOPE_INVALID"):
            async with factory.begin() as db:
                controlled_clock(db, monkeypatch, now)
                await freeze_single_sol_budget(
                    db,
                    settings,
                    run_id=run_id,
                    instrument_id=instrument,
                    calendar_authority_id=calendar_id,
                )
        async with factory() as db:
            assert not await has_guard(db, now.date())
            assert await _shared_daily_budget_used(db, business_date=now.date()) == maximum
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_install_single_protection_after_exact_natural_close(
    migrated_database_url, monkeypatch, tmp_path
):
    from integration import test_candidate_multistock_path as candidates
    from integration import test_sol_native_stage_pg as stages
    from integration.test_sol_portfolio_responses_pg import _run_native_portfolio_http
    from nexus.agents import persistence
    from nexus.paper import agent_cycle, single_sol_budget
    from nexus.persistence.models.backtest import ExchangeCalendarAuthorityRecord
    from sqlalchemy import event

    def cover_next_session(_mapper, _connection, calendar):
        if calendar.id == CALENDAR_ID:
            calendar.range_end += timedelta(days=7)

    event.listen(ExchangeCalendarAuthorityRecord, "before_insert", cover_next_session, once=True)
    start = candidates.start_schwab_agent_paper_cycle
    normal_prepare = native.test_formal_root_preserves_cycle_owner_and_freezes_native_sol_routes
    normal_reserve = persistence.reserve_daily_budget
    template = {}

    async def scheduled(db, **kwargs):
        slot = kwargs["decision_slot_at"]
        # The formal-cycle fixture contains only the prior and current exchange sessions.
        # The service-calendar reader also needs the next session from that same authority.
        from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord

        next_day = slot.date() + timedelta(days=1)
        while next_day.weekday() >= 5:
            next_day += timedelta(days=1)
        next_open = datetime.combine(
            next_day, time(9, 30), ZoneInfo("America/New_York")
        ).astimezone(UTC)
        next_close = next_open + timedelta(hours=6, minutes=30)
        db.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=next_day,
                market_open_at=next_open,
                market_close_at=next_close,
                decision_at=next_close,
            )
        )
        await db.flush()
        ticket = await agent_cycle.reserve_schwab_agent_paper_trigger(
            db,
            account_id=kwargs["authority"].account_id,
            client_idempotency_key="post-slot",
            now=slot,
            candidate_batch_id=kwargs["candidate_batch_id"],
            formal_stage_capacity=True,
            formal_model_config_id=kwargs["formal_model_config_id"],
        )
        result = await start(db, **(kwargs | {"daily_limit_usd": LIMIT}))
        await agent_cycle.finish_schwab_agent_paper_trigger(db, reservation=ticket, result=result)
        return result

    async def reserve(db, **kwargs):
        if not template:
            old = await db.get(AgentResearchRunRecord, kwargs["run_id"])
            template.update({c.name: getattr(old, c.name) for c in old.__table__.columns})
        return await normal_reserve(db, **kwargs)

    async def prepare(*args, **kwargs):
        await normal_prepare(*args, **kwargs, natural_time=True)

    async def after(factory, host, portfolio, job, at, patch, case, root, env_file):
        await _run_native_portfolio_http(
            factory, host, portfolio, job, at, patch, "complete", root, env_file
        )
        async with factory.begin() as db:
            slot = await db.scalar(
                text("select decision_slot_at from agent_paper_cycles where id=:id"),
                {"id": portfolio.cycle_id},
            )
            now = slot + timedelta(minutes=20)

            async def observed(_session):
                return now

            monkeypatch.setattr(single_sol_budget, "database_now", observed)
            run = uuid4()
            source = AgentResearchRunRecord(
                **(
                    template
                    | dict(
                        id=run,
                        actor_id="local-single-sol-research-v1",
                        idempotency_key=str(run),
                        created_at=now,
                        decision_time=now,
                        input_hash=run.hex * 2,
                    )
                )
            )
            db.add(source)
            await db.flush()
            await normal_reserve(
                db,
                run_id=run,
                now=now,
                daily_limit_usd=LIMIT,
                worst_case_usd=source.reserved_cost_usd,
                pricing_version=source.pricing_version,
                pricing_fresh=True,
            )
            assert not await has_guard(db, now.date())
            used = await _shared_daily_budget_used(db, business_date=now.date())
            insufficient = used + source.reserved_cost_usd - Decimal("0.000001")
            with pytest.raises(BudgetExhausted, match="SINGLE_SOL_NATURAL_BUDGET_PROTECTED"):
                async with db.begin_nested():
                    await freeze_single_sol_budget(
                        db,
                        host.model_copy(
                            update={
                                "agent_daily_budget_usd": insufficient,
                                "agent_paper_candidate_batch_id": portfolio.candidate_batch_id,
                            }
                        ),
                        run_id=run,
                        instrument_id=source.instrument_id,
                        calendar_authority_id=CALENDAR_ID,
                    )
            assert not await has_guard(db, now.date())
            await freeze_single_sol_budget(
                db,
                host.model_copy(
                    update={
                        "agent_daily_budget_usd": LIMIT,
                        "agent_paper_candidate_batch_id": portfolio.candidate_batch_id,
                    }
                ),
                run_id=run,
                instrument_id=source.instrument_id,
                calendar_authority_id=CALENDAR_ID,
            )
        async with factory() as db:
            assert (
                await db.scalar(text("select source_run_id from single_sol_budget_scopes")) == run
            )
            credits = (
                await db.execute(text("select task,run_id from single_sol_budget_credits"))
            ).all()
            assert len(credits) == 4 and ("synthesis", portfolio.id) in credits
            assert run not in {item.run_id for item in credits}
            assert (
                await db.scalar(
                    text("""SELECT count(*) FROM pg_constraint
                WHERE conrelid='single_sol_budget_scopes'::regclass AND contype='c'
                AND pg_get_constraintdef(oid) LIKE '%maximum <= daily_limit%'""")
                )
                == 1
            )

    monkeypatch.setattr(candidates, "start_schwab_agent_paper_cycle", scheduled)
    monkeypatch.setattr(persistence, "reserve_daily_budget", reserve)
    monkeypatch.setattr(
        native, "test_formal_root_preserves_cycle_owner_and_freezes_native_sol_routes", prepare
    )
    try:
        await stages.test_normal_sol_formal_research_reconciles_monitor_plans_once(
            migrated_database_url, monkeypatch, tmp_path, after_admission=after
        )
    finally:
        event.remove(ExchangeCalendarAuthorityRecord, "before_insert", cover_next_session)
