"""A new presentation profile must retain existing same-day obligation protection."""

import asyncio
import hashlib
import threading
from datetime import datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from integration import test_single_sol_budget_pg as budget
from integration.test_compact_sol_lifecycle_pg import compact_registration  # noqa: F401
from integration.test_sol_profile_pg import SOURCE, current_record
from nexus.agents.platform_profile import register_sol_business_profile
from nexus.agents.platform_schema import (
    COMPACT_PLATFORM_UNITS_VERSION,
    CURRENT_PLATFORM_UNITS_VERSION,
)
from nexus.paper.single_sol_compat import budget_model_compatible
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import event, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import DBAPIError


@pytest.fixture
def legacy_day_guard(monkeypatch, request):
    request.getfixturevalue("compact_registration")
    seed = budget.seed_funded_scope
    installed = []

    async def legacy_guard(session, ordinary_reserve, kwargs, old, account, batch, slot, **options):
        selected = await session.get(AgentModelConfigRecord, old.model_config_id)
        legacy = await register_sol_business_profile(
            session,
            price_record=selected.platform_profile["price_record"],
            source_evidence=SOURCE,
            units_version=CURRENT_PLATFORM_UNITS_VERSION,
        )
        assert legacy.id != selected.id
        assert legacy.worst_case_cost_usd == selected.worst_case_cost_usd
        base = {column.name: getattr(old, column.name) for column in old.__table__.columns}
        frozen = AgentResearchRunRecord(
            **(
                base
                | dict(
                    model_config_id=legacy.id,
                    model_hash=legacy.config_hash,
                    pricing_version=legacy.version,
                )
            )
        )
        await seed(
            session,
            ordinary_reserve,
            kwargs | dict(pricing_version=legacy.version),
            frozen,
            account,
            batch,
            slot,
            **options,
        )
        facts = await session.scalar(
            text(
                "SELECT jsonb_build_object('guard',to_jsonb(g),'source',to_jsonb(r)) "
                "FROM single_sol_budget_scopes g JOIN agent_research_runs r "
                "ON r.id=g.source_run_id"
            )
        )
        installed.append((legacy.id, selected.id, facts))

    monkeypatch.setattr(budget, "seed_funded_scope", legacy_guard)
    return installed


@pytest.mark.asyncio
@pytest.mark.parametrize("downgrade_race", [False, True])
async def test_compact_natural_cycle_consumes_existing_legacy_day_protection(
    migrated_database_url, monkeypatch, tmp_path, legacy_day_guard, downgrade_race
):
    from nexus.paper import single_sol_budget

    convert = single_sol_budget._convert
    fenced = []

    async def concurrent_downgrade(*args, **kwargs):
        await convert(*args, **kwargs)
        if not fenced:
            with pytest.raises(DBAPIError, match="could not obtain lock"):
                await asyncio.to_thread(
                    command.downgrade, Config("engine/alembic.ini"), "0118_compact_sol_profile"
                )
            fenced.append(True)

    if downgrade_race:
        monkeypatch.setattr(single_sol_budget, "_convert", concurrent_downgrade)
    # Reuse all existing normal-path assertions: real reservation and deferred SQL
    # credits, three completed stocks, portfolio settlement and funded readback.
    await budget.test_natural_research_and_synthesis_convert_their_own_native_reservations(
        migrated_database_url, monkeypatch, tmp_path, "local-budgeted-single-sol-v1"
    )
    assert len(legacy_day_guard) == 1
    assert fenced == ([True] if downgrade_race else [])
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory() as db:
            assert (
                await db.scalar(
                    text(
                        "SELECT jsonb_build_object('guard',to_jsonb(g),'source',to_jsonb(r)) "
                        "FROM single_sol_budget_scopes g JOIN agent_research_runs r "
                        "ON r.id=g.source_run_id"
                    )
                )
                == legacy_day_guard[0][2]
            )
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_credits")) == 4
        # An already committed mixed credit cannot be reinterpreted by 0118.
        with pytest.raises(DBAPIError, match="COMPACT_BUDGET_DOWNGRADE_FORBIDDEN"):
            await asyncio.to_thread(
                command.downgrade, Config("engine/alembic.ini"), "0118_compact_sol_profile"
            )
        async with factory() as db:
            assert await db.scalar(text("SELECT version_num FROM alembic_version")) == (
                "0119_compact_budget_obligations"
            )
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_credits")) == 4
        if not downgrade_race:
            from helpers.compact_budget_maintenance_image import verify_images

            await verify_images(factory, migrated_database_url)
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("first", ["release", "allocate"])
async def test_compact_increment_covers_legacy_obligation_under_release_race(
    migrated_database_url, monkeypatch, legacy_day_guard, first
):
    await budget.test_real_increment_converts_one_obligation_from_an_existing_plan(
        migrated_database_url, monkeypatch, first, "local-budgeted-single-sol-v1"
    )
    assert len(legacy_day_guard) == 1


@pytest.mark.asyncio
async def test_compact_increment_worker_settles_and_replaces_under_legacy_guard(
    migrated_database_url, monkeypatch, legacy_day_guard
):
    from integration import test_sol_native_preparation_pg as native
    from integration import test_sol_native_stage_pg as stages

    prepare = native.test_normal_increment_selects_sol_and_retains_exact_sdk_predecessor

    async def guarded_prepare(url, patch, *, after_admitted):
        # Keep the existing exact-parent Worker/finish/next-admission assertions;
        # add only the same real budget setup exercised by the release race cases.
        patch.setattr(
            native, "test_normal_increment_selects_sol_and_retains_exact_sdk_predecessor", prepare
        )
        await budget.test_real_increment_converts_one_obligation_from_an_existing_plan(
            url, patch, "release", "local-budgeted-single-sol-v1", after_admission=after_admitted
        )

    monkeypatch.setattr(
        native,
        "test_normal_increment_selects_sol_and_retains_exact_sdk_predecessor",
        guarded_prepare,
    )
    await stages.test_native_increment_worker_settles_only_child_and_retains_parent(
        migrated_database_url, monkeypatch
    )
    assert len(legacy_day_guard) == 1


@pytest.mark.asyncio
async def test_downgrade_first_prevents_a_waiting_compact_credit_from_committing(
    migrated_database_url, monkeypatch, tmp_path, legacy_day_guard
):
    from integration import test_candidate_multistock_path as candidates
    from nexus.paper import single_sol_budget

    convert, start = single_sol_budget._convert, candidates.start_schwab_agent_paper_cycle
    locked, release = threading.Event(), threading.Event()
    rejected, results = [], []

    def barrier(_conn, _cursor, statement, _parameters, _context, _executemany):
        if (
            statement.startswith("DO $$ BEGIN")
            and "COMPACT_BUDGET_DOWNGRADE_FORBIDDEN" in statement
        ):
            locked.set()
            if not release.wait(15):
                raise RuntimeError("isolated downgrade barrier expired")

    async def blocked_credit(*args, **kwargs):
        task = asyncio.create_task(
            asyncio.to_thread(
                command.downgrade, Config("engine/alembic.ini"), "0118_compact_sol_profile"
            )
        )
        try:
            assert await asyncio.to_thread(locked.wait, 10)
            try:
                await convert(*args, **kwargs)
            except DBAPIError as error:
                assert getattr(error.orig, "sqlstate", None) == "55P03"
                rejected.append(True)
                raise
            pytest.fail("credit committed through a downgrade's write fence")
        finally:
            release.set()
            await asyncio.wait_for(task, 15)

    async def observe(*args, **kwargs):
        result = await start(*args, **kwargs)
        results.append(result.status)
        return result

    event.listen(Engine, "before_cursor_execute", barrier)
    monkeypatch.setattr(single_sol_budget, "_convert", blocked_credit)
    monkeypatch.setattr(candidates, "start_schwab_agent_paper_cycle", observe)
    try:
        # The original DB lock error escapes preparation and rolls back. Do not
        # relabel this as the budget-refusal result or change the success fixture.
        with pytest.raises(DBAPIError, match="lock timeout"):
            await budget.test_natural_research_and_synthesis_convert_their_own_native_reservations(
                migrated_database_url, monkeypatch, tmp_path, "local-budgeted-single-sol-v1"
            )
        assert rejected == [True] and results == []
        factory = session_factory(EngineSettings(database_url=migrated_database_url))
        try:
            async with factory() as db:
                assert await db.scalar(text("SELECT version_num FROM alembic_version")) == (
                    "0118_compact_sol_profile"
                )
                assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_credits")) == 0
                assert (
                    await db.scalar(text("SELECT count(*) FROM agent_daily_budget_reservations"))
                    == 0
                )
        finally:
            await factory.kw["bind"].dispose()
    finally:
        release.set()
        event.remove(Engine, "before_cursor_execute", barrier)


@pytest.mark.asyncio
@pytest.mark.parametrize("difference", ["none", "price", "expiry", "source", "reverse", "amount"])
async def test_obligation_relation_rejects_every_economic_difference(
    migrated_database_url, difference
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        price = await current_record(factory)
        changed = dict(price)
        source = SOURCE
        if difference == "price":
            changed["ordinary_input_usd_per_million"] = str(
                Decimal(price["ordinary_input_usd_per_million"]) + Decimal("0.01")
            )
        elif difference == "expiry":
            changed["expires_at"] = (
                datetime.fromisoformat(price["expires_at"]) + timedelta(hours=1)
            ).isoformat()
        elif difference == "source":
            source = SOURCE + b" revised official evidence"
            changed["source_evidence_sha256"] = hashlib.sha256(source).hexdigest()
        async with factory.begin() as db:
            old = await register_sol_business_profile(
                db, price_record=price, source_evidence=SOURCE
            )
            new = await register_sol_business_profile(
                db,
                price_record=changed,
                source_evidence=source,
                units_version=COMPACT_PLATFORM_UNITS_VERSION,
            )
            expected, candidate = (new.id, old.id) if difference == "reverse" else (old.id, new.id)
            maximum = old.worst_case_cost_usd + (
                Decimal("0.000001") if difference == "amount" else 0
            )
            assert await budget_model_compatible(
                db, expected=expected, candidate=candidate, maximum=maximum
            ) is (difference == "none")
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_credits")) == 0
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("selection", ["exact", "compact", "price", "frozen"])
async def test_read_only_activation_checks_existing_day_guard(
    migrated_database_url, monkeypatch, tmp_path, selection
):
    from nexus.paper import single_sol_compat

    factory, settings, now, instrument, maximum, ids, calendar = await budget.ledger_fixture(
        migrated_database_url, tmp_path, monkeypatch
    )
    try:
        async with factory.begin() as db:
            budget.controlled_clock(db, monkeypatch, now)
            run = await db.get(AgentResearchRunRecord, ids[0])
            await budget.reserve_daily_budget(
                db,
                run_id=run.id,
                now=now,
                daily_limit_usd=budget.LIMIT,
                worst_case_usd=maximum,
                pricing_version=run.pricing_version,
                pricing_fresh=True,
            )
            await budget.freeze_single_sol_budget(
                db,
                settings,
                run_id=run.id,
                instrument_id=instrument,
                calendar_authority_id=calendar,
            )
            if selection != "exact":
                old = await db.get(AgentModelConfigRecord, run.model_config_id)
                price = dict(old.platform_profile["price_record"])
                if selection == "price":
                    price["output_usd_per_million"] = str(
                        Decimal(price["output_usd_per_million"]) + Decimal("0.01")
                    )
                model = await register_sol_business_profile(
                    db,
                    price_record=price,
                    source_evidence=SOURCE,
                    units_version=COMPACT_PLATFORM_UNITS_VERSION,
                )
                settings = settings.model_copy(
                    update={"agent_paper_formal_model_config_id": model.id}
                )
                if selection == "frozen":
                    from nexus.persistence.models.manual_paper import FormalIntradayIntent

                    db.add(
                        FormalIntradayIntent(
                            id=uuid4(),
                            account_id=settings.agent_paper_account_id,
                            batch_id=settings.agent_paper_candidate_batch_id,
                            model_id=old.id,
                            key_hash="a" * 64,
                            scope_hash="b" * 64,
                            calendar_id=calendar,
                            session_date=now.date(),
                            registered_at=now,
                            expires_at=now + timedelta(hours=1),
                            state="pending",
                            code="FORMAL_INTENT_REGISTERED",
                        )
                    )
        async with factory.begin() as db:
            await db.execute(text("SET TRANSACTION READ ONLY"))
            if selection in {"price", "frozen"}:
                code = (
                    "SINGLE_SOL_FROZEN_MODEL_CONFLICT"
                    if selection == "frozen"
                    else "SINGLE_SOL_SERVICE_SCOPE_MISMATCH"
                )
                with pytest.raises(ValueError, match=code):
                    await single_sol_compat.inspect_service_budget_scope(db, settings, now=now)
            else:
                result = await single_sol_compat.inspect_service_budget_scope(db, settings, now=now)
                assert result == {"status": selection, "remaining_obligations": 5}
                if selection == "compact":
                    # Natural compatibility never upgrades an explicit v5 context.
                    from nexus.paper.budgeted_single_sol import protection_context_hash

                    budget.controlled_clock(db, monkeypatch, now)
                    with pytest.raises(ValueError, match="SINGLE_SOL_BUDGET_SCOPE_INVALID"):
                        await protection_context_hash(
                            db, settings, day=now.date(), calendar_id=calendar
                        )
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_credits")) == 0
        from nexus.ops.agent_paper_recovery import inspect_recovery_readiness
        from sqlalchemy.ext.asyncio import AsyncSession

        scalar = AsyncSession.scalar

        async def clock(self, statement, *args, **kwargs):
            if str(statement) == "SELECT clock_timestamp() AS clock_timestamp_1":
                return now
            return await scalar(self, statement, *args, **kwargs)

        monkeypatch.setattr(AsyncSession, "scalar", clock)
        readiness = await inspect_recovery_readiness(
            settings.model_copy(
                update={
                    "agent_paper_cycle_enabled": True,
                    "release_revision": "a" * 40,
                }
            )
        )
        if selection in {"price", "frozen"}:
            assert readiness["blocking_codes"] == [code]
            assert readiness["failed_stage"] == "daily_budget_scope"
        else:
            assert readiness["budget_scope"] == result
            assert readiness["status"] == "blocked"
            assert readiness["blocking_codes"] == ["MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"]
            assert readiness["failed_stage"] == "account_occupancy"
        assert readiness["external_requests"] == 0 and readiness["execution_locked"] is True
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_idle_completed_source_allows_full_compact_recovery(
    migrated_database_url, tmp_path, monkeypatch, capsys
):
    from integration.test_budgeted_single_sol_pg import completed_predecessor
    from nexus.ops.agent_paper_recovery import inspect_recovery_readiness
    from nexus.paper.manual_host_exit import record_host_exit
    from nexus.persistence.models.runtime import WorkerHeartbeat
    from sqlalchemy import select

    settings, _, _ = await completed_predecessor(
        migrated_database_url, tmp_path, monkeypatch, capsys
    )
    factory = session_factory(settings)
    try:
        async with factory.begin() as db:
            # The synchronous fixture worker has returned and drained; record its
            # exact host retirement through the existing product writer.
            hosts = (
                await db.scalars(
                    select(WorkerHeartbeat).where(WorkerHeartbeat.queue == "agent_paper")
                )
            ).all()
            for host in hosts:
                await record_host_exit(
                    db,
                    worker_id=host.worker_id,
                    started_at=host.started_at,
                    process_version=host.process_version,
                )
            old = await db.get(AgentModelConfigRecord, settings.agent_paper_formal_model_config_id)
            model = await register_sol_business_profile(
                db,
                price_record=old.platform_profile["price_record"],
                source_evidence=SOURCE,
                units_version=COMPACT_PLATFORM_UNITS_VERSION,
            )
            assert old.id != model.id
            facts = text("""SELECT jsonb_build_object(
              'guard',(SELECT jsonb_agg(to_jsonb(g)) FROM single_sol_budget_scopes g),
              'requests',(SELECT jsonb_agg(to_jsonb(r) ORDER BY id) FROM manual_paper_requests r),
              'reserves',(SELECT jsonb_agg(to_jsonb(b) ORDER BY run_id)
                          FROM agent_daily_budget_reservations b),
              'credits',(SELECT jsonb_agg(to_jsonb(c)) FROM single_sol_budget_credits c))""")
            before = await db.scalar(facts)
        ready = await inspect_recovery_readiness(
            settings.model_copy(
                update={
                    "agent_paper_formal_model_config_id": model.id,
                    "agent_paper_cycle_enabled": True,
                    "release_revision": "a" * 40,
                }
            )
        )
        assert ready["status"] == "ready", ready
        assert ready["blocking_codes"] == []
        assert ready["budget_scope"] == {"status": "compact", "remaining_obligations": 5}
        assert ready["external_requests"] == 0 and ready["execution_locked"] is True
        async with factory() as db:
            # Full recovery is a database read-only transaction, not a new owner.
            assert await db.scalar(facts) == before
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_four_known_settlements_and_one_reserve_are_covered_not_fulfilled(monkeypatch):
    """Controlled read results exercise the real remaining-obligation classifier."""
    from types import SimpleNamespace

    from nexus.paper import single_sol_budget, single_sol_compat

    members = [uuid4() for _ in range(3)]
    account, batch, model = uuid4(), uuid4(), uuid4()
    rows = [
        SimpleNamespace(task=task, run_id=uuid4(), state="settled")
        for task in [*("research:" + str(i) for i in members), "synthesis"]
    ]
    rows.append(SimpleNamespace(task="increment", run_id=uuid4(), state="reserved"))
    closed_ids = {row.run_id for row in rows if row.state == "settled"}

    async def closed(session, ids, *, now):
        assert ids == closed_ids
        return closed_ids

    monkeypatch.setattr(single_sol_budget, "closed_settlements", closed)

    class ReadSession:
        async def scalar(self, statement, params):
            assert "formal_intraday_intents" in str(statement)
            return None

        async def execute(self, statement, params):
            if "FROM single_sol_budget_scopes" in str(statement):
                guard = dict(
                    account_id=account, batch_id=batch, daily_limit=Decimal("13"), members=members
                )
                return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: guard))
            assert "FROM single_sol_budget_credits" in str(statement)
            return SimpleNamespace(all=lambda: rows)

    settings = EngineSettings(
        agent_paper_account_id=account,
        agent_paper_candidate_batch_id=batch,
        agent_paper_formal_model_config_id=model,
        agent_daily_budget_usd=Decimal("13"),
    )
    assert await single_sol_compat.inspect_service_budget_scope(
        ReadSession(), settings, now=datetime.fromisoformat("2026-11-05T14:00:00+00:00")
    ) == {"status": "covered", "remaining_obligations": 0}
    assert rows[-1].state == "reserved"
