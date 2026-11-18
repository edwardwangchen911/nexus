"""The original scheduled three-stock owner funds each natural obligation once."""

from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from helpers.durable_test_clock import DisposableClock
from helpers.paper_batch import CALENDAR_ID
from integration import test_candidate_multistock_path as candidates
from integration import test_sol_native_preparation_pg as native
from nexus.agents.persistence import _shared_daily_budget_used
from nexus.monitoring.auto_increment_policy import enable_scope
from nexus.paper import agent_cycle
from nexus.paper.routine_budget import remaining, scope
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_normal_scheduled_cycle_initializes_protection_and_funds_three_tasks(
    migrated_database_url, monkeypatch
):
    from nexus.paper import research_admission_diagnostics

    errors = []
    facts = research_admission_diagnostics.admission_failure_facts

    def diagnose(error, **kwargs):
        errors.append((type(error).__name__, str(error), repr(error.__cause__)))
        return facts(error, **kwargs)

    monkeypatch.setattr(research_admission_diagnostics, "admission_failure_facts", diagnose)
    original = candidates.start_schwab_agent_paper_cycle
    clock = DisposableClock(migrated_database_url)
    controlled_url = DisposableClock.uncached_url(migrated_database_url)
    from nexus.agents import platform_profile

    normal_register = platform_profile.register_sol_business_profile
    normal_scalar = AsyncSession.scalar

    async def register(db, **kwargs):
        if clock.logical is None:
            # Use one database clock for both Python observations and SQL triggers;
            # the legacy candidate fixture otherwise substitutes only scalar reads.
            monkeypatch.setattr(AsyncSession, "scalar", normal_scalar)
            await db.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
            await db.execute(text("SET CONSTRAINTS ALL DEFERRED"))
            await clock.set(db, candidates.AT, running=True, refresh_defaults=True)
        return await normal_register(db, **kwargs)

    monkeypatch.setattr(platform_profile, "register_sol_business_profile", register)

    async def scheduled(db, **kwargs):
        at = kwargs["decision_slot_at"]
        assert clock.now().date() == at.date()
        from nexus.settings import EngineSettings

        info = EngineSettings(database_url=controlled_url).model_copy(
            update={
                "agent_paper_account_id": kwargs["authority"].account_id,
                "agent_paper_candidate_batch_id": kwargs["candidate_batch_id"],
                "agent_paper_formal_model_config_id": kwargs["formal_model_config_id"],
                "agent_daily_budget_usd": Decimal(13),
            }
        )
        db.info["engine_settings"] = info
        # The older cycle fixture has only yesterday/today. A running service
        # needs the next session too; append it without rewriting that authority.
        ny = ZoneInfo("America/New_York")
        next_day = at.astimezone(ny).date() + timedelta(days=1)
        while next_day.weekday() >= 5:
            next_day += timedelta(days=1)
        db.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=CALENDAR_ID,
                session=next_day,
                decision_at=datetime.combine(next_day, datetime.min.time(), tzinfo=ny).replace(
                    hour=16
                ),
                market_open_at=datetime.combine(next_day, datetime.min.time(), tzinfo=ny).replace(
                    hour=9, minute=30
                ),
                market_close_at=datetime.combine(next_day, datetime.min.time(), tzinfo=ny).replace(
                    hour=16
                ),
            )
        )
        await db.flush()
        await enable_scope(
            db,
            account_id=info.agent_paper_account_id,
            candidate_batch_id=info.agent_paper_candidate_batch_id,
        )
        ticket = await agent_cycle.reserve_schwab_agent_paper_trigger(
            db,
            account_id=info.agent_paper_account_id,
            client_idempotency_key="routine-natural-test",
            now=at,
            candidate_batch_id=info.agent_paper_candidate_batch_id,
            formal_stage_capacity=True,
            formal_model_config_id=info.agent_paper_formal_model_config_id,
        )
        result = await original(db, **(kwargs | {"daily_limit_usd": Decimal(13)}))
        await agent_cycle.finish_schwab_agent_paper_trigger(db, reservation=ticket, result=result)
        if result.status != "researching":
            rows = (
                await db.execute(
                    text(
                        "SELECT result_code,fact_ids FROM agent_paper_cycle_events "
                        "WHERE cycle_id=:cycle"
                    ),
                    {"cycle": result.cycle_id},
                )
            ).all()
            raise AssertionError(repr((rows, errors)))
        return result

    monkeypatch.setattr(candidates, "start_schwab_agent_paper_cycle", scheduled)

    async def verify(factory, settings, cycle):
        async with factory.begin() as db:
            g = await scope(db, clock.now().date())
            assert g is not None
            tasks, recorded = await remaining(db, g, now=clock.now())
            assert tasks == {"synthesis", "increment"}
            assert len(recorded) == 3 and all(t.startswith("research:") for t in recorded)
            assert (
                await _shared_daily_budget_used(db, business_date=g["budget_day"])
                == 3 * g["maximum"]
            )
            assert (
                await db.scalar(text("SELECT count(DISTINCT run_id) FROM routine_budget_credits"))
                == 3
            )
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_scopes")) == 0

    await native.test_formal_root_preserves_cycle_owner_and_freezes_native_sol_routes(
        controlled_url, monkeypatch, after_prepared=verify, natural_time=True
    )
