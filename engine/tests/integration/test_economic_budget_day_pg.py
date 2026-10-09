"""New budget writes cannot use a UTC day observed before a lock or qualification wait."""

from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from helpers.paper_api import settings
from nexus.agents import persistence
from nexus.agents.persistence import _shared_daily_budget_used, reserve_daily_budget
from nexus.agents.portfolio_synthesis_persistence import reserve_portfolio_synthesis_budget
from nexus.paper import economic_budget_hold
from nexus.paper.economic_budget_hold import AllocationRequest, hold_pair_budget
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentPortfolioSynthesisBudgetReservationRecord,
)
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.session import session_factory
from sqlalchemy import func, select, text
from test_economic_budget_hold_pg import request, seed_synthesis_receiver
from test_portfolio_decision_budget_pg import _seed_runs


def controlled_clock(monkeypatch, session, now):
    """Replace only the DB clock result; all locks, reads and writes remain PostgreSQL."""
    current = [now]
    scalar = session.scalar

    async def read(statement, *args, **kwargs):
        if any(
            getattr(column, "name", None) == "clock_timestamp"
            for column in getattr(statement, "selected_columns", ())
        ):
            return current[0]
        return await scalar(statement, *args, **kwargs)

    monkeypatch.setattr(session, "scalar", read)
    return current


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["lock", "write", "audit"])
async def test_new_hold_rejects_crossed_day_without_audit_or_budget(
    migrated_database_url, monkeypatch, boundary
):
    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        actual = await session.scalar(text("SELECT current_database()"))
        now = await session.scalar(select(func.clock_timestamp()))
        clock = controlled_clock(monkeypatch, session, now)
        lock = persistence._lock_budget_day
        usage = persistence._shared_daily_budget_used
        append = economic_budget_hold.append_audit_event

        async def crossed_lock(*args, **kwargs):
            await lock(*args, **kwargs)
            if boundary == "lock":
                clock[0] = now + timedelta(days=1)

        async def crossed_usage(*args, **kwargs):
            result = await usage(*args, **kwargs)
            if boundary == "write":
                clock[0] = now + timedelta(days=1)
            return result

        async def crossed_audit(*args, **kwargs):
            result = await append(*args, **kwargs)
            if boundary == "audit":
                clock[0] = now + timedelta(days=1)
            return result

        monkeypatch.setattr(persistence, "_lock_budget_day", crossed_lock)
        monkeypatch.setattr(persistence, "_shared_daily_budget_used", crossed_usage)
        monkeypatch.setattr(economic_budget_hold, "append_audit_event", crossed_audit)
        with pytest.raises(ValueError, match="ECONOMIC_HOLD_DAY_MISMATCH"):
            await hold_pair_budget(session, request(), expected_database=actual)
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 0
        assert await _shared_daily_budget_used(session, business_date=now.date()) == 0
        assert await _shared_daily_budget_used(session, business_date=clock[0].date()) == 0
    async with factory.begin() as session:
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == 0
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["research", "synthesis"])
@pytest.mark.parametrize("boundary", ["lock", "write", "audit", "usage"])
async def test_native_transfer_rejects_crossed_day_without_child_or_credit(
    migrated_database_url, monkeypatch, kind, boundary
):
    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        actual = await session.scalar(text("SELECT current_database()"))
        hold = await hold_pair_budget(
            session, request(uncovered_formal_usd="0"), expected_database=actual
        )
    if kind == "synthesis":
        run_id = await seed_synthesis_receiver(factory)
    else:
        run_id = uuid4()
        await _seed_runs(factory, research_run_ids=(run_id,), decision_run_ids=())
    reserve = reserve_portfolio_synthesis_budget if kind == "synthesis" else reserve_daily_budget
    async with factory.begin() as session:
        before = await session.scalar(select(func.count()).select_from(AuditEvent))
        clock = controlled_clock(monkeypatch, session, hold.occurred_at)
        lock = persistence._lock_budget_day
        append = economic_budget_hold.append_audit_event
        usage = persistence._shared_daily_budget_used

        async def crossed_lock(*args, **kwargs):
            await lock(*args, **kwargs)
            if boundary == "lock":
                clock[0] = hold.occurred_at + timedelta(days=1)

        async def qualified_receiver(*args, **kwargs):
            # Authority contracts have separate real-reader coverage. This test isolates
            # day validation while retaining the normal native budget and audit path.
            if boundary == "write":
                clock[0] = hold.occurred_at + timedelta(days=1)

        async def crossed_audit(*args, **kwargs):
            result = await append(*args, **kwargs)
            if boundary == "audit":
                clock[0] = hold.occurred_at + timedelta(days=1)
            return result

        async def crossed_usage(*args, **kwargs):
            result = await usage(*args, **kwargs)
            if boundary == "usage":
                clock[0] = hold.occurred_at + timedelta(days=1)
            return result

        monkeypatch.setattr(persistence, "_lock_budget_day", crossed_lock)
        monkeypatch.setattr(
            economic_budget_hold, "_require_new_transfer_authority", qualified_receiver
        )
        monkeypatch.setattr(economic_budget_hold, "append_audit_event", crossed_audit)
        monkeypatch.setattr(persistence, "_shared_daily_budget_used", crossed_usage)
        with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_DAY_MISMATCH"):
            await reserve(
                session,
                run_id=run_id,
                now=hold.occurred_at,
                daily_limit_usd=Decimal(10),
                worst_case_usd=Decimal(1),
                pricing_version="pricing-v1",
                pricing_fresh=True,
                economic_allocation=AllocationRequest(
                    hold_event_id=hold.id,
                    lane="incumbent",
                    action="synthesis" if kind == "synthesis" else "research_1",
                ),
            )
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == before
        for model in (
            AgentDailyBudgetReservationRecord,
            AgentPortfolioSynthesisBudgetReservationRecord,
        ):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
        assert await _shared_daily_budget_used(session, business_date=hold.occurred_at.date()) == 6
        assert await _shared_daily_budget_used(session, business_date=clock[0].date()) == 0
    async with factory.begin() as session:
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == before
        for model in (
            AgentDailyBudgetReservationRecord,
            AgentPortfolioSynthesisBudgetReservationRecord,
        ):
            assert await session.scalar(select(func.count()).select_from(model)) == 0
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["hold", "research", "synthesis"])
async def test_exact_existing_receipt_stays_read_only_when_day_changes_after_lock(
    migrated_database_url, monkeypatch, kind
):
    factory = session_factory(settings(migrated_database_url))
    async with factory.begin() as session:
        actual = await session.scalar(text("SELECT current_database()"))
        hold_request = request(uncovered_formal_usd="0")
        hold = await hold_pair_budget(session, hold_request, expected_database=actual)
    reserve = reserve_portfolio_synthesis_budget if kind == "synthesis" else reserve_daily_budget
    if kind != "hold":
        if kind == "synthesis":
            run_id = await seed_synthesis_receiver(factory)
        else:
            run_id = uuid4()
            await _seed_runs(factory, research_run_ids=(run_id,), decision_run_ids=())

        async def qualified(*args, **kwargs):
            return None

        monkeypatch.setattr(economic_budget_hold, "_require_new_transfer_authority", qualified)
        kwargs = dict(
            run_id=run_id,
            now=hold.occurred_at,
            daily_limit_usd=Decimal(10),
            worst_case_usd=Decimal(1),
            pricing_version="pricing-v1",
            pricing_fresh=True,
            economic_allocation=AllocationRequest(
                hold_event_id=hold.id,
                lane="incumbent",
                action="synthesis" if kind == "synthesis" else "research_1",
            ),
        )
        async with factory.begin() as session:
            original = await reserve(session, **kwargs)
    else:
        original = hold

    async def no_new_write(*args, **kwargs):
        raise AssertionError("existing receipt must not requalify or append")

    monkeypatch.setattr(economic_budget_hold, "_require_new_transfer_authority", no_new_write)
    monkeypatch.setattr(economic_budget_hold, "append_audit_event", no_new_write)
    async with factory.begin() as session:
        before = await session.scalar(select(func.count()).select_from(AuditEvent))
        clock = controlled_clock(monkeypatch, session, hold.occurred_at)
        lock = persistence._lock_budget_day

        async def crossed_lock(*args, **kwargs):
            await lock(*args, **kwargs)
            clock[0] = hold.occurred_at + timedelta(days=1)

        monkeypatch.setattr(persistence, "_lock_budget_day", crossed_lock)
        if kind == "hold":
            repeated = await hold_pair_budget(session, hold_request, expected_database=actual)
        else:
            repeated = await reserve(session, **kwargs)
        assert repeated.id == original.id
        assert await session.scalar(select(func.count()).select_from(AuditEvent)) == before
        assert await _shared_daily_budget_used(session, business_date=hold.occurred_at.date()) == 6
        assert await _shared_daily_budget_used(session, business_date=clock[0].date()) == 0
    await factory.kw["bind"].dispose()
