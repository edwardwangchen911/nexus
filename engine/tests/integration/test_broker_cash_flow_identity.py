import asyncio
from dataclasses import replace
from uuid import UUID

import pytest
from helpers.broker_cashflow import _stable_cash_flow_result
from helpers.broker_storage import (
    ACCOUNT_A,
    ACCOUNT_B,
    PORTFOLIO_A,
    PORTFOLIO_B,
    _insert_run,
    _seed_accounts,
)
from nexus.persistence.models.portfolio import BrokerCashFlow, BrokerSyncRun
from nexus.persistence.session import session_factory
from nexus.reconciliation.cash_flow_persistence import persist_cash_flows
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.exc import DBAPIError

RUN_A = UUID("0198d400-6000-7000-8000-000000000001")
RUN_B = UUID("0198d400-6000-7000-8000-000000000002")


@pytest.mark.asyncio
async def test_date_only_flow_persists_idempotently_and_detects_business_date_revision(
    migrated_database_url: str,
) -> None:
    from datetime import date

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_accounts(factory)
    result = _stable_cash_flow_result()
    fact = replace(result.facts[0], occurred_at=None, kind="fosun:100")
    result = replace(result, facts=(fact,))
    async with factory() as session, session.begin():
        await _insert_run(session, RUN_A, ACCOUNT_A, PORTFOLIO_A, "ACTIVE", "a")
        run = await session.get(BrokerSyncRun, RUN_A)
        assert await persist_cash_flows(session, run, "fosun", result) == "PASS"
    async with factory() as session, session.begin():
        run = await session.get(BrokerSyncRun, RUN_A)
        assert await persist_cash_flows(session, run, "fosun", result) == "PASS"
        changed = replace(result, facts=(replace(fact, provider_business_date=date(2026, 8, 22)),))
        assert await persist_cash_flows(session, run, "fosun", changed) == "PARTIAL"
    async with factory() as session:
        rows = (await session.scalars(select(BrokerCashFlow))).all()
        assert len(rows) == 1
        assert rows[0].occurred_at is None
        assert rows[0].provider_business_date == date(2026, 8, 23)


@pytest.mark.asyncio
async def test_cross_account_cash_flow_identity_serializes_to_conflict(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await _seed_accounts(factory)
    async with factory() as session, session.begin():
        await _insert_run(session, RUN_A, ACCOUNT_A, PORTFOLIO_A, "ACTIVE", "a")
        await _insert_run(session, RUN_B, ACCOUNT_B, PORTFOLIO_B, "ACTIVE", "b")
    result = _stable_cash_flow_result(provider_id="synthetic-cross-account-flow")
    entered = 0
    both_entered = asyncio.Event()

    async def persist(run_id: UUID) -> str:
        nonlocal entered
        async with factory() as session, session.begin():
            run = await session.get(BrokerSyncRun, run_id)
            assert run is not None
            entered += 1
            if entered == 2:
                both_entered.set()
            return await persist_cash_flows(session, run, "fosun", result)

    async with factory() as blocker, blocker.begin():
        await blocker.execute(text("LOCK TABLE broker_cash_flows IN SHARE MODE"))
        tasks = (asyncio.create_task(persist(RUN_A)), asyncio.create_task(persist(RUN_B)))
        await asyncio.wait_for(both_entered.wait(), timeout=2)
        await asyncio.sleep(0.05)
    qualities = await asyncio.wait_for(asyncio.gather(*tasks), timeout=5)

    async with factory() as session:
        count = await session.scalar(select(func.count()).select_from(BrokerCashFlow))
        assert count == 1
        with pytest.raises(DBAPIError, match="immutable"):
            await session.execute(text("UPDATE broker_cash_flows SET amount = 99"))
            await session.commit()
    assert sorted(qualities) == ["PARTIAL", "PASS"]
