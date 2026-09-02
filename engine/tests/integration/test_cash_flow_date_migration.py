"""Date precision migration is reversible only without inventing missing times."""

import asyncio
from dataclasses import replace
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from helpers.broker_cashflow import _stable_cash_flow_result
from helpers.broker_storage import ACCOUNT_A, PORTFOLIO_A, _insert_run, _seed_accounts
from nexus.persistence.models.portfolio import BrokerSyncRun
from nexus.persistence.session import session_factory
from nexus.reconciliation.cash_flow_persistence import persist_cash_flows
from nexus.settings import EngineSettings
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine


def test_cash_flow_date_migration_roundtrip(temporary_database_url, monkeypatch):
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    command.upgrade(config, "head")
    command.downgrade(config, "0032_ticker_alias_authority")
    command.upgrade(config, "head")

    async def inspect():
        engine = create_async_engine(temporary_database_url)
        try:
            async with engine.connect() as connection:
                assert (
                    await connection.scalar(
                        text(
                            "SELECT is_nullable FROM information_schema.columns "
                            "WHERE table_name='broker_cash_flows' AND column_name='occurred_at'"
                        )
                    )
                    == "YES"
                )
                assert (
                    await connection.scalar(
                        text(
                            "SELECT count(*) FROM pg_constraint WHERE "
                            "conname='ck_broker_cash_flows_date_only_identity_stable'"
                        )
                    )
                    == 1
                )
        finally:
            await engine.dispose()

    asyncio.run(inspect())


def test_downgrade_refuses_to_erase_or_invent_date_only_times(migrated_database_url):
    async def seed():
        factory = session_factory(EngineSettings(database_url=migrated_database_url))
        try:
            await _seed_accounts(factory)
            async with factory() as session, session.begin():
                run_id = uuid4()
                await _insert_run(session, run_id, ACCOUNT_A, PORTFOLIO_A, "ACTIVE", "d")
                run = await session.get(BrokerSyncRun, run_id)
                result = _stable_cash_flow_result()
                result = replace(result, facts=(replace(result.facts[0], occurred_at=None),))
                assert await persist_cash_flows(session, run, "fosun", result) == "PASS"
        finally:
            await factory.kw["bind"].dispose()

    asyncio.run(seed())
    config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
    with pytest.raises(DBAPIError) as error:
        command.downgrade(config, "0032_ticker_alias_authority")
    assert error.value.orig.sqlstate == "23502"

    async def preserved():
        engine = create_async_engine(migrated_database_url)
        try:
            async with engine.connect() as connection:
                assert (
                    await connection.scalar(
                        text("SELECT count(*) FROM broker_cash_flows WHERE occurred_at IS NULL")
                    )
                    == 1
                )
                assert (
                    await connection.scalar(text("SELECT version_num FROM alembic_version"))
                    == "0033_cash_flow_business_date"
                )
        finally:
            await engine.dispose()

    asyncio.run(preserved())
