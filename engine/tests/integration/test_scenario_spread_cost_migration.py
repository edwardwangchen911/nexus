"""Migration boundary for purpose-specific scenario spread cost evidence."""

from __future__ import annotations

import asyncio
from uuid import UUID, uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

POLICY_ID = UUID("7733d99e-1594-5f1e-87ea-e5cc23575159")
POLICY_PAYLOAD = {
    "purpose": "portfolio_scenario_cost_analysis",
    "selection": "latest_qualified_before_valuation_cutoff",
    "fallback": "disabled",
    "session_scope": "xnys_regular",
    "max_quote_age_seconds": 45,
    "calculation": "sell_half_spread_from_snapshot_mark_v1",
    "calendar_authority": {
        "library": "exchange_calendars",
        "library_version": "4.13.2",
        "calendar": "XNYS",
        "session_window_contract": "xnys_regular_open_closed_v1",
    },
    "require_verified_evidence": True,
    "require_empty_quality_flags": True,
    "alpaca": {
        "feed": "iex",
        "delivery_mode": "real_time",
        "transport": "websocket",
        "artifact_schema": "alpaca_stock_stream_v1",
        "market_scope": "US",
        "event_kind": "quote",
    },
}


def test_0050_is_single_migration_head() -> None:
    config = Config("engine/alembic.ini")
    scripts = ScriptDirectory.from_config(config)
    revision = scripts.get_revision("0050_scenario_spread_cost_policy")

    assert revision is not None
    assert revision.down_revision == "0049_pd_terminal_phases"
    assert scripts.get_heads() == [CURRENT_MIGRATION_HEAD]


def test_0050_publishes_exact_policy(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = Config("engine/alembic.ini")
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(config, "0050_scenario_spread_cost_policy")
    assert asyncio.run(_policy_row(temporary_database_url)) == {
        "id": POLICY_ID,
        "key": "portfolio_scenario_cost_analysis",
        "version": "scenario_spread_cost_policy_v1",
        "schema_version": "1",
        "payload": POLICY_PAYLOAD,
    }


def test_0050_is_data_only_and_does_not_add_tables(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = Config("engine/alembic.ini")
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(config, "0049_pd_terminal_phases")
    before = asyncio.run(_table_names(temporary_database_url))

    command.upgrade(config, "0050_scenario_spread_cost_policy")

    assert asyncio.run(_table_names(temporary_database_url)) == before


def test_0050_clean_downgrade_removes_only_exact_policy(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = Config("engine/alembic.ini")
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(config, "0050_scenario_spread_cost_policy")
    unrelated_policy_id = asyncio.run(_insert_unrelated_policy(temporary_database_url))

    command.downgrade(config, "0049_pd_terminal_phases")

    assert asyncio.run(_policy_row(temporary_database_url)) is None
    assert asyncio.run(_policy_exists(temporary_database_url, unrelated_policy_id)) is True


def test_0050_downgrade_refuses_referenced_v3_manifest(
    temporary_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = Config("engine/alembic.ini")
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(config, "0050_scenario_spread_cost_policy")
    asyncio.run(_insert_v3_claim(temporary_database_url))

    with pytest.raises(RuntimeError, match="scenario spread cost policy is referenced"):
        command.downgrade(config, "0049_pd_terminal_phases")

    assert asyncio.run(_policy_row(temporary_database_url)) is not None


async def _policy_row(database_url: str) -> dict[str, object] | None:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    text(
                        "SELECT id,key,version,schema_version,payload "
                        "FROM source_policies WHERE id=:policy_id"
                    ),
                    {"policy_id": POLICY_ID},
                )
            ).mappings().one_or_none()
            return None if row is None else dict(row)
    finally:
        await engine.dispose()


async def _table_names(database_url: str) -> set[str]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return set(
                await connection.scalars(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema='public'"
                    )
                )
            )
    finally:
        await engine.dispose()


async def _insert_unrelated_policy(database_url: str) -> UUID:
    policy_id = uuid4()
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO source_policies (id,key,version,schema_version,payload) "
                    "VALUES (:id,:key,'unrelated_v1','1','{}'::jsonb)"
                ),
                {"id": policy_id, "key": f"unrelated-{policy_id}"},
            )
    finally:
        await engine.dispose()
    return policy_id


async def _policy_exists(database_url: str, policy_id: UUID) -> bool:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return bool(
                await connection.scalar(
                    text("SELECT EXISTS (SELECT 1 FROM source_policies WHERE id=:id)"),
                    {"id": policy_id},
                )
            )
    finally:
        await engine.dispose()


async def _insert_v3_claim(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO idempotency_records ("
                    "id,actor_id,route,idempotency_key,request_hash,schema_version,response_payload"
                    ") VALUES ("
                    ":id,'migration-test',"
                    "'/internal/v1/portfolio-scenarios/evaluate:portfolio_scenario_evaluation_v3',"
                    "'scenario-spread-cost-migration',:request_hash,'portfolio_scenario_v3',"
                    "CAST(:response_payload AS jsonb))"
                ),
                {
                    "id": uuid4(),
                    "request_hash": "a" * 64,
                    "response_payload": (
                        '{"manifest_schema":"scenario_spread_evidence_manifest_v1",'
                        f'"source_policy_id":"{POLICY_ID}"}}'
                    ),
                },
            )
    finally:
        await engine.dispose()
