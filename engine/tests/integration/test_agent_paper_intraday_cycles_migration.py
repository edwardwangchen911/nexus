"""Intraday Agent paper-cycle cardinality and rollback contracts."""

import asyncio
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.paper import AgentPaperCycleRecord, StrategyDecisionRecord
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

ENGINE_ROOT = Path(__file__).parents[2]
MIGRATION_HEAD = "0059_agent_paper_intraday_cycles"


def test_0059_precedes_the_single_forward_head() -> None:
    """Catch an omitted or incorrectly chained intraday-cycle migration."""
    scripts = ScriptDirectory.from_config(Config(str(ENGINE_ROOT / "alembic.ini")))

    assert scripts.get_heads() == [CURRENT_MIGRATION_HEAD]
    revision = scripts.get_revision(MIGRATION_HEAD)
    assert revision is not None
    assert revision.down_revision == "0058_agent_paper_schwab_realtime"


def test_orm_exposes_intraday_cycle_and_decision_bindings() -> None:
    """Catch an ORM contract that still models one Agent decision per session."""
    cycle_columns = AgentPaperCycleRecord.__table__.columns
    decision_columns = StrategyDecisionRecord.__table__.columns

    assert cycle_columns["decision_slot_at"].nullable is False
    assert decision_columns["agent_paper_cycle_id"].nullable is True


def test_0059_offline_sql_replaces_daily_uniqueness_without_rewriting_history() -> None:
    """Catch missing cardinality replacement, deterministic backfill, or downgrade fence."""
    output = StringIO()
    config = Config(str(ENGINE_ROOT / "alembic.ini"), output_buffer=output)

    command.upgrade(
        config,
        "0058_agent_paper_schwab_realtime:0059_agent_paper_intraday_cycles",
        sql=True,
    )

    sql = output.getvalue()
    assert "decision_slot_at" in sql
    assert "date_trunc('hour', decision_time)" in sql
    assert "uq_agent_paper_cycles_account_slot" in sql
    assert "agent_paper_cycle_id" in sql
    assert "uq_strategy_decisions_registered_account_session" in sql
    assert "uq_strategy_decisions_agent_cycle" in sql
    assert "uq_risk_query_authorities_scope_available" in sql

    rollback_output = StringIO()
    rollback_config = Config(str(ENGINE_ROOT / "alembic.ini"), output_buffer=rollback_output)
    command.downgrade(
        rollback_config,
        "0059_agent_paper_intraday_cycles:0058_agent_paper_schwab_realtime",
        sql=True,
    )
    assert "AGENT_PAPER_INTRADAY_FACTS_EXIST" in rollback_output.getvalue()


async def _schema_contract(database_url: str) -> dict[str, object]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            columns = {
                (str(row.table_name), str(row.column_name)): str(row.is_nullable)
                for row in await connection.execute(
                    text(
                        "SELECT table_name,column_name,is_nullable "
                        "FROM information_schema.columns WHERE table_schema='public' "
                        "AND ((table_name='agent_paper_cycles' AND column_name='decision_slot_at') "
                        "OR (table_name='strategy_decisions' "
                        "AND column_name='agent_paper_cycle_id'))"
                    )
                )
            }
            indexes = {
                str(row.indexname)
                for row in await connection.execute(
                    text(
                        "SELECT indexname FROM pg_indexes WHERE schemaname='public' "
                        "AND indexname IN ('uq_agent_paper_cycles_account_slot',"
                        "'uq_strategy_decisions_registered_account_session',"
                        "'uq_strategy_decisions_agent_cycle')"
                    )
                )
            }
            constraints = {
                str(row.conname)
                for row in await connection.execute(
                    text(
                        "SELECT conname FROM pg_constraint WHERE conname IN "
                        "('fk_strategy_decisions_agent_paper_cycle',"
                        "'ck_strategy_decisions_agent_cycle_binding',"
                        "'uq_risk_query_authorities_scope_available')"
                    )
                )
            }
            return {"columns": columns, "indexes": indexes, "constraints": constraints}
    finally:
        await engine.dispose()


def test_0059_empty_database_round_trip_preserves_0058(
    monkeypatch: pytest.MonkeyPatch, temporary_database_url: str
) -> None:
    """Catch invalid PostgreSQL DDL or an irreversible empty-schema upgrade."""
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(ENGINE_ROOT / "alembic.ini"))
    command.upgrade(config, "0058_agent_paper_schwab_realtime")

    command.upgrade(config, MIGRATION_HEAD)
    upgraded = asyncio.run(_schema_contract(temporary_database_url))
    assert upgraded == {
        "columns": {
            ("agent_paper_cycles", "decision_slot_at"): "NO",
            ("strategy_decisions", "agent_paper_cycle_id"): "YES",
        },
        "indexes": {
            "uq_agent_paper_cycles_account_slot",
            "uq_strategy_decisions_registered_account_session",
            "uq_strategy_decisions_agent_cycle",
        },
        "constraints": {
            "fk_strategy_decisions_agent_paper_cycle",
            "ck_strategy_decisions_agent_cycle_binding",
            "uq_risk_query_authorities_scope_available",
        },
    }

    command.downgrade(config, "0058_agent_paper_schwab_realtime")
    downgraded = asyncio.run(_schema_contract(temporary_database_url))
    assert downgraded == {"columns": {}, "indexes": set(), "constraints": set()}


async def _seed_0058_agent_facts(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SET LOCAL session_replication_role=replica"))
            await connection.execute(
                text(
                    "INSERT INTO agent_paper_cycles "
                    "(id,account_id,portfolio_id,decision_session,decision_time,"
                    "execution_session,universe_snapshot_id,quality_report_id,"
                    "decision_price_policy_id,execution_price_policy_id,candidate_manifest,"
                    "candidate_hash,sizing_policy_version,sizing_policy_hash,execution_locked) "
                    "VALUES ('59000000-0000-7000-8000-000000000001',"
                    "'59000000-0000-7000-8000-000000000002',"
                    "'59000000-0000-7000-8000-000000000003','2026-09-14',"
                    "'2026-09-14T14:01:30Z','2026-09-14',"
                    "'59000000-0000-7000-8000-000000000004',NULL,"
                    "'4d24df4a-b510-541b-8a6a-fe7db5afaac4',"
                    "'cf525f07-4e55-5f84-a839-60d845819ec5','[]',"
                    "repeat('a',64),'agent_paper_sizing_v1',repeat('b',64),true)"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO strategy_decisions "
                    "(id,account_id,portfolio_id,strategy_id,strategy_version,"
                    "strategy_definition_id,registry_event_id,champion_generation,decision_origin,"
                    "decision_session,decision_time,execution_session,source_policy_id,"
                    "execution_source_policy_id,calendar_authority_id,universe_snapshot_id,"
                    "quality_report_id,portfolio_projection_hash,authority_hash,"
                    "strategy_parameters_hash,decision_hash,cash_weight,reason) VALUES "
                    "('59000000-0000-7000-8000-000000000005',"
                    "'59000000-0000-7000-8000-000000000002',"
                    "'59000000-0000-7000-8000-000000000003','agent_research',"
                    "'agent_paper_decision_v1',NULL,NULL,NULL,'agent_research','2026-09-14',"
                    "'2026-09-14T14:01:30Z','2026-09-14',"
                    "'4d24df4a-b510-541b-8a6a-fe7db5afaac4',"
                    "'cf525f07-4e55-5f84-a839-60d845819ec5',"
                    "'59000000-0000-7000-8000-000000000006',"
                    "'59000000-0000-7000-8000-000000000004',NULL,repeat('c',64),"
                    "repeat('d',64),repeat('e',64),repeat('f',64),1,'AGENT_RESEARCH')"
                )
            )
    finally:
        await engine.dispose()


async def _intraday_backfill(database_url: str) -> tuple[str, str]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            row = (
                await connection.execute(
                    text(
                        "SELECT cycle.decision_slot_at::text AS slot,"
                        "decision.agent_paper_cycle_id::text AS cycle_id "
                        "FROM agent_paper_cycles cycle JOIN strategy_decisions decision "
                        "ON decision.agent_paper_cycle_id=cycle.id"
                    )
                )
            ).mappings().one()
            return str(row["slot"]), str(row["cycle_id"])
    finally:
        await engine.dispose()


async def _insert_second_cycle(database_url: str, *, slot: datetime, cycle_id: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SET LOCAL session_replication_role=replica"))
            await connection.execute(
                text(
                    "INSERT INTO agent_paper_cycles "
                    "(id,account_id,portfolio_id,decision_session,decision_time,decision_slot_at,"
                    "execution_session,universe_snapshot_id,quality_report_id,"
                    "decision_price_policy_id,execution_price_policy_id,candidate_manifest,"
                    "candidate_hash,sizing_policy_version,sizing_policy_hash,execution_locked) "
                    "VALUES (:cycle_id,'59000000-0000-7000-8000-000000000002',"
                    "'59000000-0000-7000-8000-000000000003','2026-09-14',"
                    "(:slot)::timestamptz + interval '30 seconds',(:slot)::timestamptz,"
                    "'2026-09-14','59000000-0000-7000-8000-000000000004',NULL,"
                    "'4d24df4a-b510-541b-8a6a-fe7db5afaac4',"
                    "'cf525f07-4e55-5f84-a839-60d845819ec5','[]',"
                    "repeat('1',64),'agent_paper_sizing_v1',repeat('2',64),true)"
                ),
                {"cycle_id": cycle_id, "slot": slot},
            )
    finally:
        await engine.dispose()


def test_0059_backfills_existing_facts_allows_next_slot_and_fences_downgrade(
    monkeypatch: pytest.MonkeyPatch, temporary_database_url: str
) -> None:
    """Catch history loss, lingering daily uniqueness, or an unsafe downgrade."""
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(ENGINE_ROOT / "alembic.ini"))
    command.upgrade(config, "0058_agent_paper_schwab_realtime")
    asyncio.run(_seed_0058_agent_facts(temporary_database_url))

    command.upgrade(config, MIGRATION_HEAD)
    slot, cycle_id = asyncio.run(_intraday_backfill(temporary_database_url))
    assert slot == "2026-09-14 14:00:00+00"
    assert cycle_id == "59000000-0000-7000-8000-000000000001"

    asyncio.run(
        _insert_second_cycle(
            temporary_database_url,
            slot=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
            cycle_id="59000000-0000-7000-8000-000000000007",
        )
    )
    with pytest.raises(IntegrityError):
        asyncio.run(
            _insert_second_cycle(
                temporary_database_url,
                slot=datetime(2026, 9, 14, 14, 5, tzinfo=UTC),
                cycle_id="59000000-0000-7000-8000-000000000008",
            )
        )

    with pytest.raises(DBAPIError, match="AGENT_PAPER_INTRADAY_FACTS_EXIST"):
        command.downgrade(config, "0058_agent_paper_schwab_realtime")
