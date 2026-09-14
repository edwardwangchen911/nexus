"""Forward-only Schwab Agent paper policy and schema admission."""

import asyncio
from io import StringIO
from pathlib import Path
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models import paper as paper_models
from nexus.quality.source_policy import (
    paper_decision_price_authorized,
    paper_execution_price_authorized,
)
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

ENGINE_ROOT = Path(__file__).parents[2]
SCHWAB_DECISION_POLICY_ID = UUID("4d24df4a-b510-541b-8a6a-fe7db5afaac4")
SCHWAB_EXECUTION_POLICY_ID = UUID("cf525f07-4e55-5f84-a839-60d845819ec5")


def test_0058_precedes_the_intraday_cycle_head() -> None:
    """Catch an omitted or incorrectly chained Schwab admission migration."""
    scripts = ScriptDirectory.from_config(Config(str(ENGINE_ROOT / "alembic.ini")))

    assert CURRENT_MIGRATION_HEAD == "0059_agent_paper_intraday_cycles"
    assert scripts.get_heads() == [CURRENT_MIGRATION_HEAD]
    revision = scripts.get_revision(CURRENT_MIGRATION_HEAD)
    assert revision is not None
    assert revision.down_revision == "0057_agent_paper_market_window"


def test_schwab_policy_ids_are_authorized_only_for_their_exact_purposes() -> None:
    """Catch a missing policy or a broad decision/execution authorization."""
    assert getattr(paper_models, "PAPER_SCHWAB_DECISION_SOURCE_POLICY_ID", None) == (
        SCHWAB_DECISION_POLICY_ID
    )
    assert getattr(paper_models, "PAPER_SCHWAB_EXECUTION_SOURCE_POLICY_ID", None) == (
        SCHWAB_EXECUTION_POLICY_ID
    )
    assert paper_decision_price_authorized(SCHWAB_DECISION_POLICY_ID)
    assert not paper_execution_price_authorized(SCHWAB_DECISION_POLICY_ID)
    assert paper_execution_price_authorized(SCHWAB_EXECUTION_POLICY_ID)
    assert not paper_decision_price_authorized(SCHWAB_EXECUTION_POLICY_ID)


def test_0058_offline_sql_installs_narrow_schwab_contracts() -> None:
    """Catch policy seeding or constraints that omit or overstate Schwab provenance."""
    output = StringIO()
    config = Config(str(ENGINE_ROOT / "alembic.ini"), output_buffer=output)

    command.upgrade(
        config,
        "0057_agent_paper_market_window:0058_agent_paper_schwab_realtime",
        sql=True,
    )

    sql = output.getvalue()
    assert "paper_schwab_l1_decision_v1" in sql
    assert "paper_schwab_price_history_execution_v1" in sql
    assert "execution_session = decision_session" in sql
    assert "execution_session > decision_session" in sql
    assert "source='schwab' AND feed='price_history' AND delivery_mode='unknown'" in sql
    assert "schwab_price_history_raw_v1" in sql
    assert "nexus_prepare_agent_paper_schwab_price_bar_set" in sql
    assert (
        "ELSIF NEW.source_policy_id='4d24df4a-b510-541b-8a6a-fe7db5afaac4'::uuid"
        in sql
    )

    rollback_output = StringIO()
    rollback_config = Config(str(ENGINE_ROOT / "alembic.ini"), output_buffer=rollback_output)
    command.downgrade(
        rollback_config,
        "0058_agent_paper_schwab_realtime:0057_agent_paper_market_window",
        sql=True,
    )
    rollback_sql = rollback_output.getvalue()
    assert "SCHWAB_AGENT_PAPER_FACTS_EXIST" in rollback_sql
    assert "FROM public.strategy_decisions" in rollback_sql


def test_orm_metadata_matches_the_two_disjoint_policy_paths() -> None:
    """Catch application inserts that disagree with the migrated database checks."""
    cycle_checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in paper_models.AgentPaperCycleRecord.__table__.constraints
        if hasattr(constraint, "sqltext")
    }
    window_checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in paper_models.AgentPaperMarketWindowRecord.__table__.constraints
        if hasattr(constraint, "sqltext")
    }

    policy_contract = cycle_checks["ck_agent_paper_cycles_policies_fixed"]
    assert str(SCHWAB_DECISION_POLICY_ID) in policy_contract
    assert str(SCHWAB_EXECUTION_POLICY_ID) in policy_contract
    assert "execution_session = decision_session" in policy_contract
    assert "execution_session > decision_session" in policy_contract
    assert cycle_checks["ck_agent_paper_cycles_sessions_ordered"] == (
        "execution_session >= decision_session"
    )
    assert "source='schwab' AND feed='price_history' AND delivery_mode='unknown'" in (
        window_checks["ck_agent_paper_market_windows_source_fixed"]
    )

    decision_checks = {
        constraint.name: str(constraint.sqltext)
        for constraint in paper_models.StrategyDecisionRecord.__table__.constraints
        if hasattr(constraint, "sqltext")
    }
    decision_origin = decision_checks["ck_strategy_decisions_origin_authority"]
    assert str(SCHWAB_DECISION_POLICY_ID) in decision_origin
    assert str(SCHWAB_EXECUTION_POLICY_ID) in decision_origin
    assert "execution_session = decision_session" in decision_origin
    assert decision_origin.count("execution_session > decision_session") == 2
    assert decision_checks["ck_strategy_decisions_sessions_ordered"] == (
        "execution_session >= decision_session"
    )


async def _database_contract(database_url: str) -> dict[str, object]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            policies = tuple(
                (
                    await connection.execute(
                        text(
                            "SELECT id::text,key,version,payload->>'content_hash' AS content_hash "
                            "FROM source_policies WHERE id IN "
                            "('4d24df4a-b510-541b-8a6a-fe7db5afaac4'::uuid,"
                            "'cf525f07-4e55-5f84-a839-60d845819ec5'::uuid) ORDER BY id"
                        )
                    )
                ).mappings()
            )
            constraints = {
                str(row.conname): str(row.definition)
                for row in await connection.execute(
                    text(
                        "SELECT con.conname,pg_get_constraintdef(con.oid) AS definition "
                        "FROM pg_constraint con JOIN pg_class rel ON rel.oid=con.conrelid "
                        "WHERE rel.relname IN "
                        "('agent_paper_cycles','strategy_decisions','agent_paper_market_windows') "
                        "AND con.contype='c'"
                    )
                )
            }
            return {"policies": policies, "constraints": constraints}
    finally:
        await engine.dispose()


def test_0058_empty_database_round_trip_preserves_0057(
    monkeypatch: pytest.MonkeyPatch, temporary_database_url: str
) -> None:
    """Catch invalid PostgreSQL DDL or an irreversible empty-schema upgrade."""
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(ENGINE_ROOT / "alembic.ini"))
    command.upgrade(config, "0057_agent_paper_market_window")

    command.upgrade(config, "0058_agent_paper_schwab_realtime")
    upgraded = asyncio.run(_database_contract(temporary_database_url))
    assert [row["version"] for row in upgraded["policies"]] == [
        "paper_schwab_l1_decision_v1",
        "paper_schwab_price_history_execution_v1",
    ]
    assert all(len(str(row["content_hash"])) == 64 for row in upgraded["policies"])
    constraints = upgraded["constraints"]
    assert "execution_session >= decision_session" in constraints[
        "ck_agent_paper_cycles_sessions_ordered"
    ]
    assert "price_history" in constraints["ck_agent_paper_market_windows_source_fixed"]

    command.downgrade(config, "0057_agent_paper_market_window")
    downgraded = asyncio.run(_database_contract(temporary_database_url))
    assert downgraded["policies"] == ()
    assert "execution_session > decision_session" in downgraded["constraints"][
        "ck_agent_paper_cycles_sessions_ordered"
    ]
    assert "price_history" not in downgraded["constraints"][
        "ck_agent_paper_market_windows_source_fixed"
    ]


async def _seed_schwab_cycle_without_foreign_roots(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SET LOCAL session_replication_role = replica"))
            await connection.execute(
                text(
                    "INSERT INTO agent_paper_cycles "
                    "(id,account_id,portfolio_id,decision_session,decision_time,"
                    "execution_session,universe_snapshot_id,quality_report_id,"
                    "decision_price_policy_id,execution_price_policy_id,candidate_manifest,"
                    "candidate_hash,sizing_policy_version,sizing_policy_hash,execution_locked) "
                    "VALUES ('00000000-0000-7000-8000-000000000058',"
                    "'00000000-0000-7000-8000-000000000001',"
                    "'00000000-0000-7000-8000-000000000002','2026-09-14',"
                    "'2026-09-14T12:00:00Z','2026-09-14',"
                    "'00000000-0000-7000-8000-000000000003',NULL,"
                    "'4d24df4a-b510-541b-8a6a-fe7db5afaac4',"
                    "'cf525f07-4e55-5f84-a839-60d845819ec5','[]',"
                    "repeat('a',64),'agent_paper_sizing_v1',repeat('b',64),true)"
                )
            )
    finally:
        await engine.dispose()


def test_0058_refuses_downgrade_after_a_schwab_cycle_exists(
    monkeypatch: pytest.MonkeyPatch, temporary_database_url: str
) -> None:
    """Catch a rollback that would remove contracts beneath frozen Schwab facts."""
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(ENGINE_ROOT / "alembic.ini"))
    command.upgrade(config, "0058_agent_paper_schwab_realtime")
    asyncio.run(_seed_schwab_cycle_without_foreign_roots(temporary_database_url))

    with pytest.raises(IntegrityError, match="SCHWAB_AGENT_PAPER_FACTS_EXIST"):
        command.downgrade(config, "0057_agent_paper_market_window")

    assert asyncio.run(_database_contract(temporary_database_url))["policies"]
