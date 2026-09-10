"""Migration boundary tests for portfolio decision runs."""

from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from sqlalchemy import inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import create_async_engine

_TABLES = {
    "portfolio_decision_runs",
    "portfolio_decision_outputs",
    "portfolio_decision_budget_reservations",
}


def test_0049_is_the_single_migration_head() -> None:
    root = Path(__file__).resolve().parents[2]
    scripts = ScriptDirectory.from_config(Config(str(root / "alembic.ini")))
    revision_0048 = scripts.get_revision("0048_portfolio_decision_runs")
    revision_0049 = scripts.get_revision("0049_pd_terminal_phases")
    assert revision_0048 is not None
    assert revision_0048.down_revision == "0047_alpaca_market_stream"
    assert revision_0049 is not None
    assert revision_0049.down_revision == "0048_portfolio_decision_runs"
    assert scripts.get_heads() == [CURRENT_MIGRATION_HEAD]


def test_0048_schema_and_downgrade_guards(
    temporary_database_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = Path(__file__).resolve().parents[2]
    config = Config(str(root / "alembic.ini"))
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(config, "0047_alpaca_market_stream")
    command.upgrade(config, "0048_portfolio_decision_runs")

    schema = asyncio.run(_inspect_schema(temporary_database_url))
    assert schema["tables"] >= _TABLES
    assert schema["run_unique"] >= {"uq_portfolio_decision_runs_actor_idempotency"}
    assert schema["diagnostic_unique"] >= {"uq_diag_reports_id_snapshot"}
    assert schema["run_foreign_keys"] >= {
        "fk_portfolio_decision_runs_snapshot_portfolio",
        "fk_portfolio_decision_runs_diagnostic_snapshot",
    }
    assert schema["run_checks"] >= {
        "ck_portfolio_decision_runs_frozen_input_object",
        "ck_portfolio_decision_runs_evidence_manifest_bounded",
        "ck_portfolio_decision_runs_tool_result_projection_bound",
    }

    asyncio.run(_exercise_prompt_role(temporary_database_url))
    run_id = asyncio.run(_insert_guard_fixture(temporary_database_url))
    with pytest.raises(RuntimeError, match="active portfolio decision"):
        command.downgrade(config, "0047_alpaca_market_stream")

    asyncio.run(_make_fixture_terminal_but_current_day(temporary_database_url, run_id))
    with pytest.raises(RuntimeError, match="current UTC budget day"):
        command.downgrade(config, "0047_alpaca_market_stream")

    asyncio.run(_move_fixture_to_previous_budget_day(temporary_database_url, run_id))
    command.downgrade(config, "0047_alpaca_market_stream")
    assert asyncio.run(_table_names(temporary_database_url)).isdisjoint(_TABLES)


def test_0049_downgrade_rejects_early_abstention(
    temporary_database_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = Path(__file__).resolve().parents[2]
    config = Config(str(root / "alembic.ini"))
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    command.upgrade(config, "0049_pd_terminal_phases")

    command.downgrade(config, "0048_portfolio_decision_runs")
    command.upgrade(config, "0049_pd_terminal_phases")
    run_id = asyncio.run(_insert_guard_fixture(temporary_database_url))
    asyncio.run(_make_fixture_early_abstained(temporary_database_url, run_id))

    with pytest.raises(RuntimeError, match="early portfolio decision abstentions"):
        command.downgrade(config, "0048_portfolio_decision_runs")


async def _inspect_schema(database_url: str) -> dict[str, set[str]]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return await connection.run_sync(
                lambda sync: {
                    "tables": set(inspect(sync).get_table_names()),
                    "run_unique": {
                        item["name"]
                        for item in inspect(sync).get_unique_constraints("portfolio_decision_runs")
                    },
                    "diagnostic_unique": {
                        item["name"]
                        for item in inspect(sync).get_unique_constraints(
                            "portfolio_diagnostic_reports"
                        )
                    },
                    "run_foreign_keys": {
                        item["name"]
                        for item in inspect(sync).get_foreign_keys("portfolio_decision_runs")
                    },
                    "run_checks": {
                        item["name"]
                        for item in inspect(sync).get_check_constraints("portfolio_decision_runs")
                    },
                }
            )
    finally:
        await engine.dispose()


async def _exercise_prompt_role(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            for role in ("portfolio_decision", "portfolio"):
                await connection.execute(
                    text(
                        "INSERT INTO agent_prompt_versions (id,role,version,content_hash,content) "
                        "VALUES (:id,:role,:version,:hash,'prompt')"
                    ),
                    {"id": uuid4(), "role": role, "version": str(uuid4()), "hash": "a" * 64},
                )
        with pytest.raises(IntegrityError):
            async with engine.begin() as connection:
                await connection.execute(
                    text(
                        "INSERT INTO agent_prompt_versions (id,role,version,content_hash,content) "
                        "VALUES (:id,'arbitrary_role',:version,:hash,'prompt')"
                    ),
                    {"id": uuid4(), "version": str(uuid4()), "hash": "a" * 64},
                )
    finally:
        await engine.dispose()


async def _insert_guard_fixture(database_url: str):
    engine = create_async_engine(database_url)
    run_id = uuid4()
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SET LOCAL session_replication_role = replica"))
            await connection.execute(
                text(
                    "INSERT INTO portfolio_decision_runs ("
                    "id,portfolio_id,portfolio_snapshot_id,diagnostic_report_id,prompt_version_id,"
                    "model_config_id,actor_id,idempotency_key,request_hash,status,retry_count,"
                    "decision_time,input_hash,schema_hash,frozen_input,prompt_hash,model_hash,"
                    "tool_hash,evidence_hash,evidence_manifest,provider_phase,reserved_cost_usd,"
                    "pricing_version) VALUES ("
                    ":id,:portfolio,:snapshot,:diagnostic,:prompt,:model,'test','key',:hash,"
                    "'prepared',0,now(),:hash,:hash,'{}'::jsonb,:hash,:hash,:hash,:hash,"
                    "'[{}]'::jsonb,'none',1,'test')"
                ),
                {
                    "id": run_id,
                    "portfolio": uuid4(),
                    "snapshot": uuid4(),
                    "diagnostic": uuid4(),
                    "prompt": uuid4(),
                    "model": uuid4(),
                    "hash": "b" * 64,
                },
            )
    finally:
        await engine.dispose()
    return run_id


async def _make_fixture_early_abstained(database_url: str, run_id) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SET LOCAL session_replication_role = replica"))
            await connection.execute(
                text(
                    "UPDATE portfolio_decision_runs "
                    "SET status='abstained', provider_phase='first_requested' WHERE id=:id"
                ),
                {"id": run_id},
            )
    finally:
        await engine.dispose()


async def _make_fixture_terminal_but_current_day(database_url: str, run_id) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SET LOCAL session_replication_role = replica"))
            await connection.execute(
                text("UPDATE portfolio_decision_runs SET status='failed' WHERE id=:id"),
                {"id": run_id},
            )
            await connection.execute(
                text(
                    "INSERT INTO portfolio_decision_budget_reservations ("
                    "id,business_date,run_id,pricing_version,reserved_usd,settled_usd,state) "
                    "VALUES (:reservation,(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date,"
                    ":run,'test',1,1,'settled')"
                ),
                {"reservation": uuid4(), "run": run_id},
            )
    finally:
        await engine.dispose()


async def _move_fixture_to_previous_budget_day(database_url: str, run_id) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text("SET LOCAL session_replication_role = replica"))
            await connection.execute(
                text(
                    "UPDATE portfolio_decision_budget_reservations "
                    "SET business_date=(CURRENT_TIMESTAMP AT TIME ZONE 'UTC')::date - 1 "
                    "WHERE run_id=:run"
                ),
                {"run": run_id},
            )
            await connection.execute(
                text("DELETE FROM agent_prompt_versions WHERE role='portfolio_decision'")
            )
    finally:
        await engine.dispose()


async def _table_names(database_url: str) -> set[str]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return set(await connection.run_sync(lambda sync: inspect(sync).get_table_names()))
    finally:
        await engine.dispose()
