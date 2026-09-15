"""Agent paper-cycle schema authority and downgrade fences."""

import asyncio
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.paper import (
    AgentPaperCycleEventRecord,
    AgentPaperCycleRecord,
    StrategyDecisionAgentOutputRecord,
    StrategyDecisionRecord,
)
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

ENGINE_ROOT = Path(__file__).parents[2]
MIGRATION_PATH = ENGINE_ROOT / "alembic/versions/0056_agent_paper_cycle.py"
PAPER_ACCOUNT_ID = "d31672c0-3922-5ebd-8fdb-cd3174266731"
PAPER_PORTFOLIO_ID = "ee2ff673-eaf6-5076-bbab-f4c9cedfc74e"
DECISION_POLICY_ID = "2573d63a-fb6d-53d3-8d44-207766a78322"
EXECUTION_POLICY_ID = "e99532cc-e24c-52a8-8165-c8076325fbd2"


def test_0056_remains_in_the_forward_chain_and_declares_agent_paper_tables() -> None:
    scripts = ScriptDirectory.from_config(Config(str(ENGINE_ROOT / "alembic.ini")))

    assert CURRENT_MIGRATION_HEAD == "0061_agent_paper_job_isolation"
    assert scripts.get_heads() == [CURRENT_MIGRATION_HEAD]
    migration = scripts.get_revision("0056_agent_paper_cycle")
    assert migration is not None
    assert migration.down_revision == "0055_schwab_intraday_job_v3"
    assert {
        AgentPaperCycleRecord.__tablename__,
        AgentPaperCycleEventRecord.__tablename__,
        StrategyDecisionAgentOutputRecord.__tablename__,
    } == {
        "agent_paper_cycles",
        "agent_paper_cycle_events",
        "strategy_decision_agent_outputs",
    }


def test_strategy_decision_model_exposes_disjoint_origins() -> None:
    columns = StrategyDecisionRecord.__table__.columns

    assert columns["decision_origin"].nullable is False
    assert columns["execution_source_policy_id"].nullable is True
    assert columns["strategy_definition_id"].nullable is True
    assert columns["registry_event_id"].nullable is True
    assert columns["champion_generation"].nullable is True


def test_0056_migration_contains_preservation_binding_and_downgrade_fences() -> None:
    source = MIGRATION_PATH.read_text()
    downgrade = source.split("def downgrade() -> None:", maxsplit=1)[1]

    assert "registered_strategy" in source
    assert "codex-sdk-chatgpt-v4-actionable" in source
    assert "BETWEEN 1 AND 5" in source
    assert "DEFERRABLE INITIALLY DEFERRED" in source
    assert "uq_strategy_decisions_account_session" not in source
    assert "agent paper facts prevent downgrade" in downgrade
    assert downgrade.index("pg_advisory_xact_lock") < downgrade.index(
        "agent paper facts prevent downgrade"
    )


async def _schema_snapshot(database_url: str) -> dict[str, object]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            tables = {
                str(row.table_name)
                for row in await connection.execute(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema='public'"
                    )
                )
            }
            origins = {
                str(row.column_name): str(row.is_nullable)
                for row in await connection.execute(
                    text(
                        "SELECT column_name,is_nullable FROM information_schema.columns "
                        "WHERE table_schema='public' AND table_name='strategy_decisions' "
                        "AND column_name IN ('decision_origin','execution_source_policy_id',"
                        "'strategy_definition_id','registry_event_id','champion_generation')"
                    )
                )
            }
            policies = [
                dict(row)
                for row in (
                    await connection.execute(
                        text(
                            "SELECT key,version,payload->>'content_hash' AS content_hash "
                            "FROM source_policies WHERE key LIKE 'paper_%_price' ORDER BY key"
                        )
                    )
                ).mappings()
            ]
            return {"tables": tables, "origins": origins, "policies": policies}
    finally:
        await engine.dispose()


def test_0056_clean_upgrade_and_downgrade_round_trip(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(ENGINE_ROOT / "alembic.ini"))
    command.upgrade(config, "0055_schwab_intraday_job_v3")

    command.upgrade(config, "0056_agent_paper_cycle")
    upgraded = asyncio.run(_schema_snapshot(temporary_database_url))
    assert {
        "agent_paper_cycles",
        "agent_paper_cycle_events",
        "strategy_decision_agent_outputs",
    } <= upgraded["tables"]
    assert upgraded["origins"] == {
        "champion_generation": "YES",
        "decision_origin": "NO",
        "execution_source_policy_id": "YES",
        "registry_event_id": "YES",
        "strategy_definition_id": "YES",
    }
    assert upgraded["policies"] == [
        {
            "key": "paper_decision_price",
            "version": "paper_decision_price_policy_v1",
            "content_hash": "70737603eb58331d60668bb6a039e4910d856a2c992e972fc8af0adcf05db579",
        },
        {
            "key": "paper_execution_price",
            "version": "paper_execution_price_policy_v1",
            "content_hash": "41efc2c8625321bc8d9d941500fd3f809868a12fcc13ee2fefdfa4bf52705682",
        },
    ]

    command.downgrade(config, "0055_schwab_intraday_job_v3")
    downgraded = asyncio.run(_schema_snapshot(temporary_database_url))
    assert "agent_paper_cycles" not in downgraded["tables"]
    assert downgraded["origins"] == {
        "champion_generation": "NO",
        "registry_event_id": "NO",
        "strategy_definition_id": "NO",
    }
    assert downgraded["policies"] == []


async def _seed_registered_decision(database_url: str) -> str:
    decision_id = "30000000-0000-0000-0000-000000000001"
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            registry = (
                (
                    await connection.execute(
                        text(
                            "SELECT active_definition_id,last_event_id AS active_event_id,"
                            "active_generation AS generation "
                            "FROM strategy_registry_heads WHERE channel='internal_paper'"
                        )
                    )
                )
                .mappings()
                .one()
            )
            source_policy_id = await connection.scalar(
                text("SELECT id FROM source_policies ORDER BY created_at,id LIMIT 1")
            )
            await connection.execute(
                text(
                    "INSERT INTO exchange_calendar_authorities "
                    "(id,exchange,version,source,range_start,range_end,query_parameters,"
                    "completeness_status,content_hash,published_at) VALUES "
                    "('30000000-0000-0000-0000-000000000002','XNYS','agent-paper-test-v1',"
                    "'fixture','2026-09-01','2026-09-30','{}'::jsonb,'complete',repeat('a',64),"
                    "'2026-09-11T20:00:00Z')"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO universe_snapshots "
                    "(id,key,version,effective_from,source,content_hash,published_at) VALUES "
                    "('30000000-0000-0000-0000-000000000003','agent-paper-test',"
                    "'registered-v1','2026-09-01','fixture',repeat('b',64),"
                    "'2026-09-11T20:00:00Z')"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO data_snapshots "
                    "(id,source,provider_object_id,revision,observed_at,available_at,received_at,"
                    "schema_version,payload) VALUES "
                    "('30000000-0000-0000-0000-000000000004','fixture','agent-paper-test',"
                    "'registered-v1','2026-09-11T19:00:00Z','2026-09-11T19:00:00Z',"
                    "'2026-09-11T19:00:00Z','v1','{}'::jsonb)"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO quality_reports "
                    "(id,data_snapshot_id,status,schema_version,payload) VALUES "
                    "('30000000-0000-0000-0000-000000000005',"
                    "'30000000-0000-0000-0000-000000000004','PASS','v1','{}'::jsonb)"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO strategy_decisions "
                    "(id,account_id,portfolio_id,strategy_id,strategy_version,"
                    "strategy_definition_id,registry_event_id,champion_generation,decision_session,"
                    "decision_time,execution_session,source_policy_id,calendar_authority_id,"
                    "universe_snapshot_id,quality_report_id,portfolio_projection_hash,authority_hash,"
                    "strategy_parameters_hash,decision_hash,cash_weight,reason) VALUES "
                    "(:decision_id,:account_id,:portfolio_id,'baseline','baseline_v1',"
                    ":definition_id,:event_id,:generation,'2026-09-11','2026-09-11T20:00:00Z',"
                    "'2026-09-14',:source_policy_id,'30000000-0000-0000-0000-000000000002',"
                    "'30000000-0000-0000-0000-000000000003',"
                    "'30000000-0000-0000-0000-000000000005',repeat('c',64),repeat('d',64),"
                    "repeat('e',64),repeat('f',64),1,'WEEKLY_REBALANCE')"
                ),
                {
                    "decision_id": decision_id,
                    "account_id": PAPER_ACCOUNT_ID,
                    "portfolio_id": PAPER_PORTFOLIO_ID,
                    "definition_id": registry["active_definition_id"],
                    "event_id": registry["active_event_id"],
                    "generation": registry["generation"],
                    "source_policy_id": source_policy_id,
                },
            )
    finally:
        await engine.dispose()
    return decision_id


async def _registered_decision_origin(database_url: str, decision_id: str) -> dict[str, object]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            row = (
                (
                    await connection.execute(
                        text(
                            "SELECT decision_origin,execution_source_policy_id,"
                            "strategy_definition_id,registry_event_id,champion_generation "
                            "FROM strategy_decisions WHERE id=CAST(:id AS uuid)"
                        ),
                        {"id": decision_id},
                    )
                )
                .mappings()
                .one()
            )
            return dict(row)
    finally:
        await engine.dispose()


async def _expect_rejected(database_url: str, statement: str, values: dict[str, object]) -> None:
    engine = create_async_engine(database_url)
    try:
        with pytest.raises(DBAPIError):
            async with engine.begin() as connection:
                await connection.execute(text(statement), values)
    finally:
        await engine.dispose()


def test_0056_preserves_registered_decisions_and_rejects_origin_spoof(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(ENGINE_ROOT / "alembic.ini"))
    command.upgrade(config, "0055_schwab_intraday_job_v3")
    decision_id = asyncio.run(_seed_registered_decision(temporary_database_url))

    command.upgrade(config, "0056_agent_paper_cycle")
    preserved = asyncio.run(_registered_decision_origin(temporary_database_url, decision_id))
    assert preserved["decision_origin"] == "registered_strategy"
    assert preserved["execution_source_policy_id"] is None
    assert preserved["strategy_definition_id"] is not None
    assert preserved["registry_event_id"] is not None
    assert preserved["champion_generation"] >= 1

    asyncio.run(
        _expect_rejected(
            temporary_database_url,
            "INSERT INTO strategy_decisions "
            "(id,account_id,portfolio_id,strategy_id,strategy_version,decision_origin,"
            "strategy_definition_id,registry_event_id,champion_generation,decision_session,"
            "decision_time,execution_session,source_policy_id,execution_source_policy_id,"
            "calendar_authority_id,universe_snapshot_id,quality_report_id,"
            "portfolio_projection_hash,authority_hash,strategy_parameters_hash,decision_hash,"
            "cash_weight,reason) VALUES "
            "('30000000-0000-0000-0000-000000000006',:account_id,:portfolio_id,"
            "'baseline','baseline_v1','registered_strategy',NULL,NULL,NULL,'2026-09-12',"
            "'2026-09-12T20:00:00Z','2026-09-14',:decision_policy,:execution_policy,"
            "'30000000-0000-0000-0000-000000000002',"
            "'30000000-0000-0000-0000-000000000003',"
            "'30000000-0000-0000-0000-000000000005',repeat('1',64),repeat('2',64),"
            "repeat('3',64),repeat('4',64),1,'WEEKLY_REBALANCE')",
            {
                "account_id": PAPER_ACCOUNT_ID,
                "portfolio_id": PAPER_PORTFOLIO_ID,
                "decision_policy": DECISION_POLICY_ID,
                "execution_policy": EXECUTION_POLICY_ID,
            },
        )
    )


async def _seed_agent_decision_and_outputs(database_url: str) -> tuple[str, list[str]]:
    decision_id = "40000000-0000-0000-0000-000000000001"
    output_ids: list[str] = []
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO agent_prompt_versions "
                    "(id,role,version,content_hash,content) VALUES "
                    "('40000000-0000-0000-0000-000000000002','portfolio','agent-paper-test-v1',"
                    "repeat('a',64),'test')"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO agent_model_configs "
                    "(id,model_id,version,config_hash,max_input_tokens,max_tool_calls,"
                    "max_output_tokens,input_usd_per_million,output_usd_per_million,"
                    "pricing_available_at,pricing_expires_at) VALUES "
                    "('40000000-0000-0000-0000-000000000003','fixture','agent-paper-test-v1',"
                    "repeat('b',64),1,0,1,0,0,'2026-01-01T00:00:00Z','2027-01-01T00:00:00Z')"
                )
            )
            await connection.execute(
                text(
                    "INSERT INTO strategy_decisions "
                    "(id,account_id,portfolio_id,strategy_id,strategy_version,decision_origin,"
                    "strategy_definition_id,registry_event_id,champion_generation,decision_session,"
                    "decision_time,execution_session,source_policy_id,execution_source_policy_id,"
                    "calendar_authority_id,universe_snapshot_id,quality_report_id,"
                    "portfolio_projection_hash,authority_hash,strategy_parameters_hash,decision_hash,"
                    "cash_weight,reason) VALUES "
                    "(:id,:account_id,:portfolio_id,'agent_research','agent_paper_decision_v1',"
                    "'agent_research',NULL,NULL,NULL,'2026-09-12','2026-09-12T20:00:00Z',"
                    "'2026-09-14',:decision_policy,:execution_policy,"
                    "'30000000-0000-0000-0000-000000000002',"
                    "'30000000-0000-0000-0000-000000000003',"
                    "NULL,repeat('5',64),repeat('6',64),"
                    "repeat('7',64),repeat('8',64),1,'AGENT_RESEARCH')"
                ),
                {
                    "id": decision_id,
                    "account_id": PAPER_ACCOUNT_ID,
                    "portfolio_id": PAPER_PORTFOLIO_ID,
                    "decision_policy": DECISION_POLICY_ID,
                    "execution_policy": EXECUTION_POLICY_ID,
                },
            )
            for index in range(6):
                instrument_id = f"40000000-0000-0000-0000-{100 + index:012d}"
                run_id = f"40000000-0000-0000-0000-{200 + index:012d}"
                output_id = f"40000000-0000-0000-0000-{300 + index:012d}"
                output_ids.append(output_id)
                await connection.execute(
                    text(
                        "INSERT INTO instruments "
                        "(id,asset_type,exchange,currency,tick_size,risk_bucket) VALUES "
                        "(CAST(:id AS uuid),'common_stock','XNYS','USD',0.01,'equity')"
                    ),
                    {"id": instrument_id},
                )
                await connection.execute(
                    text(
                        "INSERT INTO agent_research_runs "
                        "(id,instrument_id,data_snapshot_id,prompt_version_id,model_config_id,"
                        "decision_time,actor_id,idempotency_key,status,retry_count,schema_hash,"
                        "model_hash,prompt_hash,prompt_manifest,evidence_hash,evidence_manifest,"
                        "tool_hash,input_hash,reserved_cost_usd,settled_cost_usd,pricing_version,"
                        "input_tokens,output_tokens) VALUES "
                        "(CAST(:run_id AS uuid),CAST(:instrument_id AS uuid),"
                        "'30000000-0000-0000-0000-000000000004',"
                        "'40000000-0000-0000-0000-000000000002',"
                        "'40000000-0000-0000-0000-000000000003','2026-09-12T20:00:00Z',"
                        "'agent-paper-test',:idempotency_key,'completed',0,repeat('a',64),"
                        "repeat('b',64),repeat('c',64),'{}'::jsonb,repeat('d',64),"
                        "jsonb_build_array(jsonb_build_object('fixture',true)),"
                        "repeat('e',64),repeat('f',64),0,0,"
                        "'codex-sdk-chatgpt-v4-actionable',0,0)"
                    ),
                    {
                        "run_id": run_id,
                        "instrument_id": instrument_id,
                        "idempotency_key": f"agent-paper-{index}",
                    },
                )
                await connection.execute(
                    text(
                        "INSERT INTO agent_research_outputs "
                        "(id,run_id,sequence,status,role,prompt_hash,content_hash,payload,intent) "
                        "VALUES (CAST(:id AS uuid),CAST(:run_id AS uuid),1,'completed',NULL,NULL,"
                        "repeat('a',64),'{}'::jsonb,'{}'::jsonb)"
                    ),
                    {"id": output_id, "run_id": run_id},
                )
            for output_id in output_ids[:5]:
                await connection.execute(
                    text(
                        "INSERT INTO strategy_decision_agent_outputs "
                        "(strategy_decision_id,agent_research_output_id,intent_hash,sizing_action,"
                        "binding_hash) VALUES (CAST(:decision_id AS uuid),CAST(:output_id AS uuid),"
                        "repeat('a',64),'maintain',repeat('b',64))"
                    ),
                    {"decision_id": decision_id, "output_id": output_id},
                )
    finally:
        await engine.dispose()
    return decision_id, output_ids


async def _seed_second_agent_decision(database_url: str) -> str:
    decision_id = "40000000-0000-0000-0000-000000000004"
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO strategy_decisions "
                    "(id,account_id,portfolio_id,strategy_id,strategy_version,decision_origin,"
                    "strategy_definition_id,registry_event_id,champion_generation,decision_session,"
                    "decision_time,execution_session,source_policy_id,execution_source_policy_id,"
                    "calendar_authority_id,universe_snapshot_id,quality_report_id,"
                    "portfolio_projection_hash,authority_hash,strategy_parameters_hash,decision_hash,"
                    "cash_weight,reason) VALUES "
                    "(CAST(:id AS uuid),:account_id,:portfolio_id,'agent_research',"
                    "'agent_paper_decision_v1','agent_research',NULL,NULL,NULL,'2026-09-15',"
                    "'2026-09-15T20:00:00Z','2026-09-16',:decision_policy,:execution_policy,"
                    "'30000000-0000-0000-0000-000000000002',"
                    "'30000000-0000-0000-0000-000000000003',"
                    "NULL,repeat('9',64),repeat('a',64),"
                    "repeat('b',64),repeat('c',64),1,'AGENT_RESEARCH')"
                ),
                {
                    "id": decision_id,
                    "account_id": PAPER_ACCOUNT_ID,
                    "portfolio_id": PAPER_PORTFOLIO_ID,
                    "decision_policy": DECISION_POLICY_ID,
                    "execution_policy": EXECUTION_POLICY_ID,
                },
            )
    finally:
        await engine.dispose()
    return decision_id


async def _seed_agent_cycle(database_url: str) -> str:
    cycle_id = "40000000-0000-0000-0000-000000000007"
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "INSERT INTO agent_paper_cycles "
                    "(id,account_id,portfolio_id,decision_session,decision_time,execution_session,"
                    "universe_snapshot_id,quality_report_id,decision_price_policy_id,"
                    "execution_price_policy_id,candidate_manifest,candidate_hash,"
                    "sizing_policy_version,sizing_policy_hash,execution_locked) VALUES "
                    "(CAST(:id AS uuid),:account_id,:portfolio_id,'2026-09-17',"
                    "'2026-09-17T20:00:00Z','2026-09-18',"
                    "'30000000-0000-0000-0000-000000000003',"
                    "NULL,:decision_policy,"
                    ":execution_policy,'[]'::jsonb,repeat('a',64),'agent_paper_sizing_v1',"
                    "repeat('b',64),true)"
                ),
                {
                    "id": cycle_id,
                    "account_id": PAPER_ACCOUNT_ID,
                    "portfolio_id": PAPER_PORTFOLIO_ID,
                    "decision_policy": DECISION_POLICY_ID,
                    "execution_policy": EXECUTION_POLICY_ID,
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO agent_paper_cycle_events "
                    "(cycle_id,sequence,from_status,to_status,result_code,fact_ids,event_hash,"
                    "occurred_at) VALUES (CAST(:id AS uuid),1,NULL,'prepared','PREPARED',"
                    "'{}'::jsonb,repeat('c',64),'2026-09-17T20:00:00Z')"
                ),
                {"id": cycle_id},
            )
    finally:
        await engine.dispose()
    return cycle_id


def test_0056_rejects_more_than_five_outputs_output_reuse_and_downgrade(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(ENGINE_ROOT / "alembic.ini"))
    command.upgrade(config, "0055_schwab_intraday_job_v3")
    asyncio.run(_seed_registered_decision(temporary_database_url))
    command.upgrade(config, "0056_agent_paper_cycle")
    decision_id, output_ids = asyncio.run(_seed_agent_decision_and_outputs(temporary_database_url))
    second_decision_id = asyncio.run(_seed_second_agent_decision(temporary_database_url))
    cycle_id = asyncio.run(_seed_agent_cycle(temporary_database_url))

    binding_statement = (
        "INSERT INTO strategy_decision_agent_outputs "
        "(strategy_decision_id,agent_research_output_id,intent_hash,sizing_action,binding_hash) "
        "VALUES (CAST(:decision_id AS uuid),CAST(:output_id AS uuid),repeat('a',64),'maintain',"
        "repeat('b',64))"
    )
    asyncio.run(
        _expect_rejected(
            temporary_database_url,
            binding_statement,
            {"decision_id": decision_id, "output_id": output_ids[5]},
        )
    )
    asyncio.run(
        _expect_rejected(
            temporary_database_url,
            binding_statement,
            {
                "decision_id": second_decision_id,
                "output_id": output_ids[0],
            },
        )
    )
    asyncio.run(
        _expect_rejected(
            temporary_database_url,
            "WITH idempotency AS ("
            "INSERT INTO idempotency_records "
            "(id,actor_id,route,idempotency_key,request_hash,schema_version,response_payload) "
            "VALUES ('40000000-0000-0000-0000-000000000005','agent-paper-test',"
            "'/paper/orders','missing-bindings',repeat('d',64),'v1','{}'::jsonb) RETURNING id) "
            "INSERT INTO paper_order_batches "
            "(id,account_id,strategy_decision_id,risk_policy_id,idempotency_record_id,"
            "request_hash,projection_hash,risk_input_hash,result_hash,batch_hash,evaluated_at) "
            "SELECT '40000000-0000-0000-0000-000000000006',:account_id,"
            "CAST(:decision_id AS uuid),risk.id,idempotency.id,repeat('1',64),repeat('2',64),"
            "repeat('3',64),repeat('4',64),repeat('5',64),'2026-09-15T20:01:00Z' "
            "FROM risk_policies risk CROSS JOIN idempotency WHERE risk.active",
            {"account_id": PAPER_ACCOUNT_ID, "decision_id": second_decision_id},
        )
    )
    asyncio.run(
        _expect_rejected(
            temporary_database_url,
            "UPDATE agent_paper_cycles SET candidate_hash=repeat('f',64) "
            "WHERE id=CAST(:id AS uuid)",
            {"id": cycle_id},
        )
    )
    asyncio.run(
        _expect_rejected(
            temporary_database_url,
            "INSERT INTO agent_paper_cycle_events "
            "(cycle_id,sequence,from_status,to_status,result_code,fact_ids,event_hash,occurred_at) "
            "VALUES (CAST(:id AS uuid),3,'prepared','completed','INVALID','{}'::jsonb,"
            "repeat('f',64),'2026-09-17T20:01:00Z')",
            {"id": cycle_id},
        )
    )
    with pytest.raises(DBAPIError, match="agent paper facts prevent downgrade"):
        command.downgrade(config, "0055_schwab_intraday_job_v3")
