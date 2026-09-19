"""Agent-version evaluation schema authority and downgrade fences."""

import asyncio
from datetime import UTC, date, datetime
from io import StringIO
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.agent_versions import (
    AgentVersionCyclePairRecord,
    AgentVersionEvaluationDecisionRecord,
    AgentVersionEvaluationRecord,
)
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

ENGINE_ROOT = Path(__file__).parents[2]
MIGRATION_HEAD = "0065_portfolio_management"
EVALUATION_HEAD = "0062_agent_version_evaluation"
PAIR_CONTEXT_HEAD = "0063_agent_version_pair_context"
PREVIOUS_HEAD = "0061_agent_paper_job_isolation"


def test_current_head_retains_agent_version_authority_lineage() -> None:
    scripts = ScriptDirectory.from_config(Config(str(ENGINE_ROOT / "alembic.ini")))

    assert CURRENT_MIGRATION_HEAD == MIGRATION_HEAD
    assert scripts.get_heads() == [MIGRATION_HEAD]
    revision = scripts.get_revision(MIGRATION_HEAD)
    pair_revision = scripts.get_revision(PAIR_CONTEXT_HEAD)
    assert revision is not None and revision.down_revision == "0064_prompt_release_channel"
    assert pair_revision is not None and pair_revision.down_revision == EVALUATION_HEAD
    assert {
        AgentVersionEvaluationRecord.__tablename__,
        AgentVersionCyclePairRecord.__tablename__,
        AgentVersionEvaluationDecisionRecord.__tablename__,
    } == {
        "agent_version_evaluations",
        "agent_version_cycle_pairs",
        "agent_version_evaluation_decisions",
    }


def test_0062_offline_sql_contains_fail_closed_guards_and_empty_only_downgrade() -> None:
    output = StringIO()
    config = Config(str(ENGINE_ROOT / "alembic.ini"), output_buffer=output)
    command.upgrade(config, f"{PREVIOUS_HEAD}:{EVALUATION_HEAD}", sql=True)

    sql = output.getvalue()
    assert "nexus_agent_version_evaluation_valid" in sql
    assert "nexus_agent_version_pair_valid" in sql
    assert "nexus_agent_version_decision_valid" in sql
    assert "simulation_write_internal" in sql
    assert "broker_account_bindings" in sql
    assert "execution_locked" in sql
    assert "local-agent-version-incumbent" in sql
    assert "local-agent-version-challenger" in sql
    assert "nexus_agent_version_external_account_denied" in sql
    assert "nexus_agent_version_broker_intent_denied" in sql
    assert "nexus_agent_version_cycle_scope_valid" in sql
    assert "exchange_calendar_sessions" in sql
    assert "strategy_shadow" not in sql

    rollback = StringIO()
    rollback_config = Config(str(ENGINE_ROOT / "alembic.ini"), output_buffer=rollback)
    command.downgrade(rollback_config, f"{EVALUATION_HEAD}:{PREVIOUS_HEAD}", sql=True)
    rollback_sql = rollback.getvalue()
    assert "agent version evaluation facts prevent downgrade" in rollback_sql
    assert "ERRCODE='P3D01'" in rollback_sql


def test_0063_pair_guard_uses_account_neutral_market_authority() -> None:
    output = StringIO()
    config = Config(str(ENGINE_ROOT / "alembic.ini"), output_buffer=output)
    command.upgrade(config, f"{EVALUATION_HEAD}:{MIGRATION_HEAD}", sql=True)

    sql = output.getvalue()
    assert "agent_version_market_evidence_v1|" in sql
    assert "incumbent_cycle.candidate_hash" in sql
    assert "NEW.market_evidence_hash IS DISTINCT FROM" in sql
    assert "incumbent_run.data_snapshot_id <> challenger_run.data_snapshot_id" not in sql
    assert "incumbent_run.evidence_hash <> challenger_run.evidence_hash" not in sql
    assert "LOCK TABLE public.agent_version_cycle_pairs IN ACCESS EXCLUSIVE MODE" in sql
    assert "agent version cycle pairs prevent evidence semantic change" in sql
    assert "ERRCODE='P3D01'" in sql


async def _tables(database_url: str) -> set[str]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return {
                str(row.table_name)
                for row in await connection.execute(
                    text(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema='public'"
                    )
                )
            }
    finally:
        await engine.dispose()


async def _referenced_prompt_channel(database_url: str) -> str | None:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            return await connection.scalar(
                text(
                    "SELECT prompt.release_channel "
                    "FROM agent_version_evaluations evaluation "
                    "JOIN agent_prompt_versions prompt "
                    "ON prompt.id=evaluation.challenger_prompt_version_id"
                )
            )
    finally:
        await engine.dispose()


def test_0062_clean_upgrade_and_downgrade_round_trip(
    monkeypatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(ENGINE_ROOT / "alembic.ini"))
    command.upgrade(config, PREVIOUS_HEAD)

    command.upgrade(config, MIGRATION_HEAD)
    assert {
        "agent_version_evaluations",
        "agent_version_cycle_pairs",
        "agent_version_evaluation_decisions",
    } <= asyncio.run(_tables(temporary_database_url))

    command.downgrade(config, PREVIOUS_HEAD)
    assert not {
        "agent_version_evaluations",
        "agent_version_cycle_pairs",
        "agent_version_evaluation_decisions",
    } & asyncio.run(_tables(temporary_database_url))


def test_0064_fails_closed_when_legacy_evaluation_exists(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(ENGINE_ROOT / "alembic.ini"))
    command.upgrade(config, PAIR_CONTEXT_HEAD)
    asyncio.run(_seed_evaluation(temporary_database_url))

    with pytest.raises(
        DBAPIError,
        match="existing agent version evaluations require explicit release-channel migration",
    ):
        command.upgrade(config, MIGRATION_HEAD)

    engine = create_async_engine(temporary_database_url)

    async def current_revision() -> str:
        try:
            async with engine.connect() as connection:
                return str(await connection.scalar(text("SELECT version_num FROM alembic_version")))
        finally:
            await engine.dispose()

    assert asyncio.run(current_revision()) == PAIR_CONTEXT_HEAD


async def _seed_evaluation(database_url: str, *, invalid_slot: bool = False) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    INSERT INTO instruments
                      (id,asset_type,exchange,currency,tick_size,risk_bucket)
                    VALUES
                      ('62000000-0000-0000-0000-000000000001',
                       'common_stock','NASDAQ','USD',0.01,'large_cap')
                    """
                )
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO exchange_calendar_authorities
                      (id,exchange,version,source,range_start,range_end,query_parameters,
                       completeness_status,content_hash,published_at)
                    VALUES
                      ('62000000-0000-0000-0000-000000000040','XNYS','eval-v1',
                       'exchange_calendars','2026-09-17','2026-10-02','{}'::jsonb,
                       'complete',repeat('4',64),'2026-09-16T00:00:00Z')
                    """
                )
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO exchange_calendar_sessions
                      (calendar_authority_id,session,decision_at,market_open_at,market_close_at)
                    SELECT
                      '62000000-0000-0000-0000-000000000040',session_date,
                      (session_date + time '21:00') AT TIME ZONE 'UTC',
                      (session_date + time '13:30') AT TIME ZONE 'UTC',
                      (session_date + time '20:00') AT TIME ZONE 'UTC'
                    FROM unnest(ARRAY[
                      '2026-09-17','2026-09-18','2026-09-21','2026-09-22',
                      '2026-09-23','2026-09-24','2026-09-25','2026-09-28',
                      '2026-09-29','2026-09-30','2026-10-01','2026-10-02'
                    ]::date[]) session_date
                    """
                )
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO instrument_aliases
                      (id,instrument_id,symbol,alias_type,valid_from,valid_to)
                    VALUES
                      ('62000000-0000-0000-0000-000000000002',
                       '62000000-0000-0000-0000-000000000001',
                       'AAPL','ticker','2020-01-01',NULL)
                    """
                )
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO data_snapshots
                      (id,source,provider_object_id,revision,observed_at,available_at,
                       received_at,schema_version,payload)
                    VALUES
                      ('62000000-0000-0000-0000-000000000003','fixture',
                       'agent-version-test','v1','2026-09-16T14:00:00Z',
                       '2026-09-16T14:00:00Z','2026-09-16T14:00:00Z','v1','{}'::jsonb)
                    """
                )
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO agent_prompt_versions (id,role,version,content_hash,content)
                    VALUES
                      ('62000000-0000-0000-0000-000000000004','orchestrator','eval-v1',
                       repeat('a',64),'test prompt')
                    """
                )
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO agent_model_configs
                      (id,model_id,version,config_hash,max_input_tokens,max_tool_calls,
                       max_output_tokens,input_usd_per_million,output_usd_per_million,
                       pricing_available_at,pricing_expires_at)
                    VALUES
                      ('62000000-0000-0000-0000-000000000005','test-model','eval-v1',
                       repeat('b',64),1000,1,100,0,0,
                       '2026-01-01T00:00:00Z','2027-01-01T00:00:00Z')
                    """
                )
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO agent_research_runs
                      (id,instrument_id,data_snapshot_id,prompt_version_id,model_config_id,
                       decision_time,actor_id,idempotency_key,status,retry_count,schema_hash,
                       model_hash,prompt_hash,prompt_manifest,evidence_hash,evidence_manifest,
                       tool_hash,input_hash,reserved_cost_usd,settled_cost_usd,pricing_version,
                       input_tokens,output_tokens)
                    VALUES
                      ('62000000-0000-0000-0000-000000000006',
                       '62000000-0000-0000-0000-000000000001',
                       '62000000-0000-0000-0000-000000000003',
                       '62000000-0000-0000-0000-000000000004',
                       '62000000-0000-0000-0000-000000000005',
                       '2026-09-16T14:00:00Z','migration-test','migration-test','prepared',0,
                       repeat('c',64),repeat('b',64),repeat('d',64),'{}'::jsonb,
                       repeat('e',64),'[{}]'::jsonb,repeat('f',64),repeat('1',64),
                       0,NULL,'eval-v1',NULL,NULL)
                    """
                )
            )
            for suffix, role in (("10", "incumbent"), ("20", "challenger")):
                params = {
                    "account_id": f"62000000-0000-0000-0000-0000000000{suffix}",
                    "portfolio_id": f"62000000-0000-0000-0000-0000000000{int(suffix) + 1}",
                    "paper_id": f"62000000-0000-0000-0000-0000000000{int(suffix) + 2}",
                    "name": f"Agent version {role}",
                    "code": f"agent_version_{role}_test",
                }
                await connection.execute(
                    text(
                        """
                        INSERT INTO accounts (id,provider,status)
                        VALUES (:account_id,'internal_paper_evaluation','ACTIVE')
                        """
                    ),
                    params,
                )
                await connection.execute(
                    text(
                        """
                        INSERT INTO portfolios (id,account_id,name)
                        VALUES (:portfolio_id,:account_id,:name)
                        """
                    ),
                    params,
                )
                await connection.execute(
                    text(
                        """
                        INSERT INTO paper_accounts
                          (id,account_id,portfolio_id,code,currency,account_model,
                           position_model,initial_capital,capability)
                        VALUES (:paper_id,:account_id,:portfolio_id,:code,'USD','cash',
                                'long_only',10000,'simulation_write_internal')
                        """
                    ),
                    params,
                )
            await connection.execute(
                text(
                    """
                    INSERT INTO agent_version_evaluations
                      (id,incumbent_account_id,challenger_account_id,instrument_id,
                       calendar_authority_id,
                       incumbent_version_source_run_id,challenger_prompt_version_id,
                       challenger_model_config_id,incumbent_prompt_hash,incumbent_model_hash,
                       challenger_prompt_hash,challenger_model_hash,train_slots,oos_slots,
                       policy_version,policy_hash,authority_hash)
                    VALUES
                      ('62000000-0000-0000-0000-000000000030',
                       '62000000-0000-0000-0000-000000000012',
                       '62000000-0000-0000-0000-000000000022',
                       '62000000-0000-0000-0000-000000000001',
                       '62000000-0000-0000-0000-000000000040',
                       '62000000-0000-0000-0000-000000000006',
                       '62000000-0000-0000-0000-000000000004',
                       '62000000-0000-0000-0000-000000000005',
                       repeat('d',64),repeat('b',64),repeat('a',64),repeat('b',64),
                       ARRAY[CASE WHEN :invalid_slot
                                  THEN '2026-09-19T14:00:00Z'::timestamptz
                                  ELSE '2026-09-17T14:00:00Z'::timestamptz END,
                             '2026-09-17T14:05:00Z',
                             '2026-09-18T14:00:00Z','2026-09-18T14:05:00Z']::timestamptz[],
                       ARRAY['2026-09-21T14:00:00Z','2026-09-21T14:05:00Z',
                             '2026-09-22T14:00:00Z','2026-09-22T14:05:00Z',
                             '2026-09-23T14:00:00Z','2026-09-23T14:05:00Z',
                             '2026-09-24T14:00:00Z','2026-09-24T14:05:00Z',
                             '2026-09-25T14:00:00Z','2026-09-25T14:05:00Z',
                             '2026-09-28T14:00:00Z','2026-09-28T14:05:00Z',
                             '2026-09-29T14:00:00Z','2026-09-29T14:05:00Z',
                             '2026-09-30T14:00:00Z','2026-09-30T14:05:00Z',
                             '2026-10-01T14:00:00Z','2026-10-01T14:05:00Z',
                             '2026-10-02T14:00:00Z','2026-10-02T14:05:00Z']::timestamptz[],
                       'agent_version_evaluation_v1',repeat('2',64),repeat('3',64))
                    """
                ),
                {"invalid_slot": invalid_slot},
            )
    finally:
        await engine.dispose()


async def _mutate_evaluation(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    "UPDATE agent_version_evaluations SET authority_hash=repeat('9',64) "
                    "WHERE id='62000000-0000-0000-0000-000000000030'"
                )
            )
    finally:
        await engine.dispose()


async def _add_external_id(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    INSERT INTO account_external_id_envelopes
                      (id,account_id,provider,field_context,envelope_version,aad_version,
                       key_version,nonce,ciphertext,fingerprint)
                    VALUES
                      ('62000000-0000-0000-0000-000000000050',
                       '62000000-0000-0000-0000-000000000010',
                       'internal_paper_evaluation','account_id',1,
                       'external_id_aad_v2','test',decode('00','hex'),decode('00','hex'),
                       repeat('5',64))
                    """
                )
            )
    finally:
        await engine.dispose()


async def _add_cycle(database_url: str, *, frozen_slot: bool) -> None:
    engine = create_async_engine(database_url)
    slot = datetime(2026, 9, 17 if frozen_slot else 19, 14, tzinfo=UTC)
    session = date(2026, 9, 17 if frozen_slot else 19)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    INSERT INTO agent_paper_cycles
                      (id,account_id,portfolio_id,decision_session,decision_time,
                       decision_slot_at,execution_session,universe_snapshot_id,
                       quality_report_id,decision_price_policy_id,
                       execution_price_policy_id,candidate_manifest,candidate_hash,
                       sizing_policy_version,sizing_policy_hash,execution_locked)
                    VALUES
                      ('62000000-0000-0000-0000-000000000060',
                       '62000000-0000-0000-0000-000000000012',
                       '62000000-0000-0000-0000-000000000011',CAST(:session AS date),
                       CAST(:slot AS timestamptz),CAST(:slot AS timestamptz),
                       CAST(:session AS date),'62000000-0000-0000-0000-000000000061',NULL,
                       '4d24df4a-b510-541b-8a6a-fe7db5afaac4',
                       'cf525f07-4e55-5f84-a839-60d845819ec5',
                       '[]'::jsonb,repeat('6',64),'agent_paper_sizing_v1',repeat('7',64),TRUE)
                    """
                ),
                {"session": session, "slot": slot},
            )
    finally:
        await engine.dispose()


async def _close_evaluation(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    INSERT INTO agent_version_evaluation_decisions
                      (id,evaluation_id,status,metric_report,metric_hash,
                       selected_version_source_run_id,reason_code,decision_hash)
                    VALUES
                      ('62000000-0000-0000-0000-000000000070',
                       '62000000-0000-0000-0000-000000000030','UNKNOWN','{}'::jsonb,
                       repeat('7',64),'62000000-0000-0000-0000-000000000006',
                       'OOS_PAIR_INCOMPLETE',repeat('8',64))
                    """
                )
            )
    finally:
        await engine.dispose()


async def _assert_external_guard_serializes(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            await connection.execute(
                text(
                    "SELECT pg_advisory_xact_lock("
                    "hashtextextended('nexus:agent-version-evaluation:v1',0))"
                )
            )
            insert_task = asyncio.create_task(_add_external_id(database_url))
            await asyncio.sleep(0.05)
            assert not insert_task.done()
            await transaction.commit()
            with pytest.raises(DBAPIError, match="cannot have an external id"):
                await insert_task
    finally:
        await engine.dispose()


async def _assert_cycle_guard_serializes(database_url: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            transaction = await connection.begin()
            await connection.execute(
                text(
                    "SELECT pg_advisory_xact_lock("
                    "hashtextextended('nexus:agent-version-evaluation:v1',0))"
                )
            )
            insert_task = asyncio.create_task(_add_cycle(database_url, frozen_slot=False))
            await asyncio.sleep(0.05)
            assert not insert_task.done()
            await transaction.commit()
            with pytest.raises(DBAPIError, match="cycle is outside frozen scope"):
                await insert_task
    finally:
        await engine.dispose()


def test_0062_rejects_non_xnys_evaluation_slot(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(ENGINE_ROOT / "alembic.ini"))
    command.upgrade(config, MIGRATION_HEAD)

    with pytest.raises(DBAPIError, match="evaluation window is invalid"):
        asyncio.run(_seed_evaluation(temporary_database_url, invalid_slot=True))


def test_0062_downgrade_rejects_persisted_evaluation(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(ENGINE_ROOT / "alembic.ini"))
    command.upgrade(config, MIGRATION_HEAD)
    asyncio.run(_seed_evaluation(temporary_database_url))

    asyncio.run(_assert_external_guard_serializes(temporary_database_url))
    asyncio.run(_assert_cycle_guard_serializes(temporary_database_url))
    asyncio.run(_close_evaluation(temporary_database_url))
    with pytest.raises(DBAPIError, match="cycle is outside frozen scope"):
        asyncio.run(_add_cycle(temporary_database_url, frozen_slot=True))
    with pytest.raises(DBAPIError, match="paper fact is append-only"):
        asyncio.run(_mutate_evaluation(temporary_database_url))
    with pytest.raises(DBAPIError, match="agent version evaluation facts prevent downgrade"):
        command.downgrade(config, PREVIOUS_HEAD)
