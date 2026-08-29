"""Phase 2 strategy-evolution migration, seed, backfill, and downgrade gates."""

from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

ALEMBIC_CONFIG = Path(__file__).parents[2] / "alembic.ini"
BASELINE_EXPRESSION_HASH = "f643694ad6d31f0dd504061fbd06068a93f9fb679aba9ba5b75337e2e169d13d"
EVOLUTION_TABLES = {
    "strategy_rnd_runs",
    "strategy_rnd_outputs",
    "strategy_evolution_policies",
    "strategy_promotion_policies",
    "strategy_research_lineages",
    "strategy_experiment_families",
    "strategy_experiments",
    "strategy_experiment_events",
    "strategy_experiment_attempts",
    "strategy_experiment_state_projections",
    "strategy_champion_comparisons",
    "strategy_promotion_decisions",
    "strategy_shadow_groups",
    "strategy_shadow_legs",
    "strategy_shadow_advance_attempts",
    "strategy_shadow_session_authorities",
    "strategy_shadow_session_price_inputs",
    "strategy_shadow_leg_states",
    "strategy_shadow_orders",
    "strategy_shadow_fills",
    "strategy_shadow_checkpoints",
    "strategy_shadow_checkpoint_metrics",
    "strategy_shadow_finalizations",
    "strategy_registry_events",
    "strategy_registry_heads",
}
EVOLUTION_AUTHORITY_0023_TABLES = {
    "strategy_research_lineage_projections",
    "strategy_experiment_candidate_projections",
    "strategy_candidate_validation_results",
    "strategy_oos_exposures",
    "strategy_experiment_oos_exposure_links",
    "strategy_oos_evidence_links",
    "strategy_registry_activations",
}


def _config() -> Config:
    return Config(str(ALEMBIC_CONFIG))


async def _rows(database_url: str, statement: str) -> list[dict[str, object]]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            result = await connection.execute(text(statement))
            return [dict(row) for row in result.mappings()]
    finally:
        await engine.dispose()


async def _execute(database_url: str, statement: str, parameters=None) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(statement), parameters or {})
    finally:
        await engine.dispose()


async def _table_names(database_url: str) -> set[str]:
    rows = await _rows(
        database_url,
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
    )
    return {str(row["table_name"]) for row in rows}


async def _columns(database_url: str, table_name: str) -> dict[str, str]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            result = await connection.execute(
                text(
                    "SELECT column_name, is_nullable FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name=:table_name"
                ),
                {"table_name": table_name},
            )
            return {str(row.column_name): str(row.is_nullable) for row in result}
    finally:
        await engine.dispose()


async def _seed_legacy_decision(database_url: str) -> UUID:
    decision_id = UUID("019d0000-0000-7000-8000-000000002201")
    values = {
        "source_id": UUID("019d0000-0000-7000-8000-000000002202"),
        "calendar_id": UUID("019d0000-0000-7000-8000-000000002203"),
        "universe_id": UUID("019d0000-0000-7000-8000-000000002204"),
        "snapshot_id": UUID("019d0000-0000-7000-8000-000000002205"),
        "quality_id": UUID("019d0000-0000-7000-8000-000000002206"),
        "decision_id": decision_id,
        "paper_account_id": UUID("d31672c0-3922-5ebd-8fdb-cd3174266731"),
        "portfolio_id": UUID("ee2ff673-eaf6-5076-bbab-f4c9cedfc74e"),
    }
    statements = (
        "INSERT INTO source_policies (id,key,version,schema_version,payload) VALUES "
        "(:source_id,'phase2-backfill','v1','v1','{}'::jsonb)",
        "INSERT INTO exchange_calendar_authorities "
        "(id,exchange,version,source,range_start,range_end,query_parameters,"
        "completeness_status,content_hash,published_at) VALUES "
        "(:calendar_id,'XNYS','phase2-backfill','fixture','2026-08-01','2026-08-31',"
        "'{}'::jsonb,'complete',repeat('a',64),'2026-08-21T20:00:00Z')",
        "INSERT INTO universe_snapshots "
        "(id,key,version,effective_from,source,content_hash,published_at) VALUES "
        "(:universe_id,'phase2-backfill','v1','2026-08-01','fixture',repeat('b',64),"
        "'2026-08-21T20:00:00Z')",
        "INSERT INTO data_snapshots "
        "(id,source,provider_object_id,revision,observed_at,available_at,received_at,"
        "schema_version,payload) VALUES (:snapshot_id,'fixture','phase2-backfill','v1',"
        "'2026-08-21T19:00:00Z','2026-08-21T19:00:00Z','2026-08-21T19:00:00Z',"
        "'v1','{}'::jsonb)",
        "INSERT INTO quality_reports (id,data_snapshot_id,status,schema_version,payload) "
        "VALUES (:quality_id,:snapshot_id,'PASS','v1','{}'::jsonb)",
        "INSERT INTO strategy_decisions "
        "(id,account_id,portfolio_id,strategy_id,strategy_version,decision_session,"
        "decision_time,execution_session,source_policy_id,calendar_authority_id,"
        "universe_snapshot_id,quality_report_id,portfolio_projection_hash,authority_hash,"
        "strategy_parameters_hash,decision_hash,cash_weight,reason) VALUES "
        "(:decision_id,:paper_account_id,:portfolio_id,'baseline','baseline_v1','2026-08-21',"
        "'2026-08-21T20:00:00Z','2026-08-24',:source_id,:calendar_id,:universe_id,"
        ":quality_id,repeat('c',64),repeat('d',64),repeat('e',64),repeat('f',64),1,"
        "'WEEKLY_REBALANCE')",
    )
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            for statement in statements:
                await connection.execute(text(statement), values)
    finally:
        await engine.dispose()
    return decision_id


def test_0023_precedes_the_broker_simulation_head() -> None:
    script = ScriptDirectory.from_config(_config())

    assert script.get_heads() == ["0027_fosun_market_authority"]
    revision = script.get_revision("0023_evolution_authority")
    assert revision is not None
    assert revision.down_revision == "0022_strategy_evolution"


def test_0023_adds_authority_facts_and_round_trips(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = _config()
    command.upgrade(config, "0022_strategy_evolution")
    assert EVOLUTION_AUTHORITY_0023_TABLES.isdisjoint(
        asyncio.run(_table_names(temporary_database_url))
    )

    command.upgrade(config, "0023_evolution_authority")

    assert EVOLUTION_AUTHORITY_0023_TABLES <= asyncio.run(
        _table_names(temporary_database_url)
    )
    event_columns = asyncio.run(
        _columns(temporary_database_url, "strategy_experiment_events")
    )
    family_columns = asyncio.run(
        _columns(temporary_database_url, "strategy_experiment_families")
    )
    assert event_columns["experiment_id"] == "YES"
    assert family_columns["baseline_backtest_run_id"] == "YES"
    constraints = asyncio.run(
        _rows(
            temporary_database_url,
            "SELECT conname,pg_get_constraintdef(oid) AS definition "
            "FROM pg_constraint WHERE conrelid IN "
            "('strategy_oos_exposures'::regclass,"
            "'strategy_registry_activations'::regclass)",
        )
    )
    names = {str(row["conname"]) for row in constraints}
    assert {
        "uq_strategy_oos_exposures_lineage_expression",
        "uq_strategy_oos_exposures_lineage_ordinal",
        "uq_strategy_registry_activations_event",
        "uq_strategy_registry_activations_hash",
    } <= names
    definitions = {
        str(row["conname"]): str(row["definition"]) for row in constraints
    }
    assert definitions["uq_strategy_oos_exposures_lineage_expression"] == (
        "UNIQUE (research_lineage_id, expression_hash)"
    )

    command.downgrade(config, "0022_strategy_evolution")
    assert EVOLUTION_AUTHORITY_0023_TABLES.isdisjoint(
        asyncio.run(_table_names(temporary_database_url))
    )
    assert "experiment_id" not in asyncio.run(
        _columns(temporary_database_url, "strategy_experiment_events")
    )
    assert "baseline_backtest_run_id" not in asyncio.run(
        _columns(temporary_database_url, "strategy_experiment_families")
    )
    command.upgrade(config, "0023_evolution_authority")
    assert EVOLUTION_AUTHORITY_0023_TABLES <= asyncio.run(
        _table_names(temporary_database_url)
    )


def test_0023_downgrade_refuses_lineage_trial_facts(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = _config()
    command.upgrade(config, "0023_evolution_authority")
    asyncio.run(
        _execute(
            temporary_database_url,
            "INSERT INTO strategy_research_lineages "
            "(id,lineage_key,evolution_policy_id,cumulative_oos_trial_count,status) "
            "SELECT '019d0000-0000-7000-8000-000000002310','downgrade-trial-fact',id,0,"
            "'active' FROM strategy_evolution_policies "
            "WHERE policy_kind='evolution_search_space'",
        )
    )
    asyncio.run(
        _execute(
            temporary_database_url,
            "INSERT INTO strategy_research_lineage_projections "
            "(lineage_id,cumulative_oos_trial_count,version) VALUES "
            "('019d0000-0000-7000-8000-000000002310',1,1)",
        )
    )

    with pytest.raises(DBAPIError, match="authority facts prevent downgrade") as captured:
        command.downgrade(config, "0022_strategy_evolution")

    assert captured.value.orig.sqlstate == "23514"


def test_0022_creates_exact_authorities_genesis_and_round_trips(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = _config()
    command.upgrade(config, "0021_manual_job_retry_allowance")
    assert EVOLUTION_TABLES.isdisjoint(asyncio.run(_table_names(temporary_database_url)))

    command.upgrade(config, "0022_strategy_evolution")

    assert EVOLUTION_TABLES <= asyncio.run(_table_names(temporary_database_url))
    decision_columns = asyncio.run(_columns(temporary_database_url, "strategy_decisions"))
    assert decision_columns["strategy_definition_id"] == "NO"
    assert decision_columns["registry_event_id"] == "NO"
    assert decision_columns["champion_generation"] == "NO"
    genesis = asyncio.run(
        _rows(
            temporary_database_url,
            "SELECT h.channel,h.active_generation,h.event_sequence,h.version,"
            "h.auto_promotion_paused,d.strategy_version,d.expression_hash,e.event_type,"
            "e.sequence,e.champion_generation,e.previous_hash,e.event_hash "
            "FROM strategy_registry_heads h JOIN strategy_definitions d "
            "ON d.id=h.active_definition_id JOIN strategy_registry_events e "
            "ON e.id=h.last_event_id",
        )
    )
    event_hash = str(genesis[0].pop("event_hash"))
    assert genesis == [
        {
            "channel": "internal_paper",
            "active_generation": 1,
            "event_sequence": 1,
            "version": 1,
            "auto_promotion_paused": False,
            "strategy_version": "baseline_v1",
            "expression_hash": BASELINE_EXPRESSION_HASH,
            "event_type": "GENESIS",
            "sequence": 1,
            "champion_generation": 1,
            "previous_hash": "0" * 64,
        }
    ]
    assert len(event_hash) == 64
    constraints = asyncio.run(
        _rows(
            temporary_database_url,
            "SELECT conname FROM pg_constraint "
            "WHERE conrelid='strategy_decisions'::regclass",
        )
    )
    names = {str(row["conname"]) for row in constraints}
    assert {
        "uq_strategy_decisions_account_session",
        "fk_strategy_decisions_strategy_definition",
        "fk_strategy_decisions_registry_event",
        "ck_strategy_decisions_strategy_content_addressed",
    } <= names
    assert "uq_strategy_decisions_account_version_session" not in names
    assert "ck_strategy_decisions_strategy_fixed" not in names
    policies = asyncio.run(
        _rows(
            temporary_database_url,
            "SELECT policy_kind,version,content_hash FROM strategy_evolution_policies "
            "UNION ALL SELECT 'promotion',version,content_hash "
            "FROM strategy_promotion_policies ORDER BY policy_kind",
        )
    )
    assert policies == [
        {
            "policy_kind": "candidate_budget",
            "version": "candidate_budget_policy_v1",
            "content_hash": "3e0330739f660c68b6a0999177211c567946eaa0e396969e32c061886c8b1639",
        },
        {
            "policy_kind": "evolution_search_space",
            "version": "evolution_search_space_v1",
            "content_hash": "4e3b19a0cb697b59e0692bc8570b72a236c79d926d53405c2d02f29215f674da",
        },
        {
            "policy_kind": "promotion",
            "version": "promotion_policy_v1",
            "content_hash": "e94f912ee74d7859eb6a7053b03f7200ba2ea6f346bfd6732b9907dcceed3343",
        },
    ]

    command.downgrade(config, "0021_manual_job_retry_allowance")
    assert EVOLUTION_TABLES.isdisjoint(asyncio.run(_table_names(temporary_database_url)))
    assert "strategy_definition_id" not in asyncio.run(
        _columns(temporary_database_url, "strategy_decisions")
    )
    command.upgrade(config, "0022_strategy_evolution")
    assert EVOLUTION_TABLES <= asyncio.run(_table_names(temporary_database_url))


def test_0022_backfills_legacy_forward_decision_to_genesis(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = _config()
    command.upgrade(config, "0021_manual_job_retry_allowance")
    decision_id = asyncio.run(_seed_legacy_decision(temporary_database_url))

    command.upgrade(config, "0022_strategy_evolution")

    rows = asyncio.run(
        _rows(
            temporary_database_url,
            "SELECT sd.id,sd.champion_generation,d.strategy_version,e.event_type "
            "FROM strategy_decisions sd JOIN strategy_definitions d "
            "ON d.id=sd.strategy_definition_id JOIN strategy_registry_events e "
            "ON e.id=sd.registry_event_id WHERE sd.id='" + str(decision_id) + "'",
        )
    )
    assert rows == [
        {
            "id": decision_id,
            "champion_generation": 1,
            "strategy_version": "baseline_v1",
            "event_type": "GENESIS",
        }
    ]


def test_0022_downgrade_refuses_nonseed_phase2_facts(
    monkeypatch: pytest.MonkeyPatch,
    temporary_database_url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = _config()
    command.upgrade(config, "0022_strategy_evolution")
    asyncio.run(
        _execute(
            temporary_database_url,
            "INSERT INTO strategy_evolution_policies "
            "(id,policy_kind,version,document,content_hash,published_at) VALUES "
            "('019d0000-0000-7000-8000-000000002299','candidate_budget','forged_v2',"
            "'{}'::jsonb,repeat('a',64),'2026-08-27T00:00:00Z')",
        )
    )

    with pytest.raises(DBAPIError, match="phase 2 facts prevent downgrade"):
        command.downgrade(config, "0021_manual_job_retry_allowance")
