"""PostgreSQL is the final authority for Phase 2 immutability and concurrency."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

AUTHORITY_TABLES = {
    "strategy_rnd_runs",
    "strategy_rnd_outputs",
    "strategy_evolution_policies",
    "strategy_promotion_policies",
    "strategy_research_lineages",
    "strategy_experiment_families",
    "strategy_experiments",
    "strategy_experiment_events",
    "strategy_experiment_attempts",
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
    "strategy_candidate_validation_results",
    "strategy_oos_exposures",
    "strategy_experiment_oos_exposure_links",
    "strategy_oos_evidence_links",
    "strategy_registry_activations",
}


async def _execute(database_url: str, statement: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            await connection.execute(text(statement))
    finally:
        await engine.dispose()


async def _immutable_trigger_events(database_url: str) -> dict[str, set[str]]:
    engine = create_async_engine(database_url)
    try:
        async with engine.connect() as connection:
            rows = (
                await connection.execute(
                    text(
                        "SELECT event_object_table,event_manipulation "
                        "FROM information_schema.triggers "
                        "WHERE trigger_schema='public' AND trigger_name = "
                        "'trg_' || event_object_table || '_immutable'"
                    )
                )
            ).mappings()
            result: dict[str, set[str]] = {}
            for row in rows:
                result.setdefault(str(row["event_object_table"]), set()).add(
                    str(row["event_manipulation"])
                )
            return result
    finally:
        await engine.dispose()


def test_every_phase2_authority_has_update_delete_database_guards(
    migrated_database_url: str,
) -> None:
    events = asyncio.run(_immutable_trigger_events(migrated_database_url))

    assert {table: events.get(table) for table in AUTHORITY_TABLES} == {
        table: {"UPDATE", "DELETE"} for table in AUTHORITY_TABLES
    }


@pytest.mark.parametrize(
    "statement",
    (
        "UPDATE strategy_evolution_policies SET version='changed'",
        "DELETE FROM strategy_evolution_policies",
        "UPDATE strategy_registry_events SET event_hash=repeat('f',64)",
        "DELETE FROM strategy_registry_events",
    ),
)
def test_seeded_phase2_authorities_reject_update_and_delete(
    migrated_database_url: str,
    statement: str,
) -> None:
    with pytest.raises(DBAPIError) as captured:
        asyncio.run(_execute(migrated_database_url, statement))
    assert captured.value.orig.sqlstate == "23514"


def test_registry_head_rejects_generation_regression(migrated_database_url: str) -> None:
    with pytest.raises(DBAPIError) as captured:
        asyncio.run(
            _execute(
                migrated_database_url,
                "UPDATE strategy_registry_heads SET active_generation=0,version=2 "
                "WHERE channel='internal_paper'",
            )
        )
    assert captured.value.orig.sqlstate == "23514"


def test_registry_head_rejects_active_definition_swap_without_activation(
    migrated_database_url: str,
) -> None:
    alternate_id = "019d0000-0000-7000-8000-000000003901"
    asyncio.run(
        _execute(
            migrated_database_url,
            "INSERT INTO strategy_definitions "
            "(id,strategy_version,schema_version,expression_hash,document,lifecycle,"
            "published_at,created_at) SELECT "
            f"'{alternate_id}','registry_guard_alternate',schema_version,repeat('e',64),"
            "document,'challenger',published_at,created_at FROM strategy_definitions "
            "WHERE strategy_version='baseline_v1'",
        )
    )
    with pytest.raises(DBAPIError) as captured:
        asyncio.run(
            _execute(
                migrated_database_url,
                "UPDATE strategy_registry_heads "
                f"SET active_definition_id='{alternate_id}',version=2 "
                "WHERE channel='internal_paper'",
            )
        )
    assert captured.value.orig.sqlstate == "23514"


@pytest.mark.parametrize(
    "assignment",
    (
        "auto_promotion_paused=true",
        "pending_event_id=last_event_id,pending_effective_session='2026-10-02'",
    ),
)
def test_registry_head_rejects_control_or_pending_state_without_matching_event(
    migrated_database_url: str,
    assignment: str,
) -> None:
    with pytest.raises(DBAPIError) as captured:
        asyncio.run(
            _execute(
                migrated_database_url,
                f"UPDATE strategy_registry_heads SET {assignment},version=2 "
                "WHERE channel='internal_paper'",
            )
        )
    assert captured.value.orig.sqlstate == "23514"


def test_experiment_projection_rejects_trial_count_regression(
    migrated_database_url: str,
) -> None:
    asyncio.run(_insert_lineage_family(migrated_database_url, suffix="21"))
    family_id = "019d0000-0000-7000-8000-000000003021"
    asyncio.run(
        _execute(
            migrated_database_url,
            "UPDATE strategy_research_lineage_projections "
            "SET cumulative_oos_trial_count=5,version=2",
        )
    )
    asyncio.run(
        _execute(
            migrated_database_url,
            "UPDATE strategy_experiment_state_projections "
            f"SET cumulative_oos_trial_count=5,version=2 WHERE family_id='{family_id}'",
        )
    )

    with pytest.raises(DBAPIError) as captured:
        asyncio.run(
            _execute(
                migrated_database_url,
                "UPDATE strategy_experiment_state_projections "
                f"SET cumulative_oos_trial_count=4,version=3 WHERE family_id='{family_id}'",
            )
        )
    assert captured.value.orig.sqlstate == "23514"


async def _insert_lineage_family(database_url: str, *, suffix: str) -> None:
    engine = create_async_engine(database_url)
    try:
        async with engine.begin() as connection:
            baseline_id = await connection.scalar(
                text("SELECT id FROM strategy_definitions WHERE strategy_version='baseline_v1'")
            )
            policy_id = await connection.scalar(
                text(
                    "SELECT id FROM strategy_evolution_policies "
                    "WHERE policy_kind='evolution_search_space'"
                )
            )
            lineage_id = "019d0000-0000-7000-8000-000000003001"
            await connection.execute(
                text(
                    "INSERT INTO strategy_research_lineages "
                    "(id,lineage_key,evolution_policy_id,cumulative_oos_trial_count,status) "
                    "VALUES (:id,'lineage-v1',:policy_id,0,'active') ON CONFLICT DO NOTHING"
                ),
                {"id": lineage_id, "policy_id": policy_id},
            )
            await connection.execute(
                text(
                    "INSERT INTO strategy_research_lineage_projections "
                    "(lineage_id,cumulative_oos_trial_count,version) "
                    "VALUES (:id,0,1) ON CONFLICT DO NOTHING"
                ),
                {"id": lineage_id},
            )
            family_id = f"019d0000-0000-7000-8000-0000000030{suffix}"
            await connection.execute(
                text(
                    "INSERT INTO strategy_experiment_families "
                    "(id,research_lineage_id,parent_strategy_definition_id,"
                    "champion_generation,admission_status,candidate_count,idempotency_key,"
                    "authority_hash,protocol_hash,code_hash) VALUES "
                    "(:id,:lineage_id,:baseline_id,1,'ADMITTED',1,:key,repeat('a',64),"
                    "repeat('b',64),repeat('c',64))"
                ),
                {
                    "id": family_id,
                    "lineage_id": lineage_id,
                    "baseline_id": baseline_id,
                    "key": f"family-{suffix}",
                },
            )
            experiment_id = f"019d0000-0000-7000-8000-0000000031{suffix}"
            event_id = f"019d0000-0000-7000-8000-0000000032{suffix}"
            await connection.execute(
                text(
                    "INSERT INTO strategy_experiments "
                    "(id,family_id,candidate_strategy_definition_id,candidate_ordinal,"
                    "candidate_delta,candidate_hash,expression_hash,status) VALUES "
                    "(:experiment_id,:family_id,:baseline_id,1,'{}'::jsonb,repeat('d',64),"
                    "repeat('e',64),'ADMITTED')"
                ),
                {
                    "experiment_id": experiment_id,
                    "family_id": family_id,
                    "baseline_id": baseline_id,
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO strategy_experiment_events "
                    "(id,family_id,experiment_id,sequence,event_type,idempotency_key,"
                    "actor_kind,occurred_at,payload,previous_hash,event_hash) VALUES "
                    "(:event_id,:family_id,:experiment_id,1,'ADMITTED',:event_key,'service',"
                    "'2026-08-27T00:00:00Z','{}'::jsonb,repeat('0',64),repeat('1',64))"
                ),
                {
                    "event_id": event_id,
                    "family_id": family_id,
                    "experiment_id": experiment_id,
                    "event_key": f"family-event-{suffix}",
                },
            )
            await connection.execute(
                text(
                    "INSERT INTO strategy_experiment_candidate_projections "
                    "(experiment_id,family_id,current_state,event_sequence,version) "
                    "VALUES (:experiment_id,:family_id,'ADMITTED',1,1)"
                ),
                {"experiment_id": experiment_id, "family_id": family_id},
            )
            await connection.execute(
                text(
                    "INSERT INTO strategy_experiment_state_projections "
                    "(family_id,research_lineage_id,current_state,event_sequence,version) "
                    "VALUES (:id,:lineage_id,'ADMITTED',1,1)"
                ),
                {"id": family_id, "lineage_id": lineage_id},
            )
    finally:
        await engine.dispose()


def test_only_one_nonterminal_family_per_lineage(migrated_database_url: str) -> None:
    asyncio.run(_insert_lineage_family(migrated_database_url, suffix="01"))

    with pytest.raises(DBAPIError):
        asyncio.run(_insert_lineage_family(migrated_database_url, suffix="02"))


def test_experiment_event_sequence_cannot_skip(migrated_database_url: str) -> None:
    asyncio.run(_insert_lineage_family(migrated_database_url, suffix="11"))
    family_id = "019d0000-0000-7000-8000-000000003011"
    with pytest.raises(DBAPIError) as captured:
        asyncio.run(
            _execute(
                migrated_database_url,
                "INSERT INTO strategy_experiment_events "
                "(id,family_id,sequence,event_type,idempotency_key,actor_kind,occurred_at,payload,"
                "previous_hash,event_hash) VALUES "
                f"('019d0000-0000-7000-8000-000000003013','{family_id}',3,'BACKTEST_RUNNING',"
                "'event-3','service','2026-08-27T00:01:00Z','{}'::jsonb,repeat('1',64),"
                "repeat('2',64))",
            )
        )
    assert captured.value.orig.sqlstate == "23514"
