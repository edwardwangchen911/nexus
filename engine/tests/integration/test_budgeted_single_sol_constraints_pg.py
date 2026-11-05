"""Exercise migrated database guards without relying on Python admission rejection."""

import copy
import json
import re

import pytest
from helpers.single_sol_maintenance_image import frozen_facts
from integration.test_budgeted_single_sol_pg import completed_predecessor
from nexus.ops.manual_paper_host import claim_configured_manual_request
from nexus.paper.research_request import prepare_research_request
from nexus.paper.single_sol_owner import binding_for
from nexus.paper.single_sol_research import single_sol_authority, start_single_sol
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.session import session_factory
from sqlalchemy import event, select, text
from sqlalchemy.exc import DBAPIError


def changed_insert(statement, parameters, table, column, change):
    match = re.match(r"INSERT INTO " + table + r" \(([^)]+)\) VALUES", statement)
    if match is None:
        return parameters, False
    columns = [s.strip() for s in match[1].split(",")]
    values = list(parameters)
    index = columns.index(column)
    values[index] = change(values[index])
    return tuple(values), True


@pytest.mark.asyncio
async def test_migrated_v5_guards_reject_mixed_owners_at_database_commit(
    migrated_database_url, tmp_path, monkeypatch, capsys
):
    settings, store, ids = await completed_predecessor(
        migrated_database_url, tmp_path, monkeypatch, capsys
    )
    factory = session_factory(settings)
    engine = factory.kw["bind"]
    statements = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith(("INSERT ", "UPDATE ", "DELETE ")):
            statements.append((statement, copy.deepcopy(parameters)))

    try:
        async with factory() as db:
            old = (await db.scalars(select(ManualPaperRequest))).one()
            calendar = binding_for(old).calendar_id
            donor = (
                await db.execute(text("SELECT run_id,job_id FROM research_request_members"))
            ).one()
            old_scope = old.id
            definitions = {
                name: await db.scalar(
                    text("SELECT pg_get_functiondef(to_regprocedure(:name))"), {"name": name + "()"}
                )
                for name in ("nexus_research_manifest_complete", "research_stage_route_guard")
            }
            assert all(definitions.values())
            (tmp_path / "migrated-guard-functions.json").write_text(json.dumps(definitions))
        before = await frozen_facts(factory)
        event.listen(engine.sync_engine, "before_cursor_execute", capture)
        try:
            async with factory() as db:
                offer = await single_sol_authority(
                    db, settings, instrument_id=ids[0], shared_budget=True
                )
                request = await start_single_sol(
                    db,
                    settings,
                    instrument_id=ids[0],
                    idempotency_key="constraint-source-v5",
                    expected_intent_hash=offer.intent_hash,
                    shared_budget=True,
                )
                _, claimed = await claim_configured_manual_request(
                    db, settings, request_id=request.request_id, calendar_authority_id=calendar
                )
                assert claimed
                db.info["sec_exhibit_store"] = store
                result = await prepare_research_request(db, settings, request_id=request.request_id)
                assert result.status == "active"
                await db.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
                await db.rollback()
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", capture)
        assert await frozen_facts(factory) == before

        def profile_value(field, value):
            def replace(raw):
                profile = json.loads(raw)
                profile["single_sol"][field] = value
                return json.dumps(profile)

            return replace

        cases = [
            (
                "v5-with-v4-actor",
                "agent_research_runs",
                "actor_id",
                lambda _: "local-single-sol-research-v1",
            ),
            (
                "v5-with-v4-key",
                "agent_research_runs",
                "idempotency_key",
                lambda v: v.replace("budgeted-single-sol:", "single-sol:"),
            ),
            ("v4-with-v5-owner", "manual_paper_requests", "contract_version", lambda _: 4),
            ("foreign-run", "research_stage_route", "run_id", lambda _: donor.run_id),
            ("foreign-job", "research_stage_route", "job_id", lambda _: donor.job_id),
            ("foreign-scope", "research_stage_route", "scope_id", lambda _: old_scope),
            (
                "wrong-protection-hash",
                "manual_paper_requests",
                "research_profile",
                profile_value("protection_context_hash", "0" * 64),
            ),
            (
                "wrong-budget-day",
                "manual_paper_requests",
                "research_profile",
                profile_value("budget_day", "2000-01-01"),
            ),
        ]
        # Only the disposable database receives the captured synthetic transaction.
        # No Python producer/validator runs in these probes; all DB guards stay enabled.
        results = {}
        for name, table, column, change in cases:
            changed = 0
            at_deferred_check = False
            with pytest.raises(
                DBAPIError, match="preparation route binding invalid|RESEARCH_MANIFEST_INVALID"
            ) as rejected:
                async with engine.begin() as conn:
                    for statement, parameters in statements:
                        values, hit = changed_insert(statement, parameters, table, column, change)
                        changed += hit
                        await conn.exec_driver_sql(statement, values)
                    at_deferred_check = True
                    await conn.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
            assert changed == 1, name
            if name in {"wrong-protection-hash", "wrong-budget-day"}:
                assert at_deferred_check, name
            results[name] = {
                "deferred_check_reached": at_deferred_check,
                "guard": "manifest"
                if "RESEARCH_MANIFEST_INVALID" in str(rejected.value)
                else "route",
            }
            assert await frozen_facts(factory) == before, name
        # Positive control reaches COMMIT with the same statements and all guards intact.
        async with engine.begin() as conn:
            for statement, parameters in statements:
                await conn.exec_driver_sql(statement, parameters)
            await conn.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
        async with factory() as db:
            assert await db.scalar(text("SELECT count(*) FROM manual_paper_requests")) == 2
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_scopes")) == 1
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_credits")) == 0
        (tmp_path / "constraint-results.json").write_text(
            json.dumps(
                {
                    "negative_cases": results,
                    "positive_control_committed": True,
                    "all_original_facts_preserved_on_rejection": True,
                }
            )
        )
    finally:
        await engine.dispose()
