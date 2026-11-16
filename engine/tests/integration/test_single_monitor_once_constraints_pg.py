"""Database-level controls for the one-time single-stock monitoring root.

Only disposable PostgreSQL receives synthetic normal-entry transactions. No live
model transport is used; the predecessor fixture installs its mock transport.
"""

import copy
import json
import re
from hashlib import sha256
from uuid import uuid4

import pytest
from helpers.single_sol_maintenance_image import frozen_facts
from integration.test_budgeted_single_sol_pg import completed_predecessor
from nexus.ops import manual_paper
from nexus.ops.manual_paper_host import claim_configured_manual_request
from nexus.paper.research_request import prepare_research_request
from nexus.paper.single_sol_owner import binding_for
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.session import session_factory
from sqlalchemy import event, select, text
from sqlalchemy.exc import DBAPIError


def _changed_insert(statement, parameters, table, column, change):
    match = re.match(r"INSERT INTO " + table + r" \(([^)]+)\) VALUES", statement)
    if match is None:
        return parameters, False
    columns = [s.strip() for s in match[1].split(",")]
    values = list(parameters)
    index = columns.index(column)
    old = values[index]
    values[index] = change(old)
    return tuple(values), values[index] != old


def _profile(field, value):
    def change(raw):
        profile = json.loads(raw)
        profile["single_sol"][field] = value
        return json.dumps(profile)

    return change


def _context(field, value):
    def change(raw):
        payload = json.loads(raw)
        if "independent_monitor_context" not in payload:
            return raw
        payload["independent_monitor_context"][field] = value
        return json.dumps(payload)

    return change


@pytest.mark.asyncio
async def test_single_monitor_database_guards_and_permanent_account_uniqueness(
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

    async def call(*args):
        return await manual_paper.run(settings, manual_paper.parser().parse_args(list(args)))

    try:
        async with factory() as db:
            old = (await db.scalars(select(ManualPaperRequest))).one()
            calendar = binding_for(old).calendar_id
            original_batch = (
                await db.execute(
                    text(
                        "SELECT candidate_instrument_ids FROM agent_paper_candidate_batches "
                        "WHERE id=:id"
                    ),
                    {"id": old.candidate_batch_id},
                )
            ).scalar_one()
            unchanged_guards = {
                signature: await db.scalar(
                    text("SELECT pg_get_functiondef(to_regprocedure(:s))"), {"s": signature}
                )
                for signature in (
                    "public.single_sol_increment_valid(date,uuid,uuid,uuid,uuid,numeric,uuid)",
                    "public.single_sol_credit_valid()",
                )
            }
        assert len(original_batch) > 1
        before = await frozen_facts(factory)
        event.listen(engine.sync_engine, "before_cursor_execute", capture)
        try:
            # The CLI preview is read-only; its normal producer then supplies all
            # request, snapshot, reservation, member and native-route originals.
            offer = await call("inspect-single-monitor-sol-intent", "--instrument-id", str(ids[0]))
            async with factory() as db:
                from nexus.paper.single_sol_research import start_single_sol

                captured_created_at = await db.scalar(text("SELECT now()"))
                receipt = await start_single_sol(
                    db,
                    settings,
                    instrument_id=ids[0],
                    idempotency_key="db-v6-source",
                    expected_intent_hash=offer.intent_hash,
                    shared_budget=True,
                    monitoring=True,
                )
                _, claimed = await claim_configured_manual_request(
                    db, settings, request_id=receipt.request_id, calendar_authority_id=calendar
                )
                assert claimed
                db.info["sec_exhibit_store"] = store
                result = await prepare_research_request(db, settings, request_id=receipt.request_id)
                assert result.status == "active"
                await db.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
                await db.rollback()
        finally:
            event.remove(engine.sync_engine, "before_cursor_execute", capture)
        assert statements
        assert await frozen_facts(factory) == before
        # Replay server-default custody timestamps exactly as in the captured
        # transaction; preserve the product's created_at <= frozen cutoff guard.
        assert captured_created_at.tzinfo is not None
        async with engine.begin() as conn:
            await conn.exec_driver_sql(
                "CREATE OR REPLACE FUNCTION fixture_clock.now() RETURNS timestamptz "
                "LANGUAGE sql STABLE AS $$ SELECT '"
                + captured_created_at.isoformat()
                + "'::timestamptz $$"
            )
        # Prove an unchanged replay before interpreting a negative rejection.
        async with engine.connect() as conn:
            transaction = await conn.begin()
            try:
                for statement, parameters in statements:
                    await conn.exec_driver_sql(statement, parameters)
                await conn.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
            finally:
                await transaction.rollback()
        assert await frozen_facts(factory) == before
        cases = [
            (
                "old-actor",
                "agent_research_runs",
                "actor_id",
                lambda _: "local-budgeted-single-sol-v1",
            ),
            (
                "old-prefix",
                "agent_research_runs",
                "idempotency_key",
                lambda v: v.replace("single-monitor-sol:", "budgeted-single-sol:"),
            ),
            ("v3-full-batch-required", "manual_paper_requests", "contract_version", lambda _: 3),
            ("wrong-mode", "manual_paper_requests", "mode", lambda _: "research_only"),
            (
                "old-history-policy",
                "manual_paper_requests",
                "research_profile",
                _profile("history_policy", "single_sol_original_acceptances_v1"),
            ),
            (
                "wrong-auth-scope",
                "manual_paper_requests",
                "research_profile",
                _profile("authorization_scope", "future-authorization"),
            ),
            (
                "wrong-admission",
                "manual_paper_requests",
                "research_profile",
                _profile("admission_policy", "unrestricted"),
            ),
            (
                "extra-binding-field",
                "manual_paper_requests",
                "research_profile",
                _profile("unexpected", True),
            ),
            (
                "wrong-protection",
                "manual_paper_requests",
                "research_profile",
                _profile("protection_context_hash", "0" * 64),
            ),
            (
                "wrong-day",
                "manual_paper_requests",
                "research_profile",
                _profile("budget_day", "2000-01-01"),
            ),
            (
                "other-selected-member",
                "manual_paper_requests",
                "research_profile",
                _profile("instrument_id", str(ids[1])),
            ),
            (
                "wrong-context-member",
                "data_snapshots",
                "payload",
                _context("instrument_id", str(ids[1])),
            ),
            (
                "narrowed-context-batch",
                "data_snapshots",
                "payload",
                _context("candidate_ids", [str(ids[0])]),
            ),
            (
                "wrong-context-batch-hash",
                "data_snapshots",
                "payload",
                _context("batch_hash", "0" * 64),
            ),
        ]
        for name, table, column, change in cases:
            changed = 0
            with pytest.raises(DBAPIError):
                async with engine.begin() as conn:
                    for statement, parameters in statements:
                        values, hit = _changed_insert(statement, parameters, table, column, change)
                        changed += hit
                        await conn.exec_driver_sql(statement, values)
                    await conn.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
            assert changed == 1, name
            assert await frozen_facts(factory) == before, name
        # Positive control commits unchanged originals through all real PG guards.
        async with engine.begin() as conn:
            for statement, parameters in statements:
                await conn.exec_driver_sql(statement, parameters)
            await conn.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
        async with factory() as db:
            root = (
                await db.scalars(
                    select(ManualPaperRequest).where(ManualPaperRequest.contract_version == 6)
                )
            ).one()
            assert root.mode == "research_and_monitor"
            assert (
                await db.scalar(
                    text(
                        "SELECT count(*) FROM research_request_members m "
                        "JOIN research_request_manifests f ON f.id=m.manifest_id "
                        "WHERE f.request_id=:id"
                    ),
                    {"id": root.id},
                )
                == 1
            )
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_scopes")) == 1
            assert await db.scalar(text("SELECT count(*) FROM single_sol_budget_credits")) == 0
            for signature, definition in unchanged_guards.items():
                assert (
                    await db.scalar(
                        text("SELECT pg_get_functiondef(to_regprocedure(:s))"), {"s": signature}
                    )
                    == definition
                )
            duplicate = {
                column.name: getattr(root, column.name) for column in root.__table__.columns
            }
        duplicate.update(id=uuid4(), key_hash="e" * 64, request_hash="f" * 64)
        with pytest.raises(DBAPIError, match="uq_single_monitor_once_account"):
            async with factory.begin() as db:
                db.add(ManualPaperRequest(**duplicate))
                await db.flush()
        # Readback of the original identity remains possible after duplicate denial.
        async with factory() as db:
            assert (await db.get(ManualPaperRequest, root.id)).key_hash == sha256(
                b"db-v6-source"
            ).hexdigest()
            assert (
                await db.scalar(
                    text("SELECT count(*) FROM manual_paper_requests WHERE contract_version=6")
                )
                == 1
            )
    finally:
        await engine.dispose()
