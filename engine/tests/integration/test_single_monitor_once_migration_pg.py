"""Old native owners survive exact migration roundtrip; new facts fence downgrade."""

import asyncio

import pytest
from alembic import command
from alembic.config import Config
from helpers.budgeted_single_readback import facts
from integration.test_budgeted_single_sol_pg import completed_predecessor
from nexus.ops import manual_paper
from nexus.paper.manual_request import read_manual_request
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.session import session_factory
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError


async def _guard_definitions(db):
    return {
        name: await db.scalar(
            text("SELECT pg_get_functiondef(to_regprocedure(:s))"), {"s": "public." + name}
        )
        for name in (
            "nexus_research_manifest_complete()",
            "research_stage_route_guard()",
            "nexus_validate_agent_snapshot_evidence()",
            "single_sol_increment_valid(date,uuid,uuid,uuid,uuid,numeric,uuid)",
            "single_sol_credit_valid()",
        )
    }


@pytest.mark.asyncio
async def test_single_monitor_roundtrip_preserves_old_facts_and_permanent_root_fence(
    migrated_database_url, tmp_path, monkeypatch, capsys
):
    settings, _, ids = await completed_predecessor(
        migrated_database_url, tmp_path, monkeypatch, capsys
    )
    factory = session_factory(settings)
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    config = Config("engine/alembic.ini")
    try:
        async with factory() as db:
            old = (await db.scalars(select(ManualPaperRequest))).one()
            old_id = old.id
            old_view = await read_manual_request(db, old)
            before = await facts(db)
            upgraded = await _guard_definitions(db)
            assert await db.scalar(
                text("SELECT to_regclass('public.uq_single_monitor_once_account') IS NOT NULL")
            )
        await asyncio.to_thread(command.downgrade, config, "0121_sec_cash_flow_components")
        async with factory() as db:
            assert await facts(db) == before
            assert (
                await read_manual_request(db, await db.get(ManualPaperRequest, old_id)) == old_view
            )
            baseline = await _guard_definitions(db)
            assert await db.scalar(
                text("SELECT to_regclass('public.uq_single_monitor_once_account') IS NULL")
            )
        await asyncio.to_thread(command.upgrade, config, "head")
        async with factory() as db:
            assert await _guard_definitions(db) == upgraded
            assert await facts(db) == before
        # Repeat the empty-root downgrade once to prove there was no replacement
        # accumulation or accidental broadening in any of the exact old guards.
        await asyncio.to_thread(command.downgrade, config, "0121_sec_cash_flow_components")
        async with factory() as db:
            assert await _guard_definitions(db) == baseline
            assert await facts(db) == before
        await asyncio.to_thread(command.upgrade, config, "head")

        async def call(*args):
            return await manual_paper.run(settings, manual_paper.parser().parse_args(list(args)))

        offer = await call("inspect-single-monitor-sol-intent", "--instrument-id", str(ids[0]))
        receipt = await call(
            "start-single-monitor-sol",
            "--instrument-id",
            str(ids[0]),
            "--idempotency-key",
            "migration-v6-permanent",
            "--expected-intent-hash",
            offer.intent_hash,
        )
        async with factory() as db:
            root = await db.get(ManualPaperRequest, receipt.request_id)
            assert root.contract_version == 6
            # Even an unprepared request consumes the one-time root authorization.
            assert not await db.scalar(
                text("SELECT EXISTS(SELECT FROM research_request_manifests WHERE request_id=:id)"),
                {"id": root.id},
            )
            after_root = await facts(db)
        with pytest.raises(DBAPIError, match="SINGLE_MONITOR_ONCE_DOWNGRADE_FORBIDDEN"):
            await asyncio.to_thread(command.downgrade, config, "0121_sec_cash_flow_components")
        async with factory() as db:
            assert (
                await db.scalar(text("SELECT version_num FROM alembic_version"))
                == CURRENT_MIGRATION_HEAD
            )
            assert await _guard_definitions(db) == upgraded
            assert await facts(db) == after_root
            assert (
                await read_manual_request(db, await db.get(ManualPaperRequest, old_id)) == old_view
            )
    finally:
        await factory.kw["bind"].dispose()
