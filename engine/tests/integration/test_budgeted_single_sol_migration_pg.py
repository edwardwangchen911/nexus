"""Preserve nonempty old facts; never downgrade away a new independent owner."""

import asyncio

import pytest
from alembic import command
from alembic.config import Config
from helpers.budgeted_single_readback import facts
from integration import test_budgeted_single_sol_pg as v5
from integration import test_single_sol_request_pg as legacy
from nexus.ops import manual_paper
from nexus.ops.manual_paper_host import dispatch_pure_research_request
from nexus.paper.manual_request import read_manual_request
from nexus.paper.single_sol_owner import binding_for
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.session import session_factory
from nexus.workbench.research_summary import research_candidates
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError


@pytest.mark.asyncio
async def test_nonempty_v4_roundtrip_and_v5_downgrade_fence(
    migrated_database_url, tmp_path, monkeypatch, capsys
):
    settings, store, ids = await v5.completed_predecessor(
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
            old_summary = await research_candidates(
                db, old, old_view, read_at=await db.scalar(text("SELECT clock_timestamp()"))
            )
            assert len(old_summary) == 1 and old_summary[0].read_status == "verified"
            assert old_summary[0].run_id == old_view.research.members[0].run_id
            calendar = binding_for(old).calendar_id
            before = await facts(db)
        await asyncio.to_thread(command.downgrade, config, "0116_single_sol_request")
        async with factory() as db:
            assert (
                await db.scalar(text("SELECT version_num FROM alembic_version"))
                == "0116_single_sol_request"
            )
            assert await facts(db) == before
            assert (
                await read_manual_request(db, await db.get(ManualPaperRequest, old_id)) == old_view
            )
        await asyncio.to_thread(command.upgrade, config, "head")
        async with factory() as db:
            assert await facts(db) == before

        async def call(*args):
            return await manual_paper.run(settings, manual_paper.parser().parse_args(list(args)))

        policy = ("--admission-policy", "shared-budget")
        offer = await call("inspect-single-sol-intent", "--instrument-id", str(ids[0]), *policy)
        receipt = await call(
            "start-single-sol",
            "--instrument-id",
            str(ids[0]),
            "--idempotency-key",
            "roundtrip-v5",
            "--expected-intent-hash",
            offer.intent_hash,
            *policy,
        )
        sent, _ = legacy.install_sol_http(monkeypatch, store)
        assert await dispatch_pure_research_request(
            settings, request_id=receipt.request_id, calendar_authority_id=calendar
        )
        assert sent == ["research", "critique", "synthesis"]
        async with factory() as db:
            preserved = await facts(db)
            row = await db.get(ManualPaperRequest, receipt.request_id)
            assert (await read_manual_request(db, row)).status == "completed"
        with pytest.raises(DBAPIError, match="BUDGETED_SINGLE_SOL_DOWNGRADE_FORBIDDEN"):
            await asyncio.to_thread(command.downgrade, config, "0116_single_sol_request")
        async with factory() as db:
            assert (
                await db.scalar(text("SELECT version_num FROM alembic_version"))
                == CURRENT_MIGRATION_HEAD
            )
            assert await facts(db) == preserved
            assert (
                await read_manual_request(db, await db.get(ManualPaperRequest, old_id)) == old_view
            )
    finally:
        await factory.kw["bind"].dispose()
