"""Source-level maintenance compatibility; installed-image evidence is separate."""

import pytest
from helpers.single_sol_maintenance_image import frozen_facts, refused
from integration.test_budgeted_single_sol_lifecycle_pg import prepared_owner
from nexus import worker
from nexus.agents import research_maintenance
from nexus.ops import manual_paper
from nexus.ops.manual_paper_host import dispatch_pure_research_request
from nexus.paper.manual_request import read_manual_request
from nexus.paper.research_request import finish_research_request
from nexus.paper.single_sol_owner import binding_for
from nexus.persistence.models.manual_paper import ManualPaperRequest
from sqlalchemy import text


async def refuse_new_work(factory, settings, request_id, monkeypatch):
    async with factory() as db:
        row = await db.get(ManualPaperRequest, request_id)
        binding = binding_for(row)
        before_view = await read_manual_request(db, row)
    before = await frozen_facts(factory)
    monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", True)
    await refused(
        manual_paper.run(
            settings,
            manual_paper.parser().parse_args(
                [
                    "start-single-sol",
                    "--admission-policy",
                    "shared-budget",
                    "--instrument-id",
                    str(binding.instrument_id),
                    "--idempotency-key",
                    "maintenance-must-not-create",
                    "--expected-intent-hash",
                    binding.intent_hash,
                ]
            ),
        )
    )
    await refused(
        dispatch_pure_research_request(
            settings, request_id=request_id, calendar_authority_id=binding.calendar_id
        )
    )
    await refused(
        worker.run_agent_worker_once(
            settings, job_ids=(before_view.research.members[0].job_id,), formal_stage_capacity=True
        )
    )
    async with factory() as db:
        row = await db.get(ManualPaperRequest, request_id)
        assert await read_manual_request(db, row) == before_view
    assert await frozen_facts(factory) == before
    return before_view


@pytest.mark.asyncio
async def test_v5_unused_owner_can_close_in_maintenance_without_send(
    migrated_database_url, tmp_path, monkeypatch
):
    factory, settings, _, request_id, member, _, _ = await prepared_owner(
        migrated_database_url, tmp_path, monkeypatch
    )
    try:
        await refuse_new_work(factory, settings, request_id, monkeypatch)
        async with factory.begin() as db:
            row = await db.get(ManualPaperRequest, request_id)
            result = await finish_research_request(db, row=row, cancel_unused=True)
            assert result.status == "unavailable" and result.research.members[0].usage_known
            assert await db.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 0
            assert (
                await db.scalar(
                    text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:id"),
                    {"id": member.run_id},
                )
                == "released"
            )
        closed = await frozen_facts(factory)
        monkeypatch.setattr(research_maintenance, "IMAGE_MAINTENANCE", False)
        async with factory() as db:
            row = await db.get(ManualPaperRequest, request_id)
            assert await read_manual_request(db, row) == result
        assert not await worker.run_agent_worker_once(
            settings, job_ids=(member.job_id,), formal_stage_capacity=True
        )
        assert await frozen_facts(factory) == closed
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_v4_and_v5_unknown_remain_identical_through_maintenance(
    migrated_database_url, tmp_path, monkeypatch, capsys
):
    from helpers import budgeted_single_readback
    from integration.test_budgeted_single_sol_pg import (
        test_same_day_successor_uses_own_reservation_and_one_day_guard as scenario,
    )

    original = budgeted_single_readback.verify_budgeted_readback

    async def probe(factory, settings, view, *, outcome):
        await original(factory, settings, view, outcome=outcome)
        before = await frozen_facts(factory)
        with monkeypatch.context() as patch:
            unchanged = await refuse_new_work(factory, settings, view.request_id, patch)
            assert unchanged.status == "unknown"
            async with factory.begin() as db:
                row = await db.get(ManualPaperRequest, view.request_id)
                assert await finish_research_request(db, row=row) == unchanged
        async with factory() as db:
            row = await db.get(ManualPaperRequest, view.request_id)
            assert await read_manual_request(db, row) == unchanged
            assert await db.scalar(text("SELECT count(*) FROM manual_paper_requests")) == 2
        assert await frozen_facts(factory) == before

    monkeypatch.setattr(budgeted_single_readback, "verify_budgeted_readback", probe)
    await scenario(migrated_database_url, tmp_path, monkeypatch, capsys, "timeout")
