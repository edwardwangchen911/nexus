"""Normal once-only monitor entrance, real SQL fences, synthetic model transport."""

from decimal import Decimal

import pytest
from integration import test_budgeted_single_sol_pg as v5
from integration import test_single_sol_request_pg as legacy
from nexus.ops import manual_paper
from nexus.ops.manual_paper_host import dispatch_pure_research_request
from nexus.paper.manual_request import read_manual_request
from nexus.paper.single_sol_owner import binding_for
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.persistence.session import session_factory
from sqlalchemy import text


@pytest.mark.asyncio
async def test_once_monitor_normal_lifecycle_and_old_predecessor(
    migrated_database_url, tmp_path, monkeypatch, capsys
):
    settings, store, ids = await v5.completed_predecessor(
        migrated_database_url, tmp_path, monkeypatch, capsys
    )
    factory = session_factory(settings)

    async def call(*args):
        return await manual_paper.run(settings, manual_paper.parser().parse_args(list(args)))

    try:
        offer = await call("inspect-single-monitor-sol-intent", "--instrument-id", str(ids[0]))
        args = [
            "start-single-monitor-sol",
            "--instrument-id",
            str(ids[0]),
            "--idempotency-key",
            "one-monitor",
            "--expected-intent-hash",
            offer.intent_hash,
        ]
        receipt = await call(*args)
        assert (
            receipt.mode == "research_and_monitor"
            and receipt.research_contract == "independent_monitor_research_v1"
        )
        async with factory() as db:
            from nexus.workbench.research_summary import research_candidates

            row = await db.get(ManualPaperRequest, receipt.request_id)
            assert (
                await research_candidates(
                    db, row, receipt, read_at=await db.scalar(text("SELECT clock_timestamp()"))
                )
                == ()
            )
        assert (await call(*args)).request_id == receipt.request_id
        with pytest.raises(ValueError, match="SINGLE_MONITOR_ONCE_CONSUMED"):
            await call(*[("second-monitor" if v == "one-monitor" else v) for v in args])
        sent, clients = legacy.install_sol_http(monkeypatch, store)
        async with factory() as db:
            row = await db.get(ManualPaperRequest, receipt.request_id)
            calendar = binding_for(row).calendar_id
        assert await dispatch_pure_research_request(
            settings, request_id=receipt.request_id, calendar_authority_id=calendar
        )
        assert sent == ["research", "critique", "synthesis"] and all(c.is_closed for c in clients)
        async with factory() as db:
            row = await db.get(ManualPaperRequest, receipt.request_id)
            view = await read_manual_request(db, row)
            assert view.status == "completed" and len(view.research.members) == 1
            member = view.research.members[0]
            assert member.instrument_id == ids[0] and member.usage_known
            from nexus.workbench.research_summary import research_candidates

            candidates = await research_candidates(
                db, row, view, read_at=await db.scalar(text("SELECT clock_timestamp()"))
            )
            assert len(candidates) == 1 and candidates[0].read_status == "verified"
            assert candidates[0].run_id == member.run_id
            facts = (
                await db.execute(
                    text(
                        """SELECT r.actor_id,r.idempotency_key,s.payload->>'purpose',
                        b.state,b.settled_usd
                        FROM agent_research_runs r JOIN data_snapshots s ON s.id=r.data_snapshot_id
                        JOIN agent_daily_budget_reservations b ON b.run_id=r.id WHERE r.id=:id"""
                    ),
                    {"id": member.run_id},
                )
            ).one()
            assert facts[0] == "local-single-monitor-sol-v1" and facts[1].startswith(
                "single-monitor-sol:"
            )
            assert (
                facts[2] == "independent_monitor_research_v1"
                and facts[3] == "settled"
                and facts[4] > Decimal(0)
            )
            assert (
                await db.scalar(
                    text("SELECT count(*) FROM manual_paper_requests WHERE contract_version=6")
                )
                == 1
            )
            assert await db.scalar(text("SELECT count(*) FROM paper_orders")) == 0
        assert (await call(*args)).request_id == receipt.request_id
        with pytest.raises(ValueError, match="SINGLE_MONITOR_ONCE_CONSUMED"):
            await call("inspect-single-monitor-sol-intent", "--instrument-id", str(ids[0]))
    finally:
        await factory.kw["bind"].dispose()
