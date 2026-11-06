"""Actual Worker preclaim lock order, original deadline and external contention."""

import asyncio
from time import monotonic

import pytest
from integration.test_independent_history_scope_pg import (
    test_accepted_history_three_stock_preparation_worker_and_close as exercise,
)
from nexus import worker
from nexus.agents.budget import ResearchAccountingBusy
from sqlalchemy import text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["cancel", "deadline", "stopped", "late_history", "account", "budget", "wait"]
)
async def test_native_preclaim_joins_scope_before_claim(
    migrated_database_url, monkeypatch, tmp_path, request, case
):
    async def check(factory, live, prepared, receipt, calls, clients, old):
        member = prepared.research.members[0]
        allowance = []
        original_timeout = asyncio.timeout_at

        def timeout_at(deadline):
            value = original_timeout(deadline)
            if (
                abs(
                    deadline
                    - asyncio.get_running_loop().time()
                    - worker.CAPACITY_PRECLAIM_TIMEOUT_SECONDS
                )
                < 2
            ):
                allowance.append((value, deadline))
            return value

        monkeypatch.setattr(asyncio, "timeout_at", timeout_at)
        async with factory() as holder:
            await holder.begin()
            holder_pid = await holder.scalar(text("SELECT pg_backend_pid()"))
            if case in {"account", "budget"}:
                if case == "account":
                    from nexus.paper.target_service import try_lock_portfolio_account

                    assert await try_lock_portfolio_account(holder, live.agent_paper_account_id)
                else:
                    from nexus.agents.persistence import try_lock_budget_day

                    now = await holder.scalar(text("SELECT clock_timestamp()"))
                    assert await try_lock_budget_day(holder, now.date().isoformat())
            else:
                await holder.execute(
                    text("SELECT id FROM research_stage_scope WHERE id=:id FOR UPDATE"),
                    {"id": receipt.request_id},
                )
            task = asyncio.create_task(
                worker.run_agent_worker_once(
                    live, job_ids=(member.job_id,), formal_stage_capacity=True
                )
            )
            try:
                if case in {"account", "budget"}:
                    with pytest.raises(ResearchAccountingBusy):
                        await asyncio.wait_for(task, 20)
                    assert calls == []
                    await holder.rollback()
                else:
                    end = monotonic() + 10
                    while monotonic() < end:
                        async with factory() as probe:
                            waiting = await probe.scalar(
                                text(
                                    "SELECT count(*) FROM pg_stat_activity "
                                    "WHERE :pid=ANY(pg_blocking_pids(pid)) "
                                    "AND query LIKE '%research_stage_scope%FOR UPDATE%'"
                                ),
                                {"pid": holder_pid},
                            )
                        if waiting:
                            break
                        assert not task.done()
                        await asyncio.sleep(0.02)
                    assert waiting == 1
                    assert allowance and calls == []
                    # A waiting preclaim must not retain a Run lock ahead of scope.
                    await holder.execute(
                        text("SELECT id FROM agent_research_runs WHERE id=:id FOR UPDATE NOWAIT"),
                        {"id": member.run_id},
                    )
                    if case == "cancel":
                        task.cancel()
                        with pytest.raises(asyncio.CancelledError):
                            await task
                    elif case == "deadline":
                        allowance[0][0].reschedule(asyncio.get_running_loop().time())
                        with pytest.raises(worker.CapacityPreclaimTimeout):
                            await task
                    elif case == "late_history":
                        from nexus.paper.preparation_unknown_ack import _audits
                        from nexus.persistence.models.runtime import IdempotencyRecord
                        from sqlalchemy import update

                        audit = (await _audits(holder, live.agent_paper_account_id))[0]
                        await holder.execute(
                            update(IdempotencyRecord)
                            .where(IdempotencyRecord.id == audit.payload["claim_ids"][0])
                            .values(response_payload={"late": True})
                        )
                        await holder.commit()
                        with pytest.raises(ValueError, match="INDEPENDENT_HISTORY_PROOF_INVALID"):
                            await task
                    elif case == "stopped":
                        await holder.execute(
                            text("UPDATE research_stage_scope SET stopped=true WHERE id=:id"),
                            {"id": receipt.request_id},
                        )
                        await holder.commit()
                        with pytest.raises(ValueError):
                            await task
                    else:
                        await holder.commit()
                        assert await asyncio.wait_for(task, 40)
                        assert calls == ["research", "critique", "synthesis"]
                    await holder.rollback()
            finally:
                if not task.done():
                    task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                await holder.rollback()
        async with factory.begin() as db:
            await db.execute(
                text("SELECT id FROM research_stage_scope WHERE id=:id FOR UPDATE NOWAIT"),
                {"id": receipt.request_id},
            )
            await db.execute(
                text("SELECT id FROM agent_research_runs WHERE id=:id FOR UPDATE NOWAIT"),
                {"id": member.run_id},
            )
            if case != "wait":
                assert calls == []
                assert (
                    await db.scalar(
                        text("SELECT count(*) FROM job_attempts WHERE job_id=:id"),
                        {"id": member.job_id},
                    )
                    == 0
                )
                assert (
                    await db.scalar(
                        text("SELECT count(*) FROM research_stage_authorization WHERE run_id=:id"),
                        {"id": member.run_id},
                    )
                    == 0
                )
                assert (
                    await db.scalar(
                        text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:id"),
                        {"id": member.run_id},
                    )
                    == "reserved"
                )
        assert all(c.is_closed for c in clients)

    await exercise(
        migrated_database_url, monkeypatch, tmp_path, None, request, zero_stage_check=check
    )
