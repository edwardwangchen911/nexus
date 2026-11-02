"""Paid native unknown history can be accepted only with the original protected proof."""

from datetime import timedelta
from decimal import Decimal

import httpx
import pytest
from integration.test_sol_native_preparation_pg import (
    test_formal_root_preserves_cycle_owner_and_freezes_native_sol_routes as prepare_formal,
)
from integration.test_sol_native_stage_pg import install_sol_http
from nexus import worker
from nexus.paper import agent_cycle, history_seal
from nexus.paper.manual_cancel import cancel_unused_manual_research
from nexus.paper.manual_request import require_manual_account_available
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.models.runtime import Job, JobLease
from sqlalchemy import func, select, text


async def accounting_facts(session, run_id):
    queries = (
        "SELECT to_jsonb(r)::text FROM agent_research_runs r WHERE id=:id",
        "SELECT to_jsonb(r)::text FROM agent_daily_budget_reservations r WHERE run_id=:id",
    )
    return tuple([await session.scalar(text(query), {"id": run_id}) for query in queries])


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "accept,same_day,stage,orphan",
    [
        (False, True, "research", False),
        (True, True, "research", False),
        (True, False, "critique", False),
        (True, True, "synthesis", False),
        (True, True, "critique", True),
    ],
)
async def test_native_formal_unknown_public_acceptance_preserves_paid_hold(
    migrated_database_url, monkeypatch, accept, same_day, stage, orphan
):
    async def validate(factory, settings, cycle_id):
        async def unknown(payload):
            if payload["stage"] == stage:
                raise httpx.ReadTimeout("synthetic transport unknown")

        calls, clients = install_sol_http(monkeypatch, None, on_request=unknown)
        async with factory() as session:
            cycle = await session.get(AgentPaperCycleRecord, cycle_id)
            job = await session.scalar(select(Job).order_by(Job.id).limit(1))
        if orphan:
            from uuid import UUID

            from integration.test_formal_research_orphan_pg import VanishedWorker
            from nexus.agents.research_stages import executor

            ordinary_finalize = executor.execute_and_finalize_job

            async def vanish_before_terminal(session, **kwargs):
                rows = await executor.stage_evidence(session, UUID(job.payload["run_id"]))
                if rows:
                    assert len(rows) == 2 and all(row["confirmed"] for row in rows)
                    raise VanishedWorker
                return await ordinary_finalize(session, **kwargs)

            # HTTP cleanup has finished. Lose the host before its terminal transaction.
            with monkeypatch.context() as lost:
                lost.setattr(executor, "execute_and_finalize_job", vanish_before_terminal)
                with pytest.raises(VanishedWorker):
                    await worker.run_agent_worker_once(
                        settings, job_ids=(job.id,), formal_stage_capacity=True
                    )
            async with factory.begin() as session:
                lease = await session.scalar(select(JobLease).where(JobLease.job_id == job.id))
                fresh = await session.scalar(select(func.clock_timestamp()))
                assert fresh < lease.expires_at
                live = await agent_cycle.resume_schwab_agent_paper_cycle(
                    session,
                    cycle_id=cycle_id,
                    idempotency_key="native-live-lease",
                    now=fresh,
                    formal_stage_capacity=True,
                    daily_limit_usd=Decimal("10"),
                )
                assert live.status == "researching"
                assert (await session.get(Job, job.id)).status == "running"
                assert (
                    await session.scalar(text("SELECT count(*) FROM research_stage_terminal")) == 0
                )
                expiry = lease.expires_at
        else:
            assert await worker.run_agent_worker_once(
                settings, job_ids=(job.id,), formal_stage_capacity=True
            )
        async with factory.begin() as session:
            now = (
                expiry + timedelta(seconds=1)
                if orphan
                else await session.scalar(select(func.clock_timestamp()))
            )
            result = await agent_cycle.resume_schwab_agent_paper_cycle(
                session,
                cycle_id=cycle_id,
                idempotency_key="native-failure-close",
                now=now,
                formal_stage_capacity=True,
                daily_limit_usd=Decimal("10"),
            )
            assert result.status in {"unavailable", "failed"}
            assert (
                await cancel_unused_manual_research(
                    session,
                    cycle=await session.get(AgentPaperCycleRecord, cycle_id),
                    events=await agent_cycle._events(session, cycle_id),
                    now=now,
                )
                == 0
            )
            assert (
                await session.scalar(
                    select(func.count()).select_from(Job).where(Job.status == "skipped")
                )
                == 2
            )
        async with factory() as session:
            run = await session.scalar(
                select(AgentResearchRunRecord).where(AgentResearchRunRecord.status == "failed")
            )
            assert run.settled_cost_usd is None and run.reserved_cost_usd > 0
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run.id
                )
            )
            assert hold.state == "reserved" and hold.reserved_usd == run.reserved_cost_usd
            seal_now = now + (timedelta(minutes=10) if same_day else timedelta(days=8))

            before = await accounting_facts(session, run.id)
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_manual_account_available(
                    session, account_id=cycle.account_id, now=seal_now
                )
        # Only process discovery is isolated; all DB proof, consent, audit, and
        # admission checks use the ordinary public service on synthetic facts.
        monkeypatch.setattr(history_seal, "require_research_processes_quiescent", lambda: None)
        async with factory.begin() as session:
            if not accept:
                with pytest.raises(ValueError, match="MANUAL_REQUEST_INVALID"):
                    await history_seal.seal_failed_research(
                        session,
                        account_id=cycle.account_id,
                        cycle_id=cycle_id,
                        run_ids=(run.id,),
                        now=seal_now,
                        accept_unknown_usage=False,
                        same_day=same_day,
                        expired_attempt=orphan,
                    )
            else:
                receipt = await history_seal.seal_failed_research(
                    session,
                    account_id=cycle.account_id,
                    cycle_id=cycle_id,
                    run_ids=(run.id,),
                    now=seal_now,
                    accept_unknown_usage=True,
                    same_day=same_day,
                    expired_attempt=orphan,
                )
                assert receipt.sealed_research == 1 and not receipt.replayed
        async with factory.begin() as session:
            assert await accounting_facts(session, run.id) == before
            if accept:
                assert (
                    await history_seal.seal_failed_research(
                        session,
                        account_id=cycle.account_id,
                        cycle_id=cycle_id,
                        run_ids=(run.id,),
                        now=seal_now,
                        accept_unknown_usage=True,
                        same_day=same_day,
                        expired_attempt=orphan,
                    )
                ).replayed
                await require_manual_account_available(
                    session, account_id=cycle.account_id, now=seal_now
                )
            else:
                with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                    await require_manual_account_available(
                        session, account_id=cycle.account_id, now=seal_now
                    )
        stages = ["research", "critique", "synthesis"]
        assert calls == stages[: stages.index(stage) + 1] and all(c.is_closed for c in clients)

    await prepare_formal(migrated_database_url, monkeypatch, after_prepared=validate)
