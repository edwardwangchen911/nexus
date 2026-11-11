"""A monitor transaction holds the same account gate as a formal stage grant."""

from datetime import timedelta

import pytest
from integration.test_sol_native_preparation_pg import (
    test_formal_root_preserves_cycle_owner_and_freezes_native_sol_routes as prepare_formal,
)
from integration.test_sol_native_stage_pg import install_sol_http
from nexus import worker
from nexus.agents.research_stages.formal_defer import FormalStageGrantBusy, deferred_lease_remaining
from nexus.agents.research_stages.protocol import StagedProtocol, stage_evidence, usage_known
from nexus.paper import preparation_unknown_ack, target_service
from nexus.paper.agent_cycle import resume_schwab_agent_paper_cycle
from nexus.persistence.models.agents import AgentModelConfigRecord, AgentResearchRunRecord
from nexus.persistence.models.paper import AgentPaperCycleRecord
from sqlalchemy import text


@pytest.mark.asyncio
async def test_formal_second_grant_contending_with_monitor_preserves_first_stage(
    migrated_database_url, monkeypatch
):
    requests, clients = install_sol_http(monkeypatch, None)

    async def validate(factory, settings, cycle_id):
        async with factory() as session:
            cycle = await session.get(AgentPaperCycleRecord, cycle_id)
            routes = (
                await session.execute(
                    text("SELECT run_id,job_id FROM research_stage_route ORDER BY run_id")
                )
            ).all()
            (first_run, first_job), (run_id, job_id), (third_run, third_job) = routes
        assert await worker.run_agent_worker_once(
            settings, job_ids=(first_job,), formal_stage_capacity=True
        )
        async with factory.begin() as monitor:
            monitor_pid = await monitor.scalar(text("SELECT pg_backend_pid()"))
            holding = False
            refusals = []
            cleanup = StagedProtocol.cleanup
            audits = preparation_unknown_ack._audits
            try_account = target_service.try_lock_portfolio_account

            async def after_cleanup(protocol, ticket, **kwargs):
                nonlocal holding
                await cleanup(protocol, ticket, **kwargs)
                assert ticket.stage == 1 and kwargs["confirmed"] is True
                # Use the identical advisory key and PG transaction that
                # reconcile_account_results obtains before validating candidates.
                await target_service.lock_portfolio_account(monitor, cycle.account_id)
                holding = True

            async def history_present(session, account_id):
                if holding and account_id == cycle.account_id:
                    # Isolate the existence predicate only. A failed account
                    # lock must precede validation of any historical exception.
                    return (object(),)
                return await audits(session, account_id)

            async def observe_try(session, account_id):
                result = await try_account(session, account_id)
                if holding and account_id == cycle.account_id:
                    assert await session.scalar(text("SELECT pg_backend_pid()")) != monitor_pid
                    refusals.append(result)
                return result

            monkeypatch.setattr(StagedProtocol, "cleanup", after_cleanup)
            monkeypatch.setattr(preparation_unknown_ack, "_audits", history_present)
            monkeypatch.setattr(target_service, "try_lock_portfolio_account", observe_try)
            with pytest.raises(FormalStageGrantBusy) as captured:
                await worker.run_agent_worker_once(
                    settings, job_ids=(job_id,), formal_stage_capacity=True
                )
            assert holding and refusals == [False]
        holding = False
        notice = captured.value
        assert (notice.run_id, notice.scope_id, notice.stage) == (run_id, cycle_id, 2)
        assert notice.claim.job_id == job_id
        assert await deferred_lease_remaining(settings, notice) > 0
        assert await deferred_lease_remaining(settings, notice) > 0
        async with factory() as observer:
            run = await observer.get(AgentResearchRunRecord, run_id)
            model = await observer.get(AgentModelConfigRecord, run.model_config_id)
            evidence = await stage_evidence(observer, run_id)
            assert [row["stage"] for row in evidence] == [1]
            assert evidence[0]["verified"] and evidence[0]["confirmed"] and evidence[0]["eligible"]
            assert usage_known(evidence, model=model)
            assert run.status == "prepared" and run.settled_cost_usd is None
            states = (
                await observer.execute(
                    text(
                        "SELECT s.stopped,j.status,a.outcome,h.state FROM research_stage_scope s "
                        "JOIN research_stage_member m ON m.scope_id=s.id "
                        "JOIN jobs j ON j.id=m.job_id JOIN job_attempts a ON a.job_id=j.id "
                        "JOIN agent_daily_budget_reservations h ON h.run_id=m.run_id "
                        "WHERE m.run_id=:run"
                    ),
                    {"run": run_id},
                )
            ).one()
            assert tuple(states) == (True, "running", "running", "reserved")
            assert not await observer.scalar(
                text("SELECT 1 FROM research_stage_terminal WHERE run_id=:run"), {"run": run_id}
            )
            from nexus.ops.agent_paper_loop import resolve_cycle_research_job
            from nexus.persistence.models.paper import AgentPaperCycleEventRecord
            from nexus.persistence.models.runtime import Job
            from sqlalchemy import select

            events = tuple(
                await observer.scalars(
                    select(AgentPaperCycleEventRecord).where(
                        AgentPaperCycleEventRecord.cycle_id == cycle_id
                    )
                )
            )
            jobs = tuple(
                await observer.scalars(select(Job).where(Job.id.in_([r[1] for r in routes])))
            )
            assert resolve_cycle_research_job(events=events, jobs=jobs) == (job_id, "running")
            first_cost = (await observer.get(AgentResearchRunRecord, first_run)).settled_cost_usd
            assert first_cost is not None
        after_expiry = notice.claim.expires_at + timedelta(seconds=1)
        for _ in range(2):
            async with factory.begin() as recovery:
                result = await resume_schwab_agent_paper_cycle(
                    recovery,
                    cycle_id=cycle_id,
                    idempotency_key="formal-deferred-original",
                    now=after_expiry,
                    daily_limit_usd=settings.agent_daily_budget_usd,
                    formal_stage_capacity=True,
                )
                assert result.status == "unavailable"
            async with factory() as observer:
                assert (
                    await observer.get(AgentResearchRunRecord, first_run)
                ).settled_cost_usd == first_cost
                orphan = await observer.get(AgentResearchRunRecord, run_id)
                assert orphan.status == "failed" and orphan.settled_cost_usd is not None
                assert await observer.scalar(
                    text("SELECT known FROM research_stage_terminal WHERE run_id=:r"), {"r": run_id}
                )
                other = await observer.get(Job, third_job)
                assert other.attempt_count == 1 and other.status == "skipped"
                assert other.result_code == "MANUAL_UNUSED_RESEARCH_CANCELLED"
                assert (
                    await observer.scalar(
                        text("SELECT worker_id FROM job_attempts WHERE job_id=:j"), {"j": third_job}
                    )
                    == f"manual-cancel:{cycle_id}"
                )
                assert (
                    await observer.scalar(
                        text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:r"),
                        {"r": third_run},
                    )
                    == "released"
                )
                assert not await observer.scalar(
                    text("SELECT 1 FROM research_stage_authorization WHERE run_id=:r"),
                    {"r": third_run},
                )
        assert requests == ["research", "critique", "synthesis", "research"]
        assert len(clients) == 4 and all(client.is_closed for client in clients)

    await prepare_formal(migrated_database_url, monkeypatch, after_prepared=validate)
