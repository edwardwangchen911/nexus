"""Formal recovery/seal consumes stage proof; historical siblings retain their old route."""

from datetime import timedelta
from uuid import UUID

import pytest
from integration import test_formal_research_orphan_pg as orphan_case
from integration.test_research_orphan_seal_pg import (
    test_orphan_normal_seal_closes_manual_and_admits_fresh_request as seal_case,
)
from nexus.agents.research_stages import executor, routes
from nexus.agents.research_stages.protocol import StagedProtocol
from nexus.jobs import worker
from sqlalchemy import text


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["same_day", "no_ack", "late_fact", "close_rollback"])
async def test_formal_public_recovery_seal_and_fresh_admission(
    migrated_database_url, monkeypatch, tmp_path, case
):
    state = {}
    seed = orphan_case.seed_cycle
    register = routes.register_new_prepared_runs

    async def mixed_birth(session, run_ids):
        if "target" not in state:
            # Deliberately model two legacy siblings plus one newly selected Job.
            # No committed Job or frozen identity is upgraded by this fixture.
            state["target"] = run_ids[1]
            await register(session, (run_ids[1],))
        else:
            await register(session, run_ids)

    monkeypatch.setattr(routes, "register_new_prepared_runs", mixed_birth)

    async def seed_with_capture(*args, **kwargs):
        async def capture(settings, factory, authority, manifest, clock):
            state.update(factory=factory, clock=clock)

        kwargs["before_research"] = capture
        return await seed(*args, **kwargs)

    monkeypatch.setattr(orphan_case, "seed_cycle", seed_with_capture)

    async def disappeared(*, factory, claim, clock, execute):
        assert UUID(claim.payload["run_id"]) == state["target"]
        # The pre-existing scenario owns the two exact dispatch identities and
        # interrupts only the second claim. Inject no second claim or SDK request.
        with pytest.raises(orphan_case.VanishedWorker):
            await worker.execute_and_finalize_job(None, claim=claim)
        async with factory() as session:
            route = await routes.selected_route(session, claim.job_id)
        now = state["clock"][0]
        protocol = StagedProtocol(factory)
        await protocol.install(
            scope_id=route["scope_id"],
            claims=(claim,),
            now=now,
            work_deadline=now + timedelta(seconds=100),
            cleanup_deadline=now + timedelta(seconds=110),
        )
        ticket = await protocol.authorize(claim, stage=1, now=now, wire=b"controlled-unknown")
        await protocol.cleanup(ticket, now=now, confirmed=True)
        raise orphan_case.VanishedWorker

    monkeypatch.setattr(executor, "execute_staged_claim", disappeared)
    await seal_case(migrated_database_url, monkeypatch, tmp_path, case)
    async with state["factory"]() as session:
        assert await session.scalar(text("SELECT count(*) FROM research_stage_terminal")) == 1
        assert await session.scalar(text("SELECT known FROM research_stage_terminal")) is False
        assert await session.scalar(text("SELECT count(*) FROM research_stage_authorization")) == 1
        assert (
            await session.scalar(
                text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:run"),
                {"run": state["target"]},
            )
            == "reserved"
        )
