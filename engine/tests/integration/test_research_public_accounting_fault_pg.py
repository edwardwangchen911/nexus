"""Public orphan recovery preserves legal usage and the original storage failure."""

import hashlib
from datetime import timedelta

import pytest
from helpers import staged_routes
from integration.test_staged_roots_pg import (
    test_normal_three_stock_roots_use_one_staged_protocol as roots,
)
from nexus.agents import execution
from nexus.agents.codex_formal_stage import formal_stage_job_qualified
from nexus.agents.research_stages import routes
from nexus.agents.research_stages.protocol import StagedProtocol
from nexus.jobs.models import QueueName
from nexus.jobs.repository import claim_next_job
from nexus.paper.manual_request import read_manual_request, require_manual_account_available
from nexus.paper.research_orphan import recover_formal_research_orphans
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError


class Observed(BaseException):
    pass


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["runtime", "value", "database", "busy"])
async def test_public_orphan_accounting_fault_is_not_known_held(
    migrated_database_url, monkeypatch, tmp_path, failure
):
    async def noop(*args, **kwargs):
        pass

    monkeypatch.setattr(staged_routes, "ensure_routes", noop)
    monkeypatch.setattr(staged_routes, "register_prepared_runs", noop)

    async def inspect(shared, settings, options):
        factory, now = shared["fixture"][0], shared["clock"][0]
        async with factory() as session:
            request = (await session.scalars(select(ManualPaperRequest))).one()
            account_id = request.account_id
            view = await read_manual_request(session, request)
            member = view.research.members[0]
            profile = await session.scalar(
                select(AgentResearchRunRecord.pricing_version).where(
                    AgentResearchRunRecord.id == member.run_id
                )
            )
            route = await routes.selected_route(session, member.job_id)
        async with factory.begin() as session:
            await session.execute(
                text("SELECT set_config('nexus.stage_protocol',:version,true)"),
                {"version": routes.VERSION},
            )
            claim = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="isolated-public-accounting-fault",
                now=now,
                job_ids=(member.job_id,),
                research_budget_profile=profile,
                locked_job_guard=formal_stage_job_qualified,
                lease_seconds=330,
            )
            assert claim is not None
        protocol = StagedProtocol(factory)
        await protocol.install(
            scope_id=route["scope_id"],
            claims=(claim,),
            now=now,
            work_deadline=now + timedelta(seconds=300),
            cleanup_deadline=now + timedelta(seconds=307),
            independent=True,
        )
        ticket = await protocol.authorize(claim, stage=1, now=now, wire=b"public-legal-usage")
        await protocol.observe(
            ticket, now=now, payload=b"known-failed", tokens=(10, 5), valid=False
        )
        assert await protocol.verify(
            ticket,
            now=now,
            wire_hash=hashlib.sha256(b"public-legal-usage").hexdigest(),
            payload=b"known-failed",
            tokens=(10, 5),
        )
        await protocol.cleanup(ticket, now=now, confirmed=True)
        async with factory() as session:
            assert (
                await session.scalar(text("SELECT bool_or(stopped) FROM research_stage_scope"))
                is False
            )
        errors = []

        async def fault(session, **kwargs):
            try:
                if failure == "database":
                    await session.execute(text("SELECT 1/0"))
                raise (RuntimeError if failure == "runtime" else ValueError)(
                    "original storage failure"
                )
            except Exception as error:
                errors.append(error)
                raise

        holder = factory()
        await holder.begin()
        if failure == "busy":
            day = await holder.scalar(
                text("SELECT business_date FROM agent_daily_budget_reservations WHERE run_id=:run"),
                {"run": member.run_id},
            )
            await holder.execute(
                text("SELECT pg_advisory_xact_lock(hashtext(:key))"), {"key": f"agent-budget:{day}"}
            )
        else:
            monkeypatch.setattr(execution, "settle_daily_budget", fault)
        expired = claim.expires_at + timedelta(seconds=1)
        try:
            with pytest.raises((RuntimeError, ValueError, DBAPIError)) as caught:
                async with factory.begin() as session:
                    request = await session.get(ManualPaperRequest, view.request_id)
                    await recover_formal_research_orphans(
                        session,
                        cycle=None,
                        run_ids=tuple(m.run_id for m in view.research.members),
                        now=expired,
                        research_request=request,
                    )
            if failure == "busy":
                from integration.test_research_accounting_mutex_pg import accounting_locks
                from nexus.agents.budget import ResearchAccountingBusy

                assert isinstance(caught.value, ResearchAccountingBusy) and errors == []
                async with factory() as probe:
                    assert not await accounting_locks(probe, "agent-budget:accounting")
            else:
                assert len(errors) == 1 and caught.value is errors[0]
        finally:
            await holder.close()
        async with factory() as session:
            assert await session.scalar(text("SELECT count(*) FROM research_stage_terminal")) == 0
            run = await session.get(AgentResearchRunRecord, member.run_id)
            assert run.status == "prepared"
            assert (
                run.input_tokens is None
                and run.output_tokens is None
                and run.settled_cost_usd is None
            )
            assert (
                await session.scalar(
                    text("SELECT state FROM agent_daily_budget_reservations WHERE run_id=:run"),
                    {"run": run.id},
                )
                == "reserved"
            )
            assert (
                await session.execute(
                    text(
                        "SELECT j.status,a.outcome,a.finished_at FROM jobs j JOIN "
                        "job_attempts a ON a.job_id=j.id WHERE j.id=:id"
                    ),
                    {"id": member.job_id},
                )
            ).one() == ("running", "running", None)
            # Readback remains available; failed recovery cannot authorize the account.
            receipt = await read_manual_request(
                session, await session.get(ManualPaperRequest, view.request_id)
            )
            assert receipt.request_id == view.request_id
            with pytest.raises(ValueError, match="MANUAL_ACCOUNT_BUSY_OR_UNKNOWN"):
                await require_manual_account_available(
                    session, account_id=account_id, now=expired, read_only=True
                )
        raise Observed

    with pytest.raises(Observed):
        await roots(
            migrated_database_url, monkeypatch, tmp_path, True, False, before_worker=inspect
        )
