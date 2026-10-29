"""Nonzero shared-day accounting around an isolated native research protocol.

Historical roots/decision identity use existing test fixtures; all phase,
reservation, settlement and terminal operations keep native DB guards enabled.
"""

from datetime import timedelta
from decimal import Decimal
from uuid import uuid4

import pytest
from helpers.staged_protocol import StagedProtocol
from integration.test_portfolio_decision_budget_pg import _seed_runs
from integration.test_portfolio_synthesis_persistence_pg import seed_synthesis
from integration.test_staged_protocol_pg import controlled_observe
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import (
    _shared_daily_budget_used,
    admit_research_run,
    reserve_portfolio_decision_budget,
)
from nexus.agents.portfolio_synthesis_persistence import (
    admit_portfolio_synthesis,
    persist_portfolio_synthesis_terminal,
)
from nexus.jobs.models import QueueName
from nexus.jobs.repository import claim_next_job
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentPortfolioSynthesisBudgetReservationRecord,
    AgentResearchRunRecord,
    PortfolioDecisionBudgetReservationRecord,
)
from nexus.persistence.models.runtime import Job
from sqlalchemy import select


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["known_three", "known_prefix", "unknown_later"])
async def test_staged_chain_preserves_three_native_budget_types(migrated_database_url, mode):
    engine, factory, kwargs = await seed_synthesis(migrated_database_url)
    decision_id = uuid4()
    await _seed_runs(factory, research_run_ids=(), decision_run_ids=(decision_id,))
    try:
        async with factory.begin() as session:
            synthesis = (await admit_portfolio_synthesis(session, **kwargs)).run
            synthesis.status = "running"
            await session.flush()
            await persist_portfolio_synthesis_terminal(
                session,
                run_id=synthesis.id,
                input_hash=synthesis.input_hash,
                status="failed",
                intent=None,
                input_tokens=None,
                output_tokens=None,
                now=kwargs["now"],
            )
            await reserve_portfolio_decision_budget(
                session,
                run_id=decision_id,
                now=kwargs["now"],
                daily_limit_usd=Decimal("0.0011"),
                worst_case_usd=Decimal("0.0001"),
                pricing_version="test",
                pricing_fresh=True,
            )
            old = await session.scalar(
                select(AgentResearchRunRecord).order_by(AgentResearchRunRecord.id)
            )
            assert await _shared_daily_budget_used(
                session, business_date=kwargs["now"].date()
            ) == Decimal("0.0007")
            admitted = await admit_research_run(
                session,
                actor_id="offline-staged-protocol",
                route="offline-staged-protocol",
                idempotency_key="new-protocol-budget",
                request_hash="e" * 64,
                request=ResearchRequest(
                    instrument_id=old.instrument_id,
                    data_snapshot_id=old.data_snapshot_id,
                    decision_time=old.decision_time,
                ),
                prompt_version_id=None,
                model_config_id=old.model_config_id,
                daily_limit_usd=Decimal("0.0011"),
                now=kwargs["now"],
            )
            run_id = admitted.run_id
        async with factory.begin() as session:
            job = await session.scalar(
                select(Job).where(Job.payload["run_id"].as_string() == str(run_id))
            )
            claim = await claim_next_job(
                session,
                queue=QueueName.GENERAL,
                worker_id="isolated-budget",
                now=kwargs["now"],
                lease_seconds=120,
                job_ids=(job.id,),
            )
            assert claim is not None
        protocol = StagedProtocol(factory)
        now = kwargs["now"]
        await protocol.install(
            scope_id=kwargs["cycle_id"],
            claims=(claim,),
            now=now,
            work_deadline=now + timedelta(seconds=100),
            cleanup_deadline=now + timedelta(seconds=110),
        )

        async def snapshots(session):
            return {
                model.__tablename__: {
                    r.id: tuple(getattr(r, c.name) for c in r.__table__.columns)
                    for r in await session.scalars(select(model))
                }
                for model in (
                    AgentPortfolioSynthesisBudgetReservationRecord,
                    PortfolioDecisionBudgetReservationRecord,
                )
            }

        async with factory() as session:
            prior = await snapshots(session)
        stages = 3 if mode == "known_three" else 2 if mode == "unknown_later" else 1
        for stage in range(1, stages + 1):
            ticket = await protocol.authorize(
                claim, stage=stage, now=now, wire=f"stage{stage}".encode()
            )
            if not (mode == "unknown_later" and stage == 2):
                await controlled_observe(
                    protocol, ticket, now=now, payload=f"known{stage}".encode(), tokens=(10, 10)
                )
                await protocol.cleanup(ticket, now=now, confirmed=True)
            async with factory() as session:
                assert await _shared_daily_budget_used(
                    session, business_date=now.date()
                ) == Decimal("0.0009")
                assert await snapshots(session) == prior
        known = await protocol.close(claim, now=now + timedelta(seconds=1))
        assert known is (mode != "unknown_later")
        async with factory() as session:
            assert await snapshots(session) == prior
            expected = {
                "known_three": Decimal("0.00076"),
                "known_prefix": Decimal("0.00072"),
                "unknown_later": Decimal("0.0009"),
            }[mode]
            assert await _shared_daily_budget_used(session, business_date=now.date()) == expected
            hold = await session.scalar(
                select(AgentDailyBudgetReservationRecord).where(
                    AgentDailyBudgetReservationRecord.run_id == run_id
                )
            )
            assert hold.state == ("reserved" if mode == "unknown_later" else "settled")
    finally:
        await engine.dispose()
