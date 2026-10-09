"""Explicit candidate consumers reject a real nonisolated connection before mutations."""

from decimal import Decimal
from uuid import uuid4

import pytest
from integration.test_candidate_research_authority import NOW
from nexus.agents.portfolio_synthesis_persistence import admit_portfolio_synthesis
from nexus.paper.agent_decision_store import load_portfolio_proposal, persist_portfolio_proposal
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


async def call_consumer(session, consumer, flag):
    if consumer == "admit":
        return await admit_portfolio_synthesis(
            session,
            cycle_id=uuid4(),
            research_output_ids=(uuid4(),),
            active_target_version_id=None,
            projection_hash="0" * 64,
            daily_limit_usd=Decimal("1"),
            now=NOW,
            candidate_stage_capacity=flag,
        )
    if consumer == "persist":
        return await persist_portfolio_proposal(
            session,
            cycle_id=uuid4(),
            synthesis_output_id=uuid4(),
            now=NOW,
            candidate_stage_capacity=flag,
        )
    return await load_portfolio_proposal(
        session,
        account_id=uuid4(),
        decision_id=uuid4(),
        candidate_stage_capacity=flag,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("consumer", ["admit", "persist", "load"])
@pytest.mark.parametrize("flag", [True, "true", 1])
async def test_candidate_consumers_reject_before_business_reads(
    temporary_database_factory, consumer, flag
):
    # The owned test container's actual postgres database has no NEXUS business tables.
    # Thus the fixed rejection proves no business query or lock precedes isolation validation.
    factory = session_factory(
        EngineSettings(
            database_url=temporary_database_factory.admin_url.render_as_string(hide_password=False)
        )
    )
    try:
        async with factory() as session:
            with pytest.raises(ValueError, match="CODEX_CANDIDATE_ISOLATION_REQUIRED"):
                await call_consumer(session, consumer, flag)
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("cached", [False, True])
async def test_synthesis_candidate_mode_cannot_relabel_formal_research(
    migrated_database_url, cached
):
    from integration.test_portfolio_synthesis_persistence_pg import seed_synthesis
    from nexus.agents.actionable_research import ActionableResearchError
    from nexus.persistence.models.agents import AgentPortfolioSynthesisRunRecord
    from sqlalchemy import func, select

    engine, sessions, args = await seed_synthesis(migrated_database_url)
    try:
        if cached:
            async with sessions.begin() as session:
                await admit_portfolio_synthesis(session, **args)
        async with sessions.begin() as session:
            with pytest.raises(ActionableResearchError, match="RESEARCH_OUTPUT_NOT_ACTIONABLE"):
                await admit_portfolio_synthesis(session, **args, candidate_stage_capacity=True)
            assert await session.scalar(
                select(func.count()).select_from(AgentPortfolioSynthesisRunRecord)
            ) == int(cached)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_proposal_candidate_mode_revalidates_new_cached_and_loaded_bindings(
    migrated_database_url,
):
    from integration.test_portfolio_proposal_pg import proposal_source
    from nexus.agents.actionable_research import ActionableResearchError
    from nexus.persistence.models.paper import StrategyDecisionRecord
    from sqlalchemy import func, select

    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            before = await session.scalar(select(func.count()).select_from(StrategyDecisionRecord))
            with pytest.raises(ActionableResearchError, match="RESEARCH_OUTPUT_NOT_ACTIONABLE"):
                await persist_portfolio_proposal(session, **args, candidate_stage_capacity=True)
            assert (
                await session.scalar(select(func.count()).select_from(StrategyDecisionRecord))
                == before
            )
            stored = await persist_portfolio_proposal(session, **args)
        async with sessions.begin() as session:
            with pytest.raises(ActionableResearchError, match="RESEARCH_OUTPUT_NOT_ACTIONABLE"):
                await persist_portfolio_proposal(session, **args, candidate_stage_capacity=True)
            with pytest.raises(ActionableResearchError, match="RESEARCH_OUTPUT_NOT_ACTIONABLE"):
                await load_portfolio_proposal(
                    session,
                    account_id=stored.account_id,
                    decision_id=stored.decision_id,
                    candidate_stage_capacity=True,
                )
            assert (
                await load_portfolio_proposal(
                    session, account_id=stored.account_id, decision_id=stored.decision_id
                )
                == stored
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_cached_synthesis_default_mode_revalidates_each_source(
    migrated_database_url, monkeypatch
):
    from integration.test_portfolio_synthesis_persistence_pg import seed_synthesis
    from nexus.agents import portfolio_synthesis_persistence as consumer
    from nexus.agents.actionable_research import ActionableResearchError

    engine, sessions, args = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            first = await admit_portfolio_synthesis(session, **args)
        original = consumer.load_actionable_research_output
        calls = []

        async def observe(session, **kwargs):
            calls.append(kwargs)
            return await original(session, **kwargs)

        monkeypatch.setattr(consumer, "load_actionable_research_output", observe)
        async with sessions.begin() as session:
            replay = await admit_portfolio_synthesis(session, **args)
            assert replay.replayed and replay.run.id == first.run.id
            assert len(calls) == len(args["research_output_ids"])
            assert all(call["candidate_stage_capacity"] is False for call in calls)

        async def deny_candidate_source(session, **kwargs):
            assert kwargs["candidate_stage_capacity"] is False
            raise ActionableResearchError("RESEARCH_OUTPUT_NOT_ACTIONABLE")

        monkeypatch.setattr(consumer, "load_actionable_research_output", deny_candidate_source)
        async with sessions.begin() as session:
            with pytest.raises(ActionableResearchError, match="RESEARCH_OUTPUT_NOT_ACTIONABLE"):
                await admit_portfolio_synthesis(session, **args)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_candidate_synthesis_worker_does_not_claim_formal_job(migrated_database_url):
    from integration.test_portfolio_synthesis_persistence_pg import seed_synthesis
    from nexus.agents.portfolio_synthesis_worker import read_synthesis_job, run_synthesis_job_once
    from nexus.persistence.models.runtime import Job, JobAttempt, JobLease
    from sqlalchemy import func, select

    engine, sessions, args = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            await admit_portfolio_synthesis(session, **args)
            job = await read_synthesis_job(session, cycle_id=args["cycle_id"])
            job_id = job.id

        class NeverCall:
            def supports(self, model):
                raise AssertionError("unqualified model reached transport")

            async def invoke(self, payload):
                raise AssertionError("unqualified model invoked transport")

        assert not await run_synthesis_job_once(
            sessions,
            transport=NeverCall(),
            worker_id="candidate-test",
            clock=lambda: args["now"],
            job_ids=(job_id,),
            candidate_stage_capacity=True,
        )
        async with sessions() as session:
            job = await session.get(Job, job_id)
            assert job.status == "queued" and job.attempt_count == 0
            assert (
                await session.scalar(
                    select(func.count()).select_from(JobAttempt).where(JobAttempt.job_id == job_id)
                )
                == 0
            )
            assert (
                await session.scalar(
                    select(func.count()).select_from(JobLease).where(JobLease.job_id == job_id)
                )
                == 0
            )
    finally:
        await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("entry", ["dispatch", "composition"])
async def test_candidate_synthesis_actual_database_rejected_before_reads(
    temporary_database_factory, entry
):
    from nexus.agents.portfolio_synthesis_worker import (
        CodexSynthesisTransport,
        run_synthesis_job_once,
    )
    from nexus.worker import run_portfolio_synthesis_worker_once

    settings = EngineSettings(
        database_url=temporary_database_factory.admin_url.render_as_string(hide_password=False),
        agent_runtime_mode="codex_sdk",
    )
    if entry == "composition":
        with pytest.raises(ValueError, match="CODEX_CANDIDATE_ISOLATION_REQUIRED"):
            await run_portfolio_synthesis_worker_once(
                settings, job_ids=(uuid4(),), candidate_stage_capacity=True
            )
        return
    factory = session_factory(settings)
    try:
        with pytest.raises(ValueError, match="CODEX_CANDIDATE_ISOLATION_REQUIRED"):
            await run_synthesis_job_once(
                factory,
                transport=CodexSynthesisTransport(candidate_stage_capacity=True),
                worker_id="candidate-test",
                clock=lambda: NOW,
                job_ids=(uuid4(),),
                candidate_stage_capacity=True,
            )
    finally:
        await factory.kw["bind"].dispose()
