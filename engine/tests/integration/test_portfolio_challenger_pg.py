"""Frozen multi-candidate cycle samples for Agent challenger retrospectives."""

from datetime import timedelta

import pytest
from helpers.paper_batch import INSTRUMENTS
from integration.test_portfolio_proposal_pg import proposal_source
from nexus.paper import agent_challenger
from nexus.paper.agent_challenger import AgentChallengerError
from nexus.paper.agent_cycle import _events, _resume_portfolio_synthesis
from nexus.persistence.models.paper import AgentPaperCycleRecord


@pytest.mark.asyncio
async def test_completed_portfolio_cycle_yields_all_candidate_evidence(
    migrated_database_url: str,
) -> None:
    """Dropping a candidate or synthesis output would corrupt the improvement sample."""
    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            assert cycle is not None
            result = await _resume_portfolio_synthesis(
                session,
                cycle=cycle,
                events=await _events(session, cycle.id),
                run_ids=(),
                now=args["now"] + timedelta(seconds=1),
                daily_limit_usd=None,
            )
            assert result.status == "completed"
            assert result.result_code == "NO_ORDER_REQUIRED"

        collector = getattr(agent_challenger, "collect_portfolio_challenger_cycle_sample", None)
        assert callable(collector), "multi-candidate completed cycle collector is required"
        async with sessions() as session:
            sample = await collector(
                session, cycle_id=cycle.id, as_of=args["now"] + timedelta(minutes=2)
            )
        assert sample.cycle_id == cycle.id
        assert sample.candidate_batch_id == cycle.candidate_batch_id
        assert sample.candidate_instrument_ids == tuple(sorted(item[0] for item in INSTRUMENTS[:2]))
        assert len(sample.research_run_ids) == 2
        assert sample.synthesis_output_id == args["synthesis_output_id"]
        assert sample.result_code == "NO_ORDER_REQUIRED"
        assert sample.order_ids == ()
        assert sample.fill_ids == ()
        assert sample.fill_ledger_transaction_ids == ()
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_unfinished_portfolio_cycle_is_not_an_improvement_sample(
    migrated_database_url: str,
) -> None:
    engine, sessions, args = await proposal_source(migrated_database_url)
    try:
        with pytest.raises(AgentChallengerError, match="PORTFOLIO_CHALLENGER_SAMPLE_INCOMPLETE"):
            async with sessions() as session:
                await agent_challenger.collect_portfolio_challenger_cycle_sample(
                    session, cycle_id=args["cycle_id"], as_of=args["now"] + timedelta(minutes=2)
                )
    finally:
        await engine.dispose()
