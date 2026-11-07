"""All-qualified native research must bind to its original formal decision."""

import asyncio
from pathlib import Path
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from integration.test_compact_sol_lifecycle_pg import compact_registration  # noqa: F401
from integration.test_sol_native_stage_pg import (
    test_normal_sol_formal_research_reconciles_monitor_plans_once as scenario,
)
from integration.test_sol_portfolio_responses_pg import _run_native_portfolio_http
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError


@pytest.mark.asyncio
@pytest.mark.parametrize("profile", ["v2", "compact"])
async def test_all_qualified_native_roots_reach_persisted_portfolio(
    migrated_database_url, monkeypatch, tmp_path, request, profile
):
    if profile == "compact":
        request.getfixturevalue("compact_registration")

    async def checked_portfolio(factory, host, portfolio, *args):
        await _run_native_portfolio_http(
            factory, host, portfolio, *args, expected_complete_code="NO_ORDER_REQUIRED"
        )
        async with factory() as session:
            bindings = (
                (
                    await session.execute(
                        text("""
                SELECT b.* FROM strategy_decision_agent_outputs b
                JOIN strategy_decisions d ON d.id=b.strategy_decision_id
                WHERE d.agent_paper_cycle_id=:cycle
            """),
                        {"cycle": portfolio.cycle_id},
                    )
                )
                .mappings()
                .all()
            )
            assert len(bindings) == 3
            for binding in bindings:
                assert await session.scalar(
                    text("SELECT nexus_sol_formal_output_binding_valid(:decision,:output)"),
                    {
                        "decision": binding["strategy_decision_id"],
                        "output": binding["agent_research_output_id"],
                    },
                )
                assert not await session.scalar(
                    text("SELECT nexus_sol_formal_output_binding_valid(:decision,:output)"),
                    {"decision": uuid4(), "output": binding["agent_research_output_id"]},
                )
            original = dict(bindings[0])
            from datetime import timedelta

            from nexus.paper.management_read_authority import load_proposal_read_authority
            from sqlalchemy import func, select

            authority = await load_proposal_read_authority(
                session,
                account_id=host.agent_paper_account_id,
                decision_id=original["strategy_decision_id"],
                read_as_of=await session.scalar(select(func.clock_timestamp()))
                + timedelta(minutes=15),
            )
            assert authority is not None and authority.persisted.cycle_id == portfolio.cycle_id
            cutoff = await session.scalar(select(func.clock_timestamp()))
            prior_close = await session.scalar(
                text(
                    "SELECT max(market_close_at) FROM exchange_calendar_sessions "
                    "WHERE market_close_at<:at"
                ),
                {"at": portfolio.decision_time},
            )
        from integration.test_candidate_multistock_path import _set_test_insert_clock
        from nexus.ops.agent_paper_loop import publish_due_performance_once

        await _set_test_insert_clock(factory, cutoff)
        assert await publish_due_performance_once(
            host,
            account_id=host.agent_paper_account_id,
            as_of=prior_close,
            enqueue_session_summary=False,
        )
        async with factory() as session:
            reread = await load_proposal_read_authority(
                session,
                account_id=host.agent_paper_account_id,
                decision_id=original["strategy_decision_id"],
                read_as_of=cutoff + timedelta(minutes=15),
            )
            assert reread is not None
            assert reread.persisted == authority.persisted
            assert reread.decision.id == authority.decision.id
            from nexus.paper.agent_decision_store import _rebuild_portfolio_proposal
            from nexus.persistence.models.paper import AgentPaperCycleRecord

            cycle = await session.get(AgentPaperCycleRecord, portfolio.cycle_id)
            with pytest.raises(ValueError, match="PORTFOLIO_PROPOSAL_PROJECTION_CONFLICT"):
                await _rebuild_portfolio_proposal(
                    session,
                    cycle=cycle,
                    output_id=authority.decision.portfolio_synthesis_output_id,
                    now=cutoff,
                    formal_stage_capacity=True,
                )
        with pytest.raises(DBAPIError, match="agent decision output binding is invalid"):
            async with factory.begin() as session:
                await session.execute(
                    text("""
                    INSERT INTO strategy_decision_agent_outputs
                    (strategy_decision_id,agent_research_output_id,intent_hash,sizing_action,binding_hash)
                    VALUES (:strategy_decision_id,:agent_research_output_id,
                            :intent_hash,:sizing_action,:binding_hash)
                """),
                    original,
                )
        config = Config(str(Path(__file__).resolve().parents[2] / "alembic.ini"))
        with pytest.raises(DBAPIError, match="SOL_OUTPUT_BINDING_DOWNGRADE_FORBIDDEN"):
            await asyncio.to_thread(command.downgrade, config, "0119_compact_budget_obligations")
        async with factory() as session:
            assert (
                await session.scalar(text("SELECT version_num FROM alembic_version"))
                == "0120_sol_formal_output_binding"
            )

    await scenario(
        migrated_database_url,
        monkeypatch,
        tmp_path,
        "complete",
        checked_portfolio,
        all_approved=True,
    )
