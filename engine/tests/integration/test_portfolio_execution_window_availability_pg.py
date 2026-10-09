"""Portfolio execution must follow the combined decision, not an earlier stock report."""

from datetime import timedelta

import pytest
from helpers.paper_batch import CALENDAR_ID
from integration.test_portfolio_management_e2e import _include_two
from integration.test_portfolio_proposal_pg import proposal_source
from integration.test_portfolio_synthesis_worker_pg import _seed_normal_synthesis
from nexus.paper.agent_cycle import _events, _resume_portfolio_synthesis
from nexus.paper.agent_market import PaperMarketError, resolve_agent_paper_market_scope
from nexus.persistence.models.backtest import ExchangeCalendarSessionRecord
from nexus.persistence.models.paper import (
    AgentPaperCycleRecord,
    PaperOrderEventRecord,
    PaperOrderRecord,
)
from sqlalchemy import select, text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "delay_minutes,submission_delay_minutes", [(0, 0), (15, 0), (15, 2), (None, 0)]
)
async def test_delayed_portfolio_synthesis_window_follows_actual_order_availability(
    migrated_database_url,
    monkeypatch,
    delay_minutes,
    submission_delay_minutes,
):
    import integration.test_portfolio_synthesis_persistence_pg as seed_module

    original_intent = seed_module._safe_no_trade_intent

    def complete_buy(**kwargs):
        source = original_intent(**kwargs)
        assessment = source.synthesis_assessment.model_copy(
            update={
                "stance": "buy_candidate",
                "review_conclusion": "sufficient",
                "reviews": tuple(
                    review.model_copy(
                        update={
                            "resolved": True,
                            "status": "accepted",
                            "missing_evidence": (),
                            "prevented_conclusion": None,
                        }
                    )
                    for review in source.synthesis_assessment.reviews
                ),
            }
        )
        return source.model_copy(
            update={
                "quality_status": "complete",
                "stance": "buy_candidate",
                "exposure_action": "eligible_for_increase",
                "evidence_audit": "complete",
                "synthesis_assessment": assessment,
                "role_votes": tuple(
                    vote.model_copy(update={"vote": "support"}) for vote in source.role_votes
                ),
            }
        )

    monkeypatch.setattr(seed_module, "_safe_no_trade_intent", complete_buy)
    seeded = await _seed_normal_synthesis(migrated_database_url, monkeypatch)
    engine, sessions, kwargs = seeded
    delayed_at = kwargs["now"] + timedelta(minutes=delay_minutes or 0)
    if delay_minutes is None:
        async with sessions() as session:
            close = await session.scalar(
                select(ExchangeCalendarSessionRecord.market_close_at).where(
                    ExchangeCalendarSessionRecord.calendar_authority_id == CALENDAR_ID,
                    ExchangeCalendarSessionRecord.session == kwargs["now"].date(),
                )
            )
            assert close is not None
            delayed_at = close - timedelta(minutes=3)
    kwargs["now"] = delayed_at
    try:
        # Set defaults before insertion in this isolated database. Keep the
        # immutability triggers active; no historic production fact is rewritten.
        async with sessions.begin() as session:
            for table in (
                "agent_portfolio_synthesis_runs",
                "agent_portfolio_synthesis_outputs",
                "strategy_decisions",
                "paper_orders",
            ):
                await session.execute(
                    text(
                        f"ALTER TABLE {table} ALTER COLUMN created_at "
                        f"SET DEFAULT TIMESTAMPTZ '{delayed_at.isoformat()}'"
                    )
                )
            if submission_delay_minutes:
                submitted_at = delayed_at + timedelta(minutes=submission_delay_minutes)
                await session.execute(
                    text(
                        "ALTER TABLE paper_order_events ALTER COLUMN created_at "
                        f"SET DEFAULT TIMESTAMPTZ '{submitted_at.isoformat()}'"
                    )
                )
        _, _, args = await proposal_source(
            migrated_database_url, seeded=seeded, intent_factory=_include_two
        )
        async with sessions.begin() as session:
            cycle = await session.get(AgentPaperCycleRecord, args["cycle_id"])
            result = await _resume_portfolio_synthesis(
                session,
                cycle=cycle,
                events=await _events(session, cycle.id),
                run_ids=(),
                now=args["now"] + timedelta(seconds=1),
                daily_limit_usd=None,
            )
            assert result.status == "awaiting_market", result
            orders = tuple(
                await session.scalars(
                    select(PaperOrderRecord).where(
                        PaperOrderRecord.strategy_decision_id == result.strategy_decision_id
                    )
                )
            )
            assert len(orders) == 2
            for order in orders:
                if delay_minutes is None:
                    with pytest.raises(PaperMarketError, match="PAPER_MARKET_WINDOW_UNAVAILABLE"):
                        await resolve_agent_paper_market_scope(
                            session, cycle_id=cycle.id, instrument_id=order.instrument_id
                        )
                    continue
                scope = await resolve_agent_paper_market_scope(
                    session, cycle_id=cycle.id, instrument_id=order.instrument_id
                )
                assert scope.window_start > order.created_at
                assert scope.window_start > args["now"]
                submitted = await session.scalar(
                    select(PaperOrderEventRecord)
                    .where(
                        PaperOrderEventRecord.order_id == order.id,
                        PaperOrderEventRecord.to_status == "submitted",
                    )
                    .order_by(PaperOrderEventRecord.event_seq)
                    .limit(1)
                )
                assert submitted is not None
                available_at = max(order.created_at, submitted.created_at, submitted.occurred_at)
                assert scope.window_start == available_at.replace(
                    second=0, microsecond=0
                ) + timedelta(minutes=1)
                assert scope == await resolve_agent_paper_market_scope(
                    session, cycle_id=cycle.id, instrument_id=order.instrument_id
                )
    finally:
        await engine.dispose()
