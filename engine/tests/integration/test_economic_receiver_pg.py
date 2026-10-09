"""Use the normal synthesis admission and frozen input, never provider calls."""

from datetime import date, timedelta
from uuid import UUID

import pytest
from nexus.agents.portfolio_synthesis_persistence import admit_portfolio_synthesis
from nexus.paper.economic_budget_hold import AllocationRequest
from nexus.paper.economic_receiver import require_economic_receiver
from nexus.paper.economic_version_binding import VersionBinding
from nexus.paper.portfolio_economic_preflight import EconomicPlanRequest, compile_plan
from nexus.persistence.models.market import InstrumentAlias
from nexus.persistence.models.runtime import Job
from sqlalchemy import func, select
from test_portfolio_synthesis_persistence_pg import seed_synthesis


@pytest.mark.asyncio
async def test_normal_synthesis_frozen_receiver_and_lane_prompt_are_bound(migrated_database_url):
    engine, sessions, kwargs = await seed_synthesis(migrated_database_url)
    try:
        async with sessions.begin() as session:
            run = (await admit_portfolio_synthesis(session, **kwargs)).run
            ids = run.frozen_input["candidate_instrument_ids"]
            symbols = [
                await session.scalar(
                    select(InstrumentAlias.symbol).where(
                        InstrumentAlias.instrument_id == UUID(item)
                    )
                )
                for item in ids
            ]
            plan = compile_plan(
                EconomicPlanRequest(
                    evaluation_key="normal-receiver-case",
                    instruments=tuple(
                        {"instrument_id": UUID(item), "symbol": symbol}
                        for item, symbol in zip(ids, symbols, strict=True)
                    ),
                    start_session=date(2027, 1, 4),
                    research_cost_usd="1",
                    synthesis_cost_usd="1",
                    global_daily_limit_usd="20",
                ),
                now=run.decision_time,
            )
            binding = VersionBinding(
                protocol_hash=plan.content_hash,
                proposal_hash="a" * 64,
                publication_event_hash="b" * 64,
                shared_model_id=run.model_config_id,
                shared_model_version=run.pricing_version,
                shared_model_hash=run.model_hash,
                incumbent_prompt_id=run.prompt_version_id,
                incumbent_prompt_hash=run.prompt_hash,
                challenger_prompt_id=UUID(int=999),
                challenger_prompt_hash="c" * 64,
                pricing_available_at=run.decision_time - timedelta(days=1),
                pricing_expires_at=run.decision_time + timedelta(days=2),
                configured_cost_usd="1",
                content_hash="d" * 64,
            )
            allocation = AllocationRequest(
                hold_event_id=UUID(int=900), lane="incumbent", action="synthesis"
            )
            before = await session.scalar(select(func.count()).select_from(Job))
            require_economic_receiver(plan, binding, allocation, run)
            with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_RECEIVER_INVALID"):
                require_economic_receiver(
                    plan, binding, allocation.model_copy(update={"lane": "challenger"}), run
                )
            original = run.input_hash
            run.input_hash = "0" * 64
            with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_RECEIVER_INVALID"):
                require_economic_receiver(plan, binding, allocation, run)
            run.input_hash = original
            different = plan.model_copy(
                update={
                    "request": plan.request.model_copy(
                        update={"instruments": tuple(reversed(plan.request.instruments))}
                    )
                }
            )
            with pytest.raises(ValueError, match="ECONOMIC_TRANSFER_RECEIVER_INVALID"):
                require_economic_receiver(different, binding, allocation, run)
            require_economic_receiver(plan, binding, allocation, run)
            assert await session.scalar(select(func.count()).select_from(Job)) == before
    finally:
        await engine.dispose()
