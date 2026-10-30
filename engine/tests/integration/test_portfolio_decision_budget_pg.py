"""PostgreSQL proof that both Agent run types share one daily budget."""

import asyncio
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from nexus.agents.budget import BudgetExhausted
from nexus.agents.persistence import (
    release_portfolio_decision_budget_if_reserved,
    require_reserved_portfolio_decision_budget,
    reserve_daily_budget,
    reserve_portfolio_decision_budget,
    settle_full_portfolio_decision_budget,
    settle_portfolio_decision_budget,
)
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    PortfolioDecisionBudgetReservationRecord,
    PortfolioDecisionRunRecord,
)
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

_NOW = datetime(2026, 9, 10, 8, tzinfo=UTC)


@pytest.mark.asyncio
async def test_portfolio_decision_reservation_counts_instrument_research_usage(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    decision_run_id = uuid4()
    existing_research_run_id = uuid4()
    await _seed_runs(
        factory, research_run_ids=(existing_research_run_id,), decision_run_ids=(decision_run_id,)
    )
    async with factory() as session, session.begin():
        await reserve_daily_budget(
            session,
            run_id=existing_research_run_id,
            now=_NOW,
            daily_limit_usd=Decimal("1.50"),
            worst_case_usd=Decimal("1.00"),
            pricing_version="pricing-v1",
            pricing_fresh=True,
        )

    async with factory() as session, session.begin():
        with pytest.raises(BudgetExhausted, match="daily research budget exhausted"):
            await reserve_portfolio_decision_budget(
                session,
                run_id=decision_run_id,
                now=_NOW,
                daily_limit_usd=Decimal("1.50"),
                worst_case_usd=Decimal("0.51"),
                pricing_version="pricing-v1",
                pricing_fresh=True,
            )

    async with factory() as session, session.begin():
        created = await reserve_portfolio_decision_budget(
            session,
            run_id=decision_run_id,
            now=_NOW,
            daily_limit_usd=Decimal("1.50"),
            worst_case_usd=Decimal("0.50"),
            pricing_version="pricing-v1",
            pricing_fresh=True,
        )

    async with factory() as session:
        persisted = await session.scalar(
            select(PortfolioDecisionBudgetReservationRecord).where(
                PortfolioDecisionBudgetReservationRecord.run_id == decision_run_id
            )
        )
    assert persisted is not None
    assert persisted.id == created.id
    assert (persisted.reserved_usd, persisted.state) == (Decimal("0.500000000000"), "reserved")


@pytest.mark.asyncio
async def test_concurrent_cross_type_reservations_cannot_oversubscribe(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    research_run_id = uuid4()
    decision_run_id = uuid4()
    await _seed_runs(
        factory,
        research_run_ids=(research_run_id,),
        decision_run_ids=(decision_run_id,),
    )
    barrier = asyncio.Barrier(2)

    async def reserve_research() -> AgentDailyBudgetReservationRecord:
        async with factory() as session, session.begin():
            await barrier.wait()
            return await reserve_daily_budget(
                session,
                run_id=research_run_id,
                now=_NOW,
                daily_limit_usd=Decimal("1.00"),
                worst_case_usd=Decimal("0.75"),
                pricing_version="pricing-v1",
                pricing_fresh=True,
            )

    async def reserve_decision() -> PortfolioDecisionBudgetReservationRecord:
        async with factory() as session, session.begin():
            await barrier.wait()
            return await reserve_portfolio_decision_budget(
                session,
                run_id=decision_run_id,
                now=_NOW,
                daily_limit_usd=Decimal("1.00"),
                worst_case_usd=Decimal("0.75"),
                pricing_version="pricing-v1",
                pricing_fresh=True,
            )

    results = await asyncio.wait_for(
        asyncio.gather(reserve_research(), reserve_decision(), return_exceptions=True),
        timeout=5,
    )
    assert sum(isinstance(result, BudgetExhausted) for result in results) == 1
    async with factory() as session:
        research_reservation = await session.scalar(
            select(AgentDailyBudgetReservationRecord).where(
                AgentDailyBudgetReservationRecord.run_id == research_run_id
            )
        )
        decision_reservation = await session.scalar(
            select(PortfolioDecisionBudgetReservationRecord).where(
                PortfolioDecisionBudgetReservationRecord.run_id == decision_run_id
            )
        )
    assert sum(record is not None for record in (research_reservation, decision_reservation)) == 1


@pytest.mark.asyncio
async def test_research_reservation_counts_settled_and_ignores_released_decision_budget(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    research_run_id = uuid4()
    settled_run_id = uuid4()
    released_run_id = uuid4()
    await _seed_runs(
        factory,
        research_run_ids=(research_run_id,),
        decision_run_ids=(settled_run_id, released_run_id),
    )
    async with factory() as session, session.begin():
        session.add_all(
            (
                PortfolioDecisionBudgetReservationRecord(
                    business_date=_NOW.date(),
                    run_id=settled_run_id,
                    pricing_version="pricing-v1",
                    reserved_usd=Decimal("0.90"),
                    settled_usd=Decimal("0.40"),
                    state="settled",
                ),
                PortfolioDecisionBudgetReservationRecord(
                    business_date=_NOW.date(),
                    run_id=released_run_id,
                    pricing_version="pricing-v1",
                    reserved_usd=Decimal("0.90"),
                    settled_usd=None,
                    state="released",
                ),
            )
        )

    async with factory() as session, session.begin():
        with pytest.raises(BudgetExhausted, match="daily research budget exhausted"):
            await reserve_daily_budget(
                session,
                run_id=research_run_id,
                now=_NOW,
                daily_limit_usd=Decimal("1.00"),
                worst_case_usd=Decimal("0.61"),
                pricing_version="pricing-v1",
                pricing_fresh=True,
            )

    async with factory() as session, session.begin():
        reservation = await reserve_daily_budget(
            session,
            run_id=research_run_id,
            now=_NOW,
            daily_limit_usd=Decimal("1.00"),
            worst_case_usd=Decimal("0.60"),
            pricing_version="pricing-v1",
            pricing_fresh=True,
        )
    assert reservation.reserved_usd == Decimal("0.60")


@pytest.mark.asyncio
async def test_portfolio_decision_budget_lifecycle_preserves_known_unknown_and_released_usage(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    known_id, unknown_id, released_id = uuid4(), uuid4(), uuid4()
    await _seed_runs(
        factory,
        research_run_ids=(),
        decision_run_ids=(known_id, unknown_id, released_id),
    )
    for run_id in (known_id, unknown_id, released_id):
        async with factory() as session, session.begin():
            await reserve_portfolio_decision_budget(
                session,
                run_id=run_id,
                now=_NOW,
                daily_limit_usd=Decimal("10"),
                worst_case_usd=Decimal("1"),
                pricing_version="pricing-v1",
                pricing_fresh=True,
            )

    async with factory() as session, session.begin():
        await require_reserved_portfolio_decision_budget(session, run_id=known_id)
        await settle_portfolio_decision_budget(
            session,
            run_id=known_id,
            actual_usd=Decimal("0.25"),
            input_tokens=100,
            output_tokens=50,
        )
    async with factory() as session, session.begin():
        await settle_full_portfolio_decision_budget(session, run_id=unknown_id)
    async with factory() as session, session.begin():
        await release_portfolio_decision_budget_if_reserved(session, run_id=released_id)

    async with factory() as session:
        reservations = {
            row.run_id: row
            for row in await session.scalars(
                select(PortfolioDecisionBudgetReservationRecord).where(
                    PortfolioDecisionBudgetReservationRecord.run_id.in_(
                        (known_id, unknown_id, released_id)
                    )
                )
            )
        }
        runs = {
            row.id: row
            for row in await session.scalars(
                select(PortfolioDecisionRunRecord).where(
                    PortfolioDecisionRunRecord.id.in_((known_id, unknown_id, released_id))
                )
            )
        }
    assert (
        reservations[known_id].state,
        reservations[known_id].settled_usd,
        runs[known_id].settled_cost_usd,
        runs[known_id].input_tokens,
        runs[known_id].output_tokens,
    ) == ("settled", Decimal("0.250000000000"), Decimal("0.250000000000"), 100, 50)
    assert (
        reservations[unknown_id].state,
        reservations[unknown_id].settled_usd,
        runs[unknown_id].settled_cost_usd,
        runs[unknown_id].input_tokens,
        runs[unknown_id].output_tokens,
    ) == ("settled", Decimal("1.000000000000"), Decimal("1.000000000000"), None, None)
    assert (
        reservations[released_id].state,
        reservations[released_id].settled_usd,
        runs[released_id].settled_cost_usd,
    ) == ("released", None, None)


async def _seed_runs(
    factory: async_sessionmaker[AsyncSession],
    *,
    research_run_ids: tuple[UUID, ...],
    decision_run_ids: tuple[UUID, ...],
) -> None:
    async with factory() as session, session.begin():
        await session.execute(text("SET LOCAL session_replication_role = replica"))
        for index, run_id in enumerate(research_run_ids):
            await session.execute(
                text(
                    "INSERT INTO agent_research_runs ("
                    "id,instrument_id,data_snapshot_id,prompt_version_id,model_config_id,"
                    "decision_time,actor_id,idempotency_key,status,retry_count,schema_hash,"
                    "model_hash,prompt_hash,prompt_manifest,evidence_hash,evidence_manifest,"
                    "tool_hash,input_hash,reserved_cost_usd,pricing_version) VALUES ("
                    ":id,:instrument,:snapshot,:prompt,:model,:now,'test',:key,'prepared',0,"
                    ":hash,:hash,:hash,'{}'::jsonb,:hash,'[{}]'::jsonb,:hash,:hash,1,'pricing-v1')"
                ),
                {
                    "id": run_id,
                    "instrument": uuid4(),
                    "snapshot": uuid4(),
                    "prompt": uuid4(),
                    "model": uuid4(),
                    "now": _NOW,
                    "key": f"research-{index}-{run_id}",
                    "hash": "a" * 64,
                },
            )
        for index, run_id in enumerate(decision_run_ids):
            await session.execute(
                text(
                    "INSERT INTO portfolio_decision_runs ("
                    "id,portfolio_id,portfolio_snapshot_id,diagnostic_report_id,prompt_version_id,"
                    "model_config_id,actor_id,idempotency_key,request_hash,status,retry_count,"
                    "decision_time,input_hash,schema_hash,frozen_input,prompt_hash,model_hash,"
                    "tool_hash,evidence_hash,evidence_manifest,provider_phase,reserved_cost_usd,"
                    "pricing_version) VALUES ("
                    ":id,:portfolio,:snapshot,:diagnostic,:prompt,:model,'test',:key,:hash,"
                    "'prepared',0,:now,:hash,:hash,'{}'::jsonb,:hash,:hash,:hash,:hash,"
                    "'[{}]'::jsonb,'none',1,'pricing-v1')"
                ),
                {
                    "id": run_id,
                    "portfolio": uuid4(),
                    "snapshot": uuid4(),
                    "diagnostic": uuid4(),
                    "prompt": uuid4(),
                    "model": uuid4(),
                    "key": f"decision-{index}-{run_id}",
                    "hash": "b" * 64,
                    "now": _NOW,
                },
            )
