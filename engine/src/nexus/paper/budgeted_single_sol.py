"""Versioned independent owners reuse one immutable daily protection context."""

from datetime import date
from decimal import Decimal
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.paper.research_request import database_now
from nexus.paper.single_sol_budget import (
    freeze_single_sol_budget,
    has_guard,
    require_single_sol_capacity,
)
from nexus.paper.single_sol_obligations import original_slot
from nexus.paper.single_sol_owner import BudgetedSingleSolBinding, binding_for
from nexus.persistence.models.agents import AgentModelConfigRecord
from nexus.persistence.models.manual_paper import ManualPaperRequest
from nexus.settings import EngineSettings


async def protection_context_hash(
    session: AsyncSession,
    settings: EngineSettings,
    *,
    day: date,
    calendar_id: UUID,
) -> str:
    """Bind immutable policy, never the changing balance or the new owner's identity."""
    model = await session.get(AgentModelConfigRecord, settings.agent_paper_formal_model_config_id)
    if model is None or settings.agent_paper_account_id is None:
        raise ValueError("SINGLE_SOL_BUDGET_SCOPE_INVALID")
    if (await database_now(session)).date() != day:
        raise ValueError("SINGLE_SOL_BUDGET_WINDOW_CLOSED")
    expected = await session.scalar(
        text("""SELECT single_sol_day_context_hash(
          CAST(:day AS date),CAST(:account AS uuid),CAST(:batch AS uuid),CAST(:model AS uuid),
          CAST(:calendar AS uuid),CAST(:slot AS timestamptz),CAST(:limit AS numeric),
          CAST(:maximum AS numeric))"""),
        dict(
            day=day,
            account=settings.agent_paper_account_id,
            batch=settings.agent_paper_candidate_batch_id,
            model=model.id,
            calendar=calendar_id,
            slot=original_slot(day),
            limit=settings.agent_daily_budget_usd,
            maximum=model.worst_case_cost_usd,
        ),
    )
    if not isinstance(expected, str) or len(expected) != 64:
        raise ValueError("SINGLE_SOL_BUDGET_SCOPE_INVALID")
    if await has_guard(session, day):
        actual = await session.scalar(
            text("""SELECT single_sol_day_context_hash(budget_day,account_id,batch_id,
            model_id,calendar_id,slot,daily_limit,maximum)
            FROM single_sol_budget_scopes WHERE budget_day=:day"""),
            {"day": day},
        )
        if actual != expected:
            raise ValueError("SINGLE_SOL_BUDGET_SCOPE_INVALID")
    return expected


async def freeze_or_reuse_protection(
    session: AsyncSession, settings: EngineSettings, *, row: ManualPaperRequest, run_id: UUID
) -> None:
    """The new owner and reservation must be born together; never adopt an old reserve."""
    binding = binding_for(row)
    if row.contract_version != 5 or not isinstance(binding, BudgetedSingleSolBinding):
        raise ValueError("SINGLE_SOL_REQUEST_BINDING_INVALID")
    current = await protection_context_hash(
        session, settings, day=binding.budget_day, calendar_id=binding.calendar_id
    )
    if current != binding.protection_context_hash:
        raise ValueError("SINGLE_SOL_REQUEST_BINDING_INVALID")
    if not await has_guard(session, binding.budget_day):
        await freeze_single_sol_budget(
            session,
            settings,
            run_id=run_id,
            instrument_id=binding.instrument_id,
            calendar_authority_id=binding.calendar_id,
            source_actor="local-budgeted-single-sol-v1",
        )
    owned = await session.scalar(
        text("""SELECT 1 FROM agent_research_runs r
        JOIN agent_daily_budget_reservations b ON b.run_id=r.id
        JOIN agent_model_configs m ON m.id=r.model_config_id
        WHERE r.id=:run AND r.actor_id='local-budgeted-single-sol-v1'
        AND r.idempotency_key='budgeted-single-sol:'||CAST(CAST(:request AS uuid) AS text)||':'||
                CAST(CAST(:instrument AS uuid) AS text)
        AND r.instrument_id=:instrument AND r.model_config_id=:model AND r.status='prepared'
        AND r.model_hash=m.config_hash AND r.pricing_version=m.version
        AND b.pricing_version=m.version AND b.business_date=:day AND b.state='reserved'
        AND b.settled_usd IS NULL AND b.reserved_usd=r.reserved_cost_usd
        AND pg_xact_status(b.xmin::text::xid8)='in progress'"""),
        dict(
            run=run_id,
            request=row.id,
            instrument=binding.instrument_id,
            model=row.model_config_id,
            day=binding.budget_day,
        ),
    )
    if not owned or not await require_single_sol_capacity(
        session,
        kind="dispatch",
        run_id=run_id,
        now=await database_now(session),
        daily_limit_usd=settings.agent_daily_budget_usd,
        amount=Decimal(0),
    ):
        raise ValueError("SINGLE_SOL_BUDGET_SCOPE_INVALID")
