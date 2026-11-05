"""One database predicate shared by admission, committed credits and readback.

This is only a natural-obligation funding relation, never permission to change a
frozen request, select a different model or reinterpret an independent v4/v5 root.
"""

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.agents import AgentModelConfigRecord
from nexus.settings import EngineSettings


async def budget_model_compatible(
    session: AsyncSession, *, expected: UUID, candidate: UUID, maximum: Decimal
) -> bool:
    return (
        await session.scalar(
            text("SELECT single_sol_budget_model_compatible(:expected,:candidate,:maximum)"),
            dict(expected=expected, candidate=candidate, maximum=maximum),
        )
        is True
    )


async def inspect_service_budget_scope(
    session: AsyncSession, settings: EngineSettings, *, now: datetime
) -> dict[str, object]:
    """Readiness only. Actual allocation still rechecks under the shared day lock.

    Active/unknown owners are checked by the existing account-occupancy reader.
    Unconverted, still-valid explicit intents also retain their exact model choice.
    """
    from nexus.paper.single_sol_budget import remaining_obligations

    selected = settings.agent_paper_formal_model_config_id
    if await session.scalar(
        text("""SELECT 1 FROM formal_intraday_intents
          WHERE account_id=:account AND state IN ('pending','checking')
          AND expires_at>:now AND model_id IS DISTINCT FROM CAST(:model AS uuid) LIMIT 1"""),
        dict(account=settings.agent_paper_account_id, now=now, model=selected),
    ):
        raise ValueError("SINGLE_SOL_FROZEN_MODEL_CONFLICT")
    guard = (
        (
            await session.execute(
                text("""SELECT g.*,b.candidate_instrument_ids AS members
              FROM single_sol_budget_scopes g JOIN agent_paper_candidate_batches b
              ON b.id=g.batch_id AND b.account_id=g.account_id WHERE g.budget_day=:day"""),
                {"day": now.date()},
            )
        )
        .mappings()
        .one_or_none()
    )
    if guard is None:
        return {"status": "no_guard", "remaining_obligations": 0}
    if (
        guard["account_id"] != settings.agent_paper_account_id
        or guard["batch_id"] != settings.agent_paper_candidate_batch_id
        or guard["daily_limit"] != settings.agent_daily_budget_usd
    ):
        raise ValueError("SINGLE_SOL_SERVICE_SCOPE_MISMATCH")
    remaining, _ = await remaining_obligations(
        session, day=now.date(), members=guard["members"], now=now
    )
    if not remaining:
        return {"status": "fulfilled", "remaining_obligations": 0}
    model = await session.get(AgentModelConfigRecord, selected) if selected is not None else None
    if (
        model is None
        or model.worst_case_cost_usd != guard["maximum"]
        or not await budget_model_compatible(
            session, expected=guard["model_id"], candidate=model.id, maximum=guard["maximum"]
        )
    ):
        raise ValueError("SINGLE_SOL_SERVICE_SCOPE_MISMATCH")
    return {
        "status": "exact" if model.id == guard["model_id"] else "compact",
        "remaining_obligations": len(remaining),
    }
