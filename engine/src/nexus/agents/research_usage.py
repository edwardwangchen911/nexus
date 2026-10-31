"""Shared research occupancy from original reservations and verified usage facts.

This projection neither settles nor releases anything. Terminal membership stays
frozen; pre-terminal verified stages remain visible even if finalization rolls back.
"""

from decimal import Decimal
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.agents.synthesis import observed_usage_cost
from nexus.persistence.models.agents import (
    AgentDailyBudgetReservationRecord,
    AgentModelConfigRecord,
    AgentResearchRunRecord,
)


async def research_reserved_occupancy(
    session: AsyncSession, reservation: AgentDailyBudgetReservationRecord
) -> Decimal:
    """Count max(original reservation, verified cost lower bound), never their sum."""
    if reservation.state != "reserved" or reservation.settled_usd is not None:
        raise ValueError("RESEARCH_USAGE_RESERVATION_INVALID")
    # A caller may have loaded this Run before waiting for the accounting lock.
    # Read mutable facts afresh without expiring/overwriting its ORM write set.
    run = (
        await session.execute(
            select(
                AgentResearchRunRecord.id,
                AgentResearchRunRecord.input_tokens,
                AgentResearchRunRecord.output_tokens,
                AgentResearchRunRecord.model_config_id,
                AgentResearchRunRecord.model_hash,
                AgentResearchRunRecord.pricing_version,
                AgentResearchRunRecord.reserved_cost_usd,
            ).where(AgentResearchRunRecord.id == reservation.run_id)
        )
    ).one_or_none()
    if run is None:
        raise ValueError("RESEARCH_USAGE_AUTHORITY_INVALID")
    rows = None
    tokens = (run.input_tokens, run.output_tokens)
    if await session.scalar(
        text("SELECT to_regclass('research_stage_member')")
    ) is not None and await session.scalar(
        text("SELECT 1 FROM research_stage_member WHERE run_id=:run"), {"run": run.id}
    ):
        from nexus.agents.research_stages.protocol import (
            evidence_digest,
            known_usage_lower_bound,
            stage_evidence,
            usage_known,
        )

        terminal = (
            (
                await session.execute(
                    text(
                        "SELECT evidence,chain_hash FROM research_stage_terminal WHERE run_id=:run"
                    ),
                    {"run": run.id},
                )
            )
            .mappings()
            .one_or_none()
        )
        rows = await stage_evidence(
            session, run.id, None if terminal is None else terminal["evidence"]
        )
        lower = known_usage_lower_bound(rows)
        tokens = (lower.input_tokens, lower.output_tokens)
        if terminal is not None:
            if rows != terminal["evidence"] or evidence_digest(rows) != terminal["chain_hash"]:
                raise ValueError("RESEARCH_USAGE_EVIDENCE_INVALID")
            reported = (run.input_tokens, run.output_tokens)
            if reported != tokens and not (tokens == (0, 0) and reported == (None, None)):
                raise ValueError("RESEARCH_USAGE_EVIDENCE_INVALID")
        if not any(usage_known((row,)) for row in rows):
            return reservation.reserved_usd
    if tokens == (None, None):
        return reservation.reserved_usd
    if tokens[0] is None or tokens[1] is None:
        raise ValueError("RESEARCH_USAGE_EVIDENCE_INVALID")
    model = await session.get(AgentModelConfigRecord, run.model_config_id)
    if (
        model is None
        or run.model_hash != model.config_hash
        or run.pricing_version != reservation.pricing_version
        or run.reserved_cost_usd != reservation.reserved_usd
    ):
        raise ValueError("RESEARCH_USAGE_AUTHORITY_INVALID")
    from nexus.agents.platform_usage import PLATFORM_PRICE_VERSION

    if model.version == PLATFORM_PRICE_VERSION:
        from nexus.agents.platform_profile import platform_model_matches
        from nexus.agents.platform_stage_accounting import platform_stage_accounting

        if rows is None or not platform_model_matches(model):
            raise ValueError("RESEARCH_USAGE_EVIDENCE_INVALID")
        return max(reservation.reserved_usd, platform_stage_accounting(rows).known_cost_usd)
    return max(reservation.reserved_usd, observed_usage_cost(model, tokens[0], tokens[1]))


async def require_unspent_research_reservation(session: AsyncSession, *, run_id: UUID) -> None:
    """A held observed/authorized call cannot be converted into unused capacity."""
    run = await session.get(AgentResearchRunRecord, run_id)
    if run is None or run.input_tokens is not None or run.output_tokens is not None:
        raise ValueError("RESEARCH_USAGE_RELEASE_FORBIDDEN")
    if await session.scalar(
        text("SELECT to_regclass('research_stage_authorization')")
    ) is not None and await session.scalar(
        text("SELECT 1 FROM research_stage_authorization WHERE run_id=:run LIMIT 1"),
        {"run": run_id},
    ):
        raise ValueError("RESEARCH_USAGE_RELEASE_FORBIDDEN")


async def require_research_accounting_clear(session: AsyncSession) -> None:
    """New money admission cannot waive a prior-day unsettled accounting fact.

    Ordinary in-flight reservations and unknown prefixes within their original
    reservation retain existing policies. No cost is moved to the current day.
    New allocations must hold the shared accounting/date mutex. Read-only
    recovery may inspect without it; such an observation grants no reservation.
    """
    from sqlalchemy import select

    from nexus.agents.budget import BudgetExhausted
    from nexus.persistence.models.agents import AgentResearchOutputRecord

    staged = await session.scalar(text("SELECT to_regclass('research_stage_terminal')"))
    for reservation in await session.scalars(
        select(AgentDailyBudgetReservationRecord).where(
            AgentDailyBudgetReservationRecord.state == "reserved"
        )
    ):
        if await research_reserved_occupancy(session, reservation) > reservation.reserved_usd:
            raise BudgetExhausted("RESEARCH_ACCOUNTING_UNSETTLED")
        status = await session.scalar(
            select(AgentResearchRunRecord.status).where(
                AgentResearchRunRecord.id == reservation.run_id
            )
        )
        if status not in {"completed", "failed", "abstained"}:
            continue
        known_terminal = (
            False
            if staged is None
            else await session.scalar(
                text("SELECT known FROM research_stage_terminal WHERE run_id=:run"),
                {"run": reservation.run_id},
            )
        )
        last_reason = await session.scalar(
            select(AgentResearchOutputRecord.payload["reason"].astext)
            .where(AgentResearchOutputRecord.run_id == reservation.run_id)
            .order_by(AgentResearchOutputRecord.sequence.desc())
            .limit(1)
        )
        if known_terminal is True or last_reason == "usage_accounting_failed":
            raise BudgetExhausted("RESEARCH_ACCOUNTING_UNSETTLED")
