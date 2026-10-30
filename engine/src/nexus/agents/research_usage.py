"""Shared research occupancy from original reservations and verified usage facts.

This projection neither settles nor releases anything. Terminal membership stays
frozen; pre-terminal verified stages remain visible even if finalization rolls back.
"""

from decimal import Decimal
from uuid import UUID

from sqlalchemy import text
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
    run = await session.get(AgentResearchRunRecord, reservation.run_id)
    if run is None:
        raise ValueError("RESEARCH_USAGE_AUTHORITY_INVALID")
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
