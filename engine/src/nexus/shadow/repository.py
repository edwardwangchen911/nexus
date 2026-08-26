"""Locked persistence reads and writes for Proposal Shadow startup."""

from collections.abc import Mapping
from datetime import date, datetime
from decimal import Decimal
from typing import cast
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.backtest.engine import exchange_calendar_content_hash
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.optimization import (
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
    ProposalPositionRecord,
)
from nexus.shadow.models import SHADOW_VARIANTS, ShadowVariant


async def lock_proposal_group(
    session: AsyncSession, group_id: UUID
) -> OptimizationProposalGroupRecord | None:
    """Lock the proposal root after the idempotency claim."""
    return cast(
        OptimizationProposalGroupRecord | None,
        await session.scalar(
            select(OptimizationProposalGroupRecord)
            .where(OptimizationProposalGroupRecord.id == group_id)
            .with_for_update()
        ),
    )


async def load_proposals_and_positions(
    session: AsyncSession,
    group_id: UUID,
) -> tuple[
    tuple[OptimizationProposalRecord, OptimizationProposalRecord, OptimizationProposalRecord],
    Mapping[UUID, tuple[ProposalPositionRecord, ...]],
]:
    """Load the exact immutable three-proposal bundle in fixed variant order."""
    records = tuple(
        await session.scalars(
            select(OptimizationProposalRecord).where(
                OptimizationProposalRecord.proposal_group_id == group_id
            )
        )
    )
    by_variant = {record.variant: record for record in records}
    if len(records) != 3 or set(by_variant) != set(SHADOW_VARIANTS):
        raise ValueError("PROPOSAL_NOT_EXECUTABLE")
    ordered = cast(
        tuple[
            OptimizationProposalRecord,
            OptimizationProposalRecord,
            OptimizationProposalRecord,
        ],
        tuple(by_variant[variant] for variant in SHADOW_VARIANTS),
    )
    position_rows = tuple(
        await session.scalars(
            select(ProposalPositionRecord)
            .join(OptimizationProposalRecord)
            .where(OptimizationProposalRecord.proposal_group_id == group_id)
        )
    )
    positions: dict[UUID, list[ProposalPositionRecord]] = {
        proposal.id: [] for proposal in ordered
    }
    for position in position_rows:
        positions[position.proposal_id].append(position)
    return ordered, {
        proposal_id: tuple(
            sorted(values, key=lambda item: item.instrument_id.int)
        )
        for proposal_id, values in positions.items()
    }


async def load_frozen_calendar(
    session: AsyncSession,
    *,
    market_date: date,
    started_at: datetime,
) -> tuple[ExchangeCalendarAuthorityRecord, tuple[date, ...]]:
    """Choose one published complete XNYS authority with 63+5 future sessions."""
    authorities = tuple(
        await session.scalars(
            select(ExchangeCalendarAuthorityRecord)
            .where(
                ExchangeCalendarAuthorityRecord.exchange == "XNYS",
                ExchangeCalendarAuthorityRecord.completeness_status == "complete",
                ExchangeCalendarAuthorityRecord.published_at <= started_at,
                ExchangeCalendarAuthorityRecord.range_end > market_date,
            )
            .order_by(
                ExchangeCalendarAuthorityRecord.published_at.desc(),
                ExchangeCalendarAuthorityRecord.version.desc(),
                ExchangeCalendarAuthorityRecord.id.desc(),
            )
        )
    )
    for authority in authorities:
        member_rows = tuple(
            await session.scalars(
                select(ExchangeCalendarSessionRecord)
                .where(
                    ExchangeCalendarSessionRecord.calendar_authority_id
                    == authority.id
                )
                .order_by(ExchangeCalendarSessionRecord.session)
            )
        )
        all_sessions = tuple(row.session for row in member_rows)
        decision_times = {row.session: row.decision_at for row in member_rows}
        if (
            not all_sessions
            or all_sessions[0] != authority.range_start
            or all_sessions[-1] != authority.range_end
            or any(session_date.weekday() >= 5 for session_date in all_sessions)
        ):
            continue
        try:
            content_hash = exchange_calendar_content_hash(
                all_sessions, decision_times
            )
        except (KeyError, ValueError):
            continue
        if content_hash != authority.content_hash:
            continue
        sessions = tuple(
            session_date
            for session_date in all_sessions
            if session_date > market_date
        )[:68]
        if len(sessions) == 68:
            return authority, sessions
    raise ValueError("SHADOW_CALENDAR_INCOMPLETE")


def target_weight_for(
    variant: ShadowVariant,
    position: ProposalPositionRecord,
) -> Decimal:
    """Map only immutable proposal rows to one frozen Shadow target."""
    return position.current_weight if variant == "hold_current" else position.target_weight
