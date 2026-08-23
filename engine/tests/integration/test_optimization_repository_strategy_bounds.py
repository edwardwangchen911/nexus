"""Repository shadow checks repeat the immutable strategy weight bounds."""

from decimal import Decimal
from uuid import UUID

import pytest
from helpers.portfolio_api import INSTRUMENT_ID, settings
from integration.test_optimization_lifecycle import _seed_complete_group
from nexus.optimizer.repository import transition_group
from nexus.persistence.models.optimization import (
    OptimizationProposalRecord,
    ProposalPositionRecord,
)
from nexus.persistence.session import session_factory
from sqlalchemy import select, text


@pytest.mark.asyncio
async def test_repository_shadow_rejects_negative_strategy_target(
    migrated_database_url: str,
) -> None:
    group_id = UUID("0198d500-1000-7000-8000-000000000869")
    await _seed_complete_group(migrated_database_url, group_id)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        strategy = await session.scalar(
            select(OptimizationProposalRecord).where(
                OptimizationProposalRecord.proposal_group_id == group_id,
                OptimizationProposalRecord.variant == "strategy_target",
            )
        )
        assert strategy is not None
        await session.execute(text("ALTER TABLE proposal_positions DISABLE TRIGGER USER"))
        try:
            session.add(
                ProposalPositionRecord(
                    proposal_id=strategy.id,
                    instrument_id=INSTRUMENT_ID,
                    current_weight=Decimal("0.125"),
                    target_weight=Decimal("-0.001"),
                    action="reduce",
                    reason_code="ROUND3_NEGATIVE",
                )
            )
            await session.flush()
        finally:
            await session.execute(text("ALTER TABLE proposal_positions ENABLE TRIGGER USER"))
    with pytest.raises(ValueError, match="PROPOSAL_NOT_EXECUTABLE"):
        async with factory() as session, session.begin():
            await transition_group(session, group_id, "shadowing", expected_version=1)
