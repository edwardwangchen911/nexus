"""Database-CAS lifecycle tests for immutable proposal groups."""

from uuid import UUID

import pytest
from helpers.portfolio_api import seed_portfolio_list, settings
from nexus.optimizer.repository import transition_group
from nexus.persistence.session import session_factory
from sqlalchemy import text


@pytest.mark.asyncio
async def test_incomplete_strategy_cannot_enter_shadowing_and_stale_version_conflicts(
    migrated_database_url: str,
) -> None:
    """Skipping evidence eligibility or CAS would make unsafe/stale lifecycle writes succeed."""
    group_id = UUID("0198d500-1000-7000-8000-000000000801")
    baseline_id = UUID("0198d500-1000-7000-8000-000000000041")
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await session.execute(text("INSERT INTO optimization_proposal_groups (id, baseline_portfolio_snapshot_id, status, version, policy_id) VALUES (:id, :baseline, 'compared', 1, 'e690b91d-1e24-5de3-b2b2-566a463fca25')"), {"id": group_id, "baseline": baseline_id})
        for index, variant in enumerate(("hold_current", "minimum_change", "strategy_target"), 1):
            await session.execute(text("INSERT INTO optimization_proposals (id, proposal_group_id, variant, baseline_portfolio_snapshot_id, analysis_only, strategy_evidence_complete) VALUES (:id, :group, :variant, :baseline, true, false)"), {"id": UUID(f"0198d500-1000-7000-8000-00000000080{index + 1}"), "group": group_id, "variant": variant, "baseline": baseline_id})
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="PROPOSAL_NOT_EXECUTABLE"):
            await transition_group(session, group_id, "shadowing", expected_version=1)
        with pytest.raises(ValueError, match="VERSION_CONFLICT"):
            await transition_group(session, group_id, "rejected", expected_version=0)
