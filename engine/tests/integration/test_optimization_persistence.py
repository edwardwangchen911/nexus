# ruff: noqa: E501
"""PostgreSQL enforcement for atomic immutable proposal records."""

from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError


@pytest.mark.asyncio
async def test_optimization_group_constraints_and_immutability(migrated_database_url: str) -> None:
    """A partial group, duplicate child variant, or rewritten child must be rejected by PostgreSQL."""
    from helpers.portfolio_api import seed_portfolio_list, settings
    from nexus.persistence.session import session_factory

    group_id = UUID("0198d500-1000-7000-8000-000000000501")
    baseline_id = UUID("0198d500-1000-7000-8000-000000000041")
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    with pytest.raises(IntegrityError, match="diagnostic_report_id"):
        async with factory() as session, session.begin():
            await session.execute(
                text(
                    "INSERT INTO optimization_proposal_groups (id, baseline_portfolio_snapshot_id, status, version, policy_id) VALUES (:id, :baseline, 'draft', 0, :policy)"
                ),
                {"id": group_id, "baseline": baseline_id, "policy": UUID("e690b91d-1e24-5de3-b2b2-566a463fca25")},
            )
