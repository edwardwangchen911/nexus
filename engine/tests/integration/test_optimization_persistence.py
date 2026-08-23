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
    from nexus.persistence.models.optimization import ProposalComparisonRecord
    from nexus.persistence.session import session_factory

    group_id = UUID("0198d500-1000-7000-8000-000000000501")
    baseline_id = UUID("0198d500-1000-7000-8000-000000000041")
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        await session.execute(
            text(
                "INSERT INTO optimization_proposal_groups (id, baseline_portfolio_snapshot_id, status, version, policy_id) VALUES (:id, :baseline, 'draft', 0, 'e690b91d-1e24-5de3-b2b2-566a463fca25')"
            ),
            {"id": group_id, "baseline": baseline_id},
        )
        for number, variant in enumerate(
            ("hold_current", "minimum_change", "strategy_target"), start=1
        ):
            await session.execute(
                text(
                    "INSERT INTO optimization_proposals (id, proposal_group_id, variant, baseline_portfolio_snapshot_id, analysis_only, strategy_evidence_complete) VALUES (:id, :group, :variant, :baseline, true, false)"
                ),
                {
                    "id": UUID(f"0198d500-1000-7000-8000-00000000050{number + 3}"),
                    "group": group_id,
                    "variant": variant,
                    "baseline": baseline_id,
                },
            )
        session.add_all(
            ProposalComparisonRecord(
                proposal_id=UUID(f"0198d500-1000-7000-8000-00000000050{number + 3}"),
                metrics={},
                tax_impact="unknown",
                requires_fx=False,
                non_executable=True,
                cost_complete=False,
            )
            for number in range(1, 4)
        )
    async with factory() as session:
        with pytest.raises(IntegrityError, match="exactly three variants"):
            async with session.begin():
                await session.execute(
                    text(
                        "INSERT INTO optimization_proposal_groups "
                        "(id, baseline_portfolio_snapshot_id, status, version, policy_id) "
                        "VALUES ('0198d500-1000-7000-8000-000000000510', :baseline, "
                        "'draft', 0, 'e690b91d-1e24-5de3-b2b2-566a463fca25')"
                    ),
                    {"baseline": baseline_id},
                )
        with pytest.raises(IntegrityError):
            async with session.begin():
                await session.execute(
                    text(
                        "INSERT INTO optimization_proposals (id, proposal_group_id, variant, baseline_portfolio_snapshot_id, analysis_only, strategy_evidence_complete) VALUES ('0198d500-1000-7000-8000-000000000509', :group, 'hold_current', :baseline, true, false)"
                    ),
                    {"group": group_id, "baseline": baseline_id},
                )
        with pytest.raises(IntegrityError, match="optimization record is immutable"):
            async with session.begin():
                await session.execute(
                    text(
                        "UPDATE optimization_proposals SET variant = 'minimum_change' WHERE proposal_group_id = :id AND variant = 'hold_current'"
                    ),
                    {"id": group_id},
                )
