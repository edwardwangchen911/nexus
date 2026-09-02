"""Only complete, dated persisted comparisons can influence later diagnoses."""

from datetime import timedelta
from uuid import UUID

import pytest
from helpers.optimization_api import seed_readable_group
from helpers.portfolio_api import CLOCK, PORTFOLIOS, seed_portfolio_list, settings
from nexus.diagnostics.repository import load_history
from nexus.optimizer.authority import load_admission_authority
from nexus.persistence.models.optimization import (
    OptimizationAdmissionAuthorityRecord,
    OptimizationProposalGroupRecord,
)
from nexus.persistence.session import session_factory


@pytest.mark.asyncio
@pytest.mark.parametrize("late_authority", [False, True])
async def test_history_reads_qualified_comparison_and_excludes_other_portfolio_and_future(
    migrated_database_url: str,
    late_authority: bool,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as session, session.begin():
        group_id, _ = await seed_readable_group(session)
        group = await session.get(OptimizationProposalGroupRecord, group_id)
        assert group is not None
        session.add(
            OptimizationAdmissionAuthorityRecord(
                portfolio_snapshot_id=group.baseline_portfolio_snapshot_id,
                diagnostic_report_id=group.diagnostic_report_id,
                data_snapshot_id=group.data_snapshot_id,
                source_policy_id=group.source_policy_id,
                optimizer_policy_id=group.policy_id,
                cost_policy_id=group.cost_policy_id,
                diagnostic_input_hash=group.diagnostic_input_hash,
                valuation_time=group.valuation_time,
                baseline_ready=True,
                diagnostic_complete=True,
                source_conflict=False,
                partial=False,
                requires_fx=False,
                cost_complete=True,
                portfolio_scope=group.portfolio_scope,
                created_at=CLOCK + timedelta(days=2) if late_authority else CLOCK,
            )
        )
        await session.flush()
        assert (
            await load_admission_authority(
                session,
                portfolio_snapshot_id=group.baseline_portfolio_snapshot_id,
                data_snapshot_id=group.data_snapshot_id,
                cost_policy_id=group.cost_policy_id,
            )
            is not None
        )
        history = await load_history(
            session,
            portfolio_id=PORTFOLIOS[0],
            current_snapshot_id=UUID(int=990),
            current_snapshot_as_of=CLOCK + timedelta(days=1),
            policy_version="real_advisory_policy_v1",
        )
        assert {v.variant for v in history.comparable_variants} == (
            set() if late_authority else {"minimum_change"}
        )
        assert all(v.proposal_group_id == group.id for v in history.comparable_variants)
        assert (
            any(r.object_id.startswith("proposal-comparison:") for r in history.authority_refs)
            is not late_authority
        )
        for portfolio, cutoff in (
            (PORTFOLIOS[1], CLOCK + timedelta(days=1)),
            (PORTFOLIOS[0], group.valuation_time - timedelta(seconds=1)),
        ):
            excluded = await load_history(
                session,
                portfolio_id=portfolio,
                current_snapshot_id=UUID(int=990),
                current_snapshot_as_of=cutoff,
                policy_version="real_advisory_policy_v1",
            )
            assert excluded.comparable_variants == ()
