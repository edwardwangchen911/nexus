"""Raw PostgreSQL enforcement for strategy target selection binding."""

from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from helpers.portfolio_api import INSTRUMENT_ID, PORTFOLIOS, seed_portfolio_list, settings
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID
from nexus.optimizer.cost_policy import COST_POLICY_ID
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import (
    BaselineSelectionAuthorityRecord,
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
    ProposalPositionRecord,
)
from nexus.persistence.session import session_factory
from nexus.quality.source_policy import SOURCE_POLICY_ID
from sqlalchemy.exc import IntegrityError

_CLOCK = datetime(2026, 8, 24, 3, 59, tzinfo=UTC)
_BASELINE = UUID("0198d500-1000-7000-8000-000000000041")
_DATA = UUID("0198d500-1000-7000-8000-000000000d01")
_SELECTION = UUID("0198d500-1000-7000-8000-000000000d02")
_REPORT = UUID("0198d500-1000-7000-8000-000000000d03")
_GROUP = UUID("0198d500-1000-7000-8000-000000000d04")
_PROPOSAL = UUID("0198d500-1000-7000-8000-000000000d05")
_POLICY = UUID("e690b91d-1e24-5de3-b2b2-566a463fca25")


@pytest.mark.asyncio
async def test_raw_overweight_strategy_position_is_rejected_before_group_commit(
    migrated_database_url: str,
) -> None:
    await seed_portfolio_list(migrated_database_url)
    factory = session_factory(settings(migrated_database_url))
    with pytest.raises(IntegrityError, match="strategy target exceeds bound selection"):
        async with factory() as session, session.begin():
            session.add_all(
                (
                    DataSnapshot(
                        id=_DATA, source="fixture", provider_object_id="strategy-db",
                        revision="1", observed_at=_CLOCK, available_at=_CLOCK,
                        received_at=_CLOCK, schema_version="1", payload={},
                    ),
                    PortfolioDiagnosticReportRecord(
                        id=_REPORT, portfolio_id=PORTFOLIOS[0],
                        portfolio_snapshot_id=_BASELINE, snapshot_as_of=_CLOCK,
                        trading_day=date(2026, 8, 24), policy_id=REAL_ADVISORY_POLICY_ID,
                        policy_version="real_advisory_policy_v1",
                        optimization_need="OPTIMIZATION_CANDIDATE", diagnostic_input_hash="d" * 64,
                    ),
                    BaselineSelectionAuthorityRecord(
                        id=_SELECTION, policy_version="baseline_v1", data_snapshot_id=_DATA,
                        decision_at=_CLOCK, available_at=_CLOCK,
                        evidence_refs=["selection:db"],
                        candidates=[
                            {
                                "instrument_id": str(INSTRUMENT_ID),
                                "provenance_refs": ["candidate:db"],
                            }
                        ],
                        selected_instrument_ids=[str(INSTRUMENT_ID)], evidence_hash="a" * 64,
                    ),
                )
            )
            await session.flush()
            session.add(
                OptimizationProposalGroupRecord(
                    id=_GROUP, baseline_portfolio_snapshot_id=_BASELINE,
                    portfolio_scope="internal_paper", selection_authority_id=_SELECTION,
                    status="draft", version=0, policy_id=_POLICY, diagnostic_report_id=_REPORT,
                    data_snapshot_id=_DATA, source_policy_id=SOURCE_POLICY_ID,
                    cost_policy_id=COST_POLICY_ID, diagnostic_input_hash="d" * 64,
                    valuation_time=_CLOCK, baseline_ready=True, diagnostic_complete=True,
                    source_conflict=False, partial=False, requires_fx=False,
                    cost_complete=True, authority_stale=False,
                )
            )
            session.add(
                OptimizationProposalRecord(
                    id=_PROPOSAL, proposal_group_id=_GROUP, variant="strategy_target",
                    baseline_portfolio_snapshot_id=_BASELINE, analysis_only=False,
                    strategy_evidence_complete=True, portfolio_scope="internal_paper",
                )
            )
            await session.flush()
            session.add(
                ProposalPositionRecord(
                    proposal_id=_PROPOSAL, instrument_id=INSTRUMENT_ID,
                    current_weight=Decimal("1"), target_weight=Decimal("0.126"),
                    action="reduce", reason_code="RAW_OVERWEIGHT",
                )
            )
            await session.flush()
