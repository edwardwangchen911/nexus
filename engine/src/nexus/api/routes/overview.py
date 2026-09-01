"""Read-only overview assembled exclusively from immutable persisted projections."""

from datetime import date
from typing import cast
from uuid import UUID

from fastapi import APIRouter, Depends
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.schemas.diagnostics import PortfolioDiagnosticResponse
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.api.schemas.overview import (
    OverviewEnvelope,
    OverviewProposalGroupSummaryResponse,
    OverviewResponse,
    ProposalAdmissionResponse,
)
from nexus.diagnostics.policy import REAL_ADVISORY_POLICY_ID, load_real_advisory_policy_v1
from nexus.diagnostics.repository import read_report
from nexus.optimizer.authority import load_admission_authority
from nexus.optimizer.models import ProposalGroupStatus
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.optimization import (
    OptimizationAdmissionAuthorityRecord,
    OptimizationProposalGroupRecord,
)
from nexus.persistence.models.paper import PaperAccountRecord
from nexus.persistence.models.portfolio import Account, Portfolio, PortfolioSnapshotRecord
from nexus.portfolio.service import PortfolioService
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()


@router.get(
    "/internal/v1/overview",
    response_model=OverviewEnvelope,
    operation_id="getOverview",
    responses={
        404: {"model": NexusErrorEnvelope},
        409: {"model": NexusErrorEnvelope},
        422: {"model": NexusErrorEnvelope},
    },
)
async def get_overview(
    portfolio_id: UUID | None = None,
    session_date: date | None = None,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> OverviewEnvelope:
    """Select one latest persisted diagnostic by a total order without recalculation."""
    statement = select(PortfolioDiagnosticReportRecord)
    if portfolio_id is not None:
        statement = statement.where(PortfolioDiagnosticReportRecord.portfolio_id == portfolio_id)
    if session_date is not None:
        statement = statement.where(PortfolioDiagnosticReportRecord.trading_day == session_date)
    record = await session.scalar(
        statement.order_by(
            desc(PortfolioDiagnosticReportRecord.created_at),
            desc(PortfolioDiagnosticReportRecord.id),
        ).limit(1)
    )
    if record is None:
        if session_date is not None:
            raise NexusError(
                ErrorCode.NOT_FOUND,
                "Overview diagnostic was not found.",
                status_code=404,
            )
        return await _portfolio_facts_overview(
            portfolio_id=portfolio_id,
            actor=actor,
            session=session,
        )
    try:
        policy = await load_real_advisory_policy_v1(session)
    except ValueError as error:
        raise NexusError(
            ErrorCode.DATA_PARTIAL,
            "The persisted diagnostic policy authority is unavailable.",
            status_code=409,
        ) from error
    if record.policy_id != REAL_ADVISORY_POLICY_ID or record.policy_version != policy.version:
        raise NexusError(
            ErrorCode.DATA_PARTIAL,
            "The persisted diagnostic policy authority is unavailable.",
            status_code=409,
        )
    report = await read_report(session, record)
    if report.snapshot_as_of is None:
        raise NexusError(
            ErrorCode.DATA_PARTIAL,
            "The persisted diagnostic snapshot time is unavailable.",
            status_code=409,
        )
    group = await session.scalar(
        select(OptimizationProposalGroupRecord)
        .where(OptimizationProposalGroupRecord.diagnostic_report_id == record.id)
        .order_by(
            desc(OptimizationProposalGroupRecord.created_at),
            desc(OptimizationProposalGroupRecord.id),
        )
        .limit(1)
    )
    admission_candidate = await session.scalar(
        select(OptimizationAdmissionAuthorityRecord)
        .where(
            OptimizationAdmissionAuthorityRecord.portfolio_snapshot_id
            == record.portfolio_snapshot_id,
            OptimizationAdmissionAuthorityRecord.diagnostic_report_id == record.id,
        )
        .order_by(
            desc(OptimizationAdmissionAuthorityRecord.created_at),
            desc(OptimizationAdmissionAuthorityRecord.id),
        )
        .limit(1)
    )
    admission = None
    if admission_candidate is not None:
        admission = await load_admission_authority(
            session,
            portfolio_snapshot_id=admission_candidate.portfolio_snapshot_id,
            data_snapshot_id=admission_candidate.data_snapshot_id,
            cost_policy_id=admission_candidate.cost_policy_id,
        )
    return OverviewEnvelope(
        data=OverviewResponse(
            overview_mode="DIAGNOSTIC",
            portfolio_id=record.portfolio_id,
            portfolio_name=None,
            provider=None,
            portfolio_snapshot_id=record.portfolio_snapshot_id,
            portfolio_snapshot=None,
            snapshot_as_of=report.snapshot_as_of,
            trading_day=report.trading_day,
            diagnostic_report=PortfolioDiagnosticResponse.from_report(report),
            diagnostic_unavailable_reason=None,
            proposal_group=None
            if group is None
            else OverviewProposalGroupSummaryResponse(
                proposal_group_id=group.id,
                status=cast(ProposalGroupStatus, group.status),
                version=group.version,
            ),
            proposal_admission=None
            if admission is None
            else ProposalAdmissionResponse(
                portfolio_snapshot_id=record.portfolio_snapshot_id,
                data_snapshot_id=admission.persistence.data_snapshot_id,
                cost_policy_id=admission.persistence.cost_policy_id,
            ),
            proposal_admission_reason=None
            if admission is not None
            else "OPTIMIZATION_AUTHORITY_MISSING",
        ),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=report.snapshot_as_of),
    )


async def _portfolio_facts_overview(
    *,
    portfolio_id: UUID | None,
    actor: VerifiedServiceActor,
    session: AsyncSession,
) -> OverviewEnvelope:
    statement = (
        select(
            PortfolioSnapshotRecord.id.label("snapshot_id"),
            PortfolioSnapshotRecord.portfolio_id,
            PortfolioSnapshotRecord.as_of,
            Portfolio.name.label("portfolio_name"),
            Account.provider,
        )
        .join(Portfolio, Portfolio.id == PortfolioSnapshotRecord.portfolio_id)
        .join(Account, Account.id == Portfolio.account_id)
        .outerjoin(PaperAccountRecord, PaperAccountRecord.portfolio_id == Portfolio.id)
        .where(Account.provider == "fosun", PaperAccountRecord.id.is_(None))
    )
    if portfolio_id is not None:
        statement = statement.where(PortfolioSnapshotRecord.portfolio_id == portfolio_id)
    row = (
        await session.execute(
            statement.order_by(
                desc(PortfolioSnapshotRecord.created_at),
                desc(PortfolioSnapshotRecord.id),
            ).limit(1)
        )
    ).first()
    if row is None:
        raise NexusError(ErrorCode.NOT_FOUND, "Overview facts were not found.", status_code=404)
    if row.as_of is None:
        raise NexusError(
            ErrorCode.DATA_PARTIAL,
            "The persisted portfolio snapshot time is unavailable.",
            status_code=409,
        )
    snapshot = await PortfolioService(session, row.as_of).latest_snapshot(row.portfolio_id)
    if snapshot.portfolio_snapshot_id != row.snapshot_id or snapshot.as_of != row.as_of:
        raise NexusError(
            ErrorCode.DATA_PARTIAL,
            "The persisted portfolio snapshot authority is inconsistent.",
            status_code=409,
        )
    return OverviewEnvelope(
        data=OverviewResponse(
            overview_mode="PORTFOLIO_FACTS_ONLY",
            portfolio_id=row.portfolio_id,
            portfolio_name=row.portfolio_name,
            provider="fosun",
            portfolio_snapshot_id=row.snapshot_id,
            portfolio_snapshot=snapshot,
            snapshot_as_of=row.as_of,
            trading_day=None,
            diagnostic_report=None,
            diagnostic_unavailable_reason=(
                "DIAGNOSTIC_NOT_AVAILABLE"
                if snapshot.status == "READY"
                else "SNAPSHOT_NOT_READY"
            ),
            proposal_group=None,
            proposal_admission=None,
            proposal_admission_reason=None,
        ),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=row.as_of),
    )
