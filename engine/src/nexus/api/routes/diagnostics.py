"""Read-only persisted portfolio diagnostic routes."""

from uuid import UUID

from fastapi import APIRouter, Depends
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.schemas.diagnostics import PortfolioDiagnosticEnvelope, PortfolioDiagnosticResponse
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.diagnostics.repository import latest_report_with_id
from nexus.diagnostics.service import preview_snapshot_diagnostic
from nexus.persistence.models.portfolio import PortfolioSnapshotRecord
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()


@router.get(
    "/internal/v1/portfolios/{portfolio_id}/diagnostics/latest",
    response_model=PortfolioDiagnosticEnvelope,
    operation_id="getLatestPortfolioDiagnostic",
    responses={
        404: {"model": NexusErrorEnvelope},
        409: {"model": NexusErrorEnvelope},
        422: {"model": NexusErrorEnvelope},
    },
)
async def get_latest_diagnostic(
    portfolio_id: UUID,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> PortfolioDiagnosticEnvelope:
    """Return the current formal report or an explicitly preliminary read-only projection."""
    try:
        persisted = await latest_report_with_id(session, portfolio_id=portfolio_id)
        diagnostic_report_id, report = persisted if persisted is not None else (None, None)
        snapshot_id = await session.scalar(
            select(PortfolioSnapshotRecord.id)
            .where(PortfolioSnapshotRecord.portfolio_id == portfolio_id)
            .order_by(PortfolioSnapshotRecord.created_at.desc(), PortfolioSnapshotRecord.id.desc())
            .limit(1)
        )
        if snapshot_id is not None and (
            report is None or report.portfolio_snapshot_id != snapshot_id
        ):
            report = await preview_snapshot_diagnostic(
                session, portfolio_id=portfolio_id, snapshot_id=snapshot_id
            )
            diagnostic_report_id = None
    except ValueError as error:
        raise NexusError(
            ErrorCode.DATA_PARTIAL,
            "The persisted diagnostic policy authority is unavailable.",
            status_code=409,
            details={"portfolio_id": str(portfolio_id)},
        ) from error
    if report is None:
        raise NexusError(
            ErrorCode.NOT_FOUND,
            "The portfolio diagnostic was not found.",
            status_code=404,
            details={"portfolio_id": str(portfolio_id)},
        )
    if report.snapshot_as_of is None:
        raise NexusError(
            ErrorCode.DATA_PARTIAL,
            "The persisted diagnostic snapshot time is unavailable.",
            status_code=409,
            details={"portfolio_id": str(portfolio_id)},
        )
    return PortfolioDiagnosticEnvelope(
        data=PortfolioDiagnosticResponse.from_report(
            report, diagnostic_report_id=diagnostic_report_id
        ),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=report.snapshot_as_of),
    )
