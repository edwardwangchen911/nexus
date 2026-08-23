"""Read-only persisted portfolio diagnostic routes."""

from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, Depends
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.schemas.diagnostics import PortfolioDiagnosticEnvelope
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.diagnostics.repository import latest_report
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()


@router.get(
    "/internal/v1/portfolios/{portfolio_id}/diagnostics/latest",
    response_model=PortfolioDiagnosticEnvelope,
    operation_id="getLatestPortfolioDiagnostic",
    responses={404: {"model": NexusErrorEnvelope}, 422: {"model": NexusErrorEnvelope}},
)
async def get_latest_diagnostic(
    portfolio_id: UUID,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> PortfolioDiagnosticEnvelope:
    """Return the latest immutable server-computed projection, never a browser recalculation."""
    try:
        report = await latest_report(session, portfolio_id=portfolio_id)
    except ValueError as error:
        raise NexusError(
            ErrorCode.DATA_PARTIAL,
            "The persisted diagnostic policy authority is unavailable.",
            status_code=422,
            details={"portfolio_id": str(portfolio_id)},
        ) from error
    if report is None:
        raise NexusError(
            ErrorCode.NOT_FOUND,
            "The portfolio diagnostic was not found.",
            status_code=404,
            details={"portfolio_id": str(portfolio_id)},
        )
    as_of = datetime.combine(report.trading_day, datetime.min.time(), tzinfo=UTC)
    return PortfolioDiagnosticEnvelope(
        data=report.model_dump(),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=as_of),
    )
