"""Read-only persisted portfolio diagnostic routes."""

from uuid import UUID

from fastapi import APIRouter, Depends
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.diagnostics.repository import latest_report
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()


@router.get("/internal/v1/portfolios/{portfolio_id}/diagnostics/latest")
async def get_latest_diagnostic(
    portfolio_id: UUID,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> dict[str, object]:
    """Return the latest immutable server-computed projection, never a browser recalculation."""
    del actor
    report = await latest_report(session, portfolio_id=portfolio_id)
    if report is None:
        raise NexusError(
            ErrorCode.NOT_FOUND,
            "The portfolio diagnostic was not found.",
            status_code=404,
            details={"portfolio_id": str(portfolio_id)},
        )
    return {"data": report.model_dump(mode="json")}
