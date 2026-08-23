"""Read-only persisted portfolio diagnostic routes."""

from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError

router = APIRouter()


@router.get("/internal/v1/portfolios/{portfolio_id}/diagnostics/latest")
async def get_latest_diagnostic(
    portfolio_id: UUID,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> dict[str, Any]:
    """Reserve the solely server-computed diagnostic projection endpoint."""
    del actor, session
    raise NexusError(
        ErrorCode.NOT_FOUND,
        "The portfolio diagnostic was not found.",
        status_code=404,
        details={"portfolio_id": str(portfolio_id)},
    )
