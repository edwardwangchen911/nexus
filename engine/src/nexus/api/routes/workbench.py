"""Authenticated read-only internal-paper workbench."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Request
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import get_settings, require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.routes.portfolios import _utc_now
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.api.schemas.workbench import WorkbenchEnvelope
from nexus.settings import EngineSettings
from nexus.workbench.read_service import WorkbenchReadService
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()
_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    status: {"model": NexusErrorEnvelope, "description": "Stable NEXUS error envelope."}
    for status in (401, 409, 422)
}


@router.get(
    "/internal/v1/workbench",
    operation_id="getInternalPaperWorkbench",
    response_model=WorkbenchEnvelope,
    responses=_ERROR_RESPONSES,
)
async def get_internal_paper_workbench(
    request: Request,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    now: datetime = Depends(_utc_now),
    settings: EngineSettings = Depends(get_settings),
) -> WorkbenchEnvelope:
    if request.url.query:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR,
            "The workbench query is invalid.",
            status_code=422,
        )
    data = await WorkbenchReadService(
        session,
        now,
        expected_release_revision=settings.release_revision,
    ).read()
    return WorkbenchEnvelope(
        data=data,
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )
