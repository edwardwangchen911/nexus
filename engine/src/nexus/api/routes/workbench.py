"""Authenticated read-only internal-paper workbench."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Request
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import get_session_factory, get_settings, require_service_actor
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.routes.portfolios import _utc_now
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.api.schemas.workbench import WorkbenchEnvelope
from nexus.settings import EngineSettings
from nexus.workbench.read_service import WorkbenchReadService
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

router = APIRouter()
_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    status: {"model": NexusErrorEnvelope, "description": "Stable NEXUS error envelope."}
    for status in (401, 409, 422)
}


async def workbench_read_session(
    factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
) -> AsyncIterator[AsyncSession]:
    # Authentication keeps its own nonce transaction; business projection is a
    # separate consistent snapshot and cannot write any business state.
    async with factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
        yield session


@router.get(
    "/internal/v1/workbench",
    operation_id="getInternalPaperWorkbench",
    response_model=WorkbenchEnvelope,
    responses=_ERROR_RESPONSES,
)
async def get_internal_paper_workbench(
    request: Request,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(workbench_read_session),
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
