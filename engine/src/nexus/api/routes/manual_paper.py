"""HMAC-protected explicit manual admission and persisted-only result reads."""

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Depends, Header
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import get_settings, require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.routes.paper import _ERROR_RESPONSES, _candidate_read_policy
from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.paper.manual_entry import (
    SAFE_ENTRY_CODES,
    lookup_configured_manual_request,
    read_configured_manual_request,
    start_configured_manual_request,
)
from nexus.paper.manual_request import ManualRequestInput, ManualRequestView
from nexus.settings import EngineSettings
from pydantic import BaseModel, ConfigDict
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter(dependencies=[Depends(_candidate_read_policy)])


class ManualPaperRequestEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")
    data: ManualRequestView
    meta: SuccessMetaResponse


class ManualPaperLookupEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")
    data: ManualRequestView | None
    meta: SuccessMetaResponse


@router.get(
    "/internal/v1/paper/manual-requests",
    operation_id="lookupManualPaperRequest",
    response_model=ManualPaperLookupEnvelope,
    responses=_ERROR_RESPONSES,
)
async def lookup_manual_paper_request(
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    settings: EngineSettings = Depends(get_settings),
) -> ManualPaperLookupEnvelope:
    try:
        data = await lookup_configured_manual_request(
            session, settings, idempotency_key=idempotency_key
        )
    except ValueError as error:
        raise _entry_error(error) from error
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise _entry_error(ValueError("MANUAL_RESULT_UNKNOWN"))
    return ManualPaperLookupEnvelope(
        data=data, meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now)
    )


def _entry_error(error: ValueError) -> NexusError:
    code = str(error)
    if code not in SAFE_ENTRY_CODES:
        code = "MANUAL_RESULT_UNKNOWN"
    return NexusError(
        ErrorCode.NOT_FOUND
        if code == "MANUAL_NOT_FOUND"
        else ErrorCode.IDEMPOTENCY_CONFLICT
        if code == "MANUAL_IDEMPOTENCY_CONFLICT"
        else ErrorCode.PROPOSAL_NOT_EXECUTABLE,
        "手动请求暂不可开始或读回。",
        status_code=404 if code == "MANUAL_NOT_FOUND" else 409,
        details={"reason_code": code},
    )


async def _envelope(
    session: AsyncSession, actor: VerifiedServiceActor, data: ManualRequestView
) -> ManualPaperRequestEnvelope:
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime) or now.utcoffset() is None:
        raise _entry_error(ValueError("MANUAL_RESULT_UNKNOWN"))
    return ManualPaperRequestEnvelope(
        data=data, meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now)
    )


@router.post(
    "/internal/v1/paper/manual-requests",
    operation_id="startManualPaperRequest",
    response_model=ManualPaperRequestEnvelope,
    responses=_ERROR_RESPONSES,
)
async def start_manual_paper_request(
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
    body: ManualRequestInput = Body(),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    settings: EngineSettings = Depends(get_settings),
) -> ManualPaperRequestEnvelope:
    try:
        data = await start_configured_manual_request(
            session,
            settings,
            mode=body.mode,
            idempotency_key=idempotency_key,
        )
    except ValueError as error:
        raise _entry_error(error) from error
    return await _envelope(session, actor, data)


@router.get(
    "/internal/v1/paper/manual-requests/{request_id}",
    operation_id="getManualPaperRequest",
    response_model=ManualPaperRequestEnvelope,
    responses=_ERROR_RESPONSES,
)
async def get_manual_paper_request(
    request_id: UUID,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    settings: EngineSettings = Depends(get_settings),
) -> ManualPaperRequestEnvelope:
    try:
        data = await read_configured_manual_request(session, settings, request_id=request_id)
    except ValueError as error:
        raise _entry_error(error) from error
    return await _envelope(session, actor, data)
