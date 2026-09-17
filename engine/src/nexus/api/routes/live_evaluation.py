"""Authenticated read-only live-evaluation gates."""

from typing import Any

from fastapi import APIRouter, Depends, Request
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.routes.portfolios import _utc_now
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.api.schemas.live_evaluation import LiveEvaluationEnvelope
from nexus.live_evaluation.read_service import LiveEvaluationReadService
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()
_PATH = "/internal/v1/live-evaluation"
_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    status: {"model": NexusErrorEnvelope, "description": "Stable NEXUS error envelope."}
    for status in (401, 422)
}


@router.get(
    _PATH,
    operation_id="getLiveEvaluation",
    response_model=LiveEvaluationEnvelope,
    responses=_ERROR_RESPONSES,
)
async def get_live_evaluation(
    request: Request,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> LiveEvaluationEnvelope:
    if request.url.query:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR,
            "The live-evaluation query is invalid.",
            status_code=422,
        )
    now = _utc_now(request)
    return LiveEvaluationEnvelope(
        data=await LiveEvaluationReadService(session, now).read(),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )
