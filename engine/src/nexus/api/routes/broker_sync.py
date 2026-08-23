"""Authenticated read-only broker synchronization API routes."""

import hashlib
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Request, Response, status
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
from nexus.api.routes.portfolios import _utc_now
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.api.schemas.portfolio import (
    BrokerSyncCreateRequest,
    BrokerSyncRunEnvelope,
)
from nexus.portfolio.sync_service import BrokerSyncApiService
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()
_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    error_status: {"model": NexusErrorEnvelope, "description": "Stable NEXUS error envelope."}
    for error_status in (401, 404, 409, 422, 429)
}
_CREATE_RESPONSES = {
    status.HTTP_202_ACCEPTED: {
        "description": "The read-only broker sync run was admitted or joined.",
        "headers": {
            "Location": {
                "description": "Same-origin BFF status resource.",
                "schema": {"type": "string"},
            }
        },
    },
    **_ERROR_RESPONSES,
}


@router.post(
    "/internal/v1/broker-sync-runs",
    operation_id="createBrokerSyncRun",
    response_model=BrokerSyncRunEnvelope,
    status_code=status.HTTP_202_ACCEPTED,
    responses=_CREATE_RESPONSES,
)
async def create_broker_sync_run(
    payload: BrokerSyncCreateRequest,
    request: Request,
    response: Response,
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=255)
    ],
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> BrokerSyncRunEnvelope:
    accepted_at = _utc_now(request)
    request_hash = hashlib.sha256(await request.body()).hexdigest()
    run = await BrokerSyncApiService(session).admit(
        portfolio_id=payload.portfolio_id,
        actor_id=actor.actor_id,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
        accepted_at=accepted_at,
    )
    response.headers["Location"] = f"/api/v1/broker-sync-runs/{run.broker_sync_run_id}"
    return BrokerSyncRunEnvelope(
        data=run,
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=accepted_at),
    )


@router.get(
    "/internal/v1/broker-sync-runs/{run_id}",
    operation_id="getBrokerSyncRun",
    response_model=BrokerSyncRunEnvelope,
    responses={error_status: _ERROR_RESPONSES[error_status] for error_status in (401, 404, 422)},
)
async def get_broker_sync_run(
    run_id: UUID,
    request: Request,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> BrokerSyncRunEnvelope:
    now = _utc_now(request)
    run = await BrokerSyncApiService(session).get(run_id)
    return BrokerSyncRunEnvelope(
        data=run,
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )
