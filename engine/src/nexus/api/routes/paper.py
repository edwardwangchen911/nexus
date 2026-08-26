"""Authenticated internal-paper reads and versioned user cancellation."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Body, Depends, Header, Query
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import get_settings, require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.routes.portfolios import _utc_now
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.api.schemas.paper import (
    PaperCancelEnvelope,
    PaperCancelRequest,
    PaperCancelResponse,
    PaperOrderPageEnvelope,
    PaperPortfolioEnvelope,
)
from nexus.domain.decimal import decimal_to_string
from nexus.paper.errors import PaperExecutionError
from nexus.paper.execution_service import cancel_internal_paper_order
from nexus.paper.models import InternalOrderStatus
from nexus.paper.read_service import PaperReadService
from nexus.settings import EngineSettings
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()
_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    status: {"model": NexusErrorEnvelope, "description": "Stable NEXUS error envelope."}
    for status in (401, 404, 409, 422)
}
_IdempotencyKey = Annotated[
    str,
    Header(alias="Idempotency-Key", min_length=1, max_length=255),
]


def _cursor_secrets(settings: EngineSettings) -> tuple[str, ...]:
    return tuple(
        value.get_secret_value()
        for value in (
            settings.engine_hmac_current_secret,
            settings.engine_hmac_previous_secret,
        )
        if value is not None
    )


@router.get(
    "/internal/v1/paper/portfolio",
    operation_id="getInternalPaperPortfolio",
    response_model=PaperPortfolioEnvelope,
    responses=_ERROR_RESPONSES,
)
async def get_paper_portfolio(
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    now: datetime = Depends(_utc_now),
) -> PaperPortfolioEnvelope:
    data = await PaperReadService(session, now).portfolio()
    return PaperPortfolioEnvelope(
        data=data,
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )


@router.get(
    "/internal/v1/paper/orders",
    operation_id="listInternalPaperOrders",
    response_model=PaperOrderPageEnvelope,
    responses=_ERROR_RESPONSES,
)
async def list_paper_orders(
    limit: Annotated[int, Query(ge=1, le=50)] = 50,
    status: InternalOrderStatus | None = None,
    cursor: Annotated[str | None, Query(min_length=1, max_length=2048)] = None,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    settings: EngineSettings = Depends(get_settings),
    now: datetime = Depends(_utc_now),
) -> PaperOrderPageEnvelope:
    data, as_of = await PaperReadService(
        session,
        now,
        cursor_signing_secrets=_cursor_secrets(settings),
    ).orders(status=status, cursor=cursor, limit=limit)
    return PaperOrderPageEnvelope(
        data=data,
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=as_of),
    )


@router.post(
    "/internal/v1/paper/orders/{order_id}/cancel",
    operation_id="cancelInternalPaperOrder",
    response_model=PaperCancelEnvelope,
    responses=_ERROR_RESPONSES,
)
async def cancel_paper_order(
    order_id: UUID,
    idempotency_key: _IdempotencyKey,
    body: PaperCancelRequest = Body(),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    now: datetime = Depends(_utc_now),
) -> PaperCancelEnvelope:
    reader = PaperReadService(session, now)
    if not await reader.owns_order(order_id):
        raise NexusError(
            ErrorCode.NOT_FOUND,
            "The internal paper order was not found.",
            status_code=404,
        )
    try:
        result = await cancel_internal_paper_order(
            session,
            order_id=order_id,
            expected_event_seq=body.expected_event_seq,
            requested_at=now,
            idempotency_key=idempotency_key,
        )
    except PaperExecutionError as error:
        if error.code == "PAPER_EXECUTION_IDEMPOTENCY_CONFLICT":
            raise NexusError(
                ErrorCode.IDEMPOTENCY_CONFLICT,
                error.safe_message,
                status_code=409,
            ) from error
        raise NexusError(
            ErrorCode.VERSION_CONFLICT,
            error.safe_message,
            status_code=409,
        ) from error
    return PaperCancelEnvelope(
        data=PaperCancelResponse(
            order_id=result.order_id,
            status="cancelled",
            filled_quantity=decimal_to_string(result.filled_quantity),
            remaining_quantity=decimal_to_string(result.unfilled_quantity),
            result_event_seq=result.result_event_seq,
            replayed=result.replayed,
        ),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )
