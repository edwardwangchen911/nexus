"""Authenticated portfolio read routes over frozen PostgreSQL facts."""

from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import get_settings, require_service_actor, transactional_session
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.api.schemas.portfolio import (
    PortfolioKind,
    PortfolioPageEnvelope,
    PortfolioSnapshotEnvelope,
)
from nexus.domain.time import require_utc
from nexus.portfolio.service import PortfolioService
from nexus.settings import EngineSettings
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()
_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    status: {"model": NexusErrorEnvelope, "description": "Stable NEXUS error envelope."}
    for status in (401, 404, 422)
}


def _utc_now(request: Request) -> datetime:
    clock = getattr(request.app.state, "utc_clock", None)
    return require_utc(clock() if callable(clock) else datetime.now(UTC))


@router.get(
    "/internal/v1/portfolios",
    operation_id="listPortfolios",
    response_model=PortfolioPageEnvelope,
    responses=_ERROR_RESPONSES,
)
async def list_portfolios(
    limit: Annotated[int, Query(ge=1, le=50)] = 50,
    kind: PortfolioKind | None = None,
    cursor: Annotated[str | None, Query(min_length=1, max_length=2048)] = None,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    settings: EngineSettings = Depends(get_settings),
    now: datetime = Depends(_utc_now),
) -> PortfolioPageEnvelope:
    secrets = tuple(
        value.get_secret_value()
        for value in (
            settings.engine_hmac_current_secret,
            settings.engine_hmac_previous_secret,
        )
        if value is not None
    )
    page, as_of = await PortfolioService(
        session, now, cursor_signing_secrets=secrets
    ).list_page(kind=kind, cursor=cursor, limit=limit)
    return PortfolioPageEnvelope(
        data=page,
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=as_of),
    )


@router.get(
    "/internal/v1/portfolios/{portfolio_id}/snapshots/latest",
    operation_id="getLatestPortfolioSnapshot",
    response_model=PortfolioSnapshotEnvelope,
    responses={status: _ERROR_RESPONSES[status] for status in (401, 404, 422)},
)
async def get_latest_portfolio_snapshot(
    portfolio_id: UUID,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    now: datetime = Depends(_utc_now),
) -> PortfolioSnapshotEnvelope:
    snapshot = await PortfolioService(session, now).latest_snapshot(portfolio_id)
    return PortfolioSnapshotEnvelope(
        data=snapshot,
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )
