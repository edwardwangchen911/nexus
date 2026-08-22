"""Authenticated market monitoring and private watchlist routes."""

from datetime import UTC, datetime
from typing import Annotated, Any, cast
from uuid import UUID

from fastapi import APIRouter, Depends, Query, Request
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import get_settings, require_service_actor, transactional_session
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.api.schemas.market import MarketPageEnvelope, WatchlistMembershipEnvelope
from nexus.domain.time import require_utc
from nexus.market.service import MarketService, PriceSeriesLoader
from nexus.settings import EngineSettings
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()

_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    status: {"model": NexusErrorEnvelope, "description": "Stable NEXUS error envelope."}
    for status in (401, 404, 422)
}


def _utc_now(request: Request) -> datetime:
    """Return a UTC clock that tests can inject without accepting caller time."""
    clock = getattr(request.app.state, "utc_clock", None)
    return require_utc(clock() if callable(clock) else datetime.now(UTC))


@router.get(
    "/internal/v1/market",
    operation_id="listMarket",
    response_model=MarketPageEnvelope,
    responses=_ERROR_RESPONSES,
)
async def list_market(
    request: Request,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    cursor: Annotated[str | None, Query(min_length=1, max_length=2048)] = None,
    watchlist: bool | None = None,
    selected_instrument_id: UUID | None = None,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    settings: EngineSettings = Depends(get_settings),
    now: datetime = Depends(_utc_now),
) -> MarketPageEnvelope:
    """List point-in-time quote evidence and optionally one selected series."""
    loader = getattr(request.app.state, "market_price_series_loader", None)
    service = MarketService(
        session,
        now,
        cursor_signing_secrets=_cursor_secrets(settings),
        price_series_loader=(cast(PriceSeriesLoader, loader) if loader is not None else None),
    )
    page, decision_time = await service.market_page(
        limit=limit,
        cursor=cursor,
        watchlist=watchlist,
        selected_instrument_id=selected_instrument_id,
    )
    return MarketPageEnvelope(
        data=page,
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=decision_time),
    )


@router.put(
    "/internal/v1/watchlist/{instrument_id}",
    operation_id="putWatchlistMembership",
    response_model=WatchlistMembershipEnvelope,
    responses=_ERROR_RESPONSES,
)
async def put_watchlist_membership(
    instrument_id: UUID,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    settings: EngineSettings = Depends(get_settings),
    now: datetime = Depends(_utc_now),
) -> WatchlistMembershipEnvelope:
    """Ensure the instrument belongs to the stable default private watchlist."""
    service = MarketService(
        session, now, cursor_signing_secrets=_cursor_secrets(settings)
    )
    membership = await service.set_watchlisted(instrument_id, watchlisted=True)
    return WatchlistMembershipEnvelope(
        data=membership,
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )


@router.delete(
    "/internal/v1/watchlist/{instrument_id}",
    operation_id="deleteWatchlistMembership",
    response_model=WatchlistMembershipEnvelope,
    responses=_ERROR_RESPONSES,
)
async def delete_watchlist_membership(
    instrument_id: UUID,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    settings: EngineSettings = Depends(get_settings),
    now: datetime = Depends(_utc_now),
) -> WatchlistMembershipEnvelope:
    """Ensure the instrument is absent from the stable default private watchlist."""
    service = MarketService(
        session, now, cursor_signing_secrets=_cursor_secrets(settings)
    )
    membership = await service.set_watchlisted(instrument_id, watchlisted=False)
    return WatchlistMembershipEnvelope(
        data=membership,
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )


def _cursor_secrets(settings: EngineSettings) -> tuple[str, ...]:
    secrets = (
        settings.engine_hmac_current_secret,
        settings.engine_hmac_previous_secret,
    )
    return tuple(secret.get_secret_value() for secret in secrets if secret is not None)
