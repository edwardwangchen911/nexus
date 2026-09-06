"""Read saved news; page reads never contact the news provider."""

from datetime import UTC, datetime
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
from nexus.api.schemas.integrations import SuccessMetaResponse
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.research.news import NewsResponse, read_news
from pydantic import BaseModel, ConfigDict
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()


class NewsEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")
    data: NewsResponse
    meta: SuccessMetaResponse


@router.get(
    "/internal/v1/instruments/{instrument_id}/news",
    response_model=NewsEnvelope,
    operation_id="getSavedNews",
)
async def saved_news(
    request: Request,
    instrument_id: UUID,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> NewsEnvelope:
    now = datetime.now(UTC)
    store = getattr(request.app.state, "news_object_store", None) or S3ProviderEvidenceStore(
        request.app.state.settings
    )
    data = await read_news(session, store, instrument_id=instrument_id, now=now)
    return NewsEnvelope(data=data, meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now))
