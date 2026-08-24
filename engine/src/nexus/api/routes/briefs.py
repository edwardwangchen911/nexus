"""Read-only aggregation of immutable research outputs for one UTC session date."""

from datetime import UTC, date, datetime, time, timedelta

from fastapi import APIRouter, Depends
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
from nexus.api.routes.research import _latest_output, _response
from nexus.api.schemas.agents import (
    DailyBriefEnvelope,
    DailyBriefResponse,
    ResearchRunResponse,
    utc_now,
)
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.persistence.models.agents import AgentResearchRunRecord
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()


@router.get(
    "/internal/v1/briefs/{session_date}",
    response_model=DailyBriefEnvelope,
    operation_id="getDailyBrief",
    responses={401: {"model": NexusErrorEnvelope}, 422: {"model": NexusErrorEnvelope}},
)
async def get_daily_brief(
    session_date: date,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> DailyBriefEnvelope:
    """Aggregate persisted decisions only; no model invocation or target calculation occurs."""
    start = datetime.combine(session_date, time.min, tzinfo=UTC)
    end = start + timedelta(days=1)
    runs = list(
        await session.scalars(
            select(AgentResearchRunRecord)
            .where(
                AgentResearchRunRecord.decision_time >= start,
                AgentResearchRunRecord.decision_time < end,
            )
            .order_by(AgentResearchRunRecord.decision_time, AgentResearchRunRecord.id)
        )
    )
    responses: list[ResearchRunResponse] = []
    for run in runs:
        responses.append(_response(run, await _latest_output(session, run.id)))
    now = utc_now()
    return DailyBriefEnvelope(
        data=DailyBriefResponse(session_date=session_date, generated_at=now, runs=tuple(responses)),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )
