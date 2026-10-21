"""Bounded read-only summaries of immutable research outputs for one UTC date."""

from datetime import UTC, date, datetime, time, timedelta

from fastapi import APIRouter, Depends
from nexus.agents.observation_readback import ObservationReadback
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
from nexus.api.routes.research import _hydrate_response, _latest_output
from nexus.api.schemas.agents import (
    BriefDisagreementSummary,
    BriefInvalidationSummary,
    DailyBriefEnvelope,
    DailyBriefItem,
    DailyBriefResponse,
    EvidenceCompletenessSummary,
    ResearchRunResponse,
    utc_now,
)
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.persistence.models.agents import AgentResearchRunRecord
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()
_MAX_SESSION_RUNS = 100
_MAX_PRIMARY_ITEMS = 20
_MAX_INVALIDATIONS = 50


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
    """Return bounded typed summaries; never include role/tool payloads or invoke a model."""
    start = datetime.combine(session_date, time.min, tzinfo=UTC)
    end = start + timedelta(days=1)
    selected = list(
        await session.scalars(
            select(AgentResearchRunRecord)
            .where(
                AgentResearchRunRecord.decision_time >= start,
                AgentResearchRunRecord.decision_time < end,
            )
            .order_by(AgentResearchRunRecord.decision_time, AgentResearchRunRecord.id)
            .limit(_MAX_SESSION_RUNS + 1)
        )
    )
    truncated = len(selected) > _MAX_SESSION_RUNS
    responses: list[ResearchRunResponse] = []
    reader = ObservationReadback(session, read_at=utc_now())
    await reader.prime(selected[:_MAX_SESSION_RUNS])
    for run in selected[:_MAX_SESSION_RUNS]:
        responses.append(
            await _hydrate_response(
                session, run, await _latest_output(session, run.id), observation_reader=reader
            )
        )
    candidates = tuple(
        _item(response)
        for response in responses
        if response.intent is not None and response.intent.stance == "buy_candidate"
    )[:_MAX_PRIMARY_ITEMS]
    holding_risks = tuple(
        _item(response)
        for response in responses
        if response.intent is not None
        and (
            response.intent.stance == "reduce_candidate"
            or response.intent.exposure_action == "reduce"
        )
    )[:_MAX_PRIMARY_ITEMS]
    budget_watch = tuple(
        _item(response) for response in responses if response.budget_reason == "budget_exhausted"
    )[:_MAX_PRIMARY_ITEMS]
    rejected = tuple(
        _item(response)
        for response in responses
        if response.status in {"failed", "abstained"} and response.budget_reason is None
    )[:_MAX_PRIMARY_ITEMS]
    complete = sum(response.quality_status == "complete" for response in responses)
    partial = sum(response.quality_status == "partial" for response in responses)
    failed = len(responses) - complete - partial
    disagreements = tuple(
        BriefDisagreementSummary(
            run_id=response.run_id,
            instrument_id=response.instrument_id,
            summary=response.disagreement,
        )
        for response in responses
        if response.disagreement is not None
    )[:_MAX_PRIMARY_ITEMS]
    invalidations = tuple(
        BriefInvalidationSummary(
            run_id=response.run_id,
            instrument_id=response.instrument_id,
            conditions=response.invalidation_conditions,
        )
        for response in responses
        if response.invalidation_conditions
    )[:_MAX_INVALIDATIONS]
    now = utc_now()
    return DailyBriefEnvelope(
        data=DailyBriefResponse(
            session_date=session_date,
            generated_at=now,
            candidates=candidates,
            holding_risks=holding_risks,
            rejected=rejected,
            budget_exhausted_watch=budget_watch,
            evidence_completeness=EvidenceCompletenessSummary(
                total_runs=len(responses),
                complete=complete,
                partial=partial,
                failed=failed,
            ),
            disagreements=disagreements,
            invalidations=invalidations,
            truncated=truncated,
        ),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )


def _item(response: ResearchRunResponse) -> DailyBriefItem:
    return DailyBriefItem(
        run_id=response.run_id,
        instrument_id=response.instrument_id,
        status=response.status,
        stance=None if response.intent is None else response.intent.stance,
        reason=response.budget_reason or response.abstention_reason,
    )
