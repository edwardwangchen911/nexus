"""Authenticated internal routes for append-only, read-only Agent research state."""

import hashlib
import json
from typing import Annotated, cast
from uuid import UUID

from fastapi import APIRouter, Depends, Header
from fastapi.responses import JSONResponse
from nexus.agents.budget import BudgetExhausted
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import get_settings, require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.schemas.agents import (
    AgentRunStatus,
    ResearchRunCreateRequest,
    ResearchRunEnvelope,
    ResearchRunResponse,
    utc_now,
)
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.domain.decimal import decimal_to_string
from nexus.persistence.models.agents import AgentResearchOutputRecord, AgentResearchRunRecord
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from nexus.settings import EngineSettings
from nexus.strategy.intents import ResearchIntent
from pydantic import ValidationError
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()


@router.post(
    "/internal/v1/research-runs",
    response_model=ResearchRunEnvelope,
    status_code=202,
    operation_id="createResearchRun",
    responses={
        401: {"model": NexusErrorEnvelope},
        409: {"model": NexusErrorEnvelope},
        422: {"model": NexusErrorEnvelope},
    },
)
async def create_research_run(
    body: ResearchRunCreateRequest,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
    actor: VerifiedServiceActor = Depends(require_service_actor),
    settings: EngineSettings = Depends(get_settings),
    session: AsyncSession = Depends(transactional_session),
) -> JSONResponse:
    """Append a prepared local run only when immutable prompt/model pricing authority exists."""
    now = utc_now()
    input_hash = hashlib.sha256(
        json.dumps(body.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    try:
        admission = await admit_research_run(
            session,
            actor_id=actor.actor_id,
            route="/internal/v1/research-runs",
            idempotency_key=idempotency_key,
            request_hash=input_hash,
            request=ResearchRequest.model_validate(body.model_dump()),
            prompt_version_id=None,
            model_config_id=None,
            daily_limit_usd=settings.agent_daily_budget_usd,
            now=now,
        )
    except BudgetExhausted as error:
        raise NexusError(
            ErrorCode.BUDGET_EXHAUSTED, "Research budget is exhausted.", status_code=409
        ) from error
    except IdempotencyConflict as error:
        raise NexusError(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "Idempotency key has a different request body.",
            status_code=409,
        ) from error
    except ValueError as error:
        raise NexusError(
            ErrorCode.DATA_PARTIAL,
            "Point-in-time Agent authority is unavailable.",
            status_code=409,
        ) from error
    run = admission.run
    payload = ResearchRunEnvelope(
        data=_response(run, None),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )
    return JSONResponse(
        status_code=202,
        content=payload.model_dump(mode="json"),
        headers={"Location": f"/api/v1/research-runs/{run.id}"},
    )


@router.get(
    "/internal/v1/research-runs/{run_id}",
    response_model=ResearchRunEnvelope,
    operation_id="getResearchRun",
    responses={
        401: {"model": NexusErrorEnvelope},
        404: {"model": NexusErrorEnvelope},
        422: {"model": NexusErrorEnvelope},
    },
)
async def get_research_run(
    run_id: UUID,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> ResearchRunEnvelope:
    """Return one persisted run and its latest schema-validated immutable output."""
    run = await session.get(AgentResearchRunRecord, run_id)
    if run is None:
        raise NexusError(ErrorCode.NOT_FOUND, "Research run was not found.", status_code=404)
    output = await _latest_output(session, run.id)
    return ResearchRunEnvelope(
        data=_response(run, output),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=run.decision_time),
    )


@router.get(
    "/internal/v1/instruments/{instrument_id}/research",
    response_model=ResearchRunEnvelope,
    operation_id="getInstrumentResearch",
    responses={
        401: {"model": NexusErrorEnvelope},
        404: {"model": NexusErrorEnvelope},
        422: {"model": NexusErrorEnvelope},
    },
)
async def get_instrument_research(
    instrument_id: UUID,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> ResearchRunEnvelope:
    """Select the latest immutable research record by stable timestamp/id ordering."""
    run = await session.scalar(
        select(AgentResearchRunRecord)
        .where(AgentResearchRunRecord.instrument_id == instrument_id)
        .order_by(desc(AgentResearchRunRecord.decision_time), desc(AgentResearchRunRecord.id))
        .limit(1)
    )
    if run is None:
        raise NexusError(ErrorCode.NOT_FOUND, "Instrument research was not found.", status_code=404)
    output = await _latest_output(session, run.id)
    return ResearchRunEnvelope(
        data=_response(run, output),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=run.decision_time),
    )


async def _latest_output(session: AsyncSession, run_id: UUID) -> AgentResearchOutputRecord | None:
    return cast(
        AgentResearchOutputRecord | None,
        await session.scalar(
            select(AgentResearchOutputRecord)
            .where(AgentResearchOutputRecord.run_id == run_id)
            .order_by(desc(AgentResearchOutputRecord.sequence))
            .limit(1)
        ),
    )


def _response(
    run: AgentResearchRunRecord, output: AgentResearchOutputRecord | None
) -> ResearchRunResponse:
    intent = None
    if output is not None and output.intent is not None:
        try:
            intent = ResearchIntent.model_validate(output.intent)
        except ValidationError:
            intent = None
    return ResearchRunResponse(
        run_id=run.id,
        instrument_id=run.instrument_id,
        data_snapshot_id=run.data_snapshot_id,
        decision_time=run.decision_time,
        status=cast(AgentRunStatus, run.status),
        retry_count=run.retry_count,
        reserved_cost_usd=decimal_to_string(run.reserved_cost_usd),
        settled_cost_usd=None
        if run.settled_cost_usd is None
        else decimal_to_string(run.settled_cost_usd),
        intent=intent,
    )
