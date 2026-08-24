"""Authenticated internal routes for append-only, read-only Agent research state."""

import hashlib
import json
from pathlib import Path
from typing import cast
from uuid import UUID

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
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
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
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
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> JSONResponse:
    """Append a prepared local run only when immutable prompt/model pricing authority exists."""
    prompt = await session.scalar(
        select(AgentPromptVersionRecord).order_by(desc(AgentPromptVersionRecord.created_at)).limit(1)
    )
    now = utc_now()
    model = await session.scalar(
        select(AgentModelConfigRecord)
        .where(
            AgentModelConfigRecord.pricing_available_at <= now,
            AgentModelConfigRecord.pricing_expires_at > now,
        )
        .order_by(desc(AgentModelConfigRecord.created_at))
        .limit(1)
    )
    if prompt is None or model is None:
        raise NexusError(
            ErrorCode.DATA_PARTIAL,
            "Immutable Agent prompt or current pricing authority is unavailable.",
            status_code=409,
        )
    input_hash = hashlib.sha256(
        json.dumps(body.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    schema_hash = hashlib.sha256(
        (Path(__file__).resolve().parents[5] / "contracts/agent/research-intent-v1.schema.json")
        .read_bytes()
    ).hexdigest()
    tool_hash = hashlib.sha256(
        b"get_security_snapshot|get_financial_facts|search_filing_evidence|get_price_features|"
        b"get_macro_snapshot|get_portfolio_exposure|get_portfolio_diagnostics|get_proposal_comparison"
    ).hexdigest()
    run = AgentResearchRunRecord(
        instrument_id=body.instrument_id,
        data_snapshot_id=body.data_snapshot_id,
        prompt_version_id=prompt.id,
        model_config_id=model.id,
        decision_time=body.decision_time,
        status="prepared",
        retry_count=0,
        schema_hash=schema_hash,
        model_hash=model.config_hash,
        prompt_hash=prompt.content_hash,
        tool_hash=tool_hash,
        input_hash=input_hash,
        reserved_cost_usd=model.max_cost_usd,
        settled_cost_usd=None,
    )
    session.add(run)
    await session.flush()
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
