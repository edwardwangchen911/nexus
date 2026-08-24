"""Authenticated internal routes for append-only, read-only Agent research state."""

import hashlib
import json
from datetime import datetime
from typing import Annotated, Literal, cast
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import JSONResponse
from nexus.agents.budget import BudgetExhausted
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import get_settings, require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.schemas.agents import (
    AgentRunStatus,
    ResearchAdmissionEnvelope,
    ResearchAdmissionResponse,
    ResearchRunCreateRequest,
    ResearchRunEnvelope,
    ResearchRunResponse,
    utc_now,
)
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.domain.decimal import decimal_to_string
from nexus.domain.evidence import EvidenceRef
from nexus.domain.time import require_utc
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from nexus.settings import EngineSettings
from nexus.strategy.intents import ResearchIntent
from pydantic import ValidationError
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()


@router.get(
    "/internal/v1/instruments/{instrument_id}/research-admission",
    response_model=ResearchAdmissionEnvelope,
    operation_id="getResearchAdmission",
    responses={401: {"model": NexusErrorEnvelope}, 422: {"model": NexusErrorEnvelope}},
)
async def get_research_admission(
    request: Request,
    instrument_id: UUID,
    as_of: Annotated[datetime, Query()],
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> ResearchAdmissionEnvelope:
    """Select the frozen first-run body exclusively from point-in-time database authority."""
    try:
        bounded_as_of = require_utc(as_of)
    except ValueError as error:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR, "as_of must be canonical UTC.", status_code=422
        ) from error
    clock = getattr(request.app.state, "utc_clock", None)
    now = utc_now() if clock is None else require_utc(clock())
    reason: Literal[
        "instrument_not_found",
        "decision_time_in_future",
        "data_snapshot_unavailable",
        "prompt_authority_unavailable",
        "model_pricing_unavailable",
    ] | None = None
    snapshot = None
    if bounded_as_of > now:
        reason = "decision_time_in_future"
    elif await session.get(Instrument, instrument_id) is None:
        reason = "instrument_not_found"
    else:
        snapshot = await session.scalar(
            select(DataSnapshot)
            .where(
                DataSnapshot.available_at <= bounded_as_of,
                DataSnapshot.received_at <= bounded_as_of,
            )
            .order_by(
                desc(DataSnapshot.available_at),
                desc(DataSnapshot.received_at),
                desc(DataSnapshot.id),
            )
            .limit(1)
        )
        if snapshot is None:
            reason = "data_snapshot_unavailable"
        elif not await session.scalar(
            select(AgentPromptVersionRecord.id)
            .where(AgentPromptVersionRecord.role == "orchestrator")
            .order_by(desc(AgentPromptVersionRecord.created_at))
            .limit(1)
        ):
            reason = "prompt_authority_unavailable"
        elif not await session.scalar(
            select(AgentModelConfigRecord.id)
            .where(
                AgentModelConfigRecord.pricing_available_at <= now,
                AgentModelConfigRecord.pricing_expires_at > now,
            )
            .order_by(desc(AgentModelConfigRecord.created_at))
            .limit(1)
        ):
            reason = "model_pricing_unavailable"
    response = ResearchAdmissionResponse(
        eligible=reason is None,
        reason=reason,
        instrument_id=instrument_id,
        data_snapshot_id=None if snapshot is None else snapshot.id,
        decision_time=bounded_as_of,
    )
    return ResearchAdmissionEnvelope(
        data=response,
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=bounded_as_of),
    )


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
        data=await _hydrate_response(session, run, output),
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
    as_of: Annotated[datetime, Query()],
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> ResearchRunEnvelope:
    """Select the latest immutable research record by stable timestamp/id ordering."""
    try:
        bounded_as_of = require_utc(as_of)
    except ValueError as error:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR, "as_of must be canonical UTC.", status_code=422
        ) from error
    run = await session.scalar(
        select(AgentResearchRunRecord)
        .where(
            AgentResearchRunRecord.instrument_id == instrument_id,
            AgentResearchRunRecord.decision_time <= bounded_as_of,
        )
        .order_by(desc(AgentResearchRunRecord.decision_time), desc(AgentResearchRunRecord.id))
        .limit(1)
    )
    if run is None:
        raise NexusError(ErrorCode.NOT_FOUND, "Instrument research was not found.", status_code=404)
    output = await _latest_output(session, run.id)
    return ResearchRunEnvelope(
        data=await _hydrate_response(session, run, output),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=bounded_as_of),
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


async def _hydrate_response(
    session: AsyncSession,
    run: AgentResearchRunRecord,
    output: AgentResearchOutputRecord | None,
) -> ResearchRunResponse:
    evidence_records: tuple[AgentResearchOutputEvidenceRecord, ...] = ()
    if output is not None:
        evidence_records = tuple(
            await session.scalars(
                select(AgentResearchOutputEvidenceRecord)
                .where(AgentResearchOutputEvidenceRecord.output_id == output.id)
                .order_by(AgentResearchOutputEvidenceRecord.evidence_ref_id)
            )
        )
    return _response(run, output, evidence_records)


def _response(
    run: AgentResearchRunRecord,
    output: AgentResearchOutputRecord | None,
    evidence_records: tuple[AgentResearchOutputEvidenceRecord, ...] = (),
) -> ResearchRunResponse:
    intent = None
    abstention_reason = None
    if output is not None and output.intent is not None:
        try:
            intent = ResearchIntent.model_validate(output.intent)
        except ValidationError:
            abstention_reason = "invalid_intent_schema"
    evidence = tuple(
        EvidenceRef(
            source=record.source,
            object_id=record.provider_object_id,
            available_at=record.available_at,
            location=record.location,
            content_hash=record.content_hash,
        )
        for record in evidence_records
    )
    if intent is not None and not _intent_is_authority_bound(intent, evidence):
        intent = None
        evidence = ()
        abstention_reason = "evidence_authority_invalid"
    if output is not None and output.intent is None:
        raw_reason = output.payload.get("reason")
        if isinstance(raw_reason, str) and len(raw_reason) <= 100:
            abstention_reason = raw_reason
    status = run.status
    if abstention_reason is not None and status == "completed":
        status = "abstained"
    return ResearchRunResponse(
        run_id=run.id,
        instrument_id=run.instrument_id,
        data_snapshot_id=run.data_snapshot_id,
        decision_time=run.decision_time,
        status=cast(AgentRunStatus, status),
        retry_count=run.retry_count,
        reserved_cost_usd=decimal_to_string(run.reserved_cost_usd),
        settled_cost_usd=None
        if run.settled_cost_usd is None
        else decimal_to_string(run.settled_cost_usd),
        intent=intent,
        evidence=evidence,
        role_votes=() if intent is None else intent.role_votes,
        disagreement=None if intent is None else intent.disagreement,
        invalidation_conditions=() if intent is None else intent.invalidation_conditions,
        quality_status=None if intent is None else intent.quality_status,
        budget_reason="budget_exhausted" if abstention_reason == "budget_exhausted" else None,
        abstention_reason=abstention_reason,
    )


def _intent_is_authority_bound(intent: ResearchIntent, evidence: tuple[EvidenceRef, ...]) -> bool:
    authority = {_evidence_identity(ref) for ref in evidence}
    if not authority or {_evidence_identity(ref) for ref in intent.evidence_refs} != authority:
        return False
    nested = (
        ref
        for collection in (
            intent.theses,
            intent.counter_theses,
            intent.invalidation_conditions,
            intent.entry_conditions,
            intent.exit_conditions,
        )
        for item in collection
        for ref in item.evidence_refs
    )
    return all(_evidence_identity(ref) in authority for ref in nested)


def _evidence_identity(ref: EvidenceRef) -> tuple[str, str, datetime, str, str]:
    return (ref.source, ref.object_id, ref.available_at, ref.location, ref.content_hash)
