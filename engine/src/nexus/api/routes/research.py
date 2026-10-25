"""Authenticated internal routes for append-only, read-only Agent research state."""

import hashlib
import json
from datetime import datetime
from typing import Annotated, Literal, cast
from uuid import UUID

from fastapi import APIRouter, Depends, Header, Query, Request
from fastapi.responses import JSONResponse
from nexus.agents.budget import BudgetExhausted
from nexus.agents.codex_versions import (
    CANDIDATE_BUDGET_CONFIG_VERSION,
    FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
    ISOLATED_CAPACITY_CONFIG_VERSIONS,
    PORTFOLIO_DECISION_CONFIG_VERSION,
    SDK_CONFIG_VERSIONS,
    STAGED_CONFIG_VERSIONS,
)
from nexus.agents.committee import (
    DISCLOSURE_SCOPE_PROMPT_VERSION,
    MONITORING_SCOPE_PROMPT_VERSIONS,
    StageInputIdentity,
)
from nexus.agents.concentration import condition_evaluations_match
from nexus.agents.execution_authority import load_frozen_evidence
from nexus.agents.models import ResearchRequest
from nexus.agents.observation_readback import ObservationReadback, readable_semantics
from nexus.agents.persistence import (
    admit_research_run,
    load_complete_prompt_authority,
    select_authoritative_snapshot,
)
from nexus.agents.portfolio_context import PortfolioResearchContext, read_portfolio_context
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import get_settings, require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.research_disclosure_monitoring import read_disclosure_monitoring_view
from nexus.api.research_inputs import read_research_inputs
from nexus.api.research_monitoring import read_monitoring_view
from nexus.api.research_stages import read_stage_trace
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
from nexus.market.current_identity import identity_parse_scope
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.market import Instrument
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from nexus.settings import AgentRuntimeMode, EngineSettings
from nexus.strategy.intents import ResearchIntent
from pydantic import ValidationError
from sqlalchemy import desc, select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()

# Billing display follows the stored exact SDK version, independently of admission.
_SUBSCRIPTION_RESEARCH_VERSIONS = (
    SDK_CONFIG_VERSIONS
    | ISOLATED_CAPACITY_CONFIG_VERSIONS
    | {
        CANDIDATE_BUDGET_CONFIG_VERSION,
        FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
    }
)


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
    reason: (
        Literal[
            "instrument_not_found",
            "decision_time_in_future",
            "data_snapshot_unavailable",
            "prompt_authority_unavailable",
            "model_pricing_unavailable",
        ]
        | None
    ) = None
    snapshot = None
    if bounded_as_of > now:
        reason = "decision_time_in_future"
    elif await session.get(Instrument, instrument_id) is None:
        reason = "instrument_not_found"
    else:
        snapshot = await select_authoritative_snapshot(
            session, instrument_id=instrument_id, as_of=bounded_as_of
        )
        if snapshot is None:
            reason = "data_snapshot_unavailable"
        elif await load_complete_prompt_authority(session) is None:
            reason = "prompt_authority_unavailable"
        elif not await session.scalar(
            select(AgentModelConfigRecord.id)
            .where(
                AgentModelConfigRecord.pricing_available_at <= now,
                AgentModelConfigRecord.pricing_expires_at > now,
                AgentModelConfigRecord.version.not_in(
                    (
                        *STAGED_CONFIG_VERSIONS,
                        CANDIDATE_BUDGET_CONFIG_VERSION,
                        "codex-sdk-strategy-rnd-v1",
                        PORTFOLIO_DECISION_CONFIG_VERSION,
                    )
                ),
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
        inputs=(
            await read_research_inputs(
                session, snapshot_id=snapshot.id, instrument_id=instrument_id, as_of=bounded_as_of
            )
            if snapshot is not None
            else None
        ),
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
            allow_fixture_price_authority=(settings.agent_runtime_mode is AgentRuntimeMode.FIXTURE),
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
            # Archive normalization becomes visible only when actually imported.
            (AgentResearchRunRecord.actor_id != "historical-archive-consumer-v1")
            | (AgentResearchRunRecord.created_at <= bounded_as_of),
        )
        .order_by(desc(AgentResearchRunRecord.decision_time), desc(AgentResearchRunRecord.id))
        .limit(1)
    )
    if run is None:
        raise NexusError(ErrorCode.NOT_FOUND, "Instrument research was not found.", status_code=404)
    output = await _latest_output(session, run.id)
    if (
        run.actor_id == "historical-archive-consumer-v1"
        and output is not None
        and output.created_at > bounded_as_of
    ):
        output = None
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
    *,
    observation_reader: ObservationReadback | None = None,
) -> ResearchRunResponse:
    with identity_parse_scope():
        evidence_records: tuple[AgentResearchOutputEvidenceRecord, ...] = ()
        if output is not None:
            evidence_records = tuple(
                await session.scalars(
                    select(AgentResearchOutputEvidenceRecord)
                    .where(AgentResearchOutputEvidenceRecord.output_id == output.id)
                    .order_by(AgentResearchOutputEvidenceRecord.evidence_ref_id)
                )
            )
        if _declares_current_trends(run) and not await load_frozen_evidence(
            session, run, allow_fixture_evidence=False
        ):
            # A saved conclusion cannot outlive the exact supplemental source it declares.
            # _response turns the now-unbound output into the existing safe abstained view.
            evidence_records = ()
        context = await read_portfolio_context(
            session, run.data_snapshot_id, run.instrument_id, run.decision_time
        )
        reader = observation_reader or ObservationReadback(session, read_at=utc_now())
        observation_verified = await reader.verified(run, output, evidence_records)
        response = _response(
            run,
            output,
            evidence_records,
            portfolio_context=context,
            observation_verified=observation_verified,
        )
        return response.model_copy(
            update={
                "monitoring": (
                    await read_monitoring_view(session, run, now=reader.read_at)
                    if _declares_monitoring(run)
                    else None
                ),
                "disclosure_monitoring": (
                    await read_disclosure_monitoring_view(session, run, now=reader.read_at)
                    if _declares_disclosure_monitoring(run)
                    else None
                ),
                "stage_trace": await read_stage_trace(session, run),
                "inputs": await read_research_inputs(
                    session,
                    snapshot_id=run.data_snapshot_id,
                    instrument_id=run.instrument_id,
                    as_of=run.decision_time,
                ),
            }
        )


def _declares_disclosure_monitoring(run: AgentResearchRunRecord) -> bool:
    manifest = run.prompt_manifest
    portfolio = manifest.get("portfolio") if isinstance(manifest, dict) else None
    return (
        isinstance(portfolio, dict) and portfolio.get("version") == DISCLOSURE_SCOPE_PROMPT_VERSION
    )


def _declares_monitoring(run: AgentResearchRunRecord) -> bool:
    manifest = run.prompt_manifest
    portfolio = manifest.get("portfolio") if isinstance(manifest, dict) else None
    return (
        isinstance(portfolio, dict) and portfolio.get("version") in MONITORING_SCOPE_PROMPT_VERSIONS
    )


def _declares_current_trends(run: AgentResearchRunRecord) -> bool:
    manifest = run.evidence_manifest
    return isinstance(manifest, list) and any(
        isinstance(item, dict) and item.get("source") == "alpaca_current_trends"
        for item in manifest
    )


def _response(
    run: AgentResearchRunRecord,
    output: AgentResearchOutputRecord | None,
    evidence_records: tuple[AgentResearchOutputEvidenceRecord, ...] = (),
    *,
    portfolio_context: PortfolioResearchContext | None = None,
    observation_verified: bool = False,
) -> ResearchRunResponse:
    intent = None
    abstention_reason = None
    failed_stage_input = None
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
    if intent is not None and (
        intent.instrument_id != run.instrument_id
        or intent.data_snapshot_id != run.data_snapshot_id
        or intent.decision_time != run.decision_time
        or not _intent_is_authority_bound(intent, evidence)
    ):
        intent = None
        evidence = ()
        abstention_reason = "evidence_authority_invalid"
    if intent is not None and not condition_evaluations_match(intent, portfolio_context):
        intent = None
        abstention_reason = "condition_evaluation_invalid"
    semantics_invalid = intent is not None and not readable_semantics(
        intent, run.pricing_version, observation_verified=observation_verified
    )
    if semantics_invalid:
        intent = None
        abstention_reason = "staged_action_semantics_unverified"
    if output is not None and output.intent is None:
        raw_reason = output.payload.get("reason")
        if isinstance(raw_reason, str) and len(raw_reason) <= 100:
            abstention_reason = raw_reason
        if run.status in {"failed", "abstained"} and output.payload.get("stage_input"):
            try:
                failed_stage_input = StageInputIdentity.model_validate(
                    output.payload["stage_input"]
                )
            except ValidationError:
                pass
    status = run.status
    if abstention_reason is not None and status == "completed" and not semantics_invalid:
        status = "abstained"
    return ResearchRunResponse(
        run_id=run.id,
        instrument_id=run.instrument_id,
        data_snapshot_id=run.data_snapshot_id,
        decision_time=run.decision_time,
        status=cast(AgentRunStatus, status),
        retry_count=run.retry_count,
        billing_mode="chatgpt_subscription"
        if run.pricing_version in _SUBSCRIPTION_RESEARCH_VERSIONS
        else None,
        input_tokens=run.input_tokens,
        output_tokens=run.output_tokens,
        reserved_cost_usd=decimal_to_string(run.reserved_cost_usd),
        settled_cost_usd=None
        if run.settled_cost_usd is None
        else decimal_to_string(run.settled_cost_usd),
        intent=intent,
        evidence=evidence,
        role_votes=() if intent is None else intent.role_votes,
        disagreement=None if intent is None else intent.disagreement,
        invalidation_conditions=() if intent is None else intent.invalidation_conditions,
        quality_status="partial"
        if semantics_invalid
        else (None if intent is None else intent.quality_status),
        budget_reason="budget_exhausted" if abstention_reason == "budget_exhausted" else None,
        abstention_reason=abstention_reason,
        failed_stage_input=failed_stage_input,
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
            () if intent.synthesis_assessment is None else intent.synthesis_assessment.claims,
            () if intent.synthesis_assessment is None else intent.synthesis_assessment.reviews,
            tuple(item.condition for item in intent.condition_evaluations or ()),
        )
        for item in collection
        for ref in item.evidence_refs
    )
    return all(_evidence_identity(ref) in authority for ref in nested)


def _evidence_identity(ref: EvidenceRef) -> tuple[str, str, datetime, str, str]:
    return (ref.source, ref.object_id, ref.available_at, ref.location, ref.content_hash)
