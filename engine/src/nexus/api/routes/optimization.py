# ruff: noqa: E501
"""Narrow, non-executable optimization APIs."""

import hashlib
from collections.abc import Sequence
from decimal import Decimal
from typing import Annotated, Any, Literal, cast
from uuid import UUID

from fastapi import APIRouter, Body, Depends, Header, Request
from fastapi.responses import JSONResponse
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.routes.portfolios import _utc_now
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.api.schemas.optimization import (
    EconomicComparisonResponse,
    OptimizationJobData,
    OptimizationJobEnvelope,
    OptimizationLifecycleData,
    OptimizationLifecycleEnvelope,
    OptimizationLifecycleRequest,
    OptimizationProposalGroupEnvelope,
    OptimizationProposalGroupResponse,
    OptimizationProposalPositionResponse,
    OptimizationProposalResponse,
    ProposalAdmissionRequest,
    ProposalComparisonAuthorityResponse,
    ProposalComparisonDimensionsResponse,
    ProposalComparisonMetricsResponse,
    ProposalComparisonResponse,
    ProposalDraftRequest,
    ShadowAttemptResponse,
    ShadowCheckpointResponse,
    ShadowCoverageResponse,
    ShadowEligibilityResponse,
    ShadowFillResponse,
    ShadowFreshnessResponse,
    ShadowGroupEnvelope,
    ShadowGroupResponse,
    ShadowMetricsResponse,
    ShadowMetricValueResponse,
    ShadowNavPointResponse,
    ShadowOutcomeResponse,
    ShadowPortfolioResponse,
    ShadowSafetyResponse,
    ShadowSourceResponse,
    ShadowStartData,
    ShadowStartEnvelope,
    ShadowVariantMetricsResponse,
    ShadowWorstSessionResponse,
    WhatIfEnvelope,
    WhatIfRequest,
    WhatIfResponse,
)
from nexus.audit.service import append_audit_event
from nexus.domain.decimal import decimal_to_string
from nexus.optimizer.admission import admit_queued_job
from nexus.optimizer.authority import load_admission_authority
from nexus.optimizer.compare import ComparableProposal, ComparisonDimensions, compare_proposals
from nexus.optimizer.construction import load_analysis_construction_inputs
from nexus.optimizer.cost_policy import COST_POLICY_ID
from nexus.optimizer.initial_draft import InitialDraftUnavailable, create_initial_draft
from nexus.optimizer.lifecycle import reject_group_idempotently
from nexus.optimizer.models import (
    PortfolioScope,
    ProposalAction,
    ProposalGroupStatus,
    ProposalVariant,
)
from nexus.optimizer.repository import has_executable_shadow_authority
from nexus.optimizer.research_context import read_draft_research_context
from nexus.optimizer.what_if import evaluate_what_if as evaluate_what_if_weights
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.optimization import (
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
    ProposalComparisonRecord,
    ProposalPositionRecord,
)
from nexus.persistence.models.portfolio import PortfolioSnapshotRecord
from nexus.quality.source_policy import current_proposal_shadow_price_authority_available
from nexus.reconciliation.admission_claim import claim_or_lock, validate_request_identity
from nexus.reconciliation.persistence_errors import (
    BrokerSyncPersistenceError,
    IdempotencyConflict,
)
from nexus.shadow.models import ShadowMetrics, ShadowMetricValue, VariantMetrics
from nexus.shadow.query import ShadowGroupView, load_shadow_view
from nexus.shadow.start import start_proposal_shadow
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()
_MAX_WHAT_IF_BYTES = 64 * 1024
_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    status: {"model": NexusErrorEnvelope, "description": "Stable NEXUS error envelope."}
    for status in (401, 404, 409, 422)
}
_CREATE_RESPONSES = {
    202: {
        "description": "Optimization job accepted or replayed.",
        "model": OptimizationJobEnvelope,
        "headers": {"Location": {"schema": {"type": "string"}}},
    },
    **_ERROR_RESPONSES,
}

_IdempotencyKey = Annotated[
    str, Header(alias="Idempotency-Key", min_length=1, max_length=255)
]
_IfMatch = Annotated[str, Header(alias="If-Match", min_length=1, max_length=64)]

_WHAT_IF_REQUEST_BODY = {
    "requestBody": {
        "required": True,
        "content": {"application/json": {"schema": {"$ref": "#/components/schemas/WhatIfRequest"}}},
    }
}
_PROPOSAL_ADMISSION_REQUEST_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {"schema": {"$ref": "#/components/schemas/ProposalAdmissionRequest"}}
        },
    }
}
_LIFECYCLE_REQUEST_BODY = {
    "requestBody": {
        "required": True,
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/OptimizationLifecycleRequest"}
            }
        },
    }
}


async def _bounded_what_if_request(request: Request) -> WhatIfRequest:
    """Read and cache at most 64 KiB before JSON parsing or HMAC verification."""
    if not _is_json_media_type(request.headers.get("Content-Type")):
        raise NexusError(ErrorCode.VALIDATION_ERROR, "Content-Type must be application/json.", status_code=422)
    content_length = request.headers.get("Content-Length")
    if content_length is not None:
        if not _is_canonical_content_length(content_length):
            raise NexusError(
                ErrorCode.VALIDATION_ERROR, "Content-Length is invalid.", status_code=422
            )
        if int(content_length) > _MAX_WHAT_IF_BYTES:
            raise NexusError(ErrorCode.VALIDATION_ERROR, "Request is too large.", status_code=422)
    body = bytearray()
    async for chunk in request.stream():
        if len(chunk) > _MAX_WHAT_IF_BYTES - len(body):
            raise NexusError(ErrorCode.VALIDATION_ERROR, "Request is too large.", status_code=422)
        body.extend(chunk)
    raw_body = bytes(body)
    setattr(request, "_body", raw_body)
    try:
        return WhatIfRequest.model_validate_json(raw_body)
    except ValueError as error:
        raise NexusError(ErrorCode.VALIDATION_ERROR, "Request is invalid.", status_code=422) from error


def _is_canonical_content_length(value: str) -> bool:
    return value == "0" or (
        value.isascii() and value.isdecimal() and value[0] in "123456789"
    )


def _is_json_media_type(value: str | None) -> bool:
    if value is None:
        return False
    parts = [part.strip() for part in value.split(";")]
    if not parts or parts[0].lower() != "application/json":
        return False
    return all(
        name.strip().lower() == "charset" and separator == "=" and raw_value.strip()
        for parameter in parts[1:]
        for name, separator, raw_value in (parameter.partition("="),)
    )


@router.post(
    "/internal/v1/what-if/evaluate",
    response_model=WhatIfEnvelope,
    responses=_ERROR_RESPONSES,
    openapi_extra=_WHAT_IF_REQUEST_BODY,
)
async def evaluate_what_if(
    request: Request,
    idempotency_key: _IdempotencyKey,
    payload: WhatIfRequest = Depends(_bounded_what_if_request),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> WhatIfEnvelope:
    """Return deterministic analysis and append only a non-sensitive audit summary."""
    raw_body = await request.body()
    group = await session.get(OptimizationProposalGroupRecord, payload.proposal_group_id)
    if group is None:
        raise NexusError(ErrorCode.NOT_FOUND, "Proposal group was not found.", status_code=404)
    proposal = await session.scalar(
        select(OptimizationProposalRecord).where(
            OptimizationProposalRecord.id == payload.proposal_id,
            OptimizationProposalRecord.proposal_group_id == group.id,
        )
    )
    if proposal is None:
        raise NexusError(ErrorCode.NOT_FOUND, "Proposal was not found.", status_code=404)
    allowed_instruments = set(
        await session.scalars(
            select(ProposalPositionRecord.instrument_id).where(
                ProposalPositionRecord.proposal_id == proposal.id
            )
        )
    )
    if {position.instrument_id for position in payload.positions} != allowed_instruments:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR,
            "Submitted instruments do not match the proposal universe.",
            status_code=422,
        )
    request_hash = hashlib.sha256(raw_body).hexdigest()
    _validate_claim(actor.actor_id, "/internal/v1/what-if/evaluate", idempotency_key, request_hash)
    try:
        record, owner = await claim_or_lock(
            session,
            actor.actor_id,
            "/internal/v1/what-if/evaluate",
            idempotency_key,
            request_hash,
            "what_if_v1",
        )
    except IdempotencyConflict as error:
        raise NexusError(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "Idempotency key has a different request body.",
            status_code=409,
        ) from error
    if not owner and record.request_hash != request_hash:
        raise NexusError(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "Idempotency key has a different request body.",
            status_code=409,
        )
    if owner:
        await append_audit_event(
            session,
            actor_id=actor.actor_id,
            action="what_if_evaluated",
            schema_version="1",
            payload={
                "policy_version": payload.policy_version,
                "instrument_count": len(payload.positions),
                "request_hash": request_hash,
            },
            occurred_at=_utc_now(request),
        )
    return WhatIfEnvelope(
        data=WhatIfResponse(
            proposal_group_id=group.id,
            proposal_id=proposal.id,
            policy_version=payload.policy_version,
            instrument_count=len(payload.positions),
            result=evaluate_what_if_weights(
                tuple(Decimal(position.weight) for position in payload.positions)
            ),
        ),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=_utc_now(request)),
    )


@router.post(
    "/internal/v1/optimization-proposals",
    response_model=OptimizationJobEnvelope,
    status_code=202,
    responses=_CREATE_RESPONSES,
    openapi_extra=_PROPOSAL_ADMISSION_REQUEST_BODY,
)
async def admit_optimization_proposal(
    request: Request,
    idempotency_key: _IdempotencyKey,
    body: ProposalAdmissionRequest = Body(),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> object:
    """Fail closed until one persisted authority binds baseline, diagnosis, data, source, and cost."""
    route = "/internal/v1/optimization-proposals"
    raw_body = await request.body()
    request_hash = hashlib.sha256(raw_body).hexdigest()
    _validate_claim(actor.actor_id, route, idempotency_key, request_hash)
    loaded = await load_admission_authority(
        session,
        portfolio_snapshot_id=body.portfolio_snapshot_id,
        data_snapshot_id=body.data_snapshot_id,
        cost_policy_id=body.cost_policy_id,
    )
    construction = None if loaded is not None else await load_analysis_construction_inputs(
        session, portfolio_snapshot_id=body.portfolio_snapshot_id,
        data_snapshot_id=body.data_snapshot_id, cost_policy_id=body.cost_policy_id,
        now=_utc_now(request),
    )
    if loaded is None and construction is None:
        raise NexusError(
            ErrorCode.DATA_PARTIAL,
            "Persisted optimization authority is incomplete.",
            status_code=409,
            details={"reason": "OPTIMIZATION_AUTHORITY_MISSING"},
        )
    if loaded is not None:
        portfolio_id, valuation_time = loaded.portfolio_id, loaded.persistence.valuation_time
    else:
        assert construction is not None
        portfolio_id, valuation_time = construction.portfolio_id, construction.valuation_time
    try:
        job = await admit_queued_job(
            session,
            actor_id=actor.actor_id,
            route=route,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            payload={
                "portfolio_snapshot_id": str(body.portfolio_snapshot_id),
                "data_snapshot_id": str(body.data_snapshot_id),
                "cost_policy_id": str(body.cost_policy_id),
            },
            portfolio_id=portfolio_id,
            effective_at=valuation_time,
        )
    except IdempotencyConflict as error:
        raise NexusError(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "Idempotency key has a different request body.",
            status_code=409,
        ) from error
    payload = OptimizationJobEnvelope(
        data=OptimizationJobData(job_id=job.job_id, status="QUEUED"),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=_utc_now(request)),
    )
    location = f"/api/v1/jobs/{job.job_id}"
    return JSONResponse(
        status_code=202,
        content=payload.model_dump(mode="json"),
        headers={"Location": location},
    )


@router.get(
    "/internal/v1/optimization-proposals/{group_id}",
    response_model=OptimizationProposalGroupEnvelope,
    responses=_ERROR_RESPONSES,
)
async def get_optimization_group(
    group_id: UUID,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> OptimizationProposalGroupEnvelope:
    """Return immutable children and the sole mutable lifecycle state."""
    proposal_group_id = group_id
    group = await session.get(OptimizationProposalGroupRecord, proposal_group_id)
    if group is None:
        raise NexusError(ErrorCode.NOT_FOUND, "Proposal group was not found.", status_code=404)
    baseline = await session.get(
        PortfolioSnapshotRecord, group.baseline_portfolio_snapshot_id
    )
    if baseline is None:
        raise NexusError(
            ErrorCode.DATA_PARTIAL, "Proposal baseline is missing.", status_code=409
        )
    latest_ready_snapshot_id = await session.scalar(
        select(PortfolioSnapshotRecord.id)
        .where(
            PortfolioSnapshotRecord.portfolio_id == baseline.portfolio_id,
            PortfolioSnapshotRecord.status == "READY",
        )
        .order_by(
            PortfolioSnapshotRecord.created_at.desc(),
            PortfolioSnapshotRecord.id.desc(),
        )
        .limit(1)
    )
    baseline_superseded = latest_ready_snapshot_id != baseline.id
    try:
        research_context = read_draft_research_context(
            await session.get(DataSnapshot, group.data_snapshot_id),
            portfolio_snapshot_id=group.baseline_portfolio_snapshot_id,
            diagnostic_report_id=group.diagnostic_report_id,
            valuation_time=group.valuation_time,
        )
    except ValueError as error:
        raise NexusError(
            ErrorCode.DATA_PARTIAL, "Proposal research context is invalid.", status_code=409
        ) from error
    children = list(
        (
            await session.execute(
                select(OptimizationProposalRecord)
                .where(OptimizationProposalRecord.proposal_group_id == proposal_group_id)
                .order_by(OptimizationProposalRecord.variant)
            )
        )
        .scalars()
        .all()
    )
    if len(children) != 3:
        raise NexusError(ErrorCode.DATA_PARTIAL, "Proposal group is incomplete.", status_code=409)
    by_variant = {child.variant: child for child in children}
    variants = ("hold_current", "minimum_change", "strategy_target")
    if tuple(by_variant) != variants:
        raise NexusError(ErrorCode.DATA_PARTIAL, "Proposal group variants are invalid.", status_code=409)
    symbol_rows = (await session.execute(
        select(InstrumentAlias.instrument_id, InstrumentAlias.symbol)
        .join(Instrument, Instrument.id == InstrumentAlias.instrument_id)
        .join(ProposalPositionRecord, ProposalPositionRecord.instrument_id == Instrument.id)
        .where(
            ProposalPositionRecord.proposal_id.in_(child.id for child in children),
            Instrument.created_at <= group.valuation_time,
            InstrumentAlias.created_at <= group.valuation_time,
            InstrumentAlias.alias_type == "ticker",
            InstrumentAlias.valid_from <= group.valuation_time.date(),
            InstrumentAlias.valid_to.is_(None)
            | (InstrumentAlias.valid_to >= group.valuation_time.date()),
        )
        .distinct()
    )).all()
    symbols = {instrument_id: symbol for instrument_id, symbol in symbol_rows}
    proposal_data: list[OptimizationProposalResponse] = []
    for child in children:
        positions = list(
            await session.scalars(
                select(ProposalPositionRecord)
                .where(ProposalPositionRecord.proposal_id == child.id)
                .order_by(ProposalPositionRecord.instrument_id)
            )
        )
        comparison = await session.scalar(
            select(ProposalComparisonRecord).where(ProposalComparisonRecord.proposal_id == child.id)
        )
        if comparison is None:
            raise NexusError(
                ErrorCode.DATA_PARTIAL, "Proposal comparison is incomplete.", status_code=409
            )
        try:
            metrics = comparison.metrics
            proposal_data.append(
                OptimizationProposalResponse(
                    proposal_id=child.id,
                    variant=cast(ProposalVariant, child.variant),
                    analysis_only=child.analysis_only,
                    strategy_evidence_complete=child.strategy_evidence_complete,
                    portfolio_scope=cast(PortfolioScope, child.portfolio_scope),
                    positions=tuple(
                        OptimizationProposalPositionResponse(
                            instrument_id=position.instrument_id,
                            display_symbol=symbols.get(position.instrument_id),
                            current_weight=decimal_to_string(position.current_weight),
                            target_weight=decimal_to_string(position.target_weight),
                            action=cast(ProposalAction, position.action),
                            reason_code=position.reason_code,
                        )
                        for position in positions
                    ),
                    comparison=ProposalComparisonResponse(
                        metrics=ProposalComparisonMetricsResponse(
                            dimensions=ProposalComparisonDimensionsResponse.model_validate(
                                metrics["dimensions"]
                            ),
                            ranking_eligible=cast(bool, metrics["ranking_eligible"]),
                            constraint_complete=cast(bool, metrics["constraint_complete"]),
                            eligibility_complete=cast(bool, metrics["eligibility_complete"]),
                            evidence_complete=cast(bool, metrics["evidence_complete"]),
                            stale=cast(bool, metrics["stale"]) or baseline_superseded,
                            unknown_metrics=tuple(cast(Sequence[str], metrics["unknown_metrics"])),
                            authority=ProposalComparisonAuthorityResponse.model_validate(
                                metrics["authority"]
                            ),
                        ),
                        tax_impact=cast(Literal["unknown"], comparison.tax_impact),
                        requires_fx=comparison.requires_fx,
                        non_executable=comparison.non_executable,
                        cost_complete=comparison.cost_complete,
                    ),
                )
            )
        except (KeyError, TypeError, ValueError) as error:
            raise NexusError(
                ErrorCode.DATA_PARTIAL, "Proposal comparison is invalid.", status_code=409
            ) from error
    reasons = _shadow_eligibility_reasons(group, proposal_data)
    if not reasons and not current_proposal_shadow_price_authority_available(group.source_policy_id):
        reasons.append("SHADOW_PRICE_AUTHORITY_UNAVAILABLE")
    if not reasons and not await has_executable_shadow_authority(session, group):
        reasons.append("PERSISTED_SHADOW_AUTHORITY_INCOMPLETE")
    return OptimizationProposalGroupEnvelope(
        data=OptimizationProposalGroupResponse(
            proposal_group_id=group.id,
            status=cast(ProposalGroupStatus, group.status),
            version=group.version,
            baseline_portfolio_snapshot_id=group.baseline_portfolio_snapshot_id,
            portfolio_scope=cast(PortfolioScope, group.portfolio_scope),
            diagnostic_input_hash=group.diagnostic_input_hash,
            data_snapshot_id=group.data_snapshot_id,
            source_policy_id=group.source_policy_id,
            cost_policy_id=group.cost_policy_id,
            proposals=(proposal_data[0], proposal_data[1], proposal_data[2]),
            economic_comparison=_economic_comparison(group, proposal_data),
            research_context=research_context,
            shadow_eligibility=ShadowEligibilityResponse(
                eligible=not reasons, reasons=tuple(reasons)
            ),
        ),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=group.valuation_time),
    )


@router.post(
    "/internal/v1/optimization-proposal-drafts",
    response_model=OptimizationProposalGroupEnvelope,
    status_code=201,
    responses=_ERROR_RESPONSES,
)
async def create_optimization_draft(
    request: Request,
    idempotency_key: _IdempotencyKey,
    body: ProposalDraftRequest,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> OptimizationProposalGroupEnvelope:
    """Create the first non-executable scenario without queueing a formal proposal Job."""
    request_hash = hashlib.sha256(b"initial-draft\0" + await request.body()).hexdigest()
    _validate_claim(actor.actor_id, "initial-optimization-draft", idempotency_key, request_hash)
    try:
        result = await create_initial_draft(
            session, portfolio_snapshot_id=body.portfolio_snapshot_id,
            diagnostic_report_id=body.diagnostic_report_id, actor_id=actor.actor_id,
            idempotency_key=idempotency_key, request_hash=request_hash, now=_utc_now(request),
            artifact_reader=getattr(request.app.state, "diagnostic_artifact_reader", None),
            research_context=body.research_context,
        )
    except IdempotencyConflict as error:
        raise NexusError(ErrorCode.IDEMPOTENCY_CONFLICT, "Draft request conflicts with prior request.", status_code=409) from error
    except InitialDraftUnavailable as error:
        raise NexusError(
            ErrorCode.DATA_PARTIAL, "Stored facts do not support an initial draft.",
            status_code=409, details={"reason": error.reason},
        ) from error
    except ValueError as error:
        raise NexusError(
            ErrorCode.DATA_PARTIAL, "Stored facts do not support an initial draft.",
            status_code=409, details={"reason": "INITIAL_DRAFT_EVIDENCE_INCOMPLETE"},
        ) from error
    return await get_optimization_group(result.group_id, actor=actor, session=session)


async def _transition_endpoint(
    group_id: UUID,
    request: Request,
    body: OptimizationLifecycleRequest,
    idempotency_key: str,
    actor: VerifiedServiceActor,
    session: AsyncSession,
) -> OptimizationLifecycleEnvelope:
    raw_body = await request.body()
    request_hash = hashlib.sha256(b"rejected\0" + raw_body).hexdigest()
    _validate_claim(
        actor.actor_id,
        f"optimization-proposal-lifecycle:{group_id}",
        idempotency_key,
        request_hash,
    )
    try:
        group = await reject_group_idempotently(
            session,
            actor_id=actor.actor_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            group_id=group_id,
            expected_version=body.version,
        )
    except IdempotencyConflict as error:
        raise NexusError(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "Idempotency key has a different lifecycle request.",
            status_code=409,
        ) from error
    except ValueError as error:
        code = (
            ErrorCode.PROPOSAL_NOT_EXECUTABLE
            if str(error) == "PROPOSAL_NOT_EXECUTABLE"
            else ErrorCode.VERSION_CONFLICT
            if str(error) == "VERSION_CONFLICT"
            else ErrorCode.VALIDATION_ERROR
        )
        raise NexusError(
            code,
            "Proposal lifecycle transition was rejected.",
            status_code=409
            if code in (ErrorCode.VERSION_CONFLICT, ErrorCode.PROPOSAL_NOT_EXECUTABLE)
            else 422,
        ) from error
    return OptimizationLifecycleEnvelope(
        data=OptimizationLifecycleData(
            proposal_group_id=group.group_id, status=group.status, version=group.version  # type: ignore[arg-type]
        ),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=_utc_now(request)),
    )


@router.post(
    "/internal/v1/optimization-proposals/{group_id}/shadow",
    response_model=ShadowStartEnvelope,
    responses=_ERROR_RESPONSES,
    openapi_extra=_LIFECYCLE_REQUEST_BODY,
    operation_id="startOptimizationProposalShadow",
)
async def shadow_optimization_group(
    group_id: UUID,
    request: Request,
    idempotency_key: _IdempotencyKey,
    if_match: _IfMatch,
    body: OptimizationLifecycleRequest = Body(),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> ShadowStartEnvelope:
    _validate_if_match(if_match, body.version)
    raw_body = await request.body()
    request_hash = hashlib.sha256(b"shadowing\0" + raw_body).hexdigest()
    _validate_claim(
        actor.actor_id,
        f"optimization-proposal-shadow:{group_id}",
        idempotency_key,
        request_hash,
    )
    try:
        result = await start_proposal_shadow(
            session,
            actor_id=actor.actor_id,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            group_id=group_id,
            expected_version=body.version,
        )
    except IdempotencyConflict as error:
        raise NexusError(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "Idempotency key has a different lifecycle request.",
            status_code=409,
        ) from error
    except ValueError as error:
        reason = str(error)
        public_reason = (
            reason
            if reason
            in {
                "VERSION_CONFLICT",
                "PROPOSAL_STATE_INVALID",
                "PROPOSAL_NOT_EXECUTABLE",
                "SHADOW_CALENDAR_INCOMPLETE",
            }
            else "SHADOW_START_INVALID"
        )
        code = (
            ErrorCode.PROPOSAL_NOT_EXECUTABLE
            if public_reason
            in {
                "PROPOSAL_STATE_INVALID",
                "PROPOSAL_NOT_EXECUTABLE",
                "SHADOW_CALENDAR_INCOMPLETE",
            }
            else ErrorCode.VERSION_CONFLICT
            if public_reason == "VERSION_CONFLICT"
            else ErrorCode.VALIDATION_ERROR
        )
        raise NexusError(
            code,
            "Proposal Shadow startup was rejected.",
            status_code=409
            if code in (ErrorCode.VERSION_CONFLICT, ErrorCode.PROPOSAL_NOT_EXECUTABLE)
            else 422,
            details={"reason": public_reason},
        ) from error
    return ShadowStartEnvelope(
        data=ShadowStartData(
            proposal_group_id=result.proposal_group_id,
            shadow_group_id=result.shadow_group_id,
            status=result.status,
            version=result.version,
            start_session=result.start_session,
            evaluation_end_session=result.evaluation_end_session,
            grace_end_session=result.grace_end_session,
        ),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=_utc_now(request)),
    )


@router.get(
    "/internal/v1/optimization-proposals/{group_id}/shadow",
    response_model=ShadowGroupEnvelope,
    responses=_ERROR_RESPONSES,
    operation_id="getOptimizationProposalShadow",
)
async def get_optimization_group_shadow(
    group_id: UUID,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> ShadowGroupEnvelope:
    try:
        view = await load_shadow_view(session, group_id=group_id)
    except ValueError as error:
        reason = str(error)
        if reason == "SHADOW_NOT_STARTED":
            raise NexusError(
                ErrorCode.NOT_FOUND,
                "Proposal Shadow was not started.",
                status_code=404,
                details={"reason": "SHADOW_NOT_STARTED"},
            ) from error
        if reason == "PROPOSAL_GROUP_NOT_FOUND":
            raise NexusError(
                ErrorCode.NOT_FOUND,
                "Proposal group was not found.",
                status_code=404,
                details={"reason": "PROPOSAL_GROUP_NOT_FOUND"},
            ) from error
        raise NexusError(
            ErrorCode.DATA_PARTIAL,
            "Proposal Shadow data is incomplete.",
            status_code=409,
            details={"reason": "DATA_PARTIAL"},
        ) from error
    return ShadowGroupEnvelope(
        data=_shadow_group_response(view),
        meta=SuccessMetaResponse(
            request_id=actor.request_id,
            as_of=view.freshness.database_time,
        ),
    )


def _shadow_group_response(view: ShadowGroupView) -> ShadowGroupResponse:
    return ShadowGroupResponse(
        proposal_group_id=view.proposal_group_id,
        shadow_group_id=view.shadow_group_id,
        proposal_status=view.proposal_status,
        version=view.version,
        start_session=view.start_session,
        evaluation_end_session=view.evaluation_end_session,
        grace_end_session=view.grace_end_session,
        coverage=ShadowCoverageResponse(
            completed=view.coverage.completed,
            due=view.coverage.due,
            required=view.coverage.required,
            next_expected_session=view.coverage.next_expected_session,
            missing_sessions=view.coverage.missing_sessions,
        ),
        freshness=ShadowFreshnessResponse(
            status=view.freshness.status,
            latest_session=view.freshness.latest_session,
            available_at=view.freshness.available_at,
            last_attempt_at=view.freshness.last_attempt_at,
            database_time=view.freshness.database_time,
            reason=view.freshness.reason,
        ),
        sources=tuple(
            ShadowSourceResponse(
                source=item.source,
                kind=cast(Literal["price_bars", "corporate_actions"], item.kind),
                delivery_mode=cast(
                    Literal["real_time", "delayed", "fixture", "unknown"],
                    item.delivery_mode,
                ),
                latest_session=item.latest_session,
                available_at=item.available_at,
            )
            for item in view.sources
        ),
        portfolios=cast(
            tuple[
                ShadowPortfolioResponse,
                ShadowPortfolioResponse,
                ShadowPortfolioResponse,
            ],
            tuple(
                ShadowPortfolioResponse(
                    shadow_portfolio_id=portfolio.shadow_portfolio_id,
                    variant=portfolio.variant,
                    nav=tuple(
                        ShadowNavPointResponse(
                            session=point.session,
                            nav=decimal_to_string(point.nav),
                        )
                        for point in portfolio.nav
                    ),
                    initial_outcomes=tuple(
                        ShadowOutcomeResponse(
                            instrument_id=outcome.instrument_id,
                            side=cast(
                                Literal["buy", "sell", "rebalance"], outcome.side
                            ),
                            status=cast(
                                Literal["filled", "missing_bar", "untradable"],
                                outcome.status,
                            ),
                            reason=outcome.reason,
                            fill=(
                                None
                                if outcome.fill is None
                                else ShadowFillResponse(
                                    side=cast(
                                        Literal["buy", "sell"], outcome.fill.side
                                    ),
                                    quantity=decimal_to_string(outcome.fill.quantity),
                                    price=decimal_to_string(outcome.fill.price),
                                    notional=decimal_to_string(outcome.fill.notional),
                                    cost=decimal_to_string(outcome.fill.cost),
                                )
                            ),
                        )
                        for outcome in portfolio.initial_outcomes
                    ),
                )
                for portfolio in view.portfolios
            ),
        ),
        checkpoints=tuple(
            ShadowCheckpointResponse(
                ordinal=cast(Literal[5, 21, 63], checkpoint.ordinal),
                session=checkpoint.session,
                status=cast(Literal["complete", "incomplete"], checkpoint.status),
                coverage_count=checkpoint.coverage_count,
                reason=checkpoint.reason,
                metrics=cast(
                    tuple[
                        ShadowVariantMetricsResponse,
                        ShadowVariantMetricsResponse,
                        ShadowVariantMetricsResponse,
                    ],
                    tuple(_shadow_variant_metrics(item) for item in checkpoint.metrics),
                ),
            )
            for checkpoint in view.checkpoints
        ),
        last_failed_attempt=(
            None
            if view.last_failed_attempt is None
            else ShadowAttemptResponse(
                requested_session=view.last_failed_attempt.requested_session,
                expected_session=view.last_failed_attempt.expected_session,
                result=cast(
                    Literal[
                        "data_partial",
                        "data_stale",
                        "source_conflict",
                        "session_gap",
                    ],
                    view.last_failed_attempt.result,
                ),
                reason=view.last_failed_attempt.reason,
                observed_at=view.last_failed_attempt.observed_at,
                created_at=view.last_failed_attempt.created_at,
            )
        ),
        sealed=view.sealed,
        seal_reason=view.seal_reason,
        safety=ShadowSafetyResponse(
            creates_orders=view.creates_orders,
            trading_approval=view.trading_approval,
        ),
    )


def _shadow_variant_metrics(value: VariantMetrics) -> ShadowVariantMetricsResponse:
    return ShadowVariantMetricsResponse(
        variant=value.variant,
        metrics=_shadow_metrics(value.metrics),
    )


def _shadow_metrics(value: ShadowMetrics) -> ShadowMetricsResponse:
    return ShadowMetricsResponse(
        net_cumulative_return=_shadow_metric(value.net_cumulative_return),
        return_delta_vs_hold=_shadow_metric(value.return_delta_vs_hold),
        max_drawdown=_shadow_metric(value.max_drawdown),
        drawdown_improvement=_shadow_metric(value.drawdown_improvement),
        annualized_volatility=_shadow_metric(value.annualized_volatility),
        volatility_improvement=_shadow_metric(value.volatility_improvement),
        worst_sessions=tuple(
            ShadowWorstSessionResponse(
                session=item.session,
                return_value=decimal_to_string(item.return_value),
            )
            for item in value.worst_sessions
        ),
        cumulative_turnover=_shadow_metric(value.cumulative_turnover),
        fee_drag_index_points=_shadow_metric(value.fee_drag_index_points),
    )


def _shadow_metric(value: ShadowMetricValue) -> ShadowMetricValueResponse:
    return ShadowMetricValueResponse(
        status=value.status,
        value=None if value.value is None else decimal_to_string(value.value),
        reason=value.reason,
    )


@router.post(
    "/internal/v1/optimization-proposals/{group_id}/reject",
    response_model=OptimizationLifecycleEnvelope,
    responses=_ERROR_RESPONSES,
    openapi_extra=_LIFECYCLE_REQUEST_BODY,
)
async def reject_optimization_group(
    group_id: UUID,
    request: Request,
    idempotency_key: _IdempotencyKey,
    if_match: _IfMatch,
    body: OptimizationLifecycleRequest = Body(),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> OptimizationLifecycleEnvelope:
    _validate_if_match(if_match, body.version)
    return await _transition_endpoint(
        group_id, request, body, idempotency_key, actor, session
    )


def _validate_if_match(value: str, version: int) -> None:
    if value != f'"{version}"':
        raise NexusError(
            ErrorCode.VALIDATION_ERROR,
            "If-Match must equal the request version.",
            status_code=422,
        )


def _economic_comparison(
    group: OptimizationProposalGroupRecord,
    proposals: list[OptimizationProposalResponse],
) -> EconomicComparisonResponse:
    """Describe persisted comparison evidence without changing execution eligibility."""
    expected_authority = ProposalComparisonAuthorityResponse(
        diagnostic_report_id=group.diagnostic_report_id,
        data_snapshot_id=group.data_snapshot_id,
        source_policy_id=group.source_policy_id,
        optimizer_policy_id=group.policy_id,
        cost_policy_id=group.cost_policy_id,
    )
    # Historical real-account rows may mark the zero-cost methodology complete;
    # it has never been evidence of actual broker charges.
    cost_complete = group.cost_complete and not (
        group.portfolio_scope == "real_read_only" and group.cost_policy_id == COST_POLICY_ID
    )
    inputs = []
    for proposal in proposals:
        comparison = proposal.comparison
        metrics = comparison.metrics
        inputs.append(ComparableProposal(
            variant=proposal.variant,
            dimensions=ComparisonDimensions.model_validate(metrics.dimensions.model_dump()),
            requires_fx=group.requires_fx or comparison.requires_fx,
            constraint_complete=metrics.constraint_complete,
            eligibility_complete=metrics.eligibility_complete,
            evidence_complete=(
                group.baseline_ready and group.diagnostic_complete
                and metrics.evidence_complete and metrics.ranking_eligible
                and not metrics.unknown_metrics and not comparison.non_executable
                and not proposal.analysis_only
                and proposal.portfolio_scope == group.portfolio_scope
                and metrics.authority == expected_authority
                and (proposal.variant != "strategy_target" or proposal.strategy_evidence_complete)
            ),
            stale=group.authority_stale or metrics.stale,
            partial=group.partial,
            source_conflict=group.source_conflict,
            cost_complete=cost_complete and comparison.cost_complete,
        ))
    assessment = compare_proposals(tuple(inputs))
    return EconomicComparisonResponse(
        status=assessment.status,
        recommendation=assessment.recommendation,
        reasons=assessment.reasons,
    )


def _shadow_eligibility_reasons(
    group: OptimizationProposalGroupRecord,
    proposals: list[OptimizationProposalResponse],
) -> list[str]:
    reasons: list[str] = []
    if group.status != "compared":
        reasons.append("GROUP_NOT_COMPARED")
    if group.selection_authority_id is None or group.selection_evidence_hash is None:
        reasons.append("SELECTION_AUTHORITY_MISSING")
    for condition, reason in (
        (group.baseline_ready, "BASELINE_NOT_READY"),
        (group.diagnostic_complete, "DIAGNOSTIC_INCOMPLETE"),
        (not group.source_conflict, "SOURCE_CONFLICT"),
        (not group.partial, "DATA_PARTIAL"),
        (not group.requires_fx, "FX_REQUIRED"),
        (group.cost_complete, "COST_INCOMPLETE"),
        (not group.authority_stale, "AUTHORITY_STALE"),
    ):
        if not condition:
            reasons.append(reason)
    for proposal in proposals:
        if proposal.variant == "strategy_target" and not proposal.strategy_evidence_complete:
            reasons.append("STRATEGY_EVIDENCE_INCOMPLETE")
        for condition, reason in (
            (not proposal.comparison.non_executable, "COMPARISON_NON_EXECUTABLE"),
            (proposal.comparison.cost_complete, "COST_INCOMPLETE"),
            (not proposal.comparison.requires_fx, "FX_REQUIRED"),
            (proposal.comparison.metrics.evidence_complete, "COMPARISON_EVIDENCE_INCOMPLETE"),
            (not proposal.comparison.metrics.stale, "COMPARISON_STALE"),
            (proposal.comparison.metrics.constraint_complete, "CONSTRAINTS_INCOMPLETE"),
            (proposal.comparison.metrics.eligibility_complete, "ELIGIBILITY_INCOMPLETE"),
            (proposal.comparison.metrics.ranking_eligible, "RANKING_INELIGIBLE"),
            (not proposal.comparison.metrics.unknown_metrics, "COMPARISON_UNKNOWN_METRICS"),
        ):
            if not condition:
                reasons.append(reason)
    return list(dict.fromkeys(reasons))


def _validate_claim(actor_id: str, route: str, key: str, request_hash: str) -> None:
    try:
        validate_request_identity(actor_id, route, key, request_hash)
    except BrokerSyncPersistenceError as error:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR, "Idempotency identity is invalid.", status_code=422
        ) from error
