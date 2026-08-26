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
    ShadowEligibilityResponse,
    WhatIfEnvelope,
    WhatIfRequest,
    WhatIfResponse,
)
from nexus.optimizer.admission import admit_queued_job
from nexus.optimizer.authority import load_admission_authority
from nexus.optimizer.lifecycle import reject_group_idempotently
from nexus.optimizer.models import (
    PortfolioScope,
    ProposalAction,
    ProposalGroupStatus,
    ProposalVariant,
)
from nexus.optimizer.repository import has_executable_shadow_authority
from nexus.optimizer.what_if import evaluate_what_if as evaluate_what_if_weights
from nexus.persistence.models.optimization import (
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
    ProposalComparisonRecord,
    ProposalPositionRecord,
)
from nexus.persistence.models.runtime import AuditEvent
from nexus.reconciliation.admission_claim import claim_or_lock, validate_request_identity
from nexus.reconciliation.persistence_errors import (
    BrokerSyncPersistenceError,
    IdempotencyConflict,
)
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
        session.add(
            AuditEvent(
                actor_id=actor.actor_id,
                action="what_if_evaluated",
                schema_version="1",
                payload={
                    "policy_version": payload.policy_version,
                    "instrument_count": len(payload.positions),
                    "request_hash": request_hash,
                },
            )
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
    if loaded is None:
        raise NexusError(
            ErrorCode.DATA_PARTIAL,
            "Persisted optimization authority is incomplete.",
            status_code=409,
            details={"reason": "OPTIMIZATION_AUTHORITY_MISSING"},
        )
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
                            current_weight=str(position.current_weight),
                            target_weight=str(position.target_weight),
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
                            stale=cast(bool, metrics["stale"]),
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
            shadow_eligibility=ShadowEligibilityResponse(
                eligible=not reasons, reasons=tuple(reasons)
            ),
        ),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=group.valuation_time),
    )


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
    response_model=OptimizationLifecycleEnvelope,
    responses=_ERROR_RESPONSES,
    openapi_extra=_LIFECYCLE_REQUEST_BODY,
)
async def shadow_optimization_group(
    group_id: UUID,
    request: Request,
    idempotency_key: _IdempotencyKey,
    if_match: _IfMatch,
    body: OptimizationLifecycleRequest = Body(),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> OptimizationLifecycleEnvelope:
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
        code = (
            ErrorCode.PROPOSAL_NOT_EXECUTABLE
            if reason in {"PROPOSAL_NOT_EXECUTABLE", "SHADOW_CALENDAR_INCOMPLETE"}
            else ErrorCode.VERSION_CONFLICT
            if reason == "VERSION_CONFLICT"
            else ErrorCode.VALIDATION_ERROR
        )
        raise NexusError(
            code,
            "Proposal Shadow startup was rejected.",
            status_code=409
            if code in (ErrorCode.VERSION_CONFLICT, ErrorCode.PROPOSAL_NOT_EXECUTABLE)
            else 422,
        ) from error
    return OptimizationLifecycleEnvelope(
        data=OptimizationLifecycleData(
            proposal_group_id=result.proposal_group_id,
            status=result.status,
            version=result.version,
        ),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=_utc_now(request)),
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
