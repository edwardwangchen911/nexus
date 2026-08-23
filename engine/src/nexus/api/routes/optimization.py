# ruff: noqa: E501
"""Narrow, non-executable optimization APIs."""

import hashlib
import json
from decimal import Decimal
from typing import Any
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.schemas.integrations import NexusErrorEnvelope
from nexus.domain.decimal import DecimalString
from nexus.optimizer.admission import admit_queued_job
from nexus.optimizer.authority import load_admission_authority
from nexus.optimizer.lifecycle import transition_group_idempotently
from nexus.persistence.models.optimization import (
    OptimizationProposalGroupRecord,
    OptimizationProposalRecord,
    ProposalComparisonRecord,
    ProposalPositionRecord,
)
from nexus.persistence.models.runtime import AuditEvent
from nexus.reconciliation.persistence_errors import IdempotencyConflict
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()
_MAX_WHAT_IF_BYTES = 64 * 1024
_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    status: {"model": NexusErrorEnvelope, "description": "Stable NEXUS error envelope."}
    for status in (401, 404, 409, 422)
}


class _WhatIfPosition(BaseModel):
    model_config = ConfigDict(extra="forbid")
    instrument_id: UUID
    weight: DecimalString

    @field_validator("weight")
    @classmethod
    def _weight_range(cls, value: str) -> str:
        if not Decimal(value).is_finite() or Decimal(value) < 0 or Decimal(value) > 1:
            raise ValueError("weight must be within zero and one")
        return value


class _WhatIfRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    policy_version: str = Field(pattern=r"^optimizer_v1$")
    positions: tuple[_WhatIfPosition, ...] = Field(max_length=30)

    @field_validator("positions")
    @classmethod
    def _unique_instruments(cls, value: tuple[_WhatIfPosition, ...]) -> tuple[_WhatIfPosition, ...]:
        if len({position.instrument_id for position in value}) != len(value):
            raise ValueError("instrument ids must be unique")
        return value


class _ProposalAdmissionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    portfolio_snapshot_id: UUID
    data_snapshot_id: UUID
    cost_policy_id: UUID


class _LifecycleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: int = Field(ge=0)


@router.post("/internal/v1/what-if/evaluate", responses=_ERROR_RESPONSES)
async def evaluate_what_if(
    request: Request,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> dict[str, object]:
    """Return deterministic analysis and append only a non-sensitive audit summary."""
    raw_body = await request.body()
    if len(raw_body) > _MAX_WHAT_IF_BYTES:
        raise NexusError(ErrorCode.VALIDATION_ERROR, "Request is too large.", status_code=422)
    try:
        payload = _WhatIfRequest.model_validate(json.loads(raw_body))
    except (json.JSONDecodeError, ValueError) as error:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR, "Request is invalid.", status_code=422
        ) from error
    request_hash = hashlib.sha256(raw_body).hexdigest()
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
    return {
        "data": {
            "policy_version": payload.policy_version,
            "instrument_count": len(payload.positions),
        },
        "meta": {"request_id": str(actor.request_id), "schema_version": "1"},
    }


@router.post("/internal/v1/optimization-proposals", responses=_ERROR_RESPONSES)
async def admit_optimization_proposal(
    request: Request,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> object:
    """Fail closed until one persisted authority binds baseline, diagnosis, data, source, and cost."""
    route = "/internal/v1/optimization-proposals"
    raw_body = await request.body()
    try:
        body = _ProposalAdmissionRequest.model_validate(json.loads(raw_body))
    except (json.JSONDecodeError, ValueError) as error:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR, "Request is invalid.", status_code=422
        ) from error
    request_hash = hashlib.sha256(raw_body).hexdigest()
    idempotency_key = request.headers.get("Idempotency-Key")
    if not idempotency_key:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR, "Idempotency-Key is required.", status_code=422
        )
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
            status_code=422,
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
    payload = {
        "data": {"job_id": str(job.job_id), "status": "QUEUED"},
        "meta": {"request_id": str(actor.request_id), "schema_version": "1"},
    }
    return JSONResponse(status_code=202, content=payload)


@router.get("/internal/v1/optimization-proposals/{group_id}", responses=_ERROR_RESPONSES)
async def get_optimization_group(
    group_id: str,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> dict[str, object]:
    """Return immutable children and the sole mutable lifecycle state."""
    proposal_group_id = _parse_group_id(group_id)
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
        raise NexusError(ErrorCode.DATA_PARTIAL, "Proposal group is incomplete.", status_code=422)
    proposal_data: list[dict[str, object]] = []
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
                ErrorCode.DATA_PARTIAL, "Proposal comparison is incomplete.", status_code=422
            )
        proposal_data.append(
            {
                "proposal_id": str(child.id),
                "variant": child.variant,
                "analysis_only": child.analysis_only,
                "strategy_evidence_complete": child.strategy_evidence_complete,
                "portfolio_scope": child.portfolio_scope,
                "positions": [
                    {
                        "instrument_id": str(position.instrument_id),
                        "current_weight": str(position.current_weight),
                        "target_weight": str(position.target_weight),
                        "action": position.action,
                        "reason_code": position.reason_code,
                    }
                    for position in positions
                ],
                "comparison": {
                    "metrics": comparison.metrics,
                    "tax_impact": comparison.tax_impact,
                    "requires_fx": comparison.requires_fx,
                    "non_executable": comparison.non_executable,
                    "cost_complete": comparison.cost_complete,
                },
            }
        )
    return {
        "data": {
            "proposal_group_id": str(group.id),
            "status": group.status,
            "version": group.version,
            "baseline_portfolio_snapshot_id": str(group.baseline_portfolio_snapshot_id),
            "portfolio_scope": group.portfolio_scope,
            "diagnostic_input_hash": group.diagnostic_input_hash,
            "data_snapshot_id": None
            if group.data_snapshot_id is None
            else str(group.data_snapshot_id),
            "source_policy_id": None
            if group.source_policy_id is None
            else str(group.source_policy_id),
            "cost_policy_id": None if group.cost_policy_id is None else str(group.cost_policy_id),
            "proposals": proposal_data,
        },
        "meta": {"request_id": str(actor.request_id), "schema_version": "1"},
    }


async def _transition_endpoint(
    group_id: UUID,
    target: str,
    request: Request,
    actor: VerifiedServiceActor,
    session: AsyncSession,
) -> dict[str, object]:
    raw_body = await request.body()
    idempotency_key = request.headers.get("Idempotency-Key")
    if not idempotency_key:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR, "Idempotency-Key is required.", status_code=422
        )
    try:
        body = _LifecycleRequest.model_validate(json.loads(raw_body))
        group = await transition_group_idempotently(
            session,
            actor_id=actor.actor_id,
            idempotency_key=idempotency_key,
            request_hash=hashlib.sha256(target.encode() + b"\0" + raw_body).hexdigest(),
            group_id=group_id,
            target=target,  # type: ignore[arg-type]
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
            status_code=409 if code == ErrorCode.VERSION_CONFLICT else 422,
        ) from error
    return {
        "data": {
            "proposal_group_id": str(group.group_id),
            "status": group.status,
            "version": group.version,
        }
    }


@router.post("/internal/v1/optimization-proposals/{group_id}/shadow", responses=_ERROR_RESPONSES)
async def shadow_optimization_group(
    group_id: str,
    request: Request,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> dict[str, object]:
    return await _transition_endpoint(
        _parse_group_id(group_id), "shadowing", request, actor, session
    )


@router.post("/internal/v1/optimization-proposals/{group_id}/reject", responses=_ERROR_RESPONSES)
async def reject_optimization_group(
    group_id: str,
    request: Request,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> dict[str, object]:
    return await _transition_endpoint(
        _parse_group_id(group_id), "rejected", request, actor, session
    )


def _parse_group_id(value: str) -> UUID:
    try:
        return UUID(value)
    except ValueError as error:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR, "Proposal group id is invalid.", status_code=422
        ) from error
