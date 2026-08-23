# ruff: noqa: E501
"""Narrow, non-executable optimization APIs."""

import hashlib
import json
from decimal import Decimal
from uuid import UUID

from fastapi import APIRouter, Depends, Request
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.domain.decimal import DecimalString
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.diagnostics import PortfolioDiagnosticReportRecord
from nexus.persistence.models.portfolio import PortfolioSnapshotRecord
from nexus.persistence.models.runtime import AuditEvent
from pydantic import BaseModel, ConfigDict, Field, field_validator
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()
_MAX_WHAT_IF_BYTES = 64 * 1024


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
    cost_model_version: str = Field(min_length=1, max_length=100)


@router.post("/internal/v1/what-if/evaluate")
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


@router.post("/internal/v1/optimization-proposals")
async def admit_optimization_proposal(
    request: Request,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> dict[str, object]:
    """Fail closed until one persisted authority binds baseline, diagnosis, data, source, and cost."""
    del actor
    try:
        body = _ProposalAdmissionRequest.model_validate(json.loads(await request.body()))
    except (json.JSONDecodeError, ValueError) as error:
        raise NexusError(ErrorCode.VALIDATION_ERROR, "Request is invalid.", status_code=422) from error
    snapshot = await session.scalar(
        select(PortfolioSnapshotRecord).where(PortfolioSnapshotRecord.id == body.portfolio_snapshot_id)
    )
    report = await session.scalar(
        select(PortfolioDiagnosticReportRecord).where(
            PortfolioDiagnosticReportRecord.portfolio_snapshot_id == body.portfolio_snapshot_id,
            PortfolioDiagnosticReportRecord.optimization_need == "OPTIMIZATION_CANDIDATE",
        )
    )
    data_snapshot = await session.scalar(
        select(DataSnapshot).where(DataSnapshot.id == body.data_snapshot_id)
    )
    if snapshot is None or snapshot.status != "READY" or report is None or data_snapshot is None:
        raise NexusError(
            ErrorCode.DATA_PARTIAL,
            "Persisted optimization authority is incomplete.",
            status_code=422,
            details={"reason": "OPTIMIZATION_AUTHORITY_MISSING"},
        )
    raise NexusError(
        ErrorCode.DATA_PARTIAL,
        "Persisted source and cost authority is incomplete.",
        status_code=422,
        details={"reason": "OPTIMIZATION_AUTHORITY_MISSING"},
    )
