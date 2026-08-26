"""Authenticated redacted risk-audit reads and narrowly scoped recovery actions."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Body, Depends, Header, Query, Response
from fastapi.responses import Response as BinaryResponse
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import get_settings, require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.routes.paper import _cursor_secrets
from nexus.api.routes.portfolios import _utc_now
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.api.schemas.risk_audit import (
    DrawdownAcknowledgeEnvelope,
    DrawdownAcknowledgeRequest,
    EmptyActionRequest,
    JobRetryEnvelope,
    RiskAuditPageEnvelope,
)
from nexus.audit.export import AuditExportLimitError, export_audit_ndjson_gzip
from nexus.audit.read_service import RiskAuditReadService
from nexus.audit.redaction import AuditKind
from nexus.jobs.manual_retry import ManualRetryError, retry_manual_safe_job
from nexus.risk.drawdown_ack import (
    DrawdownAcknowledgeError,
    acknowledge_drawdown_lock,
)
from nexus.settings import EngineSettings
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()
_ERROR_RESPONSES: dict[int | str, dict[str, Any]] = {
    status: {"model": NexusErrorEnvelope, "description": "Stable NEXUS error envelope."}
    for status in (401, 403, 404, 409, 422, 503)
}
_IdempotencyKey = Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)]
_IfMatch = Annotated[str, Header(alias="If-Match", min_length=3, max_length=64)]


@router.get(
    "/internal/v1/risk-audit",
    operation_id="listRiskAuditEvents",
    response_model=RiskAuditPageEnvelope,
    responses=_ERROR_RESPONSES,
)
async def list_risk_audit(
    kind: AuditKind | None = None,
    from_at: Annotated[datetime | None, Query(alias="from")] = None,
    to_at: Annotated[datetime | None, Query(alias="to")] = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
    cursor: Annotated[str | None, Query(min_length=1, max_length=2048)] = None,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    settings: EngineSettings = Depends(get_settings),
    now: datetime = Depends(_utc_now),
) -> RiskAuditPageEnvelope:
    try:
        page = await RiskAuditReadService(
            session,
            now,
            cursor_signing_secrets=_cursor_secrets(settings),
        ).page(
            kind=kind,
            from_at=from_at,
            to_at=to_at,
            cursor=cursor,
            limit=limit,
        )
    except ValueError as error:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR,
            "The request could not be completed.",
            status_code=422,
        ) from error
    return RiskAuditPageEnvelope(
        data=page,
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )


@router.get(
    "/internal/v1/risk-audit/export",
    operation_id="exportRiskAuditEvents",
    response_class=BinaryResponse,
    responses={
        200: {
            "description": "Deterministic redacted gzip NDJSON export.",
            "content": {
                "application/x-ndjson": {
                    "schema": {"type": "string", "format": "binary"}
                }
            },
        },
        **_ERROR_RESPONSES,
    },
)
async def export_risk_audit(
    from_at: Annotated[datetime, Query(alias="from")],
    to_at: Annotated[datetime, Query(alias="to")],
    kind: AuditKind | None = None,
    _: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> BinaryResponse:
    try:
        content = await export_audit_ndjson_gzip(session, from_at=from_at, to_at=to_at, kind=kind)
    except (ValueError, AuditExportLimitError) as error:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR,
            "The audit export request is outside the allowed bounds.",
            status_code=422,
        ) from error
    return BinaryResponse(
        content=content,
        media_type="application/x-ndjson",
        headers={
            "Content-Encoding": "gzip",
            "Content-Disposition": 'attachment; filename="nexus-risk-audit.ndjson.gz"',
            "Cache-Control": "no-store",
        },
    )


@router.post(
    "/internal/v1/jobs/{job_id}/retry",
    operation_id="retryManualSafeJob",
    response_model=JobRetryEnvelope,
    responses=_ERROR_RESPONSES,
)
async def retry_job(
    job_id: UUID,
    response: Response,
    idempotency_key: _IdempotencyKey,
    if_match: _IfMatch,
    _: EmptyActionRequest = Body(default_factory=EmptyActionRequest),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    now: datetime = Depends(_utc_now),
) -> JobRetryEnvelope:
    expected_version = _parse_if_match(if_match)
    try:
        result = await retry_manual_safe_job(
            session,
            job_id=job_id,
            actor_id=actor.actor_id,
            expected_version=expected_version,
            idempotency_key=idempotency_key,
            now=now,
        )
    except ManualRetryError as error:
        raise _job_retry_error(str(error)) from error
    response.headers["ETag"] = f'"{result.version}"'
    return JobRetryEnvelope(
        data=result,
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )


@router.post(
    "/internal/v1/risk/drawdown-lock/acknowledge",
    operation_id="acknowledgeDrawdownLock",
    response_model=DrawdownAcknowledgeEnvelope,
    responses=_ERROR_RESPONSES,
)
async def acknowledge_drawdown(
    response: Response,
    body: DrawdownAcknowledgeRequest,
    idempotency_key: _IdempotencyKey,
    if_match: _IfMatch,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    now: datetime = Depends(_utc_now),
) -> DrawdownAcknowledgeEnvelope:
    if _parse_if_match(if_match) != body.expected_event_seq:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR,
            "If-Match must equal the expected event sequence.",
            status_code=422,
        )
    try:
        result = await acknowledge_drawdown_lock(
            session,
            actor_id=actor.actor_id,
            expected_event_seq=body.expected_event_seq,
            idempotency_key=idempotency_key,
            now=now,
        )
    except DrawdownAcknowledgeError as error:
        raise _drawdown_error(str(error)) from error
    response.headers["ETag"] = f'"{result.event_seq}"'
    return DrawdownAcknowledgeEnvelope(
        data=result,
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )


def _parse_if_match(value: str) -> int:
    if len(value) < 3 or value[0] != '"' or value[-1] != '"':
        raise NexusError(
            ErrorCode.VALIDATION_ERROR,
            "If-Match must contain one quoted version.",
            status_code=422,
        )
    try:
        parsed = int(value[1:-1])
    except ValueError:
        parsed = 0
    if parsed < 1:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR,
            "If-Match must contain one quoted version.",
            status_code=422,
        )
    return parsed


def _job_retry_error(reason: str) -> NexusError:
    if reason == "NOT_FOUND":
        return NexusError(ErrorCode.NOT_FOUND, "The Job was not found.", status_code=404)
    if reason == "VERSION_CONFLICT":
        return NexusError(ErrorCode.VERSION_CONFLICT, "The Job version changed.", status_code=409)
    if reason.startswith("IDEMPOTENCY_"):
        return NexusError(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "The idempotency key cannot be replayed.",
            status_code=409,
        )
    if reason == "RETRY_NOT_PERMITTED":
        return NexusError(
            ErrorCode.FORBIDDEN, "The Job is not approved for manual retry.", status_code=403
        )
    return NexusError(
        ErrorCode.PROPOSAL_NOT_EXECUTABLE,
        "The Job cannot be retried in its current state.",
        status_code=409,
        details={"reason": reason},
    )


def _drawdown_error(reason: str) -> NexusError:
    if reason in {"VERSION_CONFLICT", "EVENT_TIME_CONFLICT"}:
        return NexusError(
            ErrorCode.VERSION_CONFLICT,
            "The drawdown state changed.",
            status_code=409,
        )
    if reason.startswith("IDEMPOTENCY_"):
        return NexusError(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "The idempotency key cannot be replayed.",
            status_code=409,
        )
    if reason in {
        "INTERNAL_ACCOUNT_AMBIGUOUS",
        "DRAWDOWN_STATE_UNKNOWN",
        "DRAWDOWN_STATE_INVALID",
        "PERFORMANCE_UNKNOWN",
    }:
        return NexusError(
            ErrorCode.DATA_PARTIAL,
            "The drawdown authority is incomplete.",
            status_code=409,
            details={"reason": reason},
        )
    return NexusError(
        ErrorCode.DRAWDOWN_LOCKED,
        "The drawdown lock is not eligible for acknowledgement.",
        status_code=409,
        details={"reason": reason},
    )
