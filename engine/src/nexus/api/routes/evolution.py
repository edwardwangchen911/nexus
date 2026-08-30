"""Authenticated, fail-closed strategy-evolution read and control routes."""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Depends, Header, Query, Request, Response
from fastapi.responses import JSONResponse
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import get_settings, require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.schemas.evolution import (
    EvolutionActionEnvelope,
    EvolutionActionResponse,
    EvolutionSuccessMeta,
    ExperimentDetailEnvelope,
    RegistryControlRequest,
    RollbackRequest,
    StartExperimentRequest,
    StrategyEvolutionEnvelope,
    VetoExperimentRequest,
)
from nexus.api.schemas.integrations import NexusErrorEnvelope
from nexus.audit.canonical import canonical_json
from nexus.backtest.engine import exchange_calendar_content_hash
from nexus.domain.time import require_utc
from nexus.evolution.jobs import (
    build_strategy_rnd_job_payload,
    enqueue_strategy_rnd_job,
    strategy_generation_eligibility,
)
from nexus.evolution.read_service import EvolutionReadError, EvolutionReadService
from nexus.evolution.registry import (
    RegistryServiceError,
    RegistryTransitionResultV1,
    pause_auto_promotion,
    request_registry_rollback,
    resume_auto_promotion,
    veto_strategy_experiment,
)
from nexus.persistence.idempotency import IdempotencyClaimError, claim_idempotency
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.evolution import StrategyRegistryHeadRecord
from nexus.persistence.models.runtime import IdempotencyRecord
from nexus.settings import EngineSettings
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()

_IdempotencyKey = Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)]
_IfMatch = Annotated[str, Header(alias="If-Match", min_length=7, max_length=64)]
_ETAG = re.compile(r'^"g([1-9][0-9]*)-v([1-9][0-9]*)"$')
_ERROR_RESPONSES = {
    status: {"model": NexusErrorEnvelope, "description": "Stable NEXUS error envelope."}
    for status in (401, 404, 409, 422, 503)
}
_ETAG_RESPONSE = {"headers": {"ETag": {"schema": {"type": "string"}}}}
_ASYNC_RESPONSE = {
    "headers": {
        "ETag": {"schema": {"type": "string"}},
        "Location": {"schema": {"type": "string"}},
    }
}


@router.get(
    "/internal/v1/strategy-evolution",
    operation_id="getStrategyEvolution",
    response_model=StrategyEvolutionEnvelope,
    responses={status: _ERROR_RESPONSES[status] for status in (401, 422, 503)},
)
async def get_strategy_evolution(
    request: Request,
    response: Response,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
    cursor: Annotated[str | None, Query(min_length=1, max_length=2048)] = None,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    settings: EngineSettings = Depends(get_settings),
    now: datetime = Depends(lambda: datetime.now(UTC)),
) -> StrategyEvolutionEnvelope:
    now = _utc_now(request, now)
    service = _read_service(session, now=now, settings=settings)
    try:
        data = await service.overview(limit=limit, cursor=cursor)
    except EvolutionReadError as error:
        raise _read_error(error) from error
    response.headers["ETag"] = registry_etag(
        data.champion.generation, data.champion.registry_version
    )
    return StrategyEvolutionEnvelope(data=data, meta=_meta(actor, now))


@router.get(
    "/internal/v1/strategy-experiments/{experiment_id}",
    operation_id="getStrategyExperiment",
    response_model=ExperimentDetailEnvelope,
    responses={status: _ERROR_RESPONSES[status] for status in (401, 404, 422, 503)},
)
async def get_strategy_experiment(
    experiment_id: UUID,
    request: Request,
    response: Response,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    settings: EngineSettings = Depends(get_settings),
    now: datetime = Depends(lambda: datetime.now(UTC)),
) -> ExperimentDetailEnvelope:
    now = _utc_now(request, now)
    service = _read_service(session, now=now, settings=settings)
    try:
        data = await service.detail(experiment_id)
        head = await service.require_registry_chain_integrity()
    except EvolutionReadError as error:
        raise _read_error(error) from error
    response.headers["ETag"] = registry_etag(head.active_generation, head.version)
    return ExperimentDetailEnvelope(data=data, meta=_meta(actor, now))


@router.post(
    "/internal/v1/strategy-experiments",
    operation_id="startStrategyExperiment",
    response_model=EvolutionActionEnvelope,
    status_code=202,
    responses={
        202: _ASYNC_RESPONSE,
        **{status: _ERROR_RESPONSES[status] for status in (401, 409, 422, 503)},
    },
)
async def start_strategy_experiment(
    http_request: Request,
    response: Response,
    idempotency_key: _IdempotencyKey,
    if_match: _IfMatch,
    _: StartExperimentRequest = Body(default_factory=StartExperimentRequest),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    settings: EngineSettings = Depends(get_settings),
    now: datetime = Depends(lambda: datetime.now(UTC)),
) -> EvolutionActionEnvelope | JSONResponse:
    now = _utc_now(http_request, now)
    route = "/internal/v1/strategy-experiments"
    service = _read_service(session, now=now, settings=settings)
    head = await _verified_head(service)
    replay = await _claim_action(
        session,
        actor=actor,
        route=route,
        key=idempotency_key,
        raw_body=await http_request.body(),
        if_match=if_match,
    )
    if replay is not None:
        return _apply_replay(replay, response)
    _require_etag(if_match, head.active_generation, head.version)
    eligibility = await strategy_generation_eligibility(session, now=now)
    if not eligibility.eligible:
        raise _generation_error(eligibility.reason_code)
    payload = build_strategy_rnd_job_payload(
        operation="generate_family",
        family_id=None,
        experiment_id=None,
        expected_champion_generation=head.active_generation,
    )
    job = await enqueue_strategy_rnd_job(session, payload=payload, effective_at=now)
    envelope = EvolutionActionEnvelope(
        data=EvolutionActionResponse(
            action="generate_family",
            status="QUEUED",
            reason_code="STRATEGY_EXPERIMENT_QUEUED",
            generation=head.active_generation,
            registry_version=head.version,
            resource_id=job.id,
            pending_effective_session=None,
        ),
        meta=_meta(actor, now),
    )
    location = "/internal/v1/strategy-evolution"
    await _complete_action(session, envelope=envelope, status_code=202, location=location)
    response.headers["Location"] = location
    response.headers["ETag"] = registry_etag(head.active_generation, head.version)
    return envelope


@router.post(
    "/internal/v1/strategy-experiments/{experiment_id}/veto",
    operation_id="vetoStrategyExperiment",
    response_model=EvolutionActionEnvelope,
    responses={
        200: _ETAG_RESPONSE,
        **{status: _ERROR_RESPONSES[status] for status in (401, 404, 409, 422, 503)},
    },
)
async def veto_experiment(
    experiment_id: UUID,
    body: VetoExperimentRequest,
    http_request: Request,
    response: Response,
    idempotency_key: _IdempotencyKey,
    if_match: _IfMatch,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    settings: EngineSettings = Depends(get_settings),
    now: datetime = Depends(lambda: datetime.now(UTC)),
) -> EvolutionActionEnvelope | JSONResponse:
    now = _utc_now(http_request, now)
    route = f"/internal/v1/strategy-experiments/{experiment_id}/veto"
    service = _read_service(session, now=now, settings=settings)
    head = await _verified_head(service)
    replay = await _claim_action(
        session,
        actor=actor,
        route=route,
        key=idempotency_key,
        raw_body=await http_request.body(),
        if_match=if_match,
    )
    if replay is not None:
        return _apply_replay(replay, response)
    _require_etag(if_match, head.active_generation, head.version)
    try:
        result = await veto_strategy_experiment(
            session,
            experiment_id=experiment_id,
            expected_generation=head.active_generation,
            expected_version=head.version,
            idempotency_key=_domain_key(actor.actor_id, route, idempotency_key),
            reason_code=body.reason,
            now=now,
        )
    except RegistryServiceError as error:
        raise _registry_error(error) from error
    envelope = _transition_envelope(
        actor,
        now,
        result,
        action="veto_experiment",
        status="VETOED",
    )
    await _complete_action(session, envelope=envelope, status_code=200, location=None)
    response.headers["ETag"] = registry_etag(result.active_generation, result.version)
    return envelope


@router.post(
    "/internal/v1/strategy-registry/control",
    operation_id="controlStrategyRegistry",
    response_model=EvolutionActionEnvelope,
    responses={
        200: _ETAG_RESPONSE,
        **{status: _ERROR_RESPONSES[status] for status in (401, 409, 422, 503)},
    },
)
async def control_strategy_registry(
    body: RegistryControlRequest,
    http_request: Request,
    response: Response,
    idempotency_key: _IdempotencyKey,
    if_match: _IfMatch,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    settings: EngineSettings = Depends(get_settings),
    now: datetime = Depends(lambda: datetime.now(UTC)),
) -> EvolutionActionEnvelope | JSONResponse:
    now = _utc_now(http_request, now)
    route = "/internal/v1/strategy-registry/control"
    service = _read_service(session, now=now, settings=settings)
    head = await _verified_head(service)
    replay = await _claim_action(
        session,
        actor=actor,
        route=route,
        key=idempotency_key,
        raw_body=await http_request.body(),
        if_match=if_match,
    )
    if replay is not None:
        return _apply_replay(replay, response)
    _require_etag(if_match, head.active_generation, head.version)
    command = (
        pause_auto_promotion if body.action == "pause_auto_promotion" else resume_auto_promotion
    )
    try:
        result = await command(
            session,
            expected_generation=head.active_generation,
            expected_version=head.version,
            idempotency_key=_domain_key(actor.actor_id, route, idempotency_key),
            now=now,
        )
    except RegistryServiceError as error:
        raise _registry_error(error) from error
    action = body.action
    status = "PAUSED" if action == "pause_auto_promotion" else "EFFECTIVE"
    envelope = _transition_envelope(actor, now, result, action=action, status=status)
    await _complete_action(session, envelope=envelope, status_code=200, location=None)
    response.headers["ETag"] = registry_etag(result.active_generation, result.version)
    return envelope


@router.post(
    "/internal/v1/strategy-registry/rollback",
    operation_id="rollbackStrategyRegistry",
    response_model=EvolutionActionEnvelope,
    status_code=202,
    responses={
        202: _ASYNC_RESPONSE,
        **{status: _ERROR_RESPONSES[status] for status in (401, 409, 422, 503)},
    },
)
async def rollback_strategy_registry(
    body: RollbackRequest,
    http_request: Request,
    response: Response,
    idempotency_key: _IdempotencyKey,
    if_match: _IfMatch,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    settings: EngineSettings = Depends(get_settings),
    now: datetime = Depends(lambda: datetime.now(UTC)),
) -> EvolutionActionEnvelope | JSONResponse:
    now = _utc_now(http_request, now)
    route = "/internal/v1/strategy-registry/rollback"
    service = _read_service(session, now=now, settings=settings)
    head = await _verified_head(service)
    replay = await _claim_action(
        session,
        actor=actor,
        route=route,
        key=idempotency_key,
        raw_body=await http_request.body(),
        if_match=if_match,
    )
    if replay is not None:
        return _apply_replay(replay, response)
    _require_etag(if_match, head.active_generation, head.version)
    effective_session = await _next_registry_session(session, now=now)
    try:
        result = await request_registry_rollback(
            session,
            expected_generation=head.active_generation,
            expected_version=head.version,
            effective_session=effective_session,
            idempotency_key=_domain_key(actor.actor_id, route, idempotency_key),
            reason_code=body.reason,
            now=now,
        )
    except RegistryServiceError as error:
        raise _registry_error(error) from error
    if result.status == "FAILED":
        rollback_error = NexusError(
            ErrorCode.EVOLUTION_BLOCKED,
            "The strategy registry rollback could not be scheduled.",
            status_code=422,
            details={"reason_code": result.reason_code},
        )
        payload = rollback_error.to_envelope(actor.request_id)
        await _complete_error(session, payload=payload, status_code=422)
        return JSONResponse(status_code=422, content=payload)
    envelope = _transition_envelope(
        actor,
        now,
        result,
        action="rollback_registry",
        status="PENDING_BOUNDARY",
    )
    location = "/internal/v1/strategy-evolution"
    await _complete_action(session, envelope=envelope, status_code=202, location=location)
    response.headers["Location"] = location
    response.headers["ETag"] = registry_etag(result.active_generation, result.version)
    return envelope


def parse_registry_etag(value: str) -> tuple[int, int]:
    match = _ETAG.fullmatch(value)
    if match is None:
        raise ValueError("registry ETag is invalid")
    return int(match.group(1)), int(match.group(2))


def registry_etag(generation: int, version: int) -> str:
    if generation < 1 or version < 1:
        raise ValueError("registry generation and version must be positive")
    return f'"g{generation}-v{version}"'


def _require_etag(value: str, generation: int, version: int) -> None:
    try:
        supplied = parse_registry_etag(value)
    except ValueError:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR,
            "If-Match must contain the strategy registry generation and version.",
            status_code=422,
        ) from None
    if supplied != (generation, version):
        raise NexusError(
            ErrorCode.VERSION_CONFLICT,
            "The strategy registry generation or version changed.",
            status_code=409,
            details={"reason_code": "REGISTRY_GENERATION_CONFLICT"},
        )


def _read_service(
    session: AsyncSession, *, now: datetime, settings: EngineSettings
) -> EvolutionReadService:
    secrets = tuple(
        secret.get_secret_value()
        for secret in (
            settings.engine_hmac_current_secret,
            settings.engine_hmac_previous_secret,
        )
        if secret is not None and secret.get_secret_value()
    )
    return EvolutionReadService(session, now=now, cursor_secrets=secrets)


async def _verified_head(service: EvolutionReadService) -> StrategyRegistryHeadRecord:
    try:
        return await service.require_registry_integrity()
    except EvolutionReadError as error:
        raise _read_error(error) from error


def _utc_now(request: Request, fallback: datetime) -> datetime:
    clock = getattr(request.app.state, "utc_clock", None)
    return require_utc(clock() if callable(clock) else fallback)


def _meta(actor: VerifiedServiceActor, now: datetime) -> EvolutionSuccessMeta:
    return EvolutionSuccessMeta(request_id=actor.request_id, as_of=now)


@dataclass(frozen=True)
class _ActionReplay:
    envelope: EvolutionActionEnvelope | None
    error_payload: dict[str, object] | None
    status_code: int
    location: str | None


async def _claim_action(
    session: AsyncSession,
    *,
    actor: VerifiedServiceActor,
    route: str,
    key: str,
    raw_body: bytes,
    if_match: str,
) -> _ActionReplay | None:
    request_hash = hashlib.sha256(raw_body + b"\n" + if_match.encode()).hexdigest()
    try:
        claim, created = await claim_idempotency(
            session,
            actor_id=actor.actor_id,
            route=route,
            idempotency_key=key,
            request_hash=request_hash,
            schema_version="strategy_evolution_action_v1",
        )
    except IdempotencyClaimError as error:
        raise NexusError(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "The idempotency key was already used for a different request.",
            status_code=409,
        ) from error
    if created:
        session.info["strategy_evolution_action_claim"] = claim
        return None
    payload = claim.response_payload
    if not payload:
        raise NexusError(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "The prior strategy evolution action has no completed result.",
            status_code=409,
        )
    try:
        raw_status = payload["http_status"]
        if not isinstance(raw_status, int):
            raise ValueError
        status_code = raw_status
        location = payload.get("location")
        if location is not None and not isinstance(location, str):
            raise ValueError
        if payload.get("kind") == "success":
            envelope = EvolutionActionEnvelope.model_validate(payload["body"])
            return _ActionReplay(envelope, None, status_code, location)
        error_body = payload.get("body")
        if payload.get("kind") == "error" and isinstance(error_body, dict):
            return _ActionReplay(None, error_body, status_code, location)
    except (KeyError, TypeError, ValueError):
        pass
    raise NexusError(
        ErrorCode.IDEMPOTENCY_CONFLICT,
        "The prior strategy evolution action result is invalid.",
        status_code=409,
    )


async def _complete_action(
    session: AsyncSession,
    *,
    envelope: EvolutionActionEnvelope,
    status_code: int,
    location: str | None,
) -> None:
    claim = session.info.pop("strategy_evolution_action_claim", None)
    if not isinstance(claim, IdempotencyRecord):
        raise RuntimeError("strategy evolution idempotency claim is unavailable")
    claim.response_payload = {
        "kind": "success",
        "http_status": status_code,
        "location": location,
        "body": envelope.model_dump(mode="json"),
    }
    await session.flush()


async def _complete_error(
    session: AsyncSession, *, payload: Mapping[str, object], status_code: int
) -> None:
    claim = session.info.pop("strategy_evolution_action_claim", None)
    if not isinstance(claim, IdempotencyRecord):
        raise RuntimeError("strategy evolution idempotency claim is unavailable")
    claim.response_payload = {
        "kind": "error",
        "http_status": status_code,
        "location": None,
        "body": dict(payload),
    }
    await session.flush()


def _apply_replay(
    replay: _ActionReplay, response: Response
) -> EvolutionActionEnvelope | JSONResponse:
    if replay.location is not None:
        response.headers["Location"] = replay.location
    if replay.envelope is not None:
        response.status_code = replay.status_code
        response.headers["ETag"] = registry_etag(
            replay.envelope.data.generation,
            replay.envelope.data.registry_version,
        )
        return replay.envelope
    return JSONResponse(status_code=replay.status_code, content=replay.error_payload)


def _transition_envelope(
    actor: VerifiedServiceActor,
    now: datetime,
    result: RegistryTransitionResultV1,
    *,
    action: str,
    status: str,
) -> EvolutionActionEnvelope:
    return EvolutionActionEnvelope.model_validate(
        {
            "data": {
                "action": action,
                "status": status,
                "reason_code": result.reason_code,
                "generation": result.active_generation,
                "registry_version": result.version,
                "resource_id": result.registry_event_id,
                "pending_effective_session": result.pending_effective_session,
                "live_execution_enabled": False,
            },
            "meta": _meta(actor, now).model_dump(mode="json"),
        }
    )


async def _next_registry_session(session: AsyncSession, *, now: datetime) -> date:
    authority = await session.scalar(
        select(ExchangeCalendarAuthorityRecord)
        .where(
            ExchangeCalendarAuthorityRecord.exchange == "XNYS",
            ExchangeCalendarAuthorityRecord.completeness_status == "complete",
            ExchangeCalendarAuthorityRecord.published_at <= now,
        )
        .order_by(
            ExchangeCalendarAuthorityRecord.published_at.desc(),
            ExchangeCalendarAuthorityRecord.id.desc(),
        )
        .limit(1)
    )
    rows = (
        ()
        if authority is None
        else tuple(
            await session.scalars(
                select(ExchangeCalendarSessionRecord)
                .where(ExchangeCalendarSessionRecord.calendar_authority_id == authority.id)
                .order_by(ExchangeCalendarSessionRecord.session)
            )
        )
    )
    authority_valid = (
        authority is not None
        and bool(rows)
        and rows[0].session == authority.range_start
        and rows[-1].session == authority.range_end
        and exchange_calendar_content_hash(
            tuple(item.session for item in rows),
            {item.session: item.decision_at for item in rows},
        )
        == authority.content_hash
    )
    next_session = next(
        (item.session for item in rows if item.decision_at > now),
        None,
    )
    if not authority_valid or next_session is None:
        raise NexusError(
            ErrorCode.AUTHORITY_UNAVAILABLE,
            "An authoritative future XNYS session is unavailable.",
            status_code=503,
            details={"reason_code": "HANDOVER_AUTHORITY_INCOMPLETE"},
        )
    return next_session


def _domain_key(actor_id: str, route: str, key: str) -> str:
    digest = hashlib.sha256(canonical_json([actor_id, route, key]).encode()).hexdigest()
    return f"strategy-api-{digest[:52]}"


def _generation_error(reason: str) -> NexusError:
    unavailable = {
        "STRATEGY_REGISTRY_INCOMPLETE",
        "LINEAGE_AUTHORITY_INCOMPLETE",
        "EVIDENCE_INCOMPLETE",
        "BACKTEST_ADJUSTED_AUTHORITY_UNAVAILABLE",
    }
    return NexusError(
        ErrorCode.AUTHORITY_UNAVAILABLE if reason in unavailable else ErrorCode.EVOLUTION_BLOCKED,
        "The strategy experiment cannot be queued.",
        status_code=503 if reason in unavailable else 422,
        details={"reason_code": reason},
    )


def _registry_error(error: RegistryServiceError) -> NexusError:
    reason = error.reason_code
    if reason == "EXPERIMENT_NOT_FOUND":
        return NexusError(
            ErrorCode.NOT_FOUND,
            "The strategy experiment was not found.",
            status_code=404,
        )
    if reason == "STRATEGY_REGISTRY_CONFLICT":
        return NexusError(
            ErrorCode.VERSION_CONFLICT,
            "The strategy registry generation or version changed.",
            status_code=409,
            details={"reason_code": "REGISTRY_GENERATION_CONFLICT"},
        )
    if reason in {"REGISTRY_IDEMPOTENCY_CONFLICT"}:
        return NexusError(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "The idempotency key conflicts with an existing strategy registry event.",
            status_code=409,
        )
    if reason in {
        "STRATEGY_REGISTRY_INCOMPLETE",
        "STRATEGY_REGISTRY_CHAIN_INVALID",
        "STRATEGY_EXPERIMENT_CHAIN_INVALID",
        "PROMOTION_AUTHORITY_INCOMPLETE",
        "HANDOVER_AUTHORITY_INVALID",
    }:
        return NexusError(
            ErrorCode.AUTHORITY_UNAVAILABLE,
            "The strategy registry authority is unavailable.",
            status_code=503,
            details={"reason_code": reason},
        )
    return NexusError(
        ErrorCode.EVOLUTION_BLOCKED,
        "The strategy evolution action is not allowed in the current state.",
        status_code=422,
        details={"reason_code": reason},
    )


def _read_error(error: EvolutionReadError) -> NexusError:
    if error.reason_code == "EXPERIMENT_NOT_FOUND":
        return NexusError(
            ErrorCode.NOT_FOUND,
            "The strategy experiment was not found.",
            status_code=404,
        )
    if error.reason_code == "CURSOR_INVALID":
        return NexusError(
            ErrorCode.VALIDATION_ERROR,
            "The strategy evolution cursor is invalid.",
            status_code=422,
        )
    return NexusError(
        ErrorCode.AUTHORITY_UNAVAILABLE,
        "The strategy evolution authority is unavailable.",
        status_code=503,
        details={"reason_code": error.reason_code},
    )
