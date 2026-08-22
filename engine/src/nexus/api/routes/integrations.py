"""Authenticated, deterministic integration status routes."""

import hashlib
import re
from datetime import UTC, datetime
from typing import Annotated, Literal, cast

from fastapi import APIRouter, Body, Depends, Header, Request, Response
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.schemas.integrations import (
    IntegrationConnectivityTestRequest,
    IntegrationConnectivityTestResponse,
    IntegrationMode,
    IntegrationModeChangeRequest,
    IntegrationModeChangeResponse,
    IntegrationStatusListEnvelope,
    IntegrationStatusResponse,
    NexusErrorEnvelope,
    ProviderName,
    SuccessMetaResponse,
)
from nexus.domain.time import require_utc
from nexus.persistence.models.data import ProviderCapability
from nexus.persistence.models.runtime import IdempotencyRecord, IntegrationStatus
from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()

_PROVIDERS: tuple[ProviderName, ...] = ("fosun", "alpaca", "sec", "fred", "openai")
_MODES: frozenset[str] = frozenset({"disabled", "fixture", "read_only"})
_CONTRACT_HASH = re.compile(r"^[0-9a-f]{64}$")
_FIXTURE_AS_OF = datetime(2026, 8, 22, tzinfo=UTC)
_ERROR_RESPONSES = {
    status: {"model": NexusErrorEnvelope, "description": "Stable NEXUS error envelope."}
    for status in (401, 403, 409, 422, 503)
}
_ETAG_RESPONSE = {"headers": {"ETag": {"schema": {"type": "string"}}}}


def _utc_now(request: Request) -> datetime:
    """Return a UTC clock that tests can inject on the application state."""
    clock = getattr(request.app.state, "utc_clock", None)
    return require_utc(clock() if callable(clock) else datetime.now(UTC))


@router.get(
    "/internal/v1/integrations",
    operation_id="listIntegrationStatuses",
    response_model=IntegrationStatusListEnvelope,
    responses={status: _ERROR_RESPONSES[status] for status in (401, 422)},
)
async def list_integration_statuses(
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    now: datetime = Depends(_utc_now),
) -> IntegrationStatusListEnvelope:
    """List baseline status, overridden only by admitted persisted modes."""
    rows = await session.scalars(select(IntegrationStatus))
    persisted = {row.provider: row for row in rows}
    capabilities = await _capabilities_by_provider(session, now)
    return IntegrationStatusListEnvelope(
        data=[
            _to_response(provider, persisted.get(provider), capabilities[provider])
            for provider in _PROVIDERS
        ],
        meta=_success_meta(actor, now),
    )


@router.post(
    "/internal/v1/integrations/{provider}/test",
    operation_id="testIntegrationConnectivity",
    response_model=IntegrationConnectivityTestResponse,
    responses={status: _ERROR_RESPONSES[status] for status in (401, 403, 422, 503)},
)
async def test_integration_connectivity(
    provider: ProviderName,
    _: IntegrationConnectivityTestRequest = Body(
        default_factory=IntegrationConnectivityTestRequest
    ),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    now: datetime = Depends(_utc_now),
) -> IntegrationConnectivityTestResponse:
    """Refresh only the deterministic Fosun fixture; never contact a provider."""
    persisted = await _find_status(session, provider)
    capabilities = await _capability_projection(session, provider, now)
    status = _to_response(provider, persisted, capabilities)
    if status.mode == "disabled":
        raise NexusError(
            ErrorCode.PROVIDER_UNAVAILABLE, "The provider is unavailable.", status_code=503
        )
    if status.mode != "fixture":
        raise NexusError(ErrorCode.FORBIDDEN, "The operation is not permitted.", status_code=403)
    await _refresh_fixture(session, provider, now)
    return IntegrationConnectivityTestResponse(
        data=_to_response(provider, await _find_status(session, provider), capabilities),
        meta=_success_meta(actor, now),
    )


@router.post(
    "/internal/v1/integrations/{provider}/mode",
    operation_id="changeIntegrationMode",
    response_model=IntegrationModeChangeResponse,
    responses={
        200: _ETAG_RESPONSE,
        **{status: _ERROR_RESPONSES[status] for status in (401, 403, 409, 422, 503)},
    },
)
async def change_integration_mode(
    provider: ProviderName,
    request: IntegrationModeChangeRequest,
    http_request: Request,
    response: Response,
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=255)
    ],
    if_match: Annotated[str, Header(alias="If-Match", min_length=1, max_length=64)],
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    now: datetime = Depends(_utc_now),
) -> IntegrationModeChangeResponse:
    """Version and idempotency protected mode mutation without write capability."""
    route = f"/internal/v1/integrations/{provider}/mode"
    request_hash = hashlib.sha256(await http_request.body()).hexdigest()
    replay = await _claim_or_replay(
        session,
        actor=actor,
        route=route,
        idempotency_key=idempotency_key,
        request_hash=request_hash,
    )
    if replay is not None:
        response.headers["ETag"] = _etag(replay.data.version)
        return replay
    expected_version = _parse_if_match(if_match)
    capabilities = await _capability_projection(session, provider, now)
    _require_allowed_transition(provider, request.mode, capabilities.valid)
    status = await _write_mode(session, provider, request.mode, expected_version)
    result = IntegrationModeChangeResponse(
        data=_to_response(provider, status, capabilities), meta=_success_meta(actor, now)
    )
    await session.execute(
        update(IdempotencyRecord)
        .where(
            IdempotencyRecord.actor_id == actor.actor_id,
            IdempotencyRecord.route == route,
            IdempotencyRecord.idempotency_key == idempotency_key,
        )
        .values(response_payload=result.model_dump(mode="json"))
    )
    response.headers["ETag"] = _etag(result.data.version)
    return result


async def _find_status(session: AsyncSession, provider: ProviderName) -> IntegrationStatus | None:
    result = await session.scalars(
        select(IntegrationStatus)
        .where(IntegrationStatus.provider == provider)
        .execution_options(populate_existing=True)
    )
    return result.one_or_none()


class _CapabilityProjection:
    def __init__(self, valid: ProviderCapability | None, latest: ProviderCapability | None) -> None:
        self.valid = valid
        self.latest = latest


async def _capabilities_by_provider(
    session: AsyncSession, now: datetime
) -> dict[ProviderName, _CapabilityProjection]:
    return {
        provider: await _capability_projection(session, provider, now) for provider in _PROVIDERS
    }


async def _capability_projection(
    session: AsyncSession, provider: ProviderName, now: datetime
) -> _CapabilityProjection:
    rows = await session.scalars(
        select(ProviderCapability).where(
            ProviderCapability.provider == provider, ProviderCapability.capability == "READ_ONLY"
        )
    )
    admitted = [
        capability for capability in rows if _CONTRACT_HASH.fullmatch(capability.contract_hash)
    ]
    latest = _latest_capability(admitted)
    valid = _latest_capability(
        [capability for capability in admitted if capability.expires_at > now]
    )
    return _CapabilityProjection(valid, latest)


def _latest_capability(capabilities: list[ProviderCapability]) -> ProviderCapability | None:
    if not capabilities:
        return None
    return max(
        capabilities,
        key=lambda capability: (capability.expires_at, capability.created_at, str(capability.id)),
    )


async def _refresh_fixture(session: AsyncSession, provider: ProviderName, now: datetime) -> None:
    statement = insert(IntegrationStatus).values(
        provider=provider, status="fixture", last_success_at=now, version=1
    )
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=[IntegrationStatus.provider],
            set_={
                "status": "fixture",
                "last_success_at": now,
                "version": IntegrationStatus.version + 1,
            },
        )
    )
    await session.flush()


async def _write_mode(
    session: AsyncSession, provider: ProviderName, mode: IntegrationMode, expected_version: int
) -> IntegrationStatus:
    if expected_version == 0:
        insert_statement = (
            insert(IntegrationStatus)
            .values(provider=provider, status=mode, last_success_at=None, version=1)
            .on_conflict_do_nothing(index_elements=[IntegrationStatus.provider])
            .returning(IntegrationStatus.id)
        )
        write_id = (await session.execute(insert_statement)).scalar_one_or_none()
    else:
        update_statement = (
            update(IntegrationStatus)
            .where(
                IntegrationStatus.provider == provider,
                IntegrationStatus.version == expected_version,
            )
            .values(status=mode, last_success_at=None, version=IntegrationStatus.version + 1)
            .returning(IntegrationStatus.id)
        )
        write_id = (await session.execute(update_statement)).scalar_one_or_none()
    if write_id is None:
        raise _version_conflict()
    await session.flush()
    status = await _find_status(session, provider)
    if status is None:
        raise NexusError(
            ErrorCode.INTERNAL_ERROR, "The request could not be completed.", status_code=500
        )
    return status


async def _claim_or_replay(
    session: AsyncSession,
    *,
    actor: VerifiedServiceActor,
    route: str,
    idempotency_key: str,
    request_hash: str,
) -> IntegrationModeChangeResponse | None:
    statement = (
        insert(IdempotencyRecord)
        .values(
            actor_id=actor.actor_id,
            route=route,
            idempotency_key=idempotency_key,
            request_hash=request_hash,
            schema_version="1",
            response_payload={},
        )
        .on_conflict_do_nothing(
            index_elements=[
                IdempotencyRecord.actor_id,
                IdempotencyRecord.route,
                IdempotencyRecord.idempotency_key,
            ]
        )
        .returning(IdempotencyRecord.id)
    )
    result = await session.execute(statement)
    if result.scalar_one_or_none() is not None:
        return None
    records = await session.scalars(
        select(IdempotencyRecord).where(
            IdempotencyRecord.actor_id == actor.actor_id,
            IdempotencyRecord.route == route,
            IdempotencyRecord.idempotency_key == idempotency_key,
        )
    )
    record = records.one_or_none()
    if record is None:
        raise NexusError(
            ErrorCode.INTERNAL_ERROR, "The request could not be completed.", status_code=500
        )
    if record.request_hash != request_hash:
        raise NexusError(
            ErrorCode.IDEMPOTENCY_CONFLICT,
            "The idempotency key was already used for a different request.",
            status_code=409,
        )
    return IntegrationModeChangeResponse.model_validate(record.response_payload)


def _require_allowed_transition(
    provider: ProviderName, mode: IntegrationMode, capability: ProviderCapability | None
) -> None:
    if mode == "fixture" and provider != "fosun":
        raise NexusError(ErrorCode.FORBIDDEN, "The operation is not permitted.", status_code=403)
    if mode != "read_only" or capability is not None:
        return
    if provider == "fosun":
        raise NexusError(
            ErrorCode.CAPABILITY_EXPIRED,
            "A valid capability admission is required.",
            status_code=409,
        )
    raise NexusError(
        ErrorCode.PROVIDER_UNAVAILABLE, "The provider is unavailable.", status_code=503
    )


def _to_response(
    provider: ProviderName,
    persisted: IntegrationStatus | None,
    capabilities: _CapabilityProjection,
) -> IntegrationStatusResponse:
    mode = _stored_mode(persisted)
    version = persisted.version if persisted is not None else 0
    if mode == "read_only":
        if capabilities.valid is not None:
            return IntegrationStatusResponse(
                provider=provider,
                mode="read_only",
                quality="PASS",
                as_of=persisted.last_success_at if persisted is not None else None,
                capability_expires_at=capabilities.valid.expires_at,
                contract_hash=capabilities.valid.contract_hash,
                next_action=None,
                version=version,
            )
        return _disabled_response(
            provider,
            version=version,
            quality="FAIL",
            capabilities=capabilities,
            next_action="A valid capability admission is required.",
        )
    if mode == "fixture" and provider == "fosun":
        return IntegrationStatusResponse(
            provider=provider,
            mode="fixture",
            quality="PASS",
            as_of=(persisted.last_success_at if persisted is not None else None) or _FIXTURE_AS_OF,
            capability_expires_at=None,
            contract_hash=None,
            next_action="Read-only admission is required.",
            version=version,
        )
    return _disabled_response(provider, version=version, capabilities=capabilities)


def _stored_mode(persisted: IntegrationStatus | None) -> IntegrationMode:
    if persisted is not None and persisted.status in _MODES:
        return cast(IntegrationMode, persisted.status)
    return "fixture" if persisted is None else "disabled"


def _disabled_response(
    provider: ProviderName,
    *,
    version: int,
    capabilities: _CapabilityProjection,
    quality: Literal["PASS", "PARTIAL", "FAIL"] = "PARTIAL",
    next_action: str = "Provider is disabled.",
) -> IntegrationStatusResponse:
    latest = capabilities.latest
    return IntegrationStatusResponse(
        provider=provider,
        mode="disabled",
        quality=quality,
        as_of=None,
        capability_expires_at=latest.expires_at if latest is not None else None,
        contract_hash=latest.contract_hash if latest is not None else None,
        next_action=next_action,
        version=version,
    )


def _parse_if_match(value: str) -> int:
    candidate = value[1:-1] if value.startswith('"') and value.endswith('"') else value
    try:
        version = int(candidate)
    except ValueError:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR, "The request could not be completed.", status_code=422
        ) from None
    if version < 0 or str(version) != candidate:
        raise NexusError(
            ErrorCode.VALIDATION_ERROR, "The request could not be completed.", status_code=422
        )
    return version


def _version_conflict() -> NexusError:
    return NexusError(
        ErrorCode.VERSION_CONFLICT, "The resource version is no longer current.", status_code=409
    )


def _etag(version: int) -> str:
    return f'"{version}"'


def _success_meta(actor: VerifiedServiceActor, now: datetime) -> SuccessMetaResponse:
    return SuccessMetaResponse(request_id=actor.request_id, as_of=now)
