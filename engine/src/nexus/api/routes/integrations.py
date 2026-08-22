"""HTTP wiring for authenticated deterministic integration status routes."""

from datetime import UTC, datetime
from typing import Annotated

from fastapi import APIRouter, Body, Depends, Header, Request, Response
from nexus.api.auth import VerifiedServiceActor
from nexus.api.dependencies import require_service_actor, transactional_session
from nexus.api.errors import ErrorCode, NexusError
from nexus.api.schemas.integrations import (
    IntegrationConnectivityTestRequest,
    IntegrationConnectivityTestResponse,
    IntegrationModeChangeRequest,
    IntegrationModeChangeResponse,
    IntegrationStatusListEnvelope,
    NexusErrorEnvelope,
    ProviderName,
    SuccessMetaResponse,
)
from nexus.domain.time import require_utc
from nexus.integrations.service import IntegrationStatusService
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()

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
    service = IntegrationStatusService(session, now)
    persisted = await service.all_statuses()
    capabilities = await service.capabilities()
    return IntegrationStatusListEnvelope(
        data=[
            service.response(provider, persisted.get(provider), capabilities[provider])
            for provider in service.providers
        ],
        meta=_success_meta(actor, now),
    )


@router.post(
    "/internal/v1/integrations/{provider}/test",
    operation_id="testIntegrationConnectivity",
    response_model=IntegrationConnectivityTestResponse,
    responses={
        200: _ETAG_RESPONSE,
        **{status: _ERROR_RESPONSES[status] for status in (401, 403, 409, 422, 503)},
    },
)
async def test_integration_connectivity(
    provider: ProviderName,
    http_request: Request,
    response: Response,
    idempotency_key: Annotated[
        str, Header(alias="Idempotency-Key", min_length=1, max_length=255)
    ],
    if_match: Annotated[str, Header(alias="If-Match", min_length=1, max_length=64)],
    _: IntegrationConnectivityTestRequest = Body(
        default_factory=IntegrationConnectivityTestRequest
    ),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
    now: datetime = Depends(_utc_now),
) -> IntegrationConnectivityTestResponse:
    """Refresh only the Fosun fixture through replay and version controls."""
    service = IntegrationStatusService(session, now)
    route = f"/internal/v1/integrations/{provider}/test"
    replay = await service.claim_or_replay(
        actor=actor,
        route=route,
        idempotency_key=idempotency_key,
        request_hash=service.request_hash(await http_request.body(), if_match),
        response_model=IntegrationConnectivityTestResponse,
    )
    if replay is not None:
        response.headers["ETag"] = _etag(replay.data.version)
        return replay
    expected_version = service.parse_if_match(if_match)
    persisted = await service.find_status(provider)
    capability = await service.capability(provider)
    current = service.response(provider, persisted, capability)
    if current.mode == "disabled":
        raise NexusError(
            ErrorCode.PROVIDER_UNAVAILABLE, "The provider is unavailable.", status_code=503
        )
    if current.mode != "fixture":
        raise NexusError(ErrorCode.FORBIDDEN, "The operation is not permitted.", status_code=403)
    status = await service.refresh_fixture(
        provider, expected_version=expected_version, last_success_at=now
    )
    result = IntegrationConnectivityTestResponse(
        data=service.response(provider, status, capability), meta=_success_meta(actor, now)
    )
    await service.complete_idempotency(
        actor=actor, route=route, idempotency_key=idempotency_key, response=result
    )
    response.headers["ETag"] = _etag(result.data.version)
    return result


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
    """Persist a policy-checked mode with replay and optimistic-lock controls."""
    service = IntegrationStatusService(session, now)
    route = f"/internal/v1/integrations/{provider}/mode"
    replay = await service.claim_or_replay(
        actor=actor,
        route=route,
        idempotency_key=idempotency_key,
        request_hash=service.request_hash(await http_request.body(), if_match),
        response_model=IntegrationModeChangeResponse,
    )
    if replay is not None:
        response.headers["ETag"] = _etag(replay.data.version)
        return replay
    expected_version = service.parse_if_match(if_match)
    persisted = await service.find_status(provider)
    capability = await service.capability(provider)
    admitted_contract_hash = service.require_mode_transition(
        provider, request.mode, persisted, capability
    )
    status = await service.write_status(
        provider,
        mode=request.mode,
        expected_version=expected_version,
        last_success_at=None,
        admitted_contract_hash=admitted_contract_hash,
    )
    result = IntegrationModeChangeResponse(
        data=service.response(provider, status, capability), meta=_success_meta(actor, now)
    )
    await service.complete_idempotency(
        actor=actor, route=route, idempotency_key=idempotency_key, response=result
    )
    response.headers["ETag"] = _etag(result.data.version)
    return result


def _etag(version: int) -> str:
    return f'"{version}"'


def _success_meta(actor: VerifiedServiceActor, now: datetime) -> SuccessMetaResponse:
    return SuccessMetaResponse(request_id=actor.request_id, as_of=now)
