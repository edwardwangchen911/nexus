"""Authenticated, deterministic integration status routes."""

from datetime import UTC, datetime
from typing import cast

from fastapi import APIRouter, Body, Depends
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
    ProviderName,
    SuccessMetaResponse,
)
from nexus.persistence.models.runtime import IntegrationStatus
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()

_PROVIDERS: tuple[ProviderName, ...] = ("fosun", "alpaca", "sec", "fred", "openai")
_SAFE_MUTABLE_MODES: frozenset[str] = frozenset({"disabled", "fixture"})
_FIXTURE_AS_OF = datetime(2026, 8, 22, tzinfo=UTC)


@router.get(
    "/internal/v1/integrations",
    operation_id="listIntegrationStatuses",
    response_model=IntegrationStatusListEnvelope,
)
async def list_integration_statuses(
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> IntegrationStatusListEnvelope:
    """List deterministic baseline status, overridden only by persisted safe modes."""
    result = await session.scalars(select(IntegrationStatus))
    persisted = {row.provider: row for row in result}
    return IntegrationStatusListEnvelope(
        data=[_to_response(provider, persisted.get(provider)) for provider in _PROVIDERS],
        meta=_success_meta(actor),
    )


@router.post(
    "/internal/v1/integrations/{provider}/test",
    operation_id="testIntegrationConnectivity",
    response_model=IntegrationConnectivityTestResponse,
)
async def test_integration_connectivity(
    provider: ProviderName,
    _: IntegrationConnectivityTestRequest = Body(
        default_factory=IntegrationConnectivityTestRequest
    ),
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> IntegrationConnectivityTestResponse:
    """Refresh only the deterministic Fosun fixture; never contact a provider."""
    persisted = await _find_status(session, provider)
    status = _to_response(provider, persisted)
    if status.mode == "disabled":
        raise NexusError(
            ErrorCode.PROVIDER_UNAVAILABLE,
            "The provider is unavailable.",
            status_code=503,
        )
    if status.mode != "fixture":
        raise NexusError(ErrorCode.FORBIDDEN, "The operation is not permitted.", status_code=403)

    await _persist_mode(session, provider, "fixture", refreshed=True)
    return IntegrationConnectivityTestResponse(
        data=_to_response(provider, await _find_status(session, provider)),
        meta=_success_meta(actor),
    )


@router.post(
    "/internal/v1/integrations/{provider}/mode",
    operation_id="changeIntegrationMode",
    response_model=IntegrationModeChangeResponse,
)
async def change_integration_mode(
    provider: ProviderName,
    request: IntegrationModeChangeRequest,
    actor: VerifiedServiceActor = Depends(require_service_actor),
    session: AsyncSession = Depends(transactional_session),
) -> IntegrationModeChangeResponse:
    """Persist only safe fixture/disabled state without creating write capability."""
    if request.mode == "read_only":
        if provider == "fosun":
            raise NexusError(
                ErrorCode.CAPABILITY_EXPIRED,
                "A valid capability admission is required.",
                status_code=409,
            )
        raise NexusError(ErrorCode.FORBIDDEN, "The operation is not permitted.", status_code=403)

    await _persist_mode(session, provider, request.mode)
    return IntegrationModeChangeResponse(
        data=_to_response(provider, await _find_status(session, provider)),
        meta=_success_meta(actor),
    )


async def _find_status(
    session: AsyncSession, provider: ProviderName
) -> IntegrationStatus | None:
    """Read the provider row by its unique natural key, not its UUID primary key."""
    result = await session.scalars(
        select(IntegrationStatus).where(IntegrationStatus.provider == provider)
    )
    return result.one_or_none()


async def _persist_mode(
    session: AsyncSession,
    provider: ProviderName,
    mode: IntegrationMode,
    *,
    refreshed: bool = False,
) -> None:
    """Atomically upsert safe mode state without a duplicate-provider race."""
    if mode not in _SAFE_MUTABLE_MODES:
        raise ValueError("only safe integration modes may be persisted")
    last_success_at = _FIXTURE_AS_OF if mode == "fixture" and refreshed else None
    statement = insert(IntegrationStatus).values(
        provider=provider,
        status=mode,
        last_success_at=last_success_at,
    )
    await session.execute(
        statement.on_conflict_do_update(
            index_elements=[IntegrationStatus.provider],
            set_={"status": mode, "last_success_at": last_success_at},
        )
    )
    await session.flush()


def _to_response(
    provider: ProviderName, persisted: IntegrationStatus | None
) -> IntegrationStatusResponse:
    """Derive every public field from the closed fixture mode, never configuration."""
    mode = _mode_for(provider, persisted)
    if mode == "fixture":
        return IntegrationStatusResponse(
            provider=provider,
            mode=mode,
            quality="PASS",
            as_of=(persisted.last_success_at if persisted is not None else None) or _FIXTURE_AS_OF,
            capability_expires_at=None,
            contract_hash="fixture-v1",
            next_action="Read-only admission is required.",
        )
    if mode == "read_only":
        return IntegrationStatusResponse(
            provider=provider,
            mode=mode,
            quality="PARTIAL",
            as_of=persisted.last_success_at if persisted is not None else None,
            capability_expires_at=None,
            contract_hash=None,
            next_action="A valid capability admission is required.",
        )
    return IntegrationStatusResponse(
        provider=provider,
        mode="disabled",
        quality="PARTIAL",
        as_of=None,
        capability_expires_at=None,
        contract_hash=None,
        next_action="Provider is disabled.",
    )


def _mode_for(provider: ProviderName, persisted: IntegrationStatus | None) -> IntegrationMode:
    """Use only stored closed modes; unrecognized legacy state falls back safely."""
    if persisted is not None and persisted.status in {"disabled", "fixture", "read_only"}:
        return cast(IntegrationMode, persisted.status)
    return "fixture" if provider == "fosun" else "disabled"


def _success_meta(actor: VerifiedServiceActor) -> SuccessMetaResponse:
    """Build safe, deterministic response metadata from the verified actor only."""
    return SuccessMetaResponse(request_id=actor.request_id, as_of=_FIXTURE_AS_OF)
