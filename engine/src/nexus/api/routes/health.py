"""Liveness and authenticated dependency readiness."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from fastapi import APIRouter, Depends, Request
from fastapi.responses import JSONResponse
from nexus.api.auth import reserve_hmac_nonce, verify_hmac_signature_v1
from nexus.api.dependencies import get_session_factory, get_settings
from nexus.api.errors import NexusError
from nexus.api.routes.portfolios import _utc_now
from nexus.api.schemas.integrations import NexusErrorEnvelope, SuccessMetaResponse
from nexus.api.schemas.risk_audit import (
    ReadinessComponents,
    ReadinessEnvelope,
    ReadinessResponse,
)
from nexus.readiness import (
    object_store_readiness,
    overall_readiness,
    probe_database,
    probe_general_worker,
    probe_migration,
)
from nexus.settings import EngineSettings
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

router = APIRouter()


@router.get(
    "/health/ready",
    operation_id="getEngineReadiness",
    response_model=ReadinessEnvelope,
    responses={
        401: {"model": NexusErrorEnvelope, "description": "Service authentication failed."},
        503: {"model": ReadinessEnvelope, "description": "A required component is unavailable."},
    },
)
async def readiness(
    request: Request,
    settings: EngineSettings = Depends(get_settings),
    factory: async_sessionmaker[AsyncSession] = Depends(get_session_factory),
    now: datetime = Depends(_utc_now),
) -> ReadinessEnvelope | JSONResponse:
    """Return only closed component states; provider health is deliberately excluded."""
    actor, nonce = verify_hmac_signature_v1(request, await request.body(), settings)
    components: dict[str, str] = {
        "database": "unavailable",
        "migration": "unavailable",
        "object_store": "unavailable",
        "worker": "unavailable",
    }
    try:
        async with factory() as session, session.begin():
            await reserve_hmac_nonce(session, actor=actor, nonce=nonce)
            components["database"] = await probe_database(session)
    except NexusError:
        raise
    except Exception:
        pass
    if components["database"] == "ready":
        try:
            async with factory() as session, session.begin():
                components["migration"] = await probe_migration(session)
        except Exception:
            pass
        try:
            async with factory() as session, session.begin():
                injected_clock = getattr(request.app.state, "utc_clock", None)
                components["worker"] = await probe_general_worker(
                    session,
                    now=now if callable(injected_clock) else None,
                )
        except Exception:
            pass
    probe = getattr(request.app.state, "readiness_object_store", None)
    components["object_store"] = await object_store_readiness(
        probe if _is_object_store_probe(probe) else None
    )
    typed_components = ReadinessComponents.model_validate(components)
    ready = overall_readiness(typed_components.model_dump())
    envelope = ReadinessEnvelope(
        data=ReadinessResponse(
            status="ready" if ready else "unready",
            components=typed_components,
        ),
        meta=SuccessMetaResponse(request_id=actor.request_id, as_of=now),
    )
    if ready:
        return envelope
    return JSONResponse(status_code=503, content=envelope.model_dump(mode="json"))


def _is_object_store_probe(value: Any) -> bool:
    return callable(getattr(value, "probe_bucket", None))
