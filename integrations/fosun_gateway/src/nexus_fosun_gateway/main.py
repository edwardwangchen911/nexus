"""HTTP API process entry point for the isolated Fosun gateway."""

import asyncio
import hmac
import importlib
import json
import math
import re
import time
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from threading import Event
from typing import cast

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from nexus_fosun_gateway.activation import (
    MAX_ARTIFACT_BYTES,
    MAX_PUBLICATION_RECEIPT_BYTES,
    MAX_TRUSTED_PUBLISHER_KEY_BYTES,
    RuntimeAuthorization,
    _read_bounded_or_empty,
    evaluate_scoped_activation,
)
from nexus_fosun_gateway.adapter.fixture import FixtureFosunTransport
from nexus_fosun_gateway.adapter.protocol import FosunTransport
from nexus_fosun_gateway.adapter.sdk import SdkFosunTransport
from nexus_fosun_gateway.admission import admitted_sdk_subject
from nexus_fosun_gateway.domain import GatewayError
from nexus_fosun_gateway.routes.read_only import router as read_only_router
from nexus_fosun_gateway.security.dependency_logging import (
    enforce_sdk_dependency_log_boundary,
)
from nexus_fosun_gateway.security.hmac import GatewayHmacVerifier, VerifiedGatewayActor
from nexus_fosun_gateway.security.provider_environment import (
    UnsafeProviderEnvironmentError,
    reject_unsafe_provider_environment,
)
from nexus_fosun_gateway.security.provider_tls import enforce_sdk_tls_verification
from nexus_fosun_gateway.security.tls import production_listener_configs
from nexus_fosun_gateway.settings import BROKER_LISTENER_PORT, Environment, GatewaySettings


@asynccontextmanager
async def _gateway_lifespan(app: FastAPI) -> AsyncIterator[None]:
    verifier = getattr(app.state, "hmac_verifier", None)
    if isinstance(verifier, GatewayHmacVerifier):
        verifier.mark_serving()
    try:
        yield
    finally:
        if isinstance(verifier, GatewayHmacVerifier):
            verifier.close()
        await _close_transport(app)


async def _close_transport(app: FastAPI) -> None:
    """Close admission without waiting for an already poisoned SDK daemon."""
    transport = cast(FosunTransport, app.state.transport)
    await transport.aclose()


def build_transport(
    settings: GatewaySettings,
    *,
    on_poison: Callable[[], None] | None = None,
    on_activation_expired: Callable[[datetime], None] | None = None,
    activation_checked_at: datetime | None = None,
    activation_monotonic_anchor: float | None = None,
) -> tuple[FosunTransport, RuntimeAuthorization | None]:
    """Create only the explicitly configured transport; never fall back across modes."""
    if settings.transport_mode == "fixture":
        return FixtureFosunTransport(), None
    if (
        not settings.sdk_admitted
        or settings.fsopenapi_base_url is None
        or settings.fsopenapi_api_key is None
        or settings.fsopenapi_client_private_key is None
        or settings.fsopenapi_server_public_key is None
        or settings.activation_probe_artifact is None
        or settings.activation_probe_receipt is None
        or settings.activation_probe_publisher_public_key_b64u_file is None
        or settings.activation_contract_hash_file is None
        or settings.read_only_scope_contract_version != "v1"
        or settings.read_only_scope is None
    ):
        raise _sdk_not_admitted()
    try:
        reject_unsafe_provider_environment()
    except UnsafeProviderEnvironmentError:
        raise _sdk_not_admitted() from None
    if activation_monotonic_anchor is None:
        activation_monotonic_anchor = time.monotonic()
    if activation_checked_at is None:
        activation_checked_at = datetime.now(UTC)
    trusted_publication_key_bytes = _read_bounded_or_empty(
        settings.activation_probe_publisher_public_key_b64u_file,
        max_bytes=MAX_TRUSTED_PUBLISHER_KEY_BYTES,
    )
    activation_contract_hash_bytes = _read_bounded_or_empty(
        settings.activation_contract_hash_file,
        max_bytes=64,
    )
    try:
        activation_contract_hash = activation_contract_hash_bytes.decode("ascii")
    except UnicodeError:
        raise _sdk_not_admitted() from None
    if re.fullmatch(r"[0-9a-f]{64}", activation_contract_hash) is None:
        raise _sdk_not_admitted()
    manifest_bytes = _read_bounded_or_empty(
        settings.provider_manifest,
        max_bytes=MAX_ARTIFACT_BYTES,
    )
    probe_bytes = _read_bounded_or_empty(
        settings.activation_probe_artifact,
        max_bytes=MAX_ARTIFACT_BYTES,
    )
    publication_receipt_bytes = _read_bounded_or_empty(
        settings.activation_probe_receipt,
        max_bytes=MAX_PUBLICATION_RECEIPT_BYTES,
    )
    try:
        manifest_document = json.loads(manifest_bytes)
        subject = admitted_sdk_subject(manifest_document)
        if subject is None or not hmac.compare_digest(
            subject.provider_base_url,
            settings.fsopenapi_base_url,
        ):
            raise ValueError
        activation, runtime_authorization = evaluate_scoped_activation(
            manifest_bytes,
            probe_bytes,
            publication_receipt_bytes,
            expected_scope=settings.read_only_scope,
            trusted_publication_public_key_bytes=trusted_publication_key_bytes,
            checked_at=activation_checked_at,
        )
    except (OSError, UnicodeError, ValueError):
        raise _sdk_not_admitted() from None
    if (
        activation.status != "activated_read_only"
        or activation.capability_expires_at is None
        or runtime_authorization is None
        or runtime_authorization.scope != settings.read_only_scope
        or not hmac.compare_digest(
            activation.contract_hash,
            activation_contract_hash,
        )
    ):
        raise _sdk_not_admitted()
    try:
        enforce_sdk_dependency_log_boundary()
        module = importlib.import_module("fsopenapi")
        auth_module = importlib.import_module("fsopenapi.auth")
        client_type = getattr(module, "SDKClient")
        enforce_sdk_dependency_log_boundary()
    except (ImportError, AttributeError, RuntimeError):
        raise _sdk_not_admitted() from None
    try:
        client = client_type(
            base_url=settings.fsopenapi_base_url,
            api_key=settings.fsopenapi_api_key.get_secret_value(),
            logging_enable=False,
            log_body=False,
        )
        enforce_sdk_tls_verification(client, auth_module)
    except Exception:
        raise _sdk_not_admitted() from None
    return (
        SdkFosunTransport(
            client,
            capability_expires_at=activation.capability_expires_at,
            activation_checked_at=activation.checked_at,
            activation_monotonic_anchor=activation_monotonic_anchor,
            on_poison=on_poison,
            on_activation_expired=on_activation_expired,
        ),
        runtime_authorization,
    )


def _sdk_not_admitted() -> GatewayError:
    return GatewayError(
        "SDK_NOT_ADMITTED",
        "Provider SDK has not passed the production admission gate.",
        status_code=503,
        next_action="COMPLETE_SDK_ADMISSION",
    )


def create_gateway_app(settings: GatewaySettings | None = None) -> FastAPI:
    """Create the gateway without loading a provider SDK or credentials."""
    resolved_settings = settings or GatewaySettings()
    app = FastAPI(
        title="NEXUS Fosun Gateway",
        version="0.1.0",
        lifespan=_gateway_lifespan,
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    app.state.settings = resolved_settings
    shutdown_requested = Event()
    app.state.shutdown_requested = shutdown_requested
    verifier = GatewayHmacVerifier(resolved_settings)

    def mark_activation_expired(expires_at: datetime) -> None:
        try:
            verifier.mark_activation_expired(expires_at)
        finally:
            shutdown_requested.set()

    try:
        if resolved_settings.transport_mode == "fixture":
            transport, runtime_authorization = build_transport(
                resolved_settings,
                on_poison=verifier.mark_not_ready,
                on_activation_expired=mark_activation_expired,
            )
        else:
            activation_monotonic_anchor = time.monotonic()
            activation_checked_at = verifier.trusted_activation_checked_at()
            transport, runtime_authorization = build_transport(
                resolved_settings,
                on_poison=verifier.mark_not_ready,
                on_activation_expired=mark_activation_expired,
                activation_checked_at=activation_checked_at,
                activation_monotonic_anchor=activation_monotonic_anchor,
            )
        verifier.bind_readiness_expiry(transport.readiness_expires_at())
        app.state.transport = transport
        app.state.runtime_authorization = runtime_authorization
    except BaseException:
        verifier.close()
        raise
    app.state.hmac_verifier = verifier
    app.include_router(read_only_router)

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(request: Request, error: GatewayError) -> JSONResponse:
        request_id = _safe_request_id(request)
        headers = None
        if error.retry_after_seconds is not None:
            headers = {"Retry-After": str(max(1, math.ceil(error.retry_after_seconds)))}
        return JSONResponse(
            status_code=error.status_code,
            headers=headers,
            content={
                "error": {
                    "code": error.code,
                    "message": error.message,
                    "next_action": error.next_action,
                },
                "request_id": request_id,
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(
        request: Request,
        error: StarletteHTTPException,
    ) -> JSONResponse:
        if error.status_code == 404:
            code = "NOT_FOUND"
            message = "Route not found."
        else:
            code = "HTTP_ERROR"
            message = "Request could not be handled."
        return JSONResponse(
            status_code=error.status_code,
            content={
                "error": {
                    "code": code,
                    "message": message,
                    "next_action": "USE_READ_ONLY_OPERATION",
                },
                "request_id": _safe_request_id(request),
            },
        )

    @app.get("/health/live")
    async def liveness() -> dict[str, str]:
        return {"status": "alive", "service": "nexus-fosun-gateway"}

    @app.get("/health/ready")
    async def readiness() -> JSONResponse:
        transport = cast(FosunTransport, app.state.transport)
        if app.state.hmac_verifier.is_ready() and transport.is_ready():
            return JSONResponse(
                status_code=200,
                content={"status": "ready", "service": "nexus-fosun-gateway"},
            )
        return JSONResponse(
            status_code=503,
            headers={"Retry-After": "1"},
            content={"status": "draining", "service": "nexus-fosun-gateway"},
        )

    return app


def _safe_request_id(request: Request) -> str | None:
    actor = getattr(request.state, "verified_gateway_actor", None)
    return str(actor.request_id) if isinstance(actor, VerifiedGatewayActor) else None


def run() -> None:
    """Run the isolated gateway process."""
    settings = GatewaySettings()
    app = create_gateway_app(settings)
    if settings.environment is Environment.PRODUCTION:
        asyncio.run(_serve_production(app, settings))
        return
    uvicorn.run(app, host="0.0.0.0", port=BROKER_LISTENER_PORT)


async def _serve_production(app: FastAPI, settings: GatewaySettings) -> None:
    """Serve both client-CA domains while sharing one SDK and replay journal."""
    verifier = app.state.hmac_verifier
    if not isinstance(verifier, GatewayHmacVerifier):
        raise RuntimeError("GATEWAY_HMAC_VERIFIER_REQUIRED")
    servers = tuple(
        uvicorn.Server(
            uvicorn.Config(
                app,
                host="0.0.0.0",
                port=listener.port,
                lifespan="off",
                **listener.tls_kwargs,
            )
        )
        for listener in production_listener_configs(settings)
    )
    verifier.mark_serving()
    tasks = tuple(asyncio.create_task(server.serve()) for server in servers)
    shutdown_task = asyncio.create_task(_wait_for_shutdown(app.state.shutdown_requested))
    results: list[object] | tuple[object, ...] = ()
    try:
        await asyncio.wait((*tasks, shutdown_task), return_when=asyncio.FIRST_COMPLETED)
    finally:
        for server in servers:
            server.should_exit = True
        shutdown_task.cancel()
        try:
            results = await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            await asyncio.gather(shutdown_task, return_exceptions=True)
            verifier.close()
            await _close_transport(app)
    for result in results:
        if isinstance(result, BaseException):
            raise result


async def _wait_for_shutdown(shutdown_requested: Event) -> None:
    while not shutdown_requested.is_set():
        await asyncio.sleep(0.1)
