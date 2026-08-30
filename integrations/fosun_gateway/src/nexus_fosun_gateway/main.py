"""HTTP API process entry point for the isolated Fosun gateway."""

import hmac
import importlib
import math
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from nexus_fosun_gateway.activation import evaluate_activation_paths
from nexus_fosun_gateway.adapter.fixture import FixtureFosunTransport
from nexus_fosun_gateway.adapter.protocol import FosunTransport
from nexus_fosun_gateway.adapter.sdk import SdkFosunTransport
from nexus_fosun_gateway.admission import sdk_admission_is_valid
from nexus_fosun_gateway.domain import GatewayError
from nexus_fosun_gateway.routes.read_only import router as read_only_router
from nexus_fosun_gateway.security.hmac import GatewayHmacVerifier, VerifiedGatewayActor
from nexus_fosun_gateway.security.provider_tls import enforce_sdk_tls_verification
from nexus_fosun_gateway.security.tls import production_uvicorn_kwargs
from nexus_fosun_gateway.settings import Environment, GatewaySettings


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


def build_transport(settings: GatewaySettings) -> FosunTransport:
    """Create only the explicitly configured transport; never fall back across modes."""
    if settings.transport_mode == "fixture":
        return FixtureFosunTransport()
    if (
        not sdk_admission_is_valid(settings)
        or settings.fsopenapi_base_url is None
        or settings.fsopenapi_api_key is None
        or settings.fsopenapi_client_private_key is None
        or settings.fsopenapi_server_public_key is None
        or settings.activation_probe_artifact is None
        or settings.activation_contract_hash is None
    ):
        raise _sdk_not_admitted()
    try:
        activation = evaluate_activation_paths(
            settings.provider_manifest,
            settings.activation_probe_artifact,
        )
    except (OSError, ValueError):
        raise _sdk_not_admitted() from None
    if (
        activation.status != "activated_read_only"
        or activation.capability_expires_at is None
        or not hmac.compare_digest(
            activation.contract_hash,
            settings.activation_contract_hash,
        )
    ):
        raise _sdk_not_admitted()
    try:
        module = importlib.import_module("fsopenapi")
        auth_module = importlib.import_module("fsopenapi.auth")
        client_type = getattr(module, "SDKClient")
    except (ImportError, AttributeError):
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
    return SdkFosunTransport(
        client,
        capability_expires_at=activation.capability_expires_at,
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
    )
    app.state.settings = resolved_settings
    verifier = GatewayHmacVerifier(resolved_settings)
    try:
        app.state.transport = build_transport(resolved_settings)
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
            headers = {
                "Retry-After": str(max(1, math.ceil(error.retry_after_seconds)))
            }
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
        if app.state.hmac_verifier.is_ready():
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
        uvicorn.run(
            app,
            host="0.0.0.0",
            port=8081,
            **production_uvicorn_kwargs(settings),
        )
        return
    uvicorn.run(app, host="0.0.0.0", port=8081)
