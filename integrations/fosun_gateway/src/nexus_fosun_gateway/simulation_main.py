"""Composition root for the isolated no-socket simulation fixture process."""

from __future__ import annotations

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from nexus_fosun_gateway.adapter.simulation_fixture import (
    FixtureSimulationGateway,
    FixtureSimulationGatewayScript,
)
from nexus_fosun_gateway.domain import GatewayError
from nexus_fosun_gateway.routes.simulation import router as simulation_router
from nexus_fosun_gateway.security.hmac import GatewayHmacVerifier, VerifiedGatewayActor
from nexus_fosun_gateway.settings import Environment, GatewaySettings


def create_simulation_gateway_app(
    settings: GatewaySettings,
    script: FixtureSimulationGatewayScript,
) -> FastAPI:
    """Build only the offline fixture surface and never import the Provider SDK."""
    if settings.environment is not Environment.TEST or settings.transport_mode != "fixture":
        raise ValueError("simulation fixture composition requires explicit test fixture settings")
    app = FastAPI(title="NEXUS Fosun Simulation Fixture Gateway", version="0.1.0")
    app.state.settings = settings
    app.state.hmac_verifier = GatewayHmacVerifier(settings)
    app.state.simulation_transport = FixtureSimulationGateway(script)
    app.include_router(simulation_router)

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(request: Request, error: GatewayError) -> JSONResponse:
        return JSONResponse(
            status_code=error.status_code,
            content={
                "error": {
                    "code": error.code,
                    "message": error.message,
                    "next_action": error.next_action,
                },
                "request_id": _safe_request_id(request),
            },
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(
        request: Request, error: StarletteHTTPException
    ) -> JSONResponse:
        return JSONResponse(
            status_code=error.status_code,
            content={
                "error": {
                    "code": "NOT_FOUND" if error.status_code == 404 else "HTTP_ERROR",
                    "message": "Route not found."
                    if error.status_code == 404
                    else "Request could not be handled.",
                    "next_action": "USE_SIMULATION_OPERATION",
                },
                "request_id": _safe_request_id(request),
            },
        )

    @app.get("/health/live")
    async def liveness() -> dict[str, str]:
        return {"status": "alive", "service": "nexus-fosun-simulation-fixture"}

    return app


def _safe_request_id(request: Request) -> str | None:
    actor = getattr(request.state, "verified_gateway_actor", None)
    return str(actor.request_id) if isinstance(actor, VerifiedGatewayActor) else None


def run_fixture() -> None:
    """Run the test-only fixture process without provider configuration."""
    app = create_simulation_gateway_app(
        GatewaySettings.for_test(), FixtureSimulationGatewayScript()
    )
    uvicorn.run(app, host="127.0.0.1", port=8082)
