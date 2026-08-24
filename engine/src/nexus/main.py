"""HTTP API process entry point for the NEXUS engine."""

import uvicorn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from nexus.api.errors import ErrorCode, NexusError
from nexus.api.routes.broker_sync import router as broker_sync_router
from nexus.api.routes.diagnostics import router as diagnostics_router
from nexus.api.routes.integrations import router as integrations_router
from nexus.api.routes.market import router as market_router
from nexus.api.routes.optimization import router as optimization_router
from nexus.api.routes.overview import router as overview_router
from nexus.api.routes.portfolios import router as portfolios_router
from nexus.api.routes.system import router as system_router
from nexus.api.schemas.optimization import WhatIfRequest
from nexus.domain.ids import new_uuid7
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings


def create_engine_app(settings: EngineSettings | None = None) -> FastAPI:
    """Create the stateless API application without probing dependencies."""
    app = FastAPI(title="NEXUS Engine", version="0.1.0")
    app.state.settings = settings or EngineSettings()
    app.state.session_factory = session_factory(app.state.settings)

    @app.exception_handler(NexusError)
    async def nexus_error_handler(_: Request, error: NexusError) -> JSONResponse:
        return JSONResponse(
            status_code=error.status_code,
            content=error.to_envelope(new_uuid7()),
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_error_handler(_: Request, error: StarletteHTTPException) -> JSONResponse:
        code = ErrorCode.NOT_FOUND if error.status_code == 404 else ErrorCode.VALIDATION_ERROR
        return JSONResponse(
            status_code=error.status_code,
            content=NexusError(
                code, "The request could not be completed.", status_code=error.status_code
            ).to_envelope(new_uuid7()),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(_: Request, _error: RequestValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content=NexusError(
                ErrorCode.VALIDATION_ERROR, "The request could not be completed.", status_code=422
            ).to_envelope(new_uuid7()),
        )

    @app.exception_handler(Exception)
    async def unexpected_error_handler(_: Request, _error: Exception) -> JSONResponse:
        return JSONResponse(
            status_code=500,
            content=NexusError(
                ErrorCode.INTERNAL_ERROR, "The request could not be completed.", status_code=500
            ).to_envelope(new_uuid7()),
        )

    @app.get("/health/live")
    async def liveness() -> dict[str, str]:
        return {"status": "alive", "service": "nexus-engine"}

    app.include_router(system_router)
    app.include_router(integrations_router)
    app.include_router(market_router)
    app.include_router(portfolios_router)
    app.include_router(diagnostics_router)
    app.include_router(overview_router)
    app.include_router(optimization_router)
    app.include_router(broker_sync_router)
    _install_what_if_request_schema(app)
    return app


def _install_what_if_request_schema(app: FastAPI) -> None:
    """Expose the bounded dependency payload as the same public OpenAPI component."""
    original_openapi = app.openapi

    def openapi() -> dict[str, object]:
        document = original_openapi()
        components = document.setdefault("components", {})
        schemas = components.setdefault("schemas", {})
        request_schema = WhatIfRequest.model_json_schema(
            ref_template="#/components/schemas/{model}"
        )
        definitions = request_schema.pop("$defs", {})
        schemas.update(definitions)
        schemas["WhatIfRequest"] = request_schema
        return document

    app.openapi = openapi  # type: ignore[method-assign]


def run() -> None:
    """Run the API process."""
    uvicorn.run(create_engine_app(), host="0.0.0.0", port=8000)
