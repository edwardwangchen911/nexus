"""HTTP API process entry point for the NEXUS engine."""

import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from nexus.api.errors import NexusError
from nexus.api.routes.system import router as system_router
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

    @app.get("/health/live")
    async def liveness() -> dict[str, str]:
        return {"status": "alive", "service": "nexus-engine"}

    app.include_router(system_router)
    return app


def run() -> None:
    """Run the API process."""
    uvicorn.run(create_engine_app(), host="0.0.0.0", port=8000)
