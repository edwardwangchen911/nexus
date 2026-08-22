"""HTTP API process entry point for the NEXUS engine."""

import uvicorn
from fastapi import FastAPI

from nexus.settings import EngineSettings


def create_engine_app(settings: EngineSettings | None = None) -> FastAPI:
    """Create the stateless API application without probing dependencies."""
    app = FastAPI(title="NEXUS Engine", version="0.1.0")
    app.state.settings = settings or EngineSettings()

    @app.get("/health/live")
    async def liveness() -> dict[str, str]:
        return {"status": "alive", "service": "nexus-engine"}

    return app


def run() -> None:
    """Run the API process."""
    uvicorn.run(create_engine_app(), host="0.0.0.0", port=8000)
