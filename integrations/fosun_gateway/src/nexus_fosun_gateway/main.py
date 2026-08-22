"""HTTP API process entry point for the isolated Fosun gateway."""

import uvicorn
from fastapi import FastAPI

from nexus_fosun_gateway.settings import GatewaySettings


def create_gateway_app(settings: GatewaySettings | None = None) -> FastAPI:
    """Create the gateway without loading a provider SDK or credentials."""
    app = FastAPI(title="NEXUS Fosun Gateway", version="0.1.0")
    app.state.settings = settings or GatewaySettings()

    @app.get("/health/live")
    async def liveness() -> dict[str, str]:
        return {"status": "alive", "service": "nexus-fosun-gateway"}

    return app


def run() -> None:
    """Run the isolated gateway process."""
    uvicorn.run(create_gateway_app(), host="0.0.0.0", port=8081)
