"""HTTP API process entry point for the NEXUS engine."""

import uvicorn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from nexus.api.errors import ErrorCode, NexusError
from nexus.api.routes.briefs import router as briefs_router
from nexus.api.routes.broker_simulation import router as broker_simulation_router
from nexus.api.routes.broker_sync import router as broker_sync_router
from nexus.api.routes.diagnostics import router as diagnostics_router
from nexus.api.routes.evolution import router as evolution_router
from nexus.api.routes.health import router as health_router
from nexus.api.routes.integrations import router as integrations_router
from nexus.api.routes.live_evaluation import router as live_evaluation_router
from nexus.api.routes.manual_paper import router as manual_paper_router
from nexus.api.routes.market import router as market_router
from nexus.api.routes.news import router as news_router
from nexus.api.routes.optimization import router as optimization_router
from nexus.api.routes.overview import router as overview_router
from nexus.api.routes.paper import router as paper_router
from nexus.api.routes.portfolio_decisions import router as portfolio_decisions_router
from nexus.api.routes.portfolios import router as portfolios_router
from nexus.api.routes.research import router as research_router
from nexus.api.routes.risk_audit import router as risk_audit_router
from nexus.api.routes.strategies import router as strategies_router
from nexus.api.routes.system import router as system_router
from nexus.api.routes.workbench import router as workbench_router
from nexus.api.schemas.optimization import PortfolioScenarioRequest, WhatIfRequest
from nexus.api.schemas.portfolio_decisions import PortfolioDecisionCreateRequest
from nexus.diagnostics.artifacts import ArtifactByteReader
from nexus.domain.ids import new_uuid7
from nexus.market.ingestion import MarketObjectStore
from nexus.market.object_store import S3ProviderEvidenceStore
from nexus.persistence.session import session_factory
from nexus.readiness import ReadinessObjectStore, S3ReadinessObjectStore
from nexus.settings import EngineSettings


def create_engine_app(
    settings: EngineSettings | None = None,
    *,
    readiness_object_store: ReadinessObjectStore | None = None,
    diagnostic_artifact_reader: ArtifactByteReader | None = None,
    scenario_evidence_store: MarketObjectStore | None = None,
) -> FastAPI:
    """Create the stateless API application without probing dependencies."""
    app = FastAPI(title="NEXUS Engine", version="0.1.0")
    app.state.settings = settings or EngineSettings()
    app.state.session_factory = session_factory(app.state.settings)
    app.state.diagnostic_artifact_reader = (
        diagnostic_artifact_reader
        if diagnostic_artifact_reader is not None
        else S3ProviderEvidenceStore(app.state.settings).read_normalized
    )
    app.state.readiness_object_store = readiness_object_store or S3ReadinessObjectStore(
        app.state.settings
    )
    app.state.scenario_evidence_store = scenario_evidence_store or S3ProviderEvidenceStore(
        app.state.settings
    )

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
    app.include_router(health_router)
    app.include_router(integrations_router)
    app.include_router(live_evaluation_router)
    app.include_router(market_router)
    app.include_router(news_router)
    app.include_router(portfolios_router)
    app.include_router(diagnostics_router)
    app.include_router(overview_router)
    app.include_router(optimization_router)
    app.include_router(workbench_router)
    app.include_router(paper_router)
    app.include_router(manual_paper_router)
    app.include_router(portfolio_decisions_router)
    app.include_router(broker_sync_router)
    app.include_router(broker_simulation_router)
    app.include_router(research_router)
    app.include_router(briefs_router)
    app.include_router(strategies_router)
    app.include_router(evolution_router)
    app.include_router(risk_audit_router)
    _install_what_if_request_schema(app)
    return app


def _install_what_if_request_schema(app: FastAPI) -> None:
    """Expose the bounded dependency payload as the same public OpenAPI component."""
    original_openapi = app.openapi

    def openapi() -> dict[str, object]:
        document = original_openapi()
        components = document.setdefault("components", {})
        schemas = components.setdefault("schemas", {})
        for model in (WhatIfRequest, PortfolioScenarioRequest, PortfolioDecisionCreateRequest):
            request_schema = model.model_json_schema(ref_template="#/components/schemas/{model}")
            definitions = request_schema.pop("$defs", {})
            schemas.update(definitions)
            schemas[model.__name__] = request_schema
        return document

    app.openapi = openapi  # type: ignore[method-assign]


def run() -> None:
    """Run the API process."""
    uvicorn.run(create_engine_app(), host="0.0.0.0", port=8000)
