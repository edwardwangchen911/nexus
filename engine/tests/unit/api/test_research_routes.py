"""Public contract checks for bounded internal research read APIs."""

from nexus.main import create_engine_app
from nexus.settings import EngineSettings


def test_research_routes_are_registered_with_safe_operation_ids() -> None:
    """Removing a route or exposing an execution endpoint would break the Engine API boundary."""
    paths = create_engine_app(EngineSettings.for_test()).openapi()["paths"]
    assert paths["/internal/v1/research-runs"]["post"]["operationId"] == "createResearchRun"
    assert paths["/internal/v1/research-runs/{run_id}"]["get"]["operationId"] == "getResearchRun"
    assert (
        paths["/internal/v1/instruments/{instrument_id}/research"]["get"]["operationId"]
        == "getInstrumentResearch"
    )
    assert paths["/internal/v1/briefs/{session_date}"]["get"]["operationId"] == "getDailyBrief"
