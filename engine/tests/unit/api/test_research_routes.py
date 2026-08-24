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
    instrument_operation = paths["/internal/v1/instruments/{instrument_id}/research"]["get"]
    as_of = next(
        parameter
        for parameter in instrument_operation["parameters"]
        if parameter["name"] == "as_of"
    )
    assert as_of["in"] == "query" and as_of["required"] is True
    schemas = create_engine_app(EngineSettings.for_test()).openapi()["components"]["schemas"]
    brief_properties = schemas["DailyBriefResponse"]["properties"]
    assert "runs" not in brief_properties
    assert set(brief_properties) == {
        "session_date",
        "generated_at",
        "candidates",
        "holding_risks",
        "rejected",
        "budget_exhausted_watch",
        "evidence_completeness",
        "disagreements",
        "invalidations",
        "truncated",
    }
