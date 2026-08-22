"""OpenAPI integration-status contract tests."""

from nexus.main import create_engine_app
from nexus.settings import EngineSettings


def test_openapi_exposes_only_redacted_integration_contracts() -> None:
    """Removing a route/schema or exposing configuration data must fail this test."""
    document = create_engine_app(EngineSettings.for_test()).openapi()

    paths = document["paths"]
    assert paths["/internal/v1/integrations"]["get"]["operationId"] == "listIntegrationStatuses"
    assert (
        paths["/internal/v1/integrations/{provider}/test"]["post"]["operationId"]
        == "testIntegrationConnectivity"
    )
    assert (
        paths["/internal/v1/integrations/{provider}/mode"]["post"]["operationId"]
        == "changeIntegrationMode"
    )

    schemas = document["components"]["schemas"]
    for name in (
        "IntegrationStatusResponse",
        "IntegrationModeChangeRequest",
        "IntegrationConnectivityTestRequest",
        "IntegrationConnectivityTestResponse",
        "IntegrationModeChangeResponse",
        "IntegrationStatusListEnvelope",
        "SuccessMetaResponse",
    ):
        assert name in schemas
    assert schemas["IntegrationStatusResponse"]["properties"]["provider"]["enum"] == [
        "fosun",
        "alpaca",
        "sec",
        "fred",
        "openai",
    ]
    assert schemas["IntegrationStatusResponse"]["properties"]["mode"]["enum"] == [
        "disabled",
        "fixture",
        "read_only",
    ]
    assert schemas["IntegrationStatusResponse"]["additionalProperties"] is False

    serialized = str(document).lower()
    assert document.get("servers") is None
    assert not any(
        forbidden in serialized
        for forbidden in ("password", "secret", "credential", "endpoint", "access_key")
    )
