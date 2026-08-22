"""OpenAPI integration-status contract tests."""

from datetime import datetime

import pytest
from nexus.api.schemas.integrations import IntegrationStatusResponse
from nexus.main import create_engine_app
from nexus.settings import EngineSettings
from pydantic import ValidationError


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
        "NexusErrorEnvelope",
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
    assert schemas["IntegrationStatusResponse"]["properties"]["version"] == {
        "minimum": 0,
        "title": "Version",
        "type": "integer",
    }
    assert schemas["IntegrationStatusResponse"]["additionalProperties"] is False
    assert document.get("servers") is None
    operation_ids = [
        operation["operationId"] for path in paths.values() for operation in path.values()
    ]
    assert len(operation_ids) == len(set(operation_ids))
    assert "HTTPValidationError" not in schemas
    assert "ValidationError" not in schemas

    for path, statuses in {
        "/internal/v1/integrations": ("401", "422"),
        "/internal/v1/integrations/{provider}/test": ("401", "403", "422", "503"),
        "/internal/v1/integrations/{provider}/mode": ("401", "403", "409", "422", "503"),
    }.items():
        operation = next(iter(paths[path].values()))
        for status in statuses:
            assert operation["responses"][status]["content"]["application/json"]["schema"] == {
                "$ref": "#/components/schemas/NexusErrorEnvelope"
            }

    property_names = {
        property_name.lower()
        for schema in schemas.values()
        if isinstance(schema, dict)
        for property_name in schema.get("properties", {})
    }
    assert not any(
        forbidden in property_name
        for property_name in property_names
        for forbidden in (
            "password",
            "secret",
            "credential",
            "endpoint",
            "access_key",
            "token",
            "private_key",
            "base_url",
            "account_id",
        )
    )


def test_integration_status_rejects_naive_wire_timestamps() -> None:
    """A schema accepting naïve as-of values would violate the API time contract."""
    with pytest.raises(ValidationError, match="timezone-aware"):
        IntegrationStatusResponse(
            provider="fosun",
            mode="fixture",
            quality="PASS",
            as_of=datetime(2026, 8, 22),
            capability_expires_at=None,
            contract_hash=None,
            next_action=None,
            version=0,
        )
