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

    for path in (
        "/internal/v1/integrations/{provider}/test",
        "/internal/v1/integrations/{provider}/mode",
    ):
        operation = paths[path]["post"]
        headers = {
            parameter["name"]: parameter
            for parameter in operation["parameters"]
            if parameter["in"] == "header"
        }
        assert headers["Idempotency-Key"]["required"] is True
        assert headers["If-Match"]["required"] is True
        assert "ETag" in operation["responses"]["200"]["headers"]

    public_contract_words = {
        word.lower()
        for word in _walk_openapi_strings(document)
    }
    assert not any(
        forbidden in value
        for value in public_contract_words
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
            "database_url",
            "api_key",
            "authorization",
            "bearer",
            "postgres",
            "localhost",
            "http://",
            "https://",
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


def _walk_openapi_strings(value: object) -> list[str]:
    """Return names and nested values so defaults/examples cannot hide configuration."""
    if isinstance(value, dict):
        return [
            *[str(key) for key in value],
            *[item for child in value.values() for item in _walk_openapi_strings(child)],
        ]
    if isinstance(value, list):
        return [item for child in value for item in _walk_openapi_strings(child)]
    return [value] if isinstance(value, str) else []
