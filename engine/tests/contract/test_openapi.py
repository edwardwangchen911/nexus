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
    assert paths["/internal/v1/instruments/{instrument_id}/news"]["get"]["responses"][
        "422"
    ]["content"]["application/json"]["schema"] == {
        "$ref": "#/components/schemas/NexusErrorEnvelope"
    }
    delivery_mode_schema = {
        "enum": ["real_time", "delayed", "fixture", "unknown"],
        "type": "string",
    }
    market_mode = schemas["MarketItemResponse"]["properties"]["delivery_mode"]
    assert {key: market_mode[key] for key in ("enum", "type")} == delivery_mode_schema
    series_mode = schemas["SelectedMarketSeriesResponse"]["properties"]["delivery_mode"]
    assert series_mode["anyOf"] == [delivery_mode_schema, {"type": "null"}]

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

    integration_paths = {
        path: paths[path]
        for path in (
            "/internal/v1/integrations",
            "/internal/v1/integrations/{provider}/test",
            "/internal/v1/integrations/{provider}/mode",
        )
    }
    integration_contract = {
        "paths": integration_paths,
        "schemas": _referenced_schema_contract(roots=integration_paths, schemas=schemas),
    }
    assert "NexusErrorResponse" in integration_contract["schemas"]
    public_contract_words = {
        word.lower() for word in _walk_openapi_strings(integration_contract)
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


def test_openapi_exposes_distinct_closed_shadow_start_and_read_contracts() -> None:
    """Shadow observation must not expose an imperative advance or trade surface."""
    document = create_engine_app(EngineSettings.for_test()).openapi()
    path = "/internal/v1/optimization-proposals/{group_id}/shadow"
    operations = document["paths"][path]

    assert set(operations) == {"post", "get"}
    assert operations["post"]["responses"]["200"]["content"]["application/json"][
        "schema"
    ] == {"$ref": "#/components/schemas/ShadowStartEnvelope"}
    assert operations["get"]["responses"]["200"]["content"]["application/json"][
        "schema"
    ] == {"$ref": "#/components/schemas/ShadowGroupEnvelope"}
    assert "requestBody" in operations["post"]
    assert "requestBody" not in operations["get"]
    get_headers = {
        parameter["name"]
        for parameter in operations["get"]["parameters"]
        if parameter["in"] == "header"
    }
    assert "Idempotency-Key" not in get_headers
    assert "If-Match" not in get_headers

    schemas = document["components"]["schemas"]
    shadow_schemas = {
        name: schema for name, schema in schemas.items() if name.startswith("Shadow")
    }
    assert {
        "ShadowStartData",
        "ShadowStartEnvelope",
        "ShadowGroupResponse",
        "ShadowGroupEnvelope",
    } <= set(shadow_schemas)
    assert all(
        schema.get("type") != "object" or schema.get("additionalProperties") is False
        for schema in shadow_schemas.values()
    )
    assert not any(
        forbidden in candidate
        for candidate in document["paths"]
        for forbidden in ("/advance", "/checkpoint", "/validate", "/promote")
    )
    wire = str(shadow_schemas).lower()
    assert "order_id" not in wire
    assert "trade_id" not in wire


def test_openapi_exposes_closed_strategy_evolution_surface() -> None:
    document = create_engine_app(EngineSettings.for_test()).openapi()
    paths = document["paths"]
    expected = {
        "/internal/v1/strategy-evolution": {"get"},
        "/internal/v1/strategy-experiments/{experiment_id}": {"get"},
        "/internal/v1/strategy-experiments": {"post"},
        "/internal/v1/strategy-experiments/{experiment_id}/veto": {"post"},
        "/internal/v1/strategy-registry/control": {"post"},
        "/internal/v1/strategy-registry/rollback": {"post"},
    }
    assert {path: set(paths[path]) for path in expected} == expected
    for path, methods in expected.items():
        for method in methods:
            operation = paths[path][method]
            if method == "post":
                headers = {
                    item["name"]
                    for item in operation["parameters"]
                    if item["in"] == "header"
                }
                assert {"Idempotency-Key", "If-Match"} <= headers
                if path in {
                    "/internal/v1/strategy-experiments",
                    "/internal/v1/strategy-registry/rollback",
                }:
                    assert "Location" in operation["responses"]["202"]["headers"]
    schemas = document["components"]["schemas"]
    assert {
        "StrategyEvolutionEnvelope",
        "ExperimentDetailEnvelope",
        "EvolutionActionEnvelope",
        "StartExperimentRequest",
        "VetoExperimentRequest",
        "RegistryControlRequest",
        "RollbackRequest",
    } <= set(schemas)
    start = schemas["StartExperimentRequest"]
    assert start["additionalProperties"] is False
    assert start.get("properties", {}) == {}
    public = str(
        {
            key: schemas[key]
            for key in schemas
            if any(
                token in key.lower()
                for token in ("evolution", "experiment", "champion", "registry", "shadowprogress")
            )
        }
    ).lower()
    assert not any(
        forbidden in public
        for forbidden in (
            "prompt_version_id",
            "model_config_id",
            "candidate_delta",
            "strategy_document",
            "account_id",
            "order_id",
            "credential",
            "api_key",
        )
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


def _referenced_schema_contract(
    *, roots: object, schemas: dict[str, object]
) -> dict[str, object]:
    """Resolve every component schema reachable from the selected operations."""
    reference_prefix = "#/components/schemas/"
    selected: dict[str, object] = {}
    pending = _walk_openapi_strings(roots)
    while pending:
        reference = pending.pop()
        if not reference.startswith(reference_prefix):
            continue
        name = reference.removeprefix(reference_prefix)
        if name in selected:
            continue
        schema = schemas[name]
        selected[name] = schema
        pending.extend(_walk_openapi_strings(schema))
    return selected
