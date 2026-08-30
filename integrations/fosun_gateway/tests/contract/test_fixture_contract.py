import json
from pathlib import Path

import pytest
from nexus_fosun_gateway.adapter.fixture import FixtureFosunTransport
from nexus_fosun_gateway.domain import GatewayError

FIXTURE_DIR = Path(__file__).parents[1] / "fixtures" / "fosun"
MANIFEST_PATH = Path(__file__).parents[4] / "contracts" / "providers" / "fosun" / "manifest.json"
DOCKERFILE_PATH = Path(__file__).parents[4] / "infra" / "docker" / "fosun-gateway.Dockerfile"

EXPECTED_FIXTURES = {
    "success.json",
    "empty.json",
    "pagination.json",
    "partial_fields.json",
    "price_power.json",
    "permission_error.json",
    "rate_limit.json",
    "malformed_response.json",
}


def test_provider_manifest_pins_reviewed_public_contracts_and_sdk_source() -> None:
    """Changing any reviewed source hash must require an explicit manifest review."""
    manifest = json.loads(MANIFEST_PATH.read_text())

    assert manifest == {
        "provider": "fosun",
        "reviewed_at": "2026-08-22",
        "admission": {
            "status": "blocked",
            "provider_base_url": None,
            "sdk_commit": "891d95fb4967efaa682387c1b35966ceea38fb21",
            "openapi_sha256": {
                "market": "f35cfaa3484d8507723729b68a2b8d60fa85d4d28d06991b4a434995cbfac22a",
                "trade": "379561548be045c8e531f31e88ee76c8cbdb7bda5b59e12a9b9d5c9040abd204",
                "push": "15f5416f268c9c47cdcef6a2ff7eaaa89ab4b61f1c9927074969b1e3fef32c05",
            },
            "evidence": {
                "written_permission": None,
                "provider_test_vector": None,
                "security_review": None,
                "tls_verification_remediation": None,
            },
        },
        "sdk": {
            "tag": "v1.2.0",
            "commit": "891d95fb4967efaa682387c1b35966ceea38fb21",
            "license": "UNRESOLVED",
        },
        "openapi": {
            "market": {
                "url": "https://openapi-docs.fosunxcz.com/specs/public/market/market/openapi.yaml",
                "sha256": "f35cfaa3484d8507723729b68a2b8d60fa85d4d28d06991b4a434995cbfac22a",
            },
            "trade": {
                "url": "https://openapi-docs.fosunxcz.com/specs/public/trade/openapi.yaml",
                "sha256": "379561548be045c8e531f31e88ee76c8cbdb7bda5b59e12a9b9d5c9040abd204",
            },
            "push": {
                "url": "https://openapi-docs.fosunxcz.com/specs/public/market/push/openapi.yaml",
                "sha256": "15f5416f268c9c47cdcef6a2ff7eaaa89ab4b61f1c9927074969b1e3fef32c05",
            },
        },
    }


def test_gateway_image_contains_machine_readable_provider_manifest() -> None:
    """Production admission cannot evaluate a reviewed record missing from the image."""
    dockerfile = DOCKERFILE_PATH.read_text()

    assert (
        "COPY contracts/providers/fosun/manifest.json "
        "contracts/providers/fosun/manifest.json"
    ) in dockerfile
    assert "/var/lib/nexus-fosun-gateway" in dockerfile
    assert "chmod 0700" in dockerfile


def test_fixture_corpus_is_synthetic_and_covers_required_contract_cases() -> None:
    """Removing a golden case or adding a non-synthetic external ID weakens offline coverage."""
    assert {path.name for path in FIXTURE_DIR.glob("*.json")} == EXPECTED_FIXTURES
    for path in FIXTURE_DIR.glob("*.json"):
        document = json.loads(path.read_text())
        serialized = json.dumps(document)
        for key in (
            "subAccountId",
            "orderId",
            "flowId",
            "requestId",
            "externalAccountId",
            "externalOrderId",
            "externalCashFlowId",
        ):
            values = _values_for_key(document, key)
            assert all(str(value).startswith("synthetic-") for value in values)
        assert "-----BEGIN" not in serialized


@pytest.mark.asyncio
async def test_success_empty_pagination_and_partial_fixtures_normalize() -> None:
    """Raw provider envelope variations must remain inside the adapter boundary."""
    transport = FixtureFosunTransport(FIXTURE_DIR)

    success = await transport.execute("fosun_sync_accounts", {"fixture_case": "success"})
    empty = await transport.execute("fosun_sync_accounts", {"fixture_case": "empty"})
    pagination = await transport.execute(
        "fosun_sync_portfolio", {"fixture_case": "pagination"}
    )
    partial = await transport.execute("fosun_get_quotes", {"fixture_case": "partial_fields"})

    assert success.items[0].external_account_id == "synthetic-account-001"
    assert empty.items == ()
    assert pagination.has_more is True
    assert pagination.next_cursor == "1"
    assert pagination.items[0].diluted_cost is None
    assert str(pagination.items[0].current_price) == "123.45"
    assert pagination.quality == "PARTIAL"
    assert partial.items[0].price is None
    assert partial.quality == "PARTIAL"
    assert success.as_of.utcoffset().total_seconds() == 0


@pytest.mark.asyncio
async def test_price_power_is_decoded_and_decimal_serializes_as_a_string() -> None:
    """Treating the provider mantissa as a price would overstate it by a power of ten."""
    page = await FixtureFosunTransport(FIXTURE_DIR).execute(
        "fosun_get_quotes", {"fixture_case": "price_power"}
    )

    encoded = json.loads(page.model_dump_json())
    assert encoded["items"][0]["price"] == "123.45"
    assert encoded["items"][0]["as_of"] == "2026-08-22T04:00:00Z"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("fixture_case", "code", "next_action"),
    [
        ("permission_error", "PROVIDER_PERMISSION_DENIED", "REVIEW_PROVIDER_PERMISSION"),
        ("rate_limit", "PROVIDER_RATE_LIMITED", "RETRY_AFTER_DELAY"),
        ("malformed_response", "PROVIDER_MALFORMED_RESPONSE", "REVIEW_PROVIDER_CONTRACT"),
    ],
)
async def test_fixture_errors_are_safe_and_actionable(
    fixture_case: str,
    code: str,
    next_action: str,
) -> None:
    """Provider failures must become stable errors without leaking raw responses."""
    with pytest.raises(GatewayError) as exc_info:
        await FixtureFosunTransport(FIXTURE_DIR).execute(
            "fosun_get_quotes", {"fixture_case": fixture_case}
        )

    assert exc_info.value.code == code
    assert exc_info.value.next_action == next_action
    assert "synthetic-provider-message" not in str(exc_info.value)


def _values_for_key(value: object, key: str) -> list[object]:
    if isinstance(value, dict):
        found = [value[key]] if key in value else []
        for nested in value.values():
            found.extend(_values_for_key(nested, key))
        return found
    if isinstance(value, list):
        found = []
        for nested in value:
            found.extend(_values_for_key(nested, key))
        return found
    return []
