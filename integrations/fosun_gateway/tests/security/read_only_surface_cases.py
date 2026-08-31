"""Reusable read-only gateway cases collected by smaller domain modules."""

import pytest
from fastapi.testclient import TestClient
from nexus_fosun_gateway.routes.read_only import router as read_only_router
from read_only_helpers import _post_signed

READ_ONLY_OPERATIONS = (
    "fosun_capabilities",
    "fosun_sync_accounts",
    "fosun_sync_portfolio",
    "fosun_list_orders",
    "fosun_list_cash_flows",
    "fosun_get_quotes",
    "fosun_get_bars",
)
def test_gateway_registers_exactly_seven_authenticated_read_only_operations(
    gateway_client: TestClient,
) -> None:
    """Removing a fixed route or exposing it without authentication breaks the seam."""
    for operation in READ_ONLY_OPERATIONS:
        response = gateway_client.post(f"/internal/v1/read-only/{operation}", json={})
        assert response.status_code == 401

    registered = {
        path.removeprefix("/internal/v1/read-only/")
        for path in gateway_client.app.openapi()["paths"]
        if path.startswith("/internal/v1/read-only/")
    }
    assert registered == set(READ_ONLY_OPERATIONS)
    expected_paths = {
        *(f"/internal/v1/read-only/{operation}" for operation in READ_ONLY_OPERATIONS),
        "/health/live",
        "/health/ready",
    }
    assert set(gateway_client.app.openapi()["paths"]) == expected_paths

    included_routers = [
        route
        for route in gateway_client.app.routes
        if getattr(route, "original_router", None) is not None
    ]
    assert len(included_routers) == 1
    assert included_routers[0].original_router is read_only_router
    assert {
        (route.path, frozenset(route.methods))
        for route in gateway_client.app.routes
        if getattr(route, "path", None) is not None
    } == {
        ("/health/live", frozenset({"GET"})),
        ("/health/ready", frozenset({"GET"})),
    }




@pytest.mark.parametrize(
    ("override", "nonce"),
    [
        ("url", "0198f7a0-2222-7777-8222-222222222231"),
        ("endpoint", "0198f7a0-2222-7777-8222-222222222232"),
        ("sdk_method", "0198f7a0-2222-7777-8222-222222222233"),
        ("account_type", "0198f7a0-2222-7777-8222-222222222234"),
        ("capability", "0198f7a0-2222-7777-8222-222222222235"),
    ],
)
def test_authority_overrides_are_rejected(
    gateway_client: TestClient,
    override: str,
    nonce: str,
) -> None:
    """Caller-controlled authority must not turn a fixed operation into a proxy."""
    response = _post_signed(
        gateway_client,
        "fosun_get_quotes",
        {"codes": ["usAAPL"], override: "synthetic-override"},
        nonce=nonce,
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


def test_portfolio_authority_override_is_rejected_independently(
    gateway_client: TestClient,
) -> None:
    response = _post_signed(
        gateway_client,
        "fosun_sync_portfolio",
        {
            "account_id": "synthetic-account-001",
            "read_kind": "cash",
            "url": "https://provider.invalid",
        },
        nonce="0198f7a0-2222-7777-8222-222222222236",
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


@pytest.mark.parametrize(
    ("operation", "payload", "nonce"),
    [
        (
            "fosun_list_cash_flows",
            {
                "account_id": "synthetic-account-001",
                "date_from": "not-a-date",
                "date_to": "2026-08-22",
            },
            "0198f7a0-2222-7777-8222-222222222241",
        ),
        (
            "fosun_get_bars",
            {
                "code": "usAAPL",
                "ktype": "day",
                "start_time": "2026-08-01T00:00:00",
                "end_time": "2026-08-22T00:00:00Z",
            },
            "0198f7a0-2222-7777-8222-222222222242",
        ),
        (
            "fosun_list_orders",
            {
                "account_id": "synthetic-account-001",
                "cursor": "not-an-offset",
                "limit": 20,
            },
            "0198f7a0-2222-7777-8222-222222222243",
        ),
        (
            "fosun_sync_portfolio",
            {
                "account_id": "synthetic-account-001",
                "cursor": "0",
                "limit": 20,
            },
            "0198f7a0-2222-7777-8222-222222222244",
        ),
    ],
)
def test_typed_parameters_reject_invalid_temporal_values_and_cursors(
    gateway_client: TestClient,
    operation: str,
    payload: dict[str, object],
    nonce: str,
) -> None:
    """Invalid temporal inputs must fail before reaching a provider method."""
    response = _post_signed(gateway_client, operation, payload, nonce=nonce)

    assert response.status_code == 422
    assert response.json()["error"]["next_action"] == "FIX_REQUEST"


@pytest.mark.parametrize(
    ("payload", "nonce"),
    [
        (
            {"account_id": "synthetic-account-001", "read_kind": "cash", "cursor": None},
            "0198f7a0-2222-7777-8222-222222222261",
        ),
        (
            {"account_id": "synthetic-account-001", "read_kind": "cash", "limit": 20},
            "0198f7a0-2222-7777-8222-222222222262",
        ),
        (
            {"account_id": "synthetic-account-001", "read_kind": "holdings", "limit": 20},
            "0198f7a0-2222-7777-8222-222222222263",
        ),
        (
            {
                "account_id": "synthetic-account-001",
                "read_kind": "holdings",
                "cursor": None,
            },
            "0198f7a0-2222-7777-8222-222222222264",
        ),
        (
            {
                "account_id": "synthetic-account-001",
                "read_kind": "holdings",
                "cursor": None,
                "limit": None,
            },
            "0198f7a0-2222-7777-8222-222222222265",
        ),
        (
            {
                "account_id": "synthetic-account-001",
                "read_kind": "holdings",
                "cursor": None,
                "limit": True,
            },
            "0198f7a0-2222-7777-8222-222222222266",
        ),
        (
            {
                "account_id": "synthetic-account-001",
                "read_kind": "holdings",
                "cursor": None,
                "limit": "20",
            },
            "0198f7a0-2222-7777-8222-222222222267",
        ),
        (
            {
                "account_id": "synthetic-account-001",
                "read_kind": "holdings",
                "cursor": None,
                "limit": 0,
            },
            "0198f7a0-2222-7777-8222-222222222268",
        ),
        (
            {
                "account_id": "synthetic-account-001",
                "read_kind": "holdings",
                "cursor": None,
                "limit": 101,
            },
            "0198f7a0-2222-7777-8222-222222222269",
        ),
        (
            {
                "account_id": "synthetic-account-001",
                "read_kind": "holdings",
                "cursor": True,
                "limit": 20,
            },
            "0198f7a0-2222-7777-8222-222222222270",
        ),
        (
            {
                "account_id": "synthetic-account-001",
                "read_kind": "holdings",
                "cursor": "1" * 13,
                "limit": 20,
            },
            "0198f7a0-2222-7777-8222-222222222271",
        ),
    ],
)
def test_portfolio_read_kind_has_strict_disjoint_payloads(
    gateway_client: TestClient,
    payload: dict[str, object],
    nonce: str,
) -> None:
    response = _post_signed(
        gateway_client,
        "fosun_sync_portfolio",
        payload,
        nonce=nonce,
    )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


def test_portfolio_cash_and_first_holdings_page_accept_only_their_valid_shapes(
    gateway_client: TestClient,
) -> None:
    cash = _post_signed(
        gateway_client,
        "fosun_sync_portfolio",
        {"account_id": "synthetic-account-001", "read_kind": "cash"},
        nonce="0198f7a0-2222-7777-8222-222222222272",
    )
    holdings = _post_signed(
        gateway_client,
        "fosun_sync_portfolio",
        {
            "account_id": "synthetic-account-001",
            "read_kind": "holdings",
            "cursor": None,
            "limit": 100,
        },
        nonce="0198f7a0-2222-7777-8222-222222222273",
    )

    assert cash.status_code == 200
    assert holdings.status_code == 200


@pytest.mark.parametrize(
    "path",
    [
        "/internal/v1/OrderCreate",
        "/internal/v1/OrderModify",
        "/internal/v1/OrderCancel",
        "/internal/v1/SimAccountCreate",
        "/internal/v1/SimAccountReset",
        "/internal/v1/SubscriptionCreate",
        "/internal/v1/SubscriptionUpdate",
        "/internal/v1/SubscriptionDelete",
        "/webhooks/fosun",
    ],
)
def test_read_only_image_has_no_write_or_webhook_route(
    gateway_client: TestClient,
    path: str,
) -> None:
    """A forbidden capability must remain absent and therefore return 404, not 403."""
    response = gateway_client.post(path, json={})

    assert response.status_code == 404
    assert response.json() == {
        "error": {
            "code": "NOT_FOUND",
            "message": "Route not found.",
            "next_action": "USE_READ_ONLY_OPERATION",
        },
        "request_id": None,
    }
