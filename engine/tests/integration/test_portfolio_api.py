import json
from urllib.parse import urlencode

import httpx
import pytest
from helpers.portfolio_api import (
    ORDER_ID as _ORDER_ID,
)
from helpers.portfolio_api import (
    PORTFOLIOS as _PORTFOLIOS,
)
from helpers.portfolio_api import (
    add_cash_bucket_overflow as _add_cash_bucket_overflow,
)
from helpers.portfolio_api import (
    add_snapshot_state as _add_snapshot_state,
)
from helpers.portfolio_api import (
    app as _app,
)
from helpers.portfolio_api import (
    headers as _headers,
)
from helpers.portfolio_api import (
    seed_portfolio_list as _seed_portfolio_list,
)


@pytest.mark.asyncio
async def test_portfolio_list_requires_hmac_authentication(migrated_database_url: str) -> None:
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get("/internal/v1/portfolios")

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "UNAUTHENTICATED"


@pytest.mark.asyncio
async def test_portfolio_filters_and_limits_are_bounded(migrated_database_url: str) -> None:
    app = _app(migrated_database_url)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        for query in ("limit=0", "limit=51", "kind=unknown"):
            path = f"/internal/v1/portfolios?{query}"
            response = await client.get(path, headers=_headers("GET", path))
            assert response.status_code == 422
            assert response.json()["error"]["code"] == "VALIDATION_ERROR"
        for kind in ("internal_paper", "shadow"):
            path = f"/internal/v1/portfolios?kind={kind}"
            response = await client.get(path, headers=_headers("GET", path))
            assert response.status_code == 200
            assert response.json()["data"] == {"items": [], "next_cursor": None}


@pytest.mark.asyncio
async def test_portfolio_list_is_stable_paginated_and_redacted(
    migrated_database_url: str,
) -> None:
    await _seed_portfolio_list(migrated_database_url)
    app = _app(migrated_database_url)
    first_path = "/internal/v1/portfolios?kind=real_read_only&limit=2"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://testserver"
    ) as client:
        first = await client.get(first_path, headers=_headers("GET", first_path))
        cursor = first.json()["data"]["next_cursor"]
        second_path = "/internal/v1/portfolios?" + urlencode(
            {"kind": "real_read_only", "limit": 2, "cursor": cursor}
        )
        second = await client.get(second_path, headers=_headers("GET", second_path))
        drift_path = "/internal/v1/portfolios?" + urlencode(
            {"kind": "shadow", "cursor": cursor}
        )
        drift = await client.get(drift_path, headers=_headers("GET", drift_path))
        bad_path = "/internal/v1/portfolios?cursor=not-a-cursor"
        bad = await client.get(bad_path, headers=_headers("GET", bad_path))

    assert first.status_code == second.status_code == 200
    assert [item["portfolio_id"] for item in first.json()["data"]["items"]] == [
        str(_PORTFOLIOS[0]),
        str(_PORTFOLIOS[1]),
    ]
    first_item = first.json()["data"]["items"][0]
    assert first_item["original_currencies"] == ["HKD", "USD"]
    assert first_item["latest_snapshot_status"] == "READY"
    assert first_item["last_sync_run_status"] == "READY"
    assert second.json()["data"]["items"][0]["portfolio_id"] == str(_PORTFOLIOS[2])
    assert drift.status_code == bad.status_code == 422
    encoded = json.dumps(first.json()) + json.dumps(second.json())
    assert "external-request-canary" not in encoded
    assert "secret-canary" not in encoded
    assert "account_id" not in encoded


@pytest.mark.asyncio
async def test_latest_snapshot_preserves_currency_costs_and_read_only_ids(
    migrated_database_url: str,
) -> None:
    await _seed_portfolio_list(migrated_database_url)
    path = f"/internal/v1/portfolios/{_PORTFOLIOS[0]}/snapshots/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=_headers("GET", path))

    assert response.status_code == 200
    data = response.json()["data"]
    assert data["coverage_scope"] == "broker_account"
    assert data["converted_view"] is None
    assert [bucket["currency"] for bucket in data["cash"]] == ["USD"]
    position = data["positions"][0]
    assert position["symbol"] == "00656.HK"
    assert position["currency"] == "HKD"
    assert position["avg_cost"] == "5.250000000000"
    assert position["diluted_cost"] == "4.950000000000"
    assert position["read_only"] is True
    order = data["open_orders"][0]
    assert order["broker_order_snapshot_id"] == str(_ORDER_ID)
    assert order["read_only"] is True
    assert "provider_order_id" not in order
    encoded = json.dumps(response.json())
    for canary in ("provider-order-secret-canary", "external-request-canary", "secret-canary"):
        assert canary not in encoded


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ("READY", "PARTIAL", "STALE", "RECONCILIATION_REQUIRED"))
async def test_latest_snapshot_exposes_every_state_with_nullable_as_of(
    migrated_database_url: str,
    status: str,
) -> None:
    await _seed_portfolio_list(migrated_database_url)
    snapshot_id = await _add_snapshot_state(migrated_database_url, status, as_of=None)
    path = f"/internal/v1/portfolios/{_PORTFOLIOS[1]}/snapshots/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=_headers("GET", path))

    assert response.status_code == 200
    assert response.json()["data"]["portfolio_snapshot_id"] == str(snapshot_id)
    assert response.json()["data"]["status"] == status
    assert response.json()["data"]["as_of"] is None


@pytest.mark.asyncio
async def test_latest_snapshot_requires_auth_and_returns_safe_not_found(
    migrated_database_url: str,
) -> None:
    await _seed_portfolio_list(migrated_database_url)
    missing_snapshot_path = f"/internal/v1/portfolios/{_PORTFOLIOS[1]}/snapshots/latest"
    unknown_path = "/internal/v1/portfolios/0198d500-1000-7000-8000-000000000099/snapshots/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        unauthenticated = await client.get(missing_snapshot_path)
        missing = await client.get(
            missing_snapshot_path, headers=_headers("GET", missing_snapshot_path)
        )
        unknown = await client.get(unknown_path, headers=_headers("GET", unknown_path))

    assert unauthenticated.status_code == 401
    assert missing.status_code == unknown.status_code == 404
    assert missing.json()["error"]["code"] == "NOT_FOUND"


@pytest.mark.asyncio
async def test_latest_snapshot_fails_closed_before_serializing_oversized_collections(
    migrated_database_url: str,
) -> None:
    await _seed_portfolio_list(migrated_database_url)
    await _add_cash_bucket_overflow(migrated_database_url)
    path = f"/internal/v1/portfolios/{_PORTFOLIOS[0]}/snapshots/latest"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=_headers("GET", path))

    assert response.status_code == 409
    assert response.json()["error"]["code"] == "DATA_PARTIAL"
    assert response.json()["error"]["details"] == {"collection": "cash", "maximum": 32}
    assert "data" not in response.json()
