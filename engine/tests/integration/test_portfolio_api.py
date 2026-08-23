import json
from datetime import timedelta
from urllib.parse import urlencode
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import (
    CLOCK as _CLOCK,
)
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
    add_order_only_currency as _add_order_only_currency,
)
from helpers.portfolio_api import (
    add_snapshot_state as _add_snapshot_state,
)
from helpers.portfolio_api import (
    app as _app,
)
from helpers.portfolio_api import (
    broker_sync_artifact_counts as _broker_sync_artifact_counts,
)
from helpers.portfolio_api import (
    headers as _headers,
)
from helpers.portfolio_api import (
    seed_portfolio_list as _seed_portfolio_list,
)
from helpers.portfolio_api import (
    terminalize_broker_sync as _terminalize_broker_sync,
)

_SYNC_PATH = "/internal/v1/broker-sync-runs"


def _sync_body(portfolio_id: UUID, *, padded: bool = False) -> bytes:
    separators = None if padded else (",", ":")
    return json.dumps({"portfolio_id": str(portfolio_id)}, separators=separators).encode()


def _sync_headers(body: bytes, key: str) -> dict[str, str]:
    return {
        **_headers("POST", _SYNC_PATH, body),
        "Content-Type": "application/json",
        "Idempotency-Key": key,
    }


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
        drift_path = "/internal/v1/portfolios?" + urlencode({"kind": "shadow", "cursor": cursor})
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
async def test_portfolio_list_includes_a_currency_present_only_in_open_orders(
    migrated_database_url: str,
) -> None:
    await _seed_portfolio_list(migrated_database_url)
    await _add_order_only_currency(migrated_database_url)
    path = "/internal/v1/portfolios?kind=real_read_only&limit=2"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=_headers("GET", path))

    assert response.status_code == 200
    assert response.json()["data"]["items"][0]["original_currencies"] == ["CNY", "HKD", "USD"]


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
    assert order["symbol"] == "00700.HK"
    assert order["currency"] == "HKD"
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


@pytest.mark.asyncio
async def test_sync_admission_requires_auth_idempotency_and_exact_body(
    migrated_database_url: str,
) -> None:
    body = _sync_body(_PORTFOLIOS[0])
    extra_body = json.dumps(
        {"portfolio_id": str(_PORTFOLIOS[0]), "account_id": "external-canary"}
    ).encode()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        unauthenticated = await client.post(
            _SYNC_PATH,
            content=body,
            headers={"Content-Type": "application/json", "Idempotency-Key": "key-auth"},
        )
        missing_key = await client.post(
            _SYNC_PATH,
            content=body,
            headers={**_headers("POST", _SYNC_PATH, body), "Content-Type": "application/json"},
        )
        extra = await client.post(
            _SYNC_PATH, content=extra_body, headers=_sync_headers(extra_body, "key-extra")
        )
        oversized = await client.post(
            _SYNC_PATH, content=body, headers=_sync_headers(body, "x" * 256)
        )
        malformed_body = b"{"
        malformed = await client.post(
            _SYNC_PATH, content=malformed_body, headers=_sync_headers(malformed_body, "malformed")
        )

    assert unauthenticated.status_code == 401
    assert missing_key.status_code == extra.status_code == 422
    assert oversized.status_code == malformed.status_code == 422
    assert {oversized.json()["error"]["code"], malformed.json()["error"]["code"]} == {
        "VALIDATION_ERROR"
    }


@pytest.mark.asyncio
async def test_sync_admission_replays_and_joins_without_executing_broker_work(
    migrated_database_url: str,
) -> None:
    await _seed_portfolio_list(migrated_database_url)
    body = _sync_body(_PORTFOLIOS[0])
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        first = await client.post(_SYNC_PATH, content=body, headers=_sync_headers(body, "key-1"))
        replay = await client.post(_SYNC_PATH, content=body, headers=_sync_headers(body, "key-1"))
        joined = await client.post(_SYNC_PATH, content=body, headers=_sync_headers(body, "key-2"))

    assert first.status_code == replay.status_code == joined.status_code == 202
    run_id = UUID(first.json()["data"]["broker_sync_run_id"])
    location = f"/api/v1/broker-sync-runs/{run_id}"
    assert first.headers["location"] == replay.headers["location"] == joined.headers["location"]
    assert first.headers["location"] == location
    assert replay.json()["data"]["broker_sync_run_id"] == str(run_id)
    assert joined.json()["data"]["broker_sync_run_id"] == str(run_id)
    assert await _broker_sync_artifact_counts(migrated_database_url, run_id) == (0, 0)
    encoded = json.dumps(first.json())
    for forbidden in ("account_id", "actor_id", "idempotency_key", "request_hash"):
        assert forbidden not in encoded


@pytest.mark.asyncio
async def test_sync_admission_rejects_same_key_when_exact_body_bytes_change(
    migrated_database_url: str,
) -> None:
    await _seed_portfolio_list(migrated_database_url)
    first_body = _sync_body(_PORTFOLIOS[0])
    changed_body = _sync_body(_PORTFOLIOS[0], padded=True)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        first = await client.post(
            _SYNC_PATH, content=first_body, headers=_sync_headers(first_body, "same-key")
        )
        conflict = await client.post(
            _SYNC_PATH, content=changed_body, headers=_sync_headers(changed_body, "same-key")
        )

    assert first.status_code == 202
    assert conflict.status_code == 409
    assert conflict.json()["error"]["code"] == "IDEMPOTENCY_CONFLICT"


@pytest.mark.asyncio
async def test_sync_admission_enforces_stable_successful_manual_cooldown(
    migrated_database_url: str,
) -> None:
    await _seed_portfolio_list(migrated_database_url)
    body = _sync_body(_PORTFOLIOS[0])
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        first = await client.post(_SYNC_PATH, content=body, headers=_sync_headers(body, "first"))
    run_id = UUID(first.json()["data"]["broker_sync_run_id"])
    await _terminalize_broker_sync(
        migrated_database_url, run_id, status="READY", completed_at=_CLOCK
    )

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(
            app=_app(migrated_database_url, now=_CLOCK + timedelta(seconds=30))
        ),
        base_url="http://testserver",
    ) as client:
        cooldown = await client.post(
            _SYNC_PATH, content=body, headers=_sync_headers(body, "second")
        )

    assert cooldown.status_code == 429
    assert cooldown.json()["error"]["code"] == "VALIDATION_ERROR"
    assert cooldown.json()["error"]["details"] == {"cooldown_seconds": 60}
    assert "provider" not in json.dumps(cooldown.json()).lower()


@pytest.mark.asyncio
async def test_sync_run_lookup_is_authenticated_redacted_and_safe_not_found(
    migrated_database_url: str,
) -> None:
    await _seed_portfolio_list(migrated_database_url)
    body = _sync_body(_PORTFOLIOS[0])
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        admitted = await client.post(
            _SYNC_PATH, content=body, headers=_sync_headers(body, "lookup")
        )
    run_id = UUID(admitted.json()["data"]["broker_sync_run_id"])
    await _terminalize_broker_sync(
        migrated_database_url,
        run_id,
        status="FAILED",
        completed_at=_CLOCK,
        failure_reason="provider-secret-canary",
    )
    path = f"{_SYNC_PATH}/{run_id}"
    unknown = f"{_SYNC_PATH}/0198d500-1000-7000-8000-000000000099"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app(migrated_database_url)),
        base_url="http://testserver",
    ) as client:
        unauthenticated = await client.get(path)
        found = await client.get(path, headers=_headers("GET", path))
        missing = await client.get(unknown, headers=_headers("GET", unknown))

    assert unauthenticated.status_code == 401
    assert found.status_code == 200
    assert found.json()["data"]["failure_category"] == "SYNC_FAILED"
    assert missing.status_code == 404
    encoded = json.dumps(found.json())
    for forbidden in ("provider-secret-canary", "account_id", "failure_reason", "request_hash"):
        assert forbidden not in encoded
