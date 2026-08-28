import json
from datetime import UTC, datetime

import httpx
import pytest
from helpers.broker_simulation import preparation, seed_broker_authority
from helpers.hmac import (
    TEST_HMAC_KEY_ID,
    TEST_HMAC_SECRET,
    build_test_hmac_headers,
)
from nexus.broker_simulation.intents import prepare_execution_intent
from nexus.domain.ids import new_uuid7
from nexus.main import create_engine_app
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings

_NOW = datetime(2026, 8, 28, 14, 1, tzinfo=UTC)
_BASE = "/internal/v1/broker-simulation"


def _settings(database_url: str) -> EngineSettings:
    return EngineSettings(
        NEXUS_ENV="test",
        DATABASE_URL=database_url,
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID=TEST_HMAC_KEY_ID,
        NEXUS_ENGINE_HMAC_CURRENT_SECRET=TEST_HMAC_SECRET,
    )


def _body(payload: dict[str, object]) -> bytes:
    return json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()


def _headers(
    method: str,
    path: str,
    body: bytes = b"",
    *,
    idempotency_key: str | None = None,
    if_match: str | None = None,
) -> dict[str, str]:
    headers = build_test_hmac_headers(method, path, body, nonce=str(new_uuid7()))
    if body:
        headers["Content-Type"] = "application/json"
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    if if_match is not None:
        headers["If-Match"] = if_match
    return headers


def test_broker_simulation_openapi_is_exact_and_closed() -> None:
    document = create_engine_app(EngineSettings.for_test()).openapi()
    paths = {
        path for path in document["paths"] if path.startswith("/internal/v1/broker-simulation")
    }

    assert paths == {
        _BASE,
        f"{_BASE}/intents",
        f"{_BASE}/intents/{{intent_id}}/approve",
        f"{_BASE}/intents/{{intent_id}}/reject",
        f"{_BASE}/orders/{{order_id}}/modify",
        f"{_BASE}/orders/{{order_id}}/cancel",
        f"{_BASE}/reconciliation/{{run_id}}/acknowledge",
    }
    for name in (
        "BrokerSimulationResponse",
        "BrokerIntentResponse",
        "BrokerActionResponse",
    ):
        assert document["components"]["schemas"][name]["additionalProperties"] is False


@pytest.mark.asyncio
async def test_all_broker_simulation_routes_require_authenticated_service_actor(
    migrated_database_url: str,
) -> None:
    app = create_engine_app(_settings(migrated_database_url))
    item_id = "00000000-0000-0000-0000-000000000001"
    requests = (
        ("GET", _BASE, None),
        ("POST", f"{_BASE}/intents", {"proposal_group_id": item_id, "proposal_id": item_id}),
        ("POST", f"{_BASE}/intents/{item_id}/approve", {"version": 1}),
        ("POST", f"{_BASE}/intents/{item_id}/reject", {"version": 1, "reason": "user_rejected"}),
        (
            "POST",
            f"{_BASE}/orders/{item_id}/modify",
            {"expected_event_sequence": 0, "reason": "replace_price"},
        ),
        (
            "POST",
            f"{_BASE}/orders/{item_id}/cancel",
            {"expected_event_sequence": 0, "reason": "user_cancel"},
        ),
        ("POST", f"{_BASE}/reconciliation/{item_id}/acknowledge", {}),
    )
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            responses = []
            for method, path, payload in requests:
                responses.append(
                    await client.request(method, path, json=payload)
                    if payload is not None
                    else await client.get(path)
                )
        assert {response.status_code for response in responses} == {401}
    finally:
        await app.state.session_factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_offline_view_is_redacted_and_approval_is_idempotent_without_dispatch(
    migrated_database_url: str,
) -> None:
    await seed_broker_authority(migrated_database_url)
    factory = session_factory(_settings(migrated_database_url))
    authority = preparation()
    async with factory() as session, session.begin():
        prepared = await prepare_execution_intent(session, authority)
    app = create_engine_app(_settings(migrated_database_url))
    app.state.utc_clock = lambda: _NOW
    approve_path = f"{_BASE}/intents/{prepared.intent_id}/approve"
    approve_body = _body({"version": prepared.version})
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            view = await client.get(_BASE, headers=_headers("GET", _BASE))
            stale = await client.post(
                approve_path,
                content=approve_body,
                headers=_headers(
                    "POST",
                    approve_path,
                    approve_body,
                    idempotency_key="approve-stale",
                    if_match="0" * 64,
                ),
            )
            first = await client.post(
                approve_path,
                content=approve_body,
                headers=_headers(
                    "POST",
                    approve_path,
                    approve_body,
                    idempotency_key="approve-offline-1",
                    if_match=prepared.body_hash,
                ),
            )
            replay = await client.post(
                approve_path,
                content=approve_body,
                headers=_headers(
                    "POST",
                    approve_path,
                    approve_body,
                    idempotency_key="approve-offline-1",
                    if_match=prepared.body_hash,
                ),
            )
        assert view.status_code == 200
        assert view.json()["data"]["capability"] == {
            "status": "offline_ready",
            "external_activation": "external_activation_blocked",
            "live_trading": False,
            "simulation_write_enabled": False,
        }
        assert view.json()["data"]["account"]["redacted_label"] == "Fixture SIM"
        assert "external_account" not in view.text
        assert stale.status_code == 409
        assert stale.json()["error"]["code"] == "VERSION_CONFLICT"
        assert first.status_code == replay.status_code == 200
        assert first.json()["data"]["status"] == "approved"
        assert replay.json()["data"]["replayed"] is True
        assert first.json()["data"]["dispatch_created"] is False
    finally:
        await app.state.session_factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_external_order_actions_are_blocked_server_side_before_lookup(
    migrated_database_url: str,
) -> None:
    app = create_engine_app(_settings(migrated_database_url))
    order_id = "00000000-0000-0000-0000-000000000001"
    cases = (
        ("modify", {"expected_event_sequence": 0, "reason": "replace_price"}),
        ("cancel", {"expected_event_sequence": 0, "reason": "user_cancel"}),
    )
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            for operation, payload in cases:
                path = f"{_BASE}/orders/{order_id}/{operation}"
                body = _body(payload)
                response = await client.post(
                    path,
                    content=body,
                    headers=_headers(
                        "POST",
                        path,
                        body,
                        idempotency_key=f"blocked-{operation}",
                        if_match='"0"',
                    ),
                )
                assert response.status_code == 409
                assert response.json()["error"]["code"] == "SIMULATION_WRITE_DISABLED"
                assert order_id not in response.text
    finally:
        await app.state.session_factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_prepare_body_is_bounded_and_rejects_order_authority_fields(
    migrated_database_url: str,
) -> None:
    app = create_engine_app(_settings(migrated_database_url))
    path = f"{_BASE}/intents"
    forbidden = _body(
        {
            "proposal_group_id": "00000000-0000-0000-0000-000000000001",
            "proposal_id": "00000000-0000-0000-0000-000000000002",
            "quantity": 100,
        }
    )
    oversized = b'{"padding":"' + (b"a" * (64 * 1024)) + b'"}'
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://testserver"
        ) as client:
            invalid = await client.post(
                path,
                content=forbidden,
                headers=_headers("POST", path, forbidden, idempotency_key="forbidden-order"),
            )
            too_large = await client.post(
                path,
                content=oversized,
                headers=_headers("POST", path, oversized, idempotency_key="oversized"),
            )
        assert invalid.status_code == too_large.status_code == 422
    finally:
        await app.state.session_factory.kw["bind"].dispose()
