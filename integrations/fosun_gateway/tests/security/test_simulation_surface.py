import base64
import hashlib
import hmac
import inspect
import json
import time
from uuid import UUID

import nexus_fosun_gateway.simulation_main as simulation_main
from fastapi.testclient import TestClient
from nexus_fosun_gateway.adapter.simulation_fixture import FixtureSimulationGatewayScript
from nexus_fosun_gateway.main import create_gateway_app
from nexus_fosun_gateway.settings import GatewaySettings
from nexus_fosun_gateway.simulation_main import create_simulation_gateway_app

_SECRET = "fixture-gateway-current-secret-32-bytes"
_KEY_ID = "fixture-gateway-current"


def _headers(path: str, body: bytes, *, nonce: int = 1) -> dict[str, str]:
    issued_at = int(time.time())
    nonce_value = str(UUID(f"0198f7a0-2222-7777-8222-{nonce:012d}"))
    request_id = "0198f7a0-2222-7777-8222-222222222222"
    body_hash = hashlib.sha256(body).hexdigest()
    canonical = "\n".join(("POST", path, str(issued_at), nonce_value, body_hash))
    signature = (
        base64.urlsafe_b64encode(
            hmac.new(_SECRET.encode(), canonical.encode(), hashlib.sha256).digest()
        )
        .rstrip(b"=")
        .decode()
    )
    return {
        "Content-Type": "application/json",
        "X-Nexus-Key-Id": _KEY_ID,
        "X-Nexus-Timestamp": str(issued_at),
        "X-Nexus-Nonce": nonce_value,
        "X-Nexus-Body-SHA256": body_hash,
        "X-Nexus-Signature": signature,
        "X-Nexus-Actor-Id": "nexus-broker-worker",
        "X-Request-Id": request_id,
    }


def _post(client: TestClient, suffix: str, payload: dict[str, object], nonce: int = 1):
    path = f"/internal/v1/fosun-simulation/{suffix}"
    body = json.dumps(payload, separators=(",", ":")).encode()
    return client.post(path, content=body, headers=_headers(path, body, nonce=nonce))


def test_simulation_and_read_only_composition_roots_are_disjoint() -> None:
    simulation_app = create_simulation_gateway_app(
        GatewaySettings.for_test(), FixtureSimulationGatewayScript(case="acknowledged")
    )
    read_only_app = create_gateway_app(GatewaySettings.for_test())
    simulation_paths = set(simulation_app.openapi()["paths"])
    read_only_paths = set(read_only_app.openapi()["paths"])

    expected = {
        "/internal/v1/fosun-simulation/capabilities",
        "/internal/v1/fosun-simulation/create-order",
        "/internal/v1/fosun-simulation/modify-order",
        "/internal/v1/fosun-simulation/cancel-order",
        "/internal/v1/fosun-simulation/orders",
        "/internal/v1/fosun-simulation/portfolio",
        "/internal/v1/fosun-simulation/cash-flows",
    }
    assert expected <= simulation_paths
    assert not any(path.startswith("/internal/v1/read-only") for path in simulation_paths)
    assert not any(path.startswith("/internal/v1/fosun-simulation") for path in read_only_paths)
    composition_source = inspect.getsource(simulation_main)
    assert "SdkFosunTransport" not in composition_source
    assert "read_only_router" not in composition_source
    assert "fsopenapi" not in composition_source


def test_simulation_surface_rejects_extra_and_generic_proxy_fields() -> None:
    app = create_simulation_gateway_app(
        GatewaySettings.for_test(), FixtureSimulationGatewayScript(case="acknowledged")
    )
    with TestClient(app) as client:
        response = _post(
            client,
            "create-order",
            {
                "command_id": str(UUID(int=1)),
                "account_binding_id": str(UUID(int=2)),
                "intent_order_id": str(UUID(int=3)),
                "idempotency_key": "fixture-create-1",
                "instrument_code": "AAPL.US",
                "side": "buy",
                "quantity": "10",
                "limit_price": "200.00",
                "url": "https://forbidden.invalid",
            },
        )

    assert response.status_code == 422
    assert response.json()["error"]["code"] == "VALIDATION_ERROR"


def test_simulation_surface_requires_hmac_and_enforces_request_limit() -> None:
    app = create_simulation_gateway_app(
        GatewaySettings.for_test(), FixtureSimulationGatewayScript(case="acknowledged")
    )
    with TestClient(app) as client:
        unauthenticated = client.post(
            "/internal/v1/fosun-simulation/capabilities", json={}
        )
        oversized = _post(client, "capabilities", {"padding": "x" * 70_000}, nonce=2)

    assert unauthenticated.status_code == 401
    assert oversized.status_code == 413


def test_fixture_capabilities_are_offline_only() -> None:
    app = create_simulation_gateway_app(
        GatewaySettings.for_test(), FixtureSimulationGatewayScript(case="acknowledged")
    )
    with TestClient(app) as client:
        response = _post(client, "capabilities", {}, nonce=3)

    assert response.status_code == 200
    assert response.json()["mode"] == "offline_fixture"
    assert response.json()["external_activation"] == "external_activation_blocked"


def test_simulation_surface_enforces_response_limit() -> None:
    class OversizedTransport:
        async def execute(self, operation, payload):
            del operation, payload
            return {"oversized": "x" * (1024 * 1024)}

    app = create_simulation_gateway_app(
        GatewaySettings.for_test(), FixtureSimulationGatewayScript(case="acknowledged")
    )
    app.state.simulation_transport = OversizedTransport()
    with TestClient(app) as client:
        response = _post(client, "capabilities", {}, nonce=4)

    assert response.status_code == 502
    assert response.json()["error"]["code"] == "RESPONSE_TOO_LARGE"
