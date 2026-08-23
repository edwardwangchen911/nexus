"""Reusable read-only gateway cases collected by smaller domain modules."""

import base64
import hashlib
import hmac
import json
import time
from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from nexus_fosun_gateway.adapter.fixture import FixtureFosunTransport
from nexus_fosun_gateway.main import create_gateway_app
from nexus_fosun_gateway.security.hmac import (
    GatewayHmacVerifier,
    ReplayCache,
    canonical_hmac_v1,
)
from nexus_fosun_gateway.settings import GatewaySettings
from pydantic import ValidationError

READ_ONLY_OPERATIONS = (
    "fosun_capabilities",
    "fosun_sync_accounts",
    "fosun_sync_portfolio",
    "fosun_list_orders",
    "fosun_list_cash_flows",
    "fosun_get_quotes",
    "fosun_get_bars",
)
CURRENT_KEY_ID = "fixture-gateway-current"
CURRENT_SECRET = "fixture-gateway-current-secret-32-bytes"
PREVIOUS_KEY_ID = "fixture-gateway-previous"
PREVIOUS_SECRET = "fixture-gateway-previous-secret-32-bytes"
NONCE = "0198f7a0-2222-7777-8222-222222222221"
REQUEST_ID = "0198f7a0-2222-7777-8222-222222222222"


@pytest.fixture
def gateway_client() -> Iterator[TestClient]:
    with TestClient(create_gateway_app(GatewaySettings.for_test())) as client:
        yield client


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


def test_gateway_hmac_v1_canonical_vector_matches_engine_semantics() -> None:
    """Query sorting, UTF-8 hashing, or encoding drift would break engine interoperability."""
    body = b'{"codes":["usAAPL"]}'
    nonce = "0198f7a0-2222-7777-8222-222222222299"
    canonical = canonical_hmac_v1(
        "POST",
        "/internal/v1/read-only/fosun_get_quotes?tag=z&tag=a&space=%20",
        1787371200,
        nonce,
        body,
    )
    signature = (
        base64.urlsafe_b64encode(
            hmac.new(
                b"gateway-test-domain-secret",
                canonical.encode(),
                hashlib.sha256,
            ).digest()
        )
        .rstrip(b"=")
        .decode()
    )

    assert canonical == "\n".join(
        (
            "POST",
            "/internal/v1/read-only/fosun_get_quotes?space=%20&tag=a&tag=z",
            "1787371200",
            nonce,
            "0652d23dbda3876a0be6b1d11dd1d1f54f602729dfd48192253fbcca453a7796",
        )
    )
    assert signature == "JRGjN1VT_SLz6hH9BICxPmJ1Fd2hTWmq9w8c22M14LE"


def _signed_headers(
    path: str,
    body: bytes,
    *,
    key_id: str = CURRENT_KEY_ID,
    secret: str = CURRENT_SECRET,
    nonce: str = NONCE,
    timestamp: int | None = None,
    signature: str | None = None,
) -> dict[str, str]:
    issued_at = int(time.time()) if timestamp is None else timestamp
    body_hash = hashlib.sha256(body).hexdigest()
    canonical = "\n".join(("POST", path, str(issued_at), nonce, body_hash))
    calculated = (
        base64.urlsafe_b64encode(
            hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).digest()
        )
        .rstrip(b"=")
        .decode()
    )
    return {
        "Content-Type": "application/json",
        "X-Nexus-Key-Id": key_id,
        "X-Nexus-Timestamp": str(issued_at),
        "X-Nexus-Nonce": nonce,
        "X-Nexus-Body-SHA256": body_hash,
        "X-Nexus-Signature": signature or calculated,
        "X-Nexus-Actor-Id": "nexus-engine",
        "X-Request-Id": REQUEST_ID,
    }


def _post_signed(
    client: TestClient,
    operation: str,
    payload: dict[str, object],
    **auth: object,
):
    path = f"/internal/v1/read-only/{operation}"
    body = json.dumps(payload, separators=(",", ":")).encode()
    return client.post(path, content=body, headers=_signed_headers(path, body, **auth))


def test_current_and_previous_gateway_key_domains_authenticate(
    gateway_client: TestClient,
) -> None:
    """Dropping either configured rotation identity breaks zero-downtime rotation."""
    current = _post_signed(gateway_client, "fosun_capabilities", {})
    previous = _post_signed(
        gateway_client,
        "fosun_capabilities",
        {},
        key_id=PREVIOUS_KEY_ID,
        secret=PREVIOUS_SECRET,
        nonce="0198f7a0-2222-7777-8222-222222222223",
    )

    assert current.status_code == 200
    assert previous.status_code == 200
    assert current.json()["items"][0]["mode"] == "fixture"


def test_read_only_routes_reject_non_json_before_authentication(
    gateway_client: TestClient,
) -> None:
    path = "/internal/v1/read-only/fosun_capabilities"
    body = b"{}"
    headers = _signed_headers(path, body)
    headers["Content-Type"] = "text/plain"

    response = gateway_client.post(path, content=body, headers=headers)

    assert response.status_code == 415
    assert response.json()["error"]["code"] == "UNSUPPORTED_MEDIA_TYPE"


def test_read_only_routes_stream_reject_oversized_body_before_json_parsing(
    gateway_client: TestClient,
) -> None:
    path = "/internal/v1/read-only/fosun_capabilities"
    body = b"x" * (64 * 1024 + 1)
    headers = _signed_headers(
        path,
        body,
        nonce="0198f7a0-2222-7777-8222-222222222224",
    )

    response = gateway_client.post(path, content=body, headers=headers)

    assert response.status_code == 413
    assert response.json()["error"]["code"] == "REQUEST_TOO_LARGE"


@pytest.mark.parametrize(
    ("current_secret", "previous_secret"),
    [
        ("", "fixture-gateway-previous-secret-32-bytes"),
        ("   ", "fixture-gateway-previous-secret-32-bytes"),
        ("synthetic-weak", "fixture-gateway-previous-secret-32-bytes"),
        ("fixture-gateway-current-secret-32-bytes", "   "),
        ("fixture-gateway-current-secret-32-bytes", "synthetic-weak"),
        ("x" * 32, "x" * 32),
    ],
)
def test_invalid_secret_configuration_cannot_initialize_hmac_authentication(
    current_secret: str,
    previous_secret: str,
) -> None:
    """Weak or reused secrets must fail before an authentication verifier can exist."""
    with pytest.raises(ValidationError):
        settings = GatewaySettings.model_validate(
            {
                "NEXUS_ENV": "test",
                "FOSUN_GATEWAY_CAPABILITY": "READ_ONLY",
                "FOSUN_GATEWAY_TRANSPORT_MODE": "fixture",
                "FOSUN_GATEWAY_HMAC_CURRENT_KEY_ID": "gateway-current",
                "FOSUN_GATEWAY_HMAC_CURRENT_SECRET": current_secret,
                "FOSUN_GATEWAY_HMAC_PREVIOUS_KEY_ID": "gateway-previous",
                "FOSUN_GATEWAY_HMAC_PREVIOUS_SECRET": previous_secret,
            }
        )
        GatewayHmacVerifier(settings)


def test_replay_and_expired_signatures_fail_closed(gateway_client: TestClient) -> None:
    """A replay or stale call must never reach the provider transport."""
    first = _post_signed(gateway_client, "fosun_capabilities", {})
    replay = _post_signed(gateway_client, "fosun_capabilities", {})
    expired = _post_signed(
        gateway_client,
        "fosun_capabilities",
        {},
        nonce="0198f7a0-2222-7777-8222-222222222224",
        timestamp=int(time.time()) - 61,
    )

    assert first.status_code == 200
    assert replay.status_code == 401
    assert expired.status_code == 401
    assert replay.json()["error"]["next_action"] == "REAUTHENTICATE"


def test_bad_signature_does_not_consume_nonce(gateway_client: TestClient) -> None:
    """Failing before replay reservation lets a correctly signed retry use its nonce."""
    bad = _post_signed(
        gateway_client,
        "fosun_capabilities",
        {},
        nonce="0198f7a0-2222-7777-8222-222222222225",
        signature="invalid",
    )
    good = _post_signed(
        gateway_client,
        "fosun_capabilities",
        {},
        nonce="0198f7a0-2222-7777-8222-222222222225",
    )

    assert bad.status_code == 401
    assert good.status_code == 200


def test_replay_cache_capacity_rejects_churn_without_evicting_live_nonce() -> None:
    """Capacity pressure must fail closed instead of making a live nonce reusable."""
    settings = GatewaySettings.for_test()
    app = create_gateway_app(settings)
    recording = RecordingTransport()
    app.state.transport = recording
    app.state.hmac_verifier = GatewayHmacVerifier(
        settings,
        replay_cache=ReplayCache(max_entries=2, ttl_seconds=60),
    )

    with TestClient(app) as client:
        first = _post_signed(
            client,
            "fosun_capabilities",
            {},
            nonce="0198f7a0-2222-7777-8222-222222222251",
        )
        second = _post_signed(
            client,
            "fosun_capabilities",
            {},
            nonce="0198f7a0-2222-7777-8222-222222222252",
        )
        saturated = _post_signed(
            client,
            "fosun_capabilities",
            {},
            nonce="0198f7a0-2222-7777-8222-222222222253",
        )
        replay = _post_signed(
            client,
            "fosun_capabilities",
            {},
            nonce="0198f7a0-2222-7777-8222-222222222251",
        )

    assert first.status_code == 200
    assert second.status_code == 200
    assert saturated.status_code == 401
    assert replay.status_code == 401
    assert recording.calls == 2


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


def test_unverified_request_id_header_is_not_reflected(gateway_client: TestClient) -> None:
    """A caller-controlled header must not cross the safe error envelope unchanged."""
    response = gateway_client.post(
        "/internal/v1/not-present",
        headers={"X-Request-Id": "synthetic-sensitive-header-canary"},
        json={},
    )

    assert response.status_code == 404
    assert response.json()["request_id"] is None
    assert "synthetic-sensitive-header-canary" not in response.text


def test_valid_uuid7_is_not_reflected_when_hmac_authentication_fails(
    gateway_client: TestClient,
) -> None:
    """UUID syntax alone does not make an unauthenticated correlation ID trustworthy."""
    response = gateway_client.post(
        "/internal/v1/read-only/fosun_capabilities",
        headers={"X-Request-Id": REQUEST_ID},
        json={},
    )

    assert response.status_code == 401
    assert response.json()["request_id"] is None


def test_valid_uuid7_is_not_reflected_on_absent_route(
    gateway_client: TestClient,
) -> None:
    """An absent route has no authentication step that can verify a caller request ID."""
    response = gateway_client.post(
        "/internal/v1/not-present",
        headers={"X-Request-Id": REQUEST_ID},
        json={},
    )

    assert response.status_code == 404
    assert response.json()["request_id"] is None


def test_verified_request_id_is_reflected_on_post_auth_validation_error(
    gateway_client: TestClient,
) -> None:
    """Once HMAC succeeds, a safe error may correlate to the verified request ID."""
    response = _post_signed(
        gateway_client,
        "fosun_get_quotes",
        {"codes": []},
        nonce="0198f7a0-2222-7777-8222-222222222254",
    )

    assert response.status_code == 422
    assert response.json()["request_id"] == REQUEST_ID


class RecordingTransport:
    def __init__(self) -> None:
        self.calls = 0
        self._delegate = FixtureFosunTransport()

    async def execute(self, operation, payload):
        self.calls += 1
        return await self._delegate.execute(operation, payload)
