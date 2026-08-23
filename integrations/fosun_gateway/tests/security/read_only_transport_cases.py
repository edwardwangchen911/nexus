"""Authentication, transport-boundary, replay, and request-ID cases."""

import base64
import hashlib
import hmac
import time

import pytest
from fastapi.testclient import TestClient
from nexus_fosun_gateway.main import create_gateway_app
from nexus_fosun_gateway.security.hmac import GatewayHmacVerifier, ReplayCache, canonical_hmac_v1
from nexus_fosun_gateway.settings import GatewaySettings
from pydantic import ValidationError
from read_only_helpers import (
    PREVIOUS_KEY_ID,
    PREVIOUS_SECRET,
    REQUEST_ID,
    RecordingTransport,
    _post_signed,
    _signed_headers,
)


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
