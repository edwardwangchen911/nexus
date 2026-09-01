"""Caller identity and operation authorization are independent gateway controls."""

from collections.abc import Iterator

import pytest
from fastapi.testclient import TestClient
from nexus_fosun_gateway.main import create_gateway_app
from nexus_fosun_gateway.security.hmac import GatewayHmacVerifier
from nexus_fosun_gateway.settings import GatewaySettings
from pydantic import ValidationError
from read_only_helpers import (
    CURRENT_KEY_ID,
    CURRENT_SECRET,
    MARKET_CURRENT_KEY_ID,
    MARKET_CURRENT_SECRET,
    MARKET_PREVIOUS_KEY_ID,
    MARKET_PREVIOUS_SECRET,
    PREVIOUS_KEY_ID,
    PREVIOUS_SECRET,
    RecordingTransport,
    _post_signed,
    _signed_headers,
)


@pytest.fixture
def authorized_client() -> Iterator[tuple[TestClient, RecordingTransport]]:
    settings = GatewaySettings.for_test()
    app = create_gateway_app(settings)
    recording = RecordingTransport()
    app.state.transport = recording
    app.state.hmac_verifier = GatewayHmacVerifier(settings)
    with TestClient(app) as client:
        yield client, recording


_OPERATIONS = {
    "fosun_capabilities": {},
    "fosun_sync_accounts": {},
    "fosun_sync_portfolio": {
        "account_id": "synthetic-account-001",
        "read_kind": "cash",
    },
    "fosun_list_orders": {
        "account_id": "synthetic-account-001",
        "cursor": None,
        "limit": 100,
    },
    "fosun_list_cash_flows": {
        "account_id": "synthetic-account-001",
        "cursor": None,
        "limit": 100,
        "date_from": "2026-08-01",
        "date_to": "2026-08-29",
    },
    "fosun_get_quotes": {"codes": ["usAAPL"]},
    "fosun_get_bars": {"code": "usAAPL", "ktype": "day"},
}
_IDENTITIES = (
    (CURRENT_KEY_ID, CURRENT_SECRET, "nexus-broker-worker", "broker"),
    (PREVIOUS_KEY_ID, PREVIOUS_SECRET, "nexus-broker-worker", "broker"),
    (MARKET_CURRENT_KEY_ID, MARKET_CURRENT_SECRET, "nexus-provider-worker", "market"),
    (MARKET_PREVIOUS_KEY_ID, MARKET_PREVIOUS_SECRET, "nexus-provider-worker", "market"),
)
_ALLOWED = {
    "broker": {
        "fosun_capabilities",
        "fosun_sync_accounts",
        "fosun_sync_portfolio",
        "fosun_list_orders",
        "fosun_list_cash_flows",
    },
    "market": {"fosun_capabilities", "fosun_get_quotes", "fosun_get_bars"},
}


@pytest.mark.parametrize(
    ("key_id", "secret", "actor_id", "role", "operation", "payload", "nonce"),
    [
        (
            key_id,
            secret,
            actor_id,
            role,
            operation,
            payload,
            f"0198f7a0-2222-7777-8222-{index:012d}",
        )
        for index, ((key_id, secret, actor_id, role), (operation, payload)) in enumerate(
            (
                (identity, operation)
                for identity in _IDENTITIES
                for operation in _OPERATIONS.items()
            ),
            start=321,
        )
    ],
)
def test_complete_principal_rotation_operation_matrix(
    authorized_client: tuple[TestClient, RecordingTransport],
    key_id: str,
    secret: str,
    actor_id: str,
    role: str,
    operation: str,
    payload: dict[str, object],
    nonce: str,
) -> None:
    client, recording = authorized_client

    response = _post_signed(
        client,
        operation,
        payload,
        key_id=key_id,
        secret=secret,
        actor_id=actor_id,
        nonce=nonce,
    )

    if operation in _ALLOWED[role]:
        assert response.status_code == 200
        assert recording.calls == 1
    else:
        assert response.status_code == 404
        assert response.json()["error"]["code"] == "OPERATION_NOT_ALLOWED"
        assert recording.calls == 0


def test_actor_and_request_id_tampering_invalidates_signature(
    authorized_client: tuple[TestClient, RecordingTransport],
) -> None:
    client, recording = authorized_client
    path = "/internal/v1/read-only/fosun_get_quotes"
    body = b'{"codes":["usAAPL"]}'
    headers = _signed_headers(
        path,
        body,
        key_id=MARKET_CURRENT_KEY_ID,
        secret=MARKET_CURRENT_SECRET,
        actor_id="nexus-provider-worker",
        nonce="0198f7a0-2222-7777-8222-222222222286",
    )
    headers["X-Nexus-Actor-Id"] = "nexus-broker-worker"
    actor_tampered = client.post(path, content=body, headers=headers)

    headers = _signed_headers(
        path,
        body,
        key_id=MARKET_CURRENT_KEY_ID,
        secret=MARKET_CURRENT_SECRET,
        actor_id="nexus-provider-worker",
        nonce="0198f7a0-2222-7777-8222-222222222287",
    )
    headers["X-Request-Id"] = "0198f7a0-2222-7777-8222-222222222288"
    request_tampered = client.post(path, content=body, headers=headers)

    wrong_actor_headers = _signed_headers(
        path,
        body,
        key_id=MARKET_CURRENT_KEY_ID,
        secret=MARKET_CURRENT_SECRET,
        actor_id="nexus-broker-worker",
        nonce="0198f7a0-2222-7777-8222-222222222290",
    )
    wrong_actor_signed = client.post(path, content=body, headers=wrong_actor_headers)

    assert actor_tampered.status_code == 401
    assert request_tampered.status_code == 401
    assert wrong_actor_signed.status_code == 401
    assert recording.calls == 0


def test_listener_port_binds_hmac_identity_to_market_principal(
    authorized_client: tuple[TestClient, RecordingTransport],
) -> None:
    """A broker HMAC must not authenticate on the Provider certificate listener."""
    _, recording = authorized_client
    settings = GatewaySettings.for_test()
    app = create_gateway_app(settings)
    app.state.transport = recording
    app.state.hmac_verifier = GatewayHmacVerifier(settings)

    with TestClient(app, base_url="https://testserver:8082") as market_listener:
        response = _post_signed(
            market_listener,
            "fosun_sync_portfolio",
            {"account_id": "synthetic-account-001", "read_kind": "cash"},
        )

    assert response.status_code == 401
    assert response.json()["error"]["code"] == "UNAUTHENTICATED"
    assert recording.calls == 0


def test_signature_version_is_mandatory(
    authorized_client: tuple[TestClient, RecordingTransport],
) -> None:
    client, recording = authorized_client
    path = "/internal/v1/read-only/fosun_get_quotes"
    body = b'{"codes":["usAAPL"]}'
    headers = _signed_headers(
        path,
        body,
        key_id=MARKET_CURRENT_KEY_ID,
        secret=MARKET_CURRENT_SECRET,
        actor_id="nexus-provider-worker",
        nonce="0198f7a0-2222-7777-8222-222222222289",
    )
    del headers["X-Nexus-Signature-Version"]

    response = client.post(path, content=body, headers=headers)

    v1_headers = _signed_headers(
        path,
        body,
        key_id=MARKET_CURRENT_KEY_ID,
        secret=MARKET_CURRENT_SECRET,
        actor_id="nexus-provider-worker",
        nonce="0198f7a0-2222-7777-8222-222222222299",
        signature_version="hmac-v1",
    )
    v1_response = client.post(path, content=body, headers=v1_headers)

    assert response.status_code == 401
    assert v1_response.status_code == 401
    assert recording.calls == 0


@pytest.mark.parametrize(
    ("target", "source"),
    [
        ("gateway_hmac_previous_key_id", "gateway_hmac_current_key_id"),
        ("market_gateway_hmac_current_key_id", "gateway_hmac_current_key_id"),
        ("market_gateway_hmac_previous_key_id", "market_gateway_hmac_current_key_id"),
        ("gateway_hmac_previous_secret", "gateway_hmac_current_secret"),
        ("market_gateway_hmac_current_secret", "gateway_hmac_current_secret"),
        ("market_gateway_hmac_previous_secret", "market_gateway_hmac_current_secret"),
    ],
)
def test_cross_principal_key_or_secret_reuse_is_rejected(
    target: str,
    source: str,
) -> None:
    values = GatewaySettings.for_test().model_dump()
    values[target] = values[source]

    with pytest.raises(ValidationError):
        GatewaySettings.model_validate(values)
