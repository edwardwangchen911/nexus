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
from nexus_fosun_gateway.settings import GatewaySettings

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
MARKET_CURRENT_KEY_ID = "fixture-market-gateway-current"
MARKET_CURRENT_SECRET = "fixture-market-gateway-current-secret-32-bytes"
MARKET_PREVIOUS_KEY_ID = "fixture-market-gateway-previous"
MARKET_PREVIOUS_SECRET = "fixture-market-gateway-previous-secret-32-bytes"
NONCE = "0198f7a0-2222-7777-8222-222222222221"
REQUEST_ID = "0198f7a0-2222-7777-8222-222222222222"


@pytest.fixture
def gateway_client() -> Iterator[TestClient]:
    with TestClient(create_gateway_app(GatewaySettings.for_test())) as client:
        yield client


def _signed_headers(
    path: str,
    body: bytes,
    *,
    key_id: str = CURRENT_KEY_ID,
    secret: str = CURRENT_SECRET,
    nonce: str = NONCE,
    timestamp: int | None = None,
    signature: str | None = None,
    actor_id: str = "nexus-broker-worker",
    request_id: str = REQUEST_ID,
    signature_version: str = "hmac-v2",
) -> dict[str, str]:
    issued_at = int(time.time()) if timestamp is None else timestamp
    body_hash = hashlib.sha256(body).hexdigest()
    canonical = "\n".join(
        (
            signature_version,
            "POST",
            path,
            str(issued_at),
            nonce,
            actor_id,
            request_id,
            body_hash,
        )
    )
    calculated = (
        base64.urlsafe_b64encode(
            hmac.new(secret.encode(), canonical.encode(), hashlib.sha256).digest()
        )
        .rstrip(b"=")
        .decode()
    )
    return {
        "Content-Type": "application/json",
        "X-Nexus-Signature-Version": signature_version,
        "X-Nexus-Key-Id": key_id,
        "X-Nexus-Timestamp": str(issued_at),
        "X-Nexus-Nonce": nonce,
        "X-Nexus-Body-SHA256": body_hash,
        "X-Nexus-Signature": signature or calculated,
        "X-Nexus-Actor-Id": actor_id,
        "X-Request-Id": request_id,
    }


def _post_signed(
    client: TestClient,
    operation: str,
    payload: dict[str, object],
    **auth: object,
):
    path = f"/internal/v1/read-only/{operation}"
    body = json.dumps(payload, separators=(",", ":")).encode()
    if not ({"key_id", "secret", "actor_id"} & auth.keys()):
        if operation in {"fosun_get_quotes", "fosun_get_bars"}:
            auth.update(
                key_id=MARKET_CURRENT_KEY_ID,
                secret=MARKET_CURRENT_SECRET,
                actor_id="nexus-provider-worker",
            )
    return client.post(path, content=body, headers=_signed_headers(path, body, **auth))


class RecordingTransport:
    def __init__(self) -> None:
        self.calls = 0
        self._delegate = FixtureFosunTransport()

    async def execute(self, operation, payload):
        self.calls += 1
        return await self._delegate.execute(operation, payload)

    def is_ready(self) -> bool:
        return self._delegate.is_ready()

    async def aclose(self) -> None:
        await self._delegate.aclose()
