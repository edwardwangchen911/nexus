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



class RecordingTransport:
    def __init__(self) -> None:
        self.calls = 0
        self._delegate = FixtureFosunTransport()

    async def execute(self, operation, payload):
        self.calls += 1
        return await self._delegate.execute(operation, payload)
