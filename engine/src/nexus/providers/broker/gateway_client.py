"""Fixed-origin mTLS/HMAC client for the isolated read-only Fosun gateway."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from collections.abc import Callable
from datetime import date
from typing import Literal
from uuid import UUID

import httpx

from nexus.domain.ids import new_uuid7
from nexus.providers.broker.gateway_contracts import (
    MAX_RESPONSE_BYTES,
    GatewayClientError,
    GatewayResponsePage,
    is_exact_json_media_type,
    validate_account_id,
    validate_content_length,
    validate_pagination,
)
from nexus.providers.broker.settings import BrokerWorkerSettings

GatewayOperation = Literal[
    "fosun_sync_portfolio",
    "fosun_list_orders",
    "fosun_list_cash_flows",
]
_PATHS: dict[GatewayOperation, str] = {
    "fosun_sync_portfolio": "/internal/v1/read-only/fosun_sync_portfolio",
    "fosun_list_orders": "/internal/v1/read-only/fosun_list_orders",
    "fosun_list_cash_flows": "/internal/v1/read-only/fosun_list_cash_flows",
}
class FosunGatewayClient:
    """Expose only broker reads with compile-time operation paths and payloads."""

    def __init__(
        self,
        settings: BrokerWorkerSettings,
        *,
        client: httpx.AsyncClient | None = None,
        wall_time: Callable[[], float] = time.time,
        uuid_factory: Callable[[], UUID] = new_uuid7,
    ) -> None:
        if (
            settings.gateway_origin is None
            or settings.gateway_hmac_key_id is None
            or settings.gateway_hmac_secret is None
            or settings.client_cert is None
            or settings.client_key is None
            or settings.client_ca is None
        ):
            raise ValueError("READ_ONLY_GATEWAY_IDENTITY_REQUIRED")
        self._origin = _validated_origin(settings.gateway_origin)
        self._key_id = settings.gateway_hmac_key_id
        self._secret = settings.gateway_hmac_secret.get_secret_value()
        self._wall_time = wall_time
        self._uuid_factory = uuid_factory
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(
            cert=(str(settings.client_cert), str(settings.client_key)),
            verify=str(settings.client_ca),
            timeout=httpx.Timeout(10.0),
            follow_redirects=False,
            trust_env=False,
        )

    async def read_cash(self, external_account_id: str) -> GatewayResponsePage:
        validate_account_id(external_account_id)
        return await self._post(
            "fosun_sync_portfolio",
            {"account_id": external_account_id, "read_kind": "cash"},
        )

    async def read_holdings(
        self,
        external_account_id: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> GatewayResponsePage:
        validate_account_id(external_account_id)
        validate_pagination(cursor, limit)
        return await self._post(
            "fosun_sync_portfolio",
            {
                "account_id": external_account_id,
                "read_kind": "holdings",
                "cursor": cursor,
                "limit": limit,
            },
        )

    async def read_orders(
        self,
        external_account_id: str,
        *,
        cursor: str | None,
        limit: int,
    ) -> GatewayResponsePage:
        validate_account_id(external_account_id)
        validate_pagination(cursor, limit)
        return await self._post(
            "fosun_list_orders",
            {"account_id": external_account_id, "cursor": cursor, "limit": limit},
        )

    async def read_cash_flows(
        self,
        external_account_id: str,
        *,
        date_from: date,
        date_to: date,
        cursor: str | None,
        limit: int,
    ) -> GatewayResponsePage:
        validate_account_id(external_account_id)
        validate_pagination(cursor, limit)
        if date_to < date_from:
            raise GatewayClientError("gateway date window rejected")
        return await self._post(
            "fosun_list_cash_flows",
            {
                "account_id": external_account_id,
                "cursor": cursor,
                "limit": limit,
                "date_from": date_from.isoformat(),
                "date_to": date_to.isoformat(),
            },
        )

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def _post(
        self,
        operation: GatewayOperation,
        payload: dict[str, object],
    ) -> GatewayResponsePage:
        path = _PATHS[operation]
        body = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        timestamp = int(self._wall_time())
        nonce = self._uuid_factory()
        request_id = self._uuid_factory()
        if nonce.version != 7 or request_id.version != 7:
            raise GatewayClientError("gateway request identity generation failed")
        body_hash = hashlib.sha256(body).hexdigest()
        canonical = "\n".join(("POST", path, str(timestamp), str(nonce), body_hash))
        signature = base64.urlsafe_b64encode(
            hmac.new(
                self._secret.encode("utf-8"),
                canonical.encode("utf-8"),
                hashlib.sha256,
            ).digest()
        ).rstrip(b"=")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Nexus-Key-Id": self._key_id,
            "X-Nexus-Timestamp": str(timestamp),
            "X-Nexus-Nonce": str(nonce),
            "X-Nexus-Body-SHA256": body_hash,
            "X-Nexus-Signature": signature.decode("ascii"),
            "X-Nexus-Actor-Id": "nexus-broker-worker",
            "X-Request-Id": str(request_id),
        }
        try:
            async with self._client.stream(
                "POST",
                f"{self._origin}{path}",
                content=body,
                headers=headers,
                follow_redirects=False,
            ) as response:
                if response.status_code != 200 or not is_exact_json_media_type(
                    response.headers.get("content-type")
                ):
                    raise GatewayClientError("gateway response rejected")
                validate_content_length(response.headers.get("content-length"))
                chunks: list[bytes] = []
                size = 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > MAX_RESPONSE_BYTES:
                        raise GatewayClientError("gateway response rejected")
                    chunks.append(chunk)
                raw_response = b"".join(chunks)
        except httpx.HTTPError:
            raise GatewayClientError("gateway request failed") from None
        try:
            return GatewayResponsePage.model_validate(json.loads(raw_response))
        except (ValueError, json.JSONDecodeError):
            raise GatewayClientError("gateway response contract rejected") from None


def _validated_origin(value: str) -> str:
    try:
        url = httpx.URL(value)
    except ValueError:
        raise ValueError("gateway origin must be an absolute HTTPS origin") from None
    if (
        url.scheme != "https"
        or not url.host
        or bool(url.username)
        or bool(url.password)
        or url.raw_path not in (b"", b"/")
        or url.query
        or url.fragment
    ):
        raise ValueError("gateway origin must be an absolute HTTPS origin")
    return str(url).rstrip("/")
