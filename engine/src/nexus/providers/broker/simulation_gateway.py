"""Narrow no-retry clients for the isolated broker-simulation Gateway."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Literal, Protocol
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator

from nexus.domain.ids import new_uuid7
from nexus.providers.broker.settings import BrokerWorkerSettings
from nexus.providers.broker.simulation_contracts import (
    MAX_SIMULATION_RESPONSE_BYTES,
    SimulationCancelOrderRequest,
    SimulationCommandResponse,
    SimulationCreateOrderRequest,
    SimulationModifyOrderRequest,
    SimulationPortfolioRequest,
    SimulationQueryRequest,
    SimulationSnapshotPage,
)

GatewayOutcome = Literal["acknowledged", "rejected", "transport_failed", "result_unknown"]


class SimulationGatewayError(RuntimeError):
    """Sanitized, definite Gateway failure."""


class SimulationGatewayUnknownResult(SimulationGatewayError):
    """An attempt may have reached the external boundary and must not be resent."""


class GatewayCommandResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    command_id: UUID
    outcome: GatewayOutcome
    safe_code: str = Field(min_length=1, max_length=64)
    provider_order_digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")
    as_of: datetime

    @field_validator("as_of")
    @classmethod
    def _as_of_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("Gateway result time must be timezone-aware")
        return value.astimezone(UTC)


class SimulationGateway(Protocol):
    async def create_order(self, request: SimulationCreateOrderRequest) -> GatewayCommandResult: ...

    async def modify_order(self, request: SimulationModifyOrderRequest) -> GatewayCommandResult: ...

    async def cancel_order(self, request: SimulationCancelOrderRequest) -> GatewayCommandResult: ...

    async def list_orders(self, request: SimulationQueryRequest) -> SimulationSnapshotPage: ...

    async def portfolio(self, request: SimulationPortfolioRequest) -> SimulationSnapshotPage: ...

    async def cash_flows(self, request: SimulationQueryRequest) -> SimulationSnapshotPage: ...


class FixtureSimulationGatewayClient:
    """In-process deterministic client with no HTTP dependency or credential access."""

    def __init__(self, *, outcomes: tuple[GatewayOutcome, ...]) -> None:
        self._outcomes = outcomes
        self._call_count = 0

    @property
    def call_count(self) -> int:
        return self._call_count

    async def create_order(self, request: SimulationCreateOrderRequest) -> GatewayCommandResult:
        return self._result(request.command_id)

    async def modify_order(self, request: SimulationModifyOrderRequest) -> GatewayCommandResult:
        return self._result(request.command_id)

    async def cancel_order(self, request: SimulationCancelOrderRequest) -> GatewayCommandResult:
        return self._result(request.command_id)

    async def list_orders(self, request: SimulationQueryRequest) -> SimulationSnapshotPage:
        del request
        return _empty_page()

    async def portfolio(self, request: SimulationPortfolioRequest) -> SimulationSnapshotPage:
        del request
        return _empty_page()

    async def cash_flows(self, request: SimulationQueryRequest) -> SimulationSnapshotPage:
        del request
        return _empty_page()

    def _result(self, command_id: UUID) -> GatewayCommandResult:
        if self._call_count >= len(self._outcomes):
            raise SimulationGatewayUnknownResult("fixture outcome exhausted")
        outcome = self._outcomes[self._call_count]
        self._call_count += 1
        return GatewayCommandResult(
            command_id=command_id,
            outcome=outcome,
            safe_code=f"FIXTURE_{outcome.upper()}",
            provider_order_digest=(
                hashlib.sha256(f"synthetic-order-{self._call_count}".encode()).hexdigest()
                if outcome == "acknowledged"
                else None
            ),
            as_of=datetime(2026, 8, 28, 14, tzinfo=UTC),
        )


class SimulationHttpAdmission(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    capability_status: Literal["SIMULATION_CONTRACT_ADMITTED", "SIMULATION_WRITE_ACTIVATED"]
    subject: str = Field(min_length=1, max_length=255)
    valid_until: datetime
    artifact_hash: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("valid_until")
    @classmethod
    def _valid_until_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("admission expiry must be timezone-aware")
        return value.astimezone(UTC)


class HttpSimulationGatewayClient:
    """Fixed-path HTTP client constructible only after external write admission."""

    _PATHS = {
        "create": "/internal/v1/fosun-simulation/create-order",
        "modify": "/internal/v1/fosun-simulation/modify-order",
        "cancel": "/internal/v1/fosun-simulation/cancel-order",
        "orders": "/internal/v1/fosun-simulation/orders",
        "portfolio": "/internal/v1/fosun-simulation/portfolio",
        "cash_flows": "/internal/v1/fosun-simulation/cash-flows",
    }

    def __init__(
        self,
        settings: BrokerWorkerSettings,
        *,
        admission: SimulationHttpAdmission,
        external_id_digester: Callable[[str], str] | None = None,
        client: httpx.AsyncClient | None = None,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        wall_time: Callable[[], float] = time.time,
        uuid_factory: Callable[[], UUID] = new_uuid7,
    ) -> None:
        admitted_at = now()
        if (
            admission.capability_status != "SIMULATION_WRITE_ACTIVATED"
            or admission.valid_until <= admitted_at
        ):
            raise ValueError("SIMULATION_WRITE_NOT_ACTIVATED")
        if external_id_digester is None:
            raise ValueError("SIMULATION_EXTERNAL_ID_DIGESTER_REQUIRED")
        if (
            settings.gateway_origin is None
            or settings.gateway_hmac_key_id is None
            or settings.gateway_hmac_secret is None
            or settings.client_cert is None
            or settings.client_key is None
            or settings.client_ca is None
        ):
            raise ValueError("SIMULATION_GATEWAY_IDENTITY_REQUIRED")
        self._origin = _validated_origin(settings.gateway_origin)
        self._key_id = settings.gateway_hmac_key_id
        self._secret = settings.gateway_hmac_secret.get_secret_value()
        self._digester = external_id_digester
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

    async def create_order(self, request: SimulationCreateOrderRequest) -> GatewayCommandResult:
        return await self._command("create", request)

    async def modify_order(self, request: SimulationModifyOrderRequest) -> GatewayCommandResult:
        return await self._command("modify", request)

    async def cancel_order(self, request: SimulationCancelOrderRequest) -> GatewayCommandResult:
        return await self._command("cancel", request)

    async def list_orders(self, request: SimulationQueryRequest) -> SimulationSnapshotPage:
        return SimulationSnapshotPage.model_validate(await self._post("orders", request))

    async def portfolio(self, request: SimulationPortfolioRequest) -> SimulationSnapshotPage:
        return SimulationSnapshotPage.model_validate(await self._post("portfolio", request))

    async def cash_flows(self, request: SimulationQueryRequest) -> SimulationSnapshotPage:
        return SimulationSnapshotPage.model_validate(await self._post("cash_flows", request))

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def _command(
        self,
        operation: Literal["create", "modify", "cancel"],
        request: SimulationCreateOrderRequest
        | SimulationModifyOrderRequest
        | SimulationCancelOrderRequest,
    ) -> GatewayCommandResult:
        response = SimulationCommandResponse.model_validate(await self._post(operation, request))
        digest = (
            self._digester(response.provider_order_ref)
            if response.provider_order_ref is not None
            else None
        )
        return GatewayCommandResult(
            command_id=response.command_id,
            outcome=response.outcome,
            safe_code=response.safe_code,
            provider_order_digest=digest,
            as_of=response.as_of,
        )

    async def _post(self, operation: str, request: BaseModel) -> dict[str, object]:
        path = self._PATHS[operation]
        body = json.dumps(
            request.model_dump(mode="json"), sort_keys=True, separators=(",", ":")
        ).encode()
        timestamp = int(self._wall_time())
        nonce = self._uuid_factory()
        request_id = self._uuid_factory()
        if nonce.version != 7 or request_id.version != 7:
            raise SimulationGatewayError("gateway identity generation failed")
        body_hash = hashlib.sha256(body).hexdigest()
        actor_id = "nexus-broker-worker"
        canonical = "\n".join(
            (
                "hmac-v2",
                "POST",
                path,
                str(timestamp),
                str(nonce),
                actor_id,
                str(request_id),
                body_hash,
            )
        )
        signature = base64.urlsafe_b64encode(
            hmac.new(self._secret.encode(), canonical.encode(), hashlib.sha256).digest()
        ).rstrip(b"=")
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-Nexus-Signature-Version": "hmac-v2",
            "X-Nexus-Key-Id": self._key_id,
            "X-Nexus-Timestamp": str(timestamp),
            "X-Nexus-Nonce": str(nonce),
            "X-Nexus-Body-SHA256": body_hash,
            "X-Nexus-Signature": signature.decode(),
            "X-Nexus-Actor-Id": actor_id,
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
                if response.status_code != 200:
                    raise SimulationGatewayError("simulation Gateway rejected request")
                chunks: list[bytes] = []
                size = 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > MAX_SIMULATION_RESPONSE_BYTES:
                        raise SimulationGatewayError("simulation Gateway response rejected")
                    chunks.append(chunk)
        except httpx.HTTPError:
            raise SimulationGatewayUnknownResult("simulation Gateway result unknown") from None
        try:
            decoded = json.loads(b"".join(chunks))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise SimulationGatewayUnknownResult("simulation Gateway result unknown") from None
        if not isinstance(decoded, dict):
            raise SimulationGatewayUnknownResult("simulation Gateway result unknown")
        return decoded


def _empty_page() -> SimulationSnapshotPage:
    return SimulationSnapshotPage(
        items=(),
        source_request_id="offline-fixture",
        has_more=False,
        next_cursor=None,
        as_of=datetime(2026, 8, 28, 14, tzinfo=UTC),
        quality="PASS",
    )


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
