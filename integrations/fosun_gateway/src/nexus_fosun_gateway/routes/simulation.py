"""Seven fixed simulation-only operations; no generic provider proxy exists."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Annotated, Protocol, cast
from uuid import UUID

from fastapi import APIRouter, Request
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StringConstraints,
    ValidationError,
)

from nexus_fosun_gateway.domain import GatewayError
from nexus_fosun_gateway.policy.simulation_operations import SimulationOperation

router = APIRouter(prefix="/internal/v1/fosun-simulation")
_MAX_REQUEST_BYTES = 64 * 1024
_MAX_RESPONSE_BYTES = 1024 * 1024
DecimalString = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^(0|[1-9]\d*)(\.\d+)?$", max_length=40),
]
Cursor = Annotated[str, StringConstraints(strict=True, pattern=r"^[0-9]+$", max_length=12)]


class SimulationTransport(Protocol):
    async def execute(
        self,
        operation: SimulationOperation,
        payload: Mapping[str, object],
    ) -> dict[str, object]: ...


class RequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EmptyRequest(RequestModel):
    pass


class CreateOrderRequest(RequestModel):
    command_id: UUID
    account_binding_id: UUID
    intent_order_id: UUID
    idempotency_key: str = Field(min_length=1, max_length=255)
    instrument_code: str = Field(min_length=1, max_length=32)
    side: Annotated[str, StringConstraints(pattern=r"^(buy|sell)$")]
    quantity: DecimalString
    limit_price: DecimalString


class ModifyOrderRequest(RequestModel):
    command_id: UUID
    account_binding_id: UUID
    order_binding_id: UUID
    idempotency_key: str = Field(min_length=1, max_length=255)
    quantity: DecimalString
    limit_price: DecimalString


class CancelOrderRequest(RequestModel):
    command_id: UUID
    account_binding_id: UUID
    order_binding_id: UUID
    idempotency_key: str = Field(min_length=1, max_length=255)


class OrdersRequest(RequestModel):
    account_binding_id: UUID
    cursor: Cursor | None = None
    limit: StrictInt = Field(default=100, ge=1, le=100)


class PortfolioRequest(RequestModel):
    account_binding_id: UUID


async def _execute(
    request: Request,
    operation: SimulationOperation,
    request_type: type[RequestModel],
) -> dict[str, object]:
    if not _is_json_media_type(request.headers.get("content-type")):
        raise GatewayError(
            "UNSUPPORTED_MEDIA_TYPE",
            "Request media type is invalid.",
            status_code=415,
            next_action="USE_JSON",
        )
    content_length = request.headers.get("content-length")
    if content_length is not None and (
        not content_length.isascii()
        or not content_length.isdecimal()
        or int(content_length) > _MAX_REQUEST_BYTES
    ):
        raise _request_too_large()
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > _MAX_REQUEST_BYTES:
            raise _request_too_large()
        chunks.append(chunk)
    body = b"".join(chunks)
    actor = await request.app.state.hmac_verifier.verify(request, body)
    request.state.verified_gateway_actor = actor
    try:
        decoded = json.loads(body)
        if not isinstance(decoded, dict):
            raise ValueError
        payload = request_type.model_validate(decoded)
    except (json.JSONDecodeError, ValueError, ValidationError) as error:
        raise GatewayError(
            "VALIDATION_ERROR",
            "Request payload is invalid.",
            status_code=422,
            next_action="FIX_REQUEST",
        ) from error
    transport = cast("SimulationTransport", request.app.state.simulation_transport)
    try:
        response = await transport.execute(operation, payload.model_dump(mode="json"))
        encoded = json.dumps(response, separators=(",", ":")).encode()
    except (TypeError, ValueError) as error:
        raise GatewayError(
            "SIMULATION_CONTRACT_ERROR",
            "Simulation fixture contract failed.",
            status_code=502,
            next_action="REVIEW_FIXTURE_CONTRACT",
        ) from error
    if len(encoded) > _MAX_RESPONSE_BYTES:
        raise GatewayError(
            "RESPONSE_TOO_LARGE",
            "Simulation response exceeded the fixed limit.",
            status_code=502,
            next_action="REDUCE_RESPONSE",
        )
    return response


def _is_json_media_type(value: str | None) -> bool:
    if value is None:
        return False
    parts = [part.strip().lower() for part in value.split(";")]
    return (
        parts[0] == "application/json"
        and all(part == "charset=utf-8" for part in parts[1:])
        and len(parts) <= 2
    )


def _request_too_large() -> GatewayError:
    return GatewayError(
        "REQUEST_TOO_LARGE",
        "Request payload is too large.",
        status_code=413,
        next_action="REDUCE_REQUEST",
    )


async def capabilities(request: Request) -> dict[str, object]:
    return await _execute(request, "fosun_simulation_capabilities", EmptyRequest)


async def create_order(request: Request) -> dict[str, object]:
    return await _execute(request, "fosun_simulation_create_order", CreateOrderRequest)


async def modify_order(request: Request) -> dict[str, object]:
    return await _execute(request, "fosun_simulation_modify_order", ModifyOrderRequest)


async def cancel_order(request: Request) -> dict[str, object]:
    return await _execute(request, "fosun_simulation_cancel_order", CancelOrderRequest)


async def list_orders(request: Request) -> dict[str, object]:
    return await _execute(request, "fosun_simulation_list_orders", OrdersRequest)


async def sync_portfolio(request: Request) -> dict[str, object]:
    return await _execute(request, "fosun_simulation_sync_portfolio", PortfolioRequest)


async def sync_cash_flows(request: Request) -> dict[str, object]:
    return await _execute(request, "fosun_simulation_sync_cash_flows", OrdersRequest)


router.add_api_route("/capabilities", capabilities, methods=["POST"])
router.add_api_route("/create-order", create_order, methods=["POST"])
router.add_api_route("/modify-order", modify_order, methods=["POST"])
router.add_api_route("/cancel-order", cancel_order, methods=["POST"])
router.add_api_route("/orders", list_orders, methods=["POST"])
router.add_api_route("/portfolio", sync_portfolio, methods=["POST"])
router.add_api_route("/cash-flows", sync_cash_flows, methods=["POST"])
