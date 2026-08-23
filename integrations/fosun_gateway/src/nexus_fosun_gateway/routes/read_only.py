"""Seven explicit read-only routes; no generic provider proxy exists."""

import json
from datetime import date, datetime
from typing import Annotated, Literal, cast

from fastapi import APIRouter, Request
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictInt,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)

from nexus_fosun_gateway.adapter.protocol import FosunTransport
from nexus_fosun_gateway.domain import GatewayError, GatewayItem, GatewayPage
from nexus_fosun_gateway.policy.operations import GatewayOperation

router = APIRouter(prefix="/internal/v1/read-only")
_MAX_REQUEST_BYTES = 64 * 1024
Cursor = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^[0-9]+$", max_length=12),
]


class RequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EmptyRequest(RequestModel):
    pass


class AccountPageRequest(RequestModel):
    account_id: str
    cursor: Cursor | None = None
    limit: Annotated[int, Field(ge=1, le=100)] = 100


class PortfolioReadRequest(RequestModel):
    account_id: str
    read_kind: Literal["cash", "holdings"]
    cursor: Cursor | None = None
    limit: StrictInt | None = None

    @model_validator(mode="before")
    @classmethod
    def read_kind_owns_its_exact_shape(cls, value: object) -> object:
        if not isinstance(value, dict):
            raise ValueError("portfolio request must be an object")
        read_kind = value.get("read_kind")
        if read_kind == "cash" and ({"cursor", "limit"} & value.keys()):
            raise ValueError("cash summary does not accept pagination")
        if read_kind == "holdings" and not {"cursor", "limit"}.issubset(value):
            raise ValueError("holdings requires explicit cursor and limit")
        return value

    @model_validator(mode="after")
    def holdings_pagination_is_bounded(self) -> "PortfolioReadRequest":
        if self.read_kind == "holdings" and (self.limit is None or not 1 <= self.limit <= 100):
            raise ValueError("holdings limit is outside the fixed range")
        return self


class CashFlowRequest(AccountPageRequest):
    date_from: date
    date_to: date


class QuotesRequest(RequestModel):
    codes: Annotated[tuple[str, ...], Field(min_length=1, max_length=50)]


class BarsRequest(RequestModel):
    code: str
    ktype: Literal["day", "minute"]
    start_time: datetime | None = None
    end_time: datetime | None = None
    limit: Annotated[int, Field(ge=1, le=500)] = 100

    @field_validator("start_time", "end_time")
    @classmethod
    def bar_times_are_timezone_aware(cls, value: datetime | None) -> datetime | None:
        if value is not None and (value.tzinfo is None or value.utcoffset() is None):
            raise ValueError("bar time must be timezone-aware")
        return value


async def _execute(
    request: Request,
    operation: GatewayOperation,
    request_type: type[RequestModel],
) -> GatewayPage[GatewayItem]:
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
    transport = cast("FosunTransport", request.app.state.transport)
    return await transport.execute(operation, payload.model_dump(mode="json"))


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


async def fosun_capabilities(request: Request) -> GatewayPage[GatewayItem]:
    return await _execute(request, "fosun_capabilities", EmptyRequest)


async def fosun_sync_accounts(request: Request) -> GatewayPage[GatewayItem]:
    return await _execute(request, "fosun_sync_accounts", EmptyRequest)


async def fosun_sync_portfolio(request: Request) -> GatewayPage[GatewayItem]:
    return await _execute(request, "fosun_sync_portfolio", PortfolioReadRequest)


async def fosun_list_orders(request: Request) -> GatewayPage[GatewayItem]:
    return await _execute(request, "fosun_list_orders", AccountPageRequest)


async def fosun_list_cash_flows(request: Request) -> GatewayPage[GatewayItem]:
    return await _execute(request, "fosun_list_cash_flows", CashFlowRequest)


async def fosun_get_quotes(request: Request) -> GatewayPage[GatewayItem]:
    return await _execute(request, "fosun_get_quotes", QuotesRequest)


async def fosun_get_bars(request: Request) -> GatewayPage[GatewayItem]:
    return await _execute(request, "fosun_get_bars", BarsRequest)


router.add_api_route("/fosun_capabilities", fosun_capabilities, methods=["POST"])
router.add_api_route("/fosun_sync_accounts", fosun_sync_accounts, methods=["POST"])
router.add_api_route("/fosun_sync_portfolio", fosun_sync_portfolio, methods=["POST"])
router.add_api_route("/fosun_list_orders", fosun_list_orders, methods=["POST"])
router.add_api_route("/fosun_list_cash_flows", fosun_list_cash_flows, methods=["POST"])
router.add_api_route("/fosun_get_quotes", fosun_get_quotes, methods=["POST"])
router.add_api_route("/fosun_get_bars", fosun_get_bars, methods=["POST"])
