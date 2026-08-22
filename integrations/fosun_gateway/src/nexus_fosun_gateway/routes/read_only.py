"""Seven explicit read-only routes; no generic provider proxy exists."""

import json
from datetime import date, datetime
from typing import Annotated, Literal, cast

from fastapi import APIRouter, Request
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StringConstraints,
    ValidationError,
    field_validator,
    model_validator,
)

from nexus_fosun_gateway.adapter.protocol import FosunTransport
from nexus_fosun_gateway.domain import GatewayError, GatewayItem, GatewayPage
from nexus_fosun_gateway.policy.operations import GatewayOperation

router = APIRouter(prefix="/internal/v1/read-only")
Cursor = Annotated[str, StringConstraints(pattern=r"^[0-9]+$", max_length=12)]


class RequestModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EmptyRequest(RequestModel):
    pass


class AccountPageRequest(RequestModel):
    account_id: str
    cursor: Cursor | None = None
    limit: Annotated[int, Field(ge=1, le=100)] = 100


class PortfolioReadRequest(AccountPageRequest):
    read_kind: Literal["cash", "holdings"]

    @model_validator(mode="after")
    def cash_does_not_accept_a_pagination_cursor(self) -> "PortfolioReadRequest":
        if self.read_kind == "cash" and self.cursor is not None:
            raise ValueError("cash summary is not paginated")
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
    body = await request.body()
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
router.add_api_route(
    "/fosun_sync_accounts", fosun_sync_accounts, methods=["POST"]
)
router.add_api_route(
    "/fosun_sync_portfolio", fosun_sync_portfolio, methods=["POST"]
)
router.add_api_route(
    "/fosun_list_orders", fosun_list_orders, methods=["POST"]
)
router.add_api_route(
    "/fosun_list_cash_flows", fosun_list_cash_flows, methods=["POST"]
)
router.add_api_route(
    "/fosun_get_quotes", fosun_get_quotes, methods=["POST"]
)
router.add_api_route("/fosun_get_bars", fosun_get_bars, methods=["POST"])
