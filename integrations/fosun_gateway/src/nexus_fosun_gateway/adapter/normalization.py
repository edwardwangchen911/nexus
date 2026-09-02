"""Normalize frozen public Fosun response shapes into gateway-owned models."""

from collections.abc import Mapping
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from typing import Literal, cast

from nexus_fosun_gateway.domain import (
    AccountItem,
    BarItem,
    CashFlowItem,
    CashItem,
    GatewayBarPage,
    GatewayError,
    GatewayItem,
    GatewayPage,
    OrderItem,
    PositionItem,
    QuoteItem,
)
from nexus_fosun_gateway.policy.operations import GatewayOperation


class _BarWindowMismatch(ValueError):
    """Internal marker for a valid bar whose date is outside the requested window."""


def normalize_provider_response(
    operation: GatewayOperation,
    raw: object,
    *,
    received_at: datetime,
    context: Mapping[str, object] | None = None,
) -> GatewayPage[GatewayItem]:
    """Normalize one frozen SDK response without returning raw provider data."""
    payload = context or {}
    if operation == "fosun_get_quotes":
        quotes, request_id = _success_list_envelope(raw)
        try:
            items = _quotes(quotes, received_at, payload)
            return _page(items, request_id, received_at, quality="PARTIAL")
        except (KeyError, TypeError, ValueError, InvalidOperation):
            raise _malformed() from None

    data, request_id = _success_envelope(raw)
    try:
        if operation == "fosun_sync_accounts":
            items = _accounts(data)
            return _page(items, request_id, received_at)
        if operation == "fosun_list_orders":
            items = tuple(_order(item) for item in _mappings(_list(data)))
            return _list_page(
                items, data, request_id, received_at, payload, allow_requested_capacity=True
            )
        if operation == "fosun_list_cash_flows":
            rows = _mappings(_list(data))
            items = tuple(_cash_flow(item) for item in rows)
            return _list_page(items, data, request_id, received_at, payload)
        if operation == "fosun_get_bars":
            items = _bars(data, payload, received_at)
            # Preserve the upstream response fact, never infer it from request or age.
            return GatewayBarPage.model_validate(
                {
                    **_page(items, request_id, received_at, quality="PARTIAL").model_dump(),
                    "provider_reported_delay": data.get("delay"),
                }
            )
    except _BarWindowMismatch:
        raise _window_mismatch() from None
    except (KeyError, TypeError, ValueError, InvalidOperation):
        raise _malformed() from None
    raise _malformed()


def normalize_cash_summary_response(
    raw: object,
    *,
    account_id: str,
    received_at: datetime,
) -> GatewayPage[GatewayItem]:
    """Normalize one cash-summary call without reading holdings implicitly."""
    assets, request_id = _success_envelope(raw)
    try:
        breakdown = assets.get("breakdown")
        if not isinstance(breakdown, list):
            raise ValueError
        cash = tuple(_cash(item, account_id) for item in breakdown)
        return GatewayPage[GatewayItem](
            items=cash,
            source_request_id=request_id,
            has_more=False,
            next_cursor=None,
            as_of=received_at,
            quality="PARTIAL",
        )
    except (KeyError, TypeError, ValueError, InvalidOperation):
        raise _malformed() from None


def normalize_holdings_response(
    raw: object,
    *,
    account_id: str,
    received_at: datetime,
    context: Mapping[str, object],
) -> GatewayPage[GatewayItem]:
    """Normalize exactly one holdings page with no hidden cash call."""
    holdings, request_id = _success_envelope(raw)
    try:
        positions = tuple(_position(item, account_id) for item in _mappings(_list(holdings)))
        start, page_span, total = _pagination(
            holdings,
            context,
            len(positions),
            allow_requested_capacity=True,
        )
        next_cursor = str(start + page_span) if start + page_span < total else None
        return GatewayPage[GatewayItem](
            items=positions,
            source_request_id=request_id,
            has_more=next_cursor is not None,
            next_cursor=next_cursor,
            as_of=received_at,
            quality="PARTIAL",
        )
    except (KeyError, TypeError, ValueError, InvalidOperation):
        raise _malformed() from None


def _success_envelope(raw: object) -> tuple[Mapping[str, object], str]:
    data, request_id = _success_envelope_value(raw)
    if not isinstance(data, Mapping):
        raise _malformed()
    return data, request_id


def _success_list_envelope(raw: object) -> tuple[list[object], str]:
    data, request_id = _success_envelope_value(raw)
    if not isinstance(data, list):
        raise _malformed()
    return data, request_id


def _success_envelope_value(raw: object) -> tuple[object, str]:
    if not isinstance(raw, Mapping) or "code" not in raw:
        raise _malformed()
    code = raw.get("code")
    if code != 0:
        if code == 429:
            raise GatewayError(
                "PROVIDER_RATE_LIMITED",
                "Provider rate limit reached.",
                status_code=503,
                next_action="RETRY_AFTER_DELAY",
                retry_after_seconds=_bounded_retry_after(raw.get("retryAfter")),
            )
        if code == 40301:
            raise GatewayError(
                "PROVIDER_PERMISSION_DENIED",
                "Provider permission denied.",
                status_code=502,
                next_action="REVIEW_PROVIDER_PERMISSION",
            )
        raise GatewayError(
            "PROVIDER_ERROR",
            "Provider returned an unsuccessful response.",
            status_code=502,
            next_action="REVIEW_PROVIDER_STATUS",
        )
    data = raw.get("data")
    request_id = raw.get("requestId")
    if not isinstance(request_id, str) or not request_id:
        raise _malformed()
    return data, request_id


def _accounts(data: Mapping[str, object]) -> tuple[GatewayItem, ...]:
    accounts = data.get("subAccounts")
    if not isinstance(accounts, list):
        raise ValueError
    return tuple(
        AccountItem(
            external_account_id=_required_str(item, "subAccountId"),
            status=str(_required_value(item, "status")),
            base_currency=None,
        )
        for item in _mappings(accounts)
    )


def _cash(item: Mapping[str, object], account_id: str) -> CashItem:
    return CashItem(
        external_account_id=account_id,
        currency=_required_str(item, "currency"),
        cash_balance=_decimal(item, "ledgerBalance"),
        available_cash=_decimal(item, "cashPurchasingPower"),
    )


def _position(item: Mapping[str, object], account_id: str) -> PositionItem:
    market = _required_str(item, "marketCode")
    stock = _required_str(item, "stockCode")
    average_cost = item.get("avgCost")
    diluted_cost = item.get("dilutedCost")
    current_price = item.get("price")
    return PositionItem(
        external_account_id=account_id,
        instrument_code=f"{market}{stock}",
        currency=_required_str(item, "currency"),
        quantity=_decimal(item, "quantity"),
        available_quantity=_decimal(item, "quantityAvail"),
        frozen_quantity=_decimal(item, "qtyOnHold"),
        average_cost=Decimal(str(average_cost)) if average_cost is not None else None,
        diluted_cost=Decimal(str(diluted_cost)) if diluted_cost is not None else None,
        current_price=Decimal(str(current_price)) if current_price is not None else None,
        provider_as_of=None,
    )


def _order(item: Mapping[str, object]) -> OrderItem:
    timestamp = _required_value(item, "submittedTimestamp")
    if not isinstance(timestamp, int) or isinstance(timestamp, bool):
        raise ValueError
    return OrderItem(
        external_order_id=str(_required_value(item, "orderId")),
        instrument_code=(f"{_required_str(item, 'marketCode')}{_required_str(item, 'stockCode')}"),
        side=_side(_required_value(item, "direction")),
        quantity=_decimal(item, "quantity"),
        filled_quantity=_decimal(item, "filledQuantity"),
        price=_scaled_optional(item.get("price"), item.get("power")),
        status=str(_required_value(item, "orderStatus")),
        submitted_at=_epoch_datetime(timestamp),
        updated_at=None,
    )


def _cash_flow(item: Mapping[str, object]) -> CashFlowItem:
    trade_date = item.get("tradeDate")
    business_date = None
    if trade_date is not None:
        if not isinstance(trade_date, str) or len(trade_date) != 10:
            raise ValueError
        business_date = date.fromisoformat(trade_date)
        if business_date.isoformat() != trade_date:
            raise ValueError
    created_at = _required_value(item, "createdAt")
    occurred_at = None
    if isinstance(created_at, str) and business_date is not None:
        parsed = datetime.fromisoformat(created_at)
        if parsed.tzinfo is None:
            # Preserve date precision, never attach a guessed timezone to createdAt.
            if parsed.strftime("%Y-%m-%d %H:%M:%S") != created_at:
                raise ValueError
        else:
            occurred_at = _aware_datetime(created_at)
    else:
        occurred_at = _aware_datetime(created_at)
    direction = item.get("direction")
    if direction is not None and (type(direction) is not int or direction not in (1, 2)):
        raise ValueError
    flow_type = _required_value(item, "flowType")
    if type(flow_type) is not int or flow_type < 0:
        raise ValueError
    return CashFlowItem(
        external_cash_flow_id=str(_required_value(item, "flowId")),
        currency=_required_str(item, "currency"),
        amount=_decimal(item, "amount"),
        occurred_at=occurred_at,
        provider_business_date=business_date,
        direction=cast("Literal[1, 2] | None", direction),
        flow_type=str(flow_type),
    )


def _quotes(
    rows: list[object],
    received_at: datetime,
    context: Mapping[str, object],
) -> tuple[GatewayItem, ...]:
    requested = _required_strings(context, "codes")
    by_code: dict[str, Mapping[str, object]] = {}
    for row in _mappings(rows):
        code = f"{_required_str(row, 'mkt')}{_required_str(row, 'rawSymbol')}"
        if code not in requested or code in by_code:
            raise ValueError
        by_code[code] = row
    items: list[GatewayItem] = []
    for code in requested:
        quote = by_code.get(code)
        if quote is None:
            price = None
        else:
            price = _scaled_optional(quote.get("price"), quote.get("power"))
        items.append(QuoteItem(instrument_code=code, price=price, as_of=received_at))
    return tuple(items)


def _bars(
    data: Mapping[str, object],
    context: Mapping[str, object],
    received_at: datetime,
) -> tuple[GatewayItem, ...]:
    rows = data.get("data")
    if not isinstance(rows, list):
        raise ValueError
    total = data.get("total")
    if (
        not isinstance(total, int)
        or isinstance(total, bool)
        or total < 0
        or total != len(rows)
    ):
        raise ValueError
    power = data.get("power")
    code = _required_str(context, "code")
    interval = _required_str(context, "ktype")
    limit = _context_limit(context)
    if len(rows) > limit:
        raise ValueError
    items = tuple(
        BarItem(
            instrument_code=code,
            interval=interval,
            timestamp=_bar_timestamp(_required_value(item, "time")),
            open=_scaled_required(_required_value(item, "open"), power),
            high=_scaled_required(_required_value(item, "high"), power),
            low=_scaled_required(_required_value(item, "low"), power),
            close=_scaled_required(_required_value(item, "close"), power),
            volume=_decimal(item, "vol"),
        )
        for item in _mappings(rows)
    )
    start = _optional_context_datetime(context.get("start_time"))
    end = _optional_context_datetime(context.get("end_time"))
    if (start is None) != (end is None):
        raise ValueError
    previous: datetime | None = None
    for item in items:
        if previous is not None and item.timestamp <= previous:
            raise ValueError
        if item.timestamp > received_at:
            raise ValueError
        if start is not None and end is not None:
            in_window = (
                start.date() <= item.timestamp.date() <= end.date()
                if interval == "day"
                else start <= item.timestamp <= end
            )
            if not in_window:
                raise _BarWindowMismatch
        if not all(
            value.is_finite() and value > 0
            for value in (item.open, item.high, item.low, item.close)
        ):
            raise ValueError
        if not item.volume.is_finite() or item.volume < 0:
            raise ValueError
        if item.low > min(item.open, item.close) or item.high < max(item.open, item.close):
            raise ValueError
        previous = item.timestamp
    return items


def _page(
    items: tuple[GatewayItem, ...],
    request_id: str,
    received_at: datetime,
    *,
    quality: Literal["PASS", "PARTIAL"] = "PASS",
) -> GatewayPage[GatewayItem]:
    return GatewayPage[GatewayItem](
        items=items,
        source_request_id=request_id,
        has_more=False,
        next_cursor=None,
        as_of=received_at,
        quality=quality,
    )


def _list_page(
    items: tuple[GatewayItem, ...],
    data: Mapping[str, object],
    request_id: str,
    received_at: datetime,
    context: Mapping[str, object],
    *,
    allow_requested_capacity: bool = False,
) -> GatewayPage[GatewayItem]:
    start, count, total = _pagination(
        data, context, len(items), allow_requested_capacity=allow_requested_capacity
    )
    next_cursor = str(start + count) if start + count < total else None
    quality: Literal["PASS", "PARTIAL"] = (
        "PARTIAL" if items and isinstance(items[0], OrderItem) else "PASS"
    )
    return GatewayPage[GatewayItem](
        items=items,
        source_request_id=request_id,
        has_more=next_cursor is not None,
        next_cursor=next_cursor,
        as_of=received_at,
        quality=quality,
    )


def _pagination(
    data: Mapping[str, object],
    context: Mapping[str, object],
    item_count: int,
    *,
    allow_requested_capacity: bool = False,
) -> tuple[int, int, int]:
    start = _required_value(data, "start")
    count = _required_value(data, "count")
    total = _required_value(data, "total")
    if not all(
        isinstance(value, int) and not isinstance(value, bool) for value in (start, count, total)
    ):
        raise ValueError
    start, count, total = cast("tuple[int, int, int]", (start, count, total))
    expected_start = _context_cursor(context)
    limit = _context_limit(context)
    if (
        start != expected_start
        or start < 0
        or count < 0
        or total < 0
        or start > total
    ):
        raise ValueError
    returned_count_semantics = (
        count == item_count
        and count <= limit
        and start + count <= total
        and (count != 0 or start == total)
    )
    requested_capacity_semantics = (
        allow_requested_capacity
        and count == limit
        and item_count == min(limit, total - start)
    )
    if not returned_count_semantics and not requested_capacity_semantics:
        raise ValueError
    return start, item_count, total


def _context_cursor(context: Mapping[str, object]) -> int:
    value = context.get("cursor")
    if value is None:
        return 0
    if not isinstance(value, str) or not value.isascii() or not value.isdecimal():
        raise ValueError
    return int(value)


def _context_limit(context: Mapping[str, object]) -> int:
    value = context.get("limit")
    if not isinstance(value, int) or isinstance(value, bool) or value < 1:
        raise ValueError
    return value


def _list(data: Mapping[str, object]) -> list[object]:
    value = data.get("list")
    if "list" in data and value is None and type(data.get("total")) is int and data["total"] == 0:
        # Some successful empty pages use null; pagination still proves the range.
        return []
    if not isinstance(value, list):
        raise ValueError
    return value


def _mappings(values: list[object]) -> tuple[Mapping[str, object], ...]:
    if not all(isinstance(value, Mapping) for value in values):
        raise ValueError
    return tuple(cast("Mapping[str, object]", value) for value in values)


def _required_value(item: Mapping[str, object], name: str) -> object:
    if name not in item or item[name] is None:
        raise ValueError
    return item[name]


def _required_str(item: Mapping[str, object], name: str) -> str:
    value = _required_value(item, name)
    if not isinstance(value, str) or not value:
        raise ValueError
    return value


def _required_strings(item: Mapping[str, object], name: str) -> tuple[str, ...]:
    value = item.get(name)
    if not isinstance(value, tuple | list) or not value:
        raise ValueError
    if not all(isinstance(element, str) and element for element in value):
        raise ValueError
    return tuple(cast("tuple[str, ...] | list[str]", value))


def _optional_context_datetime(value: object) -> datetime | None:
    return None if value is None else _aware_datetime(value)


def _decimal(item: Mapping[str, object], name: str) -> Decimal:
    return Decimal(str(_required_value(item, name)))


def _side(value: object) -> Literal["buy", "sell"]:
    if value == 1:
        return "buy"
    if value == 2:
        return "sell"
    raise ValueError


def _scaled_optional(value: object, power: object) -> Decimal | None:
    if value is None:
        return None
    return _scaled_required(value, power)


def _scaled_required(value: object, power: object) -> Decimal:
    exponent = _power(power)
    return Decimal(str(value)) / (Decimal(10) ** exponent)


def _power(value: object) -> int:
    if isinstance(value, int) and not isinstance(value, bool):
        exponent = value
    elif isinstance(value, str) and value.isascii() and value.isdecimal():
        exponent = int(value)
    else:
        raise ValueError
    if exponent < 0 or exponent > 12:
        raise ValueError
    return exponent


def _aware_datetime(value: object) -> datetime:
    if isinstance(value, int) and not isinstance(value, bool):
        return _epoch_datetime(value)
    if not isinstance(value, str):
        raise ValueError
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError
    return parsed.astimezone(UTC)


def _epoch_datetime(value: int) -> datetime:
    # Bound the two reviewed wire units to contemporary dates so unrelated
    # integers cannot be silently reinterpreted as timestamps.
    seconds: float
    if 946_684_800 <= value <= 4_102_444_800:
        seconds = float(value)
    elif 946_684_800_000 <= value <= 4_102_444_800_000:
        seconds = value / 1000
    else:
        raise ValueError
    return datetime.fromtimestamp(seconds, UTC)


def _bar_timestamp(value: object) -> datetime:
    if isinstance(value, int) and not isinstance(value, bool):
        text = str(value)
        if len(text) == 8:
            return datetime.strptime(text, "%Y%m%d").replace(tzinfo=UTC)
    return _aware_datetime(value)


def _bounded_retry_after(value: object) -> float:
    try:
        return min(max(float(str(value)), 0.0), 10.0)
    except ValueError:
        return 1.0


def _malformed() -> GatewayError:
    return GatewayError(
        "PROVIDER_MALFORMED_RESPONSE",
        "Provider response did not match the reviewed contract.",
        status_code=502,
        next_action="REVIEW_PROVIDER_CONTRACT",
    )


def _window_mismatch() -> GatewayError:
    return GatewayError(
        "PROVIDER_WINDOW_MISMATCH",
        "Provider response did not match the requested time window.",
        status_code=502,
        next_action="REVIEW_PROVIDER_WINDOW",
    )
