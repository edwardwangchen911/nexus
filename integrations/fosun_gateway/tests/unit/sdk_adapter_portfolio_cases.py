"""SDK adapter account composition and Provider pagination cases."""
from copy import deepcopy
from datetime import UTC, datetime
from decimal import Decimal
from types import SimpleNamespace

import pytest
from nexus_fosun_gateway.adapter.sdk import (
    SdkFosunTransport,
)
from nexus_fosun_gateway.domain import GatewayError
from sdk_adapter_helpers import NoopRateLimiter, RecordingMethod, _raw


async def test_sdk_adapter_uses_only_frozen_v1_2_read_method_surface(
    client: SimpleNamespace,
) -> None:
    """Changing an SDK method or its typed arguments breaks the reviewed seam."""
    adapter = SdkFosunTransport(client, rate_limiter=NoopRateLimiter())

    accounts = await adapter.execute("fosun_sync_accounts", {})
    cash = await adapter.execute(
        "fosun_sync_portfolio",
        {
            "account_id": "synthetic-account-001",
            "read_kind": "cash",
        },
    )
    holdings = await adapter.execute(
        "fosun_sync_portfolio",
        {
            "account_id": "synthetic-account-001",
            "read_kind": "holdings",
            "cursor": "20",
            "limit": 20,
        },
    )
    orders = await adapter.execute(
        "fosun_list_orders",
        {"account_id": "synthetic-account-001", "cursor": "40", "limit": 20},
    )
    flows = await adapter.execute(
        "fosun_list_cash_flows",
        {
            "account_id": "synthetic-account-001",
            "date_from": "2026-08-01",
            "date_to": "2026-08-22",
            "cursor": None,
            "limit": 20,
        },
    )
    quotes = await adapter.execute("fosun_get_quotes", {"codes": ("usAAPL",)})
    bars = await adapter.execute(
        "fosun_get_bars",
        {
            "code": "usAAPL",
            "ktype": "day",
            "start_time": "2026-08-01T00:00:00Z",
            "end_time": "2026-08-22T00:00:00Z",
            "limit": 100,
        },
    )

    assert accounts.items[0].external_account_id == "synthetic-account-001"
    assert len(cash.items) == 1
    assert len(holdings.items) == 1
    assert holdings.items[0].average_cost == 100
    assert holdings.items[0].diluted_cost == Decimal("98.50")
    assert holdings.items[0].current_price == Decimal("123.45")
    assert holdings.items[0].provider_as_of is None
    assert holdings.quality == "PARTIAL"
    assert orders.items[0].external_order_id == "synthetic-order-001"
    assert orders.items[0].submitted_at == datetime(2026, 8, 22, 4, 0, tzinfo=UTC)
    assert orders.items[0].updated_at is None
    assert orders.quality == "PARTIAL"
    assert flows.items[0].external_cash_flow_id == "synthetic-cash-flow-001"
    assert str(quotes.items[0].price) == "123.45"
    assert str(bars.items[0].close) == "105"
    assert client.account.list_accounts.calls == [((), {})]
    assert client.portfolio.get_assets_summary.calls == [
        ((), {"sub_account_id": "synthetic-account-001"})
    ]
    assert client.portfolio.get_holdings.calls == [
        (
            (),
            {
                "sub_account_id": "synthetic-account-001",
                "start": 20,
                "count": 20,
            },
        )
    ]
    assert client.trade.list_orders.calls == [
        (
            (),
            {
                "sub_account_id": "synthetic-account-001",
                "start": 40,
                "count": 20,
            },
        )
    ]
    assert client.trade.get_cash_flows.calls == [
        (
            (),
            {
                "sub_account_id": "synthetic-account-001",
                "trade_date_from": "2026-08-01",
                "trade_date_to": "2026-08-22",
            },
        )
    ]
    assert client.market.quote.calls == [((), {"codes": ["usAAPL"]})]
    assert client.market.kline.calls == [
        (
            (),
            {
                "code": "usAAPL",
                "ktype": "day",
                "start_time": 1785542400000,
                "end_time": 1787356800000,
                "num": 100,
            },
        )
    ]
async def test_holdings_orders_and_cashflow_pages_bind_provider_ranges_to_request(
    client: SimpleNamespace,
) -> None:
    holdings_response = deepcopy(client.portfolio.get_holdings.response)
    holdings_response["data"]["start"] = 19
    client.portfolio.get_holdings.response = holdings_response
    adapter = SdkFosunTransport(client, rate_limiter=NoopRateLimiter())
    with pytest.raises(GatewayError) as holdings_error:
        await adapter.execute(
            "fosun_sync_portfolio",
            {
                "account_id": "synthetic-account-001",
                "read_kind": "holdings",
                "cursor": "20",
                "limit": 20,
            },
        )
    assert holdings_error.value.code == "PROVIDER_MALFORMED_RESPONSE"

    orders_response = deepcopy(client.trade.list_orders.response)
    orders_response["data"]["count"] = 2
    client.trade.list_orders.response = orders_response
    with pytest.raises(GatewayError) as orders_error:
        await adapter.execute(
            "fosun_list_orders",
            {"account_id": "synthetic-account-001", "cursor": "40", "limit": 20},
        )
    assert orders_error.value.code == "PROVIDER_MALFORMED_RESPONSE"

    flows_response = deepcopy(client.trade.get_cash_flows.response)
    flows_response["data"] = {"count": 0, "list": [], "start": 0, "total": 1}
    client.trade.get_cash_flows.response = flows_response
    with pytest.raises(GatewayError) as flows_error:
        await adapter.execute(
            "fosun_list_cash_flows",
            {
                "account_id": "synthetic-account-001",
                "date_from": "2026-08-01",
                "date_to": "2026-08-22",
                "cursor": None,
                "limit": 20,
            },
        )
    assert flows_error.value.code == "PROVIDER_MALFORMED_RESPONSE"


@pytest.mark.asyncio
async def test_cash_flow_provider_page_must_match_requested_cursor_and_limit() -> None:
    """A non-pageable SDK response cannot be sliced into invented Provider pages."""
    response = _raw(
        {
            "count": 2,
            "list": [
                {
                    "flowId": "synthetic-cash-flow-001",
                    "currency": "USD",
                    "amount": "100.00",
                    "createdAt": "2026-08-21T04:00:00Z",
                    "flowType": 1,
                },
                {
                    "flowId": "synthetic-cash-flow-002",
                    "currency": "USD",
                    "amount": "200.00",
                    "createdAt": "2026-08-22T04:00:00Z",
                    "flowType": 1,
                },
            ],
            "start": 0,
            "total": 2,
        },
        "synthetic-request-flow-page",
    )
    adapter = SdkFosunTransport(
        SimpleNamespace(trade=SimpleNamespace(get_cash_flows=RecordingMethod(response))),
        rate_limiter=NoopRateLimiter(),
    )

    with pytest.raises(GatewayError) as error:
        await adapter.execute(
            "fosun_list_cash_flows",
            {
                "account_id": "synthetic-account-001",
                "date_from": "2026-08-01",
                "date_to": "2026-08-22",
                "cursor": "1",
                "limit": 1,
            },
        )
    assert error.value.code == "PROVIDER_MALFORMED_RESPONSE"


@pytest.mark.asyncio
async def test_sdk_adapter_normalizes_provider_epoch_seconds_and_milliseconds_to_utc() -> None:
    """Reviewed integer epoch shapes must not leak or become naive timestamps."""
    response = _raw(
        {
            "count": 2,
            "list": [
                {
                    "flowId": "synthetic-cash-flow-seconds",
                    "currency": "USD",
                    "amount": "100.00",
                    "createdAt": 1787356800,
                    "flowType": 1,
                },
                {
                    "flowId": "synthetic-cash-flow-milliseconds",
                    "currency": "USD",
                    "amount": "200.00",
                    "createdAt": 1787356800000,
                    "flowType": 1,
                },
            ],
            "start": 0,
            "total": 2,
        },
        "synthetic-request-epoch-shapes",
    )
    adapter = SdkFosunTransport(
        SimpleNamespace(trade=SimpleNamespace(get_cash_flows=RecordingMethod(response))),
        rate_limiter=NoopRateLimiter(),
    )

    page = await adapter.execute(
        "fosun_list_cash_flows",
        {
            "account_id": "synthetic-account-001",
            "date_from": "2026-08-01",
            "date_to": "2026-08-22",
            "cursor": None,
            "limit": 20,
        },
    )

    assert [item.occurred_at.isoformat() for item in page.items] == [
        "2026-08-22T00:00:00+00:00",
        "2026-08-22T00:00:00+00:00",
    ]
