"""SDK account composition and Provider pagination contracts."""

from copy import deepcopy
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from nexus_fosun_gateway.adapter.normalization import normalize_provider_response
from nexus_fosun_gateway.adapter.sdk import _bar_sdk_arguments
from nexus_fosun_gateway.domain import GatewayBarPage, GatewayError
from sdk_adapter_helpers import client as client
from sdk_adapter_portfolio_cases import (
    test_cash_flow_provider_page_must_match_requested_cursor_and_limit as test_cash_flow_provider_page_must_match_requested_cursor_and_limit,  # noqa: E501
)
from sdk_adapter_portfolio_cases import (
    test_holdings_accepts_provider_count_as_requested_page_capacity as test_holdings_accepts_provider_count_as_requested_page_capacity,  # noqa: E501
)
from sdk_adapter_portfolio_cases import (
    test_holdings_orders_and_cashflow_pages_bind_provider_ranges_to_request as test_holdings_orders_and_cashflow_pages_bind_provider_ranges_to_request,  # noqa: E501
)
from sdk_adapter_portfolio_cases import (
    test_sdk_adapter_normalizes_provider_epoch_seconds_and_milliseconds_to_utc as test_sdk_adapter_normalizes_provider_epoch_seconds_and_milliseconds_to_utc,  # noqa: E501
)
from sdk_adapter_portfolio_cases import (
    test_sdk_adapter_uses_only_frozen_v1_2_read_method_surface as test_sdk_adapter_uses_only_frozen_v1_2_read_method_surface,  # noqa: E501
)


@pytest.mark.parametrize("ktype", ("day", "minute"))
@pytest.mark.parametrize("bounded", (False, True))
def test_bar_request_explicitly_selects_unadjusted_prices(ktype: str, bounded: bool) -> None:
    payload = {"code": "usAAPL", "ktype": ktype, "limit": 100}
    if bounded:
        payload.update(start_time="2026-08-01T00:00:00Z", end_time="2026-08-22T00:00:00Z")

    arguments = _bar_sdk_arguments(payload)

    assert arguments["right"] == "noward"
    assert arguments["num"] == 100
    if bounded and ktype == "day":
        assert arguments["time"] == 20260823
        assert "end_time" not in arguments
    else:
        assert "time" not in arguments
        assert arguments["start_time"] == (1785542400000 if bounded else None)
        assert arguments["end_time"] == (1787356800000 if bounded else None)


def test_legacy_bar_page_cannot_claim_an_explicit_sdk_adjustment() -> None:
    page = GatewayBarPage(
        items=(),
        source_request_id="synthetic-old-bars",
        has_more=False,
        next_cursor=None,
        as_of=datetime(2026, 9, 2, tzinfo=UTC),
        quality="PARTIAL",
    )
    assert page.sdk_requested_adjustment is None


@pytest.mark.parametrize("operation", ("fosun_list_orders", "fosun_list_cash_flows"))
@pytest.mark.parametrize("total,accepted", ((0, True), (1, False)))
def test_null_list_requires_pagination_proof_of_no_records(operation, total, accepted):
    raw = {
        "code": 0,
        "requestId": "synthetic-null-list",
        "data": {"start": 0, "count": 0, "total": total, "list": None},
    }
    kwargs = {
        "received_at": datetime(2026, 9, 2, tzinfo=UTC),
        "context": {"cursor": None, "limit": 100},
    }
    if accepted:
        page = normalize_provider_response(operation, raw, **kwargs)
        assert page.items == ()
        assert page.has_more is False
        assert page.next_cursor is None
    else:
        with pytest.raises(GatewayError) as failure:
            normalize_provider_response(operation, raw, **kwargs)
        assert failure.value.code == "PROVIDER_MALFORMED_RESPONSE"


@pytest.mark.parametrize("returned,total,accepted", ((1, 1, True), (0, 0, True), (1, 2, False)))
def test_orders_capacity_count_requires_complete_page(
    client: SimpleNamespace,  # noqa: F811
    returned: int,
    total: int,
    accepted: bool,
):
    raw = deepcopy(client.trade.list_orders.response)
    raw["data"].update(start=0, count=100, total=total)
    raw["data"]["list"] = raw["data"]["list"][:returned]
    kwargs = {
        "received_at": datetime(2026, 9, 2, tzinfo=UTC),
        "context": {"cursor": None, "limit": 100},
    }
    if accepted:
        page = normalize_provider_response("fosun_list_orders", raw, **kwargs)
        assert len(page.items) == returned
        assert page.has_more is False
    else:
        with pytest.raises(GatewayError) as failure:
            normalize_provider_response("fosun_list_orders", raw, **kwargs)
        assert failure.value.code == "PROVIDER_MALFORMED_RESPONSE"
