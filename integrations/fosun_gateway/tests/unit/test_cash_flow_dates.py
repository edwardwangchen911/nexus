"""Business dates do not imply an undocumented provider timestamp zone."""

from datetime import UTC, date, datetime

import pytest
from nexus_fosun_gateway.adapter.normalization import normalize_provider_response
from nexus_fosun_gateway.domain import GatewayError


def response(**changes):
    row = {
        "flowId": 1001,
        "currency": "USD",
        "amount": "12.50",
        "createdAt": "2026-08-24 01:30:00",
        "tradeDate": "2026-08-21",
        "flowType": 100,
        "direction": 2,
    }
    row.update(changes)
    return {
        "code": 0,
        "requestId": "synthetic-flows",
        "data": {
            "start": 0,
            "count": 1,
            "total": 1,
            "list": [row],
        },
    }


def normalize(raw):
    return normalize_provider_response(
        "fosun_list_cash_flows",
        raw,
        received_at=datetime(2026, 8, 24, tzinfo=UTC),
        context={"cursor": None, "limit": 100},
    )


def test_date_only_flow_retains_business_date_without_fabricating_event_time():
    page = normalize(response())
    item = page.items[0]
    assert item.provider_business_date == date(2026, 8, 21)
    assert item.occurred_at is None
    assert item.direction == 2
    assert item.flow_type == "100"
    assert page.quality == "PASS"


def test_aware_created_time_and_distinct_trade_date_are_both_preserved():
    item = normalize(response(createdAt="2026-08-24T01:30:00+08:00")).items[0]
    assert item.occurred_at == datetime(2026, 8, 23, 17, 30, tzinfo=UTC)
    assert item.provider_business_date == date(2026, 8, 21)


@pytest.mark.parametrize(
    "changes",
    [
        {"tradeDate": None},
        {"tradeDate": "2026-02-30"},
        {"tradeDate": "2026-08-21T00:00:00Z"},
        {"createdAt": "invalid"},
        {"direction": 0},
        {"direction": True},
        {"flowType": True},
    ],
)
def test_invalid_cash_flow_semantics_still_fail_closed(changes):
    with pytest.raises(GatewayError) as error:
        normalize(response(**changes))
    assert error.value.code == "PROVIDER_MALFORMED_RESPONSE"
