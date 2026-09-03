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


@pytest.mark.parametrize("business_type", [None, 0, -1, 987654, 2**63 - 1])
def test_cash_flow_context_is_opaque_bounded_and_has_no_free_text(business_type):
    page = normalize(
        response(
            businessType=business_type,
            productCode="usSYNTH.A",
            remark="synthetic private memo",
            description="synthetic personal description",
        )
    )
    row = page.model_dump(mode="json")["items"][0]
    assert row["business_type"] == business_type
    assert row["product_code"] == "usSYNTH.A"
    assert row["occurred_at"] is None
    assert row["provider_business_date"] == "2026-08-21"
    assert "private" not in str(row) and "personal" not in str(row)
    assert "remark" not in row and "description" not in row


@pytest.mark.parametrize(
    "changes", [{}, {"businessType": None, "productCode": None}, {"productCode": ""}]
)
def test_missing_cash_flow_context_stays_unknown(changes):
    row = normalize(response(**changes)).items[0]
    assert row.business_type is None
    assert row.product_code is None


@pytest.mark.parametrize(
    "changes",
    [
        {"businessType": True},
        {"businessType": "123"},
        {"businessType": 1.0},
        {"businessType": 2**63},
        {"businessType": -(2**63) - 1},
        {"productCode": 123},
        {"productCode": "A" * 129},
        {"productCode": "synthetic private memo"},
        {"productCode": "usSYNTH\n"},
        {"productCode": " "},
        {"productCode": {"remark": "synthetic private memo"}},
    ],
)
def test_invalid_cash_flow_context_fails_without_leaking_value(changes):
    with pytest.raises(GatewayError) as error:
        normalize(response(**changes))
    assert error.value.code == "PROVIDER_MALFORMED_RESPONSE"
    assert "private" not in str(error.value)


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
