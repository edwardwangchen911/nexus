"""Synthetic broker pages only; no account captures or external calls."""

from datetime import UTC, datetime, timedelta

import pytest
from nexus.providers.broker.gateway_contracts import GatewayResponsePage

NOW = datetime(2026, 9, 9, tzinfo=UTC)


def page(*, items, request_id, quality="PASS", has_more=False, as_of=NOW):
    return GatewayResponsePage(
        items=tuple(items),
        source_request_id=request_id,
        has_more=has_more,
        next_cursor="1" if has_more else None,
        as_of=as_of,
        quality=quality,
    )


def account():
    return {
        "kind": "account",
        "external_account_id": "synthetic-account",
        "status": "active",
        "base_currency": "USD",
    }


def holding(code="usSYNX", account_id="synthetic-account"):
    return {
        "kind": "position",
        "external_account_id": account_id,
        "instrument_code": code,
        "currency": "USD",
        "quantity": "1",
        "available_quantity": "1",
        "frozen_quantity": "0",
        "average_cost": "10",
        "diluted_cost": None,
        "current_price": "11",
        "provider_as_of": NOW.isoformat(),
    }


def test_current_holding_binding_requires_complete_pass_account_scoped_page():
    from nexus.market.current_etf_identity_storage import current_holding_binding

    binding = current_holding_binding(
        symbol="SYNX",
        accounts_page=page(items=[account()], request_id="accounts-request"),
        holdings_page=page(items=[holding()], request_id="holdings-request"),
        now=NOW,
    )
    assert binding.holdings_source_request_id == "holdings-request"
    assert binding.holdings_as_of == NOW
    assert binding.provider_code == "usSYNX"


@pytest.mark.parametrize("case", ["partial", "paged", "missing", "cross_account", "stale"])
def test_current_holding_binding_fails_closed(case):
    from nexus.market.current_etf_identity_storage import current_holding_binding

    as_of = NOW - timedelta(minutes=6) if case == "stale" else NOW
    accounts_page = page(items=[account()], request_id="accounts-request", as_of=as_of)
    holdings_page = page(
        items=[
            holding(
                code="usOTHER" if case == "missing" else "usSYNX",
                account_id="other-account" if case == "cross_account" else "synthetic-account",
            )
        ],
        request_id="holdings-request",
        quality="PARTIAL" if case == "partial" else "PASS",
        has_more=case == "paged",
        as_of=as_of,
    )
    with pytest.raises(ValueError, match="CURRENT_HELD_ETF_HOLDING_INVALID"):
        current_holding_binding(
            symbol="SYNX",
            accounts_page=accounts_page,
            holdings_page=holdings_page,
            now=NOW,
        )
