"""Synthetic broker-order evidence; never real account or security facts."""

from datetime import UTC, datetime, timedelta

import pytest
from nexus.providers.broker.gateway_contracts import GatewayResponsePage

NOW = datetime(2026, 9, 22, 18, tzinfo=UTC)


def _pages(*, status: str = "40") -> tuple[GatewayResponsePage, GatewayResponsePage]:
    accounts = GatewayResponsePage(
        items=(
            {
                "kind": "account",
                "external_account_id": "synthetic-account",
                "status": "active",
                "base_currency": "USD",
            },
        ),
        source_request_id="synthetic-accounts-request",
        has_more=False,
        next_cursor=None,
        as_of=NOW - timedelta(minutes=1),
        quality="PASS",
    )
    orders = GatewayResponsePage(
        items=(
            {
                "kind": "order",
                "external_order_id": "private-order-id",
                "instrument_code": "usSYNX",
                "side": "buy",
                "quantity": "1",
                "filled_quantity": "0",
                "price": "10",
                "status": status,
                "submitted_at": (NOW - timedelta(hours=1)).isoformat(),
                "updated_at": None,
            },
        ),
        source_request_id="synthetic-orders-request",
        has_more=False,
        next_cursor=None,
        as_of=NOW,
        quality="PARTIAL",
    )
    return accounts, orders


def test_current_open_order_binding_accepts_one_active_order_without_storing_order_id():
    from nexus.market.current_etf_identity_storage import current_open_order_binding

    accounts, orders = _pages()
    binding = current_open_order_binding(
        symbol="SYNX", accounts_page=accounts, orders_page=orders, now=NOW
    )

    assert binding.provider_code == "usSYNX"
    assert binding.provider_status == "40"
    assert binding.submitted_at == NOW - timedelta(hours=1)
    assert binding.order_active is True
    assert binding.order_id_sha256 != "private-order-id"
    assert "private-order-id" not in binding.model_dump_json()


def test_current_open_order_binding_rejects_terminal_order():
    from nexus.market.current_etf_identity_storage import current_open_order_binding

    accounts, orders = _pages(status="50")
    with pytest.raises(ValueError, match="CURRENT_ETF_OPEN_ORDER_INVALID"):
        current_open_order_binding(
            symbol="SYNX", accounts_page=accounts, orders_page=orders, now=NOW
        )
