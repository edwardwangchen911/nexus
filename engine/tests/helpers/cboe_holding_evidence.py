"""Handwritten synthetic fixtures; no captured source or account data."""

from datetime import timedelta

from helpers.iso_mic import NOW
from nexus.providers.broker.gateway_contracts import GatewayResponsePage


def pages():
    common = dict(has_more=False, next_cursor=None, as_of=NOW - timedelta(minutes=1))
    accounts = GatewayResponsePage(
        items=(
            {
                "kind": "account",
                "external_account_id": "synthetic-private-account",
                "status": "active",
                "base_currency": "USD",
            },
        ),
        source_request_id="synthetic-accounts",
        quality="PASS",
        **common,
    )
    holdings = GatewayResponsePage(
        items=(
            {
                "kind": "position",
                "external_account_id": "synthetic-private-account",
                "instrument_code": "usTEST",
                "currency": "USD",
                "quantity": "2",
                "available_quantity": "2",
                "frozen_quantity": "0",
                "average_cost": None,
                "diluted_cost": None,
                "current_price": None,
                "provider_as_of": None,
            },
        ),
        source_request_id="synthetic-holdings",
        quality="PARTIAL",
        **common,
    )
    return accounts, holdings
