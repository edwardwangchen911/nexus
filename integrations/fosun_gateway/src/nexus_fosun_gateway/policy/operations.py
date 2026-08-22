"""Compile-time read-only operation allowlist."""

from typing import Final, Literal

GatewayOperation = Literal[
    "fosun_capabilities",
    "fosun_sync_accounts",
    "fosun_sync_portfolio",
    "fosun_list_orders",
    "fosun_list_cash_flows",
    "fosun_get_quotes",
    "fosun_get_bars",
]

READ_ONLY_OPERATIONS: Final[frozenset[str]] = frozenset(
    {
        "fosun_capabilities",
        "fosun_sync_accounts",
        "fosun_sync_portfolio",
        "fosun_list_orders",
        "fosun_list_cash_flows",
        "fosun_get_quotes",
        "fosun_get_bars",
    }
)
