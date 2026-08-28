"""Compile-time allowlist for the isolated simulation process."""

from typing import Final, Literal

SimulationOperation = Literal[
    "fosun_simulation_capabilities",
    "fosun_simulation_create_order",
    "fosun_simulation_modify_order",
    "fosun_simulation_cancel_order",
    "fosun_simulation_list_orders",
    "fosun_simulation_sync_portfolio",
    "fosun_simulation_sync_cash_flows",
]

SIMULATION_OPERATIONS: Final[frozenset[str]] = frozenset(
    {
        "fosun_simulation_capabilities",
        "fosun_simulation_create_order",
        "fosun_simulation_modify_order",
        "fosun_simulation_cancel_order",
        "fosun_simulation_list_orders",
        "fosun_simulation_sync_portfolio",
        "fosun_simulation_sync_cash_flows",
    }
)
