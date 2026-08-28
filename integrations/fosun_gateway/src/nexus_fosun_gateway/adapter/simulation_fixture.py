"""Deterministic no-socket fixture for the isolated simulation process."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

from nexus_fosun_gateway.policy.simulation_operations import (
    SIMULATION_OPERATIONS,
    SimulationOperation,
)

FixtureCase = Literal[
    "acknowledged",
    "rejected",
    "partial_batch",
    "pre_send_timeout",
    "post_send_unknown",
    "pagination",
    "duplicate_event",
    "reconciliation_conflict",
]
_AS_OF = "2026-08-28T14:00:00Z"


@dataclass(frozen=True)
class FixtureSimulationGatewayScript:
    case: FixtureCase = "acknowledged"


class FixtureSimulationGateway:
    """Return sanitized scripted results without SDK, credentials, or network clients."""

    def __init__(self, script: FixtureSimulationGatewayScript) -> None:
        self._script = script
        self._create_count = 0

    async def execute(
        self,
        operation: SimulationOperation,
        payload: Mapping[str, object],
    ) -> dict[str, object]:
        if operation not in SIMULATION_OPERATIONS:
            raise ValueError("simulation operation is not allowlisted")
        if operation == "fosun_simulation_capabilities":
            return {
                "mode": "offline_fixture",
                "external_activation": "external_activation_blocked",
                "operations": sorted(SIMULATION_OPERATIONS),
                "as_of": _AS_OF,
            }
        if operation in {
            "fosun_simulation_create_order",
            "fosun_simulation_modify_order",
            "fosun_simulation_cancel_order",
        }:
            return self._command_result(payload)
        if operation == "fosun_simulation_list_orders":
            return self._orders(payload)
        if operation == "fosun_simulation_sync_portfolio":
            return self._portfolio()
        return self._cash_flows()

    def _command_result(self, payload: Mapping[str, object]) -> dict[str, object]:
        command_id = payload.get("command_id")
        if not isinstance(command_id, str):
            raise ValueError("fixture command ID missing")
        self._create_count += 1
        case = self._script.case
        if case == "partial_batch":
            outcome = "acknowledged" if self._create_count == 1 else "rejected"
        else:
            outcome = {
                "acknowledged": "acknowledged",
                "rejected": "rejected",
                "pre_send_timeout": "transport_failed",
                "post_send_unknown": "result_unknown",
            }.get(case, "acknowledged")
        return {
            "command_id": command_id,
            "outcome": outcome,
            "safe_code": {
                "acknowledged": "FIXTURE_ACKNOWLEDGED",
                "rejected": "FIXTURE_REJECTED",
                "transport_failed": "FIXTURE_PRE_SEND_TIMEOUT",
                "result_unknown": "FIXTURE_RESULT_UNKNOWN",
            }[outcome],
            "provider_order_ref": (
                f"synthetic-order-{self._create_count:03d}"
                if outcome == "acknowledged"
                else None
            ),
            "as_of": _AS_OF,
        }

    def _orders(self, payload: Mapping[str, object]) -> dict[str, object]:
        cursor = payload.get("cursor")
        if cursor not in (None, "1"):
            raise ValueError("fixture cursor rejected")
        if self._script.case == "pagination":
            ordinal = 1 if cursor is None else 2
            has_more = cursor is None
        else:
            ordinal = 1
            has_more = False
        event_id = "synthetic-event-duplicate" if self._script.case == "duplicate_event" else (
            f"synthetic-event-{ordinal:03d}"
        )
        item: dict[str, object] = {
            "event_id": event_id,
            "provider_order_ref": f"synthetic-order-{ordinal:03d}",
            "instrument_code": "AAPL.US",
            "side": "buy",
            "quantity": "10",
            "filled_quantity": "0",
            "limit_price": "200.00",
            "status": "working",
            "updated_at": _AS_OF,
        }
        items = [item, dict(item)] if self._script.case == "duplicate_event" else [item]
        return _page(items, has_more=has_more, next_cursor="1" if has_more else None)

    def _portfolio(self) -> dict[str, object]:
        quality = "CONFLICT" if self._script.case == "reconciliation_conflict" else "PASS"
        return _page(
            [
                {"kind": "cash", "currency": "USD", "available_cash": "100000.00"},
                {
                    "kind": "position",
                    "instrument_code": "AAPL.US",
                    "quantity": "10",
                },
            ],
            quality=quality,
        )

    def _cash_flows(self) -> dict[str, object]:
        return _page(
            [
                {
                    "cash_flow_ref": "synthetic-flow-001",
                    "currency": "USD",
                    "amount": "100000.00",
                    "flow_type": "deposit",
                    "occurred_at": _AS_OF,
                }
            ]
        )


def _page(
    items: list[dict[str, object]],
    *,
    has_more: bool = False,
    next_cursor: str | None = None,
    quality: str = "PASS",
) -> dict[str, object]:
    return {
        "items": items,
        "source_request_id": "synthetic-simulation-request-001",
        "has_more": has_more,
        "next_cursor": next_cursor,
        "as_of": _AS_OF,
        "quality": quality,
    }
