from uuid import UUID

import pytest
from nexus_fosun_gateway.adapter.simulation_fixture import (
    FixtureSimulationGateway,
    FixtureSimulationGatewayScript,
)
from nexus_fosun_gateway.policy.simulation_operations import SIMULATION_OPERATIONS


def _create_payload(command_id: int = 1) -> dict[str, object]:
    return {
        "command_id": str(UUID(int=command_id)),
        "account_binding_id": str(UUID(int=101)),
        "intent_order_id": str(UUID(int=201)),
        "idempotency_key": f"fixture-create-{command_id}",
        "instrument_code": "AAPL.US",
        "side": "buy",
        "quantity": "10",
        "limit_price": "200.00",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "outcome"),
    [
        ("acknowledged", "acknowledged"),
        ("rejected", "rejected"),
        ("pre_send_timeout", "transport_failed"),
        ("post_send_unknown", "result_unknown"),
    ],
)
async def test_fixture_replays_closed_create_outcomes(case: str, outcome: str) -> None:
    gateway = FixtureSimulationGateway(FixtureSimulationGatewayScript(case=case))

    response = await gateway.execute("fosun_simulation_create_order", _create_payload())

    assert response["outcome"] == outcome
    assert "provider_body" not in response


@pytest.mark.asyncio
async def test_partial_batch_script_is_deterministic_and_serial() -> None:
    gateway = FixtureSimulationGateway(FixtureSimulationGatewayScript(case="partial_batch"))

    first = await gateway.execute("fosun_simulation_create_order", _create_payload(1))
    second = await gateway.execute("fosun_simulation_create_order", _create_payload(2))
    third = await gateway.execute("fosun_simulation_create_order", _create_payload(3))

    assert [first["outcome"], second["outcome"], third["outcome"]] == [
        "acknowledged",
        "rejected",
        "rejected",
    ]


@pytest.mark.asyncio
async def test_fixture_pagination_duplicate_event_and_conflict_are_explicit() -> None:
    pagination = FixtureSimulationGateway(FixtureSimulationGatewayScript(case="pagination"))
    first = await pagination.execute(
        "fosun_simulation_list_orders",
        {"account_binding_id": str(UUID(int=101)), "cursor": None, "limit": 100},
    )
    second = await pagination.execute(
        "fosun_simulation_list_orders",
        {"account_binding_id": str(UUID(int=101)), "cursor": "1", "limit": 100},
    )
    duplicate = await FixtureSimulationGateway(
        FixtureSimulationGatewayScript(case="duplicate_event")
    ).execute(
        "fosun_simulation_list_orders",
        {"account_binding_id": str(UUID(int=101)), "cursor": None, "limit": 100},
    )
    conflict = await FixtureSimulationGateway(
        FixtureSimulationGatewayScript(case="reconciliation_conflict")
    ).execute(
        "fosun_simulation_sync_portfolio",
        {"account_binding_id": str(UUID(int=101))},
    )

    assert first["next_cursor"] == "1" and first["has_more"] is True
    assert second["next_cursor"] is None and second["has_more"] is False
    assert duplicate["items"][0]["event_id"] == duplicate["items"][1]["event_id"]
    assert conflict["quality"] == "CONFLICT"


def test_simulation_operation_allowlist_is_exact_and_readable() -> None:
    assert SIMULATION_OPERATIONS == frozenset(
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
