import pytest
from helpers.broker_facts import ACCOUNT_ID, SYNC_RUN_ID, order, read_set
from helpers.broker_read import ScriptedBrokerProvider
from nexus.reconciliation.read_attempt import AttemptProgress, read_atomic_window


@pytest.mark.asyncio
async def test_six_step_read_keeps_a_and_b_order_cursor_chains_separate() -> None:
    stable = read_set(
        orders=(order("synthetic-order-1"), order("synthetic-order-2")),
    )
    provider = ScriptedBrokerProvider([(stable, stable)], page_size=1)
    progress = AttemptProgress(attempt=1)

    read_a, read_b = await read_atomic_window(
        ACCOUNT_ID,
        provider,
        SYNC_RUN_ID,
        progress,
        attempt_deadline=1,
        monotonic=lambda: 0,
    )

    assert [call[0] for call in provider.calls] == [
        "cash",
        "positions",
        "orders",
        "orders",
        "positions",
        "orders",
        "orders",
        "cash",
    ]
    order_calls = [call for call in provider.calls if call[0] == "orders"]
    assert [call[3] for call in order_calls] == [None, "1", None, "1"]
    assert [item.provider_order_id for item in read_a.open_orders] == [
        "synthetic-order-1",
        "synthetic-order-2",
    ]
    assert read_a.open_orders == read_b.open_orders
    assert [observation["phase"] for observation in progress.source_observations] == [
        "cash_a",
        "positions_a",
        "orders_a",
        "orders_a",
        "positions_b",
        "orders_b",
        "orders_b",
        "cash_b",
    ]
