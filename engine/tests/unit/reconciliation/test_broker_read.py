from datetime import timedelta
from uuid import UUID

import pytest
from helpers.broker_facts import ACCOUNT_ID, order, position, read_set
from helpers.broker_read import (
    AfterCashTimeoutProvider,
    InterruptedPaginationProvider,
    ScriptedBrokerProvider,
)
from nexus.providers.broker.base import BrokerPosition, BrokerProviderError
from nexus.reconciliation.broker_snapshot import BrokerReadSet, freeze_broker_snapshot


@pytest.mark.asyncio
async def test_ready_snapshot_contains_both_order_pages_and_b_evidence() -> None:
    stable = read_set(orders=(order("synthetic-order-1"), order("synthetic-order-2")))
    provider = ScriptedBrokerProvider([(stable, stable)], page_size=1)

    result = await freeze_broker_snapshot(ACCOUNT_ID, provider)

    assert result.status == "READY"
    assert [item.provider_order_id for item in result.portfolio_snapshot.open_orders] == [
        "synthetic-order-1",
        "synthetic-order-2",
    ]
    assert result.portfolio_snapshot.source_request_ids == (
        "synthetic-4-0",
        "synthetic-5-0",
        "synthetic-5-1",
        "synthetic-cash",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "first_failure",
    [
        (read_set(cash_value="1000"), read_set(cash_value="1002")),
        (read_set(), read_set(positions=(position(quantity="11"),))),
        (read_set(), read_set(orders=(order(filled_quantity="2"),))),
    ],
)
async def test_first_reconciliation_mismatch_retries_complete_window(
    first_failure: tuple[BrokerReadSet, BrokerReadSet],
) -> None:
    provider = ScriptedBrokerProvider([first_failure, (read_set(), read_set())])

    result = await freeze_broker_snapshot(ACCOUNT_ID, provider)

    assert result.status == "READY"
    assert result.attempt_count == 2
    assert [call[0] for call in provider.calls] == [
        "cash",
        "positions",
        "orders",
        "positions",
        "orders",
        "cash",
    ] * 2


@pytest.mark.asyncio
async def test_second_mismatch_or_interruption_requires_reconciliation() -> None:
    mismatch = (read_set(), read_set(positions=(position(quantity="11"),)))
    mismatch_result = await freeze_broker_snapshot(
        ACCOUNT_ID, ScriptedBrokerProvider([mismatch, mismatch])
    )
    interrupted = InterruptedPaginationProvider()
    interruption_result = await freeze_broker_snapshot(ACCOUNT_ID, interrupted)

    assert mismatch_result.status == "RECONCILIATION_REQUIRED"
    assert interruption_result.status == "RECONCILIATION_REQUIRED"
    assert interrupted.calls == ["cash", "positions", "cash", "positions"]


@pytest.mark.asyncio
async def test_mismatch_is_sticky_when_retry_is_unavailable() -> None:
    mismatch = (read_set(), read_set(positions=(position(quantity="11"),)))
    result = await freeze_broker_snapshot(
        ACCOUNT_ID,
        ScriptedBrokerProvider([mismatch, BrokerProviderError("unavailable")]),
    )

    assert result.status == "RECONCILIATION_REQUIRED"


@pytest.mark.asyncio
async def test_unavailable_refresh_can_only_reference_same_portfolio_ready_snapshot() -> None:
    initial = await freeze_broker_snapshot(
        ACCOUNT_ID, ScriptedBrokerProvider([(read_set(), read_set())])
    )
    result = await freeze_broker_snapshot(
        ACCOUNT_ID,
        ScriptedBrokerProvider(
            [BrokerProviderError("unavailable"), BrokerProviderError("unavailable")]
        ),
        prior_ready_snapshot=initial.portfolio_snapshot,
    )

    assert result.status == "STALE"
    assert result.portfolio_snapshot.stale_from_snapshot_id == initial.portfolio_snapshot.id
    with pytest.raises(ValueError, match="same portfolio"):
        await freeze_broker_snapshot(
            ACCOUNT_ID,
            ScriptedBrokerProvider([BrokerProviderError("unavailable")]),
            portfolio_id=UUID("0198d400-0000-7000-8000-000000000099"),
            prior_ready_snapshot=initial.portfolio_snapshot,
        )


@pytest.mark.asyncio
async def test_attempt_deadline_is_absolute_and_current_fact_failure_is_sticky() -> None:
    unavailable = await freeze_broker_snapshot(
        ACCOUNT_ID,
        ScriptedBrokerProvider(
            [(read_set(), read_set()), (read_set(), read_set())],
            call_delay=0.02,
        ),
        deadline=timedelta(milliseconds=10),
    )
    current_fact = await freeze_broker_snapshot(
        ACCOUNT_ID,
        AfterCashTimeoutProvider(),
        deadline=timedelta(milliseconds=10),
    )

    assert unavailable.status == "PARTIAL"
    assert current_fact.status == "RECONCILIATION_REQUIRED"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "positions",
    [
        (position(), position(quantity="11")),
        tuple(position(UUID(int=index + 1)) for index in range(101)),
    ],
)
async def test_duplicate_or_oversize_pages_fail_reconciliation(
    positions: tuple[BrokerPosition, ...],
) -> None:
    facts = read_set(positions=positions)
    result = await freeze_broker_snapshot(
        ACCOUNT_ID,
        ScriptedBrokerProvider([(facts, facts), (facts, facts)], page_size=101),
    )

    assert result.status == "RECONCILIATION_REQUIRED"


@pytest.mark.asyncio
async def test_partial_provider_observation_never_becomes_ready() -> None:
    stable = read_set()
    result = await freeze_broker_snapshot(
        ACCOUNT_ID,
        ScriptedBrokerProvider(
            [(stable, stable), (stable, stable)],
            page_quality="PARTIAL",
        ),
    )

    assert result.status == "RECONCILIATION_REQUIRED"
