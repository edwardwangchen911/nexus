from copy import deepcopy
from datetime import timedelta
from uuid import uuid4

import pytest
from helpers.observed_gateway import NOW, ObservedGateway
from nexus.providers.broker.base import BrokerSyncResult
from nexus.providers.broker.fosun import FosunBrokerProvider
from nexus.reconciliation.broker_snapshot import freeze_broker_snapshot
from nexus.reconciliation.finalization import InvalidBrokerSyncResult, validate_broker_sync_result


async def observed_result(**kwargs: bool) -> tuple[BrokerSyncResult, ObservedGateway]:
    account_id = uuid4()
    gateway = ObservedGateway(**kwargs)
    provider = FosunBrokerProvider(
        account_id=account_id,
        external_account_id="synthetic-account",
        client=gateway,
        instrument_ids={"usAAPL": uuid4()},
        cash_flow_kinds={},
        snapshot_time_basis="gateway_observed",
    )
    result = await freeze_broker_snapshot(account_id, provider, max_attempts=1)
    return result, gateway


@pytest.mark.asyncio
async def test_observed_snapshot_reconciles_without_inventing_provider_event_times() -> None:
    result, gateway = await observed_result()
    assert gateway.calls == 6
    assert result.status == "READY"
    snapshot = result.portfolio_snapshot
    assert snapshot.as_of == NOW + timedelta(seconds=6)
    assert snapshot.cash[0].provider_as_of is None
    assert snapshot.positions[0].provider_as_of is None
    assert snapshot.open_orders[0].updated_at is None
    assert {item.time_basis for item in snapshot.source_observations} == {"gateway_observed"}
    validate_broker_sync_result(result)


@pytest.mark.asyncio
@pytest.mark.parametrize("kwargs", ({"mismatch": True}, {"missing_price": True}))
async def test_observation_time_does_not_bypass_financial_completeness_or_reconciliation(
    kwargs: dict[str, bool],
) -> None:
    result, _ = await observed_result(**kwargs)
    assert result.status == "RECONCILIATION_REQUIRED"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mutation",
    (
        "basis",
        "response_order",
        "window",
        "duplicate",
        "nav",
        "nav_time",
        "snapshot_time",
    ),
)
async def test_observed_snapshot_is_independently_revalidated_before_persistence(
    mutation: str,
) -> None:
    result, _ = await observed_result()
    traces = deepcopy(result.attempt_traces)
    if mutation == "basis":
        traces[0]["source_observations"][0]["time_basis"] = "provider_event"
    elif mutation == "response_order":
        traces[0]["source_observations"][0]["response_as_of"] = (
            NOW + timedelta(minutes=2)
        ).isoformat()
    elif mutation == "nav":
        traces[0]["raw_facts"]["_derived_nav"]["read_a"][0]["value"] = "999999"
    elif mutation == "window":
        traces[0]["source_observations"][0]["response_as_of"] = (
            NOW - timedelta(minutes=2)
        ).isoformat()
    elif mutation == "duplicate":
        traces[0]["source_observations"][0]["source_request_id"] = "synthetic-2"
        traces[0]["source_request_ids"] = ("synthetic-2", *traces[0]["source_request_ids"][1:])
    elif mutation == "nav_time":
        traces[0]["raw_facts"]["_derived_nav"]["read_a"][0]["provider_as_of"] = NOW.isoformat()
    else:
        result = result.model_copy(
            update={
                "portfolio_snapshot": result.portfolio_snapshot.model_copy(update={"as_of": NOW})
            }
        )
    with pytest.raises(InvalidBrokerSyncResult):
        validate_broker_sync_result(result.model_copy(update={"attempt_traces": traces}))
