from copy import deepcopy
from datetime import timedelta
from uuid import UUID

import pytest
from helpers.broker_seed import INSTRUMENT_ID, NOW, READY_SNAPSHOT_ID
from helpers.broker_snapshot import _snapshot, _sync_result
from nexus.reconciliation.finalization import InvalidBrokerSyncResult, validate_broker_sync_result


def _ready_result():
    return _sync_result(
        UUID("0198d400-1000-7000-8000-000000000099"),
        _snapshot(INSTRUMENT_ID, READY_SNAPSHOT_ID, "READY"),
    )


def test_ready_evidence_binds_pass_observations_and_snapshot_read_b() -> None:
    result = _ready_result()
    validate_broker_sync_result(result)

    partial = deepcopy(result.attempt_traces[0])
    partial["source_observations"][0]["quality"] = "PARTIAL"
    with pytest.raises(InvalidBrokerSyncResult, match="PASS"):
        validate_broker_sync_result(result.model_copy(update={"attempt_traces": (partial,)}))

    unbound = result.portfolio_snapshot.model_copy(update={"source_observations": ()})
    with pytest.raises(InvalidBrokerSyncResult, match="observations"):
        validate_broker_sync_result(result.model_copy(update={"portfolio_snapshot": unbound}))


@pytest.mark.parametrize("mutation", ("order", "backtrack", "missing", "cash_multi"))
def test_ready_observation_blocks_are_monotonic_and_complete(mutation: str) -> None:
    result = _ready_result()
    trace = deepcopy(result.attempt_traces[0])
    observations = list(trace["source_observations"])
    if mutation == "order":
        observations[0], observations[1] = observations[1], observations[0]
    elif mutation == "backtrack":
        observations.insert(3, deepcopy(observations[1]))
    elif mutation == "missing":
        observations.pop(2)
    else:
        observations.insert(1, deepcopy(observations[0]))
    trace["source_observations"] = tuple(observations)
    trace["source_request_ids"] = tuple(item["source_request_id"] for item in observations)

    with pytest.raises(InvalidBrokerSyncResult, match="phase blocks"):
        validate_broker_sync_result(result.model_copy(update={"attempt_traces": (trace,)}))


def test_multi_page_phase_uses_its_last_response_as_fact_time_bound() -> None:
    result = _ready_result()
    trace = deepcopy(result.attempt_traces[0])
    observations = list(trace["source_observations"])
    last_page = deepcopy(observations[1])
    last_page["source_request_id"] = "synthetic-last-position-page"
    last_page["response_as_of"] = (NOW + timedelta(microseconds=2)).isoformat()
    observations.insert(2, last_page)
    trace["source_observations"] = tuple(observations)
    trace["source_request_ids"] = tuple(item["source_request_id"] for item in observations)
    trace["raw_facts"]["positions_a"][0]["provider_as_of"] = (
        NOW + timedelta(microseconds=1)
    ).isoformat()

    validate_broker_sync_result(result.model_copy(update={"attempt_traces": (trace,)}))
    trace["raw_facts"]["positions_a"][0]["provider_as_of"] = (
        NOW + timedelta(microseconds=3)
    ).isoformat()
    with pytest.raises(InvalidBrokerSyncResult, match="response time"):
        validate_broker_sync_result(result.model_copy(update={"attempt_traces": (trace,)}))


@pytest.mark.parametrize(
    ("phase", "field"),
    (
        ("cash_a", "provider_as_of"),
        ("positions_a", "provider_as_of"),
        ("orders_a", "updated_at"),
        ("positions_b", "provider_as_of"),
        ("orders_b", "updated_at"),
        ("cash_b", "provider_as_of"),
    ),
)
def test_fact_time_cannot_exceed_its_phase_response(phase: str, field: str) -> None:
    result = _ready_result()
    trace = deepcopy(result.attempt_traces[0])
    future_time = NOW + timedelta(microseconds=1)
    trace["raw_facts"][phase][0][field] = future_time.isoformat()
    if phase.endswith("_b"):
        collection_name = {
            "cash_b": "cash",
            "positions_b": "positions",
            "orders_b": "open_orders",
        }[phase]
        collection = getattr(result.portfolio_snapshot, collection_name)
        updated = (collection[0].model_copy(update={field: future_time}),)
        trace["raw_facts"][phase] = tuple(item.model_dump(mode="json") for item in updated)
        result = result.model_copy(
            update={
                "portfolio_snapshot": result.portfolio_snapshot.model_copy(
                    update={collection_name: updated, "as_of": future_time}
                )
            }
        )

    with pytest.raises(InvalidBrokerSyncResult, match="response time"):
        validate_broker_sync_result(result.model_copy(update={"attempt_traces": (trace,)}))


def test_snapshot_as_of_is_recomputed_from_read_b_fact_times() -> None:
    result = _ready_result()
    future = result.portfolio_snapshot.model_copy(
        update={"as_of": NOW + timedelta(microseconds=1)}
    )

    with pytest.raises(InvalidBrokerSyncResult, match="snapshot as-of"):
        validate_broker_sync_result(result.model_copy(update={"portfolio_snapshot": future}))
