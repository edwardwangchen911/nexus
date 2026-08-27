"""Pure integrity checks for strategy-experiment event authorities."""

import hashlib
from datetime import UTC, datetime
from uuid import UUID

from nexus.audit.canonical import canonical_json
from nexus.evolution.repository import experiment_chain_is_valid
from nexus.persistence.models.evolution import (
    StrategyExperimentEventRecord,
    StrategyExperimentStateProjectionRecord,
)

FAMILY_ID = UUID("019d0000-0000-7000-8000-000000007201")
LINEAGE_ID = UUID("019d0000-0000-7000-8000-000000007202")
EVENT_ID = UUID("019d0000-0000-7000-8000-000000007203")
NOW = datetime(2026, 8, 27, 15, tzinfo=UTC)


def _authority() -> tuple[
    StrategyExperimentEventRecord, StrategyExperimentStateProjectionRecord
]:
    payload = {"schema_version": "strategy_family_admitted_payload_v1"}
    identity = {
        "schema_version": "strategy_experiment_event_v1",
        "family_id": str(FAMILY_ID),
        "sequence": 1,
        "event_type": "ADMITTED",
        "idempotency_key": "repository-unit-admitted",
        "actor_kind": "service",
        "occurred_at": "2026-08-27T15:00:00.000000Z",
        "payload": payload,
        "previous_hash": "0" * 64,
    }
    event = StrategyExperimentEventRecord(
        id=EVENT_ID,
        family_id=FAMILY_ID,
        schema_version="strategy_experiment_event_v1",
        sequence=1,
        event_type="ADMITTED",
        idempotency_key="repository-unit-admitted",
        actor_kind="service",
        occurred_at=NOW,
        payload=payload,
        previous_hash="0" * 64,
        event_hash=hashlib.sha256(canonical_json(identity).encode()).hexdigest(),
        created_at=NOW,
    )
    projection = StrategyExperimentStateProjectionRecord(
        family_id=FAMILY_ID,
        research_lineage_id=LINEAGE_ID,
        current_state="ADMITTED",
        event_sequence=1,
        cumulative_oos_trial_count=0,
        version=1,
        updated_at=NOW,
    )
    return event, projection


def test_experiment_chain_verification_binds_event_content_and_projection() -> None:
    event, projection = _authority()
    assert experiment_chain_is_valid(FAMILY_ID, (event,), projection)

    event.event_hash = "f" * 64
    assert not experiment_chain_is_valid(FAMILY_ID, (event,), projection)

    event, projection = _authority()
    projection.current_state = "BACKTEST_RUNNING"
    assert not experiment_chain_is_valid(FAMILY_ID, (event,), projection)
