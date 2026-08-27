"""Canonical registry-event identity."""

from datetime import UTC, date, datetime
from uuid import UUID

from nexus.evolution.registry import registry_chain_is_valid, registry_event_content_hash
from nexus.evolution.shadow.canonical import content_hash
from nexus.persistence.models.evolution import (
    StrategyRegistryEventRecord,
    StrategyRegistryHeadRecord,
)

FROM_ID = UUID("019d0000-0000-7000-8000-000000007101")
TO_ID = UUID("019d0000-0000-7000-8000-000000007102")
NOW = datetime(2026, 10, 1, 21, tzinfo=UTC)


def _hash(*, generation: int = 2, target: UUID = TO_ID) -> str:
    return registry_event_content_hash(
        sequence=2,
        event_type="PROMOTE",
        idempotency_key="registry-unit-promotion",
        actor_kind="service",
        occurred_at=NOW,
        from_definition_id=FROM_ID,
        to_definition_id=target,
        promotion_decision_id=None,
        champion_generation=generation,
        effective_session=date(2026, 10, 2),
        payload={"schema_version": "registry_unit_v1"},
        previous_hash="a" * 64,
    )


def test_registry_event_hash_is_deterministic_and_binds_generation_and_target() -> None:
    assert _hash() == _hash()
    assert _hash() != _hash(generation=3)
    assert _hash() != _hash(target=FROM_ID)


def test_registry_chain_verification_rejects_content_hash_tampering() -> None:
    event_id = UUID("019d0000-0000-7000-8000-000000007103")
    occurred_at = datetime(2026, 8, 27, tzinfo=UTC)
    payload = {"schema_version": "strategy_registry_genesis_payload_v1"}
    identity = {
        "channel": "internal_paper",
        "schema_version": "strategy_registry_event_v1",
        "sequence": 1,
        "event_type": "GENESIS",
        "idempotency_key": "registry-unit-genesis",
        "actor_kind": "migration",
        "occurred_at": "2026-08-27T00:00:00Z",
        "from_definition_id": None,
        "to_definition_id": str(FROM_ID),
        "champion_generation": 1,
        "effective_session": None,
        "payload": payload,
        "previous_hash": "0" * 64,
    }
    event = StrategyRegistryEventRecord(
        id=event_id,
        channel="internal_paper",
        schema_version="strategy_registry_event_v1",
        sequence=1,
        event_type="GENESIS",
        idempotency_key="registry-unit-genesis",
        actor_kind="migration",
        occurred_at=occurred_at,
        from_definition_id=None,
        to_definition_id=FROM_ID,
        promotion_decision_id=None,
        champion_generation=1,
        effective_session=None,
        payload=payload,
        previous_hash="0" * 64,
        event_hash=content_hash(identity),
        created_at=occurred_at,
    )
    head = StrategyRegistryHeadRecord(
        channel="internal_paper",
        active_definition_id=FROM_ID,
        active_generation=1,
        last_event_id=event_id,
        event_sequence=1,
        auto_promotion_paused=False,
        pending_event_id=None,
        pending_effective_session=None,
        version=1,
        updated_at=occurred_at,
    )
    assert registry_chain_is_valid(head, (event,))
    event.event_hash = "f" * 64
    assert not registry_chain_is_valid(head, (event,))
