"""Canonical registry-event identity."""

from datetime import UTC, date, datetime
from uuid import UUID

from nexus.evolution.registry import (
    rebuild_registry_head,
    registry_activation_content_hash,
    registry_chain_is_valid,
    registry_event_content_hash,
    registry_head_is_valid,
)
from nexus.evolution.shadow.canonical import content_hash
from nexus.persistence.models.evolution import (
    StrategyRegistryActivationRecord,
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


def test_registry_replay_derives_every_head_field_from_events_and_activation() -> None:
    target = UUID("019d0000-0000-7000-8000-000000007104")
    events = _registry_history(target)
    transition = events[3]
    activation_id = UUID("019d0000-0000-7000-8000-000000007105")
    activated_at = NOW.replace(hour=22)
    activation = StrategyRegistryActivationRecord(
        id=activation_id,
        registry_event_id=transition.id,
        decision_session=transition.effective_session,
        activated_at=activated_at,
        from_definition_id=FROM_ID,
        to_definition_id=target,
        champion_generation=2,
        activation_hash=registry_activation_content_hash(
            registry_event_id=transition.id,
            decision_session=transition.effective_session,
            activated_at=activated_at,
            from_definition_id=FROM_ID,
            to_definition_id=target,
            champion_generation=2,
        ),
        created_at=activated_at,
    )
    rebuilt = rebuild_registry_head(events, (activation,))
    assert rebuilt.active_definition_id == target
    assert rebuilt.active_generation == 2
    assert rebuilt.auto_promotion_paused is True
    assert rebuilt.pending_event_id is None
    assert rebuilt.pending_effective_session is None
    assert rebuilt.last_event_id == events[-1].id
    assert rebuilt.event_sequence == 5
    assert rebuilt.version == len(events) + 1

    exact = _head_from_replay(rebuilt)
    assert registry_head_is_valid(exact, events, (activation,))
    mutations = {
        "active_definition_id": FROM_ID,
        "auto_promotion_paused": False,
        "pending_event_id": transition.id,
        "version": rebuilt.version + 1,
    }
    for field, forged in mutations.items():
        candidate = _head_from_replay(rebuilt)
        setattr(candidate, field, forged)
        if field == "pending_event_id":
            candidate.pending_effective_session = transition.effective_session
        assert not registry_head_is_valid(candidate, events, (activation,))


def _registry_history(target: UUID) -> tuple[StrategyRegistryEventRecord, ...]:
    genesis = _genesis_event()
    events = [genesis]
    previous_hash = genesis.event_hash
    specifications = (
        ("PAUSE", FROM_ID, FROM_ID, 1, None),
        ("RESUME", FROM_ID, FROM_ID, 1, None),
        ("PROMOTE", FROM_ID, target, 2, date(2026, 10, 2)),
        ("PAUSE", FROM_ID, FROM_ID, 2, None),
    )
    for sequence, (event_type, source, destination, generation, effective) in enumerate(
        specifications, start=2
    ):
        payload = {"schema_version": f"registry_unit_{event_type.lower()}_v1"}
        event_id = UUID(f"019d0000-0000-7000-8000-{7100 + sequence:012d}")
        event_hash = registry_event_content_hash(
            sequence=sequence,
            event_type=event_type,
            idempotency_key=f"registry-unit-{event_type.lower()}-{sequence}",
            actor_kind="service" if event_type == "PROMOTE" else "user",
            occurred_at=NOW,
            from_definition_id=source,
            to_definition_id=destination,
            promotion_decision_id=None,
            champion_generation=generation,
            effective_session=effective,
            payload=payload,
            previous_hash=previous_hash,
        )
        event = StrategyRegistryEventRecord(
            id=event_id,
            channel="internal_paper",
            schema_version="strategy_registry_event_v1",
            sequence=sequence,
            event_type=event_type,
            idempotency_key=f"registry-unit-{event_type.lower()}-{sequence}",
            actor_kind="service" if event_type == "PROMOTE" else "user",
            occurred_at=NOW,
            from_definition_id=source,
            to_definition_id=destination,
            promotion_decision_id=None,
            champion_generation=generation,
            effective_session=effective,
            payload=payload,
            previous_hash=previous_hash,
            event_hash=event_hash,
            created_at=NOW,
        )
        events.append(event)
        previous_hash = event_hash
    return tuple(events)


def _genesis_event() -> StrategyRegistryEventRecord:
    event_id = UUID("019d0000-0000-7000-8000-000000007101")
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
    return StrategyRegistryEventRecord(
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


def _head_from_replay(rebuilt) -> StrategyRegistryHeadRecord:
    return StrategyRegistryHeadRecord(
        channel="internal_paper",
        active_definition_id=rebuilt.active_definition_id,
        active_generation=rebuilt.active_generation,
        last_event_id=rebuilt.last_event_id,
        event_sequence=rebuilt.event_sequence,
        auto_promotion_paused=rebuilt.auto_promotion_paused,
        pending_event_id=rebuilt.pending_event_id,
        pending_effective_session=rebuilt.pending_effective_session,
        version=rebuilt.version,
        updated_at=NOW,
    )
