"""Canonical registry-event identity."""

from datetime import UTC, date, datetime
from uuid import UUID

from nexus.evolution.registry import registry_event_content_hash

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
