from datetime import UTC, datetime
from decimal import Decimal

import pytest
from nexus.audit.canonical import (
    AuditChainInput,
    audit_event_hash,
    canonical_audit_event,
    canonical_json,
)
from pydantic import ValidationError


def _event() -> AuditChainInput:
    return AuditChainInput(
        event_seq=7,
        occurred_at=datetime(2026, 8, 27, 1, 2, 3, 456789, tzinfo=UTC),
        actor_id="system:测试",
        action="job.succeeded",
        schema_version="audit_event_v1",
        payload={"count": 2, "nested": {"β": "值"}, "ok": True},
    )


def test_audit_event_has_stable_unicode_jcs_and_hash_vector() -> None:
    canonical = canonical_audit_event(_event())

    assert canonical == (
        '{"action":"job.succeeded","actor_id":"system:测试","event_seq":7,'
        '"occurred_at":"2026-08-27T01:02:03.456789Z","payload":{"count":2,'
        '"nested":{"β":"值"},"ok":true},"schema_version":"audit_event_v1"}'
    )
    assert audit_event_hash("a" * 64, canonical) == (
        "5f5895c8207d396ffcee0e4159bbe857b4230e24f1a7ac03e4b3afb033a19a9d"
    )


@pytest.mark.parametrize(
    "payload",
    (
        {"float": 0.1},
        {"decimal": Decimal("0.1")},
        {"large": 9_007_199_254_740_992},
        {1: "invalid"},
    ),
)
def test_audit_payload_rejects_values_outside_the_strict_jcs_subset(
    payload: dict[object, object],
) -> None:
    with pytest.raises((TypeError, ValidationError, ValueError)):
        canonical_json(payload)


def test_audit_event_rejects_non_utc_time_and_noncanonical_previous_hash() -> None:
    with pytest.raises(ValidationError):
        _event().model_copy(update={"occurred_at": datetime(2026, 8, 27)}).model_validate(
            {**_event().model_dump(), "occurred_at": datetime(2026, 8, 27)}
        )
    with pytest.raises(ValueError, match="previous hash"):
        audit_event_hash("A" * 64, canonical_audit_event(_event()))
