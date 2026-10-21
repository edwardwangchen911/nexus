"""Invalid diagnostics must degrade without leaking validation or serializer input."""

import warnings

from nexus.agents.committee import SdkFailureFacts
from nexus.agents.portfolio_synthesis_diagnostics import freeze_failure_observation


def test_constructed_invalid_nested_facts_cannot_bypass_validation_or_emit_values():
    invalid = SdkFailureFacts.model_construct(exception_kind="PRIVATE_DIAGNOSTIC_CANARY")
    with warnings.catch_warnings(record=True) as emitted:
        warnings.simplefilter("always")
        result = freeze_failure_observation({"failure_facts": invalid})
    assert result.local == "diagnostic_unavailable"
    assert result.failure_facts is None
    assert "PRIVATE_DIAGNOSTIC_CANARY" not in result.model_dump_json()
    assert not emitted


def test_diagnostic_column_preserves_legacy_synthesis_usage_acceptance_hash():
    import hashlib
    import json
    from datetime import UTC, datetime
    from uuid import UUID

    from nexus.paper.synthesis_usage_acceptance import _row_hash
    from nexus.persistence.models.agents import AgentPortfolioSynthesisOutputRecord

    legacy_values = {
        "id": str(UUID(int=1)),
        "run_id": str(UUID(int=2)),
        "status": "failed",
        "intent": None,
        "content_hash": "a" * 64,
        "binding_hash": "b" * 64,
        "created_at": "2026-01-01 00:00:00+00:00",
    }
    expected = hashlib.sha256(
        json.dumps(legacy_values, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    output = AgentPortfolioSynthesisOutputRecord(
        id=UUID(int=1),
        run_id=UUID(int=2),
        status="failed",
        intent=None,
        content_hash="a" * 64,
        binding_hash="b" * 64,
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert _row_hash(output) == expected
    output.failure_diagnostic = {"diagnostic": {"local": "diagnostic_unavailable"}}
    assert _row_hash(output) == expected
    output.content_hash = "c" * 64
    assert _row_hash(output) != expected
