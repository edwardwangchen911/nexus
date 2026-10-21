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
