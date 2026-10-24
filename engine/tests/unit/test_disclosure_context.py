from datetime import UTC, datetime
from uuid import uuid4

import pytest
from nexus.monitoring.disclosure_context import (
    DisclosureMonitoringContext,
    FrozenDisclosureBaseline,
    validate_disclosure_baseline,
)
from pydantic import ValidationError


def baseline():
    return FrozenDisclosureBaseline(
        instrument_id=uuid4(),
        snapshot_id=uuid4(),
        snapshot_hash="a" * 64,
        issuer_cik="0000123456",
        research_as_of=datetime(2026, 10, 24, tzinfo=UTC),
    )


def test_baseline_is_strict_frozen_and_json_roundtrips():
    value = baseline()
    assert FrozenDisclosureBaseline.model_validate_json(value.model_dump_json()) == value
    with pytest.raises(ValidationError):
        value.issuer_cik = "0000654321"
    with pytest.raises(ValidationError):
        FrozenDisclosureBaseline.model_validate(
            {**value.model_dump(), "instrument_id": str(value.instrument_id)}
        )
    for update in (
        {"issuer_cik": "0000000000"},
        {"snapshot_hash": "A" * 64},
        {"research_as_of": datetime(2026, 10, 24)},
    ):
        with pytest.raises(ValidationError):
            FrozenDisclosureBaseline.model_validate({**value.model_dump(), **update})


def test_context_requires_exact_baseline_scope_or_explicit_gap():
    value = baseline()
    args = dict(instrument_id=value.instrument_id, research_as_of=value.research_as_of)
    context = DisclosureMonitoringContext(**args, baseline=value, gap_reason=None)
    assert context.ready
    for update in (
        {"instrument_id": uuid4()},
        {"research_as_of": datetime(2026, 10, 23, tzinfo=UTC)},
    ):
        with pytest.raises(ValidationError):
            DisclosureMonitoringContext(**{**args, **update}, baseline=value, gap_reason=None)
    with pytest.raises(ValidationError):
        DisclosureMonitoringContext(**args, baseline=None, gap_reason=None)
    with pytest.raises(ValidationError):
        DisclosureMonitoringContext(**args, baseline=value, gap_reason="SEC_BASELINE_MISSING")


@pytest.mark.asyncio
async def test_frozen_gap_is_not_reselected_when_validating():
    value = baseline()
    for reason in ("SEC_BASELINE_MISSING", "SEC_BASELINE_INVALID"):
        context = DisclosureMonitoringContext(
            instrument_id=value.instrument_id,
            research_as_of=value.research_as_of,
            baseline=None,
            gap_reason=reason,
        )
        assert not context.ready
        assert await validate_disclosure_baseline(object(), context)
