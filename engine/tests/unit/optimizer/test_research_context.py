"""Frozen proposal context cannot claim another identity or time ordering."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.optimizer.research_context import (
    DailyResearchContextRequest,
    FrozenDailyResearchContext,
    read_draft_research_context,
)
from nexus.persistence.models.data import DataSnapshot
from pydantic import ValidationError


def _context():
    now = datetime(2026, 9, 4, tzinfo=UTC)
    return dict(
        root_id=UUID(int=1),
        run_id=UUID(int=2),
        output_id=UUID(int=3),
        output_sequence=1,
        output_hash="a" * 64,
        portfolio_snapshot_id=UUID(int=4),
        diagnostic_report_id=UUID(int=5),
        research_data_snapshot_id=UUID(int=6),
        instrument_id=UUID(int=7),
        valuation_time=now - timedelta(days=1),
        research_as_of=now - timedelta(hours=1),
        output_available_at=now,
        context_as_of=now,
        evidence_hash="b" * 64,
        run_status="failed",
        quality_status="partial",
        intent=None,
        unavailable_reason="RESEARCH_FAILED",
    )


def test_explicit_context_request_requires_utc():
    with pytest.raises(ValidationError):
        DailyResearchContextRequest(
            root_id=UUID(int=1), run_id=UUID(int=2), context_as_of=datetime(2026, 9, 4)
        )


@pytest.mark.parametrize(
    "case",
    [
        "naive",
        "research_before_valuation",
        "output_after_cutoff",
        "failed_but_complete",
        "wrong_reason",
    ],
)
def test_saved_context_validates_cross_field_contract(case):
    values = _context()
    assert FrozenDailyResearchContext.model_validate(values).mode == "context_only"
    if case == "naive":
        values["context_as_of"] = values["context_as_of"].replace(tzinfo=None)
    elif case == "research_before_valuation":
        values["research_as_of"] = values["valuation_time"] - timedelta(seconds=1)
    elif case == "output_after_cutoff":
        values["output_available_at"] = values["context_as_of"] + timedelta(seconds=1)
    elif case == "failed_but_complete":
        values["quality_status"] = "complete"
    else:
        values["unavailable_reason"] = "RESEARCH_ABSTAINED"
    with pytest.raises(ValidationError):
        FrozenDailyResearchContext.model_validate(values)


def _stored():
    context = FrozenDailyResearchContext.model_validate(_context())
    payload = {
        "schema_version": "optimization_draft_inputs_v2",
        "portfolio_snapshot_id": str(context.portfolio_snapshot_id),
        "diagnostic_report_id": str(context.diagnostic_report_id),
        "research_context": context.model_dump(mode="json"),
    }
    data = DataSnapshot(
        source="nexus_optimizer_draft",
        provider_object_id=f"{context.portfolio_snapshot_id}:{context.diagnostic_report_id}",
        schema_version=payload["schema_version"],
        payload=payload,
        observed_at=context.valuation_time,
        available_at=context.context_as_of,
        received_at=context.context_as_of,
    )
    _rehash(data)
    return data, dict(
        portfolio_snapshot_id=context.portfolio_snapshot_id,
        diagnostic_report_id=context.diagnostic_report_id,
        valuation_time=context.valuation_time,
    )


def _rehash(data):
    data.revision = hashlib.sha256(
        json.dumps(data.payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def test_reads_only_stored_context_and_preserves_legacy():
    data, binding = _stored()
    assert (
        read_draft_research_context(data, **binding).model_dump(mode="json")
        == data.payload["research_context"]
    )
    data.schema_version = "optimization_draft_inputs_v1"
    data.payload = {"schema_version": data.schema_version}
    assert read_draft_research_context(data, **binding) is None


@pytest.mark.parametrize(
    "case",
    ["hash", "schema", "source", "baseline", "diagnostic", "valuation", "receipt", "inner_id"],
)
def test_saved_context_rejects_corruption_and_mismatched_binding(case):
    data, binding = _stored()
    if case == "hash":
        data.payload["extra"] = "not originally frozen"
    elif case == "schema":
        data.schema_version = "optimization_draft_inputs_v1"
    elif case == "source":
        data.source = "untrusted"
    elif case in {"baseline", "diagnostic"}:
        key = "portfolio_snapshot_id" if case == "baseline" else "diagnostic_report_id"
        binding[key] = UUID(int=999)
    elif case == "valuation":
        data.observed_at += timedelta(seconds=1)
    elif case == "receipt":
        data.received_at -= timedelta(seconds=1)
    else:
        data.payload["research_context"]["portfolio_snapshot_id"] = str(UUID(int=999))
        _rehash(data)
    with pytest.raises(ValueError, match="^DRAFT_RESEARCH_OUTPUT_INVALID$"):
        read_draft_research_context(data, **binding)
