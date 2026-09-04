"""Projection boundary only; synthetic mappings do not prove canonical authority."""

import sys
from copy import deepcopy
from datetime import UTC, datetime
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.api import research_inputs
from pydantic import ValidationError

from .test_research_trend_contracts import trend


def test_projection_preserves_exact_values_null_and_does_not_export_internal_originals():
    payload = trend()
    payload.update({"originals": [{"raw": "private-canary"}], "instrument_id": str(UUID(int=5))})
    before = deepcopy(payload)
    result = research_inputs._project_price_trend(
        payload, evidence_ref_id=UUID(int=1), revision="a" * 64, content_hash="a" * 64
    )
    assert result.model_dump(mode="json") == trend()
    assert "private-canary" not in result.model_dump_json()
    assert payload == before


def test_full_canonical_gap_list_is_display_bounded_without_losing_total():
    payload = trend()
    payload["missing_sessions"] = [f"2026-02-{day:02d}" for day in range(1, 23)]
    payload["expected_sessions"] = 182
    # These display-only fields need not exist in the full canonical representation.
    del payload["missing_sessions_total"]
    del payload["missing_sessions_truncated"]
    result = research_inputs._project_price_trend(
        payload, evidence_ref_id=UUID(int=1), revision="a" * 64, content_hash="a" * 64
    )
    assert result.model_dump(mode="json")["missing_sessions"] == payload["missing_sessions"][:20]
    assert result.missing_sessions_total == 22 and result.missing_sessions_truncated
    assert len(payload["missing_sessions"]) == 22


def test_tail_gap_keeps_feature_observation_date_distinct_from_query_end():
    payload = trend()
    payload.update(
        {
            "last_observed_on": "2026-08-27",
            "observed_sessions": 159,
            "missing_sessions": ["2026-07-01", "2026-08-28"],
        }
    )
    result = research_inputs._project_price_trend(
        payload, evidence_ref_id=UUID(int=1), revision="a" * 64, content_hash="a" * 64
    )
    assert result.last_observed_on.isoformat() == "2026-08-27"
    assert result.query_end.isoformat() == "2026-08-28"
    assert result.features.close == "123.456789"
    assert result.missing_sessions[-1].isoformat() == "2026-08-28"


@pytest.mark.parametrize("mutation", ["missing", "wrong_unit", "extra_feature", "bad_hash"])
def test_projection_cannot_invent_missing_fields_or_promote_incompatible_facts(mutation):
    payload = trend()
    if mutation == "missing":
        del payload["received_at"]
    elif mutation == "wrong_unit":
        payload["currency"] = "HKD"
    elif mutation == "extra_feature":
        payload["features"]["invented"] = "1"
    with pytest.raises((KeyError, ValidationError)):
        research_inputs._project_price_trend(
            payload,
            evidence_ref_id=UUID(int=1),
            revision="a" * 64,
            content_hash="b" * 64 if mutation == "bad_hash" else "a" * 64,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "defect", [None, "outside_ids", "late", "instrument", "missing", "reader_rejection"]
)
async def test_projection_boundary_passes_exact_refs_and_isolates_invalid_optional(
    monkeypatch, defect
):
    """Explicit reader double: verifies wiring only, not database/canonical validation."""
    payload = trend()
    available = datetime(2026, 8, 29, 0, 1, tzinfo=UTC)
    cutoff = datetime(2026, 8, 30, tzinfo=UTC)
    instrument = UUID(int=5)
    evidence = SimpleNamespace(revision="a" * 64, content_hash="a" * 64, available_at=available)
    frozen = AsyncMock(return_value={UUID(int=1): evidence})
    monkeypatch.setattr(research_inputs, "_frozen_issuer_references", frozen)
    if defect == "missing":
        del payload["features"]
    document = SimpleNamespace(
        instrument_id=UUID(int=99) if defect == "instrument" else instrument,
        available_at=cutoff if defect == "late" else available,
        model_dump=lambda **kwargs: payload,
    )
    reader = AsyncMock(
        return_value={UUID(int=2) if defect == "outside_ids" else UUID(int=1): document}
    )
    if defect == "reader_rejection":
        reader.side_effect = ValueError("synthetic-malformed-canonical")
    boundary = ModuleType("nexus.research.current_price_trends_storage")
    boundary.read_current_price_trends = reader
    monkeypatch.setitem(sys.modules, boundary.__name__, boundary)
    session = AsyncMock()
    result = await research_inputs._read_price_trend_inputs(
        session, snapshot_id=UUID(int=10), instrument_id=instrument, as_of=cutoff
    )
    reader.assert_awaited_once_with(
        session, instrument_id=instrument, as_of=cutoff, evidence_ids=(UUID(int=1),)
    )
    frozen.assert_awaited_once_with(
        session,
        snapshot_id=UUID(int=10),
        instrument_id=instrument,
        as_of=cutoff,
        source="alpaca_current_trends",
    )
    assert (len(result) == 1) is (defect is None)
    if result:
        assert result[0].model_dump(mode="json") == trend()


@pytest.mark.asyncio
async def test_no_frozen_trend_reference_never_discovers_current_data(monkeypatch):
    monkeypatch.setattr(research_inputs, "_frozen_issuer_references", AsyncMock(return_value={}))
    # Missing real module must not be fabricated or used to discover latest data.
    monkeypatch.setitem(sys.modules, "nexus.research.current_price_trends_storage", None)
    assert (
        await research_inputs._read_price_trend_inputs(
            AsyncMock(),
            snapshot_id=UUID(int=10),
            instrument_id=UUID(int=5),
            as_of=datetime(2026, 8, 30, tzinfo=UTC),
        )
        == ()
    )
