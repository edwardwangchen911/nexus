"""Frozen macro context uses source clocks and rejects conflicting ties."""

from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.agents.database_tools import _macro_record_valid
from nexus.agents.snapshots import _macro_context_ids
from nexus.persistence.models.data import MacroObservation
from nexus.research.fred_storage import FredResearchObservation


def observation(identity=1, **changes):
    return replace(
        FredResearchObservation(
            observation_id=UUID(int=identity),
            evidence_ref_id=UUID(int=identity),
            calendar_evidence_ref_id=UUID(int=100),
            series="DGS10",
            observed_on=date(2026, 8, 24),
            value=Decimal("4.125"),
            unit="Percent",
            vintage=date(2026, 8, 28),
            available_at=datetime(2026, 9, 4, tzinfo=UTC),
            received_at=datetime(2026, 9, 4, tzinfo=UTC),
            raw_sha256="a" * 64,
            canonical_sha256=f"{identity:064x}",
        ),
        **changes,
    )


@pytest.mark.parametrize(
    "change",
    [
        {"observed_on": date(2026, 8, 25)},
        {"vintage": date(2026, 8, 29)},
        {"received_at": datetime(2026, 9, 4, tzinfo=UTC) + timedelta(seconds=1)},
    ],
)
def test_source_clocks_outrank_uuid_and_both_references_are_selected(change):
    old = observation(identity=9)
    current = observation(identity=1, **change)
    assert _macro_context_ids({old.evidence_ref_id: old, current.evidence_ref_id: current}) == {
        current.evidence_ref_id,
        current.calendar_evidence_ref_id,
    }


def test_conflicting_latest_tie_does_not_hide_an_independent_series():
    first = observation()
    conflict = observation(identity=2, value=Decimal("9"))
    independent = observation(identity=3, series="DGS2")
    result = _macro_context_ids(
        {row.evidence_ref_id: row for row in (first, conflict, independent)}
    )
    assert result == {independent.evidence_ref_id, independent.calendar_evidence_ref_id}


def test_equivalent_ties_are_stable_and_shared_calendar_is_not_duplicated():
    first, duplicate = observation(), observation(identity=2)
    forward = {row.evidence_ref_id: row for row in (first, duplicate)}
    reverse = {row.evidence_ref_id: row for row in (duplicate, first)}
    assert (
        _macro_context_ids(forward)
        == _macro_context_ids(reverse)
        == {
            first.evidence_ref_id,
            first.calendar_evidence_ref_id,
        }
    )
    assert _macro_context_ids({}) == set()


def test_new_schema_cannot_fall_back_to_legacy_or_borrow_another_observation():
    verified = observation()
    row = MacroObservation(
        id=verified.observation_id, evidence_ref_id=verified.evidence_ref_id, revision="legacy"
    )
    assert _macro_record_valid(row, {}, set())  # Original legacy protocol remains readable.
    assert not _macro_record_valid(row, {}, {verified.evidence_ref_id})
    documents = {verified.evidence_ref_id: verified}
    assert _macro_record_valid(row, documents, {verified.evidence_ref_id})
    row.id = UUID(int=99)
    assert not _macro_record_valid(row, documents, {verified.evidence_ref_id})
    row.revision = "local-corrupt"
    assert not _macro_record_valid(row, {}, set())
