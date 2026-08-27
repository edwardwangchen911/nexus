"""Strategy Shadow availability cutoffs never admit future evidence."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

from hypothesis import given
from hypothesis import strategies as st
from nexus.evolution.shadow.authority import require_available_by


@given(st.integers(min_value=1, max_value=86_400))
def test_future_evidence_is_rejected_by_decision_cutoff(offset_seconds: int) -> None:
    cutoff = datetime(2026, 9, 1, 20, tzinfo=UTC)

    assert not require_available_by(
        authority_id=UUID("019d0000-0000-7000-8000-000000006301"),
        available_at=cutoff + timedelta(seconds=offset_seconds),
        cutoff=cutoff,
    )
    assert require_available_by(
        authority_id=UUID("019d0000-0000-7000-8000-000000006301"),
        available_at=cutoff,
        cutoff=cutoff,
    )
