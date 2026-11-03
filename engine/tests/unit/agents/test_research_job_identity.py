"""Keep the pre-refactor native Job hash readable by both historical proof paths."""

from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

import pytest
from nexus.agents.codex_candidate import _candidate_job_hash
from nexus.agents.codex_formal_stage import _formal_stage_job_hash
from nexus.persistence.models.agents import AgentResearchRunRecord

# Produced by both pre-refactor readers at eeb1e6f1. Keep this independent of the writer.
EXPECTED = "2eda0c0b7ad86830481d68b9f9882c77b7a78e0652713b471a5e1502a1ac3712"


@pytest.mark.parametrize("zone", [UTC, timezone(timedelta(hours=8))])
def test_historical_job_identity_preserves_utc_microseconds_and_frozen_fields(zone):
    run = AgentResearchRunRecord(
        instrument_id=UUID(int=1),
        data_snapshot_id=UUID(int=2),
        decision_time=datetime(2026, 1, 2, 3, 4, 5, 6789, tzinfo=UTC).astimezone(zone),
        input_hash="1" * 64,
        schema_hash="2" * 64,
        model_hash="3" * 64,
        prompt_hash="4" * 64,
        evidence_hash="5" * 64,
        tool_hash="6" * 64,
        pricing_version="fixture-price-v1",
    )
    assert _candidate_job_hash(run) == _formal_stage_job_hash(run) == EXPECTED
