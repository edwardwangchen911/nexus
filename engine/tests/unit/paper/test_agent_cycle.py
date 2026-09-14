from datetime import UTC, datetime
from uuid import UUID

import pytest
from nexus.paper.agent_cycle_models import (
    CandidateManifestEntry,
    candidate_manifest_hash,
    canonical_cycle_event_hash,
    floor_decision_slot,
)
from pydantic import ValidationError


def test_floor_decision_slot_uses_utc_five_minute_boundary() -> None:
    assert floor_decision_slot(datetime(2026, 9, 14, 14, 7, 59, tzinfo=UTC)) == datetime(
        2026, 9, 14, 14, 5, tzinfo=UTC
    )


def test_floor_decision_slot_rejects_naive_time() -> None:
    with pytest.raises(ValueError, match="AGENT_PAPER_REQUEST_INVALID"):
        floor_decision_slot(datetime(2026, 9, 14, 14, 7, 59))


def _candidate(*, evidence_hash: str = "a" * 64) -> CandidateManifestEntry:
    return CandidateManifestEntry(
        instrument_id=UUID(int=1),
        symbol="AAPL",
        inclusion_reason="EXACT_US_LIQUID_V1",
        research_snapshot_id=UUID(int=2),
        prior_daily_price_authority_id=UUID(int=3),
        decision_quote_id=UUID(int=4),
        decision_trade_id=UUID(int=5),
        decision_artifact_id=UUID(int=6),
        decision_evidence_hash=evidence_hash,
        risk_query_authority_ids=(UUID(int=7), UUID(int=8), UUID(int=9)),
    )


def test_candidate_manifest_hash_binds_every_frozen_authority() -> None:
    candidate = _candidate()

    for field, value in (
        ("prior_daily_price_authority_id", UUID(int=30)),
        ("decision_quote_id", UUID(int=40)),
        ("decision_trade_id", None),
        ("decision_artifact_id", UUID(int=60)),
        ("decision_evidence_hash", "b" * 64),
        ("risk_query_authority_ids", (UUID(int=70), UUID(int=80), UUID(int=90))),
    ):
        assert candidate_manifest_hash((candidate,)) != candidate_manifest_hash(
            (candidate.model_copy(update={field: value}),)
        )


def test_candidate_manifest_only_accepts_aapl_and_exactly_three_risk_authorities() -> None:
    payload = _candidate().model_dump()
    with pytest.raises(ValidationError):
        CandidateManifestEntry.model_validate({**payload, "symbol": "MSFT"})
    with pytest.raises(ValidationError):
        CandidateManifestEntry.model_validate(
            {**payload, "risk_query_authority_ids": [str(UUID(int=7))]}
        )
    with pytest.raises(ValidationError):
        CandidateManifestEntry.model_validate(
            {**payload, "decision_evidence_hash": "not-a-hash"}
        )


def test_cycle_event_hash_binds_prior_status_facts_and_time() -> None:
    occurred_at = datetime(2026, 8, 26, 21, tzinfo=UTC)
    base = canonical_cycle_event_hash(
        cycle_id=UUID(int=1),
        sequence=2,
        from_status="prepared",
        to_status="researching",
        result_code="RESEARCH_JOBS_PREPARED",
        fact_ids={"research_run_ids": [str(UUID(int=2))]},
        occurred_at=occurred_at,
    )

    changed = canonical_cycle_event_hash(
        cycle_id=UUID(int=1),
        sequence=2,
        from_status="prepared",
        to_status="researching",
        result_code="RESEARCH_JOBS_PREPARED",
        fact_ids={"research_run_ids": [str(UUID(int=3))]},
        occurred_at=occurred_at,
    )

    assert len(base) == 64
    assert base != changed
