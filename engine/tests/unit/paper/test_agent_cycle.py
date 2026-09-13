from datetime import UTC, datetime
from uuid import UUID

from nexus.paper.agent_cycle_models import (
    CandidateAuthority,
    candidate_manifest_hash,
    canonical_cycle_event_hash,
    select_candidate_manifest,
)


def _candidate(index: int, *, paper_holding: bool = False) -> CandidateAuthority:
    return CandidateAuthority(
        instrument_id=UUID(int=index),
        symbol=f"T{index}",
        inclusion_reason="EXACT_US_LIQUID_V1",
        research_snapshot_id=UUID(int=100 + index),
        paper_holding=paper_holding,
    )


def test_candidate_manifest_is_unique_deterministic_and_bounded() -> None:
    candidates = tuple(_candidate(index) for index in range(8, 0, -1)) + (_candidate(1),)

    manifest = select_candidate_manifest(candidates)

    assert [row.instrument_id for row in manifest] == [UUID(int=index) for index in range(1, 6)]
    assert len(manifest) == 5
    assert candidate_manifest_hash(manifest) == candidate_manifest_hash(tuple(reversed(manifest)))


def test_candidate_manifest_prioritizes_internal_paper_holdings() -> None:
    manifest = select_candidate_manifest(
        tuple(_candidate(index, paper_holding=index == 9) for index in range(1, 10))
    )

    assert manifest[0].instrument_id == UUID(int=9)


def test_candidate_manifest_rejects_conflicting_authority_for_one_instrument() -> None:
    first = _candidate(1)
    changed = first.model_copy(update={"symbol": "CHANGED"})

    try:
        select_candidate_manifest((first, changed))
    except ValueError as error:
        assert str(error) == "CANDIDATE_AUTHORITY_CONFLICT"
    else:
        raise AssertionError("conflicting candidate authority must fail closed")


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
