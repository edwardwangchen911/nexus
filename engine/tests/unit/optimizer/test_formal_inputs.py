"""Formal inputs separate valuation, evidence cutoff and actual computation clocks."""

import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.optimizer.jobs import OptimizationConstructionPayload
from pydantic import ValidationError


def test_legacy_job_payload_does_not_gain_null_formal_binding():
    payload = {
        name: str(UUID(int=i))
        for i, name in enumerate(("portfolio_snapshot_id", "data_snapshot_id", "cost_policy_id"), 1)
    }
    assert (
        OptimizationConstructionPayload.model_validate(payload).model_dump(mode="json") == payload
    )


def test_formal_job_binds_the_exact_manifest_and_rejects_incomplete_binding():
    payload = {
        name: str(UUID(int=i))
        for i, name in enumerate(("portfolio_snapshot_id", "data_snapshot_id", "cost_policy_id"), 1)
    }
    payload["formal_binding"] = {
        "schema_version": "optimization_formal_binding_v2",
        "admission_authority_id": str(UUID(int=4)),
        "data_snapshot_id": str(UUID(int=2)),
        "data_snapshot_hash": "a" * 64,
        "decision_cutoff_at": "2026-08-28T21:00:00Z",
    }
    result = OptimizationConstructionPayload.model_validate(payload)
    assert result.model_dump(mode="json") == payload
    assert (
        OptimizationConstructionPayload.model_validate_json(json.dumps(payload), strict=True)
        == result
    )
    payload["formal_binding"]["data_snapshot_id"] = str(UUID(int=999))
    with pytest.raises(ValidationError):
        OptimizationConstructionPayload.model_validate(payload)


def test_explicit_null_formal_binding_cannot_fall_back_to_legacy():
    payload = {
        name: str(UUID(int=i))
        for i, name in enumerate(("portfolio_snapshot_id", "data_snapshot_id", "cost_policy_id"), 1)
    }
    with pytest.raises(ValidationError):
        OptimizationConstructionPayload.model_validate({**payload, "formal_binding": None})


def _formal_payload():
    at = datetime(2026, 8, 28, 21, tzinfo=UTC)
    references = {
        "market": {
            "decision_session": "2026-08-28",
            "execution_session": "2026-08-31",
            "decision_time": at,
            "source_policy_id": UUID(int=10),
            "calendar_authority_id": UUID(int=11),
            "universe_snapshot_id": UUID(int=12),
            "quality_report_id": UUID(int=13),
        },
        "raw_bar_set_ids": [UUID(int=14)],
        "adjusted_bar_set_ids": [UUID(int=15)],
        "sector_exposure_ids": [],
    }
    from nexus.strategy.snapshot_reader import FrozenStrategyReferences

    binding = {
        "instrument_id": UUID(int=20),
        "research_data_snapshot_id": UUID(int=21),
        "run_id": UUID(int=22),
        "output_id": UUID(int=23),
        "output_sequence": 1,
        "output_hash": "b" * 64,
        "evidence_hash": "c" * 64,
        "actual_available_at": at,
        "portfolio_snapshot_id": UUID(int=1),
        "research_as_of": at - timedelta(minutes=1),
    }
    payload = {
        "schema_version": "optimization_formal_inputs_v2",
        "portfolio_snapshot_id": UUID(int=1),
        "valuation_time": at - timedelta(minutes=2),
        "decision_cutoff_at": at,
        "materialized_at": at + timedelta(seconds=1),
        "diagnostic_report_id": UUID(int=2),
        "diagnostic_input_hash": "d" * 64,
        "research_bindings": [binding],
        "strategy_references": references,
        "strategy_references_hash": FrozenStrategyReferences.model_validate(
            references
        ).content_hash,
    }
    return payload, binding, at


def test_formal_manifest_rejects_backdated_computation_and_duplicate_research():
    from nexus.optimizer.formal_inputs import FormalOptimizationInputs

    payload, binding, at = _formal_payload()
    result = FormalOptimizationInputs.model_validate(payload)
    assert result.research_bindings[0].research_data_snapshot_id == UUID(int=21)
    for changes in (
        {"materialized_at": at - timedelta(microseconds=1)},
        {"research_bindings": [binding, binding]},
        {"strategy_references_hash": "f" * 64},
        {"research_bindings": [{**binding, "actual_available_at": at + timedelta(microseconds=1)}]},
        {"research_bindings": [{**binding, "portfolio_snapshot_id": UUID(int=999)}]},
    ):
        with pytest.raises(ValidationError):
            FormalOptimizationInputs.model_validate({**payload, **changes})


@pytest.mark.parametrize("offset_seconds", [-120, -60, 0])
def test_formal_manifest_keeps_original_strategy_clock_before_later_research_cutoff(
    offset_seconds: int,
):
    from nexus.optimizer.formal_inputs import FormalOptimizationInputs
    from nexus.strategy.snapshot_reader import FrozenStrategyReferences

    payload, _, cutoff = _formal_payload()
    market_time = cutoff + timedelta(seconds=offset_seconds)
    payload["strategy_references"]["market"]["decision_time"] = market_time
    references = FrozenStrategyReferences.model_validate(payload["strategy_references"])
    payload["strategy_references_hash"] = references.content_hash

    result = FormalOptimizationInputs.model_validate(payload)

    assert result.strategy_references.market.decision_time == market_time
    assert result.decision_cutoff_at == cutoff
    assert result.research_bindings[0].actual_available_at == cutoff
    assert result.materialized_at == cutoff + timedelta(seconds=1)
    assert result.strategy_references_hash == references.content_hash


@pytest.mark.parametrize("offset_seconds", [-121, 1])
def test_formal_manifest_rejects_strategy_clock_outside_valuation_and_cutoff(
    offset_seconds: int,
):
    from nexus.optimizer.formal_inputs import FormalOptimizationInputs
    from nexus.strategy.snapshot_reader import FrozenStrategyReferences

    payload, _, cutoff = _formal_payload()
    payload["strategy_references"]["market"]["decision_time"] = cutoff + timedelta(
        seconds=offset_seconds
    )
    payload["strategy_references_hash"] = FrozenStrategyReferences.model_validate(
        payload["strategy_references"]
    ).content_hash

    with pytest.raises(ValidationError, match="OPTIMIZATION_FORMAL_BINDING_INVALID"):
        FormalOptimizationInputs.model_validate(payload)


@pytest.mark.asyncio
async def test_formal_groups_cannot_gain_shadow_permission_from_legacy_flags():
    from nexus.optimizer.repository import has_executable_shadow_authority
    from nexus.persistence.models.optimization import OptimizationProposalGroupRecord

    class NoRead:
        async def scalar(self, statement):
            pytest.fail("a formal group must be rejected before reading legacy authority flags")

    group = OptimizationProposalGroupRecord(
        diagnostic_report_id=UUID(int=1),
        data_snapshot_id=UUID(int=2),
        source_policy_id=UUID(int=3),
        cost_policy_id=UUID(int=4),
        diagnostic_input_hash="a" * 64,
        valuation_time=datetime(2026, 8, 28, tzinfo=UTC),
        decision_cutoff_at=datetime(2026, 8, 29, tzinfo=UTC),
        baseline_ready=True,
        diagnostic_complete=True,
        source_conflict=False,
        partial=False,
        requires_fx=False,
        cost_complete=True,
        authority_stale=False,
    )
    assert not await has_executable_shadow_authority(NoRead(), group)


def test_formal_request_does_not_accept_readiness_or_targets_from_the_caller():
    from nexus.optimizer.formal_producer import FormalOptimizationRequest

    assert "baseline_ready" not in FormalOptimizationRequest.model_fields
    assert "portfolio_scope" not in FormalOptimizationRequest.model_fields
    assert "cost_complete" not in FormalOptimizationRequest.model_fields
    assert "targets" not in FormalOptimizationRequest.model_fields
