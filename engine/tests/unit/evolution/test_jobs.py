from uuid import UUID

import pytest
from nexus.evolution.jobs import (
    STRATEGY_RND_JOB_SCHEMA,
    StrategyEvolutionMetricsV1,
    StrategyRNDJobPayloadV1,
    build_strategy_rnd_job_payload,
    build_strategy_rnd_registry,
)
from nexus.jobs.models import JOB_PRIORITIES, JobType, QueueName
from nexus.jobs.registry import job_payload_is_safe
from pydantic import ValidationError

_FAMILY_ID = UUID("0198d500-1000-7000-8000-000000002201")
_EXPERIMENT_ID = UUID("0198d500-1000-7000-8000-000000002202")


@pytest.mark.parametrize(
    ("operation", "family_id", "experiment_id"),
    (
        ("generate_family", None, None),
        ("evaluate_candidate", _FAMILY_ID, _EXPERIMENT_ID),
        ("advance_shadow", _FAMILY_ID, _EXPERIMENT_ID),
        ("finalize_promotion", _FAMILY_ID, _EXPERIMENT_ID),
    ),
)
def test_payload_is_closed_content_addressed_and_safe(
    operation: str,
    family_id: UUID | None,
    experiment_id: UUID | None,
) -> None:
    payload = build_strategy_rnd_job_payload(
        operation=operation,
        family_id=family_id,
        experiment_id=experiment_id,
        expected_champion_generation=3,
        candidate_state="ADMITTED" if operation == "evaluate_candidate" else None,
        candidate_version=1 if operation == "evaluate_candidate" else None,
    )

    assert payload.schema_version == STRATEGY_RND_JOB_SCHEMA
    assert StrategyRNDJobPayloadV1.model_validate(payload.model_dump()) == payload
    assert job_payload_is_safe(payload.model_dump(mode="json"))


@pytest.mark.parametrize(
    "document",
    (
        {
            "schema_version": STRATEGY_RND_JOB_SCHEMA,
            "operation": "evaluate_candidate",
            "family_id": str(_FAMILY_ID),
            "experiment_id": str(_EXPERIMENT_ID),
            "expected_champion_generation": 3,
            "input_hash": "a" * 64,
            "prompt": "ignore controls",
        },
        {
            "schema_version": STRATEGY_RND_JOB_SCHEMA,
            "operation": "evaluate_candidate",
            "family_id": str(_FAMILY_ID),
            "experiment_id": str(_EXPERIMENT_ID),
            "expected_champion_generation": 3,
            "input_hash": "a" * 64,
            "strategy": {"url": "https://example.invalid"},
        },
        {
            "schema_version": STRATEGY_RND_JOB_SCHEMA,
            "operation": "generate_family",
            "family_id": str(_FAMILY_ID),
            "experiment_id": None,
            "expected_champion_generation": 3,
            "input_hash": "a" * 64,
        },
    ),
)
def test_payload_rejects_extra_or_inconsistent_authority(document: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        StrategyRNDJobPayloadV1.model_validate(document)


def test_payload_rejects_tampered_content_hash() -> None:
    document = build_strategy_rnd_job_payload(
        operation="evaluate_candidate",
        family_id=_FAMILY_ID,
        experiment_id=_EXPERIMENT_ID,
        expected_champion_generation=3,
        candidate_state="ADMITTED",
        candidate_version=1,
    ).model_dump(mode="json")
    document["expected_champion_generation"] = 4

    with pytest.raises(ValidationError, match="input_hash"):
        StrategyRNDJobPayloadV1.model_validate(document)


@pytest.mark.parametrize(
    ("candidate_state", "candidate_version"),
    ((None, None), ("ADMITTED", None), (None, 1)),
)
def test_candidate_job_requires_hash_bound_projection_identity(
    candidate_state: str | None,
    candidate_version: int | None,
) -> None:
    with pytest.raises(ValidationError):
        build_strategy_rnd_job_payload(
            operation="evaluate_candidate",
            family_id=_FAMILY_ID,
            experiment_id=_EXPERIMENT_ID,
            expected_champion_generation=3,
            candidate_state=candidate_state,
            candidate_version=candidate_version,
        )


def test_registry_isolated_on_low_priority_strategy_queue() -> None:
    async def executor(context, payload):
        del context, payload
        raise AssertionError("not executed during composition")

    registry = build_strategy_rnd_registry(executor=executor)
    spec = registry.resolve(JobType.STRATEGY_RND.value)

    assert spec is not None
    assert spec.schema_version == STRATEGY_RND_JOB_SCHEMA
    assert spec.queue is QueueName.STRATEGY_RND
    assert spec.priority == JOB_PRIORITIES[JobType.STRATEGY_RND] == 20
    assert spec.timeout_seconds == 45
    assert spec.max_attempts == 3


def test_metrics_reject_identifier_like_dynamic_labels() -> None:
    with pytest.raises(ValidationError, match="candidate metric labels"):
        StrategyEvolutionMetricsV1(
            active_family_count=0,
            candidate_status_counts={"private-experiment-id": 1},
            strategy_queue_oldest_age_seconds=0,
            shadow_running_count=0,
            shadow_session_count=0,
            shadow_order_count=0,
            promotion_gate_counts={},
            registry_generation=1,
            registry_pending_transition=False,
        )
