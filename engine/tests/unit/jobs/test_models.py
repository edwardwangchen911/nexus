from datetime import UTC, datetime

import pytest
from nexus.jobs.models import (
    JOB_PRIORITIES,
    JobKeyV1,
    JobStatus,
    JobType,
    QueueName,
    RetryPolicy,
    canonical_job_key,
    job_input_hash,
    require_job_transition,
)
from pydantic import ValidationError


def test_job_key_is_canonical_content_addressed_and_bounded() -> None:
    payload = {
        "data_snapshot_id": "snapshot-1",
        "policy_version": "risk_policy_v1",
        "parameters": {"quality_required": True, "attempt": 1},
    }
    input_hash = job_input_hash(payload)
    key = JobKeyV1(
        job_type=JobType.QUALITY_FREEZE,
        scope_kind="global",
        scope_id="global",
        effective_at=datetime(2026, 8, 27, 20, 30, tzinfo=UTC),
        input_hash=input_hash,
    )

    assert input_hash == job_input_hash(
        {
            "parameters": {"attempt": 1, "quality_required": True},
            "policy_version": "risk_policy_v1",
            "data_snapshot_id": "snapshot-1",
        }
    )
    assert canonical_job_key(key) == (
        "quality.freeze:global:global:2026-08-27T20:30:00.000000Z:"
        f"{input_hash}"
    )


@pytest.mark.parametrize(
    "changes",
    (
        {"effective_at": datetime(2026, 8, 27, 20, 30)},
        {"scope_id": "provider:external"},
        {"scope_id": "contains space"},
        {"scope_id": ""},
        {"input_hash": "A" * 64},
    ),
)
def test_job_key_rejects_ambiguous_or_non_utc_values(changes: dict[str, object]) -> None:
    values: dict[str, object] = {
        "job_type": JobType.DIAGNOSTICS_RUN,
        "scope_kind": "portfolio",
        "scope_id": "71000000-0000-0000-0000-000000000001",
        "effective_at": datetime(2026, 8, 27, 20, 30, tzinfo=UTC),
        "input_hash": "a" * 64,
    }
    values.update(changes)

    with pytest.raises(ValidationError):
        JobKeyV1.model_validate(values)


def test_job_input_hash_rejects_non_deterministic_values() -> None:
    with pytest.raises(TypeError, match="binary floats"):
        job_input_hash({"ratio": 0.1})
    with pytest.raises(TypeError, match="string keys"):
        job_input_hash({1: "invalid"})  # type: ignore[dict-item]


@pytest.mark.parametrize(
    ("source", "target"),
    (
        (JobStatus.QUEUED, JobStatus.RUNNING),
        (JobStatus.RUNNING, JobStatus.SUCCEEDED),
        (JobStatus.RUNNING, JobStatus.SKIPPED),
        (JobStatus.RUNNING, JobStatus.FAILED),
        (JobStatus.FAILED, JobStatus.QUEUED),
        (JobStatus.FAILED, JobStatus.DEAD_LETTER),
        (JobStatus.RUNNING, JobStatus.QUEUED),
    ),
)
def test_job_state_machine_accepts_only_documented_edges(
    source: JobStatus, target: JobStatus
) -> None:
    assert require_job_transition(source, target) is target


@pytest.mark.parametrize(
    ("source", "target"),
    (
        (JobStatus.QUEUED, JobStatus.SUCCEEDED),
        (JobStatus.SUCCEEDED, JobStatus.QUEUED),
        (JobStatus.SKIPPED, JobStatus.RUNNING),
        (JobStatus.DEAD_LETTER, JobStatus.RUNNING),
        (JobStatus.FAILED, JobStatus.SUCCEEDED),
    ),
)
def test_job_state_machine_rejects_bypasses(source: JobStatus, target: JobStatus) -> None:
    with pytest.raises(ValueError, match="transition"):
        require_job_transition(source, target)


def test_queues_priorities_and_retry_policies_are_closed() -> None:
    assert set(QueueName) == {
        QueueName.GENERAL,
        QueueName.PROVIDER,
        QueueName.BROKER,
        QueueName.STRATEGY_RND,
        QueueName.AGENT_PAPER,
    }
    assert set(RetryPolicy) == {
        RetryPolicy.AUTOMATIC_SAFE,
        RetryPolicy.MANUAL_SAFE,
        RetryPolicy.NEVER,
    }
    assert JOB_PRIORITIES[JobType.BROKER_SYNC] == 300
    assert JOB_PRIORITIES[JobType.QUALITY_FREEZE] == 280
    assert JOB_PRIORITIES[JobType.PAPER_EXECUTE_DUE] == 260
    assert JOB_PRIORITIES[JobType.PAPER_PERFORMANCE] == 220
    assert JOB_PRIORITIES[JobType.AGENT_RESEARCH] == 180
    assert JobType.AGENT_PORTFOLIO_DECISION.value == "agent.portfolio_decision"
    assert JOB_PRIORITIES[JobType.AGENT_PORTFOLIO_DECISION] == 180
    assert JOB_PRIORITIES[JobType.STRATEGY_RND] == 20
    assert set(JOB_PRIORITIES) == set(JobType)
