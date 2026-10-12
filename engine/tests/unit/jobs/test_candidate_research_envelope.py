"""The longer qualification envelope is closed to one exact Paper handler."""

from dataclasses import replace

import pytest
from nexus.agents.codex_versions import (
    CANDIDATE_BUDGET_CONFIG_VERSION,
    FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
    FULL_INPUT_CAPACITY_CONFIG_VERSION,
)
from nexus.jobs.models import JobType, QueueName, RetryPolicy
from nexus.jobs.registry import JobHandlerResult, JobHandlerSpec
from pydantic import BaseModel, ConfigDict


class Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    run_id: str


async def handler(_context):
    return JobHandlerResult(outcome="succeeded", result_code="TEST")


def candidate_spec(profile=CANDIDATE_BUDGET_CONFIG_VERSION):
    return JobHandlerSpec(
        job_type=JobType.AGENT_PAPER_RESEARCH,
        schema_version="agent_research_job_v1",
        payload_model=Payload,
        queue=QueueName.GENERAL,
        timeout_seconds=195,
        retry_policy=RetryPolicy.AUTOMATIC_SAFE,
        max_attempts=1,
        idempotency_namespace="agent.research",
        handler=handler,
        research_budget_profile=profile,
    )


def test_candidate_envelope_is_exact_and_legacy_cap_remains():
    spec = candidate_spec()
    assert spec.timeout_seconds == 195
    assert replace(spec, research_budget_profile=None, timeout_seconds=135).timeout_seconds == 135
    with pytest.raises(ValueError):
        replace(spec, research_budget_profile=None)


@pytest.mark.parametrize(
    "change",
    [
        {"research_budget_profile": "unknown"},
        {"job_type": JobType.AGENT_RESEARCH},
        {"timeout_seconds": 194},
        {"timeout_seconds": 196},
        {"max_attempts": 2},
        {"retry_policy": RetryPolicy.NEVER},
        {"schema_version": "other"},
        {"idempotency_namespace": "other"},
    ],
)
@pytest.mark.parametrize(
    "profile",
    [
        CANDIDATE_BUDGET_CONFIG_VERSION,
        FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
        FULL_INPUT_CAPACITY_CONFIG_VERSION,
    ],
)
def test_candidate_envelope_rejects_other_contracts(change, profile):
    with pytest.raises(ValueError, match="RESEARCH_BUDGET_PROFILE_INVALID"):
        replace(candidate_spec(profile), **change)
