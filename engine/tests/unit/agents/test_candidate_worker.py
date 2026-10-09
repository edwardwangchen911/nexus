"""Candidate composition is explicit and rejects broad or non-SDK execution."""

from uuid import UUID

import pytest
from nexus.agents.codex_versions import CANDIDATE_BUDGET_CONFIG_VERSION
from nexus.jobs.models import JobType
from nexus.settings import EngineSettings
from nexus.worker import _agent_registry, run_agent_worker_once


def test_candidate_registry_owns_only_paper_research():
    registry = _agent_registry(
        EngineSettings(agent_runtime_mode="codex_sdk"),
        responses_client=None,
        tools=None,
        include_agent_paper=True,
        candidate_stage_capacity=True,
    )
    assert registry.resolve(JobType.AGENT_RESEARCH.value) is None
    spec = registry.resolve(JobType.AGENT_PAPER_RESEARCH.value)
    assert spec.timeout_seconds == 195 and spec.max_attempts == 1
    assert spec.research_budget_profile == CANDIDATE_BUDGET_CONFIG_VERSION


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode, scope",
    [
        ("codex_sdk", None),
        ("codex_sdk", ()),
        ("codex_sdk", (UUID(int=1), UUID(int=2))),
        ("fixture", (UUID(int=1),)),
    ],
)
async def test_candidate_worker_rejects_invalid_scope_before_database_access(mode, scope):
    with pytest.raises(ValueError, match="CODEX_CANDIDATE_WORKER_ARGUMENT_INVALID"):
        await run_agent_worker_once(
            EngineSettings(agent_runtime_mode=mode), job_ids=scope, candidate_stage_capacity=True
        )
