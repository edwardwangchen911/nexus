"""Formal capacity composition must preserve its absolute envelope and closed scope."""

from uuid import UUID

import pytest
from nexus.agents.codex_versions import FORMAL_STAGE_CAPACITY_CONFIG_VERSION, stage_timeout_seconds
from nexus.jobs.models import JobType
from nexus.settings import EngineSettings
from nexus.worker import _agent_registry, run_agent_worker_once


def test_formal_registry_owns_exact_paper_profile_without_changing_default():
    settings = EngineSettings(agent_runtime_mode="codex_sdk")
    registry = _agent_registry(
        settings,
        responses_client=None,
        tools=None,
        include_agent_paper=True,
        formal_stage_capacity=True,
    )
    assert registry.resolve(JobType.AGENT_RESEARCH.value) is None
    spec = registry.resolve(JobType.AGENT_PAPER_RESEARCH.value)
    assert spec.timeout_seconds == 195 and spec.max_attempts == 1
    assert spec.research_budget_profile == FORMAL_STAGE_CAPACITY_CONFIG_VERSION
    default = _agent_registry(settings, responses_client=None, tools=None, include_agent_paper=True)
    assert default.resolve(JobType.AGENT_PAPER_RESEARCH.value).timeout_seconds == 135


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scope, mode, candidate",
    [
        (None, "codex_sdk", False),
        ((), "codex_sdk", False),
        ((UUID(int=1), UUID(int=2)), "codex_sdk", False),
        ((UUID(int=1),), "fixture", False),
        ((UUID(int=1),), "codex_sdk", True),
    ],
)
async def test_formal_worker_rejects_invalid_scope_before_connecting(scope, mode, candidate):
    with pytest.raises(ValueError, match="CODEX_FORMAL_WORKER_ARGUMENT_INVALID"):
        await run_agent_worker_once(
            EngineSettings(agent_runtime_mode=mode),
            job_ids=scope,
            formal_stage_capacity=True,
            candidate_stage_capacity=candidate,
        )


def test_formal_allocator_has_own_version_and_uses_original_deadline():
    version = FORMAL_STAGE_CAPACITY_CONFIG_VERSION
    assert stage_timeout_seconds("research", deadline=175, now=0, version=version) == 60
    assert stage_timeout_seconds("research", deadline=175, now=10, version=version) == 54
    assert stage_timeout_seconds("critique", deadline=175, now=67, version=version) == 45
    assert stage_timeout_seconds("synthesis", deadline=175, now=119, version=version) == 45
    assert stage_timeout_seconds("research", deadline=175, now=64, version=version) == 0


@pytest.mark.asyncio
async def test_formal_committee_keeps_worker_origin_and_frozen_model(monkeypatch):
    from datetime import datetime
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from nexus import worker
    from nexus.agents import codex_staged
    from nexus.agents.codex_formal_stage import formal_stage_config, formal_stage_config_hash
    from nexus.agents.price_authority import FrozenPriceAuthorityStatus
    from unit.agents.test_codex_sdk import _authority, _tools

    _, evidence, prompts = _authority()
    config = formal_stage_config()
    model = SimpleNamespace(**{k: v for k, v in config.items() if k != "stage_time_budget"})
    model.config_hash = formal_stage_config_hash()
    model.input_usd_per_million = model.output_usd_per_million = 0
    model.pricing_available_at = datetime.fromisoformat(config["pricing_available_at"])
    model.pricing_expires_at = datetime.fromisoformat(config["pricing_expires_at"])
    run = SimpleNamespace(
        model_config_id=UUID(int=1), model_hash=model.config_hash, pricing_version=model.version
    )
    session = SimpleNamespace(get=AsyncMock(return_value=model))
    monkeypatch.setattr(worker, "load_frozen_evidence", AsyncMock(return_value=evidence))
    monkeypatch.setattr(worker, "load_frozen_prompts", AsyncMock(return_value=prompts))
    monkeypatch.setattr(
        worker,
        "revalidate_frozen_price_authority",
        AsyncMock(return_value=FrozenPriceAuthorityStatus.VALID),
    )
    captured = {}

    def build(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace()

    monkeypatch.setattr(codex_staged, "StructuredStagedCodexCommittee", build)
    settings = EngineSettings(agent_runtime_mode="codex_sdk")
    await worker._committee(
        session,
        settings,
        run,
        responses_client=None,
        tools=_tools(),
        deadline=1195,
        formal_stage_capacity=True,
    )
    assert captured["deadline"] == 1180  # Original handler origin, not construction time.
    assert captured["stage_budget_version"] == FORMAL_STAGE_CAPACITY_CONFIG_VERSION
    assert captured["paper_stage_scope"] and captured["inline_citations"]
    assert captured["aggregate_input_tokens"] == 96000
    captured.clear()
    await worker._committee(
        session, settings, run, responses_client=None, tools=_tools(), deadline=1195
    )
    assert not captured
    model.max_input_tokens = 1
    await worker._committee(
        session,
        settings,
        run,
        responses_client=None,
        tools=_tools(),
        deadline=1195,
        formal_stage_capacity=True,
    )
    assert not captured


@pytest.mark.asyncio
@pytest.mark.parametrize("query", ["host=example.invalid", "port=9999", "database=other"])
async def test_formal_worker_rejects_connection_overrides_before_access(monkeypatch, query):
    from nexus import worker

    def forbidden(*a, **kw):
        raise AssertionError("reached database with a redirected connection")

    monkeypatch.setattr(worker, "session_factory", forbidden)
    with pytest.raises(ValueError, match="CODEX_FORMAL_WORKER_ARGUMENT_INVALID"):
        await run_agent_worker_once(
            EngineSettings(
                agent_runtime_mode="codex_sdk",
                database_url="postgresql+asyncpg://127.0.0.1/nexus_diag_formal?" + query,
            ),
            job_ids=(UUID(int=1),),
            formal_stage_capacity=True,
        )
