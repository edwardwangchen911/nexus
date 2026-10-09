"""Formal synthesis scope is explicit and rejected before any connection or attempt."""

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from nexus.agents.codex_formal_stage import formal_stage_config, formal_stage_config_hash
from nexus.agents.portfolio_synthesis_worker import CodexSynthesisTransport
from nexus.jobs.models import JobType, QueueName
from nexus.jobs.repository import claim_next_job
from nexus.settings import EngineSettings
from nexus.worker import run_portfolio_synthesis_worker_once


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"formal_stage_capacity": 1},
        {"formal_stage_capacity": "true"},
        {"candidate_stage_capacity": True},
        {"candidate_stage_capacity": 0},
        {"job_ids": ()},
        {"job_ids": (UUID(int=1), UUID(int=2))},
        {"job_ids": ("invalid",)},
        {"job_ids": None},
    ],
)
async def test_formal_synthesis_scope_fails_before_factory(change, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("factory must not be constructed")

    monkeypatch.setattr("nexus.worker.session_factory", forbidden)
    options = dict(job_ids=(UUID(int=1),), formal_stage_capacity=True)
    settings = EngineSettings(
        database_url="postgresql+psycopg://localhost/nexus_test_formal",
        agent_runtime_mode="codex_sdk",
    )
    with pytest.raises(ValueError, match="CODEX_FORMAL_WORKER_ARGUMENT_INVALID"):
        await run_portfolio_synthesis_worker_once(settings, **(options | change))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "url",
    [
        "postgresql+psycopg://remote.invalid/nexus_test_formal",
        "postgresql+psycopg://localhost/nexus_test_formal?host=remote.invalid",
        "postgresql+psycopg://localhost/nexus_test_formal?dbname=other",
        "postgresql+psycopg://localhost/nexus_test_formal?port=6543",
    ],
)
async def test_formal_synthesis_connection_redirect_fails_before_factory(url, monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("factory must not be constructed")

    monkeypatch.setattr("nexus.worker.session_factory", forbidden)
    with pytest.raises(ValueError, match="CODEX_FORMAL_WORKER_ARGUMENT_INVALID"):
        await run_portfolio_synthesis_worker_once(
            EngineSettings(database_url=url, agent_runtime_mode="codex_sdk"),
            job_ids=(UUID(int=1),),
            formal_stage_capacity=True,
        )


def test_formal_transport_requires_explicit_canonical_identity():
    values = formal_stage_config()
    model = SimpleNamespace(**values)
    model.config_hash = formal_stage_config_hash()
    model.pricing_available_at = datetime.fromisoformat(values["pricing_available_at"])
    model.pricing_expires_at = datetime.fromisoformat(values["pricing_expires_at"])
    model.input_usd_per_million = model.output_usd_per_million = 0
    assert not CodexSynthesisTransport().supports(model)
    assert not CodexSynthesisTransport(candidate_stage_capacity=True).supports(model)
    assert CodexSynthesisTransport(formal_stage_capacity=True).supports(model)
    model.config_hash = "0" * 64
    assert not CodexSynthesisTransport(formal_stage_capacity=True).supports(model)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change",
    [
        {"formal_synthesis_profile": 1},
        {"candidate_synthesis_profile": True},
        {"locked_job_guard": None},
        {"lease_seconds": 119},
        {"queue": QueueName.PROVIDER},
        {"job_ids": ()},
        {"job_types": (JobType.AGENT_PAPER_RESEARCH.value,)},
    ],
)
async def test_formal_claim_requires_locked_exact_scope_before_queries(change):
    session = AsyncMock()
    args = dict(
        queue=QueueName.GENERAL,
        worker_id="formal-synthesis-test",
        now=datetime(2026, 10, 9, 14, tzinfo=UTC),
        lease_seconds=120,
        job_types=(JobType.AGENT_PORTFOLIO_SYNTHESIS.value,),
        job_ids=(UUID(int=1),),
        locked_job_guard=AsyncMock(return_value=True),
        formal_synthesis_profile=True,
    )
    with pytest.raises(ValueError, match="FORMAL_SYNTHESIS|LOCKED_JOB_GUARD"):
        await claim_next_job(session, **(args | change))
    assert session.mock_calls == []
