"""A wall-clock lease cannot be extended by a paused monotonic SDK clock."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from nexus import worker
from nexus.agents import codex_sdk, codex_staged
from nexus.agents.committee import ROLE_ORDER
from nexus.agents.research_stages import executor as staged_executor
from nexus.agents.responses_budget import UsageSnapshot
from nexus.jobs.models import JobType
from nexus.jobs.registry import JobHandlerContext
from nexus.jobs.repository import LeaseLostError
from nexus.settings import EngineSettings
from unit.agents.test_codex_sdk import _authority, _sdk, _staged_batch, _tools


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expired_stage,completed_roles", [("research", 0), ("critique", 5), ("synthesis", 7)]
)
async def test_stages_stop_dispatch_after_wall_clock_lease_expires(
    monkeypatch, expired_stage, completed_roles
):
    request, evidence, prompts = _authority()
    now = datetime(2026, 10, 21, tzinfo=UTC)
    lease_end = now + timedelta(seconds=330)
    sent = []

    def guard():
        if now >= lease_end:
            raise LeaseLostError("job lease expired before dispatch")

    async def invoke(payload, **kwargs):
        sent.append(payload["stage"])
        return json.dumps(
            _staged_batch(payload["stage"], payload["requested_roles"])
        ), UsageSnapshot(input_tokens=300, output_tokens=80, provider_calls=1)

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    committee = codex_staged.StagedCodexCommittee(
        prompts=prompts, tools=_tools(), deadline=asyncio.get_running_loop().time() + 295
    )
    committee.dispatch_guard = guard
    for role in ROLE_ORDER[:completed_roles]:
        await committee.analyze(role, request, evidence, prompts[role])
    prior = committee.usage
    now = lease_end  # Wall time advances; monotonic allowance remains positive.
    role = ROLE_ORDER[completed_roles]
    with pytest.raises(LeaseLostError):
        await committee.analyze(role, request, evidence, prompts[role])
    assert expired_stage not in sent
    assert committee.usage == prior  # No fictitious call or zeroed earlier usage.
    assert committee._deadline > asyncio.get_running_loop().time()


@pytest.mark.asyncio
async def test_sdk_rechecks_lease_after_thread_start_before_model_dispatch(monkeypatch):
    client, thread, _, _ = _sdk(monkeypatch)
    expired = False

    def guard():
        if expired:
            raise LeaseLostError("private lease details")

    async def start(**kwargs):
        nonlocal expired
        expired = True
        return thread

    client.thread_start.side_effect = start
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await codex_sdk.invoke_codex({"frozen": True}, dispatch_guard=guard)
    assert caught.value.phase == "turn_run"
    assert caught.value.provider_call_started is False
    assert caught.value.usage is None
    assert "private lease details" not in str(caught.value)
    thread.run.assert_not_awaited()
    client.close.assert_awaited_once()


@pytest.mark.asyncio
async def test_real_worker_registry_binds_claim_clock_to_sdk_dispatch(monkeypatch):
    _, _, prompts = _authority()
    committee = codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools())
    now = datetime(2026, 10, 21, tzinfo=UTC)
    claim = SimpleNamespace(expires_at=now + timedelta(seconds=330))
    run = SimpleNamespace(id=uuid4(), status="prepared")
    context = JobHandlerContext(
        session=SimpleNamespace(get=AsyncMock(return_value=run)),
        claim=claim,
        payload=SimpleNamespace(run_id=str(run.id)),
        _clock=lambda: now,
    )
    monkeypatch.setattr(worker, "_committee", AsyncMock(return_value=committee))

    async def execute(*args, **kwargs):
        nonlocal now
        kwargs["committee"].dispatch_guard()
        now = claim.expires_at
        with pytest.raises(LeaseLostError):
            kwargs["committee"].dispatch_guard()
        return SimpleNamespace(status="completed")

    monkeypatch.setattr(staged_executor, "execute_or_capture", execute)
    registry = worker._agent_registry(
        EngineSettings(agent_runtime_mode="codex_sdk"), responses_client=None, tools=None
    )
    result = await registry.resolve(JobType.AGENT_RESEARCH.value).handler(context)
    assert result.outcome == "succeeded"


@pytest.mark.asyncio
async def test_staged_committee_propagates_lease_guard_to_actual_sdk_boundary(monkeypatch):
    request, evidence, prompts = _authority()
    client, thread, _, _ = _sdk(monkeypatch)
    expired = False

    def guard():
        if expired:
            raise LeaseLostError("lease expired during SDK startup")

    async def start(**kwargs):
        nonlocal expired
        expired = True
        return thread

    client.thread_start.side_effect = start
    committee = codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools())
    committee.dispatch_guard = guard
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await committee.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    assert caught.value.provider_call_started is False
    assert caught.value.usage is None
    thread.run.assert_not_awaited()
    client.close.assert_awaited_once()
