"""Frozen phase allocation preserves total time and historical runtime behavior."""

from unittest.mock import AsyncMock

import pytest
from nexus.agents import codex_sdk
from nexus.agents.responses_budget import UsageSnapshot


def test_phase_allocation_keeps_later_stages_and_close_inside_total_deadline():
    from nexus.agents.codex_versions import stage_timeout_seconds

    # 120-second Job, with the existing five-second terminal-write reserve removed.
    assert stage_timeout_seconds("research", deadline=115, now=0) == 53
    assert stage_timeout_seconds("critique", deadline=115, now=53) == 25
    assert stage_timeout_seconds("synthesis", deadline=115, now=85) == 23
    assert stage_timeout_seconds("research", deadline=115, now=60) == 0
    assert stage_timeout_seconds("synthesis", deadline=115, now=108) == 0


@pytest.mark.asyncio
async def test_allocated_sdk_timeout_is_explicit_and_does_not_replace_legacy_default(monkeypatch):
    called = AsyncMock(return_value=("{}", UsageSnapshot()))
    monkeypatch.setattr(codex_sdk, "_invoke_codex", called)
    await codex_sdk.invoke_codex({}, timeout_seconds=53)
    assert called.call_args.kwargs["timeout_seconds"] == 53
    await codex_sdk.invoke_codex({})
    assert called.call_args.kwargs["timeout_seconds"] == 40


@pytest.mark.asyncio
async def test_handoff_delay_cannot_move_the_allocated_absolute_deadline(monkeypatch):
    from types import SimpleNamespace

    called = AsyncMock(return_value=("{}", UsageSnapshot()))
    monkeypatch.setattr(codex_sdk, "_invoke_codex", called)
    monkeypatch.setattr(
        codex_sdk.asyncio, "get_running_loop", lambda: SimpleNamespace(time=lambda: 110.0)
    )
    # Allocation happened at 100. Ten seconds elapse before entering the SDK.
    await codex_sdk.invoke_codex({}, timeout_seconds=53, deadline_at=153)
    assert called.call_args.kwargs["invocation_state"].stage_deadline == 153


@pytest.mark.asyncio
async def test_handoff_exhaustion_stops_before_sdk_client(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import Mock

    client = Mock()
    monkeypatch.setattr(codex_sdk, "AsyncCodex", client)
    monkeypatch.setattr(codex_sdk, "codex_request_wire", lambda _: "{}")
    monkeypatch.setattr(codex_sdk, "codex_input_allowance", lambda *_: 1)
    monkeypatch.setattr(
        codex_sdk.asyncio, "get_running_loop", lambda: SimpleNamespace(time=lambda: 160.0)
    )
    with pytest.raises(TimeoutError) as error:
        await codex_sdk.invoke_codex({}, timeout_seconds=53, deadline_at=153)
    assert error.value.phase == "preflight"
    client.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [0, -1, 109, float("nan"), float("inf"), True])
async def test_invalid_stage_timeout_stops_before_sdk_initialization(value, monkeypatch):
    called = AsyncMock()
    monkeypatch.setattr(codex_sdk, "_invoke_codex", called)
    with pytest.raises(ValueError, match="CODEX_STAGE_TIMEOUT_INVALID"):
        await codex_sdk.invoke_codex({}, timeout_seconds=value)
    called.assert_not_called()


@pytest.mark.asyncio
async def test_new_phase_policy_runs_complete_committee_and_retains_cumulative_usage(monkeypatch):
    import asyncio
    import json

    from nexus.agents import codex_staged
    from nexus.agents.committee import ROLE_ORDER
    from unit.agents.test_codex_sdk import _authority, _staged_batch, _tools

    request, evidence, prompts = _authority()
    observed = []

    async def invoke(payload, *, output_model, timeout_seconds, deadline_at):
        observed.append((payload["stage"], timeout_seconds))
        remaining = deadline_at - asyncio.get_running_loop().time()
        assert remaining <= timeout_seconds
        return json.dumps(
            _staged_batch(payload["stage"], [p["role"] for p in payload["prompts"]])
        ), UsageSnapshot(input_tokens=100, output_tokens=80)

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    model = codex_staged.StagedCodexCommittee(
        prompts=prompts,
        tools=_tools(),
        deadline=asyncio.get_running_loop().time() + 120,
        stage_time_budget=True,
    )
    for role in ROLE_ORDER:
        await model.analyze(
            role=role, request=request, evidence_refs=evidence, prompt=prompts[role]
        )
    assert [stage for stage, _ in observed] == ["research", "critique", "synthesis"]
    assert 40 < observed[0][1] <= 53
    assert model.usage == UsageSnapshot(input_tokens=300, output_tokens=240, provider_calls=3)


@pytest.mark.asyncio
async def test_insufficient_later_reserves_stop_before_any_provider_attempt(monkeypatch):
    import asyncio

    from nexus.agents import codex_staged
    from unit.agents.test_codex_sdk import _authority, _tools

    request, evidence, prompts = _authority()
    called = AsyncMock()
    monkeypatch.setattr(codex_staged, "invoke_codex", called)
    model = codex_staged.StagedCodexCommittee(
        prompts=prompts,
        tools=_tools(),
        deadline=asyncio.get_running_loop().time() + 60,
        stage_time_budget=True,
    )
    with pytest.raises(ValueError, match="CODEX_AGGREGATE_ALLOWANCE_EXHAUSTED"):
        await model.analyze(
            role="orchestrator",
            request=request,
            evidence_refs=evidence,
            prompt=prompts["orchestrator"],
        )
    called.assert_not_called()
    assert model.usage.provider_calls == 0
    assert model.failure_reason == "model_stage_deadline_elapsed"


def test_research_finishing_after_old_limit_can_leave_time_for_complete_later_phases():
    from nexus.agents.codex_versions import stage_timeout_seconds

    elapsed = 0
    for stage, work in (("research", 45), ("critique", 20), ("synthesis", 20)):
        assert work <= stage_timeout_seconds(stage, deadline=115, now=elapsed)
        elapsed += work + 7  # include bounded SDK close after each phase
    assert elapsed == 106
    assert elapsed + 5 <= 120


@pytest.mark.asyncio
async def test_experimental_registration_rejects_business_database_before_writes():
    from nexus.ops.openai_local import prepare_database
    from nexus.settings import EngineSettings

    settings = EngineSettings(
        database_url="postgresql+asyncpg://localhost/nexus", agent_runtime_mode="codex_sdk"
    )
    with pytest.raises(ValueError, match="CODEX_STAGE_TIME_BUDGET_ISOLATION_REQUIRED"):
        await prepare_database(settings, actionable_paper=True, stage_time_budget=True)


@pytest.mark.asyncio
async def test_stage_preparation_consumes_the_same_absolute_budget_before_any_sdk_client(
    monkeypatch,
):
    from types import SimpleNamespace

    current = [100.0]
    monkeypatch.setattr(
        codex_sdk.asyncio, "get_running_loop", lambda: SimpleNamespace(time=lambda: current[0])
    )

    def wire(_payload):
        current[0] += 60  # bounded synchronous preparation exceeds a 53-second phase
        return "{}"

    client = AsyncMock()
    monkeypatch.setattr(codex_sdk, "codex_request_wire", wire)
    monkeypatch.setattr(codex_sdk, "codex_input_allowance", lambda *args: 0)
    monkeypatch.setattr(codex_sdk, "AsyncCodex", client)
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await codex_sdk.invoke_codex({}, timeout_seconds=53)
    assert caught.value.kind == "timeout"
    assert caught.value.phase == "preflight"
    assert caught.value.provider_call_started is False
    client.assert_not_called()
