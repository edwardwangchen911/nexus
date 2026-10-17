"""v11 exact budgets and preclaim deadline enforcement."""

import asyncio
from types import SimpleNamespace

import pytest
from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
from nexus.jobs.models import JobType, QueueName, RetryPolicy
from nexus.jobs.registry import JobHandlerSpec
from nexus.settings import EngineSettings
from nexus.worker import CapacityPreclaimTimeout, _prepare_preclaim_capacity_pool
from pydantic import BaseModel, ConfigDict


class Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")


@pytest.mark.parametrize(
    "profile,timeout,valid",
    [
        (BOUNDED_STAGE_BUDGET_CONFIG_VERSION, 315, True),
        (BOUNDED_STAGE_BUDGET_CONFIG_VERSION, 195, False),
        ("codex-sdk-chatgpt-v10-flexible-stage-budget", 315, False),
        ("unregistered-v11", 315, False),
    ],
)
def test_exact_handler_budget(profile, timeout, valid):
    def make():
        return JobHandlerSpec(
            job_type=JobType.AGENT_PAPER_RESEARCH,
            schema_version="agent_research_job_v1",
            payload_model=Payload,
            queue=QueueName.GENERAL,
            timeout_seconds=timeout,
            retry_policy=RetryPolicy.AUTOMATIC_SAFE,
            max_attempts=1,
            idempotency_namespace="agent.research",
            handler=lambda _: None,
            research_budget_profile=profile,
        )

    if valid:
        assert make().timeout_seconds == 315
    else:
        with pytest.raises(ValueError, match="RESEARCH_BUDGET_PROFILE_INVALID"):
            make()


@pytest.mark.asyncio
@pytest.mark.parametrize("synchronous", [False, True])
async def test_preclaim_deadline_rejects_async_and_sync_overrun(monkeypatch, synchronous):
    loop = asyncio.get_running_loop()
    original = loop.time
    offset = 0.0
    timeout_context = None
    original_timeout = asyncio.timeout_at

    def timeout_at(deadline):
        nonlocal timeout_context
        timeout_context = original_timeout(deadline)
        return timeout_context

    def clock():
        return original() + offset

    async def prepare(*args, **kwargs):
        nonlocal offset
        assert kwargs["deadline"] - clock() == pytest.approx(195, abs=1)
        if synchronous:
            offset = 196
        else:
            assert timeout_context is not None
            timeout_context.reschedule(loop.time())
            await asyncio.Future()

    monkeypatch.setattr(loop, "time", clock)
    monkeypatch.setattr(asyncio, "timeout_at", timeout_at)
    monkeypatch.setattr("nexus.agents.capacity_cycle.prepare_cycle_capacity_pool", prepare)
    if synchronous:
        with pytest.raises(CapacityPreclaimTimeout, match="CODEX_CAPACITY_PRECLAIM_TIMEOUT"):
            await _prepare_preclaim_capacity_pool(
                None,
                EngineSettings(),
                SimpleNamespace(pricing_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION),
            )
    else:
        with pytest.raises(CapacityPreclaimTimeout, match="CODEX_CAPACITY_PRECLAIM_TIMEOUT"):
            await _prepare_preclaim_capacity_pool(
                None,
                EngineSettings(),
                SimpleNamespace(pricing_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION),
            )


def test_formal_v11_registry_uses_exact_profile_and_scope():
    from nexus.worker import _agent_registry

    registry = _agent_registry(
        EngineSettings(agent_runtime_mode="codex_sdk"),
        responses_client=None,
        tools=None,
        include_agent_paper=True,
        formal_stage_capacity=True,
        formal_full_input_capacity=True,
        capacity_profile_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
    )
    assert registry.resolve(JobType.AGENT_RESEARCH.value) is None
    spec = registry.resolve(JobType.AGENT_PAPER_RESEARCH.value)
    assert spec.timeout_seconds == 315
    assert spec.research_budget_profile == BOUNDED_STAGE_BUDGET_CONFIG_VERSION
    assert spec.max_attempts == 1


@pytest.mark.parametrize(
    "error,expected",
    [
        (CapacityPreclaimTimeout("private-canary"), True),
        (TimeoutError("private-canary"), False),
    ],
)
def test_preclaim_safe_log_is_specific_and_does_not_expose_message(capsys, error, expected):
    import json
    from datetime import UTC, datetime

    from nexus.ops.agent_paper_loop import AgentPaperLoopAction, _emit_agent_paper_unknown_failure

    _emit_agent_paper_unknown_failure(
        "execute",
        datetime(2026, 10, 17, tzinfo=UTC),
        action=AgentPaperLoopAction.RUN_RESEARCH_JOB,
        error=error,
    )
    raw = capsys.readouterr().err
    assert "private-canary" not in raw
    result = json.loads(raw)
    assert result["error_code"] == "AGENT_PAPER_LOOP_RESULT_UNKNOWN"
    assert (result.get("failure_phase") == "research_preclaim") is expected
    assert (result.get("failure_code") == "CODEX_CAPACITY_PRECLAIM_TIMEOUT") is expected


@pytest.mark.asyncio
async def test_inner_timeout_is_not_misreported_as_preclaim_allowance(monkeypatch):
    async def timeout(*args, **kwargs):
        raise TimeoutError("inner-private")

    monkeypatch.setattr("nexus.agents.capacity_cycle.prepare_cycle_capacity_pool", timeout)
    with pytest.raises(TimeoutError) as error:
        await _prepare_preclaim_capacity_pool(
            None,
            EngineSettings(),
            SimpleNamespace(pricing_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION),
        )
    assert not isinstance(error.value, CapacityPreclaimTimeout)


@pytest.mark.asyncio
async def test_parent_cancel_during_preclaim_keeps_cancellation(monkeypatch):
    entered = asyncio.Event()

    async def prepare(*args, **kwargs):
        entered.set()
        await asyncio.Future()

    monkeypatch.setattr("nexus.agents.capacity_cycle.prepare_cycle_capacity_pool", prepare)
    task = asyncio.create_task(
        _prepare_preclaim_capacity_pool(
            None,
            EngineSettings(),
            SimpleNamespace(pricing_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION),
        )
    )
    await entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


def test_preclaim_error_other_action_does_not_claim_research_stage(capsys):
    import json
    from datetime import UTC, datetime

    from nexus.ops.agent_paper_loop import AgentPaperLoopAction, _emit_agent_paper_unknown_failure

    _emit_agent_paper_unknown_failure(
        "execute",
        datetime(2026, 10, 17, tzinfo=UTC),
        action=AgentPaperLoopAction.TRIGGER,
        error=CapacityPreclaimTimeout(),
    )
    result = json.loads(capsys.readouterr().err)
    assert "failure_phase" not in result and "failure_code" not in result
