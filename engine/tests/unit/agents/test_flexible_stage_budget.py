"""Keep the absolute envelope while a slow critique borrows unused phase time."""

import math

import pytest
from nexus.agents.codex_versions import stage_timeout_seconds

V10 = "codex-sdk-chatgpt-v10-flexible-stage-budget"
V9 = "codex-sdk-chatgpt-v9-full-input-capacity"


def test_critique_uses_parent_slack_without_spending_synthesis_or_close_reserves():
    # Worker origin 0: prepare 3 + research 20 + close 7 = critique start 30.
    # Job 180 minus terminal 5; critique can use 175-30-7-(45+7)=86.
    assert stage_timeout_seconds("critique", deadline=175, now=30, version=V9) == 45
    assert 86 - 1e-6 <= stage_timeout_seconds("critique", deadline=175, now=30, version=V10) <= 86
    assert stage_timeout_seconds("research", deadline=175, now=3, version=V10) == 60
    assert 45 - 1e-6 <= stage_timeout_seconds("synthesis", deadline=175, now=123, version=V10) <= 45
    assert stage_timeout_seconds("critique", deadline=175, now=116, version=V10) == 0


@pytest.mark.parametrize("now", [True, float("nan"), float("inf")])
def test_flexible_budget_rejects_invalid_clock(now):
    with pytest.raises(ValueError, match="CODEX_STAGE_BUDGET_CLOCK_INVALID"):
        stage_timeout_seconds("critique", deadline=175, now=now, version=V10)


def test_old_profiles_keep_their_frozen_hashes_and_phase_limits():
    from nexus.agents.codex_candidate import candidate_config_hash
    from nexus.agents.codex_capacity import capacity_config_hash
    from nexus.agents.codex_formal_stage import formal_stage_config_hash

    assert (
        candidate_config_hash()
        == "175ef8cf95fbafb050f89b4bb7f16aae278c8c9b99ee25c60cf35f044688141f"
    )
    assert (
        formal_stage_config_hash()
        == "e6a74f9855f93e2badec5640857b0f83fe22faba207911584caf2e71504ae0ac"
    )
    assert (
        capacity_config_hash() == "a456380dd56c32aa4b90348e85430d253a5e87241c3f9c276723824d35588a4c"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", [0.0, 188.028309716, 1_000_000.1])
async def test_flexible_committee_reaches_real_sdk_after_complete_pool_and_keeps_three_calls(
    monkeypatch,
    origin,
):
    import asyncio
    import json
    from unittest.mock import AsyncMock

    from nexus.agents import codex_sdk
    from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool
    from nexus.agents.codex_staged import StructuredStagedCodexCommittee
    from nexus.agents.committee import ROLE_ORDER
    from unit.agents.test_codex_sdk import (
        _assessment_payload,
        _authority,
        _sdk,
        _staged_batch,
        _tools,
    )

    real_loop = asyncio.get_running_loop()
    clock = [origin]
    monkeypatch.setattr(real_loop, "time", lambda: clock[0])
    client, thread, result, _ = _sdk(monkeypatch)
    observed = []
    original_invoke = codex_sdk.invoke_codex

    async def invoke(payload, **kwargs):
        observed.append(
            (payload["stage"], kwargs["timeout_seconds"], kwargs["deadline_at"] - origin)
        )
        stage = payload["stage"]
        work = {"research": 20, "critique": 60, "synthesis": 30}[stage]

        async def run(*args, **options):
            clock[0] += work
            result.final_response = json.dumps(
                _assessment_payload(payload["previous_claims"])
                if stage == "synthesis"
                else _staged_batch(stage, payload["requested_roles"])
            )
            return result

        async def close():
            clock[0] += 7

        thread.run.side_effect = run
        client.close.side_effect = close
        return await original_invoke(payload, **kwargs)

    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", invoke)
    request, evidence, prompts = _authority()
    committee = StructuredStagedCodexCommittee(
        prompts=prompts,
        tools=_tools(),
        deadline=origin + 180,
        aggregate_input_tokens=96000,
        max_wire_bytes=100479,
        inline_citations=True,
        paper_stage_scope=True,
        stage_time_budget=True,
        stage_budget_version=V10,
    )
    monkeypatch.setattr(
        committee,
        "_inputs",
        AsyncMock(
            return_value={"security_snapshot": {"items": []}, "financial_facts": {"items": []}}
        ),
    )
    clock[0] += 3  # preparation remains charged to the original envelope
    await preflight_capacity_pool(
        [CapacityPoolMember(committee, request, evidence)],
        expected_instrument_ids=(request.instrument_id,),
    )
    for role in ROLE_ORDER:
        await committee.analyze(role, request, evidence, prompts[role])
    expected = [("research", 60, 63), ("critique", 86, 116), ("synthesis", 45, 142)]
    assert len(observed) == len(expected)
    for (stage, timeout, deadline), (expected_stage, limit, cutoff) in zip(observed, expected):
        assert stage == expected_stage
        assert limit - 1e-6 <= timeout <= limit
        assert deadline == pytest.approx(cutoff, rel=0, abs=1e-6)
    # Preparation plus three stages and closes each advance the binary64 clock.
    assert clock[0] - origin == pytest.approx(134, rel=0, abs=7 * math.ulp(clock[0]))
    assert committee._deadline == origin + 175
    assert committee.usage.provider_calls == 3
    assert (committee.usage.input_tokens, committee.usage.output_tokens) == (900, 240)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "options",
    [
        {"timeout_seconds": 116, "deadline_at": 116, "stage_budget_version": V10},
        {
            "timeout_seconds": 116,
            "deadline_at": 117,
            "stage_budget_version": V10,
            "parent_deadline_at": 175,
        },
        {"timeout_seconds": 116, "deadline_at": 116},
    ],
)
async def test_sdk_refuses_unbound_or_reserve_spending_extension_before_client(
    monkeypatch, options
):
    from types import SimpleNamespace
    from unittest.mock import Mock

    from nexus.agents import codex_sdk

    client = Mock()
    monkeypatch.setattr(codex_sdk, "AsyncCodex", client)
    monkeypatch.setattr(
        codex_sdk.asyncio, "get_running_loop", lambda: SimpleNamespace(time=lambda: 0)
    )
    with pytest.raises(ValueError):
        await codex_sdk.invoke_codex({"stage": "critique"}, **options)
    client.assert_not_called()


@pytest.mark.asyncio
async def test_flexible_setup_refuses_business_target_before_connection(monkeypatch):
    from nexus.ops import openai_local
    from nexus.settings import EngineSettings
    from unit.ops.test_full_input_capacity_registration import V9_OPTIONS

    monkeypatch.setattr(
        openai_local, "session_factory", lambda *_: pytest.fail("connected business target")
    )
    with pytest.raises(ValueError, match="CODEX_FLEXIBLE_STAGE_ISOLATION_REQUIRED"):
        await openai_local.prepare_database(
            EngineSettings(
                database_url="postgresql+asyncpg://127.0.0.1/nexus", agent_runtime_mode="codex_sdk"
            ),
            **V9_OPTIONS,
            flexible_stage_budget=True,
        )


def test_worker_registry_binds_v10_only_in_explicit_isolated_capacity_mode():
    from nexus import worker
    from nexus.jobs.models import JobType
    from nexus.settings import EngineSettings

    settings = EngineSettings(agent_runtime_mode="codex_sdk")
    spec = worker._agent_registry(
        settings,
        responses_client=None,
        tools=None,
        include_agent_paper=True,
        full_input_capacity=True,
        capacity_profile_version=V10,
    ).resolve(JobType.AGENT_PAPER_RESEARCH.value)
    assert spec.research_budget_profile == V10
    assert spec.timeout_seconds == 195 and spec.max_attempts == 1
    default = worker._agent_registry(
        settings, responses_client=None, tools=None, include_agent_paper=True
    )
    assert default.resolve(JobType.AGENT_PAPER_RESEARCH.value).research_budget_profile is None
    with pytest.raises(ValueError, match="CODEX_CAPACITY_WORKER_ARGUMENT_INVALID"):
        worker._agent_registry(
            settings,
            responses_client=None,
            tools=None,
            include_agent_paper=True,
            formal_stage_capacity=True,
            capacity_profile_version=V10,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", [100.1, 1_000_000.1])
async def test_exhausted_parent_budget_never_starts_a_provider_call(monkeypatch, origin):
    import asyncio
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from nexus.agents import codex_staged
    from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool
    from nexus.agents.codex_staged import StructuredStagedCodexCommittee
    from unit.agents.test_codex_sdk import _authority, _tools

    request, refs, prompts = _authority()
    called = AsyncMock(side_effect=AssertionError("exhausted budget reached SDK"))
    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", called)
    monkeypatch.setattr(
        codex_staged,
        "asyncio",
        SimpleNamespace(
            get_running_loop=lambda: SimpleNamespace(time=lambda: origin + 64),
            timeout_at=asyncio.timeout_at,
        ),
    )
    committee = StructuredStagedCodexCommittee(
        prompts=prompts,
        tools=_tools(),
        deadline=origin + 180,
        aggregate_input_tokens=96000,
        max_wire_bytes=100479,
        inline_citations=True,
        paper_stage_scope=True,
        stage_time_budget=True,
        stage_budget_version=V10,
    )
    await preflight_capacity_pool(
        [CapacityPoolMember(committee, request, refs)],
        expected_instrument_ids=(request.instrument_id,),
    )
    with pytest.raises(ValueError, match="CODEX_AGGREGATE_ALLOWANCE_EXHAUSTED"):
        await committee.analyze("orchestrator", request, refs, prompts["orchestrator"])
    called.assert_not_called()
    assert committee.usage.provider_calls == 0


@pytest.mark.asyncio
async def test_prepare_delay_consumes_v10_stage_cutoff_without_initializing_sdk(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import Mock

    from nexus.agents import codex_sdk

    clock = [30.0]
    monkeypatch.setattr(
        codex_sdk.asyncio, "get_running_loop", lambda: SimpleNamespace(time=lambda: clock[0])
    )

    def wire(_):
        clock[0] = 116.0
        return "{}"

    client = Mock()
    monkeypatch.setattr(codex_sdk, "codex_request_wire", wire)
    monkeypatch.setattr(codex_sdk, "codex_input_allowance", lambda *_: 0)
    monkeypatch.setattr(codex_sdk, "AsyncCodex", client)
    with pytest.raises(codex_sdk.CodexInvocationError) as caught:
        await codex_sdk.invoke_codex(
            {"stage": "critique"},
            timeout_seconds=86,
            deadline_at=116,
            stage_budget_version=V10,
            parent_deadline_at=175,
        )
    assert caught.value.phase == "preflight" and caught.value.provider_call_started is False
    client.assert_not_called()


def test_normal_setup_loads_explicit_private_diagnostic_profile_only(monkeypatch, tmp_path):
    from unittest.mock import AsyncMock

    from nexus.ops import openai_local
    from nexus.settings import EngineSettings

    config = tmp_path / "database-url"
    config.write_text("postgresql+asyncpg://127.0.0.1/nexus_diag_flexible")
    config.chmod(0o600)
    prepare = AsyncMock()
    monkeypatch.setattr(
        openai_local,
        "local_settings",
        lambda *a, **kw: EngineSettings(
            database_url="postgresql+asyncpg://127.0.0.1/nexus", agent_runtime_mode="codex_sdk"
        ),
    )
    monkeypatch.setattr(openai_local, "prepare_database", prepare)
    monkeypatch.setattr(openai_local, "status", lambda _: {"execution_locked": True})
    monkeypatch.chdir(__import__("pathlib").Path(__file__).resolve().parents[4])
    monkeypatch.setattr(
        "sys.argv",
        [
            "nexus-openai-local",
            "setup",
            "--backend",
            "codex_sdk",
            "--formal-stage-capacity",
            "--full-input-capacity",
            "--flexible-stage-budget",
            "--database-url-file",
            str(config),
            "--stage-time-budget",
            "--allow-stage-time-budget-registration",
            "--structured-synthesis",
            "--aggregate-input-tokens",
            "96000",
            "--max-wire-bytes",
            "100479",
            "--inline-citations",
            "--actionable-paper",
            "--skip-key",
        ],
    )
    assert openai_local.main() == 0
    assert prepare.await_args.args[0].database_url.endswith("/nexus_diag_flexible")
    assert prepare.await_args.kwargs["flexible_stage_budget"] is True


def test_new_timing_identity_records_flexible_limit_and_old_receipts_round_trip():
    from nexus.agents.committee import StageInvocationTiming
    from pydantic import ValidationError

    old = {
        "allocated_timeout_ms": 45000,
        "elapsed_ms": 45035,
        "phase_elapsed_ms": {},
        "stream_progress": None,
    }
    assert StageInvocationTiming.model_validate(old).model_dump(mode="json") == old
    with pytest.raises(ValidationError):
        StageInvocationTiming(allocated_timeout_ms=116000, elapsed_ms=0, phase_elapsed_ms={})
    new = StageInvocationTiming(
        allocated_timeout_ms=116000,
        elapsed_ms=60000,
        phase_elapsed_ms={},
        flexible_critique_profile=V10,
    )
    assert new.model_dump(mode="json")["flexible_critique_profile"] == V10


@pytest.mark.asyncio
@pytest.mark.parametrize("fail", [False, True])
async def test_research_records_spent_preparation_and_remaining_budget_without_extending_it(
    monkeypatch, fail
):
    import asyncio
    import json
    from unittest.mock import AsyncMock

    from nexus.agents import codex_staged
    from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool
    from nexus.agents.codex_sdk import CodexInvocationError
    from nexus.agents.responses_budget import UsageSnapshot
    from unit.agents.test_codex_sdk import _authority, _staged_batch, _tools

    clock = [100.0]
    monkeypatch.setattr(asyncio.get_running_loop(), "time", lambda: clock[0])
    request, evidence, prompts = _authority()
    committee = codex_staged.StructuredStagedCodexCommittee(
        prompts=prompts,
        tools=_tools(),
        deadline=280.0,
        aggregate_input_tokens=96000,
        max_wire_bytes=100479,
        inline_citations=True,
        paper_stage_scope=True,
        stage_time_budget=True,
        stage_budget_version=V10,
    )
    inputs = {"security_snapshot": {"items": []}, "financial_facts": {"items": []}}
    monkeypatch.setattr(committee, "_inputs", AsyncMock(return_value=inputs))
    await preflight_capacity_pool(
        [CapacityPoolMember(committee, request, evidence)],
        expected_instrument_ids=(request.instrument_id,),
    )
    clock[0] += 4.767  # Worker/whole-pool preparation before this stage.

    async def stage_inputs(_request):
        clock[0] += 2
        return inputs

    monkeypatch.setattr(committee, "_inputs", stage_inputs)
    calls = []

    async def invoke(payload, **options):
        calls.append(options)
        assert options["timeout_seconds"] == pytest.approx(57.233)
        assert options["deadline_at"] == pytest.approx(164)
        assert options["parent_deadline_at"] == 275
        if fail:
            error = CodexInvocationError(
                "CODEX_TIMEOUT", phase="turn_run", provider_call_started=True
            )
            error.timeout_source = "stage_deadline"
            raise error
        return json.dumps(_staged_batch("research", payload["requested_roles"])), UsageSnapshot(
            input_tokens=300, output_tokens=80
        )

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    if fail:
        with pytest.raises(CodexInvocationError):
            await committee.analyze("orchestrator", request, evidence, prompts["orchestrator"])
        timing = committee.failure_diagnostic.timing
        assert committee.failure_reason == "model_usage_uncertain"
        with pytest.raises(ValueError, match="CODEX_REQUEST_ALREADY_FAILED"):
            await committee.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    else:
        await committee.analyze("orchestrator", request, evidence, prompts["orchestrator"])
        timing = committee.stage_timings["research"]
    assert timing.committee_remaining_ms == pytest.approx(168233, abs=1)
    assert timing.stage_preparation_ms == 2000
    assert committee._deadline == 275
    assert len(calls) == committee.usage.provider_calls == 1
