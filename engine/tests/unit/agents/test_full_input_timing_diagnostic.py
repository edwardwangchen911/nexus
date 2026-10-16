import asyncio
import math
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from nexus.agents.committee import StageInvocationTiming

PROFILE = "full-input-timing-diagnostic-v1"


def test_timing_profile_is_distinct_and_old_records_remain_bounded():
    with pytest.raises(ValueError):
        StageInvocationTiming(allocated_timeout_ms=274000, elapsed_ms=80000, phase_elapsed_ms={})
    timing = StageInvocationTiming(
        diagnostic_profile=PROFILE,
        allocated_timeout_ms=274000,
        elapsed_ms=80000,
        phase_elapsed_ms={},
    )
    assert timing.model_dump()["diagnostic_profile"] == PROFILE
    assert (
        "diagnostic_profile"
        not in StageInvocationTiming(
            allocated_timeout_ms=60000, elapsed_ms=1, phase_elapsed_ms={}
        ).model_dump()
    )


@pytest.mark.asyncio
async def test_sdk_diagnostic_requires_parent_and_bounded_deadline_before_client(monkeypatch):
    from nexus.agents import codex_sdk

    client = Mock()
    monkeypatch.setattr(codex_sdk, "AsyncCodex", client)
    monkeypatch.setattr(
        codex_sdk.asyncio, "get_running_loop", lambda: SimpleNamespace(time=lambda: 0)
    )
    for options in [
        dict(timeout_seconds=274, deadline_at=274),
        dict(timeout_seconds=274, deadline_at=274, parent_deadline_at=301),
        dict(timeout_seconds=300, deadline_at=300, parent_deadline_at=295),
    ]:
        with pytest.raises(ValueError):
            await codex_sdk.invoke_codex(
                {"stage": "research"}, diagnostic_profile=PROFILE, **options
            )
    client.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "version",
    ["codex-sdk-chatgpt-v9-full-input-capacity", "codex-sdk-chatgpt-v10-flexible-stage-budget"],
)
@pytest.mark.parametrize("origin", [0.0, 188.028309716, 1_000_000.1])
async def test_real_sdk_three_stages_measure_slow_research_without_retries(
    monkeypatch, origin, version
):
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

    loop = asyncio.get_running_loop()
    clock = [origin]

    # Keep asyncio timers and the code under test on the same deterministic clock.
    monkeypatch.setattr(loop, "time", lambda: clock[0])
    client, thread, result, _ = _sdk(monkeypatch)
    observed = []
    original = codex_sdk.invoke_codex

    async def invoke(payload, **options):
        observed.append((payload["stage"], options["timeout_seconds"]))

        async def run(*args, **kwargs):
            clock[0] += {"research": 80, "critique": 65, "synthesis": 30}[payload["stage"]]
            result.final_response = json.dumps(
                _assessment_payload(payload["previous_claims"])
                if payload["stage"] == "synthesis"
                else _staged_batch(payload["stage"], payload["requested_roles"])
            )
            return result

        async def close():
            clock[0] += 1

        thread.run.side_effect = run
        client.close.side_effect = close
        return await original(payload, **options)

    monkeypatch.setattr("nexus.agents.codex_staged.invoke_codex", invoke)
    request, evidence, prompts = _authority()
    committee = StructuredStagedCodexCommittee(
        prompts=prompts,
        tools=_tools(),
        deadline=origin + 300,
        aggregate_input_tokens=96000,
        max_wire_bytes=100479,
        inline_citations=True,
        paper_stage_scope=True,
        stage_time_budget=True,
        stage_budget_version=version,
        diagnostic_profile=PROFILE,
    )
    monkeypatch.setattr(
        committee,
        "_inputs",
        AsyncMock(
            return_value={"security_snapshot": {"items": []}, "financial_facts": {"items": []}}
        ),
    )
    await preflight_capacity_pool(
        [CapacityPoolMember(committee, request, evidence)],
        expected_instrument_ids=(request.instrument_id,),
    )
    for role in ROLE_ORDER:
        await committee.analyze(role, request, evidence, prompts[role])
    expected = [("research", 274), ("critique", 200), ("synthesis", 141)]
    assert len(observed) == len(expected)
    for (stage, timeout), (expected_stage, limit) in zip(observed, expected):
        assert stage == expected_stage
        assert limit - 1e-6 <= timeout <= limit
    # Three stage advances and three closes may each round once in binary64.
    assert clock[0] - origin == pytest.approx(178, rel=0, abs=6 * math.ulp(clock[0]))
    assert committee.usage.provider_calls == 3
    assert committee.usage.input_tokens == 900
    assert all(t.diagnostic_profile == PROFILE for t in committee.stage_timings.values())
