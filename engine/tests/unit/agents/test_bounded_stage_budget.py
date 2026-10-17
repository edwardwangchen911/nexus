"""A new bounded profile cannot silently widen frozen research identities."""

import json
from datetime import datetime
from types import SimpleNamespace

import pytest

V11 = "codex-sdk-chatgpt-v11-bounded-stage-budget"


def test_new_profile_is_exact_and_preserves_old_hashes_and_resource_limits():
    from nexus.agents.codex_bounded_budget import bounded_stage_config, bounded_stage_config_hash
    from nexus.agents.codex_flexible_budget import flexible_stage_config_hash
    from nexus.agents.codex_formal_stage import formal_preparation_model_matches

    config = bounded_stage_config()
    assert (
        flexible_stage_config_hash()
        == "7debe4c68c425cfa787b58c585365a90812ab1c8a453cb27f77dd959308bc7a4"
    )
    assert config["version"] == V11
    assert (
        config["max_input_tokens"],
        config["max_output_tokens"],
        config["max_sdk_turns"],
        config["max_wire_bytes"],
    ) == (96000, 8192, 3, 100479)
    budget = json.loads(config["stage_time_budget"])
    assert (
        budget["job_absolute_seconds"],
        budget["handler_absolute_seconds"],
        budget["lease_seconds"],
    ) == (300, 315, 330)
    model = SimpleNamespace(
        **{
            k: config[k]
            for k in (
                "model_id",
                "version",
                "max_input_tokens",
                "max_output_tokens",
                "max_tool_calls",
            )
        },
        config_hash=bounded_stage_config_hash(),
        input_usd_per_million=0,
        output_usd_per_million=0,
        pricing_available_at=datetime.fromisoformat(config["pricing_available_at"]),
        pricing_expires_at=datetime.fromisoformat(config["pricing_expires_at"]),
    )
    assert formal_preparation_model_matches(model)
    model.config_hash = flexible_stage_config_hash()
    assert not formal_preparation_model_matches(model)


@pytest.mark.parametrize(
    "stage,now,expected",
    [("research", 3, 181), ("critique", 110, 126), ("synthesis", 220, 68), ("research", 184, 0)],
)
def test_shared_budget_keeps_later_calls_and_cleanup_inside_same_deadline(stage, now, expected):
    from nexus.agents.codex_versions import stage_timeout_seconds

    allocated = stage_timeout_seconds(stage, deadline=295, now=now, version=V11)
    assert max(0, expected - 1e-6) <= allocated <= expected


def test_new_timing_profile_cannot_be_used_as_diagnostic_or_legacy_profile():
    from nexus.agents.committee import StageInvocationTiming

    args = dict(allocated_timeout_ms=181000, elapsed_ms=90000, phase_elapsed_ms={})
    timing = StageInvocationTiming(**args, bounded_stage_profile=V11)
    assert timing.model_dump()["bounded_stage_profile"] == V11
    for extra in (
        {},
        {"flexible_critique_profile": "codex-sdk-chatgpt-v10-flexible-stage-budget"},
        {"bounded_stage_profile": V11, "diagnostic_profile": "full-input-timing-diagnostic-v1"},
    ):
        with pytest.raises(ValueError):
            StageInvocationTiming(**args, **extra)


@pytest.mark.asyncio
@pytest.mark.parametrize("origin", [0.1, 100.1, 1024.1, 1_000_000_000.1])
@pytest.mark.parametrize("work", [(100, 90, 60), (20, 150, 40), (10, 10, 200)])
async def test_slow_research_over_old_limit_finishes_all_stages_through_sdk(
    monkeypatch, work, origin
):
    import asyncio
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
    monkeypatch.setattr(loop, "time", lambda: clock[0])
    client, thread, result, _ = _sdk(monkeypatch)
    original = codex_sdk.invoke_codex
    seen = []

    async def invoke(payload, **options):
        stage = payload["stage"]
        seen.append((stage, options["timeout_seconds"]))

        async def run(*args, **kwargs):
            clock[0] += dict(zip(("research", "critique", "synthesis"), work, strict=True))[stage]
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
        stage_budget_version=V11,
    )
    monkeypatch.setattr(
        committee,
        "_inputs",
        AsyncMock(
            return_value={"security_snapshot": {"items": []}, "financial_facts": {"items": []}}
        ),
    )
    clock[0] += 3
    await preflight_capacity_pool(
        [CapacityPoolMember(committee, request, evidence)],
        expected_instrument_ids=(request.instrument_id,),
    )
    for role in ROLE_ORDER:
        await committee.analyze(role, request, evidence, prompts[role])
    assert [stage for stage, _ in seen] == ["research", "critique", "synthesis"]
    assert all(allocation > elapsed for (_, allocation), elapsed in zip(seen, work, strict=True))
    assert committee.usage.provider_calls == 3
    assert clock[0] - origin == pytest.approx(sum(work) + 24)
    assert committee._deadline == origin + 295
    assert all(t.bounded_stage_profile == V11 for t in committee.stage_timings.values())
