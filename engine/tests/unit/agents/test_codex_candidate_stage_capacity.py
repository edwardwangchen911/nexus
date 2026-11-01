"""Finite v7 allocation qualifies complete stages without changing frozen v6."""

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from nexus.agents import codex_staged, codex_versions
from nexus.agents.committee import ROLE_ORDER
from nexus.agents.responses_budget import UsageSnapshot
from unit.agents.test_codex_sdk import _authority, _staged_batch, _tools


def test_candidate_reserves_complete_later_calls_and_cleanup():
    version = codex_versions.CANDIDATE_BUDGET_CONFIG_VERSION
    allocation = codex_versions.stage_timeout_seconds
    assert allocation("research", deadline=175, now=0, version=version) == 60
    assert 54 - 1e-6 <= allocation("research", deadline=175, now=10, version=version) <= 54
    assert allocation("critique", deadline=175, now=67, version=version) == 45
    assert allocation("synthesis", deadline=175, now=119, version=version) == 45
    assert allocation("research", deadline=175, now=64, version=version) == 0
    assert allocation("synthesis", deadline=175, now=168, version=version) == 0
    assert allocation("research", deadline=115, now=0) == 53
    assert version not in codex_versions.SDK_CONFIG_VERSIONS


@pytest.mark.asyncio
@pytest.mark.parametrize("started", [100.1, 173.41774628300004])
@pytest.mark.parametrize(
    "candidate, preparation_seconds",
    [(False, 0), (True, 0), (True, 10), ("formal", 0), ("formal", 10)],
)
async def test_complete_committee_allocation_counterexample_uses_no_real_sdk(
    monkeypatch,
    candidate,
    preparation_seconds,
    started,
):
    request, evidence, prompts = _authority()
    elapsed = float(preparation_seconds)
    observed = []
    # Translate the synthetic absolute clock to a real relative timer. The
    # allocator sees fixed origins, independent of the runner uptime.
    monkeypatch.setattr(
        codex_staged,
        "asyncio",
        SimpleNamespace(
            get_running_loop=lambda: SimpleNamespace(time=lambda: started + elapsed),
            timeout_at=lambda deadline: asyncio.timeout(deadline - (started + elapsed)),
            CancelledError=asyncio.CancelledError,
        ),
    )

    async def invoke(payload, *, output_model, timeout_seconds, deadline_at, **kwargs):
        nonlocal elapsed
        stage = payload["stage"]
        work = {"research": 47.786, "critique": 35.0, "synthesis": 35.744}[stage]
        observed.append(stage)
        if candidate and stage == "research":
            expected_limit = 54 if preparation_seconds else 60
            assert expected_limit - 1e-6 <= timeout_seconds <= expected_limit
        assert deadline_at == pytest.approx(started + elapsed + timeout_seconds)
        if work > timeout_seconds:
            elapsed += timeout_seconds
            raise TimeoutError("synthetic stage allocation exhausted")
        elapsed += work + 7  # Exercise the full close reserve before the next stage.
        return json.dumps(
            _staged_batch(stage, [p["role"] for p in payload["prompts"]])
        ), UsageSnapshot(
            input_tokens=100,
            output_tokens=80,
        )

    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    options = (
        {
            "stage_budget_version": codex_versions.FORMAL_STAGE_CAPACITY_CONFIG_VERSION
            if candidate == "formal"
            else codex_versions.CANDIDATE_BUDGET_CONFIG_VERSION
        }
        if candidate
        else {}
    )
    model = codex_staged.StagedCodexCommittee(
        prompts=prompts,
        tools=_tools(),
        deadline=started + (180 if candidate else 120),
        stage_time_budget=True,
        **options,
    )

    async def complete():
        for role in ROLE_ORDER:
            await model.analyze(
                role=role, request=request, evidence_refs=evidence, prompt=prompts[role]
            )

    if candidate:
        await complete()
        assert observed == ["research", "critique", "synthesis"]
        assert elapsed + 5 <= 180
        assert model.usage == UsageSnapshot(input_tokens=300, output_tokens=240, provider_calls=3)
    else:
        with pytest.raises(TimeoutError):
            await complete()
        assert observed == ["research", "critique"]
        assert model.usage.provider_calls == 2


@pytest.mark.parametrize(
    "options",
    [
        {"stage_budget_version": "unknown", "stage_time_budget": True, "deadline": 180},
        {"stage_budget_version": "candidate", "stage_time_budget": False, "deadline": 180},
    ],
)
def test_unknown_budget_never_constructs_an_eligible_committee(options):
    _, _, prompts = _authority()
    with pytest.raises(ValueError, match="CODEX_STAGE_BUDGET_VERSION_INVALID"):
        codex_staged.StagedCodexCommittee(prompts=prompts, tools=_tools(), **options)


def test_candidate_requires_the_original_absolute_deadline():
    _, _, prompts = _authority()
    with pytest.raises(ValueError, match="CODEX_STAGE_BUDGET_DEADLINE_REQUIRED"):
        codex_staged.StagedCodexCommittee(
            prompts=prompts,
            tools=_tools(),
            stage_time_budget=True,
            stage_budget_version=codex_versions.CANDIDATE_BUDGET_CONFIG_VERSION,
        )


@pytest.mark.parametrize("deadline", [True, False, float("nan"), float("inf")])
def test_candidate_constructor_rejects_invalid_original_deadline(deadline):
    _, _, prompts = _authority()
    with pytest.raises(ValueError, match="CODEX_STAGE_BUDGET_CLOCK_INVALID"):
        codex_staged.StagedCodexCommittee(
            prompts=prompts,
            tools=_tools(),
            stage_time_budget=True,
            deadline=deadline,
            stage_budget_version=codex_versions.CANDIDATE_BUDGET_CONFIG_VERSION,
        )


@pytest.mark.parametrize("clock", [True, float("nan"), float("inf")])
def test_invalid_candidate_clock_cannot_create_time_allowance(clock):
    with pytest.raises(ValueError, match="CODEX_STAGE_BUDGET_CLOCK_INVALID"):
        codex_versions.stage_timeout_seconds(
            "research",
            deadline=175,
            now=clock,
            version=codex_versions.CANDIDATE_BUDGET_CONFIG_VERSION,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("started", [100.1, 173.41774628300004])
async def test_candidate_preparation_exhaustion_stops_before_sdk(monkeypatch, started):
    request, evidence, prompts = _authority()
    monkeypatch.setattr(
        codex_staged,
        "asyncio",
        SimpleNamespace(
            get_running_loop=lambda: SimpleNamespace(time=lambda: started + 64),
            timeout_at=asyncio.timeout_at,
        ),
    )
    invoke = AsyncMock()
    monkeypatch.setattr(codex_staged, "invoke_codex", invoke)
    model = codex_staged.StagedCodexCommittee(
        prompts=prompts,
        tools=_tools(),
        deadline=started + 180,
        stage_time_budget=True,
        stage_budget_version=codex_versions.CANDIDATE_BUDGET_CONFIG_VERSION,
    )
    with pytest.raises(ValueError, match="CODEX_AGGREGATE_ALLOWANCE_EXHAUSTED"):
        await model.analyze(
            role="orchestrator",
            request=request,
            evidence_refs=evidence,
            prompt=prompts["orchestrator"],
        )
    invoke.assert_not_called()
    assert model.usage.provider_calls == 0
    assert model.failure_reason == "model_stage_deadline_elapsed"


@pytest.mark.parametrize("origin", [100.1, 173.41774628300004, 1_000_000.1, 1_000_000_000.1])
@pytest.mark.parametrize(
    "version",
    [
        codex_versions.CANDIDATE_BUDGET_CONFIG_VERSION,
        codex_versions.FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
        codex_versions.FULL_INPUT_CAPACITY_CONFIG_VERSION,
        codex_versions.FLEXIBLE_STAGE_BUDGET_CONFIG_VERSION,
    ],
)
@pytest.mark.parametrize(
    "stage,elapsed,expected",
    [
        ("research", 10, 54),
        ("research", 63, 1),
        ("research", 64, 0),
        ("research", 65, 0),
        ("critique", 115, 1),
        ("critique", 116, 0),
        ("critique", 117, 0),
        ("synthesis", 167, 1),
        ("synthesis", 168, 0),
        ("synthesis", 169, 0),
    ],
)
def test_clock_origin_cannot_create_budget_or_spend_close_reserves(
    origin,
    version,
    stage,
    elapsed,
    expected,
):
    # Independent envelope: 180 total, 5 terminal, 7 per-stage close;
    # future research/critique reserves are 104/52 seconds respectively.
    actual = codex_versions.stage_timeout_seconds(
        stage,
        deadline=(origin + 180) - 5,
        now=origin + elapsed,
        version=version,
    )
    if expected == 0:
        assert actual == 0
    else:
        assert expected - 1e-6 <= actual <= expected
