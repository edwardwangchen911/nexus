"""The isolated v9 profile cannot silently widen an existing runtime version."""

import pytest
from nexus.agents.codex_staged import StructuredStagedCodexCommittee
from unit.agents.test_codex_sdk import _authority, _tools

V9 = "codex-sdk-chatgpt-v9-full-input-capacity"


def committee(**overrides):
    _, _, prompts = _authority()
    options = dict(
        prompts=prompts,
        tools=_tools(),
        deadline=1000,
        aggregate_input_tokens=96000,
        max_wire_bytes=100479,
        inline_citations=True,
        paper_stage_scope=True,
        stage_time_budget=True,
        stage_budget_version=V9,
    )
    options.update(overrides)
    return StructuredStagedCodexCommittee(**options)


def test_isolated_v9_requires_exact_frozen_profile_and_preserves_v8():
    from nexus.agents.codex_versions import SDK_CONFIG_VERSIONS

    assert V9 not in SDK_CONFIG_VERSIONS
    model = committee()
    assert model._max_wire_bytes == 100479
    with pytest.raises(ValueError):
        committee(stage_budget_version="codex-sdk-chatgpt-v8-stage-capacity")
    for override in (
        {"max_wire_bytes": 64000},
        {"inline_citations": False},
        {"paper_stage_scope": False},
        {"aggregate_input_tokens": 64000},
        {"stage_time_budget": False},
        {"deadline": None},
    ):
        with pytest.raises(ValueError):
            committee(**override)


def test_v9_compiles_current_stage_without_audit_metadata_and_keeps_all_prior_claims():
    request, evidence, _ = _authority()
    model = committee()
    prior = [
        dict(
            claim_id="orchestrator:0",
            role="orchestrator",
            vote="support",
            text="原问题",
            citations=[0],
        )
    ]
    payload, _ = model.build_stage_request(
        stage="critique",
        request=request,
        evidence_refs=evidence,
        frozen_inputs={"security_snapshot": {"items": []}, "financial_facts": {"items": []}},
        previous=prior,
    )
    assert payload["previous_claims"] == prior
    assert all(set(prompt) == {"role", "content"} for prompt in payload["prompts"])
    assert "Research: identify" not in payload["instructions"]


@pytest.mark.asyncio
async def test_v9_cannot_invoke_without_complete_pool_preflight():
    request, evidence, prompts = _authority()
    model = committee()
    with pytest.raises(ValueError, match="^CODEX_CAPACITY_POOL_REQUIRED$"):
        await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
    assert model.usage.provider_calls == 0


@pytest.mark.asyncio
async def test_pool_preflight_checks_every_candidate_before_binding_any(monkeypatch):
    from unittest.mock import AsyncMock

    from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool

    members = []
    for index in range(2):
        request, evidence, _ = _authority()
        model = committee()
        inputs = {"security_snapshot": {"items": []}, "financial_facts": {"items": []}}
        if index:
            inputs["filing_disclosures"] = {"items": [], "text": "x" * 110000}
        monkeypatch.setattr(model, "_inputs", AsyncMock(return_value=inputs))
        members.append(CapacityPoolMember(model, request, evidence))
    with pytest.raises(ValueError, match="^CODEX_CAPACITY_POOL_WIRE_EXCEEDED$"):
        await preflight_capacity_pool(
            members, expected_instrument_ids=tuple(m.request.instrument_id for m in members)
        )
    assert all(m.committee._capacity_pool_manifest is None for m in members)
    assert all(m.committee.usage.provider_calls == 0 for m in members)


@pytest.mark.asyncio
async def test_pool_preflight_rejects_omitted_candidate_and_binds_exact_requests(monkeypatch):
    from unittest.mock import AsyncMock
    from uuid import uuid4

    from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool

    request, evidence, prompts = _authority()
    model = committee()
    monkeypatch.setattr(
        model,
        "_inputs",
        AsyncMock(
            return_value={"security_snapshot": {"items": []}, "financial_facts": {"items": []}}
        ),
    )
    member = CapacityPoolMember(model, request, evidence)
    with pytest.raises(ValueError, match="^CODEX_CAPACITY_POOL_BINDING_INVALID$"):
        await preflight_capacity_pool(
            [member], expected_instrument_ids=(request.instrument_id, uuid4())
        )
    result = await preflight_capacity_pool(
        [member], expected_instrument_ids=(request.instrument_id,)
    )
    assert result["candidate_count"] == 1 and result["model_calls"] == 0
    assert result["aggregate_completion_proven"] is False
    drift = request.model_copy(update={"data_snapshot_id": uuid4()})
    with pytest.raises(ValueError, match="^CODEX_CAPACITY_POOL_BINDING_INVALID$"):
        await model.analyze("orchestrator", drift, evidence, prompts["orchestrator"])


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [False, True])
@pytest.mark.parametrize("reuse", [False, True])
async def test_pool_binding_reaches_sdk_only_after_verified_unchanged_inputs(
    monkeypatch, drift, reuse
):
    import asyncio
    import json
    from contextlib import nullcontext
    from unittest.mock import AsyncMock

    from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool
    from nexus.agents.committee import ROLE_ORDER
    from nexus.agents.preparation_reuse import research_preparation_scope
    from unit.agents.test_codex_sdk import _sdk, _staged_batch

    _, thread, result, configs = _sdk(monkeypatch)
    result.final_response = json.dumps(_staged_batch("research", ROLE_ORDER[:5]))
    request, evidence, prompts = _authority()
    model = committee(deadline=asyncio.get_running_loop().time() + 190)
    inputs = {"security_snapshot": {"items": []}, "financial_facts": {"items": []}}
    changed = {**inputs, "filing_disclosures": {"items": [], "text": "changed public fixture"}}
    monkeypatch.setattr(
        model, "_inputs", AsyncMock(side_effect=[inputs, changed if drift else inputs])
    )
    with research_preparation_scope() if reuse else nullcontext():
        await preflight_capacity_pool(
            [CapacityPoolMember(model, request, evidence)],
            expected_instrument_ids=(request.instrument_id,),
        )
        if drift:
            with pytest.raises(ValueError, match="^CODEX_CAPACITY_POOL_BINDING_INVALID$"):
                await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
            thread.run.assert_not_awaited()
            assert not configs
            assert model.usage.provider_calls == 0
        else:
            output = await model.analyze("orchestrator", request, evidence, prompts["orchestrator"])
            assert output.role == "orchestrator"
            thread.run.assert_awaited_once()
            assert model.usage.provider_calls == 1


@pytest.mark.asyncio
async def test_concurrent_preflight_cannot_overwrite_already_bound_pool(monkeypatch):
    import asyncio
    from uuid import uuid4

    from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool

    request, evidence, _ = _authority()
    model = committee()
    second = request.model_copy(update={"data_snapshot_id": uuid4()})
    entered = [asyncio.Event(), asyncio.Event()]
    release = [asyncio.Event(), asyncio.Event()]

    async def inputs(req):
        index = 0 if req == request else 1
        entered[index].set()
        await release[index].wait()
        return {"security_snapshot": {"items": []}, "financial_facts": {"items": []}}

    monkeypatch.setattr(model, "_inputs", inputs)
    tasks = []
    try:
        for index, req in enumerate((request, second)):
            tasks.append(
                asyncio.create_task(
                    preflight_capacity_pool(
                        [CapacityPoolMember(model, req, evidence)],
                        expected_instrument_ids=(request.instrument_id,),
                    )
                )
            )
            await asyncio.wait_for(entered[index].wait(), 5)
        release[0].set()
        await tasks[0]
        frozen = (
            model._capacity_pool_manifest,
            model._capacity_pool_request,
            model._capacity_research_hash,
        )
        release[1].set()
        with pytest.raises(ValueError, match="^CODEX_CAPACITY_POOL_BINDING_INVALID$"):
            await tasks[1]
        assert (
            model._capacity_pool_manifest,
            model._capacity_pool_request,
            model._capacity_research_hash,
        ) == frozen
    finally:
        for event in release:
            event.set()
        await asyncio.gather(*tasks, return_exceptions=True)


@pytest.mark.asyncio
async def test_preflight_freezes_member_list_before_awaiting_inputs(monkeypatch):
    import asyncio
    from unittest.mock import AsyncMock

    from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool

    members = []
    inputs = {"security_snapshot": {"items": []}, "financial_facts": {"items": []}}
    for _ in range(3):
        request, evidence, _ = _authority()
        model = committee()
        monkeypatch.setattr(model, "_inputs", AsyncMock(return_value=inputs))
        members.append(CapacityPoolMember(model, request, evidence))
    entered, release = asyncio.Event(), asyncio.Event()

    async def blocked(_request):
        entered.set()
        await release.wait()
        return inputs

    monkeypatch.setattr(members[0].committee, "_inputs", blocked)
    original = members[:2]
    supplied = original.copy()
    task = asyncio.create_task(
        preflight_capacity_pool(
            supplied, expected_instrument_ids=tuple(m.request.instrument_id for m in original)
        )
    )
    try:
        await asyncio.wait_for(entered.wait(), 5)
        supplied[1] = members[2]
        release.set()
        await task
        assert original[1].committee._capacity_pool_manifest is not None
        assert members[2].committee._capacity_pool_manifest is None
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_three_candidate_pool_keeps_complete_claims_across_all_three_sdk_stages(monkeypatch):
    import asyncio
    import json
    from unittest.mock import AsyncMock

    import jsonschema
    from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool
    from nexus.agents.committee import ROLE_ORDER
    from nexus.agents.responses_budget import UsageSnapshot
    from unit.agents.test_codex_sdk import (
        _assessment_payload,
        _restore_view,
        _sdk,
        _staged_batch,
    )

    _, thread, result, _ = _sdk(monkeypatch)
    submitted = []

    def normalized(payload, field):
        return _restore_view(payload[field])

    async def invoke(wire, **options):
        payload = json.loads(wire)
        submitted.append(payload)
        body = (
            _assessment_payload(payload["previous_claims"])
            if payload["stage"] == "synthesis"
            else _staged_batch(payload["stage"], payload["requested_roles"])
        )
        jsonschema.validate(body, options["output_schema"])
        result.final_response = json.dumps(body)
        return result

    thread.run.side_effect = invoke
    members = []
    inputs = {"security_snapshot": {"items": []}, "financial_facts": {"items": []}}
    for _ in range(3):
        request, evidence, _ = _authority()
        model = committee(deadline=asyncio.get_running_loop().time() + 190)
        monkeypatch.setattr(model, "_inputs", AsyncMock(return_value=inputs))
        members.append(CapacityPoolMember(model, request, evidence))
    receipt = await preflight_capacity_pool(
        members, expected_instrument_ids=tuple(m.request.instrument_id for m in members)
    )
    assert receipt["candidate_count"] == 3 and thread.run.await_count == 0
    for member in members:
        model = member.committee
        outputs = [
            await model.analyze(role, member.request, member.evidence_refs, model._prompts[role])
            for role in ROLE_ORDER
        ]
        assert [output.role for output in outputs] == list(ROLE_ORDER)
        assert model.usage == UsageSnapshot(input_tokens=900, output_tokens=240, provider_calls=3)
        stages = submitted[-3:]
        assert [p["stage"] for p in stages] == ["research", "critique", "synthesis"]
        assert normalized(stages[0], "previous_claims") == []
        for payload, count in zip(stages[1:], (5, 7), strict=True):
            prior = normalized(payload, "previous_claims")
            assert [claim["claim_id"] for claim in prior] == [
                f"{role}:0" for role in ROLE_ORDER[:count]
            ]
            assert all(claim["text"] and claim["citations"] for claim in prior)
        assert model._capacity_pool_manifest == receipt["manifest_hash"]
    assert thread.run.await_count == 9
