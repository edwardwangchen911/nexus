"""The opt-in representation retains the absolute capacity guard and old domain."""

from unittest.mock import AsyncMock

import pytest
from nexus.agents.codex_capacity import (
    CapacityPoolMember,
    candidate_capacity_envelope,
    preflight_capacity_pool,
)
from unit.agents.test_codex_sdk import _authority
from unit.agents.test_full_input_capacity_v9 import committee


def model():
    _, _, prompts = _authority()
    return committee(
        prompts={
            role: p.model_copy(update={"version": "independent-monitor-research-v2"})
            for role, p in prompts.items()
        }
    )


@pytest.mark.asyncio
async def test_v2_one_byte_over_still_refuses_before_any_capacity_binding():
    request, evidence, _ = _authority()
    c = model()
    inputs = {"security_snapshot": {"items": [], "padding": ""}, "financial_facts": {"items": []}}
    base = candidate_capacity_envelope(c, request=request, evidence_refs=evidence, inputs=inputs)
    inputs["security_snapshot"]["padding"] = "x" * (100480 - base["wire_upper_bytes"])
    at_limit = candidate_capacity_envelope(
        c, request=request, evidence_refs=evidence, inputs=inputs
    )
    assert at_limit["wire_upper_bytes"] == 100480
    inputs["security_snapshot"]["padding"] = inputs["security_snapshot"]["padding"][:-1]
    assert (
        candidate_capacity_envelope(c, request=request, evidence_refs=evidence, inputs=inputs)[
            "wire_upper_bytes"
        ]
        == 100479
    )
    inputs["security_snapshot"]["padding"] += "x"
    c._inputs = AsyncMock(return_value=inputs)
    with pytest.raises(ValueError, match="^CODEX_CAPACITY_POOL_WIRE_EXCEEDED$"):
        await preflight_capacity_pool(
            [CapacityPoolMember(c, request, evidence)],
            expected_instrument_ids=(request.instrument_id,),
        )
    assert c._capacity_pool_manifest is None
    assert c.usage.provider_calls == 0


@pytest.mark.asyncio
async def test_v2_missing_vocabulary_preserves_conservative_input_rejection(monkeypatch, tmp_path):
    from nexus.agents import codex_token_budget

    monkeypatch.setattr(codex_token_budget, "tokenizer_cache_path", lambda: tmp_path / "missing")
    request, evidence, _ = _authority()
    c = model()
    inputs = {
        "security_snapshot": {"items": [], "padding": "x" * 50000},
        "financial_facts": {"items": []},
    }
    c._inputs = AsyncMock(return_value=inputs)
    with pytest.raises(ValueError, match="^CODEX_CAPACITY_POOL_INPUT_EXCEEDED$"):
        await preflight_capacity_pool(
            [CapacityPoolMember(c, request, evidence)],
            expected_instrument_ids=(request.instrument_id,),
        )
    assert c._capacity_pool_manifest is None
    assert c.usage.provider_calls == 0


def test_v2_marker_collision_cannot_silently_fall_back_to_unencoded_bound():
    request, evidence, _ = _authority()
    c = model()
    with pytest.raises(ValueError, match="^INDEPENDENT_INPUT_VIEW_UNAVAILABLE$"):
        candidate_capacity_envelope(
            c,
            request=request,
            evidence_refs=evidence,
            inputs={
                "security_snapshot": {"items": [], "literal": "$v:0"},
                "financial_facts": {"items": []},
            },
        )


def test_v2_mixed_unencoded_reasons_keep_six_byte_upper_bound(monkeypatch):
    from uuid import UUID

    from nexus.agents.codex_capacity import _monitoring_capacity_view

    request, evidence, _ = _authority()
    evidence = (evidence[0].model_copy(update={"source": "nexus_monitor_increment_v2"}),)
    context = {
        "purpose": "monitor_increment_v2",
        "trigger_checks": [
            {"trigger_id": str(UUID(int=i + 1)), "kind": "price", "event_key": ""}
            for i in range(32)
        ],
        "materials": {"contents": []},
    }
    monkeypatch.setattr(
        "nexus.monitoring.mixed_contracts.mixed_context_from_inputs", lambda _: context
    )
    monkeypatch.setattr("nexus.agents.codex_staged.mixed_context_from_inputs", lambda _: context)
    view = _monitoring_capacity_view(model(), evidence, {})
    proposal = view._outputs["market_state"].mixed_event_proposal
    assert proposal is not None and len(proposal.items) == 32
    assert all(item.rationale == "\x00" * 160 for item in proposal.items)
    for role in ("bear", "evidence_audit"):
        review = view._outputs[role].mixed_event_review
        assert review is not None and review.rationale == "\x00" * 160

    # The same real stage builder must retain literal markers in unencoded
    # auxiliary model prose rather than rejecting it after a prior SDK stage.
    from unit.agents.test_codex_sdk import _restore_view

    proposal = proposal.model_copy(
        update={
            "items": tuple(
                item.model_copy(update={"rationale": "$v:0\x00~00"}) for item in proposal.items
            )
        }
    )
    view._outputs["market_state"] = view._outputs["market_state"].model_copy(
        update={"mixed_event_proposal": proposal}
    )
    for role in ("bear", "evidence_audit"):
        review = view._outputs[role].mixed_event_review.model_copy(
            update={"proposal_hash": proposal.content_hash, "rationale": "$v:1\x1f~~"}
        )
        view._outputs[role] = view._outputs[role].model_copy(update={"mixed_event_review": review})
    for stage in ("critique", "synthesis"):
        payload, _ = view.build_stage_request(
            stage=stage,
            request=request,
            evidence_refs=evidence,
            frozen_inputs={"security_snapshot": {"items": []}, "financial_facts": {"items": []}},
            previous=[],
        )
        assert "$iv2" in payload
        restored = _restore_view(payload)
        assert restored["mixed_event_proposal"]["items"][0]["rationale"] == "$v:0\x00~00"
        if stage == "synthesis":
            assert restored["mixed_event_reviews"]["bear"]["rationale"] == "$v:1\x1f~~"
    assert view.usage.provider_calls == 0
