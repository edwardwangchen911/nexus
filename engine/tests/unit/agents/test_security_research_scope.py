"""Versioned wire scope: clarify future research without rewriting old requests."""

from copy import deepcopy
from uuid import UUID

import pytest
from nexus.agents.codex_sdk import codex_request_hash
from nexus.agents.codex_staged import StructuredStagedCodexCommittee
from nexus.agents.committee import load_portfolio_synthesis_prompt
from unit.agents.test_codex_sdk import _authority, _tools

SCOPE_VERSION = "local-portfolio-synthesis-v5-paper-scope"
STAGES = ("research", "critique", "synthesis")


def _wire(stage, *, new=False, context=True, scope=True, capacity=True, security_extra=None):
    request, evidence, prompts = _authority()
    request = request.model_copy(
        update={"instrument_id": UUID(int=1), "data_snapshot_id": UUID(int=2)}
    )
    prompts = dict(prompts)
    prompts["portfolio"] = load_portfolio_synthesis_prompt(security_research_scope=new)
    model = StructuredStagedCodexCommittee(
        prompts=prompts, tools=_tools(), paper_stage_scope=scope, inline_citations=capacity
    )
    inputs = {
        "security_snapshot": {
            "items": [
                {
                    "source": evidence[0].source,
                    "provider_object_id": evidence[0].object_id,
                    "content_hash": evidence[0].content_hash,
                    "available_at": evidence[0].model_dump(mode="json")["available_at"],
                    "evidence_ref_id": "fixture-evidence",
                    "agent_paper_context": {"execution_locked": True},
                }
            ]
            if context
            else [],
            **(security_extra or {}),
        },
        "financial_facts": {"status": "unavailable", "items": []},
    }
    prior = (
        []
        if stage == "research"
        else [
            {
                "claim_id": "orchestrator:0",
                "role": "orchestrator",
                "vote": "support",
                "text": "原始权重不能证明集中度触发。",
                "citations": [0],
            },
        ]
    )
    if stage == "synthesis":
        prior += [
            {
                "claim_id": f"{role}:0",
                "role": role,
                "vote": "abstain",
                "text": "修订会计影响尚未核实，不能建立估值结论。",
                "citations": [0],
            }
            for role in ("bear", "evidence_audit")
        ]
    before = deepcopy((inputs, prior))
    payload, output_model = model.build_stage_request(
        stage=stage,
        request=request,
        evidence_refs=evidence,
        frozen_inputs=inputs,
        previous=prior,
        financial_table_candidate=capacity,
    )
    assert (inputs, prior) == before
    return payload, output_model


@pytest.mark.parametrize("stage", STAGES)
@pytest.mark.parametrize("capacity", [False, True])
def test_new_formal_scope_reaches_each_stage_without_inventing_concentration(stage, capacity):
    payload, _ = _wire(stage, new=True, capacity=capacity)
    assert payload.get("research_scope") == "single_security"
    instructions = payload["instructions"]
    assert "holding's concentration trigger" not in instructions
    assert "original portfolio question" not in instructions
    assert "raw weights do not establish whether a concentration trigger is met" in instructions
    assert "never resolves a genuine evidence gap" in instructions
    assert (
        "Do not rewrite or infer a different vote" in instructions if stage == "synthesis" else True
    )
    if stage != "research":
        assert payload["previous_claims"][-1]["vote"] == (
            "abstain" if stage == "synthesis" else "support"
        )


@pytest.mark.parametrize("context,scope", [(False, True), (True, False), (False, False)])
@pytest.mark.parametrize("stage", STAGES)
def test_new_prompt_does_not_scope_unrelated_or_contextless_requests(context, scope, stage):
    payload, _ = _wire(stage, new=True, context=context, scope=scope)
    assert "research_scope" not in payload
    if stage == "research":
        assert "holding's concentration trigger" in payload["instructions"]
    if stage == "synthesis":
        assert "original portfolio question" in payload["instructions"]


def test_scope_prompt_is_explicit_and_keeps_unresolved_audit_and_evidence_rules():
    old = load_portfolio_synthesis_prompt()
    new = load_portfolio_synthesis_prompt(security_research_scope=True)
    assert old.version == "local-portfolio-synthesis-v4"
    assert new.version == SCOPE_VERSION and new.content_hash != old.content_hash
    assert "research_scope=single_security" in new.content
    assert "Otherwise" in new.content
    for rule in (
        "Confirming that a gap exists does not resolve it",
        "specific observable input",
        "audit vote remains non-support",
        "Never rewrite or infer a different role vote",
    ):
        assert rule in old.content and rule in new.content


@pytest.mark.parametrize(
    "extra",
    [
        {"purpose": "research_only_v2"},
        {"diagnostic_synthetic_paper_load": {}},
    ],
)
@pytest.mark.parametrize("stage", STAGES)
def test_research_only_and_synthetic_load_do_not_gain_formal_scope(extra, stage):
    payload, _ = _wire(stage, new=True, security_extra=extra)
    assert "research_scope" not in payload


LEGACY_HASHES = {
    ("research", False): "9ecb58a3de711834a33ae106afc6a88d59061ec7e174b03bd0e00258fa912021",
    ("critique", False): "86d1ccc95ceded7fb4b175b5e4f901ff49b4c38200329c8168a611d0d561fe5b",
    ("synthesis", False): "5b45b53c8fe24229d16320b267d46a16ed512102c89d6a9ad0f044cd02538348",
    ("research", True): "2426442f8526b1adb08fec0200d0c95a5e674cafb9e0db487bd9860220c43015",
    ("critique", True): "8f8db8f616000867f891c3a0962fee3acf5ed8f34b43e482b557bf652de92d83",
    ("synthesis", True): "69d4b85421e15c0735a5c29736ad9bde7bb4376e20868254c323adec3e1ea6a0",
}


@pytest.mark.parametrize("stage,capacity", LEGACY_HASHES)
def test_frozen_v4_full_wire_is_unchanged(stage, capacity):
    payload, output_model = _wire(stage, capacity=capacity)
    assert codex_request_hash(payload, output_model) == LEGACY_HASHES[stage, capacity]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "audit_vote,resolved,quality",
    [
        ("abstain", True, "partial"),
        ("support", False, "partial"),
        ("support", True, "complete"),
    ],
)
async def test_new_scope_through_sdk_and_synthesis_keeps_real_audit_gate(
    monkeypatch,
    audit_vote,
    resolved,
    quality,
):
    import asyncio
    import json
    from unittest.mock import AsyncMock

    from nexus.agents.codex_capacity import CapacityPoolMember, preflight_capacity_pool
    from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
    from nexus.agents.committee import ROLE_ORDER
    from nexus.agents.synthesis import synthesize_intent
    from nexus.persistence.models.agents import (
        AgentModelConfigRecord,
        AgentPromptVersionRecord,
        AgentResearchRunRecord,
    )
    from nexus.persistence.models.data import DataSnapshot
    from unit.agents.test_codex_sdk import _assessment_payload, _sdk, _staged_batch

    request, evidence, prompts = _authority()
    prompts = dict(prompts)
    prompts["portfolio"] = load_portfolio_synthesis_prompt(security_research_scope=True)
    _, thread, result, _ = _sdk(monkeypatch)
    seen = []

    async def run(wire, **options):
        payload = json.loads(wire)
        seen.append(payload["stage"])
        assert payload["research_scope"] == "single_security"
        if payload["stage"] == "synthesis":
            body = _assessment_payload(payload["previous_claims"])
            if resolved:
                for review in body["synthesis_assessment"]["reviews"]:
                    review.update(resolved=True, missing_evidence=[], prevented_conclusion=None)
                # Even an overconfident model cannot erase the earlier audit vote.
                body["synthesis_assessment"]["review_conclusion"] = "sufficient"
        else:
            body = _staged_batch(payload["stage"], payload["requested_roles"])
            for role in body["roles"]:
                role["vote"] = audit_vote if role["role"] == "evidence_audit" else "support"
        result.final_response = json.dumps(body)
        return result

    thread.run.side_effect = run
    committee = StructuredStagedCodexCommittee(
        prompts=prompts,
        tools=_tools(),
        paper_stage_scope=True,
        inline_citations=True,
        aggregate_input_tokens=96000,
        max_wire_bytes=100479,
        stage_time_budget=True,
        stage_budget_version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION,
        deadline=asyncio.get_running_loop().time() + 300,
    )
    monkeypatch.setattr(
        committee,
        "_inputs",
        AsyncMock(
            return_value={
                "security_snapshot": {
                    "items": [
                        {
                            "source": evidence[0].source,
                            "provider_object_id": evidence[0].object_id,
                            "content_hash": evidence[0].content_hash,
                            "available_at": evidence[0].model_dump(mode="json")["available_at"],
                            "evidence_ref_id": "fixture-evidence",
                            "agent_paper_context": {"execution_locked": True},
                        }
                    ]
                },
                "financial_facts": {"status": "unavailable", "items": []},
            }
        ),
    )
    await preflight_capacity_pool(
        [CapacityPoolMember(committee, request, evidence)],
        expected_instrument_ids=(request.instrument_id,),
    )
    outputs = [
        await committee.analyze(role, request, evidence, prompts[role]) for role in ROLE_ORDER
    ]
    intent = synthesize_intent(
        AgentResearchRunRecord(id=UUID(int=3), **request.model_dump()),
        "ABC",
        DataSnapshot(id=request.data_snapshot_id, revision="synthetic"),
        {
            role: AgentPromptVersionRecord(
                role=role,
                version=p.version,
                content=p.content,
                content_hash=p.content_hash,
            )
            for role, p in prompts.items()
        },
        AgentModelConfigRecord(
            model_id="fixture-model", version=BOUNDED_STAGE_BUDGET_CONFIG_VERSION
        ),
        evidence,
        outputs,
        price_authority_valid=True,
    )
    assert seen == list(STAGES) and committee.usage.provider_calls == 3
    assert intent.quality_status == quality
    assert all(review.resolved is resolved for review in intent.synthesis_assessment.reviews)
    if quality == "partial":
        assert intent.stance == "watch" and intent.exposure_action == "none"
        assert intent.synthesis_assessment.review_conclusion != "sufficient"
    else:
        assert intent.stance == "buy_candidate"
    assert next(v.vote for v in intent.role_votes if v.role == "evidence_audit") == audit_vote
