"""v8 admission bounds prior monitoring output before any SDK invocation."""

from copy import copy
from datetime import UTC, datetime
from unittest.mock import AsyncMock

import pytest
from nexus.agents import codex_capacity
from nexus.agents.codex_sdk import codex_request_wire
from nexus.agents.codex_staged import STAGES
from nexus.agents.committee import ROLE_ORDER, CommitteeRoleOutput, load_portfolio_synthesis_prompt
from nexus.monitoring.contracts import PriceMonitoringProposal, PriceMonitoringReview
from nexus.strategy.intents import ResearchClaim
from unit.agents.test_codex_sdk import _authority
from unit.agents.test_full_input_capacity_v9 import committee
from unit.agents.test_monitoring_producer import proposal_wire


def capacity_fixture(*, monitoring=True):
    request, evidence, prompts = _authority()
    prompts = dict(prompts)
    prompts["portfolio"] = load_portfolio_synthesis_prompt(
        security_research_scope=True,
        claim_review_scope=True,
        inference_review_scope=True,
        monitoring_scope=monitoring,
    )
    model = committee(prompts=prompts)
    inputs = {
        "security_snapshot": {
            "items": [
                {
                    "source": evidence[0].source,
                    "provider_object_id": evidence[0].object_id,
                    "content_hash": evidence[0].content_hash,
                    "available_at": evidence[0].model_dump(mode="json")["available_at"],
                    "evidence_ref_id": "fixture",
                    "agent_paper_context": {"execution_locked": True},
                }
            ],
        },
        "financial_facts": {"items": []},
    }
    return model, dict(request=request, evidence_refs=evidence, inputs=inputs)


def with_maximal_monitoring(model, args):
    # A reachable domain value: 38 digits, including four-byte Unicode digits.
    wire = proposal_wire(args["request"].decision_time)
    wire.pop("citations")
    wire["condition"]["threshold"] = "9" + "𝟏" * 25 + "." + "𝟏" * 12
    wire.update(
        rationale="\x00" * 500,
        review_due_at=datetime.max.replace(tzinfo=UTC),
        valid_until=datetime.max.replace(tzinfo=UTC),
        evidence_refs=(args["evidence_refs"][-1],) * 4,
    )
    proposal = PriceMonitoringProposal(**wire)
    review = PriceMonitoringReview(
        proposal_hash=proposal.content_hash,
        decision="accepted",
        rationale="\x00" * 500,
        evidence_refs=proposal.evidence_refs,
    )
    complete = copy(model)
    complete._outputs = {
        role: CommitteeRoleOutput(
            role=role,
            vote="support",
            claims=(ResearchClaim(text="边界", evidence_refs=args["evidence_refs"]),),
            prompt_hash=model._prompts[role].content_hash,
            input_tokens=0,
            output_tokens=0,
            monitoring_proposal=proposal if role == "market_state" else None,
            monitoring_review=review if role != "market_state" else None,
        )
        for role in ("market_state", "bear", "evidence_audit")
    }
    return complete


def test_v8_capacity_bounds_maximal_original_proposal_and_both_reviews():
    model, args = capacity_fixture()
    empty = codex_capacity.candidate_capacity_envelope(model, **args)
    populated = with_maximal_monitoring(model, args)
    for stage, roles in STAGES:
        previous = [
            dict(
                claim_id=f"{role}:{index}",
                role=role,
                vote="abstain",
                text="\x00" * 400,
                citations=[len(args["evidence_refs"]) - 1] * 3,
            )
            for role in ROLE_ORDER[: ROLE_ORDER.index(roles[0])]
            for index in range(2)
        ]
        payload, _ = populated.build_stage_request(
            stage=stage,
            request=args["request"],
            evidence_refs=args["evidence_refs"],
            frozen_inputs=args["inputs"],
            previous=previous,
            financial_table_candidate=True,
        )
        bound = next(row for row in empty["stages"] if row["stage"] == stage)
        assert bound["wire_upper_bytes"] >= len(codex_request_wire(payload).encode())
    assert model._outputs == {} and model.usage.provider_calls == 0


def test_v7_capacity_is_unchanged_by_monitoring_outputs():
    model, args = capacity_fixture(monitoring=False)
    before = codex_capacity.candidate_capacity_envelope(model, **args)
    after = codex_capacity.candidate_capacity_envelope(with_maximal_monitoring(model, args), **args)
    assert before == after
    assert [(row["stage"], row["wire_upper_bytes"]) for row in before["stages"]] == [
        ("research", 7932),
        ("critique", 31671),
        ("synthesis", 43909),
    ]


@pytest.mark.asyncio
async def test_v8_preflight_rejects_monitor_context_overflow_before_sdk(monkeypatch):
    model, args = capacity_fixture()
    # The legacy empty-context proof admits this shape; the actual largest
    # monitoring context exceeds the unchanged 100479-byte ceiling.
    args["inputs"]["security_snapshot"]["padding"] = "x" * 54000
    model._inputs = AsyncMock(return_value=args["inputs"])
    monkeypatch.setattr(codex_capacity, "codex_input_allowance", lambda *_args: 100)
    with pytest.raises(ValueError, match="^CODEX_CAPACITY_POOL_WIRE_EXCEEDED$"):
        await codex_capacity.preflight_capacity_pool(
            [codex_capacity.CapacityPoolMember(model, args["request"], args["evidence_refs"])],
            expected_instrument_ids=(args["request"].instrument_id,),
        )
    assert model._capacity_pool_manifest is None and model.usage.provider_calls == 0
