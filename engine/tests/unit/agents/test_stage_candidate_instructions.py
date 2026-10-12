"""Candidate assembly removes unrelated instructions without removing stage evidence."""

from copy import deepcopy

import pytest
from nexus.agents.codex_staged import StructuredStagedCodexCommittee
from unit.agents.test_codex_sdk import _authority, _restore_view, _tools


@pytest.mark.parametrize("stage", ["research", "critique", "synthesis"])
def test_candidate_sends_only_current_stage_instructions_and_prompt_content(stage):
    request, evidence, prompts = _authority()
    committee = StructuredStagedCodexCommittee(
        prompts=prompts, tools=_tools(), inline_citations=True
    )
    inputs = {"security_snapshot": {"items": []}, "financial_facts": {"items": []}}
    previous = [
        {
            "claim_id": "orchestrator:0",
            "role": "orchestrator",
            "vote": "support",
            "text": "研究问题",
            "citations": [0],
        }
    ]
    before = deepcopy((inputs, previous, prompts))
    args = dict(
        stage=stage,
        request=request,
        evidence_refs=evidence,
        frozen_inputs=inputs,
        previous=previous,
    )
    baseline, baseline_model = committee.build_stage_request(**args)
    candidate, candidate_model = committee.build_stage_request(
        **args, financial_table_candidate=True
    )
    assert candidate_model.model_json_schema() == baseline_model.model_json_schema()
    assert (inputs, previous, prompts) == before
    assert candidate["previous_claims"] == baseline["previous_claims"]
    assert candidate["frozen_inputs"] == baseline["frozen_inputs"]
    assert candidate["evidence"] == baseline["evidence"]
    visible_prompts = _restore_view(candidate["prompts"])
    assert all(set(prompt) == {"role", "content"} for prompt in visible_prompts)
    assert "execution" in candidate["instructions"]
    if stage == "research":
        assert "Research: identify" in candidate["instructions"]
        assert "Critique: assess" not in candidate["instructions"]
        assert "Synthesis: use two claims" not in candidate["instructions"]
        assert "leave reviewed_claim_ids empty" in candidate["instructions"]
    elif stage == "critique":
        assert "Critique: assess" in candidate["instructions"]
        assert "Research: identify" not in candidate["instructions"]
        assert "Synthesis: use two claims" not in candidate["instructions"]
        assert "reviewed_claim_ids" in candidate["instructions"]
    else:
        assert candidate["instructions"] == baseline["instructions"]
        assert "preserve that veto" in candidate["instructions"]


@pytest.mark.parametrize("invalid", [1, "true", None])
def test_candidate_requires_explicit_boolean(invalid):
    request, evidence, prompts = _authority()
    committee = StructuredStagedCodexCommittee(
        prompts=prompts, tools=_tools(), inline_citations=True
    )
    with pytest.raises(ValueError, match="^MODEL_FINANCIAL_TABLE_CANDIDATE_INVALID$"):
        committee.build_stage_request(
            stage="research",
            request=request,
            evidence_refs=evidence,
            frozen_inputs={},
            previous=[],
            financial_table_candidate=invalid,
        )
