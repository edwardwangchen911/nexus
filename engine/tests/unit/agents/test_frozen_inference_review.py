"""Frozen public AVGO prose stays partial; new guidance cannot erase an audit veto."""

import json
from copy import deepcopy
from pathlib import Path
from uuid import UUID

import pytest
from nexus.agents.codex_versions import BOUNDED_STAGE_BUDGET_CONFIG_VERSION
from nexus.agents.committee import CommitteeRoleOutput, load_portfolio_synthesis_prompt
from nexus.agents.synthesis import synthesize_intent
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from unit.agents.test_codex_sdk import _authority


@pytest.mark.parametrize("new_guidance", [False, True])
@pytest.mark.parametrize("resolve_later", [False, True])
def test_frozen_avgo_public_case_preserves_audit_veto(new_guidance, resolve_later):
    fixture = Path(__file__).parents[2] / "fixtures/agents/avgo_inference_review_public.json"
    document = json.loads(fixture.read_text())
    original = deepcopy(document)
    rows = deepcopy(document["outputs"])
    assessment = rows[-1]["synthesis_assessment"]
    unresolved = [r["claim_id"] for r in assessment["reviews"] if not r["resolved"]]
    assert unresolved == ["evidence_audit:0"]
    assert rows[0]["vote"] == "support" and rows[-2]["vote"] == "oppose"
    assert "判断阈值或标准" in rows[-2]["claims"][0]["text"]
    if resolve_later:
        # Counterfactual synthesis cannot reclassify the earlier frozen audit vote.
        assessment.update(stance="buy_candidate", review_conclusion="sufficient")
        for review in assessment["reviews"]:
            review.update(resolved=True, missing_evidence=[], prevented_conclusion=None)
    request, evidence, prompts = _authority()
    prompts = dict(prompts)
    prompts["portfolio"] = load_portfolio_synthesis_prompt(
        security_research_scope=True,
        claim_review_scope=True,
        inference_review_scope=new_guidance,
    )
    outputs = [CommitteeRoleOutput.model_validate(row) for row in rows]
    intent = synthesize_intent(
        AgentResearchRunRecord(id=UUID(int=3), **request.model_dump()),
        "AVGO",
        DataSnapshot(id=request.data_snapshot_id, revision="synthetic"),
        {
            role: AgentPromptVersionRecord(
                role=role, version=p.version, content=p.content, content_hash=p.content_hash
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
    assert intent.quality_status == "partial" and intent.stance == "watch"
    assert intent.exposure_action == "none"
    assert intent.synthesis_assessment.review_conclusion == "limited"
    assert next(v.vote for v in intent.role_votes if v.role == "evidence_audit") == "oppose"
    assert document == original
