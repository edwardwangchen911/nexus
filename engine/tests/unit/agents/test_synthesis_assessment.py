from datetime import UTC, datetime

import pytest
from nexus.agents.committee import CommitteeRoleOutput
from nexus.domain.evidence import EvidenceRef
from nexus.strategy.intents import ResearchClaim, SynthesisAssessment, SynthesisClaimReview


def evidence():
    return EvidenceRef(
        source="fixture",
        object_id="frozen:1",
        available_at=datetime(2026, 8, 28, tzinfo=UTC),
        location="memory://frozen",
        content_hash="a" * 64,
    )


def review(*, status="accepted", resolved=False):
    return SynthesisClaimReview(
        claim_id="bear:0",
        status=status,
        rationale="已回应具体审查事项。",
        evidence_refs=(evidence(),),
        resolved=resolved,
        missing_evidence=() if resolved else ("可比时点",),
        prevented_conclusion=None if resolved else "尚不能比较不同方案。",
    )


def test_accepted_issue_is_not_automatically_resolved():
    item = review()
    assert item.status == "accepted" and not item.resolved
    with pytest.raises(ValueError, match="sufficient"):
        SynthesisAssessment(
            stance="hold",
            claims=(ResearchClaim(text="承认问题。", evidence_refs=(evidence(),)),),
            reviews=(item,),
            review_conclusion="sufficient",
            uncertainties=(),
        )


def test_unresolved_review_must_explain_missing_evidence_and_consequence():
    with pytest.raises(ValueError, match="unresolved"):
        review().model_copy(update={"missing_evidence": ()}).model_validate(
            review().model_dump() | {"missing_evidence": ()}
        )


def test_absent_assessment_does_not_change_legacy_role_payload():
    output = CommitteeRoleOutput(
        role="portfolio",
        vote="abstain",
        claims=(ResearchClaim(text="观察。", evidence_refs=(evidence(),)),),
        prompt_hash="b" * 64,
        input_tokens=1,
        output_tokens=1,
    )
    assert "synthesis_assessment" not in output.model_dump(mode="json")
