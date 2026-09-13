from datetime import UTC, datetime

import pytest
from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSION
from nexus.agents.committee import CommitteeRoleOutput
from nexus.agents.synthesis import staged_intent_semantics_valid
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


@pytest.mark.parametrize("kind", ["intent", "role", "trace_role"])
def test_optional_assessment_keeps_closed_serialization_schema(kind):
    from nexus.api.schemas.agents import ResearchStageRole
    from nexus.strategy.intents import ResearchIntent

    model = {
        "intent": ResearchIntent,
        "role": CommitteeRoleOutput,
        "trace_role": ResearchStageRole,
    }[kind]
    schema = model.model_json_schema(mode="serialization")
    assert schema["additionalProperties"] is False
    assert "synthesis_assessment" in schema["properties"]
    assert "stance" in schema["properties"] if kind == "intent" else "role" in schema["properties"]


def test_actionable_version_does_not_admit_a_legacy_watch_shape():
    from unit.agents.test_execution import _synthesize

    intent = _synthesize(("support",) * 8, structured=True)
    assert not staged_intent_semantics_valid(intent, ACTIONABLE_CONFIG_VERSION)


def test_actionable_version_rejects_a_missing_evidence_audit_review_without_raising():
    from unit.agents.test_execution import _synthesize

    intent = _synthesize(("support",) * 8, actionable=True)
    assessment = intent.synthesis_assessment
    assert assessment is not None
    intent = intent.model_copy(
        update={
            "synthesis_assessment": assessment.model_copy(
                update={"reviews": (assessment.reviews[0],)}
            )
        }
    )
    assert not staged_intent_semantics_valid(intent, ACTIONABLE_CONFIG_VERSION)
