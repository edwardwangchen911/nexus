"""Private final review evidence, separate from safe CLI diagnostics and accepted outputs."""

from typing import Any

from nexus.agents.committee import CommitteeRoleOutput
from nexus.agents.synthesis import citations_bound
from nexus.domain.evidence import EvidenceRef
from nexus.ops.historical_pool_diagnostic import _hash
from nexus.strategy.intents import ResearchClaim, ResearchIntent, SynthesisClaimReview

PRIVATE_KEY = "private_research_rejection"


def capture_rejection(
    case: dict[str, Any],
    index: int,
    intent: ResearchIntent,
    outputs: list[CommitteeRoleOutput],
) -> dict[str, Any]:
    """Persist model review statements, never infer new reasons or store full model input."""
    roles = {output.role: output for output in outputs}
    assessment = intent.synthesis_assessment
    original = roles["portfolio"].synthesis_assessment
    items = []
    for review in () if assessment is None else assessment.reviews:
        role, position = review.claim_id.split(":")
        claim = roles[role].claims[int(position)]  # type: ignore[index]
        items.append(
            dict(claim=claim.model_dump(mode="json"), review=review.model_dump(mode="json"))
        )
    payload = dict(
        version=2,
        case_hash=_hash(case),
        member_ordinal=index,
        review_conclusion=None if assessment is None else assessment.review_conclusion,
        original_review_conclusion=None if original is None else original.review_conclusion,
        outcome=dict(
            original_stance=None if original is None else original.stance,
            assessment_stance=None if assessment is None else assessment.stance,
            stance=intent.stance,
            exposure_action=intent.exposure_action,
            quality_status=intent.quality_status,
            evidence_audit=intent.evidence_audit,
            audit_vote=roles["evidence_audit"].vote,
            execution_permitted=False,
        ),
        items=items,
    )
    return dict(payload=payload, digest=_hash(payload))


def safe_rejection_detail(
    case: dict[str, Any],
    stage: str,
    detail: dict[str, Any],
) -> dict[str, Any]:
    """Validate private evidence; ordinary readback exposes only enums, IDs and counts."""
    safe = dict(detail)
    if PRIVATE_KEY not in safe:
        return safe  # Old observations remain unchanged; no invented missing detail.
    envelope = safe.pop(PRIVATE_KEY)
    try:
        payload = envelope["payload"]
        index = payload["member_ordinal"]
        if (
            type(payload["version"]) is not int
            or payload["version"] not in {1, 2}
            or _hash(payload) != envelope["digest"]
            or payload["case_hash"] != _hash(case)
            or type(index) is not int
            or not 0 <= index < len(case["members"])
            or stage != f"research_{index + 1}"
            or detail["code"] != "HISTORICAL_RESEARCH_NOT_ACTIONABLE"
            or payload["review_conclusion"] not in {None, "sufficient", "limited", "insufficient"}
            or payload["original_review_conclusion"]
            not in {None, "sufficient", "limited", "insufficient"}
            or not isinstance(payload["items"], list)
            or len(payload["items"]) > 4
        ):
            raise ValueError
        if payload["version"] == 2:
            outcome = payload["outcome"]
            stances = {"watch", "hold", "buy_candidate", "reduce_candidate"}
            if (
                set(outcome)
                != {
                    "original_stance",
                    "assessment_stance",
                    "stance",
                    "exposure_action",
                    "quality_status",
                    "evidence_audit",
                    "audit_vote",
                    "execution_permitted",
                }
                or outcome["original_stance"] not in stances | {None}
                or outcome["assessment_stance"] not in stances | {None}
                or outcome["stance"] not in stances
                or outcome["exposure_action"]
                not in {"eligible_for_increase", "reduce", "maintain", "none"}
                or outcome["quality_status"] not in {"complete", "partial"}
                or outcome["evidence_audit"] not in {"complete", "partial", "failed"}
                or outcome["audit_vote"] not in {"support", "oppose", "abstain"}
                or outcome["execution_permitted"] is not False
            ):
                raise ValueError
        evidence = tuple(
            EvidenceRef.model_validate(ref) for ref in case["members"][index]["evidence"]
        )
        seen = set()
        summaries = []
        for item in payload["items"]:
            claim = ResearchClaim.model_validate(item["claim"])
            review = SynthesisClaimReview.model_validate(item["review"])
            if review.claim_id in seen or not citations_bound(
                (claim, ResearchClaim(text="review", evidence_refs=review.evidence_refs)), evidence
            ):
                raise ValueError
            seen.add(review.claim_id)
            summaries.append(
                dict(
                    claim_id=review.claim_id,
                    status=review.status,
                    resolved=review.resolved,
                    missing_evidence_count=len(review.missing_evidence),
                    claim_evidence_count=len(claim.evidence_refs),
                    review_evidence_count=len(review.evidence_refs),
                )
            )
        if (
            sum(not row["resolved"] for row in summaries)
            != detail["research_rejection"]["unresolved_review_count"]
        ):
            raise ValueError
        safe["research_rejection"] = dict(
            detail["research_rejection"], private_detail_available=True, review_details=summaries
        )
        if payload["version"] == 2:
            safe["research_rejection"]["outcome"] = dict(payload["outcome"])
        return safe
    except (KeyError, IndexError, TypeError, ValueError, AttributeError):
        raise ValueError("HISTORICAL_CASE_READBACK_INVALID") from None
