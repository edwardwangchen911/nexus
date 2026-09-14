import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.agents.actionable_research import (
    ActionableResearchError,
    admit_actionable_research_output,
)
from nexus.agents.codex_versions import ACTIONABLE_CONFIG_VERSION
from nexus.agents.price_authority import FrozenPriceAuthorityStatus
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.agents import (
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.strategy.intents import (
    ResearchClaim,
    ResearchCondition,
    ResearchIntent,
    RoleVote,
    SynthesisAssessment,
    SynthesisClaimReview,
)

_RUN_ID = UUID("0199f100-1000-7000-8000-000000000001")
_OUTPUT_ID = UUID("0199f100-1000-7000-8000-000000000002")
_INSTRUMENT_ID = UUID("0199f100-1000-7000-8000-000000000003")
_SNAPSHOT_ID = UUID("0199f100-1000-7000-8000-000000000004")
_EVIDENCE_ID = UUID("0199f100-1000-7000-8000-000000000005")
_RESEARCH_TIME = datetime(2026, 9, 12, 20, tzinfo=UTC)
_DECISION_TIME = datetime(2026, 9, 13, 13, 35, tzinfo=UTC)


def _canonical_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()


def _case(mutation: str = "valid_buy") -> dict[str, object]:
    evidence = EvidenceRef(
        source="sec",
        object_id="filing:2026-q2",
        available_at=_RESEARCH_TIME - timedelta(hours=1),
        location="memory://frozen-sec",
        content_hash="e" * 64,
    )
    reviews = tuple(
        SynthesisClaimReview(
            claim_id=f"{role}:0",
            status="accepted",
            rationale="冻结证据已逐条回应。",
            evidence_refs=(evidence,),
            resolved=True,
            missing_evidence=(),
            prevented_conclusion=None,
        )
        for role in ("bear", "evidence_audit")
    )
    assessment = SynthesisAssessment(
        stance="buy_candidate",
        claims=(ResearchClaim(text="证据支持候选方向。", evidence_refs=(evidence,)),),
        reviews=reviews,
        review_conclusion="sufficient",
        uncertainties=(),
    )
    condition = ResearchCondition(
        signal="committee.evidence_valid",
        operator="is_true",
        threshold=True,
        unit="boolean",
        evidence_refs=(evidence,),
    )
    intent = ResearchIntent(
        synthesis_assessment=assessment,
        schema_version="research_intent_v1",
        decision_id=UUID("0199f100-1000-7000-8000-000000000006"),
        instrument_id=_INSTRUMENT_ID,
        display_symbol="AAPL",
        decision_time=_RESEARCH_TIME,
        data_snapshot_id=_SNAPSHOT_ID,
        horizon="medium_term",
        stance="buy_candidate",
        exposure_action="eligible_for_increase",
        theses=(ResearchClaim(text="证据支持候选方向。", evidence_refs=(evidence,)),),
        counter_theses=(ResearchClaim(text="已审查反方主张。", evidence_refs=(evidence,)),),
        uncertainties=("仍需遵守确定性风控。",),
        invalidation_conditions=(condition,),
        entry_conditions=(condition,),
        exit_conditions=(condition,),
        valid_until=_DECISION_TIME + timedelta(days=1),
        quality_status="complete",
        evidence_audit="complete",
        role_votes=tuple(
            RoleVote(role=role, vote="support")
            for role in (
                "orchestrator",
                "fundamental",
                "market_state",
                "macro_sector",
                "bull",
                "bear",
                "evidence_audit",
                "portfolio",
            )
        ),
        disagreement="none",
        strategy_version="baseline_v1",
        model_version="gpt-test",
        prompt_version="prompt-v2",
        data_version="snapshot-v1",
        evidence_refs=(evidence,),
    )
    intent_document = intent.model_dump(mode="json")
    payload = {"quality_status": "complete"}
    output = AgentResearchOutputRecord(
        id=_OUTPUT_ID,
        run_id=_RUN_ID,
        sequence=10,
        status="completed",
        role=None,
        prompt_hash=None,
        payload=payload,
        intent=intent_document,
        content_hash=_canonical_hash(
            {"status": "completed", "role": None, "payload": payload, "intent": intent_document}
        ),
    )
    run = AgentResearchRunRecord(
        id=_RUN_ID,
        instrument_id=_INSTRUMENT_ID,
        data_snapshot_id=_SNAPSHOT_ID,
        decision_time=_RESEARCH_TIME,
        status="completed",
        pricing_version=ACTIONABLE_CONFIG_VERSION,
    )
    binding = AgentResearchOutputEvidenceRecord(
        output_id=_OUTPUT_ID,
        evidence_ref_id=_EVIDENCE_ID,
        source=evidence.source,
        provider_object_id=evidence.object_id,
        revision="2026-q2",
        available_at=evidence.available_at,
        location=evidence.location,
        content_hash=evidence.content_hash,
    )
    terminals = (output,)

    if mutation == "legacy_version":
        run.pricing_version = "codex-sdk-chatgpt-v3"
    elif mutation == "run_not_completed":
        run.status = "synthesized"
    elif mutation == "output_not_latest":
        newer = AgentResearchOutputRecord(
            id=UUID("0199f100-1000-7000-8000-000000000007"),
            run_id=_RUN_ID,
            sequence=11,
            status="failed",
            role=None,
            prompt_hash=None,
            payload={"reason": "later terminal"},
            intent=None,
            content_hash="f" * 64,
        )
        terminals = (output, newer)
    elif mutation == "intent_hash_mismatch":
        output.intent = intent.model_copy(update={"disagreement": "changed"}).model_dump(
            mode="json"
        )
    elif mutation == "expired":
        intent = intent.model_copy(update={"valid_until": _DECISION_TIME - timedelta(seconds=1)})
    elif mutation == "unresolved_bear_review":
        changed = reviews[0].model_copy(
            update={
                "status": "unresolved",
                "resolved": False,
                "missing_evidence": ("bear evidence",),
                "prevented_conclusion": "方向不能准入。",
            }
        )
        intent = intent.model_copy(
            update={
                "synthesis_assessment": assessment.model_copy(
                    update={"reviews": (changed, reviews[1]), "review_conclusion": "limited"}
                )
            }
        )
    elif mutation == "unresolved_evidence_review":
        changed = reviews[1].model_copy(
            update={
                "status": "unresolved",
                "resolved": False,
                "missing_evidence": ("audit evidence",),
                "prevented_conclusion": "证据不能准入。",
            }
        )
        intent = intent.model_copy(
            update={
                "synthesis_assessment": assessment.model_copy(
                    update={"reviews": (reviews[0], changed), "review_conclusion": "limited"}
                )
            }
        )
    elif mutation == "evidence_audit_not_support":
        votes = tuple(
            vote.model_copy(update={"vote": "abstain"}) if vote.role == "evidence_audit" else vote
            for vote in intent.role_votes
        )
        intent = intent.model_copy(update={"role_votes": votes})
    elif mutation == "future_evidence":
        binding.available_at = _DECISION_TIME + timedelta(seconds=1)

    if mutation in {
        "expired",
        "unresolved_bear_review",
        "unresolved_evidence_review",
        "evidence_audit_not_support",
    }:
        output.intent = intent.model_dump(mode="json")
        output.content_hash = _canonical_hash(
            {
                "status": output.status,
                "role": output.role,
                "payload": output.payload,
                "intent": output.intent,
            }
        )

    return {
        "run": run,
        "output": output,
        "intent": intent,
        "terminal_outputs": terminals,
        "bound_evidence": (binding,),
        "decision_time": _DECISION_TIME,
        "price_authority_status": FrozenPriceAuthorityStatus.VALID,
    }


def _partial_watch_case(mutation: str = "valid") -> dict[str, object]:
    case = _case()
    intent = case["intent"]
    output = case["output"]
    assert isinstance(intent, ResearchIntent)
    assert isinstance(output, AgentResearchOutputRecord)
    assessment = intent.synthesis_assessment
    assert assessment is not None
    unresolved_bear = assessment.reviews[0].model_copy(
        update={
            "status": "unresolved",
            "resolved": False,
            "missing_evidence": ("price authority",),
            "prevented_conclusion": "Only watch/no-trade is permitted.",
        }
    )
    intent = intent.model_copy(
        update={
            "stance": "watch",
            "exposure_action": "none",
            "quality_status": "partial",
            "evidence_audit": "partial",
            "synthesis_assessment": assessment.model_copy(
                update={
                    "stance": "watch",
                    "reviews": (unresolved_bear, assessment.reviews[1]),
                    "review_conclusion": "limited",
                }
            ),
        }
    )
    if mutation == "increase":
        intent = intent.model_copy(
            update={"stance": "buy_candidate", "exposure_action": "eligible_for_increase"}
        )
    elif mutation == "failed_audit":
        intent = intent.model_copy(update={"evidence_audit": "failed"})
    output.payload = {"quality_status": "partial"}
    output.intent = intent.model_dump(mode="json")
    output.content_hash = _canonical_hash(
        {
            "status": output.status,
            "role": output.role,
            "payload": output.payload,
            "intent": output.intent,
        }
    )
    case["intent"] = intent
    return case


@pytest.mark.parametrize(
    "mutation",
    [
        "legacy_version",
        "run_not_completed",
        "output_not_latest",
        "intent_hash_mismatch",
        "expired",
        "unresolved_bear_review",
        "unresolved_evidence_review",
        "evidence_audit_not_support",
        "future_evidence",
    ],
)
def test_actionable_v4_fails_closed(mutation: str) -> None:
    with pytest.raises(ActionableResearchError, match="RESEARCH_OUTPUT_NOT_ACTIONABLE"):
        admit_actionable_research_output(**_case(mutation))


def test_actionable_v4_maps_direction_without_weight_or_quantity() -> None:
    result = admit_actionable_research_output(**_case())
    assert result.sizing_action == "increase"
    assert result.evidence_ref_ids == (_EVIDENCE_ID,)
    assert not hasattr(result, "target_weight")
    assert not hasattr(result, "quantity")


def test_paper_admission_maps_partial_watch_to_no_trade() -> None:
    case = _partial_watch_case()
    case["price_authority_status"] = FrozenPriceAuthorityStatus.MISSING
    result = admit_actionable_research_output(
        **case,
        allow_safe_no_trade=True,
    )

    assert result.stance == "watch"
    assert result.sizing_action == "none"
    assert result.evidence_ref_ids == (_EVIDENCE_ID,)


def test_default_actionable_admission_still_rejects_partial_watch() -> None:
    with pytest.raises(ActionableResearchError, match="RESEARCH_OUTPUT_NOT_ACTIONABLE"):
        admit_actionable_research_output(**_partial_watch_case())


@pytest.mark.parametrize("mutation", ["increase", "failed_audit"])
def test_paper_no_trade_admission_cannot_grant_exposure_authority(mutation: str) -> None:
    with pytest.raises(ActionableResearchError, match="RESEARCH_OUTPUT_NOT_ACTIONABLE"):
        admit_actionable_research_output(
            **_partial_watch_case(mutation), allow_safe_no_trade=True
        )


def test_paper_no_trade_admission_rejects_corrupt_price_authority() -> None:
    case = _partial_watch_case()
    case["price_authority_status"] = FrozenPriceAuthorityStatus.INVALID
    with pytest.raises(ActionableResearchError, match="RESEARCH_OUTPUT_NOT_ACTIONABLE"):
        admit_actionable_research_output(
            **case,
            allow_safe_no_trade=True,
        )


def test_missing_price_authority_cannot_grant_increase() -> None:
    case = _case()
    case["price_authority_status"] = FrozenPriceAuthorityStatus.MISSING
    with pytest.raises(ActionableResearchError, match="RESEARCH_OUTPUT_NOT_ACTIONABLE"):
        admit_actionable_research_output(
            **case,
            allow_safe_no_trade=True,
        )
