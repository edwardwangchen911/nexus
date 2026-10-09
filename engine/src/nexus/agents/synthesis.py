"""Evidence binding, usage pricing, and non-executable research-intent synthesis."""

import json
from collections.abc import Mapping, Sequence
from datetime import timedelta
from decimal import Decimal
from typing import Literal
from uuid import NAMESPACE_URL, uuid5

from nexus.agents.actionable_research import actionable_intent_shape_valid
from nexus.agents.codex_versions import (
    ACTIONABLE_CONFIG_VERSIONS,
    CANDIDATE_BUDGET_CONFIG_VERSION,
    STAGED_CONFIG_VERSIONS,
    STRUCTURED_CONFIG_VERSIONS,
)
from nexus.agents.committee import AgentRole, CommitteeRoleOutput
from nexus.agents.concentration import evaluate_frozen_concentration_conditions
from nexus.agents.portfolio_context import PortfolioResearchContext
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.strategy.intents import ResearchClaim, ResearchCondition, ResearchIntent, RoleVote

STAGED_SEMANTICS_UNCERTAINTY = "三阶段角色表态未明确投资方向或审查缺口是否解决，仅供研究观察。"


def staged_intent_semantics_valid(intent: ResearchIntent, pricing_version: str) -> bool:
    """Legacy stage votes establish neither an investment action nor resolved objections."""
    if (
        pricing_version in ACTIONABLE_CONFIG_VERSIONS
        or pricing_version == CANDIDATE_BUDGET_CONFIG_VERSION
    ):
        return actionable_intent_shape_valid(intent)
    return pricing_version not in STAGED_CONFIG_VERSIONS or (
        intent.stance == "watch"
        and intent.exposure_action == "none"
        and intent.quality_status == "partial"
        and (
            pricing_version not in STRUCTURED_CONFIG_VERSIONS
            or intent.synthesis_assessment is not None
        )
    )


def domain_evidence(record: ResearchEvidenceRef) -> EvidenceRef:
    return EvidenceRef(
        source=record.source,
        object_id=record.provider_object_id,
        available_at=record.available_at,
        location=record.location,
        content_hash=record.content_hash,
    )


def citations_bound(
    claims: Sequence[ResearchClaim], evidence_refs: tuple[EvidenceRef, ...]
) -> bool:
    authority = {json.dumps(item.model_dump(mode="json"), sort_keys=True) for item in evidence_refs}
    return bool(claims) and all(
        claim.evidence_refs
        and all(
            json.dumps(item.model_dump(mode="json"), sort_keys=True) in authority
            for item in claim.evidence_refs
        )
        for claim in claims
    )


def usage_cost(model: AgentModelConfigRecord, input_tokens: int, output_tokens: int) -> Decimal:
    if input_tokens > model.max_input_tokens or output_tokens > model.max_output_tokens:
        raise ValueError("committee token usage exceeded immutable model limits")
    return (
        Decimal(input_tokens) * model.input_usd_per_million
        + Decimal(output_tokens) * model.output_usd_per_million
    ) / Decimal(1_000_000)


def synthesize_intent(
    run: AgentResearchRunRecord,
    symbol: str,
    snapshot: DataSnapshot,
    prompts: Mapping[AgentRole, AgentPromptVersionRecord],
    model: AgentModelConfigRecord,
    evidence_refs: tuple[EvidenceRef, ...],
    role_outputs: Sequence[CommitteeRoleOutput],
    *,
    price_authority_valid: bool,
    portfolio_context: PortfolioResearchContext | None = None,
) -> ResearchIntent:
    """Reuse role claims verbatim so synthesis cannot introduce an uncited fact."""
    fundamental_missing = symbol == "00656.HK"
    price_missing = not price_authority_valid
    votes = tuple(
        RoleVote(
            role=output.role,
            vote=(
                "abstain"
                if (fundamental_missing and output.role == "fundamental")
                or (price_missing and output.role == "market_state")
                else output.vote
            ),
        )
        for output in role_outputs
    )
    vote_values = {vote.vote for vote in votes}
    support_count = sum(vote.vote == "support" for vote in votes)
    oppose_count = sum(vote.vote == "oppose" for vote in votes)
    stance, exposure_action = _decision(
        support_count=support_count,
        oppose_count=oppose_count,
        fundamental_missing=fundamental_missing,
        price_missing=price_missing,
    )
    support_claims = tuple(
        claim
        for output, vote in zip(role_outputs, votes, strict=True)
        if vote.vote == "support"
        for claim in output.claims
    )[:5]
    oppose_claims = tuple(
        claim
        for output, vote in zip(role_outputs, votes, strict=True)
        if vote.vote == "oppose"
        for claim in output.claims
    )[:5]
    by_role = {output.role: output for output in role_outputs}
    theses = support_claims or by_role["orchestrator"].claims[:5]
    counter_theses = oppose_claims or by_role["bear"].claims[:5]
    # Candidate v7 changes only time allocation, not the frozen v6 intent shape.
    # Execution admission still excludes this qualification-only identity.
    candidate = model.version == CANDIDATE_BUDGET_CONFIG_VERSION
    structured = model.version in STRUCTURED_CONFIG_VERSIONS or candidate
    staged = model.version in STAGED_CONFIG_VERSIONS or candidate
    actionable = model.version in ACTIONABLE_CONFIG_VERSIONS or candidate
    assessment = by_role["portfolio"].synthesis_assessment if structured else None
    review_incomplete = staged and by_role["evidence_audit"].vote != "support"
    if staged:
        # Preserve the final prose and objections; the contract has no action field.
        theses = by_role["portfolio"].claims[:5]
        counter_theses = (by_role["bear"].claims + by_role["evidence_audit"].claims)[:5]
        stance, exposure_action = ("watch", "none")
    if structured:
        if assessment is None:
            raise ValueError("STRUCTURED_SYNTHESIS_ASSESSMENT_MISSING")
        if (
            actionable
            and by_role["evidence_audit"].vote != "support"
            and assessment.review_conclusion == "sufficient"
        ):
            assessment = assessment.model_copy(update={"review_conclusion": "limited"})
        review_roles: tuple[AgentRole, ...] = ("bear", "evidence_audit")
        required = {
            f"{role}:{index}"
            for role in review_roles
            for index, _ in enumerate(by_role[role].claims)
        }
        review_claims = tuple(
            ResearchClaim(text=review.rationale, evidence_refs=review.evidence_refs)
            for review in assessment.reviews
        )
        if {review.claim_id for review in assessment.reviews} != required or not citations_bound(
            assessment.claims + review_claims, evidence_refs
        ):
            raise ValueError("STRUCTURED_SYNTHESIS_REVIEW_INVALID")
        theses = assessment.claims
        review_incomplete = (
            by_role["evidence_audit"].vote != "support"
            or assessment.review_conclusion != "sufficient"
            or any(not review.resolved for review in assessment.reviews)
        )
        # Only v4 can preserve the structurally reviewed portfolio direction.
        stance = assessment.stance if actionable and not review_incomplete else "watch"
        exposure_action = exposure_action_for_stance(stance)
    extra_uncertainties = (
        ()
        if assessment is None
        else assessment.uncertainties
        + tuple(
            review.prevented_conclusion
            for review in assessment.reviews
            if review.prevented_conclusion
        )
    )
    condition_evidence = (evidence_refs[0],)
    if portfolio_context is not None and (
        portfolio_context.instrument_id != run.instrument_id
        or portfolio_context.research_as_of > run.decision_time
    ):
        raise ValueError("research observation context does not bind the run")
    return ResearchIntent(
        synthesis_assessment=assessment,
        condition_evaluations=evaluate_frozen_concentration_conditions(portfolio_context),
        schema_version="research_intent_v1",
        decision_id=uuid5(NAMESPACE_URL, f"nexus-research:{run.id}"),
        instrument_id=run.instrument_id,
        display_symbol=symbol,
        decision_time=run.decision_time,
        data_snapshot_id=run.data_snapshot_id,
        horizon="medium_term",
        stance=stance,
        exposure_action=exposure_action,
        theses=theses,
        counter_theses=counter_theses,
        uncertainties=(
            tuple(
                message
                for condition, message in (
                    (staged and not structured, STAGED_SEMANTICS_UNCERTAINTY),
                    (
                        structured and not actionable,
                        "结构化审查已保存，但尚不具备完整确定性决策准入，仅供研究观察。",
                    ),
                    (fundamental_missing, "HKEX fundamental authority is unavailable."),
                    (price_missing, "Price authority is unavailable."),
                    (review_incomplete, "Evidence review has unresolved objections."),
                )
                if condition
            )
            + extra_uncertainties
        )[:10]
        or ("Research remains subject to point-in-time evidence quality.",),
        invalidation_conditions=(
            ResearchCondition(
                signal="committee.evidence_valid",
                operator="is_false",
                threshold=True,
                unit="boolean",
                evidence_refs=condition_evidence,
            ),
        ),
        entry_conditions=(
            ResearchCondition(
                signal="committee.support_ready",
                operator="is_true",
                threshold=True,
                unit="boolean",
                evidence_refs=condition_evidence,
            ),
        ),
        exit_conditions=(
            ResearchCondition(
                signal="committee.risk_triggered",
                operator="is_true",
                threshold=True,
                unit="boolean",
                evidence_refs=condition_evidence,
            ),
        ),
        valid_until=run.decision_time + timedelta(days=7),
        quality_status="partial"
        if (staged and not actionable) or fundamental_missing or price_missing or review_incomplete
        else "complete",
        evidence_audit="partial"
        if staged and (not actionable or by_role["evidence_audit"].vote != "support")
        else "complete",
        role_votes=votes,
        disagreement="support and opposition are both present"
        if {"support", "oppose"}.issubset(vote_values)
        else "none",
        strategy_version="baseline_v1",
        model_version=model.model_id,
        prompt_version=prompts["orchestrator"].version,
        data_version=snapshot.revision,
        evidence_refs=evidence_refs,
    )


def _decision(
    *,
    support_count: int,
    oppose_count: int,
    fundamental_missing: bool,
    price_missing: bool,
) -> tuple[
    Literal["buy_candidate", "reduce_candidate", "hold", "watch"],
    Literal["none", "reduce", "maintain", "eligible_for_increase"],
]:
    if fundamental_missing or price_missing:
        return ("watch", "none")
    if oppose_count > support_count:
        return ("reduce_candidate", "reduce")
    if support_count >= oppose_count + 2:
        return ("buy_candidate", "eligible_for_increase")
    if support_count or oppose_count:
        return ("hold", "maintain")
    return ("watch", "none")


def exposure_action_for_stance(
    stance: Literal["buy_candidate", "reduce_candidate", "hold", "watch"],
) -> Literal["none", "reduce", "maintain", "eligible_for_increase"]:
    actions: dict[str, Literal["none", "reduce", "maintain", "eligible_for_increase"]] = {
        "buy_candidate": "eligible_for_increase",
        "reduce_candidate": "reduce",
        "hold": "maintain",
        "watch": "none",
    }
    return actions[stance]
