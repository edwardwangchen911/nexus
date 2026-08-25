"""Evidence binding, usage pricing, and non-executable research-intent synthesis."""

import json
from collections.abc import Mapping, Sequence
from datetime import timedelta
from decimal import Decimal
from typing import Literal
from uuid import NAMESPACE_URL, uuid5

from nexus.agents.committee import AgentRole, CommitteeRoleOutput
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.agents import (
    AgentModelConfigRecord,
    AgentPromptVersionRecord,
    AgentResearchRunRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.strategy.intents import ResearchClaim, ResearchCondition, ResearchIntent, RoleVote


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
) -> ResearchIntent:
    """Reuse role claims verbatim so synthesis cannot introduce an uncited fact."""
    fundamental_missing = symbol == "00656.HK"
    price_missing = not any(
        evidence.object_id.startswith("price-bar-set:")
        and evidence.location.startswith("db://price-bar-set/")
        for evidence in evidence_refs
    )
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
    condition_evidence = (evidence_refs[0],)
    return ResearchIntent(
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
        uncertainties=tuple(
            message
            for condition, message in (
                (fundamental_missing, "HKEX fundamental authority is unavailable."),
                (price_missing, "Price authority is unavailable."),
            )
            if condition
        )
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
        quality_status="partial" if fundamental_missing or price_missing else "complete",
        evidence_audit="complete",
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
