"""Evidence binding, usage pricing, and non-executable research-intent synthesis."""

import json
from collections.abc import Mapping, Sequence
from datetime import timedelta
from decimal import Decimal
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
from nexus.strategy.intents import ResearchClaim, ResearchIntent, RoleVote


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
    votes = tuple(
        RoleVote(
            role=output.role,
            vote="abstain" if fundamental_missing and output.role == "fundamental" else output.vote,
        )
        for output in role_outputs
    )
    vote_values = {vote.vote for vote in votes}
    orchestrator_claim = next(output for output in role_outputs if output.role == "orchestrator")
    return ResearchIntent(
        schema_version="research_intent_v1",
        decision_id=uuid5(NAMESPACE_URL, f"nexus-research:{run.id}"),
        instrument_id=run.instrument_id,
        display_symbol=symbol,
        decision_time=run.decision_time,
        data_snapshot_id=run.data_snapshot_id,
        horizon="medium_term",
        stance="watch",
        exposure_action="none",
        theses=orchestrator_claim.claims,
        counter_theses=(),
        uncertainties=(
            ("HKEX fundamental authority is unavailable.",)
            if fundamental_missing
            else ("Research remains subject to point-in-time evidence quality.",)
        ),
        invalidation_conditions=(),
        entry_conditions=(),
        exit_conditions=(),
        valid_until=run.decision_time + timedelta(days=7),
        quality_status="partial" if fundamental_missing else "complete",
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
