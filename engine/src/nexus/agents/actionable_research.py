"""Fail-closed admission for direction-only v4 Agent research outputs."""

import hashlib
import json
from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from nexus.agents.price_authority import FrozenPriceAuthorityStatus
from nexus.audit.canonical import Hash64
from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.agents import (
    AgentResearchOutputEvidenceRecord,
    AgentResearchOutputRecord,
    AgentResearchRunRecord,
)
from nexus.strategy.intents import ResearchIntent

RESEARCH_OUTPUT_NOT_ACTIONABLE = "RESEARCH_OUTPUT_NOT_ACTIONABLE"
_TERMINAL = frozenset({"completed", "failed", "abstained"})
_SIZING_ACTIONS: dict[str, Literal["increase", "reduce", "maintain", "none"]] = {
    "buy_candidate": "increase",
    "reduce_candidate": "reduce",
    "hold": "maintain",
    "watch": "none",
}
_EXPOSURE_ACTIONS = {
    "buy_candidate": "eligible_for_increase",
    "reduce_candidate": "reduce",
    "hold": "maintain",
    "watch": "none",
}


class ActionableResearchError(ValueError):
    """A deterministic rejection that must not trigger another model call."""


class ActionableResearchDecision(BaseModel):
    """Direction-only research authority; sizing remains deterministic elsewhere."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    instrument_id: UUID
    output_id: UUID
    intent_hash: Hash64
    stance: Literal["buy_candidate", "reduce_candidate", "hold", "watch"]
    sizing_action: Literal["increase", "reduce", "maintain", "none"]
    evidence_ref_ids: tuple[UUID, ...]


def actionable_intent_shape_valid(intent: ResearchIntent) -> bool:
    """Validate v4's complete review shape without granting weight or quantity."""
    assessment = intent.synthesis_assessment
    votes = {vote.role: vote.vote for vote in intent.role_votes}
    if (
        assessment is None
        or assessment.review_conclusion != "sufficient"
        or intent.quality_status != "complete"
        or intent.evidence_audit != "complete"
        or len(votes) != 8
        or votes.get("evidence_audit") != "support"
        or assessment.stance != intent.stance
        or intent.exposure_action != _EXPOSURE_ACTIONS[intent.stance]
        or not intent.theses
        or not intent.counter_theses
    ):
        return False
    reviews_by_role: dict[str, set[int]] = {"bear": set(), "evidence_audit": set()}
    for review in assessment.reviews:
        if not review.resolved:
            return False
        role, raw_index = review.claim_id.split(":", 1)
        reviews_by_role[role].add(int(raw_index))
    return all(
        bool(indexes) and indexes == set(range(max(indexes) + 1))
        for indexes in reviews_by_role.values()
    )


def safe_no_trade_intent_shape_valid(intent: ResearchIntent) -> bool:
    """Admit an incomplete conclusion only when it cannot request exposure changes."""
    assessment = intent.synthesis_assessment
    votes = {vote.role: vote.vote for vote in intent.role_votes}
    if (
        assessment is None
        or intent.stance != "watch"
        or assessment.stance != "watch"
        or intent.exposure_action != "none"
        or intent.quality_status != "partial"
        or intent.evidence_audit == "failed"
        or assessment.review_conclusion == "sufficient"
        or len(votes) != 8
        or not intent.theses
        or not intent.counter_theses
    ):
        return False
    reviews_by_role: dict[str, set[int]] = {"bear": set(), "evidence_audit": set()}
    for review in assessment.reviews:
        role, raw_index = review.claim_id.split(":", 1)
        reviews_by_role[role].add(int(raw_index))
    return all(
        bool(indexes) and indexes == set(range(max(indexes) + 1))
        for indexes in reviews_by_role.values()
    )


def admit_actionable_research_output(
    *,
    run: AgentResearchRunRecord,
    output: AgentResearchOutputRecord,
    intent: ResearchIntent,
    terminal_outputs: Sequence[AgentResearchOutputRecord],
    bound_evidence: Sequence[AgentResearchOutputEvidenceRecord],
    decision_time: datetime,
    price_authority_status: FrozenPriceAuthorityStatus,
    allow_safe_no_trade: bool = False,
    paper_decision_context_valid: bool = False,
    candidate_identity_verified: bool = False,
    formal_identity_verified: bool = False,
) -> ActionableResearchDecision:
    """Recompute immutable output/evidence facts and return only an admitted direction."""
    try:
        from nexus.agents.codex_versions import (
            ACTIONABLE_CONFIG_VERSIONS,
            CANDIDATE_BUDGET_CONFIG_VERSION,
            FORMAL_STAGE_CAPACITY_CONFIG_VERSION,
        )

        if (
            type(candidate_identity_verified) is not bool
            or type(formal_identity_verified) is not bool
            or formal_identity_verified
            and candidate_identity_verified
            or formal_identity_verified
            and run.pricing_version != FORMAL_STAGE_CAPACITY_CONFIG_VERSION
        ):
            raise ActionableResearchError(RESEARCH_OUTPUT_NOT_ACTIONABLE)
        version_admitted = (
            run.pricing_version in ACTIONABLE_CONFIG_VERSIONS
            or (
                candidate_identity_verified
                and run.pricing_version == CANDIDATE_BUDGET_CONFIG_VERSION
            )
            or (
                formal_identity_verified
                and run.pricing_version == FORMAL_STAGE_CAPACITY_CONFIG_VERSION
            )
        )

        terminals = tuple(terminal_outputs)
        bindings = tuple(bound_evidence)
        intent_document = intent.model_dump(mode="json")
        content = {
            "status": output.status,
            "role": output.role,
            "payload": output.payload,
            "intent": output.intent,
        }
        evidence_by_identity = {_evidence_identity(ref): ref for ref in intent.evidence_refs}
        binding_by_identity = {_binding_identity(binding): binding for binding in bindings}
        actionable_shape = actionable_intent_shape_valid(intent)
        safe_no_trade_shape = allow_safe_no_trade and safe_no_trade_intent_shape_valid(intent)
        price_authority_admitted = price_authority_status is FrozenPriceAuthorityStatus.VALID or (
            price_authority_status is FrozenPriceAuthorityStatus.MISSING
            and (paper_decision_context_valid or safe_no_trade_shape)
        )
        if (
            decision_time.tzinfo != UTC
            or run.status != "completed"
            or output.status != "completed"
            or output.role is not None
            or output.run_id != run.id
            or len(terminals) != 1
            or terminals[0].id != output.id
            or terminals[0].status not in _TERMINAL
            or not version_admitted
            or output.intent != intent_document
            or output.content_hash != _hash(content)
            or intent.instrument_id != run.instrument_id
            or intent.data_snapshot_id != run.data_snapshot_id
            or intent.decision_time != run.decision_time
            or decision_time < run.decision_time
            or intent.valid_until < decision_time
            or output.payload.get("quality_status") != intent.quality_status
            or not (actionable_shape or safe_no_trade_shape)
            or not price_authority_admitted
            or not bindings
            or len(evidence_by_identity) != len(intent.evidence_refs)
            or len(binding_by_identity) != len(bindings)
            or len(bindings) != len({binding.evidence_ref_id for binding in bindings})
            or any(binding.output_id != output.id for binding in bindings)
            or set(evidence_by_identity) != set(binding_by_identity)
            or any(binding.available_at > decision_time for binding in bindings)
            or not _all_nested_evidence_bound(intent, set(evidence_by_identity))
        ):
            raise ActionableResearchError(RESEARCH_OUTPUT_NOT_ACTIONABLE)
        evidence_ref_ids = tuple(
            binding_by_identity[_evidence_identity(ref)].evidence_ref_id
            for ref in intent.evidence_refs
        )
        return ActionableResearchDecision(
            instrument_id=intent.instrument_id,
            output_id=output.id,
            intent_hash=_hash(intent_document),
            stance=intent.stance,
            sizing_action=_SIZING_ACTIONS[intent.stance],
            evidence_ref_ids=evidence_ref_ids,
        )
    except ActionableResearchError:
        raise
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        raise ActionableResearchError(RESEARCH_OUTPUT_NOT_ACTIONABLE) from error


def _all_nested_evidence_bound(
    intent: ResearchIntent, authority: set[tuple[str, str, datetime, str, str]]
) -> bool:
    assessment = intent.synthesis_assessment
    collections = (
        intent.theses,
        intent.counter_theses,
        intent.invalidation_conditions,
        intent.entry_conditions,
        intent.exit_conditions,
        () if assessment is None else assessment.claims,
        () if assessment is None else assessment.reviews,
        tuple(item.condition for item in intent.condition_evaluations or ()),
    )
    return all(
        _evidence_identity(ref) in authority
        for collection in collections
        for item in collection
        for ref in item.evidence_refs
    )


def _evidence_identity(ref: EvidenceRef) -> tuple[str, str, datetime, str, str]:
    return (ref.source, ref.object_id, ref.available_at, ref.location, ref.content_hash)


def _binding_identity(
    binding: AgentResearchOutputEvidenceRecord,
) -> tuple[str, str, datetime, str, str]:
    return (
        binding.source,
        binding.provider_object_id,
        binding.available_at,
        binding.location,
        binding.content_hash,
    )


def _hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()
    ).hexdigest()
