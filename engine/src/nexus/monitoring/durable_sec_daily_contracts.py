"""Independent daily adjudication of foundation SEC event identities."""

from collections.abc import Mapping
from uuid import UUID

from pydantic import Field, model_validator

from nexus.domain.evidence import EvidenceRef
from nexus.monitoring.contracts import MonitoringModel
from nexus.monitoring.daily_contracts import DailyPendingReview
from nexus.monitoring.mixed_contracts import canonical_hash


class DurableSecPendingItem(MonitoringModel):
    event_id: UUID
    status: str = Field(pattern=r"^(addressed|unresolved)$")
    rationale: str = Field(min_length=1, max_length=160)
    evidence_refs: tuple[EvidenceRef, ...] = Field(min_length=1, max_length=3)


class DurableSecPendingProposal(MonitoringModel):
    context_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    items: tuple[DurableSecPendingItem, ...] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def unique(self) -> "DurableSecPendingProposal":
        if len({x.event_id for x in self.items}) != len(self.items):
            raise ValueError("DURABLE_SEC_PENDING_DUPLICATE")
        return self

    @property
    def content_hash(self) -> str:
        return canonical_hash(self.model_dump(mode="json"))


DurableSecPendingReview = DailyPendingReview


def pending_from_inputs(inputs: dict[str, object]) -> dict | None:
    security = inputs.get("security_snapshot")
    if not isinstance(security, dict):
        return None
    rows = [
        paper["pending_durable_sec"]
        for item in security.get("items", [])
        if isinstance(item, dict)
        and isinstance(paper := item.get("agent_paper_context"), dict)
        and paper.get("pending_durable_sec") is not None
    ]
    if len(rows) > 1:
        raise ValueError("DURABLE_SEC_PENDING_CONTEXT_INVALID")
    return rows[0] if rows else None


def pending_identity(context: object) -> tuple[str, tuple[UUID, ...]]:
    if not isinstance(context, dict) or context.get("purpose") != "durable_sec_pending_v1":
        raise ValueError("DURABLE_SEC_PENDING_CONTEXT_INVALID")
    try:
        ids = tuple(UUID(x["event_id"]) for x in context["events"])
    except (KeyError, TypeError, ValueError):
        raise ValueError("DURABLE_SEC_PENDING_CONTEXT_INVALID") from None
    if not 1 <= len(ids) <= 32 or len(set(ids)) != len(ids):
        raise ValueError("DURABLE_SEC_PENDING_CONTEXT_INVALID")
    return canonical_hash(context), ids


def validate_durable_sec_pending_proposal(
    proposal: DurableSecPendingProposal,
    *,
    context_hash: str,
    event_ids: tuple[UUID, ...],
    evidence_refs: tuple[EvidenceRef, ...],
) -> None:
    context = tuple(x for x in evidence_refs if x.source == "nexus_agent_paper_context")
    if (
        len(context) != 1
        or proposal.context_hash != context_hash
        or {x.event_id for x in proposal.items} != set(event_ids)
        or len(proposal.items) != len(event_ids)
        or any(
            not x.rationale.strip()
            or context[0] not in x.evidence_refs
            or any(ref not in evidence_refs for ref in x.evidence_refs)
            for x in proposal.items
        )
    ):
        raise ValueError("DURABLE_SEC_PENDING_PROPOSAL_INVALID")


def compile_durable_sec_pending_proposal(
    proposal: DurableSecPendingProposal,
    reviews: Mapping[str, DailyPendingReview],
    *,
    context_hash: str,
    event_ids: tuple[UUID, ...],
    evidence_refs: tuple[EvidenceRef, ...],
    audit_vote: str,
    adoption_hash: str | None,
) -> DurableSecPendingProposal:
    validate_durable_sec_pending_proposal(
        proposal, context_hash=context_hash, event_ids=event_ids, evidence_refs=evidence_refs
    )
    context = next(x for x in evidence_refs if x.source == "nexus_agent_paper_context")
    if (
        set(reviews) != {"bear", "evidence_audit"}
        or audit_vote != "support"
        or adoption_hash != proposal.content_hash
        or any(
            x.proposal_hash != proposal.content_hash
            or x.decision != "accepted"
            or context not in x.evidence_refs
            or any(ref not in evidence_refs for ref in x.evidence_refs)
            for x in reviews.values()
        )
    ):
        raise ValueError("DURABLE_SEC_PENDING_ADOPTION_INVALID")
    return proposal
