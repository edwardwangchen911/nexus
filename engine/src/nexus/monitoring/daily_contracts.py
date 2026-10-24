"""Exact, independently reviewed adjudication of frozen daily pending triggers."""

import hashlib
import json
from collections.abc import Mapping
from typing import Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from nexus.domain.evidence import EvidenceRef
from nexus.monitoring.contracts import MonitoringModel


def daily_context_identity(value: object) -> tuple[str, tuple[UUID, ...]]:
    """Read the identity of a context already qualified by the normal snapshot reader."""
    if not isinstance(value, dict) or value.get("purpose") != "daily_monitor_context_v1":
        raise ValueError("DAILY_PENDING_CONTEXT_INVALID")
    checks = value.get("trigger_checks")
    if not isinstance(checks, list) or not 1 <= len(checks) <= 32:
        raise ValueError("DAILY_PENDING_CONTEXT_INVALID")
    try:
        ids = tuple(UUID(str(item["trigger_id"])) for item in checks)
    except (ValueError, KeyError, TypeError):
        raise ValueError("DAILY_PENDING_CONTEXT_INVALID") from None
    if len(set(ids)) != len(ids):
        raise ValueError("DAILY_PENDING_CONTEXT_INVALID")
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest(), ids


def daily_context_from_inputs(inputs: dict[str, object]) -> object | None:
    return _daily_field_from_inputs(inputs, "pending_monitoring")


def daily_gap_from_inputs(inputs: dict[str, object]) -> object | None:
    return _daily_field_from_inputs(inputs, "pending_monitoring_gap")


def _daily_field_from_inputs(inputs: dict[str, object], field: str) -> object | None:
    security = inputs.get("security_snapshot")
    if not isinstance(security, dict):
        return None
    contexts = [
        paper[field]
        for item in security.get("items", [])
        if isinstance(item, dict)
        and isinstance(paper := item.get("agent_paper_context"), dict)
        and paper.get(field) is not None
    ]
    if len(contexts) > 1:
        raise ValueError("DAILY_PENDING_CONTEXT_INVALID")
    return contexts[0] if contexts else None


class DailyPendingItem(MonitoringModel):
    trigger_id: UUID
    status: Literal["addressed", "unresolved"]
    rationale: str = Field(min_length=1, max_length=160)
    evidence_refs: tuple[EvidenceRef, ...] = Field(min_length=1, max_length=3)

    @field_validator("rationale")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("DAILY_PENDING_RATIONALE_REQUIRED")
        return value


class DailyPendingProposal(MonitoringModel):
    context_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    items: tuple[DailyPendingItem, ...] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def unique(self) -> "DailyPendingProposal":
        if len({item.trigger_id for item in self.items}) != len(self.items):
            raise ValueError("DAILY_PENDING_DUPLICATE_TRIGGER")
        return self

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(
            json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


class DailyPendingReview(MonitoringModel):
    proposal_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    decision: Literal["accepted", "rejected"]
    rationale: str = Field(min_length=1, max_length=500)
    evidence_refs: tuple[EvidenceRef, ...] = Field(min_length=1, max_length=4)

    @field_validator("rationale")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("DAILY_PENDING_RATIONALE_REQUIRED")
        return value


def validate_daily_pending_proposal(
    proposal: DailyPendingProposal,
    *,
    context_hash: str,
    trigger_ids: tuple[UUID, ...],
    evidence_refs: tuple[EvidenceRef, ...],
) -> None:
    contexts = tuple(ref for ref in evidence_refs if ref.source == "nexus_agent_paper_context")
    if (
        len(contexts) != 1
        or proposal.context_hash != context_hash
        or not 1 <= len(trigger_ids) <= 32
        or len(set(trigger_ids)) != len(trigger_ids)
        or {item.trigger_id for item in proposal.items} != set(trigger_ids)
        or any(
            contexts[0] not in item.evidence_refs
            or any(ref not in evidence_refs for ref in item.evidence_refs)
            for item in proposal.items
        )
    ):
        raise ValueError("DAILY_PENDING_PROPOSAL_INVALID")


def compile_daily_pending_proposal(
    proposal: DailyPendingProposal,
    reviews: Mapping[str, DailyPendingReview],
    *,
    context_hash: str,
    trigger_ids: tuple[UUID, ...],
    evidence_refs: tuple[EvidenceRef, ...],
    audit_vote: str,
    adoption_hash: str | None,
) -> DailyPendingProposal:
    validate_daily_pending_proposal(
        proposal, context_hash=context_hash, trigger_ids=trigger_ids, evidence_refs=evidence_refs
    )
    context = next(ref for ref in evidence_refs if ref.source == "nexus_agent_paper_context")
    if (
        set(reviews) != {"bear", "evidence_audit"}
        or audit_vote != "support"
        or adoption_hash != proposal.content_hash
        or any(
            review.proposal_hash != proposal.content_hash or review.decision != "accepted"
            for review in reviews.values()
        )
        or any(
            context not in review.evidence_refs
            or any(ref not in evidence_refs for ref in review.evidence_refs)
            for review in reviews.values()
        )
    ):
        raise ValueError("DAILY_PENDING_ADOPTION_INVALID")
    return proposal
