"""Versioned per-event judgments; material availability alone grants no consumption."""

import hashlib
import json
from collections.abc import Mapping
from typing import Literal, Self
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from nexus.domain.evidence import EvidenceRef
from nexus.monitoring.contracts import MonitoringModel


def canonical_hash(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


class MixedEventItem(MonitoringModel):
    trigger_id: UUID
    kind: Literal["price", "disclosure"]
    event_key: str
    status: Literal["addressed", "unresolved"]
    rationale: str = Field(min_length=1, max_length=160)
    content_snapshot_id: UUID | None
    evidence_refs: tuple[EvidenceRef, ...] = Field(min_length=1, max_length=3)

    @field_validator("rationale")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("MIXED_EVENT_RATIONALE_REQUIRED")
        return value


class MixedEventProposal(MonitoringModel):
    context_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    items: tuple[MixedEventItem, ...] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def unique(self) -> Self:
        if len({row.trigger_id for row in self.items}) != len(self.items):
            raise ValueError("MIXED_EVENT_DUPLICATE_TRIGGER")
        return self

    @property
    def content_hash(self) -> str:
        return canonical_hash(self.model_dump(mode="json"))


class MixedEventReview(MonitoringModel):
    proposal_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    decision: Literal["accepted", "rejected"]
    rationale: str = Field(min_length=1, max_length=160)
    evidence_refs: tuple[EvidenceRef, ...] = Field(min_length=1, max_length=3)

    @field_validator("rationale")
    @classmethod
    def nonblank(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("MIXED_EVENT_RATIONALE_REQUIRED")
        return value


def mixed_context_from_inputs(inputs: dict[str, object]) -> dict[str, object] | None:
    security = inputs.get("security_snapshot")
    if not isinstance(security, dict) or security.get("purpose") != "monitor_increment_v2":
        return None
    contexts = [
        row["monitor_context"]
        for row in security.get("items", [])
        if isinstance(row, dict)
        and row.get("source") == "nexus_monitor_increment_v2"
        and isinstance(row.get("monitor_context"), dict)
    ]
    if len(contexts) != 1:
        raise ValueError("MIXED_EVENT_CONTEXT_INVALID")
    return dict(contexts[0])


def validate_mixed_proposal(
    proposal: MixedEventProposal, *, context: object, evidence_refs: tuple[EvidenceRef, ...]
) -> None:
    invalid = "MIXED_EVENT_PROPOSAL_INVALID"
    refs = tuple(ref for ref in evidence_refs if ref.source == "nexus_monitor_increment_v2")
    if (
        len(refs) != 1
        or not isinstance(context, dict)
        or context.get("purpose") != "monitor_increment_v2"
        or proposal.context_hash != refs[0].content_hash
    ):
        raise ValueError(invalid)
    try:
        expected = {str(row["trigger_id"]): row for row in context["trigger_checks"]}
        contents = {
            (
                f"sec:{row['archive']['issuer_cik']}:{row['archive']['entry']['accession_number']}"
            ): UUID(row["snapshot_id"])
            for row in context["materials"]["contents"]
        }
        if len(expected) != len(proposal.items) or set(expected) != {
            str(row.trigger_id) for row in proposal.items
        }:
            raise ValueError(invalid)
        for row in proposal.items:
            original = expected[str(row.trigger_id)]
            if (
                row.kind != original["kind"]
                or row.event_key != original["event_key"]
                or row.content_snapshot_id
                != (contents[row.event_key] if row.kind == "disclosure" else None)
                or refs[0] not in row.evidence_refs
                or any(ref not in evidence_refs for ref in row.evidence_refs)
            ):
                raise ValueError(invalid)
    except (KeyError, TypeError, ValueError):
        raise ValueError(invalid) from None


def compile_mixed_proposal(
    proposal: MixedEventProposal,
    reviews: Mapping[str, MixedEventReview],
    *,
    context: object,
    evidence_refs: tuple[EvidenceRef, ...],
    audit_vote: str,
    adoption_hash: str | None,
) -> MixedEventProposal:
    validate_mixed_proposal(proposal, context=context, evidence_refs=evidence_refs)
    ref = next(ref for ref in evidence_refs if ref.source == "nexus_monitor_increment_v2")
    if (
        set(reviews) != {"bear", "evidence_audit"}
        or audit_vote != "support"
        or adoption_hash != proposal.content_hash
        or any(
            row.decision != "accepted"
            or row.proposal_hash != proposal.content_hash
            or ref not in row.evidence_refs
            or any(item not in evidence_refs for item in row.evidence_refs)
            for row in reviews.values()
        )
    ):
        raise ValueError("MIXED_EVENT_ADOPTION_INVALID")
    return proposal
