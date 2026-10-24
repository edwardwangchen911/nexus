"""Original SEC discovery proposals and two independent, exact-hash approvals."""

import hashlib
import json
from collections.abc import Mapping
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import Field, field_validator

from nexus.domain.evidence import EvidenceRef
from nexus.domain.time import require_utc
from nexus.monitoring.contracts import DisclosureReviewCondition, MonitoringModel


class DisclosureMonitoringProposal(MonitoringModel):
    condition: DisclosureReviewCondition
    baseline_snapshot_id: UUID
    baseline_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    issuer_cik: str = Field(pattern=r"^[0-9]{10}$")
    baseline_as_of: datetime
    rationale: str = Field(min_length=1, max_length=160)
    evidence_refs: tuple[EvidenceRef, ...] = Field(min_length=1, max_length=3)
    review_due_at: datetime
    valid_until: datetime

    @field_validator("baseline_as_of", "review_due_at", "valid_until")
    @classmethod
    def utc(cls, value: datetime) -> datetime:
        return require_utc(value)

    @property
    def content_hash(self) -> str:
        return hashlib.sha256(
            json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()


class DisclosureMonitoringReview(MonitoringModel):
    proposal_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    decision: Literal["accepted", "rejected"]
    rationale: str = Field(min_length=1, max_length=160)
    evidence_refs: tuple[EvidenceRef, ...] = Field(min_length=1, max_length=3)


def disclosure_from_inputs(inputs: dict[str, object]) -> object | None:
    security = inputs.get("security_snapshot")
    if not isinstance(security, dict):
        return None
    contexts = [
        paper["disclosure_monitoring"]
        for item in security.get("items", [])
        if isinstance(item, dict)
        and isinstance(paper := item.get("agent_paper_context"), dict)
        and paper.get("disclosure_monitoring") is not None
    ]
    if len(contexts) > 1:
        raise ValueError("DISCLOSURE_CONTEXT_INVALID")
    return contexts[0] if contexts else None


def baseline_fields(
    context: object, *, instrument_id: UUID | None = None, research_as_of: datetime | None = None
) -> dict[str, object]:
    from nexus.monitoring.disclosure_context import DisclosureMonitoringContext

    frozen = DisclosureMonitoringContext.model_validate_json(json.dumps(context))
    if (
        not frozen.ready
        or frozen.baseline is None
        or instrument_id is not None
        and frozen.instrument_id != instrument_id
        or research_as_of is not None
        and frozen.research_as_of != research_as_of
    ):
        raise ValueError("DISCLOSURE_BASELINE_REQUIRED")
    baseline = frozen.baseline
    return {
        "baseline_snapshot_id": baseline.snapshot_id,
        "baseline_hash": baseline.snapshot_hash,
        "issuer_cik": baseline.issuer_cik,
        "baseline_as_of": baseline.research_as_of,
    }


def compile_disclosure_proposal(
    proposal: DisclosureMonitoringProposal,
    reviews: Mapping[str, DisclosureMonitoringReview],
    *,
    context: object,
    instrument_id: UUID,
    now: datetime,
    evidence_refs: tuple[EvidenceRef, ...],
    audit_vote: str,
    adoption_hash: str | None,
) -> DisclosureMonitoringProposal:
    expected = baseline_fields(context, instrument_id=instrument_id, research_as_of=now)
    contexts = tuple(ref for ref in evidence_refs if ref.source == "nexus_agent_paper_context")
    now = require_utc(now)
    if (
        set(reviews) != {"bear", "evidence_audit"}
        or audit_vote != "support"
        or adoption_hash != proposal.content_hash
        or len(contexts) != 1
        or any(getattr(proposal, key) != value for key, value in expected.items())
        or proposal.baseline_as_of != now
        or not now < proposal.review_due_at <= proposal.valid_until
        or not proposal.rationale.strip()
        or contexts[0] not in proposal.evidence_refs
        or any(ref not in evidence_refs or ref.available_at > now for ref in proposal.evidence_refs)
        or any(
            review.decision != "accepted"
            or review.proposal_hash != proposal.content_hash
            or not review.rationale.strip()
            or contexts[0] not in review.evidence_refs
            or any(
                ref not in evidence_refs or ref.available_at > now for ref in review.evidence_refs
            )
            for review in reviews.values()
        )
    ):
        raise ValueError("DISCLOSURE_PROPOSAL_NOT_APPROVED")
    return proposal
