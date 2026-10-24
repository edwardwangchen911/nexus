"""Only an exact, separately reviewed event set can authorize mixed consumption."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from nexus.domain.evidence import EvidenceRef


def reference():
    return EvidenceRef(
        source="nexus_monitor_increment_v2",
        object_id="context",
        available_at=datetime(2026, 10, 24, tzinfo=UTC),
        location="memory://sha256/" + "a" * 64,
        content_hash="a" * 64,
    )


def context():
    return {
        "purpose": "monitor_increment_v2",
        "trigger_checks": [
            {
                "trigger_id": str(UUID(int=2)),
                "kind": "disclosure",
                "event_key": "sec:0000123456:0000123456-26-000002",
            }
        ],
        "materials": {
            "contents": [
                {
                    "snapshot_id": str(UUID(int=3)),
                    "archive": {
                        "issuer_cik": "0000123456",
                        "entry": {"accession_number": "0000123456-26-000002"},
                    },
                }
            ]
        },
    }


def test_exact_event_adjudication_needs_both_reviewers_and_adoption():
    from nexus.monitoring.mixed_contracts import (
        MixedEventProposal,
        MixedEventReview,
        compile_mixed_proposal,
    )

    ref = reference()
    proposal = MixedEventProposal(
        context_hash=ref.content_hash,
        items=[
            dict(
                trigger_id=UUID(int=2),
                kind="disclosure",
                event_key="sec:0000123456:0000123456-26-000002",
                status="addressed",
                rationale="精确原件和附件的影响已核对。",
                content_snapshot_id=UUID(int=3),
                evidence_refs=[ref],
            )
        ],
    )
    review = MixedEventReview(
        proposal_hash=proposal.content_hash,
        decision="accepted",
        rationale="逐项核验原影响结论。",
        evidence_refs=[ref],
    )
    assert (
        compile_mixed_proposal(
            proposal,
            {"bear": review, "evidence_audit": review},
            context=context(),
            evidence_refs=(ref,),
            audit_vote="support",
            adoption_hash=proposal.content_hash,
        )
        == proposal
    )
    with pytest.raises(ValueError, match="MIXED_EVENT_ADOPTION_INVALID"):
        compile_mixed_proposal(
            proposal,
            {"bear": review},
            context=context(),
            evidence_refs=(ref,),
            audit_vote="support",
            adoption_hash=proposal.content_hash,
        )
    with pytest.raises(ValueError, match="MIXED_EVENT_ADOPTION_INVALID"):
        compile_mixed_proposal(
            proposal,
            {"bear": review, "evidence_audit": review},
            context=context(),
            evidence_refs=(ref,),
            audit_vote="abstain",
            adoption_hash=proposal.content_hash,
        )


@pytest.mark.parametrize("damage", ["member", "kind", "event", "body", "context", "reference"])
def test_event_proposal_cannot_replace_any_frozen_identity(damage):
    from nexus.monitoring.mixed_contracts import MixedEventProposal, validate_mixed_proposal

    ref = reference()
    item = dict(
        trigger_id=UUID(int=2),
        kind="disclosure",
        event_key="sec:0000123456:0000123456-26-000002",
        status="addressed",
        rationale="核对原件。",
        content_snapshot_id=UUID(int=3),
        evidence_refs=[ref],
    )
    if damage == "member":
        item["trigger_id"] = UUID(int=4)
    elif damage == "kind":
        item["kind"] = "price"
    elif damage == "event":
        item["event_key"] = "sec:0000123456:0000123456-26-000003"
    elif damage == "body":
        item["content_snapshot_id"] = UUID(int=4)
    elif damage == "reference":
        item["evidence_refs"] = [ref.model_copy(update={"source": "unbound"})]
    proposal = MixedEventProposal(
        context_hash="b" * 64 if damage == "context" else ref.content_hash, items=[item]
    )
    with pytest.raises(ValueError, match="MIXED_EVENT_PROPOSAL_INVALID"):
        validate_mixed_proposal(proposal, context=context(), evidence_refs=(ref,))
