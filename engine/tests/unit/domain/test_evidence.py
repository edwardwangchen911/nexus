from datetime import UTC, datetime

import pytest
from nexus.domain.evidence import EvidenceRef
from pydantic import ValidationError


def test_evidence_ref_serializes_available_time_as_utc() -> None:
    """Evidence consumers receive a canonical availability instant for point-in-time checks."""
    evidence = EvidenceRef(
        source="sec",
        object_id="filing-42",
        available_at=datetime(2026, 8, 22, 13, 30, tzinfo=UTC),
        location="s3://nexus/data/filing-42",
        content_hash="a" * 64,
    )

    assert evidence.model_dump(mode="json") == {
        "source": "sec",
        "object_id": "filing-42",
        "available_at": "2026-08-22T13:30:00Z",
        "location": "s3://nexus/data/filing-42",
        "content_hash": "a" * 64,
    }


def test_evidence_ref_rejects_naive_available_time() -> None:
    """Evidence without an availability timezone cannot safely participate in a decision."""
    with pytest.raises(ValidationError, match="timezone-aware"):
        EvidenceRef(
            source="sec",
            object_id="filing-42",
            available_at=datetime(2026, 8, 22, 13, 30),
            location="s3://nexus/data/filing-42",
            content_hash="a" * 64,
        )


@pytest.mark.parametrize("content_hash", ["a" * 63, "g" * 64, "A" * 64, "sha256:" + "a" * 64])
def test_evidence_ref_rejects_noncanonical_content_hash(content_hash: str) -> None:
    """Evidence hashes must be lowercase, bare SHA-256 digests for reliable matching."""
    with pytest.raises(ValidationError):
        EvidenceRef(
            source="sec",
            object_id="filing-42",
            available_at=datetime(2026, 8, 22, 13, 30, tzinfo=UTC),
            location="s3://nexus/data/filing-42",
            content_hash=content_hash,
        )
