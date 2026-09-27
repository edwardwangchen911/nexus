"""Amendments are independent, incomplete evidence, never replacement facts."""

import json
from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID

import pytest
from nexus.research.sec_disclosures import SecDisclosure


def payload(**changes):
    return {
        "instrument_id": UUID("00000000-0000-0000-0000-000000000001"),
        "form": "8-K/A",
        "issuer_cik": "0000320193",
        "accession_number": "0000320193-26-000002",
        "source_url": "https://www.sec.gov/Archives/edgar/data/320193/000032019326000002/doc.htm",
        "filed_at": datetime(2026, 9, 1, tzinfo=UTC),
        "accepted_at": datetime(2026, 9, 1, 20, tzinfo=UTC),
        "raw_content_hash": "a" * 64,
        "excerpt": "This amendment adds limited disclosure; the original is not identified.",
        "truncated": False,
    } | changes


def test_independent_amendment_has_explicit_unknown_relation_and_partial_coverage():
    from nexus.research.sec_amendments import SecAmendmentDisclosure

    document = SecAmendmentDisclosure.model_validate(payload())
    canonical = json.loads(document.canonical_bytes())
    assert canonical["form"] == "8-K/A"
    assert canonical["relation_status"] == "unknown"
    assert canonical["original_accession_number"] is None
    assert canonical["relation_evidence_ids"] == []
    assert canonical["content_coverage"] == "primary_document_only"
    assert canonical["attachments_status"] == "not_collected"
    assert canonical["accounting_revision_status"] == "unverified"
    assert canonical["truncated"] is False  # A full main text is still incomplete evidence.
    assert document.canonical_bytes() == document.canonical_bytes()


@pytest.mark.parametrize(
    "change",
    [
        {"issuer_cik": "0000000002"},
        {"issuer_cik": None},
        {"accession_number": "0000320193-26-000003"},
        {"source_url": "https://other.invalid/doc.htm"},
        {
            "source_url": "https://www.sec.gov/Archives/edgar/data/320193/000032019326000002/../doc.htm"
        },
        {"form": "8-K"},
        {"relation_status": "verified"},
        {"original_accession_number": "0000320193-26-000001"},
        {"relation_evidence_ids": ["00000000-0000-0000-0000-000000000002"]},
        {"content_coverage": "complete"},
        {"attachments_status": "complete"},
        {"accounting_revision_status": "verified"},
        {"accepted_at": datetime(2026, 9, 1)},
    ],
)
def test_unproven_identity_relation_completeness_and_accounting_claims_are_rejected(change):
    from nexus.research.sec_amendments import SecAmendmentDisclosure

    with pytest.raises(ValueError):
        SecAmendmentDisclosure.model_validate(payload(**change))


def test_legacy_schema_does_not_accept_amendment_or_gain_new_canonical_fields():
    with pytest.raises(ValueError):
        SecDisclosure.model_validate(payload())
    original = SecDisclosure.model_validate(payload(form="8-K"))
    content = original.canonical_bytes()
    assert "relation_status" not in json.loads(content)
    assert (
        sha256(content).hexdigest()
        == "bea31cc95f7ce4771476381a00572e1372e1b6d453a13981a85d1b4d89c55be4"
    )


def test_amendment_can_retain_truncation_without_claiming_full_context():
    from nexus.research.sec_amendments import SecAmendmentDisclosure

    document = SecAmendmentDisclosure.model_validate(payload(truncated=True))
    assert document.truncated is True
    assert document.content_coverage == "primary_document_only"
