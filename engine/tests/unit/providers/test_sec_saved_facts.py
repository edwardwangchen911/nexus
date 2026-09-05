"""Project only facts actually present in an authenticated stored SEC accession."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from nexus.domain.evidence import EvidenceRef
from nexus.providers import sec

ACCESSION = "0000000001-26-000001"
ACCEPTED = datetime(2026, 8, 20, 20, tzinfo=UTC)
INSTRUMENT = UUID("00000000-0000-4000-8000-000000000001")
TAG = "RevenueFromContractWithCustomerExcludingAssessedTax"


def document():
    return {
        "accession": ACCESSION,
        "accepted_at": ACCEPTED.isoformat(),
        "form": "10-Q",
        "facts": [
            {
                "taxonomy": "us-gaap",
                "tag": TAG,
                "unit": "USD",
                "fact": {
                    "accn": ACCESSION,
                    "form": "10-Q",
                    "filed": "2026-08-21",
                    "start": "2026-04-01",
                    "end": "2026-06-30",
                    "val": "123.456789012345",
                    "fy": 2026,
                    "fp": "Q2",
                },
            }
        ],
    }


def normalize(body, *, corrupt_hash=False):
    raw = json.dumps(body).encode()
    digest = "0" * 64 if corrupt_hash else sha256(raw).hexdigest()
    return sec.normalize_saved_accession_facts(
        raw,
        instrument_id=INSTRUMENT,
        accession=ACCESSION,
        accepted_at=ACCEPTED,
        form="10-Q",
        evidence_ref=EvidenceRef(
            source="sec",
            object_id=ACCESSION,
            available_at=ACCEPTED,
            location=f"memory://sha256/{digest}",
            content_hash=digest,
        ),
    )


def test_saved_accession_preserves_exact_revenue_and_later_filed_date():
    body = document()
    before = json.dumps(body)
    result = normalize(body)
    assert len(result) == 1
    fact = result[0]
    assert fact.instrument_id == INSTRUMENT and fact.canonical_fact == "revenue"
    assert fact.raw_taxonomy == "us-gaap" and fact.raw_tag == TAG
    assert fact.value == Decimal("123.456789012345")
    assert fact.period_kind == "duration" and fact.unit == "USD"
    assert fact.accepted_at == ACCEPTED
    assert fact.filed_at == datetime(2026, 8, 21, tzinfo=UTC)
    assert fact.revision == f"{ACCESSION}@{ACCEPTED.isoformat()}"
    assert json.dumps(body) == before


@pytest.mark.parametrize(
    "field,value",
    [
        ("accession", "0000000002-26-000001"),
        ("accepted_at", "2026-08-20T19:00:00+00:00"),
        ("accepted_at", "2026-08-20T20:00:00"),
        ("form", "10-K"),
        ("facts", []),
    ],
)
def test_saved_accession_rejects_mismatched_or_incomplete_document(field, value):
    body = document()
    body[field] = value
    with pytest.raises(sec.SecNormalizationError):
        normalize(body)


def test_saved_accession_rejects_hash_damage_before_projection():
    with pytest.raises(sec.SecNormalizationError, match="SEC_EVIDENCE_CONFLICT"):
        normalize(document(), corrupt_hash=True)


@pytest.mark.parametrize(
    "field,value",
    [
        ("accn", "0000000002-26-000001"),
        ("form", "10-K"),
    ],
)
def test_saved_accession_rejects_fact_from_another_filing(field, value):
    body = document()
    body["facts"][0]["fact"][field] = value
    with pytest.raises(sec.SecNormalizationError):
        normalize(body)


def test_saved_accession_rejects_duplicate_identity_fields_in_json():
    # Malformed source objects must not acquire identity via last-key-wins parsing.
    raw = b'{"accession":"wrong","accession":"0000000001-26-000001"}'
    digest = sha256(raw).hexdigest()
    with pytest.raises(sec.SecNormalizationError):
        sec.normalize_saved_accession_facts(
            raw,
            instrument_id=INSTRUMENT,
            accession=ACCESSION,
            accepted_at=ACCEPTED,
            form="10-Q",
            evidence_ref=EvidenceRef(
                source="sec",
                object_id=ACCESSION,
                available_at=ACCEPTED,
                location=f"memory://sha256/{digest}",
                content_hash=digest,
            ),
        )
