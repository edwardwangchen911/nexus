"""Operator designation is independently bound, never claimed by a submission."""

import json
from copy import deepcopy
from hashlib import sha256
from uuid import UUID

import pytest
from nexus.research.accounting_review import build_material, canonical_bytes, validate_submission

from engine.tests.unit.research.test_accounting_review_intake import NOW, manifest, submission


def designation(package, opinion):
    return {
        "source_receipts": [
            json.dumps(
                {
                    "source_url": src.source_url,
                    "received_at": src.received_at.isoformat(),
                    "sha256": src.sha256,
                    "byte_length": src.byte_length,
                }
            )
            for src in package.sources
        ],
        "designation_id": str(UUID(int=20)),
        "material_sha256": sha256(canonical_bytes(package)).hexdigest(),
        "submission_sha256": sha256(canonical_bytes(opinion)).hexdigest(),
        "reviewer_name": opinion.reviewer_name,
        "owner_message_source": "local-owner-message:synthetic-20",
        "owner_message_text": "Use the named AI reviewer for these exact EPS facts.",
        "report_source": "review-conversation:synthetic-reply-21",
        "report_text": (
            "This exact amendment does not replace the disclosed EPS; no broader assurance."
        ),
        "purpose": "owner_designated_ai_as_disclosed_eps",
    }


def test_designation_binds_independent_owner_source_report_and_exact_opinion(tmp_path):
    from nexus.research.accounting_adoption import validate_designation

    package = build_material(manifest(tmp_path), source_root=tmp_path, now=NOW)
    opinion = validate_submission(
        submission(package), package=package, source_root=tmp_path, now=NOW
    )
    data = designation(package, opinion)
    result = validate_designation(data, material=package, opinion=opinion)
    assert result.reviewer_name == "Synthetic reviewer"
    for field, wrong in (
        ("material_sha256", "a" * 64),
        ("submission_sha256", "b" * 64),
        ("reviewer_name", "Other person"),
        ("owner_message_source", ""),
        ("report_text", ""),
        ("purpose", "all_accounting_verified"),
    ):
        invalid = deepcopy(data)
        invalid[field] = wrong
        with pytest.raises(ValueError):
            validate_designation(invalid, material=package, opinion=opinion)


def test_fact_coverage_requires_every_amendment_and_keeps_conflicts(tmp_path):
    from nexus.research.accounting_adoption import fact_review_fields

    from engine.tests.unit.research.test_accounting_review_intake import AMENDMENT, FACT

    package = build_material(manifest(tmp_path), source_root=tmp_path, now=NOW)
    opinion = submission(package)
    reviews = {UUID(int=100): {"material": package.model_dump(mode="json"), "submission": opinion}}
    result = fact_review_fields(UUID(FACT), (UUID(AMENDMENT),), reviews)
    assert result["amendment_review_status"] == "as_disclosed_not_replaced"
    assert result["accounting_review_evidence_ref_ids"] == [str(UUID(int=100))]
    assert (
        fact_review_fields(UUID(FACT), (UUID(AMENDMENT), UUID(int=999)), reviews)[
            "amendment_review_status"
        ]
        == "unverified"
    )
    assert (
        fact_review_fields(UUID(int=999), (UUID(AMENDMENT),), reviews)["amendment_review_status"]
        == "unverified"
    )
    bad = deepcopy(reviews[UUID(int=100)])
    bad["submission"]["findings"][0]["conclusion"] = "requires_reconciliation"
    reviews[UUID(int=101)] = bad
    assert (
        fact_review_fields(UUID(FACT), (UUID(AMENDMENT),), reviews)["amendment_review_status"]
        == "conflict"
    )


def test_designation_requires_independent_acquisition_receipts(tmp_path):
    import json

    from nexus.research.accounting_adoption import validate_designation

    package = build_material(manifest(tmp_path), source_root=tmp_path, now=NOW)
    opinion = validate_submission(
        submission(package), package=package, source_root=tmp_path, now=NOW
    )
    data = designation(package, opinion)
    data["source_receipts"] = []
    with pytest.raises(ValueError):
        validate_designation(data, material=package, opinion=opinion)
    data = designation(package, opinion)
    bad = json.loads(data["source_receipts"][0])
    bad["sha256"] = "f" * 64
    data["source_receipts"][0] = json.dumps(bad)
    with pytest.raises(ValueError):
        validate_designation(data, material=package, opinion=opinion)


def test_full_submission_original_must_match_archived_primary_and_issuer():
    from nexus.research.accounting_source_proof import verify_amendment_original

    primary = b"<html>Original primary disclosure.</html>"
    raw = (
        b"<SEC-HEADER>\nACCESSION NUMBER: 0000320193-26-000002\n"
        b"CENTRAL INDEX KEY: 0000320193\n</SEC-HEADER>\n"
        b"<DOCUMENT>\n<TYPE>8-K/A\n<FILENAME>doc.htm\n<TEXT>\n"
        + primary
        + b"\n</TEXT>\n</DOCUMENT>"
    )
    kwargs = dict(
        accession="0000320193-26-000002",
        issuer_cik="0000320193",
        primary_url="https://www.sec.gov/Archives/edgar/data/320193/000032019326000002/doc.htm",
        primary_sha256=sha256(primary).hexdigest(),
    )
    verify_amendment_original(raw, **kwargs)
    for changed in (
        raw.replace(b"Original", b"Changed"),
        raw.replace(b"CENTRAL INDEX KEY: 0000320193", b"CENTRAL INDEX KEY: 0000000001"),
        raw.replace(b"<TYPE>8-K/A", b"<TYPE>8-K"),
        raw.replace(
            b"ACCESSION NUMBER: 0000320193-26-000002", b"ACCESSION NUMBER: 0000320193-26-000003"
        ),
    ):
        with pytest.raises(ValueError, match="ORIGINAL"):
            verify_amendment_original(changed, **kwargs)


def test_submission_primary_reconciles_only_empty_delivery_script_not_filing_text():
    from nexus.research.accounting_source_proof import verify_amendment_original

    original = b"<html><body>Original filing text.</body></html>"
    archived = original.replace(
        b"</body>", b'<script type="text/javascript" src="/delivery/check"></script></body>'
    )
    raw = (
        b"<SEC-HEADER>\nACCESSION NUMBER: 0000320193-26-000002\n"
        b"CENTRAL INDEX KEY: 0000320193\n</SEC-HEADER>"
        b"<DOCUMENT><TYPE>8-K/A\n<FILENAME>doc.htm\n<TEXT>\n<XBRL>\n"
        + original
        + b"\n</XBRL>\n</TEXT></DOCUMENT>"
    )
    kwargs = dict(
        accession="0000320193-26-000002",
        issuer_cik="0000320193",
        primary_url="https://www.sec.gov/Archives/edgar/data/320193/000032019326000002/doc.htm",
        primary_sha256=sha256(archived).hexdigest(),
        primary_raw=archived,
    )
    verify_amendment_original(raw, **kwargs)
    for changed in (
        raw.replace(b"Original filing", b"Changed filing"),
        raw.replace(b"<body>", b"<body><script>alter();</script>"),
    ):
        with pytest.raises(ValueError):
            verify_amendment_original(changed, **kwargs)


@pytest.mark.parametrize(
    "extra",
    [
        {"http_status": 500},
        {"http_status": None},
        {"status": "FAILED"},
        {"stage": "request_failed"},
    ],
)
def test_matching_receipt_identity_does_not_override_failed_or_unsupported_acquisition(
    tmp_path, extra
):
    from nexus.research.accounting_adoption import validate_designation

    package = build_material(manifest(tmp_path), source_root=tmp_path, now=NOW)
    opinion = validate_submission(
        submission(package), package=package, source_root=tmp_path, now=NOW
    )
    data = designation(package, opinion)
    receipt = json.loads(data["source_receipts"][0])
    receipt.update(extra)
    data["source_receipts"][0] = json.dumps(receipt)
    with pytest.raises(ValueError, match="ACCOUNTING_SOURCE_RECEIPT_INVALID"):
        validate_designation(data, material=package, opinion=opinion)


@pytest.mark.parametrize(
    "receipt_format", ["legacy_primary", "http_submission", "archived_submission"]
)
def test_existing_acquisition_receipt_formats_remain_supported(tmp_path, receipt_format):
    from nexus.research.accounting_adoption import validate_designation

    package = build_material(manifest(tmp_path), source_root=tmp_path, now=NOW)
    opinion = validate_submission(
        submission(package), package=package, source_root=tmp_path, now=NOW
    )
    data = designation(package, opinion)
    receipt = json.loads(data["source_receipts"][0])
    if receipt_format == "http_submission":
        receipt["http_status"] = 200
    elif receipt_format == "archived_submission":
        del receipt["source_url"]
        receipt.update(
            status="ARCHIVED",
            stage="submission_get",
            http_status=200,
            requests=1,
            documents=[{"type": "8-K/A", "sequence": "1", "filename": "doc.htm"}],
        )
    data["source_receipts"][0] = json.dumps(receipt)
    assert validate_designation(data, material=package, opinion=opinion).designation_id == UUID(
        int=20
    )
