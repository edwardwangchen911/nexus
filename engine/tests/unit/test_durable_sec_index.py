"""A bad historical row cannot erase independently verified official metadata."""

import json
from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID

import pytest
from nexus.research.durable_sec_index import build_durable_index
from nexus.research.sec_disclosure_index import build_disclosure_index


def originals():
    document = {
        "cik": 123456,
        "filings": {
            "files": [{"name": "old.json", "filingFrom": "2030-01-01", "filingTo": "2020-01-01"}],
            "recent": {
                "accessionNumber": ["0000123456-26-000001", "0000123456-26-000002"],
                "form": ["8-K", "8-K"],
                "acceptanceDateTime": ["2030-01-01T15:00:00Z", "2026-01-01T15:00:00Z"],
                "filingDate": ["2026-01-01", "2026-01-01"],
                "primaryDocument": ["old.htm", "new.htm"],
            },
        },
    }
    raw = json.dumps(document).encode()
    receipt = json.dumps(
        {
            "source_url": "https://data.sec.gov/submissions/CIK0000123456.json",
            "received_at": "2026-01-01T15:01:00Z",
            "sha256": sha256(raw).hexdigest(),
            "byte_length": len(raw),
            "http_status": 200,
        }
    ).encode()
    return raw, receipt


def arguments():
    return dict(
        instrument_id=UUID(int=1), identity_snapshot_id=UUID(int=2), issuer_cik="0000123456"
    )


def test_coverage_diagnostics_do_not_change_source_or_grant_complete_history():
    from nexus.research import durable_sec_index

    raw, receipt = originals()
    evidence = build_durable_index(
        raw, receipt, **arguments(), now=datetime(2026, 1, 1, 15, 2, tzinfo=UTC)
    )
    before = evidence.model_dump_json()
    assert durable_sec_index.coverage_details(raw, evidence) == {
        "unread_history_files": 1,
        "invalid_rows": 1,
        "coverage_start_known": True,
    }
    assert evidence.model_dump_json() == before
    assert not evidence.index.history_complete
    with pytest.raises(ValueError, match="SEC_DURABLE_SOURCE_INVALID"):
        durable_sec_index.coverage_details(raw + b" ", evidence)


def test_local_bad_row_and_history_dates_preserve_independent_entry():
    raw, receipt = originals()
    with pytest.raises(ValueError):
        build_disclosure_index(raw, receipt, **arguments())
    result = build_durable_index(
        raw, receipt, **arguments(), now=datetime(2026, 1, 1, 15, 2, tzinfo=UTC)
    )
    assert [entry.accession_number for entry in result.index.entries] == ["0000123456-26-000002"]
    assert result.index.history_complete is False
    assert len(result.issues) == 1 and result.issues[0].accession_number == "0000123456-26-000001"
    assert result.raw_content_hash == sha256(raw).hexdigest()


@pytest.mark.parametrize("damage", ["hash", "issuer"])
def test_shared_identity_or_hash_damage_rejects_entire_source(damage):
    raw, receipt = originals()
    args = arguments()
    if damage == "hash":
        raw += b" "
    else:
        args["issuer_cik"] = "0000654321"
    with pytest.raises(ValueError):
        build_durable_index(raw, receipt, **args, now=datetime(2026, 1, 1, 15, 2, tzinfo=UTC))


@pytest.mark.parametrize("accepted", ["2026-01-01T15:00Z", "2026-01-01"])
def test_truncated_timestamp_cannot_classify_activation_boundary(accepted):
    raw, receipt = originals()
    document = json.loads(raw)
    document["filings"]["recent"]["acceptanceDateTime"][1] = accepted
    raw = json.dumps(document).encode()
    proof = json.loads(receipt)
    proof.update(sha256=sha256(raw).hexdigest(), byte_length=len(raw))
    result = build_durable_index(
        raw, json.dumps(proof).encode(), **arguments(), now=datetime(2026, 1, 1, 15, 2, tzinfo=UTC)
    )
    assert result.index.entries == ()
    assert len(result.issues) == 2
    assert not result.index.history_complete


def test_duplicate_bad_accession_does_not_suppress_independent_other_key():
    raw, receipt = originals()
    document = json.loads(raw)
    recent = document["filings"]["recent"]
    for key in ("accessionNumber", "form", "acceptanceDateTime", "filingDate", "primaryDocument"):
        recent[key].append(recent[key][0])
    raw = json.dumps(document).encode()
    proof = json.loads(receipt)
    proof.update(sha256=sha256(raw).hexdigest(), byte_length=len(raw))
    result = build_durable_index(
        raw, json.dumps(proof).encode(), **arguments(), now=datetime(2026, 1, 1, 15, 2, tzinfo=UTC)
    )
    assert [v.accession_number for v in result.index.entries] == ["0000123456-26-000002"]
    assert not result.index.history_complete


def test_generic_non_target_metadata_is_retained_for_conflict_readback():
    raw, receipt = originals()
    document = json.loads(raw)
    recent = document["filings"]["recent"]
    recent["acceptanceDateTime"][0] = "2026-01-01T14:00:00Z"
    recent["form"][0] = "10-K"
    raw = json.dumps(document).encode()
    proof = json.loads(receipt)
    proof.update(sha256=sha256(raw).hexdigest(), byte_length=len(raw))
    result = build_durable_index(
        raw, json.dumps(proof).encode(), **arguments(), now=datetime(2026, 1, 1, 15, 2, tzinfo=UTC)
    )
    assert len(result.metadata_entries) == 2
    assert result.metadata_entries[0].form == "10-K"
    assert len(result.index.entries) == 1
    assert not result.index.history_complete


def test_legitimate_non_target_relative_path_is_metadata_not_filing_permission():
    raw, receipt = originals()
    document = json.loads(raw)
    recent = document["filings"]["recent"]
    recent["acceptanceDateTime"][0] = "2026-01-01T14:00:00Z"
    recent["form"][0] = "4"
    recent["primaryDocument"][0] = "xslF345X05/form4.xml"
    raw = json.dumps(document).encode()
    proof = json.loads(receipt)
    proof.update(sha256=sha256(raw).hexdigest(), byte_length=len(raw))
    result = build_durable_index(
        raw, json.dumps(proof).encode(), **arguments(), now=datetime(2026, 1, 1, 15, 2, tzinfo=UTC)
    )
    assert result.metadata_entries[0].primary_document == "xslF345X05/form4.xml"
    assert len(result.index.entries) == 1 and result.index.entries[0].form == "8-K"
    assert result.issues == ()


@pytest.mark.parametrize(
    "primary",
    ["../old.htm", "xsl/../old.htm", "xsl//old.htm", "/old.htm", "https://www.sec.gov/old.htm"],
)
def test_unsafe_non_target_path_stays_local_unknown(primary):
    raw, receipt = originals()
    document = json.loads(raw)
    recent = document["filings"]["recent"]
    recent["acceptanceDateTime"][0] = "2026-01-01T14:00:00Z"
    recent["form"][0] = "4"
    recent["primaryDocument"][0] = primary
    raw = json.dumps(document).encode()
    proof = json.loads(receipt)
    proof.update(sha256=sha256(raw).hexdigest(), byte_length=len(raw))
    result = build_durable_index(
        raw, json.dumps(proof).encode(), **arguments(), now=datetime(2026, 1, 1, 15, 2, tzinfo=UTC)
    )
    assert len(result.issues) == 1 and len(result.index.entries) == 1
