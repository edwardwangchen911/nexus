"""SEC metadata events retain source custody, issuer and coverage without interpretation."""

import hashlib
import json
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.research.sec_disclosure_index import build_disclosure_index

INSTRUMENT = UUID("00000000-0000-4000-8000-000000000001")
IDENTITY = UUID("00000000-0000-4000-8000-000000000002")
CIK = "0000000001"
RECEIVED = "2026-01-05T20:00:00+00:00"
FIELDS = ("accessionNumber", "form", "acceptanceDateTime", "filingDate", "primaryDocument")


def source(rows=None, files=None):
    if rows is None:
        rows = [("0000000042-26-000001", "8-K", "2026-01-05T19:00:00Z", "2026-01-05", "report.htm")]
    return {
        "cik": 1,
        "filings": {
            "recent": {key: [row[i] for row in rows] for i, key in enumerate(FIELDS)},
            "files": files or [],
        },
    }


def encode(document):
    return json.dumps(document, separators=(",", ":")).encode()


def receipt(raw, **changes):
    return encode(
        {
            "source_url": f"https://data.sec.gov/submissions/CIK{CIK}.json",
            "received_at": RECEIVED,
            "sha256": hashlib.sha256(raw).hexdigest(),
            "byte_length": len(raw),
            "http_status": 200,
        }
        | changes
    )


def build(raw=None, receipt_bytes=None, **kwargs):
    raw = encode(source()) if raw is None else raw
    return build_disclosure_index(
        raw,
        receipt(raw) if receipt_bytes is None else receipt_bytes,
        instrument_id=INSTRUMENT,
        identity_snapshot_id=IDENTITY,
        issuer_cik=CIK,
        **kwargs,
    )


@pytest.mark.parametrize("schema", [None, "sec_identity_source_receipt_v1"])
def test_exact_public_metadata_is_bound_without_financial_inference(schema):
    raw = encode(source())
    item = build(raw, receipt(raw, **({"schema": schema} if schema else {})))
    assert item.instrument_id == INSTRUMENT and item.identity_snapshot_id == IDENTITY
    assert item.issuer_cik == CIK and item.raw_content_hash == hashlib.sha256(raw).hexdigest()
    assert item.source_url == f"https://data.sec.gov/submissions/CIK{CIK}.json"
    assert item.received_at == datetime(2026, 1, 5, 20, tzinfo=UTC)
    (entry,) = item.entries
    assert entry.accession_number == "0000000042-26-000001"  # Filing agent CIK may differ.
    assert entry.form == "8-K" and entry.primary_document == "report.htm"
    assert entry.filed_at == datetime(2026, 1, 5, tzinfo=UTC)
    assert item.coverage_start_at == entry.accepted_at and item.history_complete
    assert (
        item.canonical_bytes()
        == json.dumps(item.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
    )


def test_all_recent_forms_define_coverage_and_ties_are_retained():
    rows = [
        ("0000000042-26-000001", "8-K", "2026-01-05T14:00:00-05:00", "2026-01-05", "report.htm"),
        ("0000000042-26-000002", "8-K/A", "2026-01-05T19:00:00Z", "2026-01-05", "amend.html"),
        ("0000000042-26-000003", "4", "2026-01-02T19:00:00Z", "2026-01-02", "ownership.xml"),
    ]
    item = build(encode(source(rows, [{"name": "older.json"}])))
    assert len(item.entries) == 2 and item.entries[0].accepted_at == item.entries[1].accepted_at
    assert item.entries[0].accepted_at.tzinfo == UTC
    assert item.coverage_start_at == datetime(2026, 1, 2, 19, tzinfo=UTC)
    assert not item.history_complete


@pytest.mark.parametrize("files,complete", [([], True), ([{"name": "older.json"}], False)])
def test_empty_current_index_is_success_and_does_not_invent_coverage(files, complete):
    result = build(encode(source([], files)))
    assert result.entries == () and result.coverage_start_at is None
    assert result.history_complete is complete


def test_missing_history_declaration_is_not_complete():
    document = source()
    del document["filings"]["files"]
    assert not build(encode(document)).history_complete


def test_identical_duplicate_dedup_and_conflicting_duplicate_rejected():
    row = ("0000000042-26-000001", "8-K", "2026-01-05T19:00:00Z", "2026-01-05", "report.htm")
    assert len(build(encode(source([row, row]))).entries) == 1
    for i, value in [
        (1, "8-K/A"),
        (2, "2026-01-04T19:00:00Z"),
        (3, "2026-01-04"),
        (4, "other.htm"),
    ]:
        changed = list(row)
        changed[i] = value
        with pytest.raises(ValueError):
            build(encode(source([row, changed])))


@pytest.mark.parametrize(
    "changes",
    [
        {"http_status": 201},
        {"http_status": True},
        {"byte_length": True},
        {"byte_length": 1},
        {"sha256": "0" * 64},
        {"source_url": "https://data.sec.gov/submissions/CIK0000000002.json"},
        {"source_url": f"https://data.sec.gov/submissions/CIK{CIK}.json?extra=1"},
        {"schema": "other"},
        {"unexpected": "ignored?"},
        {"received_at": "2026-01-05T20:00:00"},
        {"received_at": "2026-01-04T20:00:00Z"},
    ],
)
def test_receipt_tampering_or_unbound_fields_fail(changes):
    raw = encode(source())
    with pytest.raises(ValueError):
        build(raw, receipt(raw, **changes))


@pytest.mark.parametrize("cik", [2, True, "0000000002", "1.0", None])
def test_wrong_or_invalid_issuer_is_rejected(cik):
    document = source()
    document["cik"] = cik
    with pytest.raises(ValueError):
        build(encode(document))


@pytest.mark.parametrize(
    "field,value",
    [
        ("accessionNumber", "bad"),
        ("acceptanceDateTime", "2026-01-05T19:00:00"),
        ("acceptanceDateTime", "2026-01-05T21:00:00Z"),
        ("filingDate", "2026-01-06"),
        ("filingDate", "2026-01-05T00:00:00Z"),
        ("primaryDocument", "../report.htm"),
        ("primaryDocument", "folder/report.htm"),
        ("primaryDocument", "report.htm?x=1"),
        ("primaryDocument", "report.xml"),
        ("primaryDocument", "a" * 256 + ".htm"),
        ("primaryDocument", "report.htm#anchor"),
        ("primaryDocument", "report%2ehtm"),
        ("form", None),
    ],
)
def test_row_contract_rejects_invalid_source_fields(field, value):
    document = source()
    document["filings"]["recent"][field][0] = value
    with pytest.raises(ValueError):
        build(encode(document))


def test_amendment_paths_and_non_target_times_receive_same_validation():
    document = source()
    document["filings"]["recent"]["form"] = ["8-K/A"]
    document["filings"]["recent"]["primaryDocument"] = ["../report.htm"]
    with pytest.raises(ValueError):
        build(encode(document))
    document = source()
    document["filings"]["recent"]["form"] = ["4"]
    document["filings"]["recent"]["acceptanceDateTime"] = ["2099-01-01T00:00:00Z"]
    with pytest.raises(ValueError):
        build(encode(document))


def test_array_shape_capacity_and_raw_byte_boundaries():
    document = source()
    document["filings"]["recent"]["form"] = []
    with pytest.raises(ValueError):
        build(encode(document))
    rows = [
        (f"0000000042-26-{i:06d}", "8-K", "2026-01-05T19:00:00Z", "2026-01-05", "report.htm")
        for i in range(1000)
    ]
    assert len(build(encode(source(rows))).entries) == 1000
    assert len(build(encode(source(rows + [rows[0]]))).entries) == 1000
    raw = encode(source())
    assert build(raw + b" " * (8 * 1024 * 1024 - len(raw))).entries
    with pytest.raises(ValueError):
        build(raw + b" " * (8 * 1024 * 1024 + 1 - len(raw)))


def test_duplicate_json_keys_in_source_or_receipt_are_rejected():
    raw = encode(source()).replace(b'"cik":1', b'"cik":2,"cik":1')
    with pytest.raises(ValueError):
        build(raw)
    raw = encode(source())
    with pytest.raises(ValueError):
        build(
            raw, receipt(raw).replace(b'"http_status":200', b'"http_status":500,"http_status":200')
        )


def test_future_receipt_is_not_local_custody():
    raw = encode(source())
    with pytest.raises(ValueError):
        build(raw, receipt(raw, received_at=(datetime.now(UTC) + timedelta(days=2)).isoformat()))


def test_large_recent_index_retains_non_target_rows_without_increasing_event_capacity():
    rows = [
        (f"0000000042-26-{i:06d}", "4", "2026-01-05T19:00:00Z", "2026-01-05", "ownership.xml")
        for i in range(10000)
    ]
    rows[0] = ("0000000042-26-000000", "8-K", "2026-01-05T19:00:00Z", "2026-01-05", "r.htm")
    assert len(build(encode(source(rows[:1001]))).entries) == 1
    assert len(build(encode(source(rows))).entries) == 1
    with pytest.raises(ValueError):
        build(encode(source(rows + [rows[0]])))


def test_target_entry_capacity_does_not_truncate():
    rows = [
        (f"0000000042-26-{i:06d}", "8-K", "2026-01-05T19:00:00Z", "2026-01-05", "report.htm")
        for i in range(1001)
    ]
    with pytest.raises(ValueError):
        build(encode(source(rows)))
