"""An exhibit is independently bound evidence, never a replacement primary document."""

import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import UUID

import pytest

NOW = datetime(2026, 9, 5, 12, tzinfo=UTC)
ACCESSION = "0000000042-26-000001"
DIRECTORY = "https://www.sec.gov/Archives/edgar/data/1/000000004226000001/"


def archive(raw, url, *, received=NOW):
    return raw, json.dumps(
        {
            "source_url": url,
            "sha256": sha256(raw).hexdigest(),
            "byte_length": len(raw),
            "received_at": received.isoformat(),
        }
    ).encode()


def exhibit_archives():
    metadata = {
        "cik": 1,
        "filings": {
            "recent": {
                "accessionNumber": [ACCESSION],
                "form": ["8-K"],
                "acceptanceDateTime": ["2026-09-02T20:00:00Z"],
                "filingDate": ["2026-09-02"],
                "primaryDocument": ["primary.htm"],
            }
        },
    }
    parent = (
        b'<p>Item 2.02. Unaudited results are furnished, not filed.</p><a href="ex99.htm">99.1</a>'
    )
    exhibit = (
        b"<h1>Unaudited quarterly results</h1><script>hidden dangerous text</script>"
        + b"<p>Context evidence remains distinct. </p>" * 420
        + b"<table><tr><th>Quarter / cumulative</th><th>USD millions</th></tr>"
        b"<tr><td>GAAP operating cash flow</td><td>12.125</td></tr></table>"
        b"<p>Non-GAAP reconciliation. Forward guidance is not actual results.</p>"
    )
    return [
        archive(
            json.dumps(metadata).encode(), "https://data.sec.gov/submissions/CIK0000000001.json"
        ),
        archive(parent, DIRECTORY + "primary.htm"),
        archive(exhibit, DIRECTORY + "ex99.htm"),
    ]


def build(archives=None, **changes):
    from nexus.research.sec_exhibits import build_exhibit_disclosure

    return build_exhibit_disclosure(
        instrument_id=UUID(int=1),
        identity_snapshot_id=UUID(int=2),
        issuer_cik="0000000001",
        archives=exhibit_archives() if archives is None else archives,
        now=NOW,
        **changes,
    )


def test_parent_and_complete_exhibit_remain_distinct_after_legacy_limit():
    report = build()
    assert report.form == "8-K" and report.source_url == DIRECTORY + "primary.htm"
    assert "operating cash flow" not in report.excerpt
    assert 12000 < len(report.exhibit.excerpt) <= 32000
    assert "GAAP operating cash flow" in report.exhibit.excerpt
    assert "Quarter / cumulative" in report.exhibit.excerpt
    assert "USD millions" in report.exhibit.excerpt and "12.125" in report.exhibit.excerpt
    assert "hidden dangerous text" not in report.exhibit.excerpt
    assert report.exhibit.truncated is False and report.exhibit.number == "99.1"
    assert report.exhibit.source_url == DIRECTORY + "ex99.htm"
    assert report.available_at == NOW
    assert len(report.originals) == 3 and len(report.receipt_hashes) == 3


@pytest.mark.parametrize("damage", ["hidden_link", "future_filed", "invalid_filings"])
def test_parent_metadata_and_visible_link_are_required(damage):
    values = exhibit_archives()
    if damage == "hidden_link":
        parent = values[1][0].replace(b"<a href=", b"<div hidden><a href=") + b"</div>"
        values[1] = archive(parent, DIRECTORY + "primary.htm")
    else:
        metadata = json.loads(values[0][0])
        if damage == "future_filed":
            metadata["filings"]["recent"]["filingDate"] = ["2099-01-01"]
        else:
            metadata["filings"] = []
        values[0] = archive(json.dumps(metadata).encode(), json.loads(values[0][1])["source_url"])
    with pytest.raises(ValueError):
        build(values)


def test_archived_child_receipt_can_bind_the_exact_parent_hash():
    values = exhibit_archives()
    receipt = json.loads(values[2][1])
    receipt["parent_sha256"] = sha256(values[1][0]).hexdigest()
    values[2] = values[2][0], json.dumps(receipt).encode()
    assert build(values).originals[2].parent_sha256 == receipt["parent_sha256"]
    receipt["parent_sha256"] = "0" * 64
    values[2] = values[2][0], json.dumps(receipt).encode()
    with pytest.raises(ValueError):
        build(values)


@pytest.mark.parametrize(
    "damage",
    [
        "raw_hash",
        "receipt_length",
        "future_parent",
        "future_exhibit",
        "wrong_cik",
        "wrong_form",
        "wrong_primary",
        "duplicate_accession",
        "amendment",
        "cross_issuer",
        "cross_accession",
        "external",
        "multiple",
        "missing",
        "fragment",
        "encoded_path",
        "parent_receipt_url",
        "exhibit_receipt_url",
        "too_long",
        "too_large",
        "empty_text",
    ],
)
def test_invalid_original_chain_is_rejected_before_any_storage(damage):
    archives = deepcopy(exhibit_archives())
    if damage in {
        "raw_hash",
        "receipt_length",
        "future_parent",
        "future_exhibit",
        "parent_receipt_url",
        "exhibit_receipt_url",
    }:
        index = 2 if damage in {"future_exhibit", "exhibit_receipt_url"} else 1
        raw, receipt_raw = archives[index]
        receipt = json.loads(receipt_raw)
        if damage == "raw_hash":
            receipt["sha256"] = "0" * 64
        elif damage == "receipt_length":
            receipt["byte_length"] += 1
        elif damage.startswith("future"):
            receipt["received_at"] = (NOW + timedelta(seconds=1)).isoformat()
        else:
            receipt["source_url"] = DIRECTORY + "other.htm"
        archives[index] = raw, json.dumps(receipt).encode()
    elif damage in {"wrong_cik", "wrong_form", "wrong_primary", "duplicate_accession", "amendment"}:
        metadata = json.loads(archives[0][0])
        recent = metadata["filings"]["recent"]
        if damage == "wrong_cik":
            metadata["cik"] = 2
        elif damage == "wrong_form":
            recent["form"] = ["10-Q"]
        elif damage == "wrong_primary":
            recent["primaryDocument"] = ["other.htm"]
        elif damage == "amendment":
            recent["form"] = ["8-K/A"]
        else:
            for values in recent.values():
                values.extend(values[:])
        archives[0] = archive(
            json.dumps(metadata).encode(), json.loads(archives[0][1])["source_url"]
        )
    elif damage in {"too_long", "too_large", "empty_text"}:
        raw = (
            b"<p>" + b"x" * 32001 + b"</p>"
            if damage == "too_long"
            else (
                b"x" * (8 * 1024 * 1024 + 1)
                if damage == "too_large"
                else b"<script>no visible text here</script>"
            )
        )
        archives[2] = archive(raw, DIRECTORY + "ex99.htm")
    else:
        href = {
            "cross_issuer": DIRECTORY.replace("/data/1/", "/data/2/") + "ex99.htm",
            "cross_accession": "../000000004226000002/ex99.htm",
            "external": "https://other.invalid/ex99.htm",
            "fragment": "ex99.htm#table",
            "encoded_path": "%65x99.htm",
        }.get(damage, "ex99.htm")
        parent = b"<p>Unaudited results are furnished, not filed.</p>" + (
            b"" if damage == "missing" else f'<a href="{href}">99.1</a>'.encode()
        )
        if damage == "multiple":
            parent += b'<a href="second.htm">99.1</a>'
        archives[1] = archive(parent, DIRECTORY + "primary.htm")
    with pytest.raises(ValueError):
        build(archives)
