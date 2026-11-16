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
