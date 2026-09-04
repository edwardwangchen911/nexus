"""Synthetic collector archives: exact data, explicit identity, no ledger authority."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from nexus.research.alpaca_actions_archive import load_action_collection
from nexus.research.alpaca_actions_storage import action_tool_payload

NOW = datetime(2026, 8, 10, tzinfo=UTC)
QUERY = [
    ["symbols", "TEST"],
    ["start", "2026-08-01"],
    ["end", "2026-08-09"],
    ["region", "us"],
    ["data_quality", "all"],
    ["sort", "asc"],
    ["limit", "1000"],
]


def archive(tmp_path, *, pages=None, receipts=None, manifest_changes=None):
    directory = tmp_path / "collection"
    directory.mkdir(mode=0o700)
    if pages is None:
        pages = [
            b'{"corporate_actions":{"cash_dividends":[{"id":"synthetic-1",'
            b'"symbol":"TEST","rate":0.123456789012345678901,"currency":"",'
            b'"ex_date":"2026-08-12","unknown_future_field":{"x":null}}]},'
            b'"next_page_token":null}'
        ]
    refs = []
    cursor = None
    for number, raw in enumerate(pages, 1):
        raw_name, receipt_name = f"page-{number:03}.json", f"page-{number:03}.receipt.json"
        receipt = {
            "schema": "nexus_alpaca_actions_receipt_v1",
            "source": "alpaca",
            "origin": "https://data.alpaca.markets",
            "path": "/v1/corporate-actions",
            "query": QUERY + ([["page_token", cursor]] if cursor else []),
            "raw_file": raw_name,
            "sha256": sha256(raw).hexdigest(),
            "byte_length": len(raw),
            "received_at": (NOW - timedelta(minutes=10 - number)).isoformat(),
        }
        if receipts:
            receipt.update(receipts.get(number, {}))
        (directory / raw_name).write_bytes(raw)
        (directory / receipt_name).write_text(json.dumps(receipt))
        try:
            cursor = json.loads(raw)["next_page_token"]
        except (ValueError, KeyError):
            cursor = None
        refs.append({"raw_file": raw_name, "receipt_file": receipt_name})
    manifest = {
        "schema": "nexus_alpaca_actions_collection_v1",
        "source": "alpaca",
        "origin": "https://data.alpaca.markets",
        "path": "/v1/corporate-actions",
        "query": QUERY,
        "pages": refs,
        "error_code": None,
        "status": "collected",
        "pagination_exhausted": True,
        "source_completeness": "unknown",
        "execution_locked": True,
        "historical_backtest_eligible": False,
    }
    manifest.update(manifest_changes or {})
    (directory / "manifest.json").write_text(json.dumps(manifest))
    for path in directory.iterdir():
        path.chmod(0o600)
    return directory


def load(directory, **kwargs):
    return load_action_collection(
        directory, instrument_id=UUID(int=1), symbol="TEST", now=NOW, **kwargs
    )


def test_archive_freezes_exact_numbers_identity_original_references_and_unknowns(tmp_path):
    directory = archive(tmp_path)
    result = load(directory)
    document = result.document
    event = document.events[0]
    assert event.rate == Decimal("0.123456789012345678901")
    assert event.currency == "" and event.process_date is None
    assert event.page_number == 1 and event.index == 0 and event.category == "cash_dividends"
    assert document.total_event_count == 1 and document.unbound_event_count == 0
    assert document.available_at == NOW - timedelta(minutes=9)
    assert (
        document.pages[0].raw_content_hash
        == sha256((directory / "page-001.json").read_bytes()).hexdigest()
    )
    assert (
        document.manifest_content_hash
        == sha256((directory / "manifest.json").read_bytes()).hexdigest()
    )
    assert type(document).model_validate_json(document.canonical_bytes()) == document
    assert document.canonical_bytes() == load(directory).document.canonical_bytes()
    assert document.pagination_exhausted and document.execution_locked
    assert not document.ledger_eligible and not document.historical_backtest_eligible
    assert document.revision_semantics == "local_content_hash"
    assert document.source_completeness == "unknown"
    assert len(result.originals) == 3


def test_missing_and_mismatched_identity_are_unbound_not_filled_from_request(tmp_path):
    entries = [
        {"id": "a"},
        {"id": "b", "symbol": "OTHER"},
        {"id": "c", "old_symbol": "TEST", "new_symbol": "NEW"},
    ]
    raw = json.dumps({"corporate_actions": {"renames": entries}, "next_page_token": None}).encode()
    doc = load(archive(tmp_path, pages=[raw])).document
    assert doc.total_event_count == 3 and doc.unbound_event_count == 2
    assert len(doc.events) == 1
    assert doc.events[0].symbol is None and doc.events[0].old_symbol == "TEST"
    assert doc.events[0].index == 2


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "partial"},
        {"pagination_exhausted": False},
        {"error_code": "failure"},
        {"source": "other"},
        {"execution_locked": False},
        {"source_completeness": "complete"},
        {"pages": [{"raw_file": "../secret", "receipt_file": "page-001.receipt.json"}]},
        {"query": QUERY[:-1]},
        {"historical_backtest_eligible": True},
    ],
)
def test_untrusted_manifest_is_rejected(tmp_path, changes):
    with pytest.raises(ValueError, match="^ALPACA_ACTIONS_ARCHIVE_INVALID$"):
        load(archive(tmp_path, manifest_changes=changes))


@pytest.mark.parametrize(
    "changes",
    [
        {"sha256": "0" * 64},
        {"byte_length": True},
        {"byte_length": 1},
        {"received_at": (NOW + timedelta(seconds=1)).isoformat()},
        {"received_at": "2026-08-01T00:00:00"},
        {"source": "other"},
        {"query": QUERY + [["page_token", "unexpected"]]},
    ],
)
def test_bad_receipt_cannot_be_imported(tmp_path, changes):
    with pytest.raises(ValueError, match="^ALPACA_ACTIONS_ARCHIVE_INVALID$"):
        load(archive(tmp_path, receipts={1: changes}))


@pytest.mark.parametrize(
    "field,value",
    [
        ("rate", True),
        ("rate", "NaN"),
        ("rate", {}),
        ("ex_date", "2026-02-30"),
        ("process_date", 123),
        ("currency", 3),
        ("symbol", 3),
        ("new_rate", ""),
    ],
)
def test_invalid_provided_event_fields_reject_whole_archive(tmp_path, field, value):
    event = {"id": "a", "symbol": "TEST", field: value}
    raw = json.dumps(
        {"corporate_actions": {"cash_dividends": [event]}, "next_page_token": None}
    ).encode()
    with pytest.raises(ValueError, match="^ALPACA_ACTIONS_ARCHIVE_INVALID$"):
        load(archive(tmp_path, pages=[raw]))


def test_token_chain_must_end_once_without_extra_pages_or_reversed_receipts(tmp_path):
    def raw(token):
        return json.dumps({"corporate_actions": {}, "next_page_token": token}).encode()

    directory = archive(tmp_path, pages=[raw("next"), raw(None)])
    assert load(directory).document.total_event_count == 0
    receipt_path = directory / "page-002.receipt.json"
    receipt = json.loads(receipt_path.read_bytes())
    receipt["received_at"] = (NOW - timedelta(days=1)).isoformat()
    receipt_path.write_text(json.dumps(receipt))
    with pytest.raises(ValueError):
        load(directory)


@pytest.mark.parametrize("tokens", [[None, None], ["next", "next"], ["next", "last"]])
def test_early_end_loop_or_unexhausted_token_rejects(tmp_path, tokens):
    pages = [
        json.dumps({"corporate_actions": {}, "next_page_token": token}).encode() for token in tokens
    ]
    with pytest.raises(ValueError):
        load(archive(tmp_path, pages=pages))


def test_duplicate_keys_symlinks_and_unlisted_pages_reject(tmp_path):
    directory = archive(tmp_path)
    original = directory / "page-001.json"
    saved = original.read_bytes()
    original.unlink()
    target = tmp_path / "private-original"
    target.write_bytes(saved)
    original.symlink_to(target)
    with pytest.raises(ValueError):
        load(directory)
    original.unlink()
    original.write_bytes(saved)
    original.chmod(0o600)
    extra = directory / "page-002.json"
    extra.write_bytes(b"{}")
    with pytest.raises(ValueError):
        load(directory)
    extra.unlink()
    manifest = directory / "manifest.json"
    manifest.write_bytes(
        manifest.read_bytes().replace(
            b'"status": "collected"', b'"status":"failed","status":"collected"'
        )
    )
    with pytest.raises(ValueError):
        load(directory)


def test_tool_truncates_only_view_not_canonical_evidence(tmp_path):
    raw = json.dumps(
        {
            "corporate_actions": {"future": [{"id": str(i), "symbol": "TEST"} for i in range(33)]},
            "next_page_token": None,
        }
    ).encode()
    doc = load(archive(tmp_path, pages=[raw])).document
    before = doc.canonical_bytes()
    payload = action_tool_payload(doc)
    assert len(payload["events"]) == 32 and payload["truncated"] is True
    assert payload["total_event_count"] == 33 and payload["unbound_event_count"] == 0
    assert payload["source_completeness"] == "unknown"
    assert payload["available_at_semantics"] == "maximum_page_receipt_not_historical_availability"
    assert doc.canonical_bytes() == before and len(doc.events) == 33
