"""Listed historical originals never erase uncertainty or masquerade as HTML."""

import hashlib
import json
from datetime import UTC, datetime

import pytest
from nexus.research.sec_listed_history import build_listed_index
from unit.research.test_sec_disclosure_index import CIK, FIELDS, IDENTITY, INSTRUMENT

NOW = datetime(2026, 1, 6, 20, tzinfo=UTC)
NAME = f"CIK{CIK}-submissions-001.json"


def packed(value, name=None):
    raw = json.dumps(value).encode()
    url = f"https://data.sec.gov/submissions/{name or f'CIK{CIK}.json'}"
    proof = json.dumps(
        dict(
            source_url=url,
            received_at=NOW.isoformat(),
            http_status=200,
            byte_length=len(raw),
            sha256=hashlib.sha256(raw).hexdigest(),
        )
    ).encode()
    return raw, proof


def rows(*values):
    return {key: [row[i] for row in values] for i, key in enumerate(FIELDS)}


def case():
    recent = rows(("0000000042-26-000001", "8-K", "2026-01-05T19:00:00Z", "2026-01-05", "new.htm"))
    old = rows(("0000000042-20-000001", "8-K/A", "2020-01-05T19:00:00Z", "2020-01-05", "old.txt"))
    parent = dict(
        cik=int(CIK),
        filings=dict(
            recent=recent,
            files=[dict(name=NAME, filingCount=1, filingFrom="2020-01-05", filingTo="2020-01-05")],
        ),
    )
    return parent, old


def build(parent, history, closing=None):
    return build_listed_index(
        parent=packed(parent),
        history=history,
        closing=packed(parent if closing is None else closing),
        instrument_id=INSTRUMENT,
        identity_snapshot_id=IDENTITY,
        issuer_cik=CIK,
    )


def test_complete_listed_original_retains_txt_and_explicit_metadata_version():
    parent, old = case()
    result = build(parent, ((NAME, *packed(old, NAME)),))
    assert result.history_complete and len(result.entries) == 2
    assert result.parser_version == "sec_listed_history_v2"
    assert {x.primary_document for x in result.entries} == {"new.htm", "old.txt"}
    assert result.recent_accessions == ("0000000042-26-000001",)


def test_missing_history_remains_partial_and_does_not_erase_recent():
    parent, _ = case()
    result = build(parent, ())
    assert not result.history_complete and len(result.entries) == 1


@pytest.mark.parametrize("mode", ["unlisted", "count", "conflict", "path", "closing"])
def test_bad_sources_never_produce_complete(mode):
    parent, old = case()
    closing = None
    name = NAME
    if mode == "unlisted":
        name = "CIK0000000002-submissions-001.json"
    elif mode == "count":
        parent["filings"]["files"][0]["filingCount"] = 2
    elif mode == "conflict":
        old["accessionNumber"][0] = parent["filings"]["recent"]["accessionNumber"][0]
    elif mode == "path":
        old["primaryDocument"][0] = "../old.txt"
    else:
        closing = json.loads(json.dumps(parent))
        closing["filings"]["files"] = []
    with pytest.raises(ValueError):
        build(parent, ((name, *packed(old, name)),), closing)


def test_txt_metadata_cannot_enter_html_interpretation(monkeypatch):
    from nexus.monitoring import filing_content

    parent, old = case()
    item = next(
        x
        for x in build(parent, ((NAME, *packed(old, NAME)),)).entries
        if x.primary_document.endswith(".txt")
    )

    def forbidden():
        pytest.fail("TXT metadata reached HTML parser")

    monkeypatch.setattr(filing_content, "_Inventory", forbidden)
    with pytest.raises(ValueError, match="SEC_MONITOR_DOCUMENT_FORMAT_UNSUPPORTED"):
        filing_content.document_inventory(b"not an HTML document", entry=item, issuer_cik=CIK)


@pytest.mark.parametrize(
    "name",
    [
        "../old.txt",
        "https://example.invalid/old.txt",
        "a%2fb.txt",
        "a\\b.txt",
        "old.txt?x=1",
        "old.txt#part",
        "old.xml",
        "old.txt/extra",
    ],
)
def test_versioned_metadata_keeps_file_path_boundaries(name):
    parent, old = case()
    old["primaryDocument"][0] = name
    with pytest.raises(ValueError):
        build(parent, ((NAME, *packed(old, NAME)),))


def test_identical_cross_page_row_is_deduplicated_but_keeps_recent_membership():
    parent, old = case()
    parent["filings"]["recent"] = old
    result = build(parent, ((NAME, *packed(old, NAME)),))
    assert result.history_complete and len(result.entries) == 1
    assert result.recent_accessions == (old["accessionNumber"][0],)


def test_non_target_filing_without_primary_document_still_counts_in_coverage():
    parent, old = case()
    old["form"][0] = "10-Q"
    old["primaryDocument"][0] = ""
    result = build(parent, ((NAME, *packed(old, NAME)),))
    assert result.history_complete and len(result.entries) == 1
    assert result.coverage_start_at == datetime(2020, 1, 5, 19, tzinfo=UTC)
    old["form"][0] = "8-K"
    with pytest.raises(ValueError):
        build(parent, ((NAME, *packed(old, NAME)),))


@pytest.mark.parametrize(
    "damage",
    [
        "missing_declaration",
        "unequal_arrays",
        "extra_file",
        "over_limit",
        "wrong_receipt",
        "duplicate_file",
    ],
)
def test_source_completeness_is_not_inferred_from_a_successful_single_get(damage):
    parent, old = case()
    history = ((NAME, *packed(old, NAME)),)
    if damage == "missing_declaration":
        del parent["filings"]["files"]
        result = build(parent, ())
        assert not result.history_complete
        return
    if damage == "unequal_arrays":
        old["form"] = []
        history = ((NAME, *packed(old, NAME)),)
    elif damage == "extra_file":
        parent["filings"]["files"] *= 2
    elif damage == "over_limit":
        parent["filings"]["files"][0]["filingCount"] = 10001
    elif damage == "duplicate_file":
        history *= 2
    elif damage == "wrong_receipt":
        raw, proof = packed(old, NAME)
        value = json.loads(proof)
        value["http_status"] = 206
        history = ((NAME, raw, json.dumps(value).encode()),)
    with pytest.raises(ValueError):
        build(parent, history)


@pytest.mark.asyncio
async def test_txt_document_collection_rejects_before_http(tmp_path):
    import httpx
    from nexus.ops.monitor_content_local import _collect_documents

    parent, old = case()
    item = next(
        x
        for x in build(parent, ((NAME, *packed(old, NAME)),)).entries
        if x.primary_document.endswith(".txt")
    )
    tmp_path.chmod(0o700)

    def forbidden(request):
        pytest.fail("TXT metadata caused document HTTP request")

    async with httpx.AsyncClient(transport=httpx.MockTransport(forbidden)) as client:
        with pytest.raises(ValueError, match="SEC_MONITOR_DOCUMENT_FORMAT_UNSUPPORTED"):
            await _collect_documents(
                entry=item,
                issuer_cik=CIK,
                archive_dir=tmp_path / "txt",
                contact="test@example.invalid",
                http_client=client,
            )
    receipt = json.loads((tmp_path / "txt/result.json").read_text())
    assert receipt["provider_requests"] == 0 and receipt["job_created"] is False


@pytest.mark.asyncio
async def test_collector_counts_directory_before_any_additional_http(tmp_path, monkeypatch):
    import httpx
    from nexus.ops import sec_listed_local as collector

    parent, _ = case()
    raw, receipt = packed(parent)
    files = {
        "directory.json": b'{"synthetic":"directory"}',
        "directory-receipt.json": b'{"synthetic":"receipt"}',
        "NVDA-submissions.json": raw,
        "NVDA-submissions-receipt.json": receipt,
    }
    for name, content in files.items():
        (tmp_path / name).write_bytes(content)
    cap = sum(map(len, files.values()))
    assert len(raw) + len(receipt) < cap
    monkeypatch.setattr(collector, "MAX_RAW_BYTES", cap)
    requests = []

    def handle(request):
        requests.append(request)
        raise AssertionError("directory must consume remaining bytes before HTTP")

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        with pytest.raises(ValueError, match="SEC_INDEX_HISTORY_LIMIT"):
            await collector.collect_listed_sources(
                archive_dir=tmp_path, symbol="NVDA", contact="test@example.invalid", client=client
            )
    assert requests == []
    assert not (tmp_path / "history-manifest.json").exists()
    assert json.loads((tmp_path / "history-failure.json").read_text())["requests"] == 0
