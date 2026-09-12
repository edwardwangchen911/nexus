import json
from datetime import UTC, datetime
from types import SimpleNamespace
from uuid import UUID

import httpx
import nexus.ops.pending_identity_local as resolver_module
import pytest
from nexus.ops.pending_identity_local import (
    CollectedIdentityEvidence,
    _load_existing_evidence,
    bind_fund_mapping,
    collect_common_stock_evidence,
    collect_fund_evidence,
    resolve_pending_identities,
    select_common_stock_filing,
    select_fund_filing,
)
from pydantic import SecretStr

NOW = datetime(2026, 9, 12, 6, tzinfo=UTC)


def test_load_existing_evidence_reuses_complete_protected_archive(tmp_path) -> None:
    item = tmp_path / "item-01"
    for directory in (item, item / "fmp", item / "sec", item / "selectors"):
        directory.mkdir(mode=0o700)
    files = {
        "fmp/profile.json": b"profile",
        "fmp/receipt.json": b"receipt",
        "sec/submissions.raw": b"submissions",
        "sec/filing-index.raw": b"index",
        "sec/disclosure.raw": b"disclosure",
        "sec/receipts.json": b"receipts",
        "sec/mapping-binding.json": b"binding",
        "selectors/company_tickers_mf.json": b"mapping",
        "selectors/submissions.json": b"submissions",
        "selectors/class-search.html": b"search",
        "selectors/selection.json": json.dumps(
            {"schema": "pending_fund_selection_v1", "symbol": "FUND"}
        ).encode(),
    }
    for relative, content in files.items():
        path = item / relative
        path.write_bytes(content)
        path.chmod(0o600)

    result = _load_existing_evidence(item, symbol="FUND", kind="equity_etf")

    assert result.request_count == 0
    assert result.mapping_file == item / "selectors/company_tickers_mf.json"


def _submissions(cik: int, *rows: tuple[str, str, str, str, str]) -> bytes:
    fields = (
        "accessionNumber",
        "acceptanceDateTime",
        "filingDate",
        "form",
        "primaryDocument",
    )
    return json.dumps(
        {
            "cik": cik,
            "filings": {
                "recent": {
                    field: [row[index] for row in rows] for index, field in enumerate(fields)
                }
            },
        }
    ).encode()


def test_select_common_stock_filing_uses_unique_exchange_mapping_and_latest_report() -> None:
    mapping = json.dumps(
        {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [[123456, "Example Inc", "ACME", "Nasdaq"]],
        }
    ).encode()
    submissions = _submissions(
        123456,
        ("0000123456-26-000001", "2026-02-01T12:00:00Z", "2026-02-01", "10-K", "annual.htm"),
        ("0000123456-26-000002", "2026-05-01T12:00:00Z", "2026-05-01", "10-Q", "quarter.htm"),
        ("0000123456-26-000003", "2026-06-01T12:00:00Z", "2026-06-01", "8-K", "event.htm"),
    )

    selected = select_common_stock_filing(
        mapping_raw=mapping,
        submissions_raw=submissions,
        symbol="ACME",
        received_at=NOW,
    )

    assert selected.cik == "0000123456"
    assert selected.exchange == "XNAS"
    assert selected.accession == "0000123456-26-000002"
    assert selected.primary_document == "quarter.htm"
    assert selected.source_url == (
        "https://www.sec.gov/Archives/edgar/data/123456/000012345626000002/quarter.htm"
    )


def test_select_common_stock_filing_rejects_duplicate_ticker_mapping() -> None:
    mapping = json.dumps(
        {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [
                [123456, "Example Inc", "ACME", "Nasdaq"],
                [654321, "Other Inc", "ACME", "NYSE"],
            ],
        }
    ).encode()

    with pytest.raises(ValueError, match="PENDING_IDENTITY_COMMON_MAPPING_UNAVAILABLE"):
        select_common_stock_filing(
            mapping_raw=mapping,
            submissions_raw=_submissions(123456),
            symbol="ACME",
            received_at=NOW,
        )


def test_select_fund_filing_uses_unique_share_class_and_latest_497k() -> None:
    mapping = json.dumps(
        {
            "fields": ["cik", "seriesId", "classId", "symbol"],
            "data": [[777777, "S000000001", "C000000001", "FUND"]],
        }
    ).encode()
    submissions = _submissions(
        777777,
        ("0000777777-26-000010", "2026-03-01T12:00:00Z", "2026-03-01", "497K", "old.htm"),
        ("0000777777-26-000011", "2026-08-01T12:00:00Z", "2026-08-01", "497K", "current.htm"),
    )

    selected = select_fund_filing(
        mapping_raw=mapping,
        submissions_raw=submissions,
        symbol="FUND",
        received_at=NOW,
    )

    assert selected.cik == "0000777777"
    assert selected.series_id == "S000000001"
    assert selected.class_id == "C000000001"
    assert selected.accession == "0000777777-26-000011"


def test_select_fund_filing_rejects_future_or_missing_497k() -> None:
    mapping = json.dumps(
        {
            "fields": ["cik", "seriesId", "classId", "symbol"],
            "data": [[777777, "S000000001", "C000000001", "FUND"]],
        }
    ).encode()
    submissions = _submissions(
        777777,
        ("0000777777-26-000011", "2026-10-01T12:00:00Z", "2026-10-01", "497K", "future.htm"),
        ("0000777777-26-000012", "2026-08-01T12:00:00Z", "2026-08-01", "N-1A", "registration.htm"),
    )

    with pytest.raises(ValueError, match="PENDING_IDENTITY_FUND_FILING_UNAVAILABLE"):
        select_fund_filing(
            mapping_raw=mapping,
            submissions_raw=submissions,
            symbol="FUND",
            received_at=NOW,
        )


@pytest.mark.asyncio
async def test_collect_common_stock_evidence_uses_four_bounded_requests_and_archives_selectors(
    tmp_path,
) -> None:
    mapping = json.dumps(
        {
            "fields": ["cik", "name", "ticker", "exchange"],
            "data": [[123456, "Example Inc", "ACME", "Nasdaq"]],
        }
    ).encode()
    submissions = _submissions(
        123456,
        ("0000123456-26-000002", "2026-05-01T12:00:00Z", "2026-05-01", "10-Q", "quarter.htm"),
    )
    responses = {
        ("financialmodelingprep.com", "/stable/profile"): (
            b'[{"symbol":"ACME","cik":"0000123456","currency":"USD",'
            b'"exchange":"NASDAQ","isEtf":false}]'
        ),
        ("www.sec.gov", "/files/company_tickers_exchange.json"): mapping,
        ("data.sec.gov", "/submissions/CIK0000123456.json"): submissions,
        (
            "www.sec.gov",
            "/Archives/edgar/data/123456/000012345626000002/quarter.htm",
        ): b"<html>filing</html>",
    }
    requests: list[tuple[str, str]] = []

    def respond(request: httpx.Request) -> httpx.Response:
        key = (request.url.host, request.url.path)
        requests.append(key)
        return httpx.Response(200, content=responses[key])

    output_dir = tmp_path / "pending"
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await collect_common_stock_evidence(
            symbol="ACME",
            fmp_key=SecretStr("synthetic-secret"),
            sec_contact="nexus@example.com",
            output_dir=output_dir,
            client=client,
        )

    assert result.request_count == 4
    assert requests == list(responses)
    assert result.fmp_archive == output_dir / "fmp"
    assert result.sec_archive == output_dir / "sec"
    assert (output_dir / "fmp" / "profile.json").read_bytes() == responses[
        ("financialmodelingprep.com", "/stable/profile")
    ]
    assert (output_dir / "selectors" / "company_tickers_exchange.json").read_bytes() == mapping
    assert (output_dir / "selectors" / "submissions.json").read_bytes() == submissions
    assert (output_dir / "sec" / "quarter.htm").read_bytes() == b"<html>filing</html>"
    assert all(path.stat().st_mode & 0o077 == 0 for path in output_dir.rglob("*"))


class _MappingVersions:
    def __init__(self, key: str, version_id: str, stored_at: datetime) -> None:
        self.key = key
        self.version_id = version_id
        self.stored_at = stored_at

    def list_object_versions(self, **kwargs: object) -> dict[str, object]:
        assert kwargs == {"Bucket": "evidence", "Prefix": self.key, "MaxKeys": 1000}
        return {
            "IsTruncated": False,
            "Versions": [
                {"Key": self.key, "VersionId": self.version_id, "LastModified": self.stored_at}
            ],
        }


class _MappingStore:
    _bucket = "evidence"
    _prefix = "provider/market"

    def __init__(self, stored_at: datetime) -> None:
        self.version_id = "version-1"
        self.key = "provider/market/raw/sec/2026-09-12/ticker-directory/" + "a" * 64 + ".json"
        self._client = _MappingVersions(self.key, self.version_id, stored_at)

    async def put_if_absent(self, *, key: str, content: bytes) -> str:
        assert key == "raw/sec/2026-09-12/ticker-directory/" + "a" * 64 + ".json"
        assert content == b"mapping"
        return f"s3://evidence/{self._prefix}/{key}?versionId={self.version_id}"


@pytest.mark.asyncio
async def test_bind_fund_mapping_freezes_exact_object_version(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(
        "nexus.ops.pending_identity_local.sha256",
        lambda _: type("Digest", (), {"hexdigest": lambda self: "a" * 64})(),
    )
    stored_at = datetime(2026, 9, 12, 5, 30, tzinfo=UTC)
    mapping_file = tmp_path / "mapping.json"
    mapping_file.write_bytes(b"mapping")
    mapping_file.chmod(0o600)
    sec_archive = tmp_path / "sec"
    sec_archive.mkdir(mode=0o700)

    await bind_fund_mapping(
        _MappingStore(stored_at),
        mapping_file=mapping_file,
        sec_archive=sec_archive,
        session_date=stored_at.date(),
    )

    assert json.loads((sec_archive / "mapping-binding.json").read_bytes()) == {
        "key": "provider/market/raw/sec/2026-09-12/ticker-directory/" + "a" * 64 + ".json",
        "version_id": "version-1",
        "sha256": "a" * 64,
        "stored_at": stored_at.isoformat(),
        "original_received_at": None,
    }
    assert (sec_archive / "mapping-binding.json").stat().st_mode & 0o077 == 0


@pytest.mark.asyncio
async def test_collect_fund_evidence_uses_five_bounded_requests_and_archives_exact_inputs(
    tmp_path,
) -> None:
    mapping = json.dumps(
        {
            "fields": ["cik", "seriesId", "classId", "symbol"],
            "data": [[777777, "S000000001", "C000000001", "FUND"]],
        }
    ).encode()
    submissions = _submissions(
        777777,
        ("0000777777-26-000011", "2026-08-01T12:00:00Z", "2026-08-01", "497K", "current.htm"),
    )
    root = "/Archives/edgar/data/777777/000077777726000011"
    responses = {
        ("financialmodelingprep.com", "/stable/profile"): (
            b'[{"symbol":"FUND","cik":"0000777777","currency":"USD",'
            b'"exchange":"AMEX","isEtf":true}]'
        ),
        ("www.sec.gov", "/files/company_tickers_mf.json"): mapping,
        ("www.sec.gov", "/cgi-bin/browse-edgar"): (
            b"<html><body>Class/Contract: C000000001 Synthetic Class Ticker Symbol: FUND "
            b'<a href="/Archives/edgar/data/777777/000077777726000011/'
            b'0000777777-26-000011-index.html">Documents</a></body></html>'
        ),
        ("data.sec.gov", "/submissions/CIK0000777777.json"): submissions,
        ("www.sec.gov", root + "/0000777777-26-000011-index.html"): (
            b"<html>S000000001 C000000001 FUND "
            b'<a href="/Archives/edgar/data/777777/000077777726000011/current.htm">'
            b"document</a></html>"
        ),
        ("www.sec.gov", root + "/current.htm"): b"<html>FUND current prospectus document</html>",
    }
    requests: list[tuple[str, str]] = []

    def respond(request: httpx.Request) -> httpx.Response:
        key = (request.url.host, request.url.path)
        requests.append(key)
        return httpx.Response(200, content=responses[key])

    output_dir = tmp_path / "pending-fund"
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await collect_fund_evidence(
            symbol="FUND",
            fmp_key=SecretStr("synthetic-secret"),
            sec_contact="nexus@example.com",
            output_dir=output_dir,
            client=client,
        )

    assert result.request_count == 6
    assert requests == list(responses)
    assert result.mapping_file == output_dir / "selectors" / "company_tickers_mf.json"
    assert (output_dir / "sec" / "submissions.raw").read_bytes() == submissions
    assert b"S000000001 C000000001 FUND" in (output_dir / "sec" / "filing-index.raw").read_bytes()
    assert (output_dir / "sec" / "disclosure.raw").read_bytes() == (
        b"<html>FUND current prospectus document</html>"
    )
    assert json.loads((output_dir / "sec" / "receipts.json").read_bytes()) == [
        json.loads((output_dir / "sec" / f"receipt-{number}.json").read_bytes())
        for number in range(1, 4)
    ]
    assert all(path.stat().st_mode & 0o077 == 0 for path in output_dir.rglob("*"))


@pytest.mark.asyncio
async def test_collect_fund_evidence_skips_latest_497k_for_another_share_class(tmp_path) -> None:
    mapping = json.dumps(
        {
            "fields": ["cik", "seriesId", "classId", "symbol"],
            "data": [[777777, "S000000001", "C000000001", "FUND"]],
        }
    ).encode()
    submissions = _submissions(
        777777,
        ("0000777777-26-000012", "2026-09-01T12:00:00Z", "2026-09-01", "497K", "other.htm"),
        ("0000777777-26-000011", "2026-08-01T12:00:00Z", "2026-08-01", "497K", "current.htm"),
    )
    selected_root = "/Archives/edgar/data/777777/000077777726000011"
    responses = {
        ("financialmodelingprep.com", "/stable/profile"): (
            b'[{"symbol":"FUND","cik":"0000777777","currency":"USD",'
            b'"exchange":"AMEX","isEtf":true}]'
        ),
        ("www.sec.gov", "/files/company_tickers_mf.json"): mapping,
        ("www.sec.gov", "/cgi-bin/browse-edgar"): (
            b"<html><body>Class/Contract: C000000001 Synthetic Class Ticker Symbol: FUND "
            b'<a href="/Archives/edgar/data/777777/000077777726000011/'
            b'0000777777-26-000011-index.html">Documents</a></body></html>'
        ),
        ("data.sec.gov", "/submissions/CIK0000777777.json"): submissions,
        ("www.sec.gov", selected_root + "/0000777777-26-000011-index.html"): (
            b"<html>S000000001 C000000001 FUND "
            b'<a href="/Archives/edgar/data/777777/000077777726000011/current.htm">'
            b"document</a></html>"
        ),
        ("www.sec.gov", selected_root + "/current.htm"): (
            b"<html>FUND current prospectus document</html>"
        ),
    }
    requests: list[tuple[str, str]] = []

    def respond(request: httpx.Request) -> httpx.Response:
        key = (request.url.host, request.url.path)
        requests.append(key)
        return httpx.Response(200, content=responses[key])

    output_dir = tmp_path / "pending-fund"
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await collect_fund_evidence(
            symbol="FUND",
            fmp_key=SecretStr("synthetic-secret"),
            sec_contact="nexus@example.com",
            output_dir=output_dir,
            client=client,
        )

    assert result.request_count == 6
    assert requests == list(responses)
    assert (output_dir / "sec" / "disclosure.raw").read_bytes() == (
        b"<html>FUND current prospectus document</html>"
    )
    selection = json.loads((output_dir / "selectors" / "selection.json").read_bytes())
    assert selection["accession"] == "0000777777-26-000011"
    assert (
        b"Class/Contract: C000000001"
        in (output_dir / "selectors" / "class-search.html").read_bytes()
    )


@pytest.mark.asyncio
async def test_collect_fund_evidence_uses_class_search_instead_of_issuer_recent_order(
    tmp_path,
) -> None:
    mapping = json.dumps(
        {
            "fields": ["cik", "seriesId", "classId", "symbol"],
            "data": [[777777, "S000000001", "C000000001", "FUND"]],
        }
    ).encode()
    submissions = _submissions(
        777777,
        ("0000777777-26-000015", "2026-09-05T12:00:00Z", "2026-09-05", "497K", "other5.htm"),
        ("0000777777-26-000014", "2026-09-04T12:00:00Z", "2026-09-04", "497K", "other4.htm"),
        ("0000777777-26-000013", "2026-09-03T12:00:00Z", "2026-09-03", "497K", "other3.htm"),
        ("0000777777-26-000012", "2026-09-02T12:00:00Z", "2026-09-02", "497K", "other2.htm"),
        ("0000777777-26-000011", "2026-09-01T12:00:00Z", "2026-09-01", "497K", "document.htm"),
    )
    root = "/Archives/edgar/data/777777/000077777726000011"
    responses = {
        ("financialmodelingprep.com", "/stable/profile"): (
            b'[{"symbol":"FUND","cik":"0000777777","currency":"USD",'
            b'"exchange":"AMEX","isEtf":true}]'
        ),
        ("www.sec.gov", "/files/company_tickers_mf.json"): mapping,
        ("www.sec.gov", "/cgi-bin/browse-edgar"): (
            b"<html><body>Class/Contract: C000000001 Synthetic Class Ticker Symbol: FUND "
            b'<a href="/Archives/edgar/data/777777/000077777726000011/'
            b'0000777777-26-000011-index.html">Documents</a></body></html>'
        ),
        ("data.sec.gov", "/submissions/CIK0000777777.json"): submissions,
        ("www.sec.gov", root + "/0000777777-26-000011-index.html"): (
            b"<html>S000000001 C000000001 FUND "
            b'<a href="/Archives/edgar/data/777777/000077777726000011/document.htm">'
            b"document</a></html>"
        ),
        ("www.sec.gov", root + "/document.htm"): (b"<html>FUND current prospectus document</html>"),
    }

    def respond(request: httpx.Request) -> httpx.Response:
        key = (request.url.host, request.url.path)
        return httpx.Response(200, content=responses[key])

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await collect_fund_evidence(
            symbol="FUND",
            fmp_key=SecretStr("synthetic-secret"),
            sec_contact="nexus@example.com",
            output_dir=tmp_path / "pending-fund",
            client=client,
        )

    assert result.request_count == 6


class _FakeTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False


class _FakeSession:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return False

    def begin(self):
        return _FakeTransaction()

    async def execute(self, statement):
        return None


class _FakeBind:
    async def dispose(self) -> None:
        return None


class _FakeFactory:
    def __init__(self) -> None:
        self.kw = {"bind": _FakeBind()}

    def __call__(self):
        return _FakeSession()


class _FakeBroker:
    def __init__(self) -> None:
        self.accounts_calls = 0
        self.holdings_calls = 0

    async def read_accounts(self):
        self.accounts_calls += 1
        return SimpleNamespace(items=({},), quality="PASS", has_more=False)

    async def read_holdings(self, external_account_id, *, cursor, limit):
        assert external_account_id == "opaque-account"
        assert cursor is None
        assert limit == 100
        self.holdings_calls += 1
        return SimpleNamespace()


@pytest.mark.asyncio
async def test_resolver_processes_persisted_batch_with_one_broker_read(
    tmp_path, monkeypatch
) -> None:
    pending_reads = 0

    async def read_pending(session, *, as_of):
        nonlocal pending_reads
        pending_reads += 1
        if pending_reads == 1:
            return {
                UUID(int=1): SimpleNamespace(
                    provider_code="usAAA",
                    recommended_identity_route="current_held_common_stock",
                ),
                UUID(int=2): SimpleNamespace(
                    provider_code="usBBB",
                    recommended_identity_route="current_held_fund_or_structured",
                ),
            }
        return {}

    async def collect_common(**kwargs):
        item_dir = kwargs["output_dir"]
        item_dir.mkdir(mode=0o700)
        return CollectedIdentityEvidence(item_dir / "fmp", item_dir / "sec", 4)

    async def collect_fund(**kwargs):
        item_dir = kwargs["output_dir"]
        item_dir.mkdir(mode=0o700)
        mapping_file = item_dir / "mapping.json"
        mapping_file.write_bytes(b"mapping")
        return CollectedIdentityEvidence(item_dir / "fmp", item_dir / "sec", 6, mapping_file)

    async def import_identity(*args, **kwargs):
        return SimpleNamespace(snapshot_id=UUID(int=3))

    async def no_op(*args, **kwargs):
        return None

    class FakeAccountItem:
        @staticmethod
        def model_validate(item):
            return SimpleNamespace(external_account_id="opaque-account")

    monkeypatch.setattr(resolver_module, "session_factory", lambda settings: _FakeFactory())
    monkeypatch.setattr(resolver_module, "read_pending_instrument_discoveries", read_pending)
    monkeypatch.setattr(resolver_module, "collect_common_stock_evidence", collect_common)
    monkeypatch.setattr(resolver_module, "collect_fund_evidence", collect_fund)
    monkeypatch.setattr(resolver_module, "bind_fund_mapping", no_op)
    monkeypatch.setattr(resolver_module, "import_current_held_common_stock", import_identity)
    monkeypatch.setattr(resolver_module, "import_current_held_etf", import_identity)
    monkeypatch.setattr(resolver_module, "read_current_held_common_stock", no_op)
    monkeypatch.setattr(resolver_module, "read_current_held_etf", no_op)
    monkeypatch.setattr(resolver_module, "AccountItem", FakeAccountItem)
    broker = _FakeBroker()

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(500))
    ) as client:
        result = await resolve_pending_identities(
            object(),
            object(),
            fmp_key=SecretStr("synthetic-secret"),
            sec_contact="nexus@example.com",
            output_dir=tmp_path / "resolver",
            client=client,
            store=object(),
            broker=broker,
        )

    assert result == {
        "status": "resolved",
        "pending_before": 2,
        "resolved": 2,
        "remaining": 0,
        "manual_review_required": 0,
        "resolution_counts": {"common_stock": 1, "equity_etf": 1},
        "portfolio_refresh_required": True,
        "public_requests": 10,
        "broker_requests": 2,
        "execution_locked": True,
    }
    assert broker.accounts_calls == 1
    assert broker.holdings_calls == 1
