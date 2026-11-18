import gzip
import json
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime
from uuid import UUID

import httpx
import pytest
from nexus.ops import sec_published_day as published
from nexus.ops.research_monitor import parser, run_configured

DAY = date(2026, 11, 17)
MODIFIED = "Wed, 18 Nov 2026 03:01:52 GMT"
FILE = "master.20261117.idx"
SCOPE = {"rule_id": "1", "business_date": "2026-11-17"}
CONTACT = "nexus-test@example.invalid"
TARGET = "1045810|NVIDIA|8-K|20261117|edgar/data/1045810/0001045810-26-000088.txt"
OTHER = "320193|Apple|4|20261112|edgar/data/320193/0000123456-26-000001.txt"
HEADER = (
    "Description:           Daily Index of EDGAR Dissemination Feed\n"
    "Last Data Received:    Nov 17, 2026\n"
    "Comments:              webmaster@sec.gov\n"
    "Anonymous FTP:         ftp://ftp.sec.gov/edgar/\n"
    " \n"
    "CIK|Company Name|Form Type|Date Filed|File Name\n"
    "-------------------------\n"
)


def directory(*names):
    return json.dumps(
        {
            "directory": {
                "name": "daily-index/2026/QTR4/",
                "item": [
                    {
                        "name": name,
                        "type": "file",
                        "href": name,
                        "last-modified": "11/17/2026 10:01:52 PM",
                        "size": "450 KB",
                    }
                    for name in names
                ],
            }
        }
    ).encode()


def read(folder):
    return published._verify(
        folder,
        day=DAY,
        issuer_cik="0001045810",
        forms=("8-K", "8-K/A"),
        scope=SCOPE,
        now=datetime.now(UTC),
    )


def transport(
    monkeypatch,
    listing,
    index,
    *,
    modified=MODIFIED,
    length=None,
    compressed=False,
    no_footer=False,
):
    calls = []
    client = httpx.AsyncClient

    def respond(request):
        assert request.method == "GET"
        assert request.url.host == "www.sec.gov"
        assert request.url.path.startswith("/Archives/edgar/daily-index/2026/QTR4/")
        assert request.headers["accept-encoding"] == "gzip, deflate"
        name = request.url.path.rsplit("/", 1)[1]
        calls.append(name)
        if name == "index.json":
            return httpx.Response(200, content=listing)
        assert name == FILE
        body = gzip.compress(index) if compressed else index
        if no_footer:
            body = body[:-8]
        return httpx.Response(
            200,
            stream=httpx.ByteStream(body),
            headers={
                "Last-Modified": modified,
                "Content-Length": str(len(body) if length is None else length),
                "Content-Encoding": "gzip" if compressed else "identity",
            },
        )

    def configured(*, follow_redirects):
        assert not follow_redirects
        return client(transport=httpx.MockTransport(respond), follow_redirects=False)

    monkeypatch.setattr(published.httpx, "AsyncClient", configured)
    return calls


@pytest.mark.asyncio
async def test_published_version_readback_preserves_past_filing_dates_and_repeated_rows(
    tmp_path, monkeypatch
):
    body = (HEADER + "\n".join((TARGET, OTHER, OTHER))).encode()
    calls = transport(monkeypatch, directory(FILE), body)
    await published._collect(tmp_path, DAY, CONTACT, SCOPE)
    proof = read(tmp_path)
    assert proof["proof_scope"] == "published_index_version"
    assert proof["accepted_time_coverage_proven"] is False
    assert proof["rows_verified"] == 3
    assert proof["matching_rows"] == [
        {"form": "8-K", "accession": "0001045810-26-000088", "filing_date": "2026-11-17"}
    ]
    assert calls == ["index.json", FILE]

    response_path = tmp_path / "index-response.json"
    response = json.loads(response_path.read_bytes())
    assert response["framing"]["last_modified"] == MODIFIED
    response["framing"]["last_modified"] = "Wed, 18 Nov 2026 03:01:53 GMT"
    response_path.write_text(json.dumps(response))
    with pytest.raises(ValueError, match=published.INVALID):
        read(tmp_path)
    response["framing"]["last_modified"] = MODIFIED
    response_path.write_text(json.dumps(response))

    (tmp_path / (FILE + ".http-body")).write_bytes(body[:-1])
    with pytest.raises(ValueError, match="SEC_INDEX_ARCHIVE_INVALID"):
        read(tmp_path)
    assert calls == ["index.json", FILE]


@pytest.mark.asyncio
async def test_zero_matching_rows_only_proves_the_published_file(tmp_path, monkeypatch):
    transport(monkeypatch, directory(FILE), (HEADER + OTHER).encode())
    await published._collect(tmp_path, DAY, CONTACT, SCOPE)
    proof = read(tmp_path)
    assert proof["matching_rows"] == []
    assert proof["accepted_time_coverage_proven"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("listing", "modified", "length", "code", "requests"),
    [
        (directory(), MODIFIED, None, "SEC_DAY_INDEX_NOT_PUBLISHED", 1),
        (directory(FILE, FILE), MODIFIED, None, published.INVALID, 1),
        (directory(FILE), "Wed, 18 Nov 2026 03:01:53 GMT", None, published.INVALID, 2),
        (directory(FILE), MODIFIED, 100, published.INVALID, 2),
    ],
)
async def test_unpublished_mismatched_or_truncated_version_has_no_proof(
    tmp_path, monkeypatch, listing, modified, length, code, requests
):
    calls = transport(
        monkeypatch, listing, (HEADER + TARGET).encode(), modified=modified, length=length
    )
    with pytest.raises(ValueError, match=code):
        await published._collect(tmp_path, DAY, CONTACT, SCOPE)
    assert len(calls) == requests
    assert not (tmp_path / "index-response.json").exists()


@pytest.mark.asyncio
async def test_complete_http_frame_with_incomplete_gzip_stream_is_rejected(tmp_path, monkeypatch):
    body = (HEADER + TARGET).encode()
    calls = transport(monkeypatch, directory(FILE), body, compressed=True, no_footer=True)
    with pytest.raises(ValueError, match=published.INVALID):
        await published._collect(tmp_path, DAY, CONTACT, SCOPE)
    assert calls == ["index.json", FILE]
    assert not (tmp_path / "index-response.json").exists()


@pytest.mark.parametrize(
    "body",
    [
        HEADER + TARGET + "\n320193|broken",
        HEADER + TARGET + "\n" + TARGET.replace("|8-K|", "|4|"),
        HEADER.replace("Nov 17", "Nov 16") + TARGET,
        HEADER + TARGET.replace("/1045810/", "/320193/"),
    ],
)
def test_malformed_or_conflicting_rows_cannot_supply_version_proof(body):
    with pytest.raises(ValueError, match=published.INVALID):
        published._matching_rows(
            body.encode(), day=DAY, issuer_cik="0001045810", forms=("8-K", "8-K/A")
        )


def test_normal_monitor_entry_has_a_distinct_closed_day_input():
    args = parser().parse_args(
        [
            "inspect-sec-published-day",
            "--date",
            DAY.isoformat(),
            "--archive-dir",
            ".secrets/proof",
            "--rule-id",
            "a6d89a7d-39a9-47e5-ad5d-c3c4af07759b",
        ]
    )
    assert args.day == DAY
    assert args.action == "inspect-sec-published-day"


@pytest.mark.asyncio
async def test_normal_entry_binds_collection_to_one_rule_and_archives_gzip(tmp_path, monkeypatch):
    rule_id = UUID("a6d89a7d-39a9-47e5-ad5d-c3c4af07759b")
    account_id = UUID("39fa5261-6623-4b39-8812-284c8294d774")
    instrument_id = UUID("bcbe0d12-d92a-400c-83c4-e05a9960719e")
    alias_rule_id = UUID("f2862926-9b10-428d-8904-03126143ba2d")
    active = False

    class FakeSession:
        async def execute(self, query):
            assert str(query) in {"SET TRANSACTION READ ONLY", "SET LOCAL statement_timeout='5s'"}

        async def get(self, model, rule_key):
            assert model is published.SecMonitorRule
            return model(
                id=rule_key,
                account_id=account_id,
                instrument_id=instrument_id,
                issuer_cik="0001045810",
                forms=["8-K", "8-K/A"],
                effective_at=datetime(2026, 11, 17, 14, tzinfo=UTC),
            )

        async def scalar(self, statement):
            if str(statement) == "SELECT version_num FROM alembic_version":
                return published.CURRENT_MIGRATION_HEAD
            return datetime.now(UTC)

    class FakeFactory:
        @asynccontextmanager
        async def begin(self):
            nonlocal active
            active = True
            try:
                yield FakeSession()
            finally:
                active = False

        async def dispose(self):
            assert not active

        @property
        def kw(self):
            return {"bind": self}

    monkeypatch.setattr(published, "session_factory", lambda settings: FakeFactory())
    (tmp_path / ".secrets").mkdir(mode=0o700)
    calls = transport(monkeypatch, directory(FILE), (HEADER + TARGET).encode(), compressed=True)
    env = {
        "DATABASE_URL": "postgresql+asyncpg://postgres@127.0.0.1:55433/nexus",
        "NEXUS_AGENT_PAPER_ACCOUNT_ID": str(account_id),
        "SEC_CONTACT": CONTACT,
    }

    def arguments(action, identity):
        return parser().parse_args(
            [
                "--repo-root",
                str(tmp_path),
                action,
                "--rule-id",
                str(identity),
                "--date",
                DAY.isoformat(),
                "--archive-dir",
                ".secrets/published",
            ]
        )

    collected = await run_configured(env, arguments("collect-sec-published-day", rule_id))
    verified = await run_configured(env, arguments("inspect-sec-published-day", rule_id))
    assert collected["provider_requests"] == 2
    assert verified["provider_requests"] == 0
    assert collected["sha256"] == verified["sha256"]
    assert not verified["accepted_time_coverage_proven"]
    with pytest.raises(ValueError, match=published.INVALID):
        await run_configured(env, arguments("inspect-sec-published-day", alias_rule_id))
    assert calls == ["index.json", FILE]
