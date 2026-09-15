"""The single-company local path must reach persisted research without unrelated keys."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord, AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.research import Filing, FinancialFactRecord, ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures/sec"
INSTRUMENT = UUID("0198d500-1000-7000-8000-000000009001")


def disclosure_responder(requests, *, failure=None, form="6-K"):
    base = sec_responder(requests)

    def respond(request):
        if request.url.path == "/files/company_tickers_exchange.json":
            return base(request)
        requests.append(request)
        assert request.method == "GET"
        if request.url.path == "/submissions/CIK0000320193.json":
            return httpx.Response(
                200,
                json={
                    "cik": "0000320193",
                    "tickers": ["AAPL"],
                    "exchanges": ["Nasdaq"],
                    "filings": {
                        "recent": {
                            "accessionNumber": ["0000320193-26-000001"],
                            "acceptanceDateTime": [
                                "2099-01-01T20:00:00Z"
                                if failure == "future"
                                else "2026-08-21T20:00:00"
                                if failure == "naive"
                                else "2026-08-21T20:00:00Z"
                            ],
                            "filingDate": ["invalid" if failure == "date" else "2026-08-21"],
                            "form": ["20-F" if failure == "unsupported" else form],
                            "primaryDocument": [
                                "../secret.htm" if failure == "path" else "doc.htm"
                            ],
                        }
                    },
                },
            )
        assert request.url.host == "www.sec.gov"
        assert request.url.path == "/Archives/edgar/data/320193/000032019326000001/doc.htm"
        if failure == "redirect":
            return httpx.Response(302, headers={"Location": "https://other.invalid/"})
        return httpx.Response(
            200,
            content=(
                b"<html><body><script>hidden</script>"
                b"<p>Company has not decided on the reported facility.</p></body></html>"
            ),
        )

    return respond


def fund_responder(requests, *, failure=None):
    original = disclosure_responder([])

    def respond(request):
        requests.append(request)
        assert request.method == "GET"
        if request.url.path == "/files/company_tickers_mf.json":
            row = [320193, "S000000001", "C000000001", "AAPL"]
            return httpx.Response(
                200,
                json={
                    "fields": ["cik", "seriesId", "classId", "symbol"],
                    "data": [row, row] if failure == "ambiguous" else [row],
                },
            )
        if request.url.path == "/submissions/CIK0000320193.json":
            payload = original(request).json()
            payload["filings"]["recent"]["form"] = ["6-K" if failure == "form" else "497K"]
            return httpx.Response(200, json=payload)
        if request.url.path.endswith("-index.html"):
            series = "S000000002" if failure == "series" else "S000000001"
            share_class = "C000000002" if failure == "class" else "C000000001"
            extra_class = " C000000003" if failure == "multiple" else ""
            filename = "other.htm" if failure == "document_link" else "doc.htm"
            return httpx.Response(
                200,
                text=(
                    f"<p>Form 497K {series} {share_class}{extra_class} AAPL</p>"
                    f'<a href="/Archives/edgar/data/320193/000032019326000001/{filename}">'
                    "Primary disclosure</a>"
                ),
            )
        assert request.url.path == "/Archives/edgar/data/320193/000032019326000001/doc.htm"
        return httpx.Response(
            200,
            text="<p>"
            + ("OTHER" if failure == "body" else "AAPL")
            + " fund tracks a sector index; concentration risk applies.</p>",
        )

    return respond


class InMemoryRawEvidenceStore:
    def __init__(self):
        self.objects = {}

    async def put_if_absent(self, *, key, content):
        assert self.objects.setdefault(key, content) == content
        return f"memory://sha256/{key.rsplit('/', 1)[-1]}"


async def seed_instrument(settings, *, asset_type="common_stock", exchange="XNAS"):
    from datetime import date
    from decimal import Decimal

    async with session_factory(settings)() as session, session.begin():
        session.add(
            Instrument(
                id=INSTRUMENT,
                asset_type=asset_type,
                exchange=exchange,
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="us_equity",
            )
        )
        await session.flush()
        session.add(
            InstrumentAlias(
                instrument_id=INSTRUMENT,
                alias_type="ticker",
                symbol="AAPL",
                valid_from=date(2020, 1, 1),
                valid_to=None,
            )
        )


def sec_responder(
    requests,
    *,
    wrong_issuer=False,
    reject=False,
    ambiguous=False,
    wrong_submission=False,
    annual_history=False,
):
    def respond(request):
        requests.append(request)
        assert request.method == "GET"
        assert request.headers["User-Agent"] == "NEXUS test@example.invalid"
        if reject:
            return httpx.Response(429)
        if request.url.host == "www.sec.gov":
            assert request.url.path == "/files/company_tickers_exchange.json"
            return httpx.Response(
                200,
                json={
                    "fields": ["cik", "name", "ticker", "exchange"],
                    "data": [[320193, "Apple Inc.", "AAPL", "Nasdaq"]]
                    + ([[42, "Different company", "AAPL", "NYSE"]] if ambiguous else []),
                },
            )
        assert request.url.host == "data.sec.gov"
        if request.url.path == "/api/xbrl/companyfacts/CIK0000320193.json":
            payload = json.loads((FIXTURES / "companyfacts.json").read_bytes())
            payload["cik"] = 42 if wrong_issuer else 320193
            payload["facts"]["us-gaap"]["EarningsPerShareDiluted"]["units"]["USD/shares"][0][
                "val"
            ] = 1.25
            if annual_history:
                for tagged in payload["facts"]["us-gaap"].values():
                    for entries in tagged["units"].values():
                        latest = entries[0]
                        for accession, fiscal_year, period_start, period_end in (
                            ("0000320193-25-000079", 2025, "2024-09-29", "2025-09-27"),
                            ("0000320193-24-000123", 2024, "2023-10-01", "2024-09-28"),
                        ):
                            annual = dict(latest)
                            annual.update(
                                {
                                    "accn": accession,
                                    "fy": fiscal_year,
                                    "fp": "FY",
                                    "form": "10-K",
                                    "filed": f"{fiscal_year}-10-31",
                                    "end": period_end,
                                    "frame": f"CY{fiscal_year}",
                                }
                            )
                            if annual.get("start") is not None:
                                annual["start"] = period_start
                            entries.append(annual)
        else:
            assert request.url.path == "/submissions/CIK0000320193.json"
            payload = json.loads((FIXTURES / "submissions.json").read_bytes())
            payload.update({"cik": "0000320193", "tickers": ["AAPL"], "exchanges": ["Nasdaq"]})
            if wrong_submission:
                payload["tickers"] = ["MSFT"]
            if annual_history:
                recent = payload["filings"]["recent"]
                for key, values in {
                    "accessionNumber": ["0000320193-25-000079", "0000320193-24-000123"],
                    "acceptanceDateTime": [
                        "2025-10-31T10:01:26Z",
                        "2024-11-01T10:01:26Z",
                    ],
                    "filingDate": ["2025-10-31", "2024-11-01"],
                    "form": ["10-K", "10-K"],
                }.items():
                    recent[key].extend(values)
            # Real submissions also contain non-financial forms with no Company Facts.
            recent = payload["filings"]["recent"]
            for key, value in {
                "accessionNumber": "0000320193-26-000002",
                "acceptanceDateTime": "2026-08-21T20:00:00Z",
                "filingDate": "2026-08-21",
                "form": "4",
            }.items():
                recent[key].append(value)
        return httpx.Response(200, json=payload)

    return respond


@pytest.mark.asyncio
@pytest.mark.parametrize(("asset_type", "exchange"), [("common_stock", "XNYS"), ("equity", "XNAS")])
async def test_sec_only_refresh_archives_real_shape_and_freezes_inputs(
    migrated_database_url, asset_type, exchange
):
    from nexus.ops.sec_local import refresh_sec_research

    settings = EngineSettings(database_url=migrated_database_url)
    from nexus.ops.openai_local import prepare_database

    await prepare_database(settings)
    await seed_instrument(settings, asset_type=asset_type, exchange=exchange)
    requests = []
    store = InMemoryRawEvidenceStore()
    async with httpx.AsyncClient(transport=httpx.MockTransport(sec_responder(requests))) as client:
        first = await refresh_sec_research(
            settings,
            INSTRUMENT,
            contact="test@example.invalid",
            http_client=client,
            store=store,
        )
        assert first["status"] == "RESEARCH_INPUTS_PREPARED"
        assert first["provider_requests"] == 3
        assert first["model_requests"] == 0
        assert first["execution_locked"] is True
        assert first["filings_created"] == 1
        assert first["instrument_exchange"] == exchange
        assert first["sec_exchange"] == "XNAS"
        second = await refresh_sec_research(
            settings,
            INSTRUMENT,
            contact="test@example.invalid",
            http_client=client,
            store=store,
        )
        assert second["filings_created"] == 0
        assert second["filings_replayed"] == 1
    assert len(requests) == 6
    async with session_factory(settings)() as session:
        assert await session.scalar(select(func.count()).select_from(Filing)) == 1
        assert await session.scalar(select(func.count()).select_from(FinancialFactRecord)) == 11
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 2
        assert (await session.get(Instrument, INSTRUMENT)).exchange == exchange
        assert (
            await session.scalar(select(func.count()).select_from(AgentDataSnapshotEvidenceRecord))
            == 2
        )
        facts = list(await session.scalars(select(FinancialFactRecord)))
        assert {item.accession_number for item in facts} == {"0000320193-26-000001"}
        assert next(
            item.value for item in facts if item.canonical_fact == "diluted_eps"
        ) == Decimal("1.25")

    # The returned link uses the existing authenticated admission/BFF contract.
    from datetime import UTC, datetime

    from helpers.portfolio_api import app, headers

    path = (
        f"/internal/v1/instruments/{INSTRUMENT}/research-admission?"
        + first["research_path"].split("?", 1)[1]
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=datetime.now(UTC))),
        base_url="http://testserver",
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200
    assert response.json()["data"]["eligible"] is True
    assert response.json()["data"]["data_snapshot_id"] == first["data_snapshot_id"]
    inputs = response.json()["data"]["inputs"]
    assert inputs["display_symbol"] == "AAPL"
    assert len(inputs["filings"]) == 1
    assert inputs["filings"][0]["source"] == "sec"
    assert inputs["filings"][0]["form"] == "10-Q"
    assert inputs["total_facts"] == len(inputs["facts"]) == 11
    assert inputs["truncated"] is False
    eps = next(fact for fact in inputs["facts"] if fact["fact"] == "diluted_eps")
    assert Decimal(eps["value"]) == Decimal("1.25")
    assert eps["period_start"] is not None
    assert eps["evidence_ref_id"] == inputs["filings"][0]["evidence_ref_id"]


@pytest.mark.asyncio
async def test_sec_only_refresh_freezes_latest_annual_and_quarterly_filings(
    migrated_database_url,
):
    from nexus.ops.openai_local import prepare_database
    from nexus.ops.sec_local import refresh_sec_research

    settings = EngineSettings(database_url=migrated_database_url)
    await prepare_database(settings)
    await seed_instrument(settings)
    requests = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(sec_responder(requests, annual_history=True))
    ) as client:
        result = await refresh_sec_research(
            settings,
            INSTRUMENT,
            contact="test@example.invalid",
            http_client=client,
            store=InMemoryRawEvidenceStore(),
        )

    assert result["filings_created"] == 2
    async with session_factory(settings)() as session:
        filings = list(await session.scalars(select(Filing).order_by(Filing.accepted_at)))
        assert [(row.accession_number, row.form) for row in filings] == [
            ("0000320193-25-000079", "10-K"),
            ("0000320193-26-000001", "10-Q"),
        ]
        assert {
            row.accession_number
            for row in await session.scalars(select(FinancialFactRecord))
        } == {"0000320193-25-000079", "0000320193-26-000001"}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["contact", "rate_limit", "issuer", "ambiguous", "submission"])
async def test_sec_only_refresh_stops_without_fake_research(migrated_database_url, failure):
    from nexus.ops.sec_local import refresh_sec_research

    settings = EngineSettings(database_url=migrated_database_url)
    await seed_instrument(settings)
    requests = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            sec_responder(
                requests,
                wrong_issuer=failure == "issuer",
                reject=failure == "rate_limit",
                ambiguous=failure == "ambiguous",
                wrong_submission=failure == "submission",
            )
        )
    ) as client:
        with pytest.raises((ValueError, RuntimeError)):
            await refresh_sec_research(
                settings,
                INSTRUMENT,
                contact=None if failure == "contact" else "test@example.invalid",
                http_client=client,
                store=InMemoryRawEvidenceStore(),
            )
    assert (
        len(requests)
        == {"contact": 0, "rate_limit": 1, "issuer": 3, "ambiguous": 1, "submission": 3}[failure]
    )
    async with session_factory(settings)() as session:
        assert await session.scalar(select(func.count()).select_from(Filing)) == 0
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 0


@pytest.mark.asyncio
async def test_sec_issuer_mismatch_archives_raw_response_before_validation(
    migrated_database_url,
):
    """Archival must precede identity validation so a rejected response remains diagnosable."""
    from nexus.ops.sec_local import refresh_sec_research
    from nexus.providers.sec import SecProviderError

    settings = EngineSettings(database_url=migrated_database_url)
    await seed_instrument(settings)
    requests = []
    store = InMemoryRawEvidenceStore()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(sec_responder(requests, wrong_submission=True))
    ) as client:
        with pytest.raises(SecProviderError, match="SEC request retry budget exhausted"):
            await refresh_sec_research(
                settings,
                INSTRUMENT,
                contact="test@example.invalid",
                http_client=client,
                store=store,
            )

    archived = [content for key, content in store.objects.items() if "/submissions/" in key]
    assert len(archived) == 1
    assert json.loads(archived[0])["tickers"] == ["MSFT"]
    assert len(requests) == 3
    async with session_factory(settings)() as session:
        assert await session.scalar(select(func.count()).select_from(Filing)) == 0
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("form", ["6-K", "497K", "8-K"])
async def test_disclosure_reaches_sdk_api_and_portfolio_without_fabricated_facts(
    migrated_database_url,
    monkeypatch,
    form,
):
    from helpers.portfolio_api import app, headers
    from nexus.agents import codex_sdk
    from nexus.agents.committee import ROLE_ORDER
    from nexus.agents.responses_budget import UsageSnapshot
    from nexus.ops.openai_local import prepare_database
    from nexus.ops.sec_local import refresh_sec_research
    from nexus.portfolio.research import read_position_research
    from nexus.research.sec_disclosures import SCHEMA_VERSION, read_disclosures
    from nexus.worker import run_agent_worker_once

    settings = EngineSettings(database_url=migrated_database_url, agent_runtime_mode="codex_sdk")
    await prepare_database(settings)
    await seed_instrument(settings)
    requests = []
    store = InMemoryRawEvidenceStore()
    options = (
        {"fund_accession": "0000320193-26-000001"} if form == "497K" else {"disclosure_only": True}
    )
    if form == "8-K":
        options["disclosure_form"] = "8-K"
    request_limit = 4 if form == "497K" else 3
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            fund_responder(requests)
            if form == "497K"
            else disclosure_responder(requests, form=form)
        )
    ) as client:
        first = await refresh_sec_research(
            settings,
            INSTRUMENT,
            contact="test@example.invalid",
            http_client=client,
            store=store,
            **options,
        )
        replay = await refresh_sec_research(
            settings,
            INSTRUMENT,
            contact="test@example.invalid",
            http_client=client,
            store=store,
            **options,
        )
    assert first["provider_requests"] == replay["provider_requests"] == request_limit
    assert first["model_requests"] == 0 and first["execution_locked"] is True
    assert first["filings_created"] == replay["filings_replayed"] == 1
    assert replay["filings_created"] == 0
    assert len(requests) == request_limit * 2 and not any(
        "companyfacts" in str(item.url) for item in requests
    )
    assert any("/disclosure/" in key for key in store.objects)
    if form == "497K":
        assert first["sec_exchange"] is None
        assert any("/filing-index/" in key for key in store.objects)
    now = datetime.now(UTC)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=now)),
        base_url="http://testserver",
    ) as client:
        path = (
            f"/internal/v1/instruments/{INSTRUMENT}/research-admission?"
            + first["research_path"].split("?", 1)[1]
        )
        response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200
        admission = response.json()["data"]
        assert admission["eligible"] is True
        assert admission["inputs"]["facts"] == [] and admission["inputs"]["total_facts"] == 0
        filing = admission["inputs"]["filings"][0]
        assert filing["form"] == form and filing["disclosure_truncated"] is False
        assert filing["disclosure_excerpt"] == (
            "AAPL fund tracks a sector index; concentration risk applies."
            if form == "497K"
            else "Company has not decided on the reported facility."
        )
        assert filing["source_url"].endswith("/000032019326000001/doc.htm")
        body = json.dumps(
            {
                "instrument_id": str(INSTRUMENT),
                "data_snapshot_id": first["data_snapshot_id"],
                "decision_time": admission["decision_time"],
            }
        ).encode()
        path = "/internal/v1/research-runs"
        submitted = await client.post(
            path,
            content=body,
            headers={**headers("POST", path, body), "Idempotency-Key": "disclosure-research"},
        )
        assert submitted.status_code == 202
        run_id = submitted.json()["data"]["run_id"]
        invoke = AsyncMock(
            return_value=(
                json.dumps(
                    {
                        "roles": [
                            {
                                "role": role,
                                "vote": "abstain",
                                "claims": [
                                    {
                                        "text": "冻结文本资料已核验；缺少合格行情。",
                                        "citations": [0],
                                    }
                                ],
                                "conflicting_facts": False,
                            }
                            for role in ROLE_ORDER
                        ]
                    }
                ),
                UsageSnapshot(input_tokens=300, output_tokens=80, provider_calls=1),
            )
        )
        monkeypatch.setattr(codex_sdk, "invoke_codex", invoke)
        assert await run_agent_worker_once(settings) is True
        invoke.assert_awaited_once()
        frozen = invoke.call_args.args[0]["frozen_inputs"]
        assert frozen["financial_facts"]["items"] == []
        if form == "497K":
            assert (
                frozen["filing_disclosures"]["items"][0]["disclosure"]["fund_identity"]["class_id"]
                == "C000000001"
            )
        assert (
            frozen["filing_disclosures"]["items"][0]["disclosure"]["excerpt"]
            == filing["disclosure_excerpt"]
        )
        path = f"/internal/v1/research-runs/{run_id}"
        result = (await client.get(path, headers=headers("GET", path))).json()["data"]
        assert result["status"] == "completed" and result["quality_status"] == "partial"
        assert result["intent"]["exposure_action"] == "none"
    async with session_factory(settings)() as session:
        assert await session.scalar(select(func.count()).select_from(FinancialFactRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(Filing)) == 1
        assert (await session.get(AgentResearchRunRecord, UUID(run_id))).retry_count == 0
        assert (await read_position_research(session, (INSTRUMENT,), read_at=datetime.now(UTC)))[
            INSTRUMENT
        ].status == "completed"
        evidence_id = UUID(filing["evidence_ref_id"])
        source = await session.scalar(
            select(DataSnapshot).where(DataSnapshot.schema_version == SCHEMA_VERSION)
        )
        if form == "8-K":
            from nexus.agents.snapshots import prepare_research_snapshot

            evidence = await session.get(ResearchEvidenceRef, evidence_id)
            assert evidence.available_at == source.available_at == source.received_at
            assert source.observed_at < source.received_at
            assert datetime.fromisoformat(filing["received_at"]) == source.received_at
            assert (
                await prepare_research_snapshot(
                    session, INSTRUMENT, as_of=source.received_at - timedelta(microseconds=1)
                )
                is None
            )
        assert (
            await read_disclosures(
                session,
                instrument_id=INSTRUMENT,
                evidence_ids=(evidence_id,),
                as_of=source.received_at - timedelta(seconds=1),
            )
            == {}
        )
        assert (
            await read_disclosures(
                session, instrument_id=UUID(int=42), evidence_ids=(evidence_id,), as_of=now
            )
            == {}
        )
        # In-memory corruption only; immutable persisted evidence is never rewritten.
        source.payload = {**source.payload, "excerpt": "Altered disclosure must not be trusted."}
        with session.no_autoflush:
            assert (
                await read_disclosures(
                    session, instrument_id=INSTRUMENT, evidence_ids=(evidence_id,), as_of=now
                )
                == {}
            )
    assert await run_agent_worker_once(settings) is False


@pytest.mark.asyncio
async def test_8k_revision_preserves_old_frozen_text_and_receipt(migrated_database_url):
    from nexus.agents.snapshots import prepare_research_snapshot
    from nexus.api.research_inputs import read_research_inputs
    from nexus.research.sec_disclosures import SecDisclosure, persist_disclosure

    settings = EngineSettings(database_url=migrated_database_url)
    await seed_instrument(settings)
    accepted = datetime(2026, 8, 21, 20, tzinfo=UTC)
    received = accepted + timedelta(days=1)
    document = SecDisclosure(
        instrument_id=INSTRUMENT,
        issuer_cik="0000320193",
        form="8-K",
        accession_number="0000320193-26-000001",
        source_url="https://www.sec.gov/Archives/edgar/data/320193/000032019326000001/doc.htm",
        filed_at=accepted.replace(hour=0),
        accepted_at=accepted,
        raw_content_hash="a" * 64,
        excerpt="Original operating update for investors.",
        truncated=False,
    )
    factory = session_factory(settings)
    location = "memory://sha256/" + sha256(document.canonical_bytes()).hexdigest()
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="RECEIPT_TIME_INVALID"):
            await persist_disclosure(
                session, document, location=location, received_at=accepted - timedelta(seconds=1)
            )
        assert await persist_disclosure(session, document, location=location, received_at=received)
        old = await prepare_research_snapshot(session, INSTRUMENT, as_of=received)
        assert old is not None
        assert not await persist_disclosure(
            session, document, location=location, received_at=received + timedelta(seconds=1)
        )
        revised = document.model_copy(
            update={
                "raw_content_hash": "b" * 64,
                "excerpt": "Revised operating update for investors.",
            }
        )
        later = received + timedelta(hours=1)
        revised_location = "memory://sha256/" + sha256(revised.canonical_bytes()).hexdigest()
        assert await persist_disclosure(
            session, revised, location=revised_location, received_at=later
        )
        current = await prepare_research_snapshot(session, INSTRUMENT, as_of=later)
        original = await read_research_inputs(
            session, snapshot_id=old.id, instrument_id=INSTRUMENT, as_of=later
        )
        latest = await read_research_inputs(
            session, snapshot_id=current.id, instrument_id=INSTRUMENT, as_of=later
        )
        assert [f.disclosure_excerpt for f in original.filings] == [document.excerpt]
        assert {f.disclosure_excerpt for f in latest.filings} == {document.excerpt, revised.excerpt}
        assert await session.scalar(select(func.count()).select_from(Filing)) == 2
        assert sorted(await session.scalars(select(ResearchEvidenceRef.available_at))) == [
            received,
            later,
        ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("amended_at", "blocked"),
    [
        ("2026-08-21T21:00:00Z", True),
        ("2026-08-21T20:00:00Z", True),
        ("2026-08-21T19:00:00Z", False),
        ("unknown", True),
        ("2026-08-21T21:00:00", True),
        ("2099-01-01T00:00:00Z", True),
    ],
)
async def test_8k_does_not_hide_a_newer_or_undated_amendment(
    migrated_database_url, amended_at, blocked
):
    from nexus.ops.sec_local import refresh_sec_research

    settings = EngineSettings(database_url=migrated_database_url)
    await seed_instrument(settings)
    requests = []
    original = disclosure_responder(requests, form="8-K")

    def respond(request):
        response = original(request)
        if request.url.path == "/submissions/CIK0000320193.json":
            payload = response.json()
            recent = payload["filings"]["recent"]
            for key, value in {
                "accessionNumber": "0000320193-26-000002",
                "acceptanceDateTime": amended_at,
                "filingDate": "2026-08-21",
                "form": "8-K/A",
                "primaryDocument": "amendment.htm",
            }.items():
                recent[key].append(value)
            return httpx.Response(200, json=payload)
        return response

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        call = refresh_sec_research(
            settings,
            INSTRUMENT,
            contact="test@example.invalid",
            http_client=client,
            store=InMemoryRawEvidenceStore(),
            disclosure_only=True,
            disclosure_form="8-K",
        )
        if blocked:
            with pytest.raises(ValueError, match="AMENDMENT_UNSUPPORTED|ACCEPTANCE_TIME_INVALID"):
                await call
        else:
            assert (await call)["filings_created"] == 1
    assert len(requests) == (2 if blocked else 3)
    async with session_factory(settings)() as session:
        assert await session.scalar(select(func.count()).select_from(Filing)) == int(not blocked)


@pytest.mark.asyncio
@pytest.mark.parametrize("form", ["6-K", "8-K"])
@pytest.mark.parametrize("failure", ["path", "future", "naive", "date", "unsupported", "redirect"])
async def test_disclosure_error_stops_before_persisting_or_retrying(
    migrated_database_url, failure, form
):
    from nexus.ops.sec_local import refresh_sec_research

    settings = EngineSettings(database_url=migrated_database_url)
    await seed_instrument(settings)
    requests = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(disclosure_responder(requests, failure=failure, form=form))
    ) as client:
        with pytest.raises((ValueError, RuntimeError)):
            await refresh_sec_research(
                settings,
                INSTRUMENT,
                contact="test@example.invalid",
                http_client=client,
                store=InMemoryRawEvidenceStore(),
                disclosure_only=True,
                disclosure_form=form,
            )
    assert len(requests) == (3 if failure == "redirect" else 2)
    async with session_factory(settings)() as session:
        assert await session.scalar(select(func.count()).select_from(Filing)) == 0
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["ambiguous", "form", "series", "class", "multiple", "document_link", "body"]
)
async def test_fund_identity_mismatch_never_becomes_research(migrated_database_url, failure):
    from nexus.ops.sec_local import refresh_sec_research

    settings = EngineSettings(database_url=migrated_database_url)
    await seed_instrument(settings)
    requests = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(fund_responder(requests, failure=failure))
    ) as client:
        with pytest.raises((ValueError, RuntimeError)):
            await refresh_sec_research(
                settings,
                INSTRUMENT,
                contact="test@example.invalid",
                http_client=client,
                store=InMemoryRawEvidenceStore(),
                fund_accession="0000320193-26-000001",
            )
    assert len(requests) == {"ambiguous": 1, "form": 2, "body": 4}.get(failure, 3)
    async with session_factory(settings)() as session:
        assert await session.scalar(select(func.count()).select_from(Filing)) == 0
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 0
