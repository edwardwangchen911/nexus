"""The single-company local path must reach persisted research without unrelated keys."""

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord, AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.research import Filing, FinancialFactRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures/sec"
INSTRUMENT = UUID("0198d500-1000-7000-8000-000000009001")


def disclosure_responder(requests, *, failure=None):
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
                            "form": ["20-F" if failure == "unsupported" else "6-K"],
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
    requests, *, wrong_issuer=False, reject=False, ambiguous=False, wrong_submission=False
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
        else:
            assert request.url.path == "/submissions/CIK0000320193.json"
            payload = json.loads((FIXTURES / "submissions.json").read_bytes())
            payload.update({"cik": "0000320193", "tickers": ["AAPL"], "exchanges": ["Nasdaq"]})
            if wrong_submission:
                payload["tickers"] = ["MSFT"]
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
async def test_disclosure_reaches_sdk_api_and_portfolio_without_fabricated_facts(
    migrated_database_url,
    monkeypatch,
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
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(disclosure_responder(requests))
    ) as client:
        first = await refresh_sec_research(
            settings,
            INSTRUMENT,
            contact="test@example.invalid",
            http_client=client,
            store=store,
            disclosure_only=True,
        )
        replay = await refresh_sec_research(
            settings,
            INSTRUMENT,
            contact="test@example.invalid",
            http_client=client,
            store=store,
            disclosure_only=True,
        )
    assert first["provider_requests"] == replay["provider_requests"] == 3
    assert first["model_requests"] == 0 and first["execution_locked"] is True
    assert first["filings_created"] == replay["filings_replayed"] == 1
    assert replay["filings_created"] == 0
    assert len(requests) == 6 and not any("companyfacts" in str(item.url) for item in requests)
    assert any(key.endswith(".html") for key in store.objects)
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
        assert filing["form"] == "6-K" and filing["disclosure_truncated"] is False
        assert filing["disclosure_excerpt"] == "Company has not decided on the reported facility."
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
                                        "text": "披露称未决定扩产；缺少财务指标和合格行情。",
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
@pytest.mark.parametrize("failure", ["path", "future", "naive", "date", "unsupported", "redirect"])
async def test_disclosure_error_stops_before_persisting_or_retrying(migrated_database_url, failure):
    from nexus.ops.sec_local import refresh_sec_research

    settings = EngineSettings(database_url=migrated_database_url)
    await seed_instrument(settings)
    requests = []
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(disclosure_responder(requests, failure=failure))
    ) as client:
        with pytest.raises((ValueError, RuntimeError)):
            await refresh_sec_research(
                settings,
                INSTRUMENT,
                contact="test@example.invalid",
                http_client=client,
                store=InMemoryRawEvidenceStore(),
                disclosure_only=True,
            )
    assert len(requests) == (3 if failure == "redirect" else 2)
    async with session_factory(settings)() as session:
        assert await session.scalar(select(func.count()).select_from(Filing)) == 0
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 0
