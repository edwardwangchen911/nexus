"""The single-company local path must reach persisted research without unrelated keys."""

import json
from decimal import Decimal
from pathlib import Path
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
