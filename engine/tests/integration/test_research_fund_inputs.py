"""Public synthetic issuer holdings stay in their exact frozen read-side scope."""

import hashlib
from datetime import timedelta
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import app, headers
from helpers.research_snapshots import INSTRUMENT, NOW, seed_filing
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.api.research_inputs import read_research_inputs
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.session import session_factory
from nexus.research.ssga_fund_observations import parse_fund_observation, persist_fund_observation
from nexus.settings import EngineSettings
from sqlalchemy import event, text
from sqlalchemy.engine import Engine

from engine.tests.unit.research.test_ssga_fund_observations import originals

RECEIPT = NOW + timedelta(minutes=1)
DECISION = NOW + timedelta(minutes=2)


async def _seed(session, *, weight="-0.125"):
    html, workbook = originals(weight=weight)
    document = parse_fund_observation(
        html=html, workbook=workbook, instrument_id=INSTRUMENT, symbol="TEST",
        html_url="https://www.ssga.com/us/en/individual/etfs/synthetic-test",
        workbook_url="https://www.ssga.com/library-content/products/fund-data/etfs/us/test.xlsx",
        html_received_at=NOW, workbook_received_at=RECEIPT,
    )
    for raw, receipt in ((html, NOW), (workbook, RECEIPT)):
        digest = hashlib.sha256(raw).hexdigest()
        session.add(DataArtifact(
            uri="memory://sha256/" + digest, sha256=digest, byte_size=len(raw),
            mime_type="application/octet-stream", schema_version="issuer_raw_v1", source="ssga",
            observed_at=receipt, available_at=receipt, received_at=receipt,
        ))
    await session.flush()
    return await persist_fund_observation(
        session, document,
        location="memory://sha256/" + hashlib.sha256(document.canonical_bytes()).hexdigest(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("include_sec", [False, True])
async def test_fund_only_and_mixed_frozen_inputs_reach_get_without_business_writes(
    migrated_database_url, monkeypatch, include_sec,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await seed_filing(session, source="sec" if include_sec else "synthetic")
        old = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        evidence = await _seed(session)
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=DECISION)
        assert frozen is not None
        result = await read_research_inputs(
            session, snapshot_id=frozen.id, instrument_id=INSTRUMENT, as_of=DECISION
        )
        assert result is not None
        observation = result.issuer_fund_observations[0]
        assert observation.evidence_ref_id == evidence.id
        assert observation.industries[0].weight_percent == "100.00"
        assert observation.holdings[0].weight_percent == "-0.125"
        assert observation.holdings[1].weight_percent == "0"
        assert observation.holdings[0].classification is None
        assert observation.denominator is observation.source_published_at is None
        assert observation.valid_until is observation.residual_classification is None
        assert observation.html_received_at == NOW
        assert observation.workbook_received_at == observation.available_at == RECEIPT
        assert not observation.historical_backtest_eligible and not observation.sector_risk_eligible
        assert observation.total_industries == 1 and observation.total_holdings == 2
        assert not observation.truncated and observation.weight_unit == "percent"
        assert len(result.filings) == int(include_sec)
        assert result.issuer_distributions == ()
        earlier = await read_research_inputs(
            session, snapshot_id=frozen.id, instrument_id=INSTRUMENT, as_of=NOW
        )
        assert earlier is None or earlier.issuer_fund_observations == ()
        wrong = await read_research_inputs(
            session, snapshot_id=frozen.id, instrument_id=UUID(int=999), as_of=DECISION
        )
        assert wrong is None
        if old is not None:
            unchanged = await read_research_inputs(
                session, snapshot_id=old.id, instrument_id=INSTRUMENT, as_of=DECISION
            )
            assert unchanged is not None and unchanged.issuer_fund_observations == ()

    async def forbidden(*args, **kwargs):
        pytest.fail("research inputs GET must not fetch external data")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", forbidden)
    writes = []

    def observe(connection, cursor, statement, parameters, context, many):
        if statement.lstrip().split()[0].upper() in {"INSERT", "UPDATE", "DELETE", "MERGE"}:
            writes.append(statement.lstrip())

    event.listen(Engine, "before_cursor_execute", observe)
    try:
        path = (f"/internal/v1/instruments/{INSTRUMENT}/research-admission"
                "?as_of=2026-09-02T04%3A02%3A00Z")
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(migrated_database_url, now=DECISION)),
            base_url="http://test",
        ) as client:
            response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200
        inputs = response.json()["data"]["inputs"]
        assert inputs["issuer_fund_observations"][0]["holdings"][0]["weight_percent"] == "-0.125"
        assert len(inputs["filings"]) == int(include_sec)
        assert len(writes) == 1 and writes[0].startswith("INSERT INTO used_nonces ")
    finally:
        event.remove(Engine, "before_cursor_execute", observe)


@pytest.mark.asyncio
@pytest.mark.parametrize("weight", ["malformed", "99"])
async def test_invalid_optional_fund_payload_does_not_hide_valid_sec(
    migrated_database_url, weight,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await seed_filing(session)
        evidence = await _seed(session)
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=DECISION)
        assert frozen is not None
        await session.execute(text(
            "UPDATE data_snapshots SET payload=jsonb_set(payload, '{holdings,0,weight_percent}',"
            " to_jsonb(CAST(:weight AS text))) WHERE source='ssga_fund_observation'"
            " AND revision=:revision"
        ), {"weight": weight, "revision": evidence.revision})
        result = await read_research_inputs(
            session, snapshot_id=frozen.id, instrument_id=INSTRUMENT, as_of=DECISION
        )
        assert result is not None and len(result.filings) == 1
        assert result.issuer_fund_observations == ()


@pytest.mark.asyncio
async def test_valid_optional_fund_decimal_too_long_for_wire_does_not_hide_sec(
    migrated_database_url,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await seed_filing(session)
        await _seed(session, weight="0." + "0" * 39 + "1")
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=DECISION)
        assert frozen is not None
        result = await read_research_inputs(
            session, snapshot_id=frozen.id, instrument_id=INSTRUMENT, as_of=DECISION
        )
        assert result is not None and len(result.filings) == 1
        assert result.issuer_fund_observations == ()
