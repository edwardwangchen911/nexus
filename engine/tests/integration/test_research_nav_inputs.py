"""Archived synthetic NAV reaches a bounded exact-snapshot research display."""

import hashlib
from datetime import date, timedelta
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import app, headers
from helpers.research_snapshots import INSTRUMENT, NOW, seed_filing
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.api.research_inputs import read_research_inputs
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.session import session_factory
from nexus.research.ssga_nav_history import parse_nav_history
from nexus.research.ssga_nav_storage import persist_nav_history
from nexus.settings import EngineSettings
from sqlalchemy import event, text
from sqlalchemy.engine import Engine

from engine.tests.unit.research.test_ssga_nav_history import originals

RECEIPT = NOW + timedelta(minutes=1)
DECISION = NOW + timedelta(minutes=2)


async def _seed(session, *, nav="12.345678901234567"):
    rows = [
        [(date(2026, 7, 1) + timedelta(days=i)).strftime("%d-%b-%Y"), nav,
         "0" if i == 44 else "", "2.5E+6" if i == 44 else ""]
        for i in reversed(range(45))
    ]
    html, workbook = originals(rows=rows)
    document = parse_nav_history(
        html=html, workbook=workbook, instrument_id=INSTRUMENT, symbol="TEST",
        html_url="https://www.ssga.com/us/en/individual/etfs/synthetic-test",
        workbook_url="https://www.ssga.com/library-content/products/fund-data/etfs/us/nav-test.xlsx",
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
    return await persist_nav_history(
        session, document,
        location="memory://sha256/" + hashlib.sha256(document.canonical_bytes()).hexdigest(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("include_sec", [False, True])
async def test_nav_display_is_exact_bounded_and_get_has_no_business_writes(
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
        history = result.issuer_nav_histories[0]
        assert history.evidence_ref_id == evidence.id
        assert history.currency == "USD" and history.nav_unit == "currency_per_share"
        assert history.total_observations == 45 and len(history.observations) == 32
        assert history.truncated and history.coverage_start == date(2026, 7, 1)
        assert history.coverage_end == date(2026, 8, 14)
        assert history.observations[0].date == date(2026, 7, 14)
        assert any(row.date.weekday() >= 5 for row in history.observations)
        assert history.observations[-1].nav == "12.345678901234567"
        assert history.observations[-1].shares_outstanding == "0"
        assert history.observations[-1].total_net_assets == "2500000"
        assert history.observations[0].shares_outstanding is None
        assert history.html_received_at == NOW and history.available_at == RECEIPT
        assert history.source_published_at is history.reinvestment_method is None
        assert not history.market_price_eligible and not history.total_return_eligible
        assert not history.historical_backtest_eligible
        assert len(result.filings) == int(include_sec)
        checks = [(frozen.id, INSTRUMENT, NOW), (frozen.id, UUID(int=999), DECISION)]
        if old is not None:
            checks.append((old.id, INSTRUMENT, DECISION))
        for snapshot_id, instrument_id, cutoff in checks:
            other = await read_research_inputs(
                session, snapshot_id=snapshot_id, instrument_id=instrument_id, as_of=cutoff
            )
            assert other is None or other.issuer_nav_histories == ()

    async def forbidden(*args, **kwargs):
        pytest.fail("NAV inputs GET must not fetch external data")

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
        assert len(inputs["issuer_nav_histories"][0]["observations"]) == 32
        assert len(writes) == 1 and writes[0].startswith("INSERT INTO used_nonces ")
    finally:
        event.remove(Engine, "before_cursor_execute", observe)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["hash", "wire"])
async def test_unusable_optional_nav_does_not_hide_sec(migrated_database_url, invalid):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await seed_filing(session)
        evidence = await _seed(session, nav="0." + "0" * 39 + "1" if invalid == "wire" else "12")
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=DECISION)
        assert frozen is not None
        if invalid == "hash":
            await session.execute(text(
                "UPDATE data_snapshots SET payload=jsonb_set(payload, '{observations,0,nav}',"
                " '\"99\"'::jsonb) WHERE source='ssga_nav_history' AND revision=:revision"
            ), {"revision": evidence.revision})
        result = await read_research_inputs(
            session, snapshot_id=frozen.id, instrument_id=INSTRUMENT, as_of=DECISION
        )
        assert result is not None and len(result.filings) == 1
        assert result.issuer_nav_histories == ()
