"""Synthetic issuer observations reach the existing read-only research endpoint."""

import hashlib
from datetime import timedelta
from decimal import Decimal
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import app, headers
from integration.test_research_snapshot_preparation import INSTRUMENT, NOW, seed_filing
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.api.research_inputs import read_research_inputs
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.models.research import Filing, FinancialFactRecord
from nexus.persistence.session import session_factory
from nexus.research.ssga_distributions import parse_distribution, persist_distribution
from nexus.settings import EngineSettings
from sqlalchemy import event, select, text

from engine.tests.unit.research.test_ssga_distributions import identity, workbook

RECEIPT = NOW + timedelta(minutes=1)
DECISION = NOW + timedelta(minutes=2)


async def seed_issuer(session):
    document = parse_distribution(
        raw=workbook(),
        identity_html=identity(),
        instrument_id=INSTRUMENT,
        symbol="TEST",
        identity_url="https://www.ssga.com/us/en/individual/etfs/synthetic-fund-test",
        received_at=RECEIPT,
    ).model_copy(update={"long_term_capital_gain": Decimal("0")})
    for digest, size in (
        (document.raw_content_hash, document.raw_byte_length),
        (document.identity_content_hash, document.identity_byte_length),
    ):
        session.add(
            DataArtifact(
                uri="memory://sha256/" + digest,
                sha256=digest,
                byte_size=size,
                mime_type="application/octet-stream",
                schema_version="issuer_raw_v1",
                source="ssga",
                observed_at=RECEIPT,
                available_at=RECEIPT,
                received_at=RECEIPT,
            )
        )
    await session.flush()
    return await persist_distribution(
        session,
        document,
        location="memory://sha256/" + hashlib.sha256(document.canonical_bytes()).hexdigest(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("include_sec,financial_count", [(False, 0), (True, 1), (True, 101)])
async def test_issuer_only_and_mixed_inputs_reach_get_without_writes_or_external_calls(
    migrated_database_url,
    monkeypatch,
    include_sec,
    financial_count,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await seed_filing(session, source="sec" if include_sec else "synthetic")
        if include_sec:
            filing = await session.scalar(select(Filing))
            for offset in range(financial_count):
                session.add(
                    FinancialFactRecord(
                        filing_id=filing.id,
                        evidence_ref_id=filing.evidence_ref_id,
                        instrument_id=INSTRUMENT,
                        canonical_fact="net_income",
                        raw_taxonomy="us-gaap",
                        raw_tag="NetIncomeLoss",
                        unit="USD",
                        value=Decimal("123.45"),
                        period_kind="duration",
                        period_start=NOW.date() - timedelta(days=90 + offset),
                        period_end=NOW.date() - timedelta(days=offset),
                        fiscal_year=2026,
                        fiscal_period="Q3",
                        form=filing.form,
                        filed_at=NOW,
                        accepted_at=NOW,
                        accession_number=filing.accession_number,
                        revision=filing.revision,
                        created_at=NOW,
                    )
                )
            await session.flush()
        old = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        evidence = await seed_issuer(session)
        snapshot = await prepare_research_snapshot(session, INSTRUMENT, as_of=DECISION)
        assert snapshot is not None
        inputs = await read_research_inputs(
            session,
            snapshot_id=snapshot.id,
            instrument_id=INSTRUMENT,
            as_of=DECISION,
        )
        assert inputs is not None
        assert len(inputs.issuer_distributions) == 1
        fact = inputs.issuer_distributions[0]
        assert fact.evidence_ref_id == evidence.id
        assert fact.dividend_per_share == "0.125000"
        assert fact.short_term_capital_gain is None and fact.long_term_capital_gain == "0"
        assert fact.available_at == fact.received_at == RECEIPT
        assert fact.amount_basis == "per_share" and fact.currency == "USD"
        assert fact.historical_backtest_eligible is False
        assert inputs.total_facts == financial_count
        assert len(inputs.facts) == min(financial_count, 100)
        assert len(inputs.filings) == int(include_sec)
        assert inputs.truncated is (financial_count > 100)
        assert (
            await read_research_inputs(
                session,
                snapshot_id=snapshot.id,
                instrument_id=UUID(int=99),
                as_of=DECISION,
            )
        ) is None
        earlier = await read_research_inputs(
            session,
            snapshot_id=snapshot.id,
            instrument_id=INSTRUMENT,
            as_of=NOW,
        )
        assert earlier is None or earlier.issuer_distributions == ()
        if old is not None:
            previous = await read_research_inputs(
                session,
                snapshot_id=old.id,
                instrument_id=INSTRUMENT,
                as_of=DECISION,
            )
            assert previous is not None and previous.issuer_distributions == ()

    async def forbidden(*args, **kwargs):
        pytest.fail("research GET must not make an external HTTP request")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", forbidden)
    engine_app = app(migrated_database_url, now=DECISION)
    statements = []

    def observe(connection, cursor, statement, parameters, context, many):
        statements.append(statement.lstrip())

    from sqlalchemy.engine import Engine

    event.listen(Engine, "before_cursor_execute", observe)
    try:
        path = (
            f"/internal/v1/instruments/{INSTRUMENT}/research-admission"
            "?as_of=2026-09-02T04%3A02%3A00Z"
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=engine_app), base_url="http://test"
        ) as client:
            response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200, response.text
        body = response.json()["data"]["inputs"]
        assert body["issuer_distributions"][0]["dividend_per_share"] == "0.125000"
        assert body["issuer_distributions"][0]["short_term_capital_gain"] is None
        assert body["issuer_distributions"][0]["long_term_capital_gain"] == "0"
        assert body["total_facts"] == financial_count
        assert body["truncated"] is (financial_count > 100)
        writes = [
            statement
            for statement in statements
            if statement.split()[0].upper() in {"INSERT", "UPDATE", "DELETE", "MERGE"}
        ]
        # Existing HMAC replay protection writes its nonce even for a read-only GET.
        assert len(writes) == 1 and writes[0].startswith("INSERT INTO used_nonces ")
    finally:
        event.remove(Engine, "before_cursor_execute", observe)


@pytest.mark.asyncio
@pytest.mark.parametrize("amount", ["999", "malformed"])
async def test_bound_issuer_invalid_payload_does_not_hide_sec(migrated_database_url, amount):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await seed_filing(session)
        evidence = await seed_issuer(session)
        snapshot = await prepare_research_snapshot(session, INSTRUMENT, as_of=DECISION)
        assert snapshot is not None
        await session.execute(
            text(
                "UPDATE data_snapshots SET payload=jsonb_set(payload, '{dividend_per_share}', "
                "to_jsonb(CAST(:amount AS text))) "
                "WHERE source='ssga_distribution' AND revision=:revision"
            ),
            {"revision": evidence.revision, "amount": amount},
        )
        result = await read_research_inputs(
            session,
            snapshot_id=snapshot.id,
            instrument_id=INSTRUMENT,
            as_of=DECISION,
        )
        assert result is not None and result.issuer_distributions == ()
        assert len(result.filings) == 1
    engine_app = app(migrated_database_url, now=DECISION)
    path = (
        f"/internal/v1/instruments/{INSTRUMENT}/research-admission?as_of=2026-09-02T04%3A02%3A00Z"
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=engine_app), base_url="http://test"
    ) as client:
        response = await client.get(path, headers=headers("GET", path))
    assert response.status_code == 200
    assert response.json()["data"]["inputs"]["issuer_distributions"] == []
    assert len(response.json()["data"]["inputs"]["filings"]) == 1
