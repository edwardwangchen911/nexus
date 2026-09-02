"""Freeze persisted filing evidence, then exercise the existing research API/worker."""

import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import app, headers
from nexus.ops.openai_local import prepare_database
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord, AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot, PriceBarSetAuthority
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.research import Filing, FinancialFactRecord, ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select

NOW = datetime(2026, 9, 2, 4, tzinfo=UTC)
INSTRUMENT = UUID("0198d500-1000-7000-8000-000000002001")


async def seed_filing(session, *, source="sec", accepted_at=NOW, created_at=NOW):
    session.add(
        Instrument(
            id=INSTRUMENT,
            asset_type="equity",
            exchange="XNAS",
            currency="USD",
            tick_size=Decimal("0.01"),
            risk_bucket="us_equity",
        )
    )
    await session.flush()
    session.add(
        InstrumentAlias(
            instrument_id=INSTRUMENT,
            symbol="TEST",
            alias_type="ticker",
            valid_from=NOW.date(),
            valid_to=None,
        )
    )
    evidence = ResearchEvidenceRef(
        source=source,
        provider_object_id="filing:test",
        revision="test-v1",
        available_at=NOW,
        created_at=created_at,
        content_hash="a" * 64,
        location="memory://sha256/" + "a" * 64,
        byte_length=10,
    )
    session.add(evidence)
    await session.flush()
    session.add(
        Filing(
            instrument_id=INSTRUMENT,
            evidence_ref_id=evidence.id,
            source=source,
            provider_object_id=evidence.provider_object_id,
            revision=evidence.revision,
            accession_number="0000000001-26-000001",
            form="10-Q",
            filed_at=NOW,
            accepted_at=accepted_at,
            created_at=created_at,
        )
    )
    await session.flush()
    return evidence


@pytest.mark.asyncio
async def test_prepared_snapshot_reaches_api_and_worker_without_price_promotion(
    migrated_database_url,
    monkeypatch,
):
    from nexus.agents.snapshots import prepare_research_snapshot
    from nexus.worker import run_agent_worker_once

    settings = EngineSettings(database_url=migrated_database_url)
    await prepare_database(settings)
    factory = session_factory(settings)
    async with factory() as session, session.begin():
        evidence = await seed_filing(session)
        snapshot = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        assert snapshot is not None
        again = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        assert again.id == snapshot.id
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 1
        links = list(await session.scalars(select(AgentDataSnapshotEvidenceRecord)))
        assert [(link.evidence_ref_id, link.authority_kind) for link in links] == [
            (evidence.id, "research")
        ]
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 0
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 0

    monkeypatch.setattr("nexus.api.routes.research.utc_now", lambda: NOW)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=NOW)),
        base_url="http://testserver",
    ) as client:
        path = (
            f"/internal/v1/instruments/{INSTRUMENT}/research-admission"
            "?as_of=2026-09-02T04%3A00%3A00Z"
        )
        admission = await client.get(path, headers=headers("GET", path))
        assert admission.status_code == 200
        assert admission.json()["data"]["eligible"] is True
        assert admission.json()["data"]["data_snapshot_id"] == str(snapshot.id)
        body = json.dumps(
            {
                "instrument_id": str(INSTRUMENT),
                "data_snapshot_id": str(snapshot.id),
                "decision_time": "2026-09-02T04:00:00Z",
            }
        ).encode()
        path = "/internal/v1/research-runs"
        response = await client.post(
            path,
            content=body,
            headers={
                **headers("POST", path, body),
                "Idempotency-Key": "snapshot-to-worker",
            },
        )
        assert response.status_code == 202
        assert await run_agent_worker_once(settings) is True
        path = "/internal/v1/research-runs/" + response.json()["data"]["run_id"]
        result = await client.get(path, headers=headers("GET", path))
        assert result.status_code == 200
        data = result.json()["data"]
        assert data["status"] == "completed"
        assert data["quality_status"] == "partial"
        assert data["intent"]["exposure_action"] == "none"

    # The actual local entrypoint must work without a key and must not enqueue a run.
    environment = dict(
        os.environ,
        DATABASE_URL=migrated_database_url,
        NEXUS_OFFLINE_SOCKET_GUARD="1",
        PYTHONPATH=str(Path(__file__).resolve().parents[3] / "scripts/offline_guard"),
    )
    environment.pop("OPENAI_API_KEY", None)
    process = subprocess.run(
        [
            sys.executable,
            "-m",
            "nexus.ops.openai_local",
            "prepare-research",
            "--instrument-id",
            str(INSTRUMENT),
            "--as-of",
            "2026-09-02T04:00:00Z",
        ],
        env=environment,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert process.returncode == 0, process.stderr
    prepared = json.loads(process.stdout)
    assert prepared["data_snapshot_id"] == str(snapshot.id)
    assert prepared["model_requests"] == 0
    assert prepared["research_completed"] is False
    assert prepared["execution_locked"] is True
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 1
        assert await session.scalar(select(func.count()).select_from(AgentResearchRunRecord)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "fixture",
        "future_acceptance",
        "late_arrival",
        "other_instrument",
        "empty",
    ],
)
async def test_unusable_evidence_does_not_create_a_snapshot(migrated_database_url, case):
    from nexus.agents.snapshots import prepare_research_snapshot

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        if case != "empty":
            await seed_filing(
                session,
                source="fixture" if case == "fixture" else "sec",
                accepted_at=NOW + timedelta(days=1) if case == "future_acceptance" else NOW,
                created_at=NOW + timedelta(days=1) if case == "late_arrival" else NOW,
            )
        instrument_id = UUID(int=42) if case == "other_instrument" else INSTRUMENT
        snapshot = await prepare_research_snapshot(session, instrument_id, as_of=NOW)
        assert snapshot is None
        assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 0
        assert (
            await session.scalar(select(func.count()).select_from(AgentDataSnapshotEvidenceRecord))
            == 0
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["current", "late_fact", "unfrozen", "other_instrument"])
async def test_display_inputs_respect_exact_frozen_scope(migrated_database_url, case):
    from nexus.agents.snapshots import prepare_research_snapshot
    from nexus.api.research_inputs import read_research_inputs

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await seed_filing(session)
        snapshot = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        filing = await session.scalar(select(Filing))
        assert snapshot is not None and filing is not None
        if case == "unfrozen":
            extra = ResearchEvidenceRef(
                source="sec",
                provider_object_id="filing:unfrozen",
                revision="other-v1",
                available_at=NOW,
                created_at=NOW,
                content_hash="b" * 64,
                location="memory://sha256/" + "b" * 64,
                byte_length=10,
            )
            session.add(extra)
            await session.flush()
            filing = Filing(
                instrument_id=INSTRUMENT,
                evidence_ref_id=extra.id,
                source="sec",
                provider_object_id=extra.provider_object_id,
                revision=extra.revision,
                accession_number="0000000001-26-000002",
                form="10-Q",
                filed_at=NOW,
                accepted_at=NOW,
                created_at=NOW,
            )
            session.add(filing)
            await session.flush()
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
                period_start=NOW.date() - timedelta(days=90),
                period_end=NOW.date(),
                fiscal_year=2026,
                fiscal_period="Q3",
                form=filing.form,
                filed_at=NOW,
                accepted_at=NOW,
                accession_number=filing.accession_number,
                frame=None,
                revision=filing.revision,
                created_at=NOW + timedelta(days=1) if case == "late_fact" else NOW,
            )
        )
        await session.flush()
        result = await read_research_inputs(
            session,
            snapshot_id=snapshot.id,
            instrument_id=UUID(int=42) if case == "other_instrument" else INSTRUMENT,
            as_of=NOW,
        )
        if case == "other_instrument":
            assert result is None
        else:
            assert result is not None
            assert len(result.filings) == 1
            assert result.total_facts == len(result.facts) == (1 if case == "current" else 0)
