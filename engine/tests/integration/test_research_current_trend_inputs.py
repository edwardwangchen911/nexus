"""Normal archive/import/freeze/API; synthetic HTTP/S3, no provider-reader doubles."""

import json
import os
import subprocess
from copy import deepcopy
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import quote
from uuid import UUID
from zoneinfo import ZoneInfo

import httpx
import pytest
from helpers.portfolio_api import app, headers
from helpers.research_snapshots import seed_filing
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run, select_authoritative_snapshot
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.api.research_inputs import read_research_inputs
from nexus.ops import agent_paper_loop
from nexus.ops.openai_local import prepare_database
from nexus.paper.candidate_batch import CandidateInstrumentIdentity
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import DataSnapshot
from nexus.research.current_price_trends import SOURCE
from nexus.research.current_price_trends_storage import persist_current_price_trends
from sqlalchemy import event, func, select
from sqlalchemy.engine import Engine

from .test_current_price_trends import INSTRUMENT, setup_trends


async def setup_frozen_trends(url, directory, monkeypatch, *, has_base=True):
    settings, factory, store, _, archive = await setup_trends(url, directory, monkeypatch)
    await prepare_database(settings)
    async with factory.begin() as session:
        if has_base:
            await seed_filing(session, instrument_id=INSTRUMENT, seed_instrument=False)
        old = await prepare_research_snapshot(session, INSTRUMENT, as_of=datetime.now(UTC))
    async with factory.begin() as session:
        evidence = await persist_current_price_trends(session, archive, store=store)
    decision = datetime.now(UTC)
    async with factory.begin() as session:
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=decision)
        if frozen is None:
            return factory, old, frozen, decision, evidence, []
        assert str(evidence.id) in frozen.payload["evidence_ids"]
        assert str(evidence.id) not in old.payload["evidence_ids"]
        runs = []
        for index, snapshot in enumerate((old, frozen)):
            cutoff = datetime.fromisoformat(snapshot.payload["as_of"])
            admitted = await admit_research_run(
                session,
                actor_id="test",
                route="/internal/v1/research-runs",
                idempotency_key=f"trend-display-{index}",
                request_hash=str(index) * 64,
                request=ResearchRequest(
                    instrument_id=INSTRUMENT, data_snapshot_id=snapshot.id, decision_time=cutoff
                ),
                prompt_version_id=None,
                model_config_id=None,
                daily_limit_usd=Decimal("3"),
                now=cutoff,
            )
            runs.append(admitted.run.id)
    return factory, old, frozen, decision, evidence, runs


@pytest.mark.asyncio
async def test_normal_archive_reaches_exact_frozen_api_without_display_writes(
    migrated_database_url, tmp_path, monkeypatch
):
    factory, old, frozen, decision, evidence, runs = await setup_frozen_trends(
        migrated_database_url, tmp_path / "archive", monkeypatch
    )
    old_payload, old_hash = deepcopy(old.payload), old.revision
    async with factory() as session:
        result = await read_research_inputs(
            session, snapshot_id=frozen.id, instrument_id=INSTRUMENT, as_of=decision
        )
        assert len(result.filings) == len(result.current_price_trends) == 1
        trend = result.current_price_trends[0]
        assert trend.evidence_ref_id == evidence.id
        assert trend.features.sma_200 == "100.500000000000"
        assert trend.source == SOURCE and trend.revision == trend.content_hash
        assert not any(
            (
                trend.market_price_eligible,
                trend.historical_backtest_eligible,
                trend.total_return_eligible,
            )
        )
        assert trend.execution_locked
        for snapshot_id, instrument, cutoff in (
            (old.id, INSTRUMENT, decision),
            (frozen.id, UUID(int=999), decision),
            (frozen.id, INSTRUMENT, evidence.available_at - timedelta(microseconds=1)),
        ):
            other = await read_research_inputs(
                session, snapshot_id=snapshot_id, instrument_id=instrument, as_of=cutoff
            )
            assert other is None or other.current_price_trends == ()

    async def forbidden(*args, **kwargs):
        pytest.fail("Display must not call a provider or model")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", forbidden)
    writes = []

    def observe(connection, cursor, statement, parameters, context, many):
        if statement.lstrip().split()[0].upper() in {"INSERT", "UPDATE", "DELETE", "MERGE"}:
            writes.append(statement.lstrip())

    event.listen(Engine, "before_cursor_execute", observe)
    captures = []
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(migrated_database_url, now=decision)),
            base_url="http://test",
        ) as client:
            paths = [
                f"/internal/v1/instruments/{INSTRUMENT}/research-admission?as_of="
                + quote(decision.isoformat().replace("+00:00", "Z"), safe=""),
                f"/internal/v1/research-runs/{runs[1]}",
                f"/internal/v1/research-runs/{runs[0]}",
            ]
            for index, path in enumerate(paths):
                response = await client.get(path, headers=headers("GET", path))
                assert response.status_code == 200
                inputs = response.json()["data"]["inputs"]
                assert len(inputs["filings"]) == 1
                assert inputs["current_price_trends"] == (
                    [] if index == 2 else [trend.model_dump(mode="json")]
                )
                captures.append({"path": path, "body": response.json()})
        assert len(writes) == 3 and all(x.startswith("INSERT INTO used_nonces ") for x in writes)
    finally:
        event.remove(Engine, "before_cursor_execute", observe)
    assert old.payload == old_payload and old.revision == old_hash
    capture = tmp_path / "synthetic-api-responses.json"
    capture.write_text(json.dumps(captures), encoding="utf-8")
    # Only the synthetic API projection crosses into the actual BFF test process.
    # No inherited database, provider, model, or authentication environment.
    checked = subprocess.run(
        ["npm", "run", "test:unit", "--", "tests/unit/research/trend-pg-boundary.test.ts"],
        cwd=Path(__file__).resolve().parents[3],
        env={"PATH": os.environ["PATH"], "CI": "1", "NEXUS_TREND_API_CAPTURE": str(capture)},
        text=True,
        capture_output=True,
        timeout=60,
        check=False,
    )
    assert checked.returncode == 0, checked.stdout + checked.stderr
    assert "1 passed" in checked.stdout and "skipped" not in checked.stdout


@pytest.mark.asyncio
async def test_trend_only_cannot_bootstrap_research(migrated_database_url, tmp_path, monkeypatch):
    factory, old, frozen, decision, _, _ = await setup_frozen_trends(
        migrated_database_url, tmp_path / "archive", monkeypatch, has_base=False
    )
    assert old is None and frozen is None
    async with factory() as session:
        assert (
            await read_research_inputs(
                session, snapshot_id=UUID(int=999), instrument_id=INSTRUMENT, as_of=decision
            )
            is None
        )


@pytest.mark.asyncio
async def test_agent_paper_preparation_binds_existing_trend_without_recollecting(
    migrated_database_url, tmp_path, monkeypatch
):
    settings, factory, store, _, archive = await setup_trends(
        migrated_database_url, tmp_path / "archive", monkeypatch
    )
    await prepare_database(settings)
    async with factory.begin() as session:
        await seed_filing(session, instrument_id=INSTRUMENT, seed_instrument=False)
        old = await prepare_research_snapshot(session, INSTRUMENT, as_of=datetime.now(UTC))
        assert old is not None
    async with factory.begin() as session:
        evidence = await persist_current_price_trends(session, archive, store=store)
    async with factory() as session:
        documents = await agent_paper_loop.read_current_price_trends(
            session, instrument_id=INSTRUMENT, as_of=datetime.now(UTC)
        )
        prior_session = documents[evidence.id].last_observed_on
        assert prior_session is not None

    candidate = CandidateInstrumentIdentity(
        instrument_id=INSTRUMENT,
        symbol="AAPL",
        asset_type="common_stock",
        exchange="XNYS",
        currency="USD",
    )
    calendar_id = UUID(int=711)
    calendar_now = datetime.now(UTC)
    today = calendar_now.astimezone(ZoneInfo("America/New_York")).date()
    # The bound service calendar covers the decision window, while the trend
    # evidence was collected and persisted from an older historical window.
    start = prior_session
    async with factory.begin() as session:
        session.add(
            ExchangeCalendarAuthorityRecord(
                id=calendar_id,
                exchange="XNYS",
                version="current-trend-bound-test",
                source="fixture",
                range_start=start,
                range_end=today + timedelta(days=2),
                query_parameters={},
                completeness_status="complete",
                content_hash="c" * 64,
                published_at=calendar_now - timedelta(days=1),
                created_at=calendar_now - timedelta(days=1),
            )
        )
        await session.flush()
        for day in sorted(
            {prior_session, today, today + timedelta(days=1), today + timedelta(days=2)}
        ):
            session.add(
                ExchangeCalendarSessionRecord(
                    calendar_authority_id=calendar_id,
                    session=day,
                    decision_at=datetime.combine(day, time(20, 1), tzinfo=UTC),
                    market_open_at=datetime.combine(day, time(13, 30), tzinfo=UTC),
                    market_close_at=datetime.combine(day, time(20), tzinfo=UTC),
                    created_at=calendar_now - timedelta(days=1),
                )
            )

    async def inspect_prior(
        _settings, *, account_id, now, expected_calendar_authority_id, candidate
    ):
        assert expected_calendar_authority_id == calendar_id
        assert candidate.instrument_id == INSTRUMENT
        return agent_paper_loop.PriorDailyScope(
            instrument_id=INSTRUMENT,
            prior_session=prior_session,
            authority_ready=True,
        )

    async def forbidden(*_args, **_kwargs):
        pytest.fail("existing trend evidence must not call Alpaca or import again")

    monkeypatch.setattr(agent_paper_loop, "_inspect_prior_daily_scope", inspect_prior)
    monkeypatch.setattr(agent_paper_loop, "collect_prices", forbidden)
    monkeypatch.setattr(agent_paper_loop, "import_trends", forbidden)

    first = await agent_paper_loop.prepare_current_trends_once(
        settings,
        candidate=candidate,
        expected_calendar_authority_id=calendar_id,
        account_id=UUID(int=1),
        repo_root=tmp_path,
        now=datetime.now(UTC),
    )
    assert first == "prepared"

    async with factory() as session:
        selected = await select_authoritative_snapshot(
            session, instrument_id=INSTRUMENT, as_of=datetime.now(UTC)
        )
        assert selected is not None and selected.id != old.id
        binding = await session.scalar(
            select(AgentDataSnapshotEvidenceRecord).where(
                AgentDataSnapshotEvidenceRecord.data_snapshot_id == selected.id,
                AgentDataSnapshotEvidenceRecord.instrument_id == INSTRUMENT,
                AgentDataSnapshotEvidenceRecord.evidence_ref_id == evidence.id,
                AgentDataSnapshotEvidenceRecord.authority_kind == "research",
                AgentDataSnapshotEvidenceRecord.source == SOURCE,
            )
        )
        assert binding is not None
        before = await session.scalar(
            select(func.count())
            .select_from(DataSnapshot)
            .where(DataSnapshot.source == "nexus_research")
        )

    second = await agent_paper_loop.prepare_current_trends_once(
        settings,
        candidate=candidate,
        expected_calendar_authority_id=calendar_id,
        account_id=UUID(int=1),
        repo_root=tmp_path,
        now=datetime.now(UTC),
    )
    assert second == "ready"
    async with factory() as session:
        after = await session.scalar(
            select(func.count())
            .select_from(DataSnapshot)
            .where(DataSnapshot.source == "nexus_research")
        )
        assert after == before
    assert not (tmp_path / ".secrets").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", ["missing", "hash", "instrument", "calendar"])
async def test_malformed_optional_trend_preserves_frozen_sec(
    migrated_database_url, tmp_path, monkeypatch, defect
):
    factory, _, frozen, decision, _, _ = await setup_frozen_trends(
        migrated_database_url, tmp_path / "archive", monkeypatch
    )
    async with factory() as session:
        row = await session.scalar(select(DataSnapshot).where(DataSnapshot.source == SOURCE))
        payload = deepcopy(row.payload)
        if defect == "missing":
            del payload["features"]
        elif defect == "hash":
            payload["features"]["close"] = "999"
        elif defect == "instrument":
            payload["instrument_id"] = str(UUID(int=999))
        else:
            del payload["calendar"]
        row.payload = payload
        # Read-path corruption only: do not bypass immutable SQL guards or change saved history.
        with session.no_autoflush:
            result = await read_research_inputs(
                session, snapshot_id=frozen.id, instrument_id=INSTRUMENT, as_of=decision
            )
        assert len(result.filings) == 1
        assert result.current_price_trends == ()
