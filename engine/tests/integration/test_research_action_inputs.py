"""Synthetic archived actions reach the real exact-snapshot API without business writes."""

import json
from dataclasses import replace
from datetime import timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import app, headers
from helpers.research_snapshots import INSTRUMENT, NOW, seed_filing
from integration.test_alpaca_action_research import persist_originals
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.api.research_inputs import read_research_inputs
from nexus.ops.openai_local import prepare_database
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.session import session_factory
from nexus.research.alpaca_actions_archive import load_action_collection
from nexus.settings import EngineSettings
from sqlalchemy import event, select, text
from sqlalchemy.engine import Engine
from unit.research.test_alpaca_actions import archive

RECEIPT = NOW + timedelta(minutes=1)
DECISION = NOW + timedelta(minutes=2)


def observation(tmp_path, *, count=35, unbound=2, receipt=RECEIPT):
    rows = [
        dict(id=f"synthetic-{index}", symbol="TEST", rate="0", currency="")
        for index in range(count)
    ]
    if count:
        rows[0].update(
            rate="0.123456789012345678901",
            old_rate="0.000001",
            new_rate="1E+999999999",
            ex_date="2026-08-12",
        )
    rows.extend(dict(id=f"unbound-{index}") for index in range(unbound))
    raw = json.dumps(
        {"corporate_actions": {"cash_dividends": rows}, "next_page_token": None}
    ).encode()
    directory = archive(tmp_path, pages=[raw], receipts={1: {"received_at": receipt.isoformat()}})
    return load_action_collection(directory, instrument_id=INSTRUMENT, symbol="TEST")


@pytest.mark.asyncio
@pytest.mark.parametrize("include_sec", [False, True])
async def test_archived_actions_reach_exact_bounded_readonly_api(
    migrated_database_url,
    tmp_path,
    monkeypatch,
    include_sec,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    validated = observation(tmp_path)
    async with factory() as session, session.begin():
        await seed_filing(session, source="sec" if include_sec else "synthetic")
        old = await prepare_research_snapshot(session, INSTRUMENT, as_of=NOW)
        old_hash = old.revision if old else None
        evidence = await persist_originals(session, validated)
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=DECISION)
        assert frozen is not None
        result = await read_research_inputs(
            session, snapshot_id=frozen.id, instrument_id=INSTRUMENT, as_of=DECISION
        )
        assert result is not None
        action = result.corporate_action_observations[0]
        assert action.evidence_ref_id == evidence.id
        assert action.content_hash == action.revision == evidence.content_hash
        assert action.total_event_count == 37 and action.bound_event_count == 35
        assert action.unbound_event_count == 2 and len(action.events) == 32 and action.truncated
        assert action.events[0].rate == "0.123456789012345678901"
        assert action.events[0].old_rate == "0.000001"
        assert action.events[0].new_rate is None
        assert action.events[0].unavailable_numeric_fields == ("new_rate",)
        assert action.events[0].currency == "" and action.events[0].process_date is None
        assert action.events[1].rate == "0"
        assert action.available_at == RECEIPT
        assert action.query_start.isoformat() == "2026-08-01"
        assert action.query_end.isoformat() == "2026-08-09"
        assert action.events[0].ex_date.isoformat() == "2026-08-12"
        assert action.source_completeness == "unknown" and action.execution_locked
        assert not action.historical_backtest_eligible and not action.ledger_eligible
        assert len(result.filings) == int(include_sec)
        checks = [(frozen.id, INSTRUMENT, NOW), (frozen.id, UUID(int=999), DECISION)]
        if old:
            checks.append((old.id, INSTRUMENT, DECISION))
            await session.refresh(old)
            assert old.revision == old_hash
        for snapshot_id, instrument_id, cutoff in checks:
            other = await read_research_inputs(
                session, snapshot_id=snapshot_id, instrument_id=instrument_id, as_of=cutoff
            )
            assert other is None or other.corporate_action_observations == ()

    async def forbidden(*args, **kwargs):
        pytest.fail("Research display GET must not fetch external data or call a model")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", forbidden)
    writes = []

    def observe(connection, cursor, statement, parameters, context, many):
        if statement.lstrip().split()[0].upper() in {"INSERT", "UPDATE", "DELETE", "MERGE"}:
            writes.append(statement.lstrip())

    event.listen(Engine, "before_cursor_execute", observe)
    try:
        path = (
            f"/internal/v1/instruments/{INSTRUMENT}/research-admission"
            "?as_of=2026-09-02T04%3A02%3A00Z"
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(migrated_database_url, now=DECISION)),
            base_url="http://test",
        ) as client:
            response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200
        inputs = response.json()["data"]["inputs"]
        assert inputs["corporate_action_observations"] == [action.model_dump(mode="json")]
        assert len(writes) == 1 and writes[0].startswith("INSERT INTO used_nonces ")
    finally:
        event.remove(Engine, "before_cursor_execute", observe)


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", ["payload", "late_receipt", "wrong_object"])
async def test_bad_optional_action_does_not_hide_sec(migrated_database_url, tmp_path, invalid):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await seed_filing(session)
        await persist_originals(session, observation(tmp_path))
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=DECISION)
        assert frozen is not None
        if invalid == "payload":
            await session.execute(
                text(
                    "UPDATE data_snapshots SET payload=jsonb_set(payload, '{events,0,rate}',"
                    " '\"99\"'::jsonb) WHERE source='alpaca_action_observation'"
                )
            )
        elif invalid == "late_receipt":
            await session.execute(
                text(
                    "UPDATE data_snapshots SET received_at=:late "
                    "WHERE source='alpaca_action_observation'"
                ),
                {"late": DECISION + timedelta(seconds=1)},
            )
        else:
            await session.execute(
                text(
                    "UPDATE data_snapshots SET provider_object_id='other-object' "
                    "WHERE source='alpaca_action_observation'"
                ),
            )
        result = await read_research_inputs(
            session, snapshot_id=frozen.id, instrument_id=INSTRUMENT, as_of=DECISION
        )
        assert result is not None and len(result.filings) == 1
        assert result.corporate_action_observations == ()


@pytest.mark.asyncio
@pytest.mark.parametrize("count,unbound", [(0, 0), (0, 2)])
async def test_empty_bound_events_still_show_query_and_unknown_completeness(
    migrated_database_url,
    tmp_path,
    count,
    unbound,
):
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        await seed_filing(session, source="synthetic")
        await persist_originals(session, observation(tmp_path, count=count, unbound=unbound))
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=DECISION)
        result = await read_research_inputs(
            session, snapshot_id=frozen.id, instrument_id=INSTRUMENT, as_of=DECISION
        )
        action = result.corporate_action_observations[0]
        assert action.events == () and action.bound_event_count == 0
        assert action.unbound_event_count == action.total_event_count == unbound
        assert action.source_completeness == "unknown" and not action.truncated


@pytest.mark.asyncio
async def test_exact_run_replay_keeps_old_observation_after_new_revision(
    migrated_database_url,
    tmp_path,
):
    settings = EngineSettings(database_url=migrated_database_url)
    await prepare_database(settings)
    factory = session_factory(settings)
    async with factory() as session, session.begin():
        await seed_filing(session)
        first = await persist_originals(session, observation(tmp_path, count=1))
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=DECISION)
        old_payload = json.dumps(frozen.payload, sort_keys=True)
        old_revision = frozen.revision
        admitted = await admit_research_run(
            session,
            actor_id="test",
            route="/internal/v1/research-runs",
            idempotency_key="action-display-exact-run",
            request_hash="b" * 64,
            request=ResearchRequest(
                instrument_id=INSTRUMENT, data_snapshot_id=frozen.id, decision_time=DECISION
            ),
            prompt_version_id=None,
            model_config_id=None,
            daily_limit_usd=Decimal("3"),
            now=DECISION,
        )
        run_id = admitted.run.id
        later_dir = tmp_path / "later"
        later_dir.mkdir()
        later = DECISION + timedelta(minutes=2)
        later_archive = observation(later_dir, count=2, receipt=DECISION + timedelta(minutes=1))
        existing = set(await session.scalars(select(DataArtifact.sha256)))
        # The collector manifest may be identical across imports; retain its existing artifact.
        later_archive = replace(
            later_archive,
            originals=tuple(
                item
                for item in later_archive.originals
                if sha256(item.content).hexdigest() not in existing
            ),
        )
        second = await persist_originals(session, later_archive)
        newer = await prepare_research_snapshot(session, INSTRUMENT, as_of=later)
        assert newer.id != frozen.id and second.id != first.id
        exact = await read_research_inputs(
            session, snapshot_id=frozen.id, instrument_id=INSTRUMENT, as_of=later
        )
        assert exact.corporate_action_observations[0].evidence_ref_id == first.id
        assert exact.corporate_action_observations[0].bound_event_count == 1
        await session.refresh(frozen)
        assert frozen.revision == old_revision
        assert json.dumps(frozen.payload, sort_keys=True) == old_payload

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app(migrated_database_url, now=later)),
        base_url="http://test",
    ) as client:
        path = f"/internal/v1/research-runs/{run_id}"
        response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200
        run = response.json()["data"]
        action = run["inputs"]["corporate_action_observations"][0]
        assert run["data_snapshot_id"] == str(frozen.id)
        assert action["evidence_ref_id"] == str(first.id)
        assert action["content_hash"] == first.content_hash
        assert action["bound_event_count"] == 1
