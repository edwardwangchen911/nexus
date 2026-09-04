"""Real FRED HTTP adapter, originals, PG facts and exact canonical reader."""

import copy
import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import uuid4

import httpx
import pytest
from helpers.portfolio_api import settings
from nexus.persistence.models.data import DataArtifact, DataSnapshot, MacroObservation
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.providers.fred_http import FredHttpTransport
from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy
from nexus.research.fred_collection import collect_fred_vintage, load_fred_collection
from nexus.research.fred_observations import SCHEMA, parse_collected_vintage
from nexus.research.fred_storage import import_fred_vintage, read_fred_observations
from pydantic import SecretStr
from sqlalchemy import func, select, text
from unit.research.test_fred_observations import captured_payloads, parse


class MemoryStore:
    def __init__(self):
        self.contents = {}

    async def put_if_absent(self, *, key, content):
        if key in self.contents:
            assert self.contents[key] == content
        self.contents[key] = content
        return f"memory://sha256/{sha256(content).hexdigest()}"


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", ["schema", "authority"])
async def test_replay_rejects_wrong_ownership_contract(migrated_database_url, defect):
    sample = parse()
    factory = session_factory(settings(migrated_database_url))
    async with factory() as db, db.begin():
        ids = await import_fred_vintage(db, collection=sample, store=MemoryStore())
    async with factory() as db:
        if defect == "schema":
            record = await db.scalar(
                select(DataSnapshot).where(DataSnapshot.schema_version == SCHEMA)
            )
            record.schema_version = "legacy"
        else:
            record = await db.get(MacroObservation, ids[0])
            record.authority_version = "legacy_task3"
        with db.no_autoflush, pytest.raises(ValueError, match="FRED_CANONICAL_INVALID"):
            await import_fred_vintage(db, collection=sample, store=MemoryStore())
        await db.rollback()


@pytest.mark.asyncio
async def test_same_today_archive_replays_before_conservative_availability(migrated_database_url):
    from nexus.research.fred_observations import FredCapture, FredVintageRequest

    at = datetime.now(UTC)
    day = at.date().isoformat()
    metadata, page = captured_payloads()
    for value in (metadata, metadata["seriess"][0], page):
        value["realtime_start"] = value["realtime_end"] = day
    page["observation_start"] = page["observation_end"] = day
    page["count"] = 1
    page["observations"] = [
        {"date": day, "value": "4.1", "realtime_start": day, "realtime_end": day}
    ]
    sample = parse_collected_vintage(
        FredVintageRequest(series="DGS10", observation_start=day, observation_end=day, vintage=day),
        FredCapture("series", None, json.dumps(metadata).encode(), at),
        (FredCapture("observations", 0, json.dumps(page).encode(), at),),
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory() as db, db.begin():
        ids = await import_fred_vintage(db, collection=sample, store=MemoryStore())
    async with factory() as db, db.begin():
        assert (await db.get(MacroObservation, ids[0])).available_at > datetime.now(UTC)
        assert await read_fred_observations(db, as_of=datetime.now(UTC)) == {}
        assert await import_fred_vintage(db, collection=sample, store=MemoryStore()) == ids
        assert await read_fred_observations(db, as_of=datetime.now(UTC)) == {}


@pytest.mark.asyncio
async def test_late_materialization_is_not_visible_at_original_receipt(migrated_database_url):
    sample = parse()
    factory = session_factory(settings(migrated_database_url))
    async with factory() as db, db.begin():
        # Restore actual clocks in this disposable DB; the common fixture freezes Jan 1.
        for table in (
            "data_artifacts",
            "data_snapshots",
            "research_evidence_refs",
            "macro_observations",
        ):
            await db.execute(
                text(f"ALTER TABLE {table} ALTER COLUMN created_at SET DEFAULT clock_timestamp()")
            )
    async with factory() as db, db.begin():
        ids = await import_fred_vintage(db, collection=sample, store=MemoryStore())
    async with factory() as db, db.begin():
        now = datetime.now(UTC)
        row = await db.get(MacroObservation, ids[0])
        assert sample.received_at < row.created_at <= now
        assert await read_fred_observations(db, as_of=sample.received_at) == {}
        assert len(await read_fred_observations(db, as_of=now)) == 2
        assert await import_fred_vintage(db, collection=sample, store=MemoryStore()) == ids


@pytest.mark.asyncio
async def test_adoption_published_after_receipt_can_support_current_import(migrated_database_url):
    from nexus.research.fred_observations import FredCapture, FredVintageRequest

    metadata, page = captured_payloads()
    for value in (metadata, metadata["seriess"][0], page):
        value["realtime_start"] = value["realtime_end"] = "2025-12-30"
    page["observation_start"] = page["observation_end"] = "2025-12-29"
    page["count"] = 1
    page["observations"] = [
        {
            "date": "2025-12-29",
            "value": "4.1",
            "realtime_start": "2025-12-30",
            "realtime_end": "2025-12-30",
        }
    ]
    receipt = datetime(2025, 12, 31, 20, tzinfo=UTC)
    request = FredVintageRequest(
        series="DGS10",
        observation_start="2025-12-29",
        observation_end="2025-12-29",
        vintage="2025-12-30",
    )
    sample = parse_collected_vintage(
        request,
        FredCapture("series", None, json.dumps(metadata).encode(), receipt),
        (FredCapture("observations", 0, json.dumps(page).encode(), receipt),),
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory() as db, db.begin():
        ids = await import_fred_vintage(db, collection=sample, store=MemoryStore())
        assert len(ids) == 1
        assert await read_fred_observations(db, as_of=receipt) == {}
        row = next(iter((await read_fred_observations(db, as_of=datetime.now(UTC))).values()))
        assert row.available_at == datetime(2026, 1, 1, tzinfo=UTC)
        assert row.received_at == receipt


@pytest.mark.asyncio
async def test_real_collector_to_pg_reader_exact_scope_and_replay(migrated_database_url, tmp_path):
    sample = parse()
    responses = captured_payloads()
    calls = []

    def handle(request):
        calls.append(request)
        return httpx.Response(200, content=json.dumps(responses[len(calls) - 1]).encode())

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as client:
        transport = FredHttpTransport(
            FixedOriginHttpClient(
                provider_name="fred",
                origin=AllowedOrigin(scheme="https", host="api.stlouisfed.org"),
                policy=HttpRequestPolicy(max_attempts=1),
                client=client,
            ),
            SecretStr("isolated-key"),
        )
        await collect_fred_vintage(
            sample.request,
            transport=transport,
            output_dir=tmp_path,
            clock=lambda: sample.received_at,
        )
    collection = load_fred_collection(tmp_path)
    factory = session_factory(settings(migrated_database_url))
    store = MemoryStore()
    async with factory() as db, db.begin():
        ids = await import_fred_vintage(db, collection=collection, store=store)
        assert len(ids) == 2
    async with factory() as db, db.begin():
        current = datetime.now(UTC)
        rows = await read_fred_observations(db, as_of=current)
        assert {value.observation_id for value in rows.values()} == set(ids)
        assert all(
            value.unit == "Percent" and value.available_at == sample.received_at
            for value in rows.values()
        )
        assert {value.value for value in rows.values()} == {Decimal("4.123456789012"), Decimal("0")}
        assert len({value.calendar_evidence_ref_id for value in rows.values()}) == 1
        assert (
            await read_fred_observations(db, as_of=sample.received_at - timedelta(microseconds=1))
            == {}
        )
        assert await read_fred_observations(db, as_of=sample.received_at, evidence_ids=()) == {}
        assert (
            await read_fred_observations(db, as_of=sample.received_at, evidence_ids=(uuid4(),))
            == {}
        )
        key = next(iter(rows))
        assert set(await read_fred_observations(db, as_of=current, evidence_ids=(key,))) == {key}
        assert await import_fred_vintage(db, collection=collection, store=store) == ids
        assert await db.scalar(select(func.count()).select_from(MacroObservation)) == 2
        assert await db.scalar(select(func.count()).select_from(DataArtifact)) == 3
        canonical = await db.scalar(
            select(DataSnapshot).where(DataSnapshot.schema_version == SCHEMA)
        )
        assert len(canonical.payload["captures"]) == 2
        assert '"value": "."' in canonical.payload["captures"][1]["raw_text"]
        assert len(store.contents) == 4
    assert len(calls) == 2


@pytest.mark.asyncio
async def test_conservative_next_session_still_bounds_early_receipt(migrated_database_url):
    original = parse()
    at = datetime(2026, 8, 28, 20, tzinfo=UTC)
    collection = parse_collected_vintage(
        original.request,
        replace(original.metadata, received_at=at),
        tuple(replace(p, received_at=at) for p in original.pages),
    )
    factory = session_factory(settings(migrated_database_url))
    async with factory() as db, db.begin():
        await import_fred_vintage(db, collection=collection, store=MemoryStore())
        assert await read_fred_observations(db, as_of=at) == {}
        open_at = datetime(2026, 8, 31, 13, 30, tzinfo=UTC)
        assert await read_fred_observations(db, as_of=open_at) == {}
        rows = await read_fred_observations(db, as_of=datetime.now(UTC))
        assert len(rows) == 2
        assert all(row.available_at == open_at and row.received_at == at for row in rows.values())


@pytest.mark.asyncio
@pytest.mark.parametrize("defect", ["raw", "unit", "vintage", "calendar", "receipt"])
async def test_optional_corrupt_binding_is_unavailable_without_relabeling(
    migrated_database_url, defect
):
    sample = parse()
    factory = session_factory(settings(migrated_database_url))
    async with factory() as db, db.begin():
        ids = await import_fred_vintage(db, collection=sample, store=MemoryStore())
    # Corrupt only this connection's ORM view; never update immutable history or disable guards.
    async with factory() as db:
        canonical = await db.scalar(
            select(DataSnapshot).where(DataSnapshot.schema_version == SCHEMA)
        )
        row = await db.get(MacroObservation, ids[0])
        if defect == "unit":
            row.unit = "lin"
        elif defect == "vintage":
            row.real_time_start -= timedelta(days=1)
        elif defect == "calendar":
            row.calendar_evidence_ref_id = uuid4()
        else:
            payload = copy.deepcopy(canonical.payload)
            if defect == "raw":
                payload["captures"][1]["raw_text"] = "{}"
            else:
                payload["captures"][0]["receipt"]["received_at"] = "2026-08-28T12:00:00+00:00"
            canonical.payload = payload
        with db.no_autoflush:
            visible = await read_fred_observations(db, as_of=datetime.now(UTC))
        assert row.evidence_ref_id not in visible
        await db.rollback()
    async with factory() as db:
        assert len(await read_fred_observations(db, as_of=datetime.now(UTC))) == 2


@pytest.mark.asyncio
async def test_same_vintage_changed_content_creates_local_revision_without_rewriting(
    migrated_database_url,
):
    original = parse()
    metadata, page = captured_payloads()
    page["observations"][0]["value"] = "4.12"
    changed = parse(metadata, page)
    factory = session_factory(settings(migrated_database_url))
    async with factory() as db, db.begin():
        old_ids = await import_fred_vintage(db, collection=original, store=MemoryStore())
        old_ref = await db.get(
            ResearchEvidenceRef, (await db.get(MacroObservation, old_ids[0])).evidence_ref_id
        )
        old_hash = old_ref.content_hash
        new_ids = await import_fred_vintage(db, collection=changed, store=MemoryStore())
        assert not set(old_ids).intersection(new_ids)
        assert old_ref.content_hash == old_hash
        assert old_ref.revision.startswith("local-2026-08-28:2026-08-24:")
        assert len(await read_fred_observations(db, as_of=datetime.now(UTC))) == 4
