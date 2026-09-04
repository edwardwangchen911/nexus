"""Real archive/import/freeze/HTTP; only external HTTP and object transport are synthetic."""

import json
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from urllib.parse import quote
from uuid import UUID

import httpx
import pytest
from helpers.portfolio_api import app, headers
from helpers.research_snapshots import seed_filing
from nexus.agents.models import ResearchRequest
from nexus.agents.persistence import admit_research_run
from nexus.agents.snapshots import prepare_research_snapshot
from nexus.api.research_inputs import read_research_inputs
from nexus.ops.daily_once import DailyOnceSettings
from nexus.ops.fmp_profile_local import import_profile
from nexus.ops.fred_local import import_collection
from nexus.ops.openai_local import prepare_database
from nexus.persistence.models.agents import AgentDataSnapshotEvidenceRecord as Frozen
from nexus.persistence.models.data import DataSnapshot, MacroObservation
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.providers.fred_http import FredHttpTransport
from nexus.providers.http import AllowedOrigin, FixedOriginHttpClient, HttpRequestPolicy
from nexus.research.fred_collection import collect_fred_vintage, load_fred_collection
from pydantic import SecretStr
from sqlalchemy import event, select
from sqlalchemy.engine import Engine
from unit.research.test_fred_observations import captured_payloads, parse

from .test_fmp_profile import INSTRUMENT, setup_profile


async def setup_inputs(url, tmp_path, *, value="0", has_base=True):
    settings, factory, store, _, _ = await setup_profile(url, tmp_path / "profile")
    await prepare_database(settings)
    async with factory.begin() as session:
        if has_base:
            await seed_filing(session, instrument_id=INSTRUMENT, seed_instrument=False)
        old = await prepare_research_snapshot(session, INSTRUMENT, as_of=datetime.now(UTC))
    imported = await import_profile(
        settings,
        instrument_id=INSTRUMENT,
        symbol="AAPL",
        collection_dir=tmp_path / "profile",
        store=store,
    )
    directory = tmp_path / "fred"
    directory.mkdir(mode=0o700)
    metadata, page = captured_payloads()
    page["observations"][-1]["value"] = value
    bodies = [metadata, page]

    def respond(request):
        return httpx.Response(200, content=json.dumps(bodies.pop(0)).encode())

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        transport = FredHttpTransport(
            FixedOriginHttpClient(
                provider_name="fred",
                origin=AllowedOrigin(scheme="https", host="api.stlouisfed.org"),
                policy=HttpRequestPolicy(max_attempts=1),
                client=client,
            ),
            SecretStr("synthetic-key"),
        )
        await collect_fred_vintage(parse().request, transport=transport, output_dir=directory)
    await import_collection(
        DailyOnceSettings.model_validate(settings.model_dump()),
        collection=load_fred_collection(directory),
        store=store,
    )
    decision = datetime.now(UTC)
    async with factory.begin() as session:
        frozen = await prepare_research_snapshot(session, INSTRUMENT, as_of=decision)
        if frozen is None:
            return factory, old, frozen, decision, imported, []
        runs = []
        for index, snapshot in enumerate((old, frozen)):
            cutoff = datetime.fromisoformat(snapshot.payload["as_of"])
            admitted = await admit_research_run(
                session,
                actor_id="test",
                route="/internal/v1/research-runs",
                idempotency_key=f"provider-display-{index}",
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
    return factory, old, frozen, decision, imported, runs


@pytest.mark.asyncio
@pytest.mark.parametrize("value", ["0", "-0.125"])
async def test_normal_archives_reach_admission_and_exact_run_without_new_writes(
    migrated_database_url, tmp_path, monkeypatch, value
):
    factory, old, frozen, decision, imported, runs = await setup_inputs(
        migrated_database_url, tmp_path, value=value
    )
    old_payload, old_hash = dict(old.payload), old.revision
    async with factory() as session:
        result = await read_research_inputs(
            session, snapshot_id=frozen.id, instrument_id=INSTRUMENT, as_of=decision
        )
        assert len(result.company_profiles) == len(result.macro_observations) == 1
        profile, macro = result.company_profiles[0], result.macro_observations[0]
        assert profile.evidence_ref_id == UUID(imported["evidence_id"])
        assert profile.industry == "Vendor Industry" and profile.sector_taxonomy is None
        assert not profile.financial_fact_eligible and profile.execution_locked
        assert Decimal(macro.value) == Decimal(value) and macro.unit == "Percent"
        assert macro.series == "DGS10" and macro.observed_on.isoformat() == "2026-08-26"
        assert str(macro.calendar_evidence_ref_id) in frozen.payload["evidence_ids"]
        assert macro.content_hash == macro.content_hash.lower() and len(macro.content_hash) == 64
        assert not macro.historical_backtest_eligible and macro.execution_locked
        for snapshot_id, instrument_id, cutoff in (
            (old.id, INSTRUMENT, decision),
            (frozen.id, UUID(int=999), decision),
            (frozen.id, INSTRUMENT, datetime(2026, 1, 1, tzinfo=UTC)),
        ):
            other = await read_research_inputs(
                session, snapshot_id=snapshot_id, instrument_id=instrument_id, as_of=cutoff
            )
            assert other is None or not (other.company_profiles or other.macro_observations)

    async def forbidden(*args, **kwargs):
        pytest.fail("Display must not call external sources or a model")

    monkeypatch.setattr(httpx.AsyncHTTPTransport, "handle_async_request", forbidden)
    writes = []

    def observe(connection, cursor, statement, parameters, context, many):
        if statement.lstrip().split()[0].upper() in {"INSERT", "UPDATE", "DELETE", "MERGE"}:
            writes.append(statement.lstrip())

    event.listen(Engine, "before_cursor_execute", observe)
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(migrated_database_url, now=decision)),
            base_url="http://test",
        ) as client:
            paths = [
                f"/internal/v1/instruments/{INSTRUMENT}/research-admission?as_of="
                + quote(decision.isoformat(), safe=""),
                f"/internal/v1/research-runs/{runs[1]}",
                f"/internal/v1/research-runs/{runs[0]}",
            ]
            for index, path in enumerate(paths):
                response = await client.get(path, headers=headers("GET", path))
                assert response.status_code == 200
                inputs = response.json()["data"]["inputs"]
                assert inputs["company_profiles"] == (
                    [] if index == 2 else [profile.model_dump(mode="json")]
                )
                assert inputs["macro_observations"] == (
                    [] if index == 2 else [macro.model_dump(mode="json")]
                )
        assert len(writes) == 3 and all(x.startswith("INSERT INTO used_nonces ") for x in writes)
    finally:
        event.remove(Engine, "before_cursor_execute", observe)
    assert old.payload == old_payload and old.revision == old_hash


@pytest.mark.asyncio
async def test_provider_only_does_not_create_research_base(migrated_database_url, tmp_path):
    factory, old, frozen, decision, _, _ = await setup_inputs(
        migrated_database_url, tmp_path, has_base=False
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
@pytest.mark.parametrize(
    "problem", ["profile_payload", "macro_payload", "calendar_hash", "macro_ref", "old_format"]
)
async def test_bad_optional_provider_preserves_independent_sec_and_other_source(
    migrated_database_url, tmp_path, problem
):
    factory, _, frozen, decision, _, _ = await setup_inputs(migrated_database_url, tmp_path)
    async with factory() as session:
        if problem == "old_format":
            row = await session.scalar(select(DataSnapshot).where(DataSnapshot.source == "fred"))
            row.schema_version = "legacy_macro_v1"
        elif problem.endswith("payload"):
            source = "fmp_company_profile" if problem.startswith("profile") else "fred"
            row = await session.scalar(select(DataSnapshot).where(DataSnapshot.source == source))
            row.payload = {"unexpected": "synthetic-private-canary"}
        else:
            source = "exchange_calendars" if problem == "calendar_hash" else "fred"
            row = await session.scalar(
                select(ResearchEvidenceRef).where(
                    ResearchEvidenceRef.source == source,
                    ResearchEvidenceRef.id.in_(
                        [UUID(value) for value in frozen.payload["evidence_ids"]]
                    ),
                )
            )
            row.content_hash = "0" * 64
        with session.no_autoflush:
            result = await read_research_inputs(
                session, snapshot_id=frozen.id, instrument_id=INSTRUMENT, as_of=decision
            )
        assert len(result.filings) == 1
        assert len(result.company_profiles) == (0 if problem.startswith("profile") else 1)
        assert len(result.macro_observations) == (1 if problem.startswith("profile") else 0)
        assert "synthetic-private-canary" not in result.model_dump_json()


@pytest.mark.asyncio
@pytest.mark.parametrize("other_calendar", [False, True])
async def test_macro_without_its_exact_frozen_calendar_is_not_displayed(
    migrated_database_url, tmp_path, other_calendar
):
    factory, _, frozen, decision, _, _ = await setup_inputs(migrated_database_url, tmp_path)
    async with factory.begin() as session:
        rows = list(
            await session.scalars(select(Frozen).where(Frozen.data_snapshot_id == frozen.id))
        )
        selected = [row for row in rows if row.source != "exchange_calendars"]
        if other_calendar:
            from dataclasses import replace

            from nexus.research.fred_observations import parse_collected_vintage
            from nexus.research.fred_storage import import_fred_vintage

            from .test_fred_vintage_storage import MemoryStore

            metadata, page = captured_payloads()
            for item in (metadata, metadata["seriess"][0], page, *page["observations"]):
                item["realtime_start"] = item["realtime_end"] = "2026-08-31"
            sample = parse()
            sample = parse_collected_vintage(
                sample.request.model_copy(
                    update={"vintage": sample.request.vintage.replace(day=31)}
                ),
                replace(sample.metadata, raw_payload=json.dumps(metadata).encode()),
                (replace(sample.pages[0], raw_payload=json.dumps(page).encode()),),
            )
            ids = await import_fred_vintage(session, collection=sample, store=MemoryStore())
            observation = await session.get(MacroObservation, ids[0])
            other = await session.get(ResearchEvidenceRef, observation.calendar_evidence_ref_id)
            assert other.id not in [row.evidence_ref_id for row in rows]
            selected.append(
                Frozen(
                    data_snapshot_id=frozen.id,
                    instrument_id=INSTRUMENT,
                    evidence_ref_id=other.id,
                    source=other.source,
                    provider_object_id=other.provider_object_id,
                    revision=other.revision,
                    available_at=other.available_at,
                    location=other.location,
                    content_hash=other.content_hash,
                    authority_kind="research",
                )
            )
            decision = datetime.now(UTC)
        # A deliberately incomplete historical freeze, not a mocked canonical reader.
        payload = {**frozen.payload, "evidence_ids": [str(row.evidence_ref_id) for row in selected]}
        incomplete = DataSnapshot(
            source="nexus_research",
            provider_object_id=str(INSTRUMENT),
            revision=sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest(),
            observed_at=decision,
            available_at=decision,
            received_at=decision,
            schema_version="research_filings_v1",
            payload=payload,
        )
        session.add(incomplete)
        await session.flush()
        for row in selected:
            session.add(
                Frozen(
                    data_snapshot_id=incomplete.id,
                    instrument_id=INSTRUMENT,
                    evidence_ref_id=row.evidence_ref_id,
                    source=row.source,
                    provider_object_id=row.provider_object_id,
                    revision=row.revision,
                    available_at=row.available_at,
                    location=row.location,
                    content_hash=row.content_hash,
                    authority_kind="research",
                )
            )
        await session.flush()
        result = await read_research_inputs(
            session, snapshot_id=incomplete.id, instrument_id=INSTRUMENT, as_of=decision
        )
        assert len(result.filings) == len(result.company_profiles) == 1
        assert result.macro_observations == ()


@pytest.mark.asyncio
async def test_forged_macro_row_cannot_borrow_a_valid_frozen_reference(
    migrated_database_url, tmp_path
):
    factory, _, frozen, decision, _, _ = await setup_inputs(migrated_database_url, tmp_path)
    async with factory.begin() as session:
        original = await session.scalar(
            select(MacroObservation).where(
                MacroObservation.evidence_ref_id.in_(
                    [UUID(value) for value in frozen.payload["evidence_ids"]]
                ),
            )
        )
        session.add(
            MacroObservation(
                source="fred",
                series="DGS2",
                observed_at=original.observed_at,
                available_at=original.available_at,
                revision=original.revision,
                value=Decimal("99"),
                unit="Percent",
                provider_object_id=original.provider_object_id,
                authority_version="research_macro_v1",
                real_time_start=original.real_time_start,
                real_time_end=original.real_time_end,
                evidence_ref_id=original.evidence_ref_id,
                calendar_evidence_ref_id=original.calendar_evidence_ref_id,
                calendar_source=original.calendar_source,
                calendar_provider_object_id=original.calendar_provider_object_id,
                calendar_revision=original.calendar_revision,
                display_sanity_only=False,
            )
        )
        await session.flush()
        result = await read_research_inputs(
            session, snapshot_id=frozen.id, instrument_id=INSTRUMENT, as_of=decision
        )
        assert [item.series for item in result.macro_observations] == ["DGS10"]
        assert result.macro_observations[0].observation_id == original.id
