"""T0 baseline selection from exact normal-import originals; no HTTP or business database."""

from copy import deepcopy
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

import pytest
from helpers.current_identity import MemoryStore
from helpers.disclosure_baseline import import_baseline_capture, import_baseline_identity
from nexus.monitoring.disclosure_context import (
    freeze_disclosure_baseline,
    validate_disclosure_baseline,
)
from nexus.ops.sec_index_local import import_index
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.models.runtime import Job, OutboxEvent
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text


async def fixture(database_url, tmp_path):
    settings = EngineSettings(
        database_url=database_url, object_store_endpoint="http://127.0.0.1:9000"
    )
    store = MemoryStore()
    identity = await import_baseline_identity(settings, store, tmp_path / "identity")
    first = await import_baseline_capture(settings, store, tmp_path / "first", identity=identity)
    return settings, store, identity, first


@pytest.mark.asyncio
async def test_normal_identity_import_binds_existing_candidate_instrument(
    migrated_database_url, tmp_path
):
    settings = EngineSettings(
        database_url=migrated_database_url, object_store_endpoint="http://127.0.0.1:9000"
    )
    factory = session_factory(settings)
    instrument_id, store = uuid4(), MemoryStore()
    async with factory() as session, session.begin():
        session.add(
            Instrument(
                id=instrument_id,
                asset_type="common_stock",
                exchange="NASDAQ",
                currency="USD",
                risk_bucket="EQUITY",
                tick_size=None,
            )
        )
        await session.flush()
        session.add(
            InstrumentAlias(
                instrument_id=instrument_id,
                symbol="AAPL",
                alias_type="ticker",
                valid_from=datetime.now(UTC).date(),
                valid_to=None,
            )
        )
    identity = await import_baseline_identity(
        settings, store, tmp_path / "identity", exchange="NASDAQ"
    )
    assert identity.instrument_id == instrument_id
    first = await import_baseline_capture(
        settings, store, tmp_path / "index", identity=identity, exchange="NASDAQ"
    )
    async with factory() as session:
        session.info["sec_exhibit_store"] = store
        frozen = await freeze_disclosure_baseline(
            session, instrument_id=instrument_id, as_of=datetime.now(UTC)
        )
        assert frozen.ready and frozen.baseline.snapshot_id == UUID(first["snapshot_id"])
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_normal_import_freezes_old_baseline_reimport_and_later_arrival_do_not_rewrite(
    migrated_database_url, tmp_path
):
    settings, store, identity, first = await fixture(migrated_database_url, tmp_path)
    factory = session_factory(settings)
    async with factory() as session:
        session.info["sec_exhibit_store"] = store
        as_of = await session.scalar(select(func.clock_timestamp()))
        frozen = await freeze_disclosure_baseline(
            session, instrument_id=identity.instrument_id, as_of=as_of
        )
        assert frozen.ready and frozen.baseline.snapshot_id == UUID(first["snapshot_id"])
        assert await validate_disclosure_baseline(session, frozen)
        original = frozen.model_dump_json()
    repeated = await import_index(
        settings,
        identity_snapshot_id=identity.snapshot_id,
        archive_dir=tmp_path / "first",
        store=store,
    )
    assert repeated["status"] == "existing" and repeated["snapshot_id"] == first["snapshot_id"]
    second = await import_baseline_capture(
        settings,
        store,
        tmp_path / "second",
        identity=identity,
        received=datetime.now(UTC) - timedelta(minutes=10),
    )
    async with factory() as session, session.begin():
        await session.execute(text("SET TRANSACTION READ ONLY"))
        session.info["sec_exhibit_store"] = store
        assert await validate_disclosure_baseline(session, frozen)
        assert frozen.model_dump_json() == original
        old = await freeze_disclosure_baseline(
            session, instrument_id=identity.instrument_id, as_of=as_of
        )
        assert old == frozen
        current = await freeze_disclosure_baseline(
            session, instrument_id=identity.instrument_id, as_of=datetime.now(UTC)
        )
        assert current.ready and current.baseline.snapshot_id == UUID(second["snapshot_id"])
        assert await session.scalar(select(func.count()).select_from(Job)) == 0
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 0
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_missing_late_and_wrong_scope_remain_explicit(migrated_database_url, tmp_path):
    settings, store, identity, first = await fixture(migrated_database_url, tmp_path)
    factory = session_factory(settings)
    async with factory() as session:
        session.info["sec_exhibit_store"] = store
        row = await session.get(DataSnapshot, UUID(first["snapshot_id"]))
        for instrument_id, as_of in (
            (uuid4(), datetime.now(UTC)),
            (identity.instrument_id, row.available_at - timedelta(seconds=1)),
        ):
            context = await freeze_disclosure_baseline(
                session, instrument_id=instrument_id, as_of=as_of
            )
            assert not context.ready and context.gap_reason == "SEC_BASELINE_MISSING"
            assert await validate_disclosure_baseline(session, context)
        good = await freeze_disclosure_baseline(
            session, instrument_id=identity.instrument_id, as_of=datetime.now(UTC)
        )
        for update in (
            {"snapshot_hash": "a" * 64},
            {"issuer_cik": "0000654321"},
            {"snapshot_id": uuid4()},
        ):
            changed = good.model_copy(update={"baseline": good.baseline.model_copy(update=update)})
            assert not await validate_disclosure_baseline(session, changed)
        wrong_instrument = uuid4()
        changed = good.model_copy(
            update={
                "instrument_id": wrong_instrument,
                "baseline": good.baseline.model_copy(update={"instrument_id": wrong_instrument}),
            }
        )
        assert not await validate_disclosure_baseline(session, changed)
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "future_field", ["observed_at", "available_at", "received_at", "created_at"]
)
async def test_each_local_time_excludes_late_capture(migrated_database_url, tmp_path, future_field):
    settings, store, identity, first = await fixture(migrated_database_url, tmp_path)
    factory = session_factory(settings)
    async with factory() as session, session.begin():
        session.info["sec_exhibit_store"] = store
        cutoff = datetime.now(UTC)
        row = await session.get(DataSnapshot, UUID(first["snapshot_id"]))
        values = {
            column.name: getattr(row, column.name)
            for column in row.__table__.columns
            if column.name not in {"id", "created_at"}
        }
        values.update(revision="b" * 64, observed_at=cutoff, created_at=cutoff)
        values[future_field] = cutoff + timedelta(days=1)
        session.add(DataSnapshot(**values))
        await session.flush()
        frozen = await freeze_disclosure_baseline(
            session, instrument_id=identity.instrument_id, as_of=cutoff
        )
        assert frozen.ready and frozen.baseline.snapshot_id == row.id
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["instrument", "issuer", "schema"])
async def test_corrupt_latest_metadata_is_invalid_not_missing_or_old_fallback(
    migrated_database_url, tmp_path, corruption
):
    settings, store, identity, first = await fixture(migrated_database_url, tmp_path)
    factory = session_factory(settings)
    async with factory() as session, session.begin():
        session.info["sec_exhibit_store"] = store
        row = await session.get(DataSnapshot, UUID(first["snapshot_id"]))
        values = {
            column.name: getattr(row, column.name)
            for column in row.__table__.columns
            if column.name not in {"id", "created_at"}
        }
        payload = deepcopy(row.payload)
        if corruption == "instrument":
            payload["index"]["instrument_id"] = str(uuid4())
        elif corruption == "issuer":
            payload["index"]["issuer_cik"] = "0000654321"
        else:
            values["schema_version"] = "invalid"
        values.update(payload=payload, revision="c" * 64, observed_at=datetime.now(UTC))
        session.add(DataSnapshot(**values))
        await session.flush()
        context = await freeze_disclosure_baseline(
            session, instrument_id=identity.instrument_id, as_of=datetime.now(UTC)
        )
        assert not context.ready and context.gap_reason == "SEC_BASELINE_INVALID"
    await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["corrupt", "missing_store", "unreadable_store"])
async def test_latest_invalid_never_falls_back_to_older_valid_capture(
    migrated_database_url, tmp_path, failure
):
    settings, store, identity, _ = await fixture(migrated_database_url, tmp_path)
    second = await import_baseline_capture(
        settings,
        store,
        tmp_path / "second",
        identity=identity,
        received=datetime.now(UTC) - timedelta(minutes=10),
    )
    factory = session_factory(settings)
    async with factory() as session:
        session.info["sec_exhibit_store"] = store
        good = await freeze_disclosure_baseline(
            session, instrument_id=identity.instrument_id, as_of=datetime.now(UTC)
        )
        if failure == "missing_store":
            session.info.clear()
        elif failure == "unreadable_store":
            store.objects.clear()
        else:
            row = await session.get(DataSnapshot, UUID(second["snapshot_id"]))
            key = row.payload["originals"][3]["uri"].removeprefix("s3://synthetic/")
            store.objects[key] = b"broken"
        context = await freeze_disclosure_baseline(
            session, instrument_id=identity.instrument_id, as_of=datetime.now(UTC)
        )
        assert not context.ready and context.gap_reason == "SEC_BASELINE_INVALID"
        assert not await validate_disclosure_baseline(session, good)
    await factory.kw["bind"].dispose()
