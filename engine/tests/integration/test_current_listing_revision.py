"""Synthetic append-only listing correction, explicitly consumed without rewriting history."""

import asyncio
import importlib
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import exchange_calendars
import pytest
from helpers.current_identity import MemoryStore, archives, filing, profile
from nexus.market.current_identity_storage import import_current_common_stock, read_current_identity
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias, UniverseSnapshot
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text


async def seed(factory, *, currency="USD", asset_type="common_stock"):
    identifier = uuid4()
    async with factory() as s, s.begin():
        s.add(
            Instrument(
                id=identifier,
                asset_type=asset_type,
                exchange="XNAS",
                currency=currency,
                risk_bucket="EQUITY",
                tick_size=None,
            )
        )
        await s.flush()
        s.add(
            InstrumentAlias(
                instrument_id=identifier,
                symbol="AAPL",
                alias_type="ticker",
                valid_from=datetime.now(UTC).date() - timedelta(days=3),
                valid_to=None,
            )
        )
    return identifier


def config(url):
    return EngineSettings(database_url=url).model_copy(
        update={"object_store_endpoint": "http://127.0.0.1:9000"}
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("old_proof", [False, True])
async def test_listing_revision_only_changes_explicit_current_consumer(
    migrated_database_url, tmp_path, old_proof
):
    ops = importlib.import_module("nexus.ops.instrument_identity_local")
    revise = getattr(ops, "revise_listing")
    factory = session_factory(config(migrated_database_url))
    store = MemoryStore()
    identifier = await seed(factory)
    fm, sc = archives(tmp_path / "new")
    old_id = None
    try:
        if old_proof:
            old_fm, old_sc = archives(
                tmp_path / "old",
                fmp=profile(exchange="NASDAQ"),
                sec=filing()
                .replace(b"exch:XNYS", b"exch:XNAS")
                .replace(b"New York Stock Exchange", b"The Nasdaq Stock Market LLC"),
            )
            async with factory() as s, s.begin():
                previous = await import_current_common_stock(
                    s, store, symbol="AAPL", fmp_archive=old_fm, sec_archive=old_sc
                )
                old_id = previous.snapshot_id
        async with factory() as s:
            instrument = await s.get(Instrument, identifier)
            original_exchange = instrument.exchange
            before = await s.scalar(select(func.count()).select_from(DataSnapshot))
        first, replay = await asyncio.gather(
            revise(
                config(migrated_database_url),
                symbol="AAPL",
                fmp_archive=fm,
                sec_archive=sc,
                store=store,
            ),
            revise(
                config(migrated_database_url),
                symbol="AAPL",
                fmp_archive=fm,
                sec_archive=sc,
                store=store,
            ),
        )
        assert first.instrument_id == identifier and first.snapshot_id == replay.snapshot_id
        assert {first.status, replay.status} == {"created", "existing"}
        module = importlib.import_module("nexus.market.current_listing_revision")
        async with factory() as s:
            proof = await module.read_listing_revision(s, store, snapshot_id=first.snapshot_id)
            assert proof.original_exchange == original_exchange
            assert proof.identity.exchange == "XNYS" and proof.instrument_id == identifier
            assert (await s.get(Instrument, identifier)).exchange == original_exchange
            assert await s.scalar(select(func.count()).select_from(DataSnapshot)) == before + 1
            assert await s.scalar(select(func.count()).select_from(UniverseSnapshot)) == 0
            with pytest.raises(ValueError):
                await read_current_identity(s, store, snapshot_id=first.snapshot_id)
            if old_id:
                assert (
                    await read_current_identity(s, store, snapshot_id=old_id)
                ).identity.exchange == "XNAS"
            with pytest.raises(ValueError):
                await module.read_listing_revision(
                    s,
                    store,
                    snapshot_id=first.snapshot_id,
                    as_of=proof.checked_at - timedelta(microseconds=1),
                )
        effective = (
            exchange_calendars.get_calendar("XNYS")
            .date_to_session(datetime.now(UTC).date() + timedelta(days=7), direction="next")
            .date()
        )
        old = await ops.prepare_universe(
            config(migrated_database_url), effective_session=effective, store=store
        )
        new = await ops.prepare_universe(
            config(migrated_database_url),
            effective_session=effective,
            store=store,
            use_listing_revisions=True,
        )
        old_member = next(m for m in old["members"] if m["symbol"] == "AAPL")
        new_member = next(m for m in new["members"] if m["symbol"] == "AAPL")
        assert old_member["proof_snapshot_id"] == (str(old_id) if old_id else None)
        assert new_member["proof_snapshot_id"] == str(first.snapshot_id)
        assert new_member["listing_exchange"] == "XNYS"
        assert new_member["instrument_id"] == str(identifier)
        assert new["candidate_version"] != old["candidate_version"]
        assert len(new["members"]) == 49 and new["candidate"] is None
        assert new["historical_backtest_eligible"] is False and new["execution_locked"] is True
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["currency", "type", "cik", "raw", "fingerprint", "alias"])
async def test_listing_revision_rejects_non_listing_conflicts(
    migrated_database_url, tmp_path, case
):
    ops = importlib.import_module("nexus.ops.instrument_identity_local")
    revise = getattr(ops, "revise_listing")
    factory = session_factory(config(migrated_database_url))
    store = MemoryStore()
    identifier = await seed(
        factory,
        currency="HKD" if case == "currency" else "USD",
        asset_type="equity_etf" if case == "type" else "common_stock",
    )
    fm, sc = archives(tmp_path)
    try:
        if case == "cik":
            async with factory() as s, s.begin():
                s.add(
                    InstrumentAlias(
                        instrument_id=identifier,
                        alias_type="cik",
                        symbol="654321",
                        valid_from=datetime.now(UTC).date(),
                        valid_to=None,
                    )
                )
        if case not in {"raw", "fingerprint", "alias"}:
            with pytest.raises(ValueError):
                await revise(
                    config(migrated_database_url),
                    symbol="AAPL",
                    fmp_archive=fm,
                    sec_archive=sc,
                    store=store,
                )
            async with factory() as s:
                assert await s.scalar(select(func.count()).select_from(DataSnapshot)) == 0
        else:
            imported = await revise(
                config(migrated_database_url),
                symbol="AAPL",
                fmp_archive=fm,
                sec_archive=sc,
                store=store,
            )
            target_id = imported.snapshot_id
            if case == "raw":
                store.objects[next(iter(store.objects))] = b"corrupt"
            elif case == "alias":
                async with factory() as s, s.begin():
                    await s.execute(
                        text(
                            "ALTER TABLE instrument_aliases DROP CONSTRAINT "
                            "ex_instrument_aliases_ticker_instrument_period"
                        )
                    )
                    s.add(
                        InstrumentAlias(
                            instrument_id=identifier,
                            symbol="OTHER",
                            alias_type="ticker",
                            valid_from=datetime.now(UTC).date(),
                            valid_to=None,
                        )
                    )
            else:
                async with factory() as s, s.begin():
                    original = await s.get(DataSnapshot, imported.snapshot_id)
                    payload = dict(original.payload)
                    payload["instrument_fingerprint"] = "0" * 64
                    bad = DataSnapshot(
                        source=original.source,
                        schema_version=original.schema_version,
                        provider_object_id=original.provider_object_id,
                        revision="tampered",
                        observed_at=original.observed_at,
                        received_at=original.received_at,
                        available_at=original.available_at,
                        payload=payload,
                    )
                    s.add(bad)
                    await s.flush()
                    target_id = bad.id
            module = importlib.import_module("nexus.market.current_listing_revision")
            async with factory() as s:
                with pytest.raises(ValueError):
                    await module.read_listing_revision(s, store, snapshot_id=target_id)
        if case == "fingerprint":
            effective = (
                exchange_calendars.get_calendar("XNYS")
                .date_to_session(datetime.now(UTC).date() + timedelta(days=7), direction="next")
                .date()
            )
            report = await ops.prepare_universe(
                config(migrated_database_url),
                effective_session=effective,
                store=store,
                use_listing_revisions=True,
            )
            member = next(m for m in report["members"] if m["symbol"] == "AAPL")
            assert member["gap"] == "IDENTITY_PROOF_INVALID"
            assert member["proof_snapshot_id"] is None
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_revision_reader_rejects_snapshot_created_after_cutoff(
    migrated_database_url, tmp_path
):
    from sqlalchemy import event

    ops = importlib.import_module("nexus.ops.instrument_identity_local")
    factory = session_factory(config(migrated_database_url))
    await seed(factory)
    fm, sc = archives(tmp_path)

    def future_row(_mapper, _connection, target):
        if target.source == "current_listing_revision":
            target.created_at = datetime.now(UTC) + timedelta(days=1)

    event.listen(DataSnapshot, "before_insert", future_row)
    try:
        with pytest.raises(ValueError, match="CURRENT_LISTING_READBACK_INVALID"):
            await ops.revise_listing(
                config(migrated_database_url),
                symbol="AAPL",
                fmp_archive=fm,
                sec_archive=sc,
                store=MemoryStore(),
            )
    finally:
        event.remove(DataSnapshot, "before_insert", future_row)
        await factory.kw["bind"].dispose()
