"""Isolated PostgreSQL registration/readback with handwritten synthetic archives."""

import asyncio
import importlib
import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from helpers.current_identity import MemoryStore
from nexus.persistence.models.data import DataArtifact, DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias, UniverseSnapshot
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select, text
from sqlalchemy.exc import IntegrityError


@pytest.fixture
def sources():
    module = importlib.import_module("helpers.cboe_fund_sources")
    pages = importlib.import_module("helpers.cboe_holding_evidence").pages
    contents = list(module.source_chain_contents())
    now = datetime.now(UTC)
    # Handwritten fixtures use historical identity facts and current diagnostic
    # captures. Advance only synthetic clocks, never a captured real source.
    for old, age in (
        (b"14 September 2026", -30),
        (b"28 September 2026", -15),
        (b"12 October 2026", 30),
    ):
        contents[4] = contents[4].replace(
            old, (now + timedelta(days=age)).strftime("%d %B %Y").encode()
        )
    listing_time = (now - timedelta(minutes=2)).astimezone(ZoneInfo("America/New_York"))
    contents[8] = contents[8].replace(
        b"2026-10-10 20:00:04 EDT", listing_time.strftime("%Y-%m-%d %H:%M:%S %Z").encode()
    )
    directories = list(module.directories())
    for values in (contents, directories):
        for number in range(0, len(values), 2):
            receipt = json.loads(values[number + 1])
            receipt["received_at"] = (now - timedelta(minutes=1)).isoformat()
            receipt["raw_sha256" if number == 0 and values is contents else "sha256"] = sha256(
                values[number]
            ).hexdigest()
            receipt["raw_byte_length" if number == 0 and values is contents else "byte_length"] = (
                len(values[number])
            )
            values[number + 1] = json.dumps(receipt).encode()
    return tuple(contents), tuple(directories), pages


def api():
    return importlib.import_module("nexus.market.cboe_held_identity_storage")


def fresh_pages(pages):
    a, h = pages()
    # A provider observation precedes registration; do not make correctness
    # depend on submillisecond host/container clock alignment.
    now = datetime.now(UTC) - timedelta(minutes=1)
    return a.model_copy(update={"as_of": now}), h.model_copy(update={"as_of": now})


@pytest.mark.asyncio
async def test_register_repeat_readback_and_keep_universe_closed(migrated_database_url, sources):
    contents, dirs, pages = sources
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = MemoryStore()
    a, h = fresh_pages(pages)
    try:
        async with factory() as s, s.begin():
            assert (await s.scalar(text("SELECT current_database()"))).startswith("nexus_test_")
            result = await api().import_cboe_held_identity(
                s,
                store,
                symbol="TEST",
                contents=contents,
                directory_contents=dirs,
                accounts_page=a,
                holdings_page=h,
            )
        async with factory() as s, s.begin():
            proof = await api().read_cboe_held_identity(s, store, snapshot_id=result.snapshot_id)
            repeat = await api().import_cboe_held_identity(
                s,
                store,
                symbol="TEST",
                contents=contents,
                directory_contents=dirs,
                accounts_page=a,
                holdings_page=h,
            )
            assert repeat.status == "existing" and repeat.snapshot_id == result.snapshot_id
            assert proof.evidence.identity.exchange == "BATS"
            assert proof.evidence.identity.strategy_kind == "daily_2x_long_single_stock"
            assert proof.evidence.execution_locked and not proof.evidence.new_position_eligible
            from nexus.market.scope import US_EQUITY_EXCHANGES

            assert "BATS" not in US_EQUITY_EXCHANGES
            assert await s.scalar(select(func.count()).select_from(UniverseSnapshot)) == 0
            assert await s.scalar(select(func.count()).select_from(Instrument)) == 1
            assert await s.scalar(select(func.count()).select_from(InstrumentAlias)) == 1
            original = await s.scalar(select(DataArtifact).where(DataArtifact.source == "fmp"))
            assert original.observed_at == datetime.fromisoformat(
                json.loads(contents[1])["received_at"]
            )
        a2, h2 = fresh_pages(pages)
        h2 = h2.model_copy(update={"source_request_id": "synthetic-new-holdings-request"})
        async with factory() as s, s.begin():
            second = await api().import_cboe_held_identity(
                s,
                store,
                symbol="TEST",
                contents=contents,
                directory_contents=dirs,
                accounts_page=a2,
                holdings_page=h2,
            )
            assert second.status == "created"
            assert second.instrument_id == result.instrument_id
            assert second.snapshot_id != result.snapshot_id
            assert await s.scalar(select(func.count()).select_from(Instrument)) == 1
            assert await s.scalar(select(func.count()).select_from(InstrumentAlias)) == 1
        # Stored object corruption must be noticed, not trusted because an alias exists.
        key = next(iter(store.objects))
        store.objects[key] += b"\n"
        async with factory() as s:
            with pytest.raises(ValueError, match="CBOE_HELD_IDENTITY_READBACK_INVALID"):
                await api().read_cboe_held_identity(s, store, snapshot_id=result.snapshot_id)
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_unproven_existing_master_is_not_adopted(migrated_database_url, sources):
    from nexus.market.universe import stable_instrument_id

    contents, dirs, pages = sources
    a, h = fresh_pages(pages)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = MemoryStore()
    try:
        async with factory() as s, s.begin():
            assert (await s.scalar(text("SELECT current_database()"))).startswith("nexus_test_")
            s.add(
                Instrument(
                    id=stable_instrument_id("TEST"),
                    asset_type="equity_etf",
                    exchange="BATS",
                    currency="USD",
                    risk_bucket="EQUITY",
                    tick_size=None,
                )
            )
            await s.flush()
            s.add(
                InstrumentAlias(
                    id=uuid4(),
                    instrument_id=stable_instrument_id("TEST"),
                    symbol="TEST",
                    valid_from=datetime.now(UTC).date(),
                )
            )
        async with factory() as s, s.begin():
            with pytest.raises(ValueError, match="CBOE_HELD_IDENTITY_MASTER_CONFLICT"):
                await api().import_cboe_held_identity(
                    s,
                    store,
                    symbol="TEST",
                    contents=contents,
                    directory_contents=dirs,
                    accounts_page=a,
                    holdings_page=h,
                )
            assert await s.scalar(select(func.count()).select_from(DataSnapshot)) == 0
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_tampered_snapshot_and_master_are_rejected(migrated_database_url, sources):
    contents, dirs, pages = sources
    a, h = fresh_pages(pages)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = MemoryStore()
    try:
        async with factory() as s, s.begin():
            assert (await s.scalar(text("SELECT current_database()"))).startswith("nexus_test_")
            result = await api().import_cboe_held_identity(
                s,
                store,
                symbol="TEST",
                contents=contents,
                directory_contents=dirs,
                accounts_page=a,
                holdings_page=h,
            )
        async with factory() as s, s.begin():
            async with s.begin_nested():
                instrument = await s.get(Instrument, result.instrument_id)
                instrument.asset_type = "common_stock"
                with pytest.raises(IntegrityError, match="immutable"):
                    await s.flush()
            # Do not disable immutable-table guards to manufacture a bad update.
        async with factory() as s, s.begin():
            original = await s.get(DataSnapshot, result.snapshot_id)
            forged = DataSnapshot(
                source=original.source,
                schema_version=original.schema_version,
                provider_object_id=original.provider_object_id,
                revision="synthetic-forged",
                observed_at=original.observed_at + timedelta(microseconds=1),
                received_at=original.received_at,
                available_at=original.available_at,
                payload={**original.payload, "revision_override": "untrusted"},
            )
            s.add(forged)
            await s.flush()
            with pytest.raises(ValueError, match="CBOE_HELD_IDENTITY_READBACK_INVALID"):
                await api().read_cboe_held_identity(s, store, snapshot_id=forged.id)
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_concurrent_same_request_registers_exactly_once(migrated_database_url, sources):
    contents, dirs, pages = sources
    a, h = fresh_pages(pages)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = MemoryStore()

    async def register():
        async with factory() as s, s.begin():
            assert (await s.scalar(text("SELECT current_database()"))).startswith("nexus_test_")
            return await api().import_cboe_held_identity(
                s,
                store,
                symbol="TEST",
                contents=contents,
                directory_contents=dirs,
                accounts_page=a,
                holdings_page=h,
            )

    try:
        first, second = await asyncio.gather(register(), register())
        assert first.snapshot_id == second.snapshot_id
        assert {first.status, second.status} == {"created", "existing"}
        async with factory() as s:
            assert await s.scalar(select(func.count()).select_from(DataSnapshot)) == 1
            assert await s.scalar(select(func.count()).select_from(Instrument)) == 1
            assert await s.scalar(select(func.count()).select_from(InstrumentAlias)) == 1
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_normal_held_ingress_reads_once_then_registers_and_resolves(
    migrated_database_url, sources, tmp_path
):
    from nexus.ops.cboe_identity_local import import_held_identity

    contents, dirs, pages = sources
    a, h = fresh_pages(pages)
    store = MemoryStore()
    settings = EngineSettings(
        database_url=migrated_database_url, object_store_endpoint="http://127.0.0.1:9000"
    )
    factory = session_factory(settings)
    tmp_path.chmod(0o700)
    locations = {}
    names = [
        ("fmp", ("profile.json", "receipt.json")),
        ("sec", ("submission.txt", "receipt.json")),
        (
            "mic",
            (
                "publication.html",
                "publication.html.receipt.json",
                "ISO10383_MIC.csv",
                "ISO10383_MIC.csv.receipt.json",
            ),
        ),
        (
            "listing",
            (
                "listed.html",
                "listed.html.receipt.json",
                "listed.csv",
                "listed.csv.receipt.json",
                "product.html",
                "product.html.receipt.json",
            ),
        ),
    ]
    n = 0
    for folder, files in names:
        directory = tmp_path / folder
        directory.mkdir(mode=0o700)
        locations[folder] = directory
        for name in files:
            path = directory / name
            path.write_bytes(contents[n])
            path.chmod(0o600)
            n += 1
    directory = tmp_path / "directories"
    directory.mkdir(mode=0o700)
    for name, raw in zip(
        ("company.json", "company.receipt.json", "fund.json", "fund.receipt.json"),
        dirs,
        strict=True,
    ):
        path = directory / name
        path.write_bytes(raw)
        path.chmod(0o600)

    class Broker:
        accounts = 0
        holdings = 0

        async def read_accounts(self):
            self.accounts += 1
            return a

        async def read_holdings(self, account_id, *, cursor, limit):
            self.holdings += 1
            assert cursor is None and limit == 100
            return h

    broker = Broker()
    try:
        async with factory() as s:
            assert (await s.scalar(text("SELECT current_database()"))).startswith("nexus_test_")
        result = await import_held_identity(
            settings,
            None,
            symbol="TEST",
            fmp_archive=locations["fmp"],
            submission_file=locations["sec"] / "submission.txt",
            mic_archive=locations["mic"],
            listing_archive=locations["listing"],
            company_directory_file=directory / "company.json",
            company_receipt_file=directory / "company.receipt.json",
            fund_directory_file=directory / "fund.json",
            fund_receipt_file=directory / "fund.receipt.json",
            store=store,
            broker=broker,
        )
        assert (broker.accounts, broker.holdings) == (1, 1)
        async with factory() as s:
            assert await s.scalar(select(func.count()).select_from(UniverseSnapshot)) == 0
            assert await s.scalar(select(func.count()).select_from(DataSnapshot)) == 1
    finally:
        await factory.kw["bind"].dispose()
