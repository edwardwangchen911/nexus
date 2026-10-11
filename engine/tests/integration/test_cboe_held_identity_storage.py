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
            from nexus.ops.fosun_local_refresh import (
                _load_local_broker_identity,
                _resolve_local_instrument,
            )
            from nexus.providers.broker.fosun_contracts import PositionItem

            def reader():
                return store

            assert (
                await _resolve_local_instrument(
                    s,
                    "usTEST",
                    currency="USD",
                    identity_date=datetime.now(UTC).date(),
                    expected_id=result.instrument_id,
                    identity_store_factory=reader,
                )
                == result.instrument_id
            )
            broker_identity = await _load_local_broker_identity(
                s,
                holdings=(PositionItem.model_validate(h.items[0]),),
                identity_store_factory=reader,
            )
            assert broker_identity.instrument_ids["usTEST"] == result.instrument_id
            from nexus.market.scope import US_EQUITY_EXCHANGES

            assert "BATS" not in US_EQUITY_EXCHANGES
            from nexus.ops.fosun_local_refresh import _ensure_instrument

            assert (
                await _ensure_instrument(
                    s,
                    "usTEST",
                    currency="USD",
                    valid_from=datetime.now(UTC).date(),
                    expected_id=result.instrument_id,
                    identity_store_factory=reader,
                )
                == result.instrument_id
            )
            with pytest.raises(ValueError, match="LOCAL_BROKER_INSTRUMENT_INVALID"):
                await _resolve_local_instrument(
                    s, "usTEST", currency="USD", identity_date=datetime.now(UTC).date()
                )
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
            with pytest.raises(ValueError, match="LOCAL_BROKER_INSTRUMENT_INVALID"):
                await _resolve_local_instrument(
                    s,
                    "usTEST",
                    currency="USD",
                    identity_date=datetime.now(UTC).date(),
                    identity_store_factory=lambda: store,
                )
            with pytest.raises(ValueError, match="LOCAL_BROKER_INSTRUMENT_INVALID"):
                await _load_local_broker_identity(
                    s,
                    holdings=(PositionItem.model_validate(h.items[0]),),
                    identity_store_factory=lambda: store,
                )
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
            from nexus.ops.fosun_local_refresh import _resolve_local_instrument

            with pytest.raises(ValueError, match="LOCAL_BROKER_INSTRUMENT_INVALID"):
                await _resolve_local_instrument(
                    s,
                    "usTEST",
                    currency="USD",
                    identity_date=datetime.now(UTC).date(),
                    identity_store_factory=lambda: store,
                )
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
            from nexus.ops.fosun_local_refresh import _resolve_local_instrument

            # Latest corrupt evidence cannot fall back to an older valid proof.
            with pytest.raises(ValueError, match="LOCAL_BROKER_INSTRUMENT_INVALID"):
                await _resolve_local_instrument(
                    s,
                    "usTEST",
                    currency="USD",
                    identity_date=datetime.now(UTC).date(),
                    identity_store_factory=lambda: store,
                )
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
    from nexus.ops.fosun_local_refresh import _load_local_broker_identity
    from nexus.providers.broker.fosun_contracts import PositionItem

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
            identity = await _load_local_broker_identity(
                s,
                holdings=(PositionItem.model_validate(h.items[0]),),
                identity_store_factory=lambda: store,
            )
            assert identity.instrument_ids["usTEST"] == result.instrument_id
            assert await s.scalar(select(func.count()).select_from(UniverseSnapshot)) == 0
            assert await s.scalar(select(func.count()).select_from(DataSnapshot)) == 1
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.parametrize("corrupt_proof", [False, True])
@pytest.mark.asyncio
async def test_broker_refresh_persists_bats_holding_and_api_readback(
    migrated_database_url, sources, monkeypatch, corrupt_proof
):
    """Normal refresh wiring with synthetic gateway pages and a real isolated DB."""
    from types import SimpleNamespace

    import httpx
    from helpers.portfolio_api import app, headers
    from nexus.ops import fosun_local_refresh as refresh
    from nexus.persistence.models.portfolio import PortfolioSnapshotRecord
    from nexus.providers.broker.gateway_contracts import GatewayResponsePage

    contents, dirs, pages = sources
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    class SnapshotStore(MemoryStore):
        async def read_normalized(self, *args, **kwargs):
            return await self.read_raw(*args, **kwargs)

    store = SnapshotStore()
    a, h = fresh_pages(pages)
    observed = datetime.now(UTC) - timedelta(seconds=2)
    account = a.items[0]["external_account_id"]
    h = h.model_copy(
        update={
            "as_of": observed,
            "items": (
                {
                    **h.items[0],
                    "average_cost": "10",
                    "current_price": "12",
                    "provider_as_of": observed.isoformat(),
                },
            ),
        }
    )
    calls = []

    def page(kind, items):
        calls.append(kind)
        return GatewayResponsePage(
            items=tuple(items),
            source_request_id=f"synthetic-{kind}-{len(calls)}",
            has_more=False,
            next_cursor=None,
            as_of=observed,
            quality="PASS",
        )

    class Broker:
        async def read_accounts(self):
            return page("accounts", a.items)

        async def read_holdings(self, account_id, *, cursor, limit):
            assert account_id == account and cursor is None and limit == 100
            return page("holdings", h.items)

        async def read_cash(self, account_id):
            assert account_id == account
            return page(
                "cash",
                [
                    {
                        "kind": "cash",
                        "external_account_id": account,
                        "currency": "USD",
                        "cash_balance": "1000",
                        "available_cash": "1000",
                        "provider_as_of": observed.isoformat(),
                    }
                ],
            )

        async def read_orders(self, account_id, *, cursor, limit):
            return page("orders", [])

        async def read_cash_flows(self, account_id, **kwargs):
            return page("cash_flows", [])

        async def aclose(self):
            calls.append("closed")

    try:
        async with factory() as s, s.begin():
            assert (await s.scalar(text("SELECT current_database()"))).startswith("nexus_test_")
            await api().import_cboe_held_identity(
                s,
                store,
                symbol="TEST",
                contents=contents,
                directory_contents=dirs,
                accounts_page=a,
                holdings_page=h,
            )
        if corrupt_proof:
            store.objects[next(iter(store.objects))] = b"invalid"
        monkeypatch.setattr(refresh, "_broker_settings", lambda settings: object())
        monkeypatch.setattr(refresh, "FosunGatewayClient", lambda settings: Broker())
        monkeypatch.setattr(refresh, "S3ProviderEvidenceStore", lambda settings: store)
        if corrupt_proof:
            with pytest.raises(ValueError, match="LOCAL_BROKER_INSTRUMENT_INVALID"):
                await refresh.refresh_local_fosun_broker(
                    SimpleNamespace(database_url=migrated_database_url), now=observed
                )
            assert calls == ["accounts", "holdings", "closed"]
            async with factory() as s:
                assert (
                    await s.scalar(select(func.count()).select_from(PortfolioSnapshotRecord)) == 0
                )
            return
        result = await refresh.refresh_local_fosun_broker(
            SimpleNamespace(database_url=migrated_database_url), now=observed
        )
        assert result["snapshot_status"] == "READY"
        assert result["provider_requests"]["quotes"] == result["provider_requests"]["bars"] == 0
        async with factory() as s:
            portfolio_id = await s.scalar(select(PortfolioSnapshotRecord.portfolio_id))
            assert await s.scalar(select(func.count()).select_from(PortfolioSnapshotRecord)) == 1
            assert await s.scalar(select(func.count()).select_from(UniverseSnapshot)) == 0
        path = f"/internal/v1/portfolios/{portfolio_id}/snapshots/latest"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app(migrated_database_url, now=datetime.now(UTC))),
            base_url="http://testserver",
        ) as client:
            response = await client.get(path, headers=headers("GET", path))
        assert response.status_code == 200
        data = response.json()["data"]
        assert data["positions"][0]["symbol"] == "TEST"
        assert data["positions"][0]["read_only"] is True
        assert data["execution_locked"] is True
    finally:
        await factory.kw["bind"].dispose()
