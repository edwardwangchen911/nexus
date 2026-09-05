"""Normal ETF entry, real isolated PostgreSQL, synthetic sources and object store."""

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import exchange_calendars
import pytest
from helpers.current_etf_identity import archives
from helpers.current_identity import MemoryStore
from nexus.market.current_etf_identity_storage import read_current_etf
from nexus.ops.etf_identity_local import import_identity
from nexus.ops.instrument_identity_local import prepare_universe
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias, UniverseSnapshot
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select


def config(url):
    return EngineSettings(database_url=url).model_copy(
        update={"object_store_endpoint": "http://127.0.0.1:9000"}
    )


@pytest.mark.asyncio
async def test_normal_import_concurrent_idempotence_and_strict_read(
    migrated_database_url, tmp_path
):
    settings = config(migrated_database_url)
    fm, sec, mapping = archives(tmp_path)
    store = MemoryStore()
    results = await asyncio.gather(
        *[
            import_identity(
                settings,
                symbol="IWM",
                fmp_archive=fm,
                sec_archive=sec,
                mapping_file=mapping,
                store=store,
            )
            for _ in range(2)
        ]
    )
    assert results[0].snapshot_id == results[1].snapshot_id
    assert {r.status for r in results} == {"created", "existing"}
    factory = session_factory(settings)
    try:
        async with factory() as s:
            proof = await read_current_etf(s, store, snapshot_id=results[0].snapshot_id)
            assert proof.identity.exchange == "ARCX"
            assert proof.identity.fmp_exchange == "AMEX"
            assert proof.identity.mapping_original_received_at is None
            assert proof.execution_locked and not proof.historical_backtest_eligible
            assert await s.scalar(select(func.count()).select_from(UniverseSnapshot)) == 0
            instrument = await s.get(Instrument, proof.instrument_id)
            assert instrument.tick_size is None
        effective = (
            exchange_calendars.get_calendar("XNYS")
            .date_to_session(datetime.now(UTC).date() + timedelta(days=7), direction="next")
            .date()
        )
        old = await prepare_universe(settings, effective_session=effective, store=store)
        new = await prepare_universe(
            settings, effective_session=effective, store=store, use_etf_identities=True
        )
        assert len(old["members"]) == len(new["members"]) == 49
        assert (
            next(m for m in old["members"] if m["symbol"] == "IWM")["gap"]
            == "IDENTITY_TYPE_UNSUPPORTED"
        )
        assert next(m for m in new["members"] if m["symbol"] == "IWM")["gap"] is None
        assert new["candidate"] is None and new["execution_locked"]
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_existing_master_never_overwritten(migrated_database_url, tmp_path):
    settings = config(migrated_database_url)
    factory = session_factory(settings)
    fm, sec, mapping = archives(tmp_path)
    store = MemoryStore()
    identifier = uuid4()
    try:
        async with factory() as s, s.begin():
            s.add(
                Instrument(
                    id=identifier,
                    asset_type="equity_etf",
                    exchange="XNYS",
                    currency="USD",
                    risk_bucket="EQUITY",
                    tick_size=None,
                )
            )
            await s.flush()
            s.add(
                InstrumentAlias(
                    instrument_id=identifier,
                    symbol="IWM",
                    alias_type="ticker",
                    valid_from=datetime.now(UTC).date() - timedelta(days=1),
                )
            )
        with pytest.raises(ValueError, match="CURRENT_ETF_MASTER_CONFLICT"):
            await import_identity(
                settings,
                symbol="IWM",
                fmp_archive=fm,
                sec_archive=sec,
                mapping_file=mapping,
                store=store,
            )
        async with factory() as s:
            assert (await s.get(Instrument, identifier)).exchange == "XNYS"
            assert await s.scalar(select(func.count()).select_from(DataSnapshot)) == 0
        assert store.objects == {}
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_raw_tamper_rejected_on_read(migrated_database_url, tmp_path):
    settings = config(migrated_database_url)
    factory = session_factory(settings)
    fm, sec, mapping = archives(tmp_path)
    store = MemoryStore()
    try:
        result = await import_identity(
            settings,
            symbol="IWM",
            fmp_archive=fm,
            sec_archive=sec,
            mapping_file=mapping,
            store=store,
        )
        key = next(iter(store.objects))
        store.objects[key] += b" altered"
        async with factory() as s:
            with pytest.raises(ValueError):
                await read_current_etf(s, store, snapshot_id=result.snapshot_id)
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_share_class_proof_preserves_iwm_serialization_and_name_difference(
    migrated_database_url, tmp_path
):
    from helpers.current_etf_identity import share_class_document
    from nexus.market.current_etf_identity_storage import EtfProof

    settings = config(migrated_database_url)
    store = MemoryStore()
    fm, sec, mapping = archives(tmp_path / "old")
    first = await import_identity(
        settings, symbol="IWM", fmp_archive=fm, sec_archive=sec, mapping_file=mapping, store=store
    )
    factory = session_factory(settings)
    try:
        async with factory() as session:
            original = (
                await read_current_etf(session, store, snapshot_id=first.snapshot_id)
            ).model_dump(mode="json")
        fm, sec, mapping = archives(
            tmp_path / "new",
            symbol="VTI",
            company_name="Synthetic Wrong Provider Label",
            index_name="Synthetic Total Stock Index Fund",
            document=share_class_document(),
        )
        new = await import_identity(
            settings,
            symbol="VTI",
            fmp_archive=fm,
            sec_archive=sec,
            mapping_file=mapping,
            store=store,
        )
        async with factory() as session:
            proof = await read_current_etf(session, store, snapshot_id=new.snapshot_id)
            assert proof.identity.fmp_name_matches_sec is False
            assert proof.identity.fmp_name == "Synthetic Wrong Provider Label"
            assert (
                await read_current_etf(session, store, snapshot_id=first.snapshot_id)
            ).model_dump(mode="json") == original
            assert "fmp_name" not in original["identity"]
            payload = proof.model_dump(mode="json")
            assert EtfProof.model_validate(payload).model_dump(mode="json") == payload
            payload["identity"]["unexpected"] = "ignored?"
            with pytest.raises(ValueError):
                EtfProof.model_validate(payload)
        effective = (
            exchange_calendars.get_calendar("XNYS")
            .date_to_session(datetime.now(UTC).date() + timedelta(days=7), direction="next")
            .date()
        )
        report = await prepare_universe(
            settings,
            effective_session=effective,
            store=store,
            use_listing_revisions=True,
            use_etf_identities=True,
        )
        assert len(report["members"]) == 49
        bound = next(m for m in report["members"] if m["symbol"] == "VTI")
        assert bound["gap"] is None
        assert bound["fmp_name_matches_sec"] is False
        assert bound["legal_name"] != bound["fmp_name"]
        assert report["candidate"] is None and report["execution_locked"]
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_nasdaq_normal_import_read_and_future_keep_old_identity_json(
    migrated_database_url, tmp_path
):
    from helpers.current_etf_identity import nasdaq_originals
    from nexus.market.current_etf_identity_storage import EtfProof

    settings = config(migrated_database_url)
    store = MemoryStore()
    fm, sec, mapping = archives(tmp_path / "old")
    old = await import_identity(
        settings, symbol="IWM", fmp_archive=fm, sec_archive=sec, mapping_file=mapping, store=store
    )
    factory = session_factory(settings)
    try:
        async with factory() as s:
            prior = (await read_current_etf(s, store, snapshot_id=old.snapshot_id)).model_dump(
                mode="json"
            )
        fm, sec, mapping = archives(tmp_path / "new", original_values=nasdaq_originals())
        new = await import_identity(
            settings,
            symbol="QQQ",
            fmp_archive=fm,
            sec_archive=sec,
            mapping_file=mapping,
            store=store,
        )
        async with factory() as s:
            proof = await read_current_etf(s, store, snapshot_id=new.snapshot_id)
            assert proof.identity.exchange == "XNAS"
            assert (await s.get(Instrument, proof.instrument_id)).exchange == "XNAS"
            assert proof.identity.filing_index_ticker is None
            assert proof.alias_valid_from == proof.checked_at.date()
            assert not proof.historical_backtest_eligible and proof.execution_locked
            assert (await read_current_etf(s, store, snapshot_id=old.snapshot_id)).model_dump(
                mode="json"
            ) == prior
            payload = proof.model_dump(mode="json")
            assert EtfProof.model_validate(payload).model_dump(mode="json") == payload
            payload["identity"]["invented"] = "ignored?"
            with pytest.raises(ValueError):
                EtfProof.model_validate(payload)
        effective = (
            exchange_calendars.get_calendar("XNYS")
            .date_to_session(datetime.now(UTC).date() + timedelta(days=7), direction="next")
            .date()
        )
        report = await prepare_universe(
            settings,
            effective_session=effective,
            store=store,
            use_listing_revisions=True,
            use_etf_identities=True,
        )
        bound = next(m for m in report["members"] if m["symbol"] == "QQQ")
        assert bound["gap"] is None and bound["listing_exchange"] == "XNAS"
        assert bound["filing_index_ticker"] is None
        assert bound["ticker_binding_basis"] == "current_directory_and_bound_497k_cover"
        assert report["candidate"] is None and len(report["members"]) == 49
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_uit_normal_import_read_future_preserves_existing_etf_json(
    migrated_database_url, tmp_path
):
    import json
    from datetime import UTC, datetime, timedelta

    import exchange_calendars
    from helpers.current_etf_identity import archives
    from helpers.current_identity import MemoryStore
    from helpers.current_uit_identity import originals
    from nexus.backtest.future_identity import prepare_future_identity_bindings
    from nexus.market.current_etf_identity_storage import read_current_etf
    from nexus.ops.etf_identity_local import import_identity
    from nexus.persistence.models.data import DataSnapshot
    from nexus.persistence.session import session_factory
    from nexus.settings import EngineSettings

    settings = EngineSettings(database_url=migrated_database_url).model_copy(
        update={"object_store_endpoint": "http://127.0.0.1:9000"}
    )
    factory = session_factory(settings)
    store = MemoryStore()
    fm, sc, mapping = archives(tmp_path / "old")
    try:
        old = await import_identity(
            settings,
            symbol="IWM",
            fmp_archive=fm,
            sec_archive=sc,
            mapping_file=mapping,
            store=store,
        )
        async with factory() as session:
            old_json = json.dumps(
                (await session.get(DataSnapshot, old.snapshot_id)).payload, sort_keys=True
            )
        folder = tmp_path / "uit"
        folder.mkdir(mode=0o700)
        names = (
            "profile.json",
            "receipt.json",
            "submissions.raw",
            "submissions-receipt.json",
            "filing-index.raw",
            "index-receipt.json",
            "disclosure.raw",
            "disclosure-receipt.json",
        )
        for name, raw in zip(names, originals(), strict=True):
            path = folder / name
            path.write_bytes(raw)
            path.chmod(0o600)
        new = await import_identity(
            settings,
            symbol="SPY",
            fmp_archive=folder,
            sec_archive=folder,
            mapping_file=None,
            store=store,
        )
        async with factory() as session:
            proof = await read_current_etf(session, store, snapshot_id=new.snapshot_id)
            assert proof.identity.legal_structure == "unit_investment_trust"
            assert proof.alias_valid_from == proof.checked_at.date()
            assert "series_id" not in proof.identity.model_dump()
            legacy = await read_current_etf(session, store, snapshot_id=old.snapshot_id)
            assert json.dumps(legacy.model_dump(mode="json"), sort_keys=True) == old_json
        effective = (
            exchange_calendars.get_calendar("XNYS")
            .date_to_session(datetime.now(UTC).date() + timedelta(days=7), direction="next")
            .date()
        )
        async with factory() as session:
            result = await prepare_future_identity_bindings(
                session, store, effective_session=effective, use_etf_identities=True
            )
        bound = next(m for m in result["members"] if m["symbol"] == "SPY")
        assert bound["gap"] is None and bound["legal_structure"] == "unit_investment_trust"
        assert bound["listing_exchange"] == "ARCX"
        assert len(result["members"]) == 49 and result["candidate"] is None
        assert result["execution_locked"] and not result["historical_backtest_eligible"]
    finally:
        await factory.kw["bind"].dispose()
