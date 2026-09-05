import importlib
from datetime import UTC, datetime

import pytest
from helpers.current_identity import MemoryStore, archives, multiple_registrant_filing
from nexus.market.universe import stable_instrument_id
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import InstrumentAlias, UniverseSnapshot
from nexus.persistence.models.paper import PaperOrderRecord
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, select


@pytest.mark.asyncio
async def test_first_identity_normal_import_readback_and_replay(migrated_database_url, tmp_path):
    module = importlib.import_module("nexus.market.current_identity_storage")
    fm, sc = archives(tmp_path)
    store = MemoryStore()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        ops = importlib.import_module("nexus.ops.instrument_identity_local")
        config = EngineSettings(database_url=migrated_database_url).model_copy(
            update={"object_store_endpoint": "http://127.0.0.1:9000"}
        )
        first = await ops.import_identity(
            config, store=store, symbol="AAPL", fmp_archive=fm, sec_archive=sc
        )
        assert first.status == "created"
        assert first.instrument_id == stable_instrument_id("AAPL")
        async with factory() as session, session.begin():
            replay = await module.import_current_common_stock(
                session, store, symbol="AAPL", fmp_archive=fm, sec_archive=sc
            )
            assert replay.status == "existing"
            assert replay.snapshot_id == first.snapshot_id
            alias = await session.scalar(select(InstrumentAlias))
            assert alias.valid_from == datetime.now(UTC).date()
            assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 1
            for model in (UniverseSnapshot, AgentResearchRunRecord, PaperOrderRecord):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
            await module.read_current_identity(session, store, snapshot_id=first.snapshot_id)
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_multi_cik_normal_import_stored_read_future_binding_keeps_legacy_json(
    migrated_database_url, tmp_path
):
    import json
    from datetime import timedelta
    from hashlib import sha256

    import exchange_calendars
    from nexus.backtest.future_identity import prepare_future_identity_bindings

    module = importlib.import_module("nexus.market.current_identity_storage")
    ops = importlib.import_module("nexus.ops.instrument_identity_local")
    config = EngineSettings(database_url=migrated_database_url).model_copy(
        update={"object_store_endpoint": "http://127.0.0.1:9000"}
    )
    factory = session_factory(config)
    store = MemoryStore()
    old_fmp, old_sec = archives(tmp_path / "single")
    new_fmp, new_sec = archives(tmp_path / "multiple", sec=multiple_registrant_filing())
    try:
        old = await ops.import_identity(
            config, store=store, symbol="AAPL", fmp_archive=old_fmp, sec_archive=old_sec
        )
        async with factory() as session:
            old_payload = (await session.get(DataSnapshot, old.snapshot_id)).payload
            old_json = json.dumps(old_payload, sort_keys=True)
            assert len(old_payload["identity"]["facts"]) == 4
        new = await ops.import_identity(
            config, store=store, symbol="AAPL", fmp_archive=new_fmp, sec_archive=new_sec
        )
        assert new.instrument_id == old.instrument_id == stable_instrument_id("AAPL")
        assert new.snapshot_id != old.snapshot_id
        async with factory() as session:
            stored = (await session.get(DataSnapshot, new.snapshot_id)).payload
            assert stored["identity"]["cik"] == "0000123456"
            assert len(stored["identity"]["facts"]) == 5
            assert {
                f["value"]
                for f in stored["identity"]["facts"]
                if f["name"] == "dei:EntityCentralIndexKey"
            } == {"0000123456", "0000654321"}
        # Separate transactions force both the new proof and the old JSON through the reader.
        async with factory() as session:
            proof = await module.read_current_identity(session, store, snapshot_id=new.snapshot_id)
            legacy = await module.read_current_identity(session, store, snapshot_id=old.snapshot_id)
            assert proof.model_dump(mode="json") == stored
            assert json.dumps(legacy.model_dump(mode="json"), sort_keys=True) == old_json
            assert proof.identity.cik == "0000123456"
            assert (
                json.dumps(
                    (await session.get(DataSnapshot, old.snapshot_id)).payload, sort_keys=True
                )
                == old_json
            )
        future = (
            exchange_calendars.get_calendar("XNYS")
            .date_to_session(datetime.now(UTC).date() + timedelta(days=7), direction="next")
            .date()
        )
        async with factory() as session:
            prepared = await prepare_future_identity_bindings(
                session, store, effective_session=future
            )
        bound = next(m for m in prepared["members"] if m["symbol"] == "AAPL")
        assert bound["gap"] is None
        assert bound["instrument_id"] == str(new.instrument_id)
        assert bound["proof_snapshot_id"] == str(new.snapshot_id)
        assert (
            bound["proof_hash"]
            == sha256(
                json.dumps(stored, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
        )
        assert prepared["status"] == "BLOCKED" and prepared["candidate"] is None
        assert prepared["execution_locked"] is True
        assert prepared["historical_backtest_eligible"] is False
        async with factory() as session:
            assert await session.scalar(select(func.count()).select_from(DataSnapshot)) == 2
            for model in (UniverseSnapshot, AgentResearchRunRecord, PaperOrderRecord):
                assert await session.scalar(select(func.count()).select_from(model)) == 0
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    ["legal_existing", "wrong_currency", "wrong_exchange", "expired", "occupied_id", "wrong_cik"],
)
async def test_existing_identity_reuse_or_fixed_conflict(migrated_database_url, tmp_path, case):
    from datetime import timedelta
    from uuid import uuid4

    from nexus.persistence.models.market import Instrument

    module = importlib.import_module("nexus.market.current_identity_storage")
    fm, sc = archives(tmp_path)
    store = MemoryStore()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    identifier = stable_instrument_id("AAPL") if case == "occupied_id" else uuid4()
    try:
        async with factory() as s, s.begin():
            s.add(
                Instrument(
                    id=identifier,
                    asset_type="common_stock",
                    exchange="XNAS" if case == "wrong_exchange" else "NYSE",
                    currency="HKD" if case == "wrong_currency" else "USD",
                    risk_bucket="EQUITY",
                    tick_size=None,
                )
            )
            await s.flush()
            s.add(
                InstrumentAlias(
                    instrument_id=identifier,
                    symbol="OTHER" if case == "occupied_id" else "AAPL",
                    alias_type="ticker",
                    valid_from=datetime.now(UTC).date() - timedelta(days=3),
                    valid_to=datetime.now(UTC).date() - timedelta(days=1)
                    if case == "expired"
                    else None,
                )
            )
            if case == "wrong_cik":
                s.add(
                    InstrumentAlias(
                        instrument_id=identifier,
                        symbol="0000654321",
                        alias_type="cik",
                        valid_from=datetime.now(UTC).date(),
                        valid_to=None,
                    )
                )
        async with factory() as s:
            before = await s.scalar(
                select(
                    func.md5(
                        func.row_to_json(Instrument.__table__.table_valued()).cast(
                            importlib.import_module("sqlalchemy").Text
                        )
                    )
                )
            )
        if case == "legal_existing":
            async with factory() as s, s.begin():
                result = await module.import_current_common_stock(
                    s, store, symbol="AAPL", fmp_archive=fm, sec_archive=sc
                )
                assert result.status == "existing" and result.instrument_id == identifier
        else:
            with pytest.raises(ValueError, match="CURRENT_IDENTITY_(MASTER|SOURCE)_CONFLICT"):
                async with factory() as s, s.begin():
                    await module.import_current_common_stock(
                        s, store, symbol="AAPL", fmp_archive=fm, sec_archive=sc
                    )
        async with factory() as s:
            after = await s.scalar(
                select(
                    func.md5(
                        func.row_to_json(Instrument.__table__.table_valued()).cast(
                            importlib.import_module("sqlalchemy").Text
                        )
                    )
                )
            )
            assert after == before
            assert await s.scalar(select(func.count()).select_from(Instrument)) == 1
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_object_readback_failure_rolls_back_identity(migrated_database_url, tmp_path):
    from nexus.persistence.models.market import Instrument

    module = importlib.import_module("nexus.market.current_identity_storage")
    fm, sc = archives(tmp_path)

    class BadStore(MemoryStore):
        async def read_raw(self, key, *, uri):
            return b"tampered"

    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        with pytest.raises(ValueError, match="CURRENT_IDENTITY_ARTIFACT_INVALID"):
            async with factory() as s, s.begin():
                await module.import_current_common_stock(
                    s, BadStore(), symbol="AAPL", fmp_archive=fm, sec_archive=sc
                )
        async with factory() as s:
            for model in (Instrument, InstrumentAlias, DataSnapshot):
                assert await s.scalar(select(func.count()).select_from(model)) == 0
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_concurrent_import_and_preexisting_fmp_artifact(migrated_database_url, tmp_path):
    import asyncio

    from nexus.persistence.models.data import DataArtifact
    from nexus.research.fmp_profile_storage import save_profile_artifact

    module = importlib.import_module("nexus.market.current_identity_storage")
    fm, sc = archives(tmp_path)
    store = MemoryStore()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory() as s, s.begin():
            original_uri = await save_profile_artifact(
                s,
                store,
                content=(fm / "profile.json").read_bytes(),
                schema="fmp_profile_raw_v1",
                received_at=datetime.now(UTC),
            )

        async def run():
            async with factory() as s, s.begin():
                return await module.import_current_common_stock(
                    s, store, symbol="AAPL", fmp_archive=fm, sec_archive=sc
                )

        first, second = await asyncio.gather(run(), run())
        assert {first.status, second.status} == {"created", "existing"}
        assert first.snapshot_id == second.snapshot_id
        async with factory() as s:
            proof = await module.read_current_identity(s, store, snapshot_id=first.snapshot_id)
            assert proof.artifacts[0].uri == original_uri
            assert await s.scalar(select(func.count()).select_from(DataArtifact)) == 4
            assert await s.scalar(select(func.count()).select_from(DataSnapshot)) == 1
            assert len(store.objects) == 4
        from datetime import timedelta

        import exchange_calendars
        from nexus.backtest.universe import preflight_us_liquid_v1

        future = (
            exchange_calendars.get_calendar("XNYS")
            .date_to_session(datetime.now(UTC).date() + timedelta(days=7), direction="next")
            .date()
        )
        async with factory() as s, s.begin():
            preflight = await preflight_us_liquid_v1(s, future)
            assert preflight.member_count == 49
            assert preflight.status == "BACKTEST_UNIVERSE_PREFLIGHT_BLOCKED"
            assert set(preflight.gap_categories) == {"INSTRUMENT_MISSING", "TICKER_ALIAS_MISSING"}
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize("successor", [False, True])
async def test_reader_rechecks_current_date_and_current_binding(
    migrated_database_url, tmp_path, monkeypatch, successor
):
    from datetime import timedelta
    from types import SimpleNamespace
    from uuid import uuid4

    from nexus.persistence.models.market import Instrument
    from sqlalchemy import literal

    module = importlib.import_module("nexus.market.current_identity_storage")
    old_clock = datetime.now(UTC) - timedelta(days=2)
    fm, sc = archives(tmp_path, received=(old_clock - timedelta(hours=1)).isoformat())
    store = MemoryStore()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    try:
        async with factory() as s, s.begin():
            instrument_id = uuid4()
            s.add(
                Instrument(
                    id=instrument_id,
                    asset_type="common_stock",
                    exchange="XNYS",
                    currency="USD",
                    tick_size=None,
                    risk_bucket="EQUITY",
                )
            )
            await s.flush()
            s.add(
                InstrumentAlias(
                    instrument_id=instrument_id,
                    symbol="AAPL",
                    alias_type="ticker",
                    valid_from=old_clock.date() - timedelta(days=1),
                    valid_to=old_clock.date() + timedelta(days=1),
                )
            )
        with monkeypatch.context() as clock:
            clock.setattr(
                module, "func", SimpleNamespace(clock_timestamp=lambda: literal(old_clock))
            )
            async with factory() as s, s.begin():
                result = await module.import_current_common_stock(
                    s, store, symbol="AAPL", fmp_archive=fm, sec_archive=sc
                )
        if successor:
            async with factory() as s, s.begin():
                replacement = uuid4()
                s.add(
                    Instrument(
                        id=replacement,
                        asset_type="common_stock",
                        exchange="XNYS",
                        currency="USD",
                        tick_size=None,
                        risk_bucket="EQUITY",
                    )
                )
                await s.flush()
                s.add(
                    InstrumentAlias(
                        instrument_id=replacement,
                        symbol="AAPL",
                        alias_type="ticker",
                        valid_from=datetime.now(UTC).date(),
                        valid_to=None,
                    )
                )
        with pytest.raises(ValueError, match="CURRENT_IDENTITY_READBACK_INVALID"):
            async with factory() as s:
                await module.read_current_identity(s, store, snapshot_id=result.snapshot_id)
    finally:
        await factory.kw["bind"].dispose()
