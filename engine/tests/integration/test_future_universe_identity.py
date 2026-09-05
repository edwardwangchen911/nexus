"""Future preparation preserves old definitions and requires sourced current identity."""

import importlib
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import exchange_calendars
import pytest
from helpers.current_identity import MemoryStore, archives
from nexus.backtest.universe import load_us_liquid_v1_definition, preflight_us_liquid_v1
from nexus.market.current_identity_storage import import_current_common_stock
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.market import Instrument, InstrumentAlias, UniverseSnapshot
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import func, literal, select, text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case", ["valid", "no_proof", "expired", "future_expiry", "broken_raw", "future_ambiguous"]
)
async def test_future_binding_uses_verified_broker_id_without_relaxing_v1(
    migrated_database_url, tmp_path, monkeypatch, case
):
    ops = importlib.import_module("nexus.ops.instrument_identity_local")
    prepare = getattr(ops, "prepare_universe")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    settings = EngineSettings(database_url=migrated_database_url).model_copy(
        update={"object_store_endpoint": "http://127.0.0.1:9000"}
    )
    store = MemoryStore()
    old_clock = datetime.now(UTC) - timedelta(days=2)
    fm, sc = archives(tmp_path, received=(old_clock - timedelta(hours=1)).isoformat())
    actual_id = uuid4()
    today = datetime.now(UTC).date()
    calendar = exchange_calendars.get_calendar("XNYS")
    effective = calendar.date_to_session(today + timedelta(days=7), direction="next").date()
    try:
        async with factory() as s, s.begin():
            s.add(
                Instrument(
                    id=actual_id,
                    asset_type="common_stock",
                    exchange="NYSE",
                    currency="USD",
                    risk_bucket="EQUITY",
                    tick_size=None,
                )
            )
            await s.flush()
            alias = InstrumentAlias(
                instrument_id=actual_id,
                alias_type="ticker",
                symbol="AAPL",
                valid_from=today - timedelta(days=3),
                valid_to=(
                    today - timedelta(days=1)
                    if case == "expired"
                    else today
                    if case == "future_expiry"
                    else None
                ),
            )
            s.add(alias)
        proof_id = None
        if case != "no_proof":
            with monkeypatch.context() as clock:
                if case == "expired":
                    clock.setattr(
                        importlib.import_module("nexus.market.current_identity_storage"),
                        "func",
                        SimpleNamespace(clock_timestamp=lambda: literal(old_clock)),
                    )
                async with factory() as s, s.begin():
                    imported = await import_current_common_stock(
                        s, store, symbol="AAPL", fmp_archive=fm, sec_archive=sc
                    )
                    proof_id = imported.snapshot_id
        if case == "future_ambiguous":
            async with factory() as s, s.begin():
                await s.execute(
                    text(
                        "ALTER TABLE instrument_aliases DROP CONSTRAINT "
                        "ex_instrument_aliases_ticker_instrument_period"
                    )
                )
                s.add(
                    InstrumentAlias(
                        instrument_id=actual_id,
                        alias_type="ticker",
                        symbol="OTHER",
                        valid_from=effective,
                        valid_to=None,
                    )
                )
        if case == "broken_raw":
            key = next(iter(store.objects))
            store.objects[key] = b"corrupt"
        old_hash = load_us_liquid_v1_definition(effective).content_hash
        async with factory() as s, s.begin():
            legacy = await preflight_us_liquid_v1(s, effective)
        assert "TICKER_ALIAS_MISMATCH" in legacy.gap_categories or case in {
            "expired",
            "future_expiry",
        }
        before_count = 0 if proof_id is None else 1
        result = await prepare(settings, effective_session=effective, store=store)
        assert result["status"] == "BLOCKED" and result["candidate"] is None
        assert result["source_definition_hash"] == old_hash
        assert len(result["members"]) == 49
        member = next(m for m in result["members"] if m["symbol"] == "AAPL")
        if case == "valid":
            assert member["instrument_id"] == str(actual_id)
            assert member["proof_snapshot_id"] == str(proof_id)
            assert member["gap"] is None
            assert len(member["proof_hash"]) == 64
        else:
            assert member["instrument_id"] is None and member["gap"]
        etfs = [m for m in result["members"] if m["asset_type"] != "common_stock"]
        assert etfs and all(m["gap"] == "IDENTITY_TYPE_UNSUPPORTED" for m in etfs)
        assert result["historical_backtest_eligible"] is False
        assert result["execution_locked"] is True
        assert load_us_liquid_v1_definition(effective).content_hash == old_hash
        async with factory() as s:
            assert await s.scalar(select(func.count()).select_from(UniverseSnapshot)) == 0
            assert await s.scalar(select(func.count()).select_from(DataSnapshot)) == before_count
    finally:
        await factory.kw["bind"].dispose()
