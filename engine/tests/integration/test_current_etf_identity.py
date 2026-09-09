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
from nexus.providers.broker.fosun_contracts import PositionItem
from nexus.providers.broker.gateway_contracts import GatewayResponsePage
from nexus.settings import EngineSettings
from sqlalchemy import func, select


def config(url):
    return EngineSettings(database_url=url).model_copy(
        update={"object_store_endpoint": "http://127.0.0.1:9000"}
    )


def held_pages(symbol, *, request_id="synthetic-holdings-request", as_of=None):
    as_of = as_of or datetime.now(UTC)
    account = {
        "kind": "account",
        "external_account_id": "synthetic-account",
        "status": "active",
        "base_currency": "USD",
    }
    holding = PositionItem(
        kind="position",
        external_account_id="synthetic-account",
        instrument_code="us" + symbol,
        currency="USD",
        quantity="1",
        available_quantity="1",
        frozen_quantity="0",
        average_cost="10",
        diluted_cost=None,
        current_price="11",
        provider_as_of=as_of,
    )
    return (
        GatewayResponsePage(
            items=(account,),
            source_request_id="synthetic-accounts-request",
            has_more=False,
            next_cursor=None,
            as_of=as_of,
            quality="PASS",
        ),
        GatewayResponsePage(
            items=(holding.model_dump(mode="json"),),
            source_request_id=request_id,
            has_more=False,
            next_cursor=None,
            as_of=as_of,
            quality="PASS",
        ),
        holding,
    )


@pytest.mark.asyncio
async def test_current_held_etf_import_is_read_only_and_outside_fixed_universe(
    migrated_database_url, tmp_path
):
    from nexus.market.current_etf_identity_storage import (
        import_current_held_etf,
        read_current_held_etf,
    )
    from nexus.market.universe import stable_instrument_id

    symbol = "SYNX"
    document = b"""<html><body>FORM 497K Synthetic Held Equity Fund SYNX | NYSE Arca
Summary Prospectus Investment Objective The Synthetic Held Equity Fund seeks to track
an index composed of broadly diversified U.S. equities. Fees and Expenses
Purchase and Sale of Fund Shares The Fund is an exchange-traded fund.
Tax Information</body></html>"""
    settings = config(migrated_database_url)
    fm, sec, mapping = archives(
        tmp_path,
        symbol=symbol,
        company_name="Synthetic Held Equity Fund",
        document=document,
    )
    store = MemoryStore()
    factory = session_factory(settings)
    try:
        accounts_page, holdings_page, holding = held_pages(symbol)
        async with factory() as session, session.begin():
            result = await import_current_held_etf(
                session,
                store,
                symbol=symbol,
                fmp_archive=fm,
                sec_archive=sec,
                mapping_file=mapping,
                accounts_page=accounts_page,
                holdings_page=holdings_page,
            )
        async with factory() as session:
            proof = await read_current_held_etf(session, store, snapshot_id=result.snapshot_id)
            assert proof.scope == "fosun_current_holding"
            assert proof.holding.holdings_source_request_id == "synthetic-holdings-request"
            assert proof.instrument_id == stable_instrument_id(symbol)
            assert proof.execution_locked
            assert not proof.universe_published
            assert not proof.new_position_eligible
            assert not proof.historical_backtest_eligible
            assert await session.scalar(select(func.count()).select_from(UniverseSnapshot)) == 0
            from nexus.ops.fosun_local_refresh import _load_local_broker_identity

            broker_identity = await _load_local_broker_identity(session, holdings=(holding,))
            assert broker_identity.instrument_ids["us" + symbol] == proof.instrument_id

        second_accounts, second_holdings, _ = held_pages(
            symbol, request_id="synthetic-holdings-request-2"
        )
        async with factory() as session, session.begin():
            second = await import_current_held_etf(
                session,
                store,
                symbol=symbol,
                fmp_archive=fm,
                sec_archive=sec,
                mapping_file=mapping,
                accounts_page=second_accounts,
                holdings_page=second_holdings,
            )
        assert second.instrument_id == result.instrument_id
        assert second.snapshot_id != result.snapshot_id
        async with factory() as session, session.begin():
            repeated = await import_current_held_etf(
                session,
                store,
                symbol=symbol,
                fmp_archive=fm,
                sec_archive=sec,
                mapping_file=mapping,
                accounts_page=second_accounts,
                holdings_page=second_holdings,
            )
        assert repeated.status == "existing"
        assert repeated.snapshot_id == second.snapshot_id
        effective = (
            exchange_calendars.get_calendar("XNYS")
            .date_to_session(datetime.now(UTC).date() + timedelta(days=7), direction="next")
            .date()
        )
        universe = await prepare_universe(
            settings, effective_session=effective, store=store, use_etf_identities=True
        )
        assert len(universe["members"]) == 49
        assert symbol not in {member["symbol"] for member in universe["members"]}
    finally:
        await factory.kw["bind"].dispose()


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
@pytest.mark.parametrize("symbol", ["SPY", "DIA"])
async def test_uit_normal_import_read_future_preserves_existing_etf_json(
    migrated_database_url, tmp_path, symbol
):
    import json
    from datetime import UTC, datetime, timedelta

    import exchange_calendars
    from helpers.current_etf_identity import archives
    from helpers.current_identity import MemoryStore
    from helpers.current_uit_identity import dia_originals, originals
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
        for name, raw in zip(
            names, originals() if symbol == "SPY" else dia_originals(), strict=True
        ):
            path = folder / name
            path.write_bytes(raw)
            path.chmod(0o600)
        new = await import_identity(
            settings,
            symbol=symbol,
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
        bound = next(m for m in result["members"] if m["symbol"] == symbol)
        assert bound["gap"] is None and bound["legal_structure"] == "unit_investment_trust"
        assert bound["listing_exchange"] == "ARCX"
        assert len(result["members"]) == 49 and result["candidate"] is None
        assert result["execution_locked"] and not result["historical_backtest_eligible"]
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_treasury_explicit_import_read_future_preserves_type_and_old_json(
    migrated_database_url, tmp_path
):
    from helpers.current_etf_identity import treasury_originals
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
        fm, sec, mapping = archives(tmp_path / "new", original_values=treasury_originals())
        with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
            await import_identity(
                settings,
                symbol="TLT",
                fmp_archive=fm,
                sec_archive=sec,
                mapping_file=mapping,
                store=store,
            )
        new = await import_identity(
            settings,
            symbol="TLT",
            allow_treasury=True,
            fmp_archive=fm,
            sec_archive=sec,
            mapping_file=mapping,
            store=store,
        )
        async with factory() as s:
            proof = await read_current_etf(s, store, snapshot_id=new.snapshot_id)
            assert proof.identity.exchange == "XNAS"
            assert (await s.get(Instrument, proof.instrument_id)).exchange == "XNAS"
            assert proof.identity.asset_type == "non_equity_etf"
            instrument = await s.get(Instrument, proof.instrument_id)
            assert (
                instrument.asset_type == "non_equity_etf"
                and instrument.risk_bucket == "US_TREASURY"
            )
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
        bound = next(m for m in report["members"] if m["symbol"] == "TLT")
        assert bound["gap"] is None and bound["listing_exchange"] == "XNAS"
        assert bound["asset_type"] == "non_equity_etf"
        assert bound["underlying_asset_class"] == "us_treasury_bonds"
        assert (
            next(m for m in report["members"] if m["symbol"] == "GLD")["gap"]
            == "IDENTITY_PROOF_MISSING"
        )
        default = await prepare_universe(settings, effective_session=effective, store=store)
        assert (
            next(m for m in default["members"] if m["symbol"] == "TLT")["gap"]
            == "IDENTITY_TYPE_UNSUPPORTED"
        )
        assert report["candidate"] is None and len(report["members"]) == 49
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_gold_explicit_import_read_future_preserves_type_and_old_json(
    migrated_database_url, tmp_path
):
    from helpers.current_gold_identity import gold_originals
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
        fm = sec = tmp_path / "gold"
        fm.mkdir(mode=0o700)
        mapping = None
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
        for name, raw in zip(names, gold_originals("viewer"), strict=True):
            path = fm / name
            path.write_bytes(raw)
            path.chmod(0o600)
        with pytest.raises(ValueError, match="CURRENT_ETF_IDENTITY_INVALID"):
            await import_identity(
                settings,
                symbol="GLD",
                fmp_archive=fm,
                sec_archive=sec,
                mapping_file=mapping,
                store=store,
            )
        new = await import_identity(
            settings,
            symbol="GLD",
            allow_gold=True,
            fmp_archive=fm,
            sec_archive=sec,
            mapping_file=mapping,
            store=store,
        )
        async with factory() as s:
            proof = await read_current_etf(s, store, snapshot_id=new.snapshot_id)
            assert proof.identity.exchange == "ARCX"
            assert (await s.get(Instrument, proof.instrument_id)).exchange == "ARCX"
            assert proof.identity.asset_type == "non_equity_etf"
            instrument = await s.get(Instrument, proof.instrument_id)
            assert instrument.asset_type == "non_equity_etf" and instrument.risk_bucket == "GOLD"
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
        bound = next(m for m in report["members"] if m["symbol"] == "GLD")
        assert bound["gap"] is None and bound["listing_exchange"] == "ARCX"
        assert bound["asset_type"] == "non_equity_etf"
        assert bound["underlying_asset_class"] == "gold_bullion"
        assert bound["legal_structure"] == "new_york_investment_trust"
        assert len(proof.identity.registration_facts) == 4
        assert "series_id" not in proof.identity.model_dump()
        default = await prepare_universe(settings, effective_session=effective, store=store)
        assert (
            next(m for m in default["members"] if m["symbol"] == "GLD")["gap"]
            == "IDENTITY_TYPE_UNSUPPORTED"
        )
        assert report["candidate"] is None and len(report["members"]) == 49
    finally:
        await factory.kw["bind"].dispose()
