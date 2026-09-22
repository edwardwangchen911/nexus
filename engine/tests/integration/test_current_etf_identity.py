"""Normal ETF entry, real isolated PostgreSQL, synthetic sources and object store."""

import asyncio
import json
from datetime import UTC, datetime, timedelta
from hashlib import sha256
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


def etn_archive(path, *, symbol="SYNZ"):
    path.mkdir(mode=0o700)
    fmp_path, sec_path = path / "fmp", path / "sec"
    fmp_path.mkdir(mode=0o700)
    sec_path.mkdir(mode=0o700)
    fmp = json.dumps(
        [
            {
                "symbol": symbol,
                "companyName": "Synthetic Market Exposure ETN",
                "currency": "USD",
                "cusip": "000000000",
                "isin": "US0000000002",
                "exchange": "NASDAQ",
                "isEtf": True,
                "isFund": False,
            }
        ]
    ).encode()
    fmp_receipt = json.dumps(
        {
            "schema": "fmp_profile_receipt_v1",
            "source": "fmp",
            "origin": "https://financialmodelingprep.com",
            "path": "/stable/profile",
            "symbol": symbol,
            "raw_sha256": sha256(fmp).hexdigest(),
            "raw_byte_length": len(fmp),
            "received_at": "2026-01-02T12:01:00+00:00",
        }
    ).encode()
    for name, raw in (("profile.json", fmp), ("receipt.json", fmp_receipt)):
        target = fmp_path / name
        target.write_bytes(raw)
        target.chmod(0o600)
    submissions = json.dumps(
        {
            "cik": "0000123456",
            "name": "Synthetic Bank",
            "tickers": ["BANK"],
            "exchanges": ["Nasdaq"],
            "filings": {
                "recent": {
                    "accessionNumber": ["0000123456-26-000001"],
                    "acceptanceDateTime": ["2026-01-02T12:00:00.000Z"],
                    "form": ["424B2"],
                    "primaryDocument": ["synthetic.htm"],
                }
            },
        }
    ).encode()
    index = b"""<html><body>SEC Accession No. 0000123456-26-000001
CIK: 0000123456 Form 424B2
<a href="synthetic.htm">synthetic.htm</a></body></html>"""
    document = f"""<html><body>Filed Pursuant to Rule 424(b)(2)
<table><tr><td>Synthetic Bank CIK: 0000123456</td></tr>
<tr><td>Synthetic Market Exposure Exchange Traded Notes due January 2, 2036</td></tr>
<tr><td>CUSIP</td><td>Ticker Symbol</td><td>ISIN</td></tr>
<tr><td>000000000</td><td>{symbol}</td><td>US0000000002</td></tr></table>
Listing Exchange: The Nasdaq Stock Market LLC
The notes are senior unsecured debt securities of Synthetic Bank.
</body></html>""".encode()
    raws = (submissions, index, document)
    urls = (
        "https://data.sec.gov/submissions/CIK0000123456.json",
        "https://www.sec.gov/Archives/edgar/data/123456/000012345626000001/"
        "0000123456-26-000001-index.html",
        "https://www.sec.gov/Archives/edgar/data/123456/000012345626000001/synthetic.htm",
    )
    receipts = json.dumps(
        [
            {
                "source_url": url,
                "received_at": "2026-01-02T12:01:00+00:00",
                "sha256": sha256(raw).hexdigest(),
                "byte_length": len(raw),
                "http_status": 200,
            }
            for url, raw in zip(urls, raws, strict=True)
        ]
    ).encode()
    for name, raw in zip(
        ("submissions.raw", "filing-index.raw", "disclosure.raw", "receipts.json"),
        (*raws, receipts),
        strict=True,
    ):
        target = sec_path / name
        target.write_bytes(raw)
        target.chmod(0o600)
    return fmp_path, sec_path


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
            quality="PARTIAL",
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
async def test_current_held_multi_asset_etf_import_preserves_locked_identity(
    migrated_database_url, tmp_path
):
    from nexus.market.current_etf_identity_storage import (
        import_current_held_etf,
        read_current_held_etf,
    )

    symbol = "SYNX"
    document = b"""<html><body>497K Summary Prospectus May 1, 2026
Ticker: SYNX Synthetic Allocation ETF listed on The Nasdaq Stock Market LLC.
Investment Objective The Fund seeks total return from a diversified allocation.
Fund Fees and Expenses Costs apply.
Principal Investment Strategies The Fund invests substantially in common stocks and equity
securities and also invests in bonds, debt securities, and other fixed income instruments.
Principal Risks Market and credit risks apply.
Ticker: SYNX Unlike most ETFs, shares trade in the secondary market.
</body></html>"""
    settings = config(migrated_database_url)
    fm, sec, mapping = archives(
        tmp_path,
        symbol=symbol,
        exchange="NASDAQ",
        company_name="Synthetic Allocation ETF",
        document=document,
    )
    accounts_page, holdings_page, _ = held_pages(symbol)
    store = MemoryStore()
    factory = session_factory(settings)
    try:
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
            instrument = await session.get(Instrument, proof.instrument_id)
            assert proof.identity.asset_type == "non_equity_etf"
            assert proof.identity.risk_bucket == "MULTI_ASSET"
            assert instrument.asset_type == "non_equity_etf"
            assert instrument.risk_bucket == "MULTI_ASSET"
            assert proof.execution_locked and not proof.new_position_eligible
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_current_open_order_etf_import_is_read_only_and_outside_universe(
    migrated_database_url, tmp_path
):
    from nexus.market.current_etf_identity_storage import (
        import_current_open_order_etf,
        read_current_held_etf,
    )

    symbol = "SYNX"
    document = b"""<html><body>497K Summary Prospectus May 1, 2026
Ticker: SYNX Synthetic Leveraged Equity ETF listed on The Nasdaq Stock Market LLC.
Investment Objective The Fund seeks daily results linked to shares of a United States company.
Fund Fees and Expenses Costs apply.
Principal Investment Strategies The Fund obtains equity exposure through common stocks and swaps.
Principal Risks Market risks apply.
Ticker: SYNX Unlike most ETFs, shares trade in the secondary market.
</body></html>"""
    settings = config(migrated_database_url)
    fm, sec, mapping = archives(
        tmp_path,
        symbol=symbol,
        exchange="NASDAQ",
        company_name="Synthetic Leveraged Equity ETF",
        document=document,
    )
    accounts_page, _, _ = held_pages(symbol)
    orders_page = GatewayResponsePage(
        items=(
            {
                "kind": "order",
                "external_order_id": "private-order-id",
                "instrument_code": "us" + symbol,
                "side": "buy",
                "quantity": "1",
                "filled_quantity": "0",
                "price": "10",
                "status": "40",
                "submitted_at": (accounts_page.as_of - timedelta(hours=1)).isoformat(),
                "updated_at": None,
            },
        ),
        source_request_id="synthetic-orders-request",
        has_more=False,
        next_cursor=None,
        as_of=accounts_page.as_of,
        quality="PARTIAL",
    )
    store = MemoryStore()
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            result = await import_current_open_order_etf(
                session,
                store,
                symbol=symbol,
                fmp_archive=fm,
                sec_archive=sec,
                mapping_file=mapping,
                accounts_page=accounts_page,
                orders_page=orders_page,
            )
        async with factory() as session:
            proof = await read_current_held_etf(session, store, snapshot_id=result.snapshot_id)
            assert proof.scope == "fosun_current_open_order"
            assert proof.holding is None and proof.order is not None
            assert proof.order.order_id_sha256 != "private-order-id"
            assert proof.identity.asset_type == "equity_etf"
            assert proof.execution_locked and not proof.new_position_eligible
            assert await session.scalar(select(func.count()).select_from(UniverseSnapshot)) == 0
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
async def test_current_held_etn_import_preserves_debt_identity_and_refresh_alias(
    migrated_database_url, tmp_path
):
    from nexus.market.current_etn_identity_storage import (
        import_current_held_etn,
        read_current_held_etn,
    )
    from nexus.market.universe import stable_instrument_id
    from nexus.ops.fosun_local_refresh import _load_local_broker_identity

    symbol = "SYNZ"
    settings = config(migrated_database_url)
    fmp, sec = etn_archive(tmp_path / "etn")
    store = MemoryStore()
    factory = session_factory(settings)
    try:
        accounts_page, holdings_page, holding = held_pages(symbol)
        async with factory() as session, session.begin():
            result = await import_current_held_etn(
                session,
                store,
                symbol=symbol,
                fmp_archive=fmp,
                sec_archive=sec,
                accounts_page=accounts_page,
                holdings_page=holdings_page,
            )
        async with factory() as session, session.begin():
            repeated = await import_current_held_etn(
                session,
                store,
                symbol=symbol,
                fmp_archive=fmp,
                sec_archive=sec,
                accounts_page=accounts_page,
                holdings_page=holdings_page,
            )
            assert repeated.status == "existing"
            assert repeated.snapshot_id == result.snapshot_id
        async with factory() as session:
            proof = await read_current_held_etn(session, store, snapshot_id=result.snapshot_id)
            instrument = await session.get(Instrument, proof.instrument_id)
            assert proof.instrument_id == stable_instrument_id(symbol)
            assert proof.execution_locked and not proof.universe_published
            assert instrument is not None
            assert instrument.asset_type == "exchange_traded_note"
            assert instrument.risk_bucket == "EXCHANGE_TRADED_NOTE"
            broker_identity = await _load_local_broker_identity(session, holdings=(holding,))
            assert broker_identity.instrument_ids["us" + symbol] == proof.instrument_id
            assert await session.scalar(select(func.count()).select_from(UniverseSnapshot)) == 0
        async with factory() as session:
            alias = await session.get(InstrumentAlias, proof.alias_id)
            assert alias is not None
            alias.symbol = "OTHER"
            with session.no_autoflush:
                with pytest.raises(ValueError, match="CURRENT_HELD_ETN_READBACK_INVALID"):
                    await read_current_held_etn(session, store, snapshot_id=result.snapshot_id)
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
