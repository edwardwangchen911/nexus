"""Synthetic identity selection through the normal broker freeze/persist path."""

from datetime import datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace
from uuid import uuid4, uuid5

import pytest
from helpers.observed_gateway import NOW, ObservedGateway
from nexus.ops import fosun_local_refresh as refresh
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.session import session_factory
from nexus.providers.broker.fosun_contracts import PositionItem
from nexus.settings import EngineSettings
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError


@pytest.fixture(autouse=True)
def fixed_refresh_clock(monkeypatch):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW if tz is not None else NOW.replace(tzinfo=None)

    monkeypatch.setattr(refresh, "datetime", FixedDatetime)


def settings(url):
    return EngineSettings(
        NEXUS_ENV="test",
        DATABASE_URL=url,
        NEXUS_ENGINE_HMAC_CURRENT_KEY_ID="test",
        NEXUS_ENGINE_HMAC_CURRENT_SECRET="test",
    )


def holding():
    return PositionItem(
        kind="position",
        external_account_id="synthetic-account",
        instrument_code="usAAPL",
        currency="USD",
        quantity="1",
        available_quantity="1",
        frozen_quantity="0",
        average_cost="90",
        diluted_cost=None,
        current_price="100",
        provider_as_of=None,
    )


async def seed(
    session, *, start, end=None, exchange="XNAS", currency="USD", symbol="AAPL", identifier=None
):
    identifier = identifier or uuid4()
    session.add(
        Instrument(
            id=identifier,
            asset_type="common_stock",
            exchange=exchange,
            currency=currency,
            tick_size=Decimal("0.01"),
            risk_bucket="EQUITY",
        )
    )
    await session.flush()
    session.add(
        InstrumentAlias(
            instrument_id=identifier,
            symbol=symbol,
            alias_type="ticker",
            valid_from=start,
            valid_to=end,
        )
    )
    await session.flush()
    return identifier


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ["expired", "future", "currency", "market"])
async def test_invalid_holding_identity_rejected_before_freeze(migrated_database_url, case):
    today = NOW.date()
    factory = session_factory(settings(migrated_database_url))
    async with factory() as s, s.begin():
        await seed(
            s,
            start=today + timedelta(days=1) if case == "future" else today - timedelta(days=500),
            end=today - timedelta(days=1) if case == "expired" else None,
            currency="HKD" if case == "currency" else "USD",
            exchange="XHKG" if case == "market" else "XNAS",
        )
    async with factory() as s, s.begin():
        with pytest.raises(ValueError, match="LOCAL_BROKER_INSTRUMENT_"):
            await refresh._load_local_broker_identity(s, holdings=(holding(),))


@pytest.mark.asyncio
async def test_current_alias_survives_history_through_broker_persistence(migrated_database_url):
    today = NOW.date()
    factory = session_factory(settings(migrated_database_url))
    async with factory() as s, s.begin():
        await seed(s, start=today - timedelta(days=700), end=today - timedelta(days=501))
        current = await seed(s, start=today - timedelta(days=500), end=today + timedelta(days=1))
    async with factory() as s:
        identity = await refresh._load_local_broker_identity(s, holdings=(holding(),))
        assert identity.instrument_ids["usAAPL"] == current
    frozen, flows = await refresh._read_broker_facts(
        ObservedGateway(),
        account_id=identity.account_id,
        portfolio_id=identity.portfolio_id,
        sync_run_id=identity.sync_run_id,
        external_account_id="synthetic-account",
        instrument_ids=identity.instrument_ids,
        cash_flow_end=NOW.date(),
        previous_cash_flow_watermark=None,
    )
    async with factory() as s, s.begin():
        stored = await refresh._persist_ready_broker_snapshot(
            s,
            account_id=identity.account_id,
            portfolio_id=identity.portfolio_id,
            frozen=frozen,
            holdings=(holding(),),
            instrument_ids=identity.instrument_ids,
            valid_from=today,
            completed_at=NOW + timedelta(seconds=7),
            cash_flow_result=flows,
        )
        assert stored.id == frozen.portfolio_snapshot.id


@pytest.mark.asyncio
async def test_ensure_rejects_expected_identity_drift(migrated_database_url):
    today = NOW.date()
    factory = session_factory(settings(migrated_database_url))
    async with factory() as s, s.begin():
        await seed(s, start=today - timedelta(days=1))
    async with factory() as s, s.begin():
        with pytest.raises(ValueError, match="LOCAL_BROKER_INSTRUMENT_"):
            await refresh._ensure_instrument(
                s, "usAAPL", currency="USD", valid_from=today, expected_id=uuid4()
            )


@pytest.mark.asyncio
async def test_database_rejects_overlapping_ticker_identity(migrated_database_url):
    factory = session_factory(settings(migrated_database_url))
    async with factory() as s, s.begin():
        await seed(s, start=NOW.date())
    with pytest.raises(IntegrityError):
        async with factory() as s, s.begin():
            await seed(s, start=NOW.date())


@pytest.mark.asyncio
async def test_order_only_expired_broker_identity_rejected(migrated_database_url):
    factory = session_factory(settings(migrated_database_url))
    expired_id = uuid5(refresh._INSTRUMENT_NAMESPACE, "usMSFT")
    async with factory() as s, s.begin():
        await seed(
            s,
            start=NOW.date() - timedelta(days=500),
            end=NOW.date() - timedelta(days=1),
            symbol="MSFT",
            identifier=expired_id,
        )
    async with factory() as s:
        identity = await refresh._load_local_broker_identity(s, holdings=(holding(),))
    frozen, flows = await read_frozen(identity, order_code="usMSFT")
    assert frozen.portfolio_snapshot.open_orders[0].instrument_id == expired_id
    with pytest.raises(ValueError, match="LOCAL_BROKER_INSTRUMENT_INVALID"):
        async with factory() as s, s.begin():
            await persist_frozen(s, identity, frozen, flows)


async def read_frozen(identity, order_code="usAAPL"):
    return await refresh._read_broker_facts(
        ObservedGateway(order_code=order_code),
        account_id=identity.account_id,
        portfolio_id=identity.portfolio_id,
        sync_run_id=identity.sync_run_id,
        external_account_id="synthetic-account",
        instrument_ids=identity.instrument_ids,
        cash_flow_end=NOW.date(),
        previous_cash_flow_watermark=None,
    )


async def persist_frozen(s, identity, frozen, flows):
    return await refresh._persist_ready_broker_snapshot(
        s,
        account_id=identity.account_id,
        portfolio_id=identity.portfolio_id,
        frozen=frozen,
        holdings=(holding(),),
        instrument_ids=identity.instrument_ids,
        valid_from=identity.identity_date,
        completed_at=NOW + timedelta(seconds=7),
        cash_flow_result=flows,
    )


@pytest.mark.asyncio
async def test_fresh_market_identity_is_current_and_reusable_by_broker(
    migrated_database_url, monkeypatch
):
    factory = session_factory(settings(migrated_database_url))
    async with factory() as s:
        identity = await refresh._load_local_broker_identity(s, holdings=(holding(),))

    class IdentityStageFinished(Exception):
        pass

    async def stop_after_identity(*args, **kwargs):
        raise IdentityStageFinished

    # Exercise the normal market identity stage; external evidence persistence is unrelated.
    monkeypatch.setattr(refresh, "_ensure_market_universe", stop_after_identity)
    async with factory() as s, s.begin():
        with pytest.raises(IdentityStageFinished):
            await refresh._persist_market_snapshot(
                s,
                None,
                settings=None,
                accounts_page=SimpleNamespace(as_of=NOW),
                holdings=(holding(),),
                identity=identity,
                market_reads=SimpleNamespace(
                    quote_raw=SimpleNamespace(received_at=NOW), bars_raw=(), codes=("usAAPL",)
                ),
                request_start_at=NOW - timedelta(days=400),
                request_end_at=NOW,
                calendar_rows=(),
                authorized_at=NOW,
            )
        alias = await s.scalar(select(InstrumentAlias))
        assert alias.valid_from == NOW.date()
        assert alias.instrument_id == identity.instrument_ids["usAAPL"]
    frozen, flows = await read_frozen(identity)
    async with factory() as s, s.begin():
        stored = await persist_frozen(s, identity, frozen, flows)
        assert stored.id == frozen.portfolio_snapshot.id


@pytest.mark.asyncio
async def test_known_hk_order_identity_is_validated_without_losing_read_support(
    migrated_database_url,
):
    factory = session_factory(settings(migrated_database_url))
    hk_holding = holding().model_copy(update={"instrument_code": "hk00656", "currency": "HKD"})
    async with factory() as s, s.begin():
        await seed(
            s,
            start=NOW.date() - timedelta(days=1),
            symbol="00656.HK",
            exchange="XHKG",
            currency="HKD",
        )
    async with factory() as s:
        identity = await refresh._load_local_broker_identity(s, holdings=(hk_holding,))

    class KnownHkGateway(ObservedGateway):
        async def read_cash(self, *args, **kwargs):
            page = await super().read_cash(*args, **kwargs)
            return page.model_copy(update={"items": ({**page.items[0], "currency": "HKD"},)})

        async def read_holdings(self, *args, **kwargs):
            page = await super().read_holdings(*args, **kwargs)
            return page.model_copy(update={"items": (hk_holding.model_dump(mode="json"),)})

    frozen, flows = await refresh._read_broker_facts(
        KnownHkGateway(order_code="hk00656"),
        account_id=identity.account_id,
        portfolio_id=identity.portfolio_id,
        sync_run_id=identity.sync_run_id,
        external_account_id="synthetic-account",
        instrument_ids=identity.instrument_ids,
        cash_flow_end=NOW.date(),
        previous_cash_flow_watermark=None,
    )

    async def persist():
        async with factory() as s, s.begin():
            return await refresh._persist_ready_broker_snapshot(
                s,
                account_id=identity.account_id,
                portfolio_id=identity.portfolio_id,
                frozen=frozen,
                holdings=(),
                instrument_ids=identity.instrument_ids,
                valid_from=identity.identity_date,
                completed_at=NOW + timedelta(seconds=7),
                cash_flow_result=flows,
            )

    assert (await persist()).id == frozen.portfolio_snapshot.id
