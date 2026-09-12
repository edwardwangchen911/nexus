"""PostgreSQL-only market loaders preserve frozen authority and decision time."""

from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import uuid4

import pytest
from helpers.price_bar_authority import (
    INSTRUMENT_ID,
    MANIFEST_ID,
    insert_valid_authority_from_prepare,
    seed_price_candidate,
)
from nexus.jobs.market_authority import ProviderMarketBinding
from nexus.market.database_series import load_daily_series, load_intraday_series
from nexus.market.ingestion import archive_fosun_price_page_raw, ingest_price_page
from nexus.market.intraday_ingestion import ingest_intraday_snapshot
from nexus.persistence.models.backtest import (
    ExchangeCalendarAuthorityRecord,
    ExchangeCalendarSessionRecord,
)
from nexus.persistence.models.data import ProviderCapability
from nexus.persistence.models.runtime import IntegrationStatus
from nexus.persistence.session import session_factory
from nexus.providers.base import (
    InstrumentId,
    MarketDataProvenance,
    PriceBar,
    ProviderPage,
)
from nexus.providers.market.fosun_gateway import RawGatewayResponse
from nexus.providers.market.snapshot_contracts import (
    MarketSnapshot,
    SnapshotBatch,
    SnapshotMinuteBar,
    SnapshotQuote,
    SnapshotTrade,
)
from nexus.settings import EngineSettings
from sqlalchemy import text

DECISION_TIME = datetime(2026, 8, 28, 15, 1, 30, tzinfo=UTC)


@dataclass
class _ObjectStore:
    objects: dict[str, bytes] = field(default_factory=dict)
    temporary: dict[str, bytes] = field(default_factory=dict)

    async def archive_raw(self, key: str, payload: bytes) -> str:
        self.objects.setdefault(key, payload)
        return f"memory://{key}"

    async def put_normalized(self, key: str, payload: bytes) -> str:
        self.objects.setdefault(key, payload)
        return f"memory://{key}"

    async def put_temporary(self, key: str, payload: bytes) -> None:
        self.temporary[key] = payload

    async def promote(self, temporary_key: str, final_key: str) -> str:
        self.objects.setdefault(final_key, self.temporary[temporary_key])
        return f"memory://{final_key}"

    async def delete_temporary(self, key: str) -> None:
        self.temporary.pop(key, None)


def _batch(*, received_at: datetime, close: str) -> SnapshotBatch:
    observed = received_at.replace(second=0, microsecond=0) - timedelta(minutes=1)
    price = Decimal(close)
    return SnapshotBatch(
        snapshots=(
            MarketSnapshot(
                instrument_id=InstrumentId(INSTRUMENT_ID),
                symbol="AAPL",
                trade=SnapshotTrade(
                    price=price,
                    observed_at=received_at - timedelta(seconds=2),
                    provider_trade_id=f"trade-{close}",
                ),
                quote=SnapshotQuote(
                    bid=price - Decimal("0.02"),
                    ask=price + Decimal("0.02"),
                    observed_at=received_at - timedelta(seconds=1),
                ),
                minute_bar=SnapshotMinuteBar(
                    bar=PriceBar(
                        instrument_id=InstrumentId(INSTRUMENT_ID),
                        session=date(2026, 8, 28),
                        open=price - Decimal("0.10"),
                        high=price + Decimal("0.10"),
                        low=price - Decimal("0.20"),
                        close=price,
                        volume=Decimal("1200"),
                        observed_at=observed,
                        available_at=received_at,
                        received_at=received_at,
                        revision="snapshot-v1",
                    )
                ),
            ),
        ),
        raw_payload=f'{{"close":"{close}"}}'.encode(),
        received_at=received_at,
        provenance=MarketDataProvenance(
            source="alpaca",
            feed="iex",
            adjustment="raw",
            interval="1Min",
            mode="read_only",
            delivery_mode="real_time",
        ),
    )


@pytest.mark.asyncio
async def test_current_database_loaders_admit_current_alpaca_before_decision_only(
    migrated_database_url: str,
) -> None:
    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    await seed_price_candidate(factory)
    await insert_valid_authority_from_prepare(factory, MANIFEST_ID)
    store = _ObjectStore()
    async with factory() as session, session.begin():
        await ingest_intraday_snapshot(
            session,
            store,
            _batch(received_at=DECISION_TIME - timedelta(seconds=10), close="103"),
            commit=False,
        )
    async with factory() as session, session.begin():
        await ingest_intraday_snapshot(
            session,
            store,
            _batch(received_at=DECISION_TIME + timedelta(seconds=10), close="999"),
            commit=False,
        )

    async with factory() as session:
        daily = await load_daily_series(session, INSTRUMENT_ID, DECISION_TIME)
        intraday = await load_intraday_series(session, INSTRUMENT_ID, DECISION_TIME)

    assert daily == ()
    assert intraday.source == "alpaca"
    assert intraday.feed == "iex"
    assert intraday.delivery_mode == "real_time"
    assert intraday.bars == ()
    assert intraday.trade is not None
    assert intraday.trade.last == Decimal("103")
    assert intraday.quote is not None
    assert intraday.quote.bid == Decimal("102.98")
    assert intraday.quote.ask == Decimal("103.02")
    assert intraday.quality_reasons == ()


@pytest.mark.asyncio
async def test_current_database_loaders_preserve_frozen_fosun_v2_bindings_after_expiry(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    await seed_price_candidate(factory)
    checked_at = datetime.now(UTC)
    received_at = checked_at - timedelta(seconds=1)
    capability_expires_at = checked_at + timedelta(seconds=30)
    decision_time = capability_expires_at + timedelta(seconds=1)
    status_id = uuid4()
    quote_capability_id = uuid4()
    bars_capability_id = uuid4()
    contract_hash = "a" * 64
    async with factory() as session, session.begin():
        session.add_all(
            (
                ProviderCapability(
                    id=quote_capability_id,
                    provider="fosun",
                    capability="READ_ONLY",
                    contract_hash=contract_hash,
                    expires_at=capability_expires_at,
                    schema_version="1",
                    payload={
                        "operation": "fosun_get_quotes",
                        "market_scope": "US",
                        "delivery_mode": "real_time",
                    },
                    created_at=checked_at - timedelta(hours=1),
                ),
                ProviderCapability(
                    id=bars_capability_id,
                    provider="fosun",
                    capability="READ_ONLY",
                    contract_hash=contract_hash,
                    expires_at=capability_expires_at,
                    schema_version="1",
                    payload={
                        "operation": "fosun_get_bars",
                        "market_scope": "US",
                        "delivery_mode": "real_time",
                    },
                    created_at=checked_at - timedelta(hours=1),
                ),
                IntegrationStatus(
                    id=status_id,
                    provider="fosun",
                    status="read_only",
                    last_success_at=received_at,
                    version=1,
                    mode_changed_at=checked_at - timedelta(hours=2),
                    capability_invalidated_at=None,
                    admitted_contract_hash=contract_hash,
                    created_at=checked_at - timedelta(hours=2),
                ),
            )
        )
    quote_binding = ProviderMarketBinding(
        provider_capability_id=quote_capability_id,
        provider_status_id=status_id,
        provider_status_version=1,
        provider_contract_hash=contract_hash,
        provider_authorized_at=checked_at,
    )
    bars_binding = quote_binding.model_copy(update={"provider_capability_id": bars_capability_id})
    daily_bar = PriceBar(
        instrument_id=InstrumentId(INSTRUMENT_ID),
        session=date(2026, 8, 28),
        open=Decimal("100"),
        high=Decimal("104"),
        low=Decimal("99"),
        close=Decimal("103"),
        volume=Decimal("1200"),
        observed_at=received_at - timedelta(days=1),
        available_at=received_at,
        received_at=received_at,
        revision="fosun-daily-v2",
    )
    store = _ObjectStore()
    raw_payload = b'{"bars":"fosun"}'
    calendar_authority_id = uuid4()
    request_end_at = datetime(2026, 8, 28, tzinfo=UTC)
    request_start_at = request_end_at - timedelta(days=400)
    async with factory() as session, session.begin():
        session.add(
            ExchangeCalendarAuthorityRecord(
                id=calendar_authority_id,
                exchange="XNYS",
                version=f"market-database-series-{calendar_authority_id}",
                source="fixture",
                range_start=request_start_at.date(),
                range_end=decision_time.date(),
                query_parameters={},
                completeness_status="complete",
                content_hash="c" * 64,
                published_at=received_at - timedelta(days=2),
            )
        )
        session.add(
            ExchangeCalendarSessionRecord(
                calendar_authority_id=calendar_authority_id,
                session=daily_bar.session,
                decision_at=daily_bar.observed_at,
                market_open_at=daily_bar.observed_at - timedelta(hours=6, minutes=30),
                market_close_at=daily_bar.observed_at,
            )
        )
        archived = await archive_fosun_price_page_raw(
            session,
            store,
            RawGatewayResponse(
                operation="fosun_get_bars",
                gateway_request_id=uuid4(),
                request_body_sha256="b" * 64,
                raw_payload=raw_payload,
                sent_at=received_at - timedelta(milliseconds=250),
                received_at=received_at,
            ),
            commit=False,
        )
        await ingest_price_page(
            session,
            store,
            ProviderPage(
                items=(daily_bar,),
                next_cursor=None,
                raw_payload=raw_payload,
                source_request_id="fosun-daily-v2",
                as_of=received_at,
                provenance=MarketDataProvenance(
                    source="fosun",
                    feed="l1",
                    adjustment="raw",
                    interval="1Day",
                    mode="read_only",
                    delivery_mode="real_time",
                ),
            ),
            provider_binding=bars_binding,
            raw_artifact=archived.raw_artifact,
            calendar_authority_id=calendar_authority_id,
            gateway_request_start_at=request_start_at,
            gateway_request_end_at=request_end_at,
            commit=False,
        )
    minute_observed = received_at.replace(second=0, microsecond=0) - timedelta(minutes=1)
    intraday_batch = SnapshotBatch(
        snapshots=(
            MarketSnapshot(
                instrument_id=InstrumentId(INSTRUMENT_ID),
                symbol="AAPL",
                trade=SnapshotTrade(
                    price=Decimal("103.50"),
                    observed_at=received_at,
                    event_kind="snapshot",
                    quality_flags=(
                        "FOSUN_PROVIDER_TIMESTAMP_UNAVAILABLE",
                        "INTRADAY_BID_ASK_UNAVAILABLE",
                    ),
                ),
                quote=None,
                minute_bar=SnapshotMinuteBar(
                    bar=PriceBar(
                        instrument_id=InstrumentId(INSTRUMENT_ID),
                        session=date(2026, 8, 28),
                        open=Decimal("103"),
                        high=Decimal("104"),
                        low=Decimal("102.5"),
                        close=Decimal("103.5"),
                        volume=Decimal("1000"),
                        observed_at=minute_observed,
                        available_at=received_at,
                        received_at=received_at,
                        revision="fosun-minute-v2",
                    )
                ),
            ),
        ),
        raw_payload=b'{"schema":"fosun_gateway_market_snapshot_v1"}',
        received_at=received_at,
        provenance=MarketDataProvenance(
            source="fosun",
            feed="l1",
            adjustment="raw",
            interval="1Min",
            mode="read_only",
            delivery_mode="real_time",
        ),
    )
    async with factory() as session, session.begin():
        await ingest_intraday_snapshot(
            session,
            store,
            intraday_batch,
            quote_binding=quote_binding,
            bar_bindings={INSTRUMENT_ID: bars_binding},
            commit=False,
        )

    async with factory() as session:
        daily = await load_daily_series(session, INSTRUMENT_ID, decision_time)
        intraday = await load_intraday_series(session, INSTRUMENT_ID, decision_time)

    assert len(daily) == 1
    assert daily[0].source == "fosun"
    assert daily[0].feed == "l1"
    assert daily[0].contract_hash == contract_hash
    assert daily[0].bars[0].close == Decimal("103")
    assert intraday.source == "fosun"
    assert intraday.feed == "l1"
    assert intraday.delivery_mode == "real_time"
    assert [bar.close for bar in intraday.bars] == [Decimal("103.5")]
    assert intraday.trade is not None
    assert intraday.trade.last == Decimal("103.50")
    assert intraday.quote is None

    async with factory() as session, session.begin():
        await session.execute(
            text(
                "UPDATE integration_status "
                "SET status='disabled', version=2, "
                "mode_changed_at=:changed_at, capability_invalidated_at=:changed_at, "
                "admitted_contract_hash=NULL WHERE id=:status_id"
            ),
            {
                "changed_at": decision_time + timedelta(seconds=1),
                "status_id": status_id,
            },
        )
    async with factory() as session:
        replayed_daily = await load_daily_series(session, INSTRUMENT_ID, decision_time)
        replayed_intraday = await load_intraday_series(session, INSTRUMENT_ID, decision_time)

    assert replayed_daily == daily
    assert replayed_intraday == intraday


@pytest.mark.asyncio
async def test_intraday_loader_excludes_quote_received_after_decision_time(
    migrated_database_url: str,
) -> None:
    settings = EngineSettings(database_url=migrated_database_url)
    factory = session_factory(settings)
    await seed_price_candidate(factory)
    digest = sha256(b'{"late":"quote"}').hexdigest()
    observed_at = DECISION_TIME - timedelta(minutes=1)
    available_at = DECISION_TIME - timedelta(seconds=1)
    received_at = DECISION_TIME + timedelta(seconds=10)

    async with factory() as session, session.begin():
        artifact_id = uuid4()
        await session.execute(
            text(
                "INSERT INTO data_artifacts "
                "(id,uri,sha256,byte_size,mime_type,schema_version,source,observed_at,"
                "available_at,received_at) VALUES "
                "(:id,:uri,:digest,16,'application/json','test_v1','alpaca',"
                ":observed_at,:available_at,:received_at)"
            ),
            {
                "id": artifact_id,
                "uri": f"memory://sha256/{digest}",
                "digest": digest,
                "observed_at": observed_at,
                "available_at": available_at,
                "received_at": received_at,
            },
        )
        await session.execute(
            text(
                "INSERT INTO latest_quotes "
                "(id,instrument_id,source,feed,delivery_mode,bid,ask,last,observed_at,"
                "available_at,received_at,revision,quality_flags,event_kind,content_hash,"
                "evidence_status,data_artifact_id) VALUES "
                "(:id,:instrument_id,'alpaca','iex','real_time',NULL,NULL,999,"
                ":observed_at,:available_at,:received_at,:digest,'[]'::jsonb,'trade',"
                "repeat('f',64),'verified',:artifact_id)"
            ),
            {
                "id": uuid4(),
                "instrument_id": INSTRUMENT_ID,
                "observed_at": observed_at,
                "available_at": available_at,
                "received_at": received_at,
                "digest": digest,
                "artifact_id": artifact_id,
            },
        )

    async with factory() as session:
        intraday = await load_intraday_series(session, INSTRUMENT_ID, DECISION_TIME)

    assert intraday.trade is None
