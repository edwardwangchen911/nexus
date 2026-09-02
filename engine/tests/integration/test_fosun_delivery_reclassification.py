import asyncio
import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import uuid4

import pytest
from alembic import command
from alembic.config import Config
from helpers.fosun_lineage import seed_fosun_daily_lineage
from helpers.portfolio_api import settings
from nexus.market.fosun_delivery import reclassify_daily_delivery
from nexus.market.ingestion import ingest_price_page
from nexus.ops.fosun_local_refresh import _ensure_market_bindings
from nexus.persistence.models.data import PriceBarSetAuthority, PriceSeriesManifest
from nexus.persistence.models.market import Instrument, InstrumentAlias
from nexus.persistence.session import session_factory
from nexus.providers.base import MarketDataProvenance, PriceBar, ProviderPage
from nexus.readiness import probe_migration
from sqlalchemy import select, text
from sqlalchemy.exc import DBAPIError


class Store:
    def __init__(self):
        self.objects = {}

    async def archive_raw(self, key, payload):
        self.objects[f"memory://{key}"] = payload
        return f"memory://{key}"

    put_normalized = archive_raw

    async def read(self, uri):
        return self.objects[uri]


async def seed(session, store, *, delay=True, response_symbol="usALPHA"):
    now = datetime.now(UTC)
    received = now - timedelta(minutes=6)
    observed = (received - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    instrument_id = uuid4()
    session.add(
        Instrument(
            id=instrument_id,
            asset_type="common_stock",
            exchange="XNAS",
            currency="USD",
            tick_size=Decimal("0.01"),
            risk_bucket="technology",
        )
    )
    await session.flush()
    session.add(
        InstrumentAlias(
            instrument_id=instrument_id,
            symbol="ALPHA",
            alias_type="ticker",
            valid_from=observed.date() - timedelta(days=500),
            created_at=received - timedelta(days=1),
        )
    )
    _, binding = await _ensure_market_bindings(
        session, authorized_at=received + timedelta(seconds=1), delivery_mode="unknown"
    )
    raw = json.dumps(
        {
            "items": [
                {
                    "kind": "bar",
                    "instrument_code": response_symbol,
                    "interval": "day",
                    "timestamp": observed.isoformat(),
                    "open": "100",
                    "high": "102",
                    "low": "99",
                    "close": "101",
                    "volume": "1000",
                }
            ],
            "source_request_id": "synthetic-delivery",
            "has_more": False,
            "next_cursor": None,
            "as_of": received.isoformat(),
            "quality": "PARTIAL",
            "sdk_requested_adjustment": "noward",
            "provider_reported_delay": delay,
        }
    ).encode()
    lineage = await seed_fosun_daily_lineage(
        session,
        manifest_id=uuid4(),
        provider_authorized_at=binding.provider_authorized_at,
        sessions=(observed.date(),),
        received_at=received,
        raw_payload=raw,
    )
    store.objects[lineage.raw_artifact.uri] = raw
    page = ProviderPage(
        items=(
            PriceBar(
                instrument_id=instrument_id,
                session=observed.date(),
                open=Decimal(100),
                high=Decimal(102),
                low=Decimal(99),
                close=Decimal(101),
                volume=Decimal(1000),
                observed_at=observed,
                available_at=received,
                received_at=received,
                revision=sha256(raw).hexdigest(),
            ),
        ),
        next_cursor=None,
        raw_payload=raw,
        source_request_id="synthetic-delivery",
        as_of=received,
        provenance=MarketDataProvenance(
            source="fosun",
            feed="l1",
            adjustment="raw",
            interval="1Day",
            mode="read_only",
            delivery_mode="unknown",
        ),
    )
    result = await ingest_price_page(
        session,
        store,
        page,
        provider_binding=binding,
        raw_artifact=lineage.raw_artifact,
        calendar_authority_id=lineage.calendar_authority_id,
        gateway_request_start_at=lineage.request_start_at,
        gateway_request_end_at=lineage.request_end_at,
        commit=False,
    )
    return result.manifest.id


@pytest.mark.asyncio
async def test_cached_response_creates_new_version_and_preserves_original(
    migrated_database_url,
    monkeypatch,
):
    factory = session_factory(settings(migrated_database_url))
    store = Store()
    try:
        async with factory() as session, session.begin():
            parent_id = await seed(session, store)
        class AheadClock(datetime):
            @classmethod
            def now(cls, tz=None):
                return datetime.now(tz) + timedelta(seconds=5)

        monkeypatch.setattr("nexus.market.fosun_delivery.datetime", AheadClock)
        async with factory() as session, session.begin():
            before = await session.scalar(text("select clock_timestamp()"))
            new_id = await reclassify_daily_delivery(
                session, manifest_id=parent_id, raw_reader=store.read, normalized_reader=store.read
            )
        async with factory() as session, session.begin():
            replay = await reclassify_daily_delivery(
                session, manifest_id=parent_id, raw_reader=store.read, normalized_reader=store.read
            )
            old = await session.get(PriceSeriesManifest, parent_id)
            new = await session.get(PriceSeriesManifest, new_id)
            assert replay == new_id != parent_id
            assert old.delivery_mode == "unknown" and new.delivery_mode == "delayed"
            assert old.data_artifact_id == new.data_artifact_id
            assert old.gateway_transport_artifact_id == new.gateway_transport_artifact_id
            assert new.provider_authorized_at >= before
            authority = await session.scalar(
                select(PriceBarSetAuthority).where(
                    PriceBarSetAuthority.price_series_manifest_id == new_id
                )
            )
            assert authority is not None and authority.available_at >= before
        # A downgrade with real version history must refuse, preserving both versions.
        monkeypatch.setenv("DATABASE_URL", migrated_database_url)
        with pytest.raises(DBAPIError):
            await asyncio.to_thread(
                command.downgrade, Config("engine/alembic.ini"), "0033_cash_flow_business_date"
            )
        async with factory() as session:
            assert (
                await session.scalar(text("select version_num from alembic_version"))
                == "0034_capability_versions"
            )
            assert await session.get(PriceSeriesManifest, new_id) is not None
            assert await probe_migration(session) == "ready"
    finally:
        await factory.kw["bind"].dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("delay", "response_symbol", "code"),
    (
        (None, "usALPHA", "FOSUN_RESPONSE_DECLARATION_MISSING"),
        (True, "usOTHER", "FOSUN_NORMALIZED_RESPONSE_MISMATCH"),
        (True, "usALPHA", "FOSUN_RAW_EVIDENCE_MISMATCH"),
    ),
)
async def test_cached_response_cannot_invent_delivery_or_cross_instrument(
    migrated_database_url,
    delay,
    response_symbol,
    code,
):
    factory = session_factory(settings(migrated_database_url))
    store = Store()
    try:
        async with factory() as session, session.begin():
            parent = await seed(session, store, delay=delay, response_symbol=response_symbol)
        async with factory() as session, session.begin():

            async def read_raw(uri):
                payload = await store.read(uri)
                return payload + b" " if code == "FOSUN_RAW_EVIDENCE_MISMATCH" else payload

            with pytest.raises(ValueError, match=code):
                await reclassify_daily_delivery(
                    session, manifest_id=parent, raw_reader=read_raw, normalized_reader=store.read
                )
    finally:
        await factory.kw["bind"].dispose()


def test_capability_migration_can_roll_back_before_versions_exist(
    migrated_database_url,
    monkeypatch,
):
    monkeypatch.setenv("DATABASE_URL", migrated_database_url)
    config = Config("engine/alembic.ini")
    command.downgrade(config, "0033_cash_flow_business_date")
    command.upgrade(config, "0034_capability_versions")
