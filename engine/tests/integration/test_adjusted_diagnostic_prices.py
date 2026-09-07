from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from uuid import UUID

import pytest
from alembic import command
from alembic.config import Config
from alembic.script import ScriptDirectory
from nexus.diagnostics.assembler import load_diagnostic_source_policy
from nexus.diagnostics.series_authority import select_authoritative_series
from nexus.market.ingestion import ingest_price_page
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.market import Instrument
from nexus.persistence.session import session_factory
from nexus.providers.base import MarketDataProvenance, PriceBar, ProviderPage
from nexus.settings import EngineSettings

INSTRUMENT_ID = UUID("019d0000-0000-7000-8000-0000000003a1")


class FakeObjectStore:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def archive_raw(self, key: str, payload: bytes) -> str:
        self.objects[key] = payload
        return f"memory://{key}"

    async def put_normalized(self, key: str, payload: bytes) -> str:
        self.objects[key] = payload
        return f"memory://{key}"


class _Reader:
    def __init__(self, store: FakeObjectStore) -> None:
        self.store = store

    async def __call__(self, uri: str) -> bytes:
        return self.store.objects[uri.removeprefix("memory://")]


@pytest.mark.asyncio
async def test_v3_selects_only_frozen_alpaca_all_authority(
    migrated_database_url: str,
) -> None:
    now = datetime.now(UTC)
    bar = PriceBar(
        instrument_id=INSTRUMENT_ID,
        session=date.today() - timedelta(days=2),
        open=Decimal("100"),
        high=Decimal("102"),
        low=Decimal("99"),
        close=Decimal("101"),
        volume=Decimal("1000"),
        observed_at=now - timedelta(days=2),
        available_at=now - timedelta(minutes=1),
        received_at=now - timedelta(minutes=1),
        revision="source-revision",
        quality_flags=(),
    )
    page = ProviderPage(
        items=(bar,),
        next_cursor=None,
        raw_payload=b'{"symbol":"TEST","bars":[{"closed":true}],"next_page_token":null}',
        source_request_id=None,
        as_of=bar.received_at,
        provenance=MarketDataProvenance(
            source="alpaca",
            feed="sip",
            adjustment="all",
            interval="1Day",
            mode="delayed",
            delivery_mode="delayed",
        ),
    )
    store = FakeObjectStore()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        session.add(
            Instrument(
                id=INSTRUMENT_ID,
                asset_type="common_stock",
                exchange="XNYS",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="synthetic",
            )
        )
        await session.flush()
        result = await ingest_price_page(session, store, page, commit=False)
        assert result.price_authority is not None
    as_of = datetime.now(UTC) + timedelta(seconds=1)
    async with factory() as session:
        policy = await load_diagnostic_source_policy(session, as_of)
        selected = await select_authoritative_series(
            session, INSTRUMENT_ID, as_of, _Reader(store), policy
        )

    assert policy.version == "source_policy_v3"
    assert selected.quality == "PASS"
    assert selected.selected is not None
    assert selected.selected[0].source == "alpaca"
    assert selected.selected[0].adjustment == "all"


def test_adjusted_policy_migration_roundtrip(
    temporary_database_url: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    engine_root = Path(__file__).resolve().parents[2]
    monkeypatch.setenv("DATABASE_URL", temporary_database_url)
    config = Config(str(engine_root / "alembic.ini"))
    assert ScriptDirectory.from_config(config).get_heads() == [CURRENT_MIGRATION_HEAD]
    command.upgrade(config, CURRENT_MIGRATION_HEAD)
    command.downgrade(config, "0044_news_research")
    command.upgrade(config, CURRENT_MIGRATION_HEAD)
