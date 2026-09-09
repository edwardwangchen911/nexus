import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.market.database_series import load_intraday_series
from nexus.ops.alpaca_market_stream import collect_market_stream, import_market_stream
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.models.market import Instrument, InstrumentAlias, LatestQuote
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings
from sqlalchemy import delete, func, select, text, update
from sqlalchemy.exc import IntegrityError

INSTRUMENT_ID = UUID("019d0000-0000-7000-8000-0000000000a1")


class Socket:
    def __init__(self, frames: list[object]) -> None:
        self.frames = list(frames)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return None

    async def recv(self):
        return json.dumps(self.frames.pop(0))

    async def send(self, message: str) -> None:
        del message


class Store:
    def __init__(self) -> None:
        self.objects: dict[str, bytes] = {}

    async def archive_raw(self, key: str, payload: bytes) -> str:
        self.objects.setdefault(key, payload)
        return f"memory://{key}"

    async def read_raw(self, key: str, *, uri: str) -> bytes:
        assert uri == f"memory://{key}"
        return self.objects[key]


def _frames(symbol: str, event_at: datetime) -> list[object]:
    timestamp = event_at.isoformat().replace("+00:00", "Z")
    return [
        [{"T": "success", "msg": "connected"}],
        [{"T": "success", "msg": "authenticated"}],
        [
            {
                "T": "subscription",
                "trades": [symbol],
                "quotes": [symbol],
                "bars": [],
                "updatedBars": [],
                "dailyBars": [],
                "statuses": [],
                "lulds": [],
                "corrections": [symbol],
                "cancelErrors": [symbol],
            }
        ],
        [
            {
                "T": "t",
                "S": symbol,
                "i": 17,
                "x": "V",
                "p": 227.15,
                "s": 2,
                "c": ["@"],
                "t": timestamp,
                "z": "C",
            },
            {
                "T": "q",
                "S": symbol,
                "bx": "V",
                "bp": 227.14,
                "bs": 1,
                "ax": "V",
                "ap": 227.18,
                "as": 1,
                "c": ["R"],
                "t": timestamp,
                "z": "C",
            },
        ],
    ]


@pytest.mark.asyncio
async def test_imported_iex_stream_is_idempotent_and_visible_to_intraday_loader(
    migrated_database_url: str,
    tmp_path,
) -> None:
    event_at = datetime.now(UTC) - timedelta(seconds=1)
    frames = _frames("AAPL", event_at)
    directory = tmp_path / "stream"
    await collect_market_stream(
        symbol="AAPL",
        directory=directory,
        key="example-key",
        secret="example-secret",
        max_frames=4,
        connector=lambda *args, **kwargs: Socket(frames),
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session, session.begin():
        session.add(
            Instrument(
                id=INSTRUMENT_ID,
                asset_type="equity",
                exchange="XNAS",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="us_equity",
                created_at=event_at - timedelta(days=1),
            )
        )
        session.add(
            InstrumentAlias(
                instrument_id=INSTRUMENT_ID,
                symbol="AAPL",
                alias_type="ticker",
                valid_from=date(2020, 1, 1),
                valid_to=None,
                created_at=event_at - timedelta(days=1),
            )
        )

    store = Store()
    for replay in range(2):
        async with factory() as session, session.begin():
            result = await import_market_stream(
                session,
                store,
                instrument_id=INSTRUMENT_ID,
                directory=directory,
                imported_at=datetime.now(UTC) + timedelta(seconds=replay),
            )
        assert result.created_observations == (2 if replay == 0 else 0)

    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 1
        assert await session.scalar(select(func.count()).select_from(LatestQuote)) == 2
        view = await load_intraday_series(
            session,
            INSTRUMENT_ID,
            datetime.now(UTC) + timedelta(seconds=3),
        )
        artifact = await session.scalar(select(DataArtifact))
        assert artifact is not None
    assert view.source == "alpaca"
    assert view.feed == "iex"
    assert view.delivery_mode == "real_time"
    assert view.bars == ()
    assert view.trade is not None and view.trade.last == Decimal("227.15")
    assert view.quote is not None and view.quote.bid == Decimal("227.14")
    assert view.quote.ask == Decimal("227.18")

    async with factory() as session:
        with pytest.raises(IntegrityError, match="immutable"):
            await session.execute(
                update(DataArtifact)
                .where(DataArtifact.id == artifact.id)
                .values(uri="memory://tampered")
            )
            await session.commit()
        await session.rollback()
    async with factory() as session:
        with pytest.raises(IntegrityError, match="immutable"):
            await session.execute(delete(DataArtifact).where(DataArtifact.id == artifact.id))
            await session.commit()
        await session.rollback()
    async with factory() as session, session.begin():
        await session.execute(
            text("ALTER TABLE data_artifacts DISABLE TRIGGER USER")
        )
        await session.execute(
            update(DataArtifact)
            .where(DataArtifact.id == artifact.id)
            .values(observed_at=artifact.observed_at - timedelta(microseconds=1))
        )
        await session.execute(
            text("ALTER TABLE data_artifacts ENABLE TRIGGER USER")
        )
    async with factory() as session, session.begin():
        with pytest.raises(
            ValueError,
            match="ALPACA_MARKET_STREAM_ARTIFACT_REPLAY_CONFLICT",
        ):
            await import_market_stream(
                session,
                store,
                instrument_id=INSTRUMENT_ID,
                directory=directory,
                imported_at=datetime.now(UTC) + timedelta(seconds=4),
            )


@pytest.mark.asyncio
async def test_import_rejects_usd_instrument_outside_us_equity_exchanges(
    migrated_database_url: str,
    tmp_path,
) -> None:
    event_at = datetime.now(UTC) - timedelta(seconds=1)
    directory = tmp_path / "stream"
    await collect_market_stream(
        symbol="HKG656",
        directory=directory,
        key="example-key",
        secret="example-secret",
        max_frames=4,
        connector=lambda *args, **kwargs: Socket(_frames("HKG656", event_at)),
    )
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    instrument_id = UUID("019d0000-0000-7000-8000-0000000000b2")
    async with factory() as session, session.begin():
        session.add(
            Instrument(
                id=instrument_id,
                asset_type="equity",
                exchange="XHKG",
                currency="USD",
                tick_size=Decimal("0.01"),
                risk_bucket="hk_equity",
                created_at=event_at - timedelta(days=1),
            )
        )
        session.add(
            InstrumentAlias(
                instrument_id=instrument_id,
                symbol="HKG656",
                alias_type="ticker",
                valid_from=date(2020, 1, 1),
                valid_to=None,
                created_at=event_at - timedelta(days=1),
            )
        )

    store = Store()
    async with factory() as session, session.begin():
        with pytest.raises(ValueError, match="ALPACA_MARKET_STREAM_IDENTITY_REQUIRED"):
            await import_market_stream(
                session,
                store,
                instrument_id=instrument_id,
                directory=directory,
                imported_at=datetime.now(UTC),
            )
    assert store.objects == {}
