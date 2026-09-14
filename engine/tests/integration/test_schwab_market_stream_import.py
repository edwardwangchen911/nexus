import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.market.database_series import load_intraday_series
from nexus.ops import schwab_market_stream
from nexus.persistence.models.data import DataArtifact
from nexus.persistence.models.market import Instrument, InstrumentAlias, LatestQuote
from nexus.persistence.session import session_factory
from nexus.providers.market.schwab_stream import SchwabStreamCredentials
from nexus.settings import EngineSettings
from sqlalchemy import func, select

INSTRUMENT_ID = UUID("019d0000-0000-7000-8000-0000000000c1")


class DatabaseUUID(UUID):
    """Mirror UUID subclasses returned by database drivers such as asyncpg."""


DATABASE_INSTRUMENT_ID = DatabaseUUID(str(INSTRUMENT_ID))
CREDENTIALS = SchwabStreamCredentials(
    access_token="access-token",
    customer_id="customer",
    correlation_id="correlation",
    channel="channel",
    function_id="function",
)


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


def _frames(event_at: datetime) -> list[object]:
    millis = int(event_at.timestamp() * 1000)
    return [
        {
            "response": [
                {
                    "service": "ADMIN",
                    "command": "LOGIN",
                    "requestid": "1",
                    "timestamp": millis - 2,
                    "content": {"code": 0, "msg": "LOGIN command succeeded"},
                }
            ]
        },
        {
            "response": [
                {
                    "service": "LEVELONE_EQUITIES",
                    "command": "SUBS",
                    "requestid": "2",
                    "timestamp": millis - 1,
                    "content": {"code": 0, "msg": "SUBS command succeeded"},
                }
            ]
        },
        {
            "data": [
                {
                    "service": "LEVELONE_EQUITIES",
                    "timestamp": millis,
                    "command": "SUBS",
                    "content": [
                        {
                            "key": "AAPL",
                            "1": 227.14,
                            "2": 227.18,
                            "3": 227.15,
                            "4": 3,
                            "5": 2,
                            "8": 1000,
                            "9": 1,
                            "34": millis,
                            "35": millis,
                            "37": millis,
                            "38": millis,
                            "39": "ARCX",
                            "40": "XNAS",
                            "41": "XNYS",
                            "delayed": False,
                            "assetMainType": "EQUITY",
                        }
                    ],
                }
            ]
        },
    ]


@pytest.mark.asyncio
async def test_schwab_archive_import_accepts_database_uuid_subclass(
    migrated_database_url: str,
    tmp_path,
) -> None:
    event_at = datetime.now(UTC) - timedelta(seconds=1)
    directory = tmp_path / "schwab-stream"
    await schwab_market_stream.collect_schwab_stream(
        symbols=("AAPL",),
        directory=directory,
        streamer_url="wss://streamer-api.schwab.com/ws",
        credentials=CREDENTIALS,
        max_frames=3,
        connector=lambda *args, **kwargs: Socket(_frames(event_at)),
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
            result = await schwab_market_stream.import_schwab_market_stream(
                session,
                store,
                bindings={"AAPL": DATABASE_INSTRUMENT_ID},
                directory=directory,
                imported_at=datetime.now(UTC) + timedelta(seconds=replay),
            )
        assert result.created_observations == (2 if replay == 0 else 0)

    async with factory() as session:
        artifact = await session.scalar(select(DataArtifact))
        observations = tuple(await session.scalars(select(LatestQuote)))
        view = await load_intraday_series(
            session,
            INSTRUMENT_ID,
            datetime.now(UTC) + timedelta(seconds=3),
        )
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 1
        assert await session.scalar(select(func.count()).select_from(LatestQuote)) == 2
    assert artifact is not None
    assert result.artifact_id == artifact.id
    assert result.quote_ids == {
        "AAPL": next(row.id for row in observations if row.event_kind == "quote")
    }
    assert result.trade_ids == {
        "AAPL": next(row.id for row in observations if row.event_kind == "trade")
    }
    assert artifact.source == "schwab"
    assert artifact.schema_version == "schwab_level_one_stream_v1"
    assert view.source == "schwab"
    assert view.feed == "l1"
    assert view.delivery_mode == "real_time"
    assert view.trade is not None and view.trade.last == Decimal("227.15")
    assert view.quote is not None and view.quote.bid == Decimal("227.14")
    assert view.quote.ask == Decimal("227.18")
