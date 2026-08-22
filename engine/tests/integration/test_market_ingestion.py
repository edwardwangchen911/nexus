from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest
from nexus.market.ingestion import ingest_price_page, serialize_price_bars
from nexus.persistence.models.data import DataArtifact, PriceSeriesManifest
from nexus.persistence.session import session_factory
from nexus.providers.base import PriceBar, ProviderPage
from nexus.settings import EngineSettings
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError

INSTRUMENT_ID = UUID("38df053d-1e87-5938-b279-a1ef5ab4643e")
OBSERVED_AT = datetime(2026, 8, 21, 20, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 8, 21, 20, 15, tzinfo=UTC)
RECEIVED_AT = datetime(2026, 8, 21, 20, 16, tzinfo=UTC)


def _bars() -> tuple[PriceBar, ...]:
    return (
        PriceBar(
            instrument_id=INSTRUMENT_ID,
            session=date(2026, 8, 21),
            open=Decimal("100.000000000000"),
            high=Decimal("101.000000000000"),
            low=Decimal("99.000000000000"),
            close=Decimal("100.500000000000"),
            volume=Decimal("1000.000000000000"),
            observed_at=OBSERVED_AT,
            available_at=AVAILABLE_AT,
            received_at=RECEIVED_AT,
            revision="fixture-r1",
            quality_flags=("synthetic",),
        ),
    )


def _page() -> ProviderPage[PriceBar]:
    return ProviderPage(
        items=_bars(),
        next_cursor=None,
        raw_payload=b'{"bars":[{"fixture":true}],"request_id":"fixture-1"}',
        source_request_id="fixture-1",
        as_of=RECEIVED_AT,
    )


class FakeObjectStore:
    def __init__(self, fail_at: str | None = None) -> None:
        self.fail_at = fail_at
        self.events: list[tuple[str, str]] = []
        self.objects: dict[str, bytes] = {}
        self.temporary: dict[str, bytes] = {}

    async def archive_raw(self, key: str, payload: bytes) -> str:
        self.events.append(("archive_raw", key))
        if self.fail_at == "archive_raw":
            raise OSError("archive failure")
        self.objects[key] = payload
        return f"memory://{key}"

    async def put_temporary(self, key: str, payload: bytes) -> None:
        self.events.append(("put_temporary", key))
        if self.fail_at == "put_temporary":
            raise OSError("temporary write failure")
        self.temporary[key] = payload

    async def promote(self, temporary_key: str, final_key: str) -> str:
        self.events.append(("promote", final_key))
        if self.fail_at == "promote":
            raise OSError("promote failure")
        self.objects[final_key] = self.temporary.pop(temporary_key)
        return f"memory://{final_key}"

    async def delete_temporary(self, key: str) -> None:
        self.events.append(("delete_temporary", key))
        self.temporary.pop(key, None)

    async def delete(self, key: str) -> None:
        self.events.append(("delete", key))
        self.objects.pop(key, None)


@pytest.mark.asyncio
async def test_ingestion_archives_raw_then_promotes_parquet_before_metadata_commit(
    migrated_database_url: str,
) -> None:
    store = FakeObjectStore()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        result = await ingest_price_page(
            session,
            store,
            _page(),
            source="fixture",
            feed="synthetic",
            adjustment="all",
        )

    assert [event for event, _ in store.events[:3]] == [
        "archive_raw",
        "put_temporary",
        "promote",
    ]
    assert result.raw_uri.startswith("memory://raw/market/")
    assert result.artifact.uri.startswith("memory://normalized/market/")
    assert store.objects[result.artifact.uri.removeprefix("memory://")].startswith(b"PAR1")
    assert store.temporary == {}

    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 1
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_at", ["archive_raw", "put_temporary", "promote"])
async def test_object_failure_rolls_back_metadata_and_safely_cleans_temporary_objects(
    migrated_database_url: str,
    fail_at: str,
) -> None:
    store = FakeObjectStore(fail_at)
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        with pytest.raises(OSError):
            await ingest_price_page(
                session,
                store,
                _page(),
                source="fixture",
                feed="synthetic",
                adjustment="all",
            )

    assert store.temporary == {}
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0


@pytest.mark.asyncio
async def test_database_commit_failure_rolls_back_new_manifest_and_cleans_promoted_object(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    first_store = FakeObjectStore()
    async with factory() as session:
        await ingest_price_page(
            session,
            first_store,
            _page(),
            source="fixture",
            feed="synthetic",
            adjustment="all",
        )

    second_store = FakeObjectStore()
    async with factory() as session:
        with pytest.raises(IntegrityError):
            await ingest_price_page(
                session,
                second_store,
                _page(),
                source="fixture",
                feed="synthetic",
                adjustment="all",
            )

    assert any(event == "delete" for event, _ in second_store.events)
    assert not any(key.startswith("normalized/market/") for key in second_store.objects)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 1
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 1


def test_normalized_parquet_is_deterministic_for_the_same_bars() -> None:
    bars = _bars()

    assert serialize_price_bars(bars) == serialize_price_bars(tuple(reversed(bars)))
