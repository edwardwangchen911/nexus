from datetime import UTC, date, datetime
from decimal import Decimal
from itertools import permutations
from uuid import UUID

import pytest
from nexus.market import price_bar_authority
from nexus.market.ingestion import (
    ingest_price_page,
    object_uri_matches_key,
    prepare_price_page,
    serialize_price_bars,
)
from nexus.persistence.models.data import (
    DataArtifact,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.persistence.session import session_factory
from nexus.providers.base import MarketDataProvenance, PriceBar, ProviderPage
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
        provenance=MarketDataProvenance(
            source="fixture",
            feed="synthetic",
            adjustment="all",
            interval="1Day",
            mode="fixture",
            delivery_mode="fixture",
        ),
    )


def _clean_page() -> ProviderPage[PriceBar]:
    clean_bar = _bars()[0].model_copy(update={"quality_flags": ()})
    return _page().model_copy(update={"items": (clean_bar,)})


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

    async def put_normalized(self, key: str, payload: bytes) -> str:
        self.events.append(("put_normalized", key))
        if self.fail_at == "put_normalized":
            raise OSError("normalized write failure")
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


class QueryClaimingObjectStore(FakeObjectStore):
    async def put_normalized(self, key: str, payload: bytes) -> str:
        await super().put_normalized(key, payload)
        return f"memory://wrong-object?claimed={key}"


class RawQueryClaimingObjectStore(FakeObjectStore):
    async def archive_raw(self, key: str, payload: bytes) -> str:
        await super().archive_raw(key, payload)
        return f"memory://wrong-object?claimed={key}"


@pytest.mark.asyncio
async def test_prepare_rejects_normalized_uri_that_only_claims_key_in_query() -> None:
    with pytest.raises(RuntimeError, match="MARKET_NORMALIZED_OBJECT_URI_MISMATCH"):
        await prepare_price_page(QueryClaimingObjectStore(), _page())


@pytest.mark.asyncio
async def test_prepare_rejects_raw_uri_that_only_claims_key_in_query() -> None:
    with pytest.raises(RuntimeError, match="MARKET_RAW_OBJECT_URI_MISMATCH"):
        await prepare_price_page(RawQueryClaimingObjectStore(), _page())


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("memory://normalized/market/date/id/digest.parquet", True),
        (
            "s3://bucket/provider/market/normalized/market/date/id/digest.parquet?versionId=v%2F1",
            True,
        ),
        ("memory://prefix/normalized/market/date/id/digest.parquet", False),
        (
            "memory://wrong?claimed=normalized/market/date/id/digest.parquet",
            False,
        ),
        (
            "s3://bucket/provider/market/wrong.parquet"
            "?claimed=normalized/market/date/id/digest.parquet",
            False,
        ),
        (
            "s3://user@bucket/provider/market/normalized/market/date/id/digest.parquet"
            "?versionId=v1",
            False,
        ),
        (
            "s3://bucket/provider/market/normalized/market/date/id/digest.parquet"
            "?versionId=v1#fragment",
            False,
        ),
        (
            "s3://bucket/provider/market/normalized/market/date/id/digest.parquet?versionId=v1#",
            False,
        ),
        (
            "s3://bucket/provider/market/normalized/market/date/id/digest.parquet"
            "?versionId=v1&extra=1",
            False,
        ),
        (
            "s3://bucket/provider/market/normalized/market/date/id/digest.parquet?versionId=%ZZ",
            False,
        ),
        (
            "s3://bucket/provider/market/normalized/market/date/id/digest.parquet"
            "?versionId=null",
            False,
        ),
        (
            "s3://bucket/provider/market/normalized/market/date/id/digest.parquet"
            "?versionId=%6Eull",
            False,
        ),
        (
            "s3://bucket/provider/../normalized/market/date/id/digest.parquet?versionId=v1",
            False,
        ),
        (
            "https://bucket/normalized/market/date/id/digest.parquet?versionId=v1",
            False,
        ),
    ],
)
def test_normalized_object_uri_contract_requires_exact_structured_key(
    uri: str,
    expected: bool,
) -> None:
    key = "normalized/market/date/id/digest.parquet"
    assert object_uri_matches_key(uri, key) is expected


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
        )

    assert [event for event, _ in store.events[:2]] == [
        "archive_raw",
        "put_normalized",
    ]
    assert result.raw_uri.startswith("memory://raw/market/")
    assert result.artifact.uri.startswith("memory://normalized/market/")
    assert result.manifest.delivery_mode == "fixture"
    assert result.price_authority is None
    assert result.price_evidence_ref is None
    assert store.objects[result.artifact.uri.removeprefix("memory://")].startswith(b"PAR1")
    assert store.temporary == {}

    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 1
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 1
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 1
        assert await session.scalar(select(func.count()).select_from(ResearchEvidenceRef)) == 1
        persisted = await session.scalar(select(PriceSeriesManifest))
        assert persisted is not None
        assert persisted.delivery_mode == "fixture"
        evidence = await session.get(ResearchEvidenceRef, persisted.evidence_ref_id)
        assert evidence is not None
        assert (
            evidence.provider_object_id,
            evidence.revision,
            evidence.location,
            evidence.content_hash,
        ) == (
            f"price-series-manifest:{persisted.id}",
            result.artifact.sha256,
            result.artifact.uri,
            result.artifact.sha256,
        )


@pytest.mark.asyncio
async def test_clean_ingestion_atomically_finalizes_db_bar_set_evidence(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        result = await ingest_price_page(session, FakeObjectStore(), _clean_page())

    assert result.price_authority is not None
    assert result.price_evidence_ref is not None
    assert result.evidence_ref.content_hash == result.artifact.sha256
    assert result.price_evidence_ref.content_hash == result.price_authority.content_hash
    assert result.price_evidence_ref.location == (
        f"db://price-bar-set/{result.manifest.id}@sha256:{result.price_authority.content_hash}"
    )
    assert result.price_evidence_ref.content_hash != result.artifact.sha256


@pytest.mark.asyncio
async def test_flagged_ingestion_remains_diagnostic_only(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        result = await ingest_price_page(session, FakeObjectStore(), _page())

    assert result.price_authority is None
    assert result.price_evidence_ref is None


@pytest.mark.asyncio
async def test_authority_constraint_failure_rolls_back_ingestion_metadata(
    migrated_database_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A rejected authority cannot leave archive metadata or Bars committed."""
    monkeypatch.setattr(price_bar_authority, "HASH_VERSION", "not-a-valid-hash-version")
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        with pytest.raises(IntegrityError):
            await ingest_price_page(session, FakeObjectStore(), _clean_page())

    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 0
        assert await session.scalar(select(func.count()).select_from(ResearchEvidenceRef)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarRecord)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceBarSetAuthority)) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_at", ["archive_raw", "put_normalized"])
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
            )

    assert store.temporary == {}
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 0
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 0


@pytest.mark.asyncio
async def test_duplicate_ingestion_never_deletes_the_shared_content_addressed_object(
    migrated_database_url: str,
) -> None:
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    store = FakeObjectStore()
    async with factory() as session:
        first = await ingest_price_page(session, store, _page())

    async with factory() as session:
        with pytest.raises(IntegrityError):
            await ingest_price_page(session, store, _page())

    final_key = first.artifact.uri.removeprefix("memory://")
    assert final_key in store.objects
    assert not any(event == "delete" for event, _ in store.events)
    async with factory() as session:
        assert await session.scalar(select(func.count()).select_from(DataArtifact)) == 1
        assert await session.scalar(select(func.count()).select_from(PriceSeriesManifest)) == 1


@pytest.mark.asyncio
async def test_concurrent_duplicate_ingestion_keeps_the_shared_final_object(
    migrated_database_url: str,
) -> None:
    import asyncio

    store = FakeObjectStore()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))

    async def ingest_once() -> object:
        async with factory() as session:
            try:
                return await ingest_price_page(session, store, _page())
            except IntegrityError as error:
                return error

    results = await asyncio.gather(ingest_once(), ingest_once())

    assert sum(not isinstance(result, IntegrityError) for result in results) == 1
    final_keys = [key for key in store.objects if key.startswith("normalized/market/")]
    assert len(final_keys) == 1
    assert store.objects[final_keys[0]].startswith(b"PAR1")
    assert not any(event == "delete" for event, _ in store.events)


@pytest.mark.asyncio
async def test_ingestion_provenance_cannot_be_overridden_by_the_caller(
    migrated_database_url: str,
) -> None:
    store = FakeObjectStore()
    factory = session_factory(EngineSettings(database_url=migrated_database_url))
    async with factory() as session:
        with pytest.raises(TypeError):
            await ingest_price_page(  # type: ignore[call-arg]
                session,
                store,
                _page(),
                source="alpaca",
            )


def test_normalized_parquet_is_deterministic_under_complete_row_permutations() -> None:
    base = _bars()[0]
    bars = (
        base,
        base.model_copy(
            update={
                "open": Decimal("99.5"),
                "high": Decimal("101.5"),
                "volume": Decimal("900"),
                "observed_at": OBSERVED_AT.replace(microsecond=1),
                "received_at": RECEIVED_AT.replace(microsecond=1),
                "quality_flags": ("z", "a"),
            }
        ),
        base.model_copy(
            update={
                "low": Decimal("98.5"),
                "volume": Decimal("1100"),
                "observed_at": OBSERVED_AT.replace(microsecond=2),
                "received_at": RECEIVED_AT.replace(microsecond=2),
                "quality_flags": ("a", "z"),
            }
        ),
    )

    payloads = {serialize_price_bars(order) for order in permutations(bars)}

    assert len(payloads) == 1
