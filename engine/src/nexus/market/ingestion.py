"""Raw-first atomic market page ingestion."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC
from hashlib import sha256
from io import BytesIO
from typing import Protocol
from uuid import uuid4

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.data import DataArtifact, PriceSeriesManifest
from nexus.providers.base import PriceBar, ProviderPage


class MarketObjectStore(Protocol):
    """Small S3-compatible boundary faked by all Task 7 tests."""

    async def archive_raw(self, key: str, payload: bytes) -> str: ...

    async def put_temporary(self, key: str, payload: bytes) -> None: ...

    async def promote(self, temporary_key: str, final_key: str) -> str: ...

    async def delete_temporary(self, key: str) -> None: ...

    async def delete(self, key: str) -> None: ...


@dataclass(frozen=True)
class MarketIngestionResult:
    """Committed database metadata plus the immutable raw evidence URI."""

    raw_uri: str
    artifact: DataArtifact
    manifest: PriceSeriesManifest


async def ingest_price_page(
    session: AsyncSession,
    object_store: MarketObjectStore,
    page: ProviderPage[PriceBar],
    *,
    source: str,
    feed: str,
    adjustment: str,
) -> MarketIngestionResult:
    """Archive raw, atomically publish Parquet, then commit metadata."""
    if not page.items:
        raise ValueError("market page must contain at least one bar")
    if not page.raw_payload:
        raise ValueError("market page must retain its raw payload")
    instrument_ids = {bar.instrument_id for bar in page.items}
    if len(instrument_ids) != 1:
        raise ValueError("one market page cannot mix instruments")

    received_at = max(bar.received_at or bar.available_at for bar in page.items)
    date_path = received_at.astimezone(UTC).strftime("%Y/%m/%d")
    raw_digest = sha256(page.raw_payload).hexdigest()
    request_id = page.source_request_id or raw_digest[:16]
    raw_key = f"raw/market/{date_path}/{request_id}/{raw_digest}.json"
    temporary_key: str | None = None
    final_key: str | None = None
    promoted = False
    committed = False
    try:
        raw_uri = await object_store.archive_raw(raw_key, page.raw_payload)
        parquet_payload = serialize_price_bars(page.items)
        parquet_digest = sha256(parquet_payload).hexdigest()
        instrument_id = next(iter(instrument_ids))
        final_key = (
            f"normalized/market/{date_path}/{instrument_id}/{parquet_digest}.parquet"
        )
        temporary_key = f"temporary/market/{uuid4()}.parquet"
        await object_store.put_temporary(temporary_key, parquet_payload)
        parquet_uri = await object_store.promote(temporary_key, final_key)
        promoted = True

        observed = [bar.observed_at or bar.available_at for bar in page.items]
        artifact = DataArtifact(
            uri=parquet_uri,
            sha256=parquet_digest,
            byte_size=len(parquet_payload),
            mime_type="application/vnd.apache.parquet",
            schema_version="price_bars_v1",
            source=source,
            observed_at=min(observed),
            available_at=max(bar.available_at for bar in page.items),
            received_at=received_at,
        )
        session.add(artifact)
        await session.flush()
        manifest = PriceSeriesManifest(
            data_artifact_id=artifact.id,
            instrument=str(instrument_id),
            source=source,
            feed=feed,
            adjustment=adjustment,
            start_at=min(observed),
            end_at=max(observed),
            row_count=len(page.items),
        )
        session.add(manifest)
        await session.commit()
        committed = True
        return MarketIngestionResult(raw_uri=raw_uri, artifact=artifact, manifest=manifest)
    except Exception:
        await session.rollback()
        if promoted and not committed and final_key is not None:
            await _safe_delete_final(object_store, final_key)
        raise
    finally:
        if temporary_key is not None:
            await _safe_delete_temporary(object_store, temporary_key)


def serialize_price_bars(bars: Sequence[PriceBar]) -> bytes:
    """Produce deterministic typed Parquet for the same normalized bars."""
    ordered = sorted(
        bars,
        key=lambda bar: (
            str(bar.instrument_id),
            bar.session,
            bar.available_at,
            bar.revision,
            str(bar.close),
        ),
    )
    decimal_type = pa.decimal128(38, 12)
    schema = pa.schema(
        [
            pa.field("instrument_id", pa.string(), nullable=False),
            pa.field("session", pa.date32(), nullable=False),
            pa.field("open", decimal_type, nullable=False),
            pa.field("high", decimal_type, nullable=False),
            pa.field("low", decimal_type, nullable=False),
            pa.field("close", decimal_type, nullable=False),
            pa.field("volume", decimal_type, nullable=False),
            pa.field("observed_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("available_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("received_at", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("revision", pa.string(), nullable=False),
            pa.field("quality_flags", pa.list_(pa.string()), nullable=False),
        ]
    )
    rows = [
        {
            "instrument_id": str(bar.instrument_id),
            "session": bar.session,
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "observed_at": bar.observed_at or bar.available_at,
            "available_at": bar.available_at,
            "received_at": bar.received_at or bar.available_at,
            "revision": bar.revision,
            "quality_flags": list(bar.quality_flags),
        }
        for bar in ordered
    ]
    table = pa.Table.from_pylist(rows, schema=schema)
    buffer = BytesIO()
    pq.write_table(
        table,
        buffer,
        compression="zstd",
        use_dictionary=False,
        write_statistics=True,
        version="2.6",
        data_page_version="2.0",
    )
    return buffer.getvalue()


async def _safe_delete_temporary(store: MarketObjectStore, key: str) -> None:
    try:
        await store.delete_temporary(key)
    except Exception:
        return


async def _safe_delete_final(store: MarketObjectStore, key: str) -> None:
    try:
        await store.delete(key)
    except Exception:
        return
