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

from nexus.domain.ids import new_uuid7
from nexus.persistence.models.data import DataArtifact, PriceBarRecord, PriceSeriesManifest
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.providers.base import MarketDataProvenance, PriceBar, ProviderPage


class MarketObjectStore(Protocol):
    """Small S3-compatible boundary faked by all Task 7 tests."""

    async def archive_raw(self, key: str, payload: bytes) -> str: ...

    async def put_temporary(self, key: str, payload: bytes) -> None: ...

    async def promote(self, temporary_key: str, final_key: str) -> str: ...

    async def delete_temporary(self, key: str) -> None: ...


@dataclass(frozen=True)
class MarketIngestionResult:
    """Committed database metadata plus the immutable raw evidence URI."""

    raw_uri: str
    artifact: DataArtifact
    manifest: PriceSeriesManifest
    evidence_ref: ResearchEvidenceRef
    provenance: MarketDataProvenance


async def ingest_price_page(
    session: AsyncSession,
    object_store: MarketObjectStore,
    page: ProviderPage[PriceBar],
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

        observed = [bar.observed_at or bar.available_at for bar in page.items]
        artifact = DataArtifact(
            uri=parquet_uri,
            sha256=parquet_digest,
            byte_size=len(parquet_payload),
            mime_type="application/vnd.apache.parquet",
            schema_version="price_bars_v1",
            source=page.provenance.source,
            observed_at=min(observed),
            available_at=max(bar.available_at for bar in page.items),
            received_at=received_at,
        )
        session.add(artifact)
        await session.flush()
        manifest_id = new_uuid7()
        evidence_ref = ResearchEvidenceRef(
            source=artifact.source,
            provider_object_id=f"price-series-manifest:{manifest_id}",
            revision=artifact.sha256,
            available_at=artifact.available_at,
            location=artifact.uri,
            content_hash=artifact.sha256,
            byte_length=artifact.byte_size,
        )
        session.add(evidence_ref)
        await session.flush()
        manifest = PriceSeriesManifest(
            id=manifest_id,
            evidence_ref_id=evidence_ref.id,
            data_artifact_id=artifact.id,
            instrument=str(instrument_id),
            source=page.provenance.source,
            feed=page.provenance.feed,
            delivery_mode=page.provenance.delivery_mode,
            adjustment=page.provenance.adjustment,
            interval=page.provenance.interval,
            start_at=min(observed),
            end_at=max(observed),
            row_count=len(page.items),
        )
        session.add(manifest)
        await session.flush()
        session.add_all(
            PriceBarRecord(
                price_series_manifest_id=manifest.id,
                instrument_id=bar.instrument_id,
                session=bar.session,
                open=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=bar.volume,
                observed_at=bar.observed_at or bar.available_at,
                available_at=bar.available_at,
                received_at=bar.received_at or bar.available_at,
                revision=bar.revision,
                quality_flags=list(bar.quality_flags),
            )
            for bar in page.items
        )
        await session.commit()
        return MarketIngestionResult(
            raw_uri=raw_uri,
            artifact=artifact,
            manifest=manifest,
            evidence_ref=evidence_ref,
            provenance=page.provenance,
        )
    except Exception:
        await session.rollback()
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
            str(bar.open),
            str(bar.high),
            str(bar.low),
            str(bar.close),
            str(bar.volume),
            bar.observed_at or bar.available_at,
            bar.available_at,
            bar.received_at or bar.available_at,
            bar.revision,
            tuple(sorted(bar.quality_flags)),
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
            "quality_flags": sorted(bar.quality_flags),
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
