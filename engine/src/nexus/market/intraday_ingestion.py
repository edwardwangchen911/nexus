"""Raw-first, replay-safe intraday Snapshot ingestion."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from uuid import UUID, uuid4

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import canonical_json
from nexus.domain.ids import new_uuid7
from nexus.market.ingestion import MarketObjectStore, serialize_price_bars
from nexus.market.intraday_authority import finalize_intraday_bar_set
from nexus.persistence.models.data import (
    DataArtifact,
    IntradayBarSetAuthority,
    PriceBarRecord,
    PriceSeriesManifest,
)
from nexus.persistence.models.market import LatestQuote
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.providers.base import PriceBar
from nexus.providers.market.snapshot_contracts import SnapshotBatch


@dataclass(frozen=True)
class IntradayIngestionResult:
    raw_uri: str
    raw_artifact: DataArtifact
    created_trade_observations: int
    created_quote_observations: int
    created_bar_authorities: int
    replayed: int


@dataclass(frozen=True)
class _PublishedBar:
    bar: PriceBar
    payload: bytes
    digest: str
    uri: str


def canonical_quote_observation(
    *,
    instrument_id: UUID,
    source: str,
    feed: str,
    delivery_mode: str,
    event_kind: str,
    bid: Decimal | None,
    ask: Decimal | None,
    last: Decimal | None,
    observed_at: datetime,
    available_at: datetime,
    received_at: datetime,
    revision: str,
) -> bytes:
    """Return the exact canonical bytes covered by one observation hash."""
    return canonical_json(
        {
            "instrument_id": str(instrument_id),
            "source": source,
            "feed": feed,
            "delivery_mode": delivery_mode,
            "event_kind": event_kind,
            "bid": None if bid is None else str(bid),
            "ask": None if ask is None else str(ask),
            "last": None if last is None else str(last),
            "observed_at": _utc_text(observed_at),
            "available_at": _utc_text(available_at),
            "received_at": _utc_text(received_at),
            "revision": revision,
        }
    ).encode("utf-8")


async def ingest_intraday_snapshot(
    session: AsyncSession,
    object_store: MarketObjectStore,
    batch: SnapshotBatch,
    *,
    commit: bool = True,
) -> IntradayIngestionResult:
    """Archive raw bytes, publish normalized objects, then expose DB authorities."""
    if not batch.raw_payload:
        raise ValueError("intraday batch must retain its raw payload")
    raw_digest = sha256(batch.raw_payload).hexdigest()
    date_path = batch.received_at.astimezone(UTC).strftime("%Y/%m/%d")
    raw_key = f"raw/market/intraday/{date_path}/{raw_digest}.json"
    temporary_keys: list[str] = []
    try:
        raw_uri = await object_store.archive_raw(raw_key, batch.raw_payload)
        published: list[_PublishedBar] = []
        for snapshot in batch.snapshots:
            if snapshot.minute_bar is None:
                continue
            bar = snapshot.minute_bar.bar
            payload = serialize_price_bars((bar,))
            digest = sha256(payload).hexdigest()
            final_key = (
                f"normalized/market/intraday/{date_path}/{snapshot.instrument_id}/{digest}.parquet"
            )
            temporary_key = f"temporary/market/{uuid4()}.parquet"
            temporary_keys.append(temporary_key)
            await object_store.put_temporary(temporary_key, payload)
            uri = await object_store.promote(temporary_key, final_key)
            published.append(_PublishedBar(bar=bar, payload=payload, digest=digest, uri=uri))

        observed_times = tuple(
            observed
            for snapshot in batch.snapshots
            for observed in (
                None if snapshot.trade is None else snapshot.trade.observed_at,
                None if snapshot.quote is None else snapshot.quote.observed_at,
                None if snapshot.minute_bar is None else snapshot.minute_bar.bar.observed_at,
            )
            if observed is not None
        )
        raw_artifact, _ = await _get_or_create_artifact(
            session,
            uri=raw_uri,
            digest=raw_digest,
            byte_size=len(batch.raw_payload),
            mime_type="application/json",
            schema_version="alpaca_stock_snapshots_v1",
            source=batch.provenance.source,
            observed_at=min(observed_times, default=batch.received_at),
            available_at=batch.received_at,
            received_at=batch.received_at,
        )

        created_trade = 0
        created_quote = 0
        created_bars = 0
        replayed = 0
        for snapshot in batch.snapshots:
            if snapshot.trade is not None:
                revision = (
                    raw_digest
                    if snapshot.trade.provider_trade_id is None
                    else f"{snapshot.trade.provider_trade_id}:{raw_digest}"
                )
                created = await _insert_observation(
                    session,
                    raw_artifact=raw_artifact,
                    instrument_id=snapshot.instrument_id,
                    source=batch.provenance.source,
                    feed=batch.provenance.feed,
                    delivery_mode=batch.provenance.delivery_mode,
                    event_kind="trade",
                    bid=None,
                    ask=None,
                    last=snapshot.trade.price,
                    observed_at=snapshot.trade.observed_at,
                    available_at=batch.received_at,
                    received_at=batch.received_at,
                    revision=revision,
                )
                created_trade += int(created)
                replayed += int(not created)
            if snapshot.quote is not None:
                created = await _insert_observation(
                    session,
                    raw_artifact=raw_artifact,
                    instrument_id=snapshot.instrument_id,
                    source=batch.provenance.source,
                    feed=batch.provenance.feed,
                    delivery_mode=batch.provenance.delivery_mode,
                    event_kind="quote",
                    bid=snapshot.quote.bid,
                    ask=snapshot.quote.ask,
                    last=None,
                    observed_at=snapshot.quote.observed_at,
                    available_at=batch.received_at,
                    received_at=batch.received_at,
                    revision=raw_digest,
                )
                created_quote += int(created)
                replayed += int(not created)

        for item in published:
            created = await _persist_bar_authority(
                session,
                item=item,
                source=batch.provenance.source,
                feed=batch.provenance.feed,
                delivery_mode=batch.provenance.delivery_mode,
            )
            created_bars += int(created)
            replayed += int(not created)

        if commit:
            await session.commit()
        return IntradayIngestionResult(
            raw_uri=raw_uri,
            raw_artifact=raw_artifact,
            created_trade_observations=created_trade,
            created_quote_observations=created_quote,
            created_bar_authorities=created_bars,
            replayed=replayed,
        )
    except Exception:
        await session.rollback()
        raise
    finally:
        for temporary_key in temporary_keys:
            await _safe_delete_temporary(object_store, temporary_key)


async def _insert_observation(
    session: AsyncSession,
    *,
    raw_artifact: DataArtifact,
    instrument_id: UUID,
    source: str,
    feed: str,
    delivery_mode: str,
    event_kind: str,
    bid: Decimal | None,
    ask: Decimal | None,
    last: Decimal | None,
    observed_at: datetime,
    available_at: datetime,
    received_at: datetime,
    revision: str,
) -> bool:
    canonical = canonical_quote_observation(
        instrument_id=instrument_id,
        source=source,
        feed=feed,
        delivery_mode=delivery_mode,
        event_kind=event_kind,
        bid=bid,
        ask=ask,
        last=last,
        observed_at=observed_at,
        available_at=available_at,
        received_at=received_at,
        revision=revision,
    )
    content_hash = sha256(canonical).hexdigest()
    fixture = source == "fixture"
    values = {
        "id": new_uuid7(),
        "instrument_id": instrument_id,
        "source": source,
        "feed": feed,
        "delivery_mode": delivery_mode,
        "bid": bid,
        "ask": ask,
        "last": last,
        "observed_at": observed_at,
        "available_at": available_at,
        "received_at": received_at,
        "revision": revision,
        "quality_flags": [],
        "event_kind": event_kind,
        "content_hash": content_hash,
        "evidence_status": "fixture" if fixture else "verified",
        "data_artifact_id": None if fixture else raw_artifact.id,
    }
    inserted_id = await session.scalar(
        pg_insert(LatestQuote)
        .values(**values)
        .on_conflict_do_nothing(
            index_elements=[
                LatestQuote.instrument_id,
                LatestQuote.source,
                LatestQuote.feed,
                LatestQuote.event_kind,
                LatestQuote.observed_at,
                LatestQuote.content_hash,
            ]
        )
        .returning(LatestQuote.id)
    )
    if inserted_id is not None:
        return True
    existing = await session.scalar(
        select(LatestQuote).where(
            LatestQuote.instrument_id == instrument_id,
            LatestQuote.source == source,
            LatestQuote.feed == feed,
            LatestQuote.event_kind == event_kind,
            LatestQuote.observed_at == observed_at,
            LatestQuote.content_hash == content_hash,
        )
    )
    if existing is None or any(
        getattr(existing, field) != value for field, value in values.items() if field != "id"
    ):
        raise RuntimeError("INTRADAY_OBSERVATION_REPLAY_CONFLICT")
    return False


async def _persist_bar_authority(
    session: AsyncSession,
    *,
    item: _PublishedBar,
    source: str,
    feed: str,
    delivery_mode: str,
) -> bool:
    await session.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended('intraday-manifest:' || :digest, 0))"),
        {"digest": item.digest},
    )
    artifact, _ = await _get_or_create_artifact(
        session,
        uri=item.uri,
        digest=item.digest,
        byte_size=len(item.payload),
        mime_type="application/vnd.apache.parquet",
        schema_version="intraday_price_bars_v1",
        source=source,
        observed_at=item.bar.observed_at or item.bar.available_at,
        available_at=item.bar.available_at,
        received_at=item.bar.received_at or item.bar.available_at,
    )
    existing = await session.scalar(
        select(IntradayBarSetAuthority)
        .join(
            PriceSeriesManifest,
            PriceSeriesManifest.id == IntradayBarSetAuthority.price_series_manifest_id,
        )
        .where(
            PriceSeriesManifest.data_artifact_id == artifact.id,
            PriceSeriesManifest.instrument == str(item.bar.instrument_id),
            PriceSeriesManifest.source == source,
            PriceSeriesManifest.feed == feed,
            PriceSeriesManifest.delivery_mode == delivery_mode,
            PriceSeriesManifest.interval == "1Min",
        )
    )
    if existing is not None:
        return False

    manifest_id = new_uuid7()
    evidence = ResearchEvidenceRef(
        source=source,
        provider_object_id=f"intraday-price-series-manifest:{manifest_id}",
        revision=artifact.sha256,
        available_at=artifact.available_at,
        location=artifact.uri,
        content_hash=artifact.sha256,
        byte_length=artifact.byte_size,
    )
    session.add(evidence)
    await session.flush()
    observed_at = item.bar.observed_at or item.bar.available_at
    manifest = PriceSeriesManifest(
        id=manifest_id,
        evidence_ref_id=evidence.id,
        data_artifact_id=artifact.id,
        instrument=str(item.bar.instrument_id),
        source=source,
        feed=feed,
        delivery_mode=delivery_mode,
        adjustment="raw",
        interval="1Min",
        start_at=observed_at,
        end_at=observed_at,
        row_count=1,
    )
    session.add(manifest)
    await session.flush()
    session.add(
        PriceBarRecord(
            price_series_manifest_id=manifest.id,
            instrument_id=item.bar.instrument_id,
            session=item.bar.session,
            open=item.bar.open,
            high=item.bar.high,
            low=item.bar.low,
            close=item.bar.close,
            volume=item.bar.volume,
            observed_at=observed_at,
            available_at=item.bar.available_at,
            received_at=item.bar.received_at or item.bar.available_at,
            revision=item.bar.revision,
            quality_flags=list(item.bar.quality_flags),
        )
    )
    await session.flush()
    finalized = await finalize_intraday_bar_set(session, manifest)
    if finalized is None:
        raise RuntimeError("INTRADAY_BAR_AUTHORITY_INVALID")
    return True


async def _get_or_create_artifact(
    session: AsyncSession,
    *,
    uri: str,
    digest: str,
    byte_size: int,
    mime_type: str,
    schema_version: str,
    source: str,
    observed_at: datetime,
    available_at: datetime,
    received_at: datetime,
) -> tuple[DataArtifact, bool]:
    values = {
        "id": new_uuid7(),
        "uri": uri,
        "sha256": digest,
        "byte_size": byte_size,
        "mime_type": mime_type,
        "schema_version": schema_version,
        "source": source,
        "observed_at": observed_at,
        "available_at": available_at,
        "received_at": received_at,
    }
    inserted_id = await session.scalar(
        pg_insert(DataArtifact)
        .values(**values)
        .on_conflict_do_nothing(index_elements=[DataArtifact.sha256])
        .returning(DataArtifact.id)
    )
    artifact = (
        await session.get(DataArtifact, inserted_id)
        if inserted_id is not None
        else await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
    )
    if artifact is None or any(
        getattr(artifact, field) != value for field, value in values.items() if field != "id"
    ):
        raise RuntimeError("INTRADAY_ARTIFACT_REPLAY_CONFLICT")
    return artifact, inserted_id is not None


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


async def _safe_delete_temporary(store: MarketObjectStore, key: str) -> None:
    try:
        await store.delete_temporary(key)
    except Exception:
        return
