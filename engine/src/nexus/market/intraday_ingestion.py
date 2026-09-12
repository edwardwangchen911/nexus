"""Raw-first, replay-safe intraday Snapshot ingestion."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import canonical_json
from nexus.domain.ids import new_uuid7
from nexus.jobs.market_authority import ProviderMarketBinding
from nexus.market.artifact_authority import (
    data_artifact_identity_sha256,
    lock_and_load_data_artifacts,
)
from nexus.market.ingestion import (
    MarketObjectStore,
    object_uri_matches_key,
    serialize_price_bars,
)
from nexus.market.intraday_authority import finalize_intraday_bar_set
from nexus.market.object_store import ProviderObjectIOUnavailable
from nexus.persistence.models.data import (
    DataArtifact,
    IntradayBarSetAuthority,
    PriceBarRecord,
    PriceSeriesManifest,
)
from nexus.persistence.models.market import LatestQuote
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.providers.base import PriceBar
from nexus.providers.market.snapshot_contracts import RawSnapshotEnvelope, SnapshotBatch


@dataclass(frozen=True)
class IntradayIngestionResult:
    raw_uri: str
    raw_artifact: DataArtifact
    created_trade_observations: int
    created_quote_observations: int
    created_bar_authorities: int
    replayed: int


@dataclass(frozen=True)
class IntradayRawArchive:
    raw_uri: str
    raw_artifact: DataArtifact


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
    quality_flags: tuple[str, ...] = (),
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
            "quality_flags": sorted(quality_flags),
        }
    ).encode("utf-8")


async def ingest_intraday_snapshot(
    session: AsyncSession,
    object_store: MarketObjectStore,
    batch: SnapshotBatch,
    *,
    raw_artifact: DataArtifact | None = None,
    quote_binding: ProviderMarketBinding | None = None,
    bar_binding: ProviderMarketBinding | None = None,
    bar_bindings: Mapping[UUID, ProviderMarketBinding] | None = None,
    commit: bool = True,
) -> IntradayIngestionResult:
    """Archive raw bytes, publish normalized objects, then expose DB authorities."""
    if not batch.raw_payload:
        raise ValueError("intraday batch must retain its raw payload")
    _validate_bindings(
        batch,
        quote_binding=quote_binding,
        bar_binding=bar_binding,
        bar_bindings=bar_bindings,
    )
    raw_digest = sha256(batch.raw_payload).hexdigest()
    date_path = batch.received_at.astimezone(UTC).strftime("%Y/%m/%d")
    try:
        frozen_raw_identity = (
            None if raw_artifact is None else data_artifact_identity_sha256(raw_artifact)
        )
        if raw_artifact is None:
            archived = await archive_intraday_snapshot_raw(
                session,
                object_store,
                RawSnapshotEnvelope(
                    raw_payload=batch.raw_payload,
                    received_at=batch.received_at,
                    provenance=batch.provenance,
                ),
                commit=False,
            )
            raw_artifact = archived.raw_artifact
            frozen_raw_identity = data_artifact_identity_sha256(raw_artifact)
        elif (
            raw_artifact.sha256 != raw_digest
            or raw_artifact.byte_size != len(batch.raw_payload)
            or raw_artifact.source != batch.provenance.source
        ):
            raise RuntimeError("INTRADAY_RAW_ARCHIVE_MISMATCH")
        locked = await lock_and_load_data_artifacts(session, (raw_artifact.id,))
        current_raw_artifact = locked.get(raw_artifact.id)
        expected_raw_key = _intraday_raw_key(raw_digest)
        expected_raw_schema = (
            "fosun_gateway_market_snapshot_v1"
            if batch.provenance.source == "fosun"
            else "schwab_level_one_stream_v1"
            if batch.provenance.source == "schwab"
            else "alpaca_stock_snapshots_v1"
        )
        if (
            current_raw_artifact is None
            or frozen_raw_identity is None
            or data_artifact_identity_sha256(current_raw_artifact) != frozen_raw_identity
            or current_raw_artifact.sha256 != raw_digest
            or current_raw_artifact.byte_size != len(batch.raw_payload)
            or current_raw_artifact.mime_type != "application/json"
            or current_raw_artifact.schema_version != expected_raw_schema
            or current_raw_artifact.source != batch.provenance.source
            or not (
                current_raw_artifact.observed_at
                <= current_raw_artifact.available_at
                <= current_raw_artifact.received_at
                <= batch.received_at
            )
            or not object_uri_matches_key(current_raw_artifact.uri, expected_raw_key)
        ):
            raise RuntimeError("INTRADAY_RAW_ARCHIVE_MISMATCH")
        raw_artifact = current_raw_artifact
        published: list[_PublishedBar] = []
        for snapshot in batch.snapshots:
            if snapshot.minute_bar is None:
                continue
            # The published normalized identity must name the exact transport
            # response for every admitted provider. Otherwise two distinct raw
            # snapshots with the same OHLCV collapse into one unauditable bar.
            bar = snapshot.minute_bar.bar.model_copy(update={"revision": raw_digest})
            payload = serialize_price_bars((bar,))
            digest = sha256(payload).hexdigest()
            final_key = (
                f"normalized/market/intraday/{date_path}/{snapshot.instrument_id}/{digest}.parquet"
            )
            uri = await object_store.put_normalized(final_key, payload)
            if not object_uri_matches_key(uri, final_key):
                raise RuntimeError("INTRADAY_NORMALIZED_OBJECT_URI_MISMATCH")
            published.append(_PublishedBar(bar=bar, payload=payload, digest=digest, uri=uri))

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
                    event_kind=snapshot.trade.event_kind,
                    bid=None,
                    ask=None,
                    last=snapshot.trade.price,
                    observed_at=snapshot.trade.observed_at,
                    available_at=batch.received_at,
                    received_at=batch.received_at,
                    revision=revision,
                    quality_flags=snapshot.trade.quality_flags,
                    provider_binding=quote_binding,
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
                    quality_flags=(),
                    provider_binding=quote_binding,
                )
                created_quote += int(created)
                replayed += int(not created)

        for item in published:
            current_bar_binding = (
                None if bar_bindings is None else bar_bindings.get(item.bar.instrument_id)
            )
            created = await _persist_bar_authority(
                session,
                item=item,
                source=batch.provenance.source,
                feed=batch.provenance.feed,
                delivery_mode=batch.provenance.delivery_mode,
                provider_binding=current_bar_binding or bar_binding,
                raw_artifact=raw_artifact,
            )
            created_bars += int(created)
            replayed += int(not created)

        if commit:
            await session.commit()
        return IntradayIngestionResult(
            raw_uri=raw_artifact.uri,
            raw_artifact=raw_artifact,
            created_trade_observations=created_trade,
            created_quote_observations=created_quote,
            created_bar_authorities=created_bars,
            replayed=replayed,
        )
    except ProviderObjectIOUnavailable:
        # The worker passes commit=False and owns rollback on unwind, preserving
        # its one-shot deadline. Standalone commit=True callers retain the
        # function's rollback-before-error contract.
        if commit:
            await session.rollback()
        raise
    except Exception:
        await session.rollback()
        raise


async def archive_intraday_snapshot_raw(
    session: AsyncSession,
    object_store: MarketObjectStore,
    envelope: RawSnapshotEnvelope,
    *,
    commit: bool = True,
) -> IntradayRawArchive:
    """Persist exact response bytes and database provenance before parsing them."""
    raw_digest = sha256(envelope.raw_payload).hexdigest()
    raw_key = _intraday_raw_key(raw_digest)
    try:
        raw_uri = await object_store.archive_raw(raw_key, envelope.raw_payload)
        if not object_uri_matches_key(raw_uri, raw_key):
            raise RuntimeError("INTRADAY_RAW_OBJECT_URI_MISMATCH")
        raw_artifact, _ = await _get_or_create_artifact(
            session,
            uri=raw_uri,
            digest=raw_digest,
            byte_size=len(envelope.raw_payload),
            mime_type="application/json",
            schema_version=(
                "fosun_gateway_market_snapshot_v1"
                if envelope.provenance.source == "fosun"
                else "schwab_level_one_stream_v1"
                if envelope.provenance.source == "schwab"
                else "alpaca_stock_snapshots_v1"
            ),
            source=envelope.provenance.source,
            observed_at=envelope.received_at,
            available_at=envelope.received_at,
            received_at=envelope.received_at,
        )
        if commit:
            await session.commit()
        return IntradayRawArchive(raw_uri=raw_artifact.uri, raw_artifact=raw_artifact)
    except ProviderObjectIOUnavailable:
        # The raw object write precedes all database access. Do not let an
        # unrelated rollback failure replace the deterministic object stop code.
        raise
    except Exception:
        await session.rollback()
        raise


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
    quality_flags: tuple[str, ...],
    provider_binding: ProviderMarketBinding | None,
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
        quality_flags=quality_flags,
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
        "quality_flags": list(quality_flags),
        "event_kind": event_kind,
        "content_hash": content_hash,
        "evidence_status": "fixture" if fixture else "verified",
        "data_artifact_id": None if fixture else raw_artifact.id,
        **_binding_values(provider_binding),
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
    provider_binding: ProviderMarketBinding | None,
    raw_artifact: DataArtifact,
) -> bool:
    if item.bar.revision != raw_artifact.sha256 or raw_artifact.source != source:
        raise RuntimeError("INTRADAY_BAR_RAW_REVISION_MISMATCH")
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
        gateway_transport_artifact_id=(raw_artifact.id if source == "fosun" else None),
        daily_calendar_authority_id=None,
        gateway_request_start_at=None,
        gateway_request_end_at=None,
        instrument=str(item.bar.instrument_id),
        source=source,
        feed=feed,
        delivery_mode=delivery_mode,
        adjustment="raw",
        interval="1Min",
        start_at=observed_at,
        end_at=observed_at,
        row_count=1,
        **_binding_values(provider_binding),
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
    content_identity = (
        "uri",
        "sha256",
        "byte_size",
        "mime_type",
        "schema_version",
        "source",
    )
    if artifact is None or any(
        getattr(artifact, field) != values[field] for field in content_identity
    ):
        raise RuntimeError("INTRADAY_ARTIFACT_REPLAY_CONFLICT")
    return artifact, inserted_id is not None


def _intraday_raw_key(digest: str) -> str:
    return f"raw/market/intraday/sha256/{digest[:2]}/{digest}.json"


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _validate_bindings(
    batch: SnapshotBatch,
    *,
    quote_binding: ProviderMarketBinding | None,
    bar_binding: ProviderMarketBinding | None,
    bar_bindings: Mapping[UUID, ProviderMarketBinding] | None,
) -> None:
    if bar_binding is not None and bar_bindings is not None:
        raise ValueError("FOSUN_BAR_BINDING_AMBIGUOUS")
    if batch.provenance.source == "fosun":
        if quote_binding is None:
            raise ValueError("FOSUN_QUOTE_BINDING_REQUIRED")
        bar_instruments = {
            snapshot.instrument_id
            for snapshot in batch.snapshots
            if snapshot.minute_bar is not None
        }
        if bar_instruments:
            if bar_binding is None and (
                bar_bindings is None or set(bar_bindings) != bar_instruments
            ):
                raise ValueError("FOSUN_BAR_BINDING_REQUIRED")
        elif bar_bindings:
            raise ValueError("FOSUN_BAR_BINDING_FORBIDDEN")
        return
    if quote_binding is not None or bar_binding is not None or bar_bindings is not None:
        raise ValueError("FOSUN_MARKET_BINDING_FORBIDDEN")


def _binding_values(binding: ProviderMarketBinding | None) -> dict[str, object | None]:
    if binding is None:
        return {
            "provider_capability_id": None,
            "provider_status_id": None,
            "provider_status_version": None,
            "provider_contract_hash": None,
            "provider_authorized_at": None,
        }
    return binding.model_dump()
