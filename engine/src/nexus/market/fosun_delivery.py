"""Reclassify stored daily bars from their actual response, preserving old facts."""

from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.diagnostics.artifacts import ArtifactByteReader
from nexus.domain.ids import new_uuid7
from nexus.jobs.market_authority import (
    acquire_fosun_market_authority_locks,
    load_frozen_fosun_market_authority,
)
from nexus.market.ingestion import serialize_price_bars
from nexus.market.price_bar_authority import finalize_price_bar_set
from nexus.persistence.models.data import (
    DataArtifact,
    DataSnapshot,
    PriceBarRecord,
    PriceSeriesManifest,
    ProviderCapability,
)
from nexus.persistence.models.market import InstrumentAlias
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.providers.base import InstrumentId, PriceBar
from nexus.providers.market.fosun_gateway import FosunBarPage, _load_exact_json

_SCHEMA = "fosun_daily_delivery_v1"


async def reclassify_daily_delivery(
    session: AsyncSession,
    *,
    manifest_id: UUID,
    raw_reader: ArtifactByteReader,
    normalized_reader: ArtifactByteReader,
) -> UUID:
    """Read local objects only; caller owns the transaction. No replayed Provider call."""
    parent = await session.get(PriceSeriesManifest, manifest_id)
    if parent is None or (
        parent.source,
        parent.feed,
        parent.interval,
        parent.adjustment,
        parent.delivery_mode,
    ) != ("fosun", "l1", "1Day", "raw", "unknown"):
        raise ValueError("FOSUN_UNKNOWN_DAILY_PARENT_REQUIRED")
    current = await load_frozen_fosun_market_authority(
        session, check_time=datetime.now(UTC), operation="fosun_get_bars"
    )
    if current is None or current.contract_hash != parent.provider_contract_hash:
        raise ValueError("FOSUN_CURRENT_READ_AUTHORITY_REQUIRED")
    await acquire_fosun_market_authority_locks(session, (current,))
    rechecked = await load_frozen_fosun_market_authority(
        session, check_time=datetime.now(UTC), operation="fosun_get_bars"
    )
    if rechecked != current:
        raise ValueError("FOSUN_READ_AUTHORITY_CHANGED")
    receipt_key = f"daily-delivery:{parent.id}"
    prior = await session.scalar(
        select(DataSnapshot).where(
            DataSnapshot.source == "fosun",
            DataSnapshot.provider_object_id == receipt_key,
            DataSnapshot.schema_version == _SCHEMA,
        )
    )
    if prior is not None:
        return UUID(str(prior.payload["manifest_id"]))
    raw = await session.get(DataArtifact, parent.gateway_transport_artifact_id)
    normalized = await session.get(DataArtifact, parent.data_artifact_id)
    if (
        raw is None
        or normalized is None
        or (
            raw.source != "fosun"
            or raw.schema_version != "fosun_gateway_market_bars_v1"
            or normalized.source != "fosun"
            or normalized.schema_version != "price_bars_v1"
        )
    ):
        raise ValueError("FOSUN_STORED_ARTIFACT_REQUIRED")
    raw_bytes = await raw_reader(raw.uri)
    if sha256(raw_bytes).hexdigest() != raw.sha256 or len(raw_bytes) != raw.byte_size:
        raise ValueError("FOSUN_RAW_EVIDENCE_MISMATCH")
    page = FosunBarPage.model_validate(_load_exact_json(raw_bytes))
    if page.provider_reported_delay is None or page.sdk_requested_adjustment != "noward":
        raise ValueError("FOSUN_RESPONSE_DECLARATION_MISSING")
    mode = "delayed" if page.provider_reported_delay else "real_time"
    instrument_id = UUID(parent.instrument)
    symbol = await session.scalar(
        select(InstrumentAlias.symbol)
        .where(
            InstrumentAlias.instrument_id == instrument_id,
            InstrumentAlias.alias_type == "ticker",
            InstrumentAlias.valid_from <= parent.end_at.date(),
            (
                InstrumentAlias.valid_to.is_(None)
                | (InstrumentAlias.valid_to >= parent.end_at.date())
            ),
        )
        .order_by(InstrumentAlias.created_at.desc())
        .limit(1)
    )
    rows = (
        await session.scalars(
            select(PriceBarRecord)
            .where(
                PriceBarRecord.price_series_manifest_id == parent.id,
            )
            .order_by(PriceBarRecord.observed_at)
        )
    ).all()
    if not rows or len(rows) != parent.row_count or len(rows) != len(page.items):
        raise ValueError("FOSUN_BAR_COUNT_MISMATCH")
    bars = tuple(
        PriceBar(
            instrument_id=InstrumentId(row.instrument_id),
            session=row.session,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.close,
            volume=row.volume,
            observed_at=row.observed_at,
            available_at=row.available_at,
            received_at=row.received_at,
            revision=row.revision,
            quality_flags=tuple(row.quality_flags),
        )
        for row in rows
    )
    for bar, item in zip(bars, page.items, strict=True):
        if (
            item.instrument_code != f"us{symbol}"
            or item.interval != "day"
            or bar.instrument_id != instrument_id
            or bar.revision != raw.sha256
            or bar.quality_flags
            or bar.received_at != raw.received_at
            or (bar.observed_at, bar.open, bar.high, bar.low, bar.close, bar.volume)
            != (item.timestamp, item.open, item.high, item.low, item.close, item.volume)
        ):
            raise ValueError("FOSUN_NORMALIZED_RESPONSE_MISMATCH")
    normalized_bytes = await normalized_reader(normalized.uri)
    if (
        sha256(normalized_bytes).hexdigest() != normalized.sha256
        or len(normalized_bytes) != normalized.byte_size
        or serialize_price_bars(bars) != normalized_bytes
    ):
        raise ValueError("FOSUN_NORMALIZED_EVIDENCE_MISMATCH")
    # Append a version with actual creation time, identical permissions and expiry.
    # Neither old capability nor old normalized records are changed.
    capability_id = current.capability_id
    if current.delivery_mode != mode:
        capability = ProviderCapability(
            provider="fosun",
            capability="READ_ONLY",
            contract_hash=current.contract_hash,
            expires_at=current.capability_expires_at,
            schema_version="1",
            payload={"operation": "fosun_get_bars", "market_scope": "US", "delivery_mode": mode},
            created_at=datetime.now(UTC),
        )
        session.add(capability)
        await session.flush()
        capability_id = capability.id
    authorized_at = datetime.now(UTC)
    new_id = new_uuid7()
    evidence = ResearchEvidenceRef(
        source="fosun",
        provider_object_id=f"price-series-manifest:{new_id}",
        revision=normalized.sha256,
        available_at=normalized.available_at,
        location=normalized.uri,
        content_hash=normalized.sha256,
        byte_length=normalized.byte_size,
    )
    session.add(evidence)
    await session.flush()
    manifest = PriceSeriesManifest(
        id=new_id,
        evidence_ref_id=evidence.id,
        data_artifact_id=normalized.id,
        gateway_transport_artifact_id=raw.id,
        daily_calendar_authority_id=parent.daily_calendar_authority_id,
        gateway_request_start_at=parent.gateway_request_start_at,
        gateway_request_end_at=parent.gateway_request_end_at,
        instrument=parent.instrument,
        source="fosun",
        feed="l1",
        delivery_mode=mode,
        adjustment="raw",
        interval="1Day",
        start_at=parent.start_at,
        end_at=parent.end_at,
        row_count=parent.row_count,
        provider_capability_id=capability_id,
        provider_status_id=current.status_id,
        provider_status_version=current.status_version,
        provider_contract_hash=current.contract_hash,
        provider_authorized_at=authorized_at,
    )
    session.add(manifest)
    await session.flush()
    session.add_all(
        PriceBarRecord(
            price_series_manifest_id=new_id,
            instrument_id=row.instrument_id,
            session=row.session,
            open=row.open,
            high=row.high,
            low=row.low,
            close=row.close,
            volume=row.volume,
            observed_at=row.observed_at,
            available_at=row.available_at,
            received_at=row.received_at,
            revision=row.revision,
            quality_flags=row.quality_flags,
        )
        for row in rows
    )
    await session.flush()
    finalized = await finalize_price_bar_set(session, manifest)
    session.add(
        DataSnapshot(
            source="fosun",
            provider_object_id=receipt_key,
            revision=raw.sha256,
            observed_at=raw.observed_at,
            available_at=authorized_at,
            received_at=authorized_at,
            schema_version=_SCHEMA,
            payload={
                "parent_manifest_id": str(parent.id),
                "manifest_id": str(new_id),
                "raw_sha256": raw.sha256,
                "delivery_mode": mode,
                "price_authority_created": finalized is not None,
            },
        )
    )
    await session.flush()
    return new_id
