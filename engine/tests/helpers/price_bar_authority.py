"""Deterministic PostgreSQL fixtures for price-Bar set authority tests."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

from nexus.domain.ids import new_uuid7
from nexus.persistence.models.data import (
    DataArtifact,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.market import Instrument
from nexus.persistence.models.research import ResearchEvidenceRef
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

INSTRUMENT_ID = UUID("018f75b4-4d86-7c36-bc4c-cd71f2f64520")
MANIFEST_ID = UUID("018f75b4-4d86-7c36-bc4c-cd71f2f64521")
OBSERVED_AT = datetime(2026, 8, 24, 20, tzinfo=UTC)
AVAILABLE_AT = datetime(2026, 8, 24, 20, 15, tzinfo=UTC)
RECEIVED_AT = datetime(2026, 8, 24, 20, 15, 1, tzinfo=UTC)


@dataclass(frozen=True)
class SeededPriceCandidate:
    instrument_id: UUID
    manifest_id: UUID
    bar_ids: tuple[UUID, ...]


async def seed_price_candidate(
    factory: async_sessionmaker[AsyncSession],
    *,
    manifest_id: UUID = MANIFEST_ID,
    source: str = "alpaca",
    feed: str = "sip",
    delivery_mode: str = "delayed",
    interval: str = "1Day",
    adjustment: str = "all",
    quality_flags: tuple[str, ...] = (),
    bar_count: int = 1,
    manifest_overrides: Mapping[str, object] | None = None,
    bar_overrides: Mapping[str, object] | None = None,
) -> SeededPriceCandidate:
    """Insert one manifest whose stored boundaries exactly match its Bars."""
    return await _seed_price_candidate(
        factory,
        manifest_id=manifest_id,
        source=source,
        feed=feed,
        delivery_mode=delivery_mode,
        interval=interval,
        adjustment=adjustment,
        quality_flags=quality_flags,
        bar_count=bar_count,
        manifest_overrides=manifest_overrides,
        bar_overrides=bar_overrides,
        reverse_bar_insertion=False,
    )


async def seed_price_candidate_in_reverse(
    factory: async_sessionmaker[AsyncSession],
    *,
    manifest_id: UUID,
    bar_count: int,
) -> SeededPriceCandidate:
    """Insert an otherwise standard candidate with reverse physical Bar order."""
    return await _seed_price_candidate(
        factory,
        manifest_id=manifest_id,
        source="alpaca",
        feed="sip",
        delivery_mode="delayed",
        interval="1Day",
        adjustment="all",
        quality_flags=(),
        bar_count=bar_count,
        manifest_overrides=None,
        bar_overrides=None,
        reverse_bar_insertion=True,
    )


async def _seed_price_candidate(
    factory: async_sessionmaker[AsyncSession],
    *,
    manifest_id: UUID,
    source: str,
    feed: str,
    delivery_mode: str,
    interval: str,
    adjustment: str,
    quality_flags: tuple[str, ...],
    bar_count: int,
    manifest_overrides: Mapping[str, object] | None,
    bar_overrides: Mapping[str, object] | None,
    reverse_bar_insertion: bool,
) -> SeededPriceCandidate:
    manifest_overrides = dict(manifest_overrides or {})
    bar_overrides = dict(bar_overrides or {})
    artifact_digest = sha256(str(manifest_id).encode()).hexdigest()
    artifact_id = new_uuid7()
    parquet_evidence_id = new_uuid7()
    bar_ids = tuple(new_uuid7() for _ in range(bar_count))
    rows: list[PriceBarRecord] = []
    for index, bar_id in enumerate(bar_ids):
        days_back = bar_count - index - 1
        observed_at = OBSERVED_AT - timedelta(days=days_back)
        values: dict[str, object] = {
            "id": bar_id,
            "price_series_manifest_id": manifest_id,
            "instrument_id": INSTRUMENT_ID,
            "session": observed_at.date(),
            "open": Decimal("101.000000000000"),
            "high": Decimal("103.000000000000"),
            "low": Decimal("100.000000000000"),
            "close": Decimal("102.000000000000"),
            "volume": Decimal("1200000.000000000000"),
            "observed_at": observed_at,
            "available_at": AVAILABLE_AT - timedelta(days=days_back),
            "received_at": RECEIVED_AT - timedelta(days=days_back),
            "revision": "1",
            "quality_flags": list(quality_flags),
        }
        if index == bar_count - 1:
            values.update(bar_overrides)
        rows.append(PriceBarRecord(**values))
    observed_values = [row.observed_at for row in rows]
    async with factory() as session, session.begin():
        if await session.get(Instrument, INSTRUMENT_ID) is None:
            session.add(
                Instrument(
                    id=INSTRUMENT_ID,
                    asset_type="equity",
                    exchange="NASDAQ",
                    currency="USD",
                    tick_size=Decimal("0.01"),
                    risk_bucket="equity",
                )
            )
        session.add(
            DataArtifact(
                id=artifact_id,
                uri=f"memory://sha256/{artifact_digest}",
                sha256=artifact_digest,
                byte_size=471,
                mime_type="application/vnd.apache.parquet",
                schema_version="price_bars_v1",
                source=source,
                observed_at=min(observed_values),
                available_at=max(row.available_at for row in rows),
                received_at=max(row.received_at for row in rows),
            )
        )
        session.add(
            ResearchEvidenceRef(
                id=parquet_evidence_id,
                source=source,
                provider_object_id=f"price-series-manifest:{manifest_id}",
                revision=artifact_digest,
                available_at=max(row.available_at for row in rows),
                location=f"memory://sha256/{artifact_digest}",
                content_hash=artifact_digest,
                byte_length=471,
            )
        )
        await session.flush()
        manifest_values: dict[str, object] = {
            "id": manifest_id,
            "evidence_ref_id": parquet_evidence_id,
            "data_artifact_id": artifact_id,
            "instrument": str(INSTRUMENT_ID),
            "source": source,
            "feed": feed,
            "delivery_mode": delivery_mode,
            "adjustment": adjustment,
            "interval": interval,
            "start_at": min(observed_values),
            "end_at": max(observed_values),
            "row_count": bar_count,
        }
        manifest_values.update(manifest_overrides)
        session.add(PriceSeriesManifest(**manifest_values))
        await session.flush()
        session.add_all(list(reversed(rows)) if reverse_bar_insertion else rows)
    return SeededPriceCandidate(INSTRUMENT_ID, manifest_id, bar_ids)


async def insert_valid_authority_from_prepare(
    factory: async_sessionmaker[AsyncSession], manifest_id: UUID
) -> PriceBarSetAuthority:
    async with factory() as session, session.begin():
        manifest = await session.get(PriceSeriesManifest, manifest_id)
        assert manifest is not None
        prepared = (
            (
                await session.execute(
                    text("SELECT * FROM nexus_prepare_price_bar_set(:manifest_id)"),
                    {"manifest_id": manifest_id},
                )
            )
            .mappings()
            .one()
        )
        digest = str(prepared["content_hash"])
        evidence = ResearchEvidenceRef(
            source=manifest.source,
            provider_object_id=f"price-bar-set:{manifest_id}",
            revision=digest,
            available_at=prepared["available_at"],
            location=f"db://price-bar-set/{manifest_id}@sha256:{digest}",
            content_hash=digest,
            byte_length=int(prepared["byte_length"]),
        )
        session.add(evidence)
        await session.flush()
        authority = PriceBarSetAuthority(
            price_series_manifest_id=manifest_id,
            evidence_ref_id=evidence.id,
            source_policy_id=UUID("3c60888d-e92d-5e04-913c-7d427004cfef"),
            source=manifest.source,
            provider_object_id=evidence.provider_object_id,
            revision=digest,
            hash_version="price_bar_set_sha256_v1",
            policy_version="agent_price_authority_v1",
            content_hash=digest,
            byte_length=evidence.byte_length,
            bar_count=int(prepared["bar_count"]),
            first_session=prepared["first_session"],
            last_session=prepared["last_session"],
            available_at=prepared["available_at"],
        )
        session.add(authority)
        await session.flush()
    return authority


def clean_bar(*, manifest_id: UUID, session: date) -> PriceBarRecord:
    observed_at = datetime.combine(session, time(20), tzinfo=UTC)
    return PriceBarRecord(
        price_series_manifest_id=manifest_id,
        instrument_id=INSTRUMENT_ID,
        session=session,
        open=Decimal("101.000000000000"),
        high=Decimal("103.000000000000"),
        low=Decimal("100.000000000000"),
        close=Decimal("102.000000000000"),
        volume=Decimal("1200000.000000000000"),
        observed_at=observed_at,
        available_at=observed_at + timedelta(minutes=15),
        received_at=observed_at + timedelta(minutes=15, seconds=1),
        revision="1",
        quality_flags=[],
    )
