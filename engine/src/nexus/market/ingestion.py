"""Raw-first atomic market page ingestion."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from io import BytesIO
from typing import Protocol
from urllib.parse import unquote, urlsplit
from uuid import UUID

import pyarrow as pa  # type: ignore[import-untyped]
import pyarrow.parquet as pq  # type: ignore[import-untyped]
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.domain.ids import new_uuid7
from nexus.jobs.market_authority import ProviderMarketBinding
from nexus.market.object_store import ProviderObjectIOUnavailable
from nexus.market.price_bar_authority import finalize_price_bar_set
from nexus.persistence.models.data import (
    DataArtifact,
    PriceBarRecord,
    PriceBarSetAuthority,
    PriceSeriesManifest,
)
from nexus.persistence.models.research import ResearchEvidenceRef
from nexus.providers.base import MarketDataProvenance, PriceBar, ProviderPage
from nexus.providers.market.fosun_gateway import RawGatewayResponse


class MarketObjectStore(Protocol):
    """Small S3-compatible boundary faked by all Task 7 tests."""

    async def archive_raw(self, key: str, payload: bytes) -> str: ...

    async def read_raw(self, key: str, *, uri: str) -> bytes: ...

    async def put_normalized(self, key: str, payload: bytes) -> str: ...


@dataclass(frozen=True)
class MarketIngestionResult:
    """Committed database metadata plus the immutable raw evidence URI."""

    raw_uri: str
    artifact: DataArtifact
    manifest: PriceSeriesManifest
    evidence_ref: ResearchEvidenceRef
    provenance: MarketDataProvenance
    price_authority: PriceBarSetAuthority | None
    price_evidence_ref: ResearchEvidenceRef | None
    raw_artifact: DataArtifact | None = None


@dataclass(frozen=True)
class MarketRawArchive:
    raw_uri: str
    raw_artifact: DataArtifact


@dataclass(frozen=True)
class PreparedPricePage:
    """Immutable object-store result ready for transaction-only persistence."""

    page: ProviderPage[PriceBar]
    bars: tuple[PriceBar, ...]
    raw_uri: str
    raw_digest: str
    parquet_payload: bytes
    parquet_digest: str
    parquet_key: str
    parquet_uri: str


async def archive_fosun_price_page_raw(
    session: AsyncSession,
    object_store: MarketObjectStore,
    response: RawGatewayResponse,
    *,
    commit: bool = True,
) -> MarketRawArchive:
    """Archive exact Fosun gateway bytes before any bar interpretation."""
    if response.operation != "fosun_get_bars":
        raise ValueError("FOSUN_BAR_RESPONSE_REQUIRED")
    digest = sha256(response.raw_payload).hexdigest()
    date_path = response.received_at.astimezone(UTC).strftime("%Y/%m/%d")
    key = f"raw/market/fosun/{date_path}/{response.gateway_request_id}/{digest}.json"
    try:
        uri = await object_store.archive_raw(key, response.raw_payload)
        if not object_uri_matches_key(uri, key):
            raise RuntimeError("FOSUN_MARKET_RAW_OBJECT_URI_MISMATCH")
        values = {
            "id": new_uuid7(),
            "uri": uri,
            "sha256": digest,
            "byte_size": len(response.raw_payload),
            "mime_type": "application/json",
            "schema_version": "fosun_gateway_market_bars_v1",
            "source": "fosun",
            "observed_at": response.received_at,
            "available_at": response.received_at,
            "received_at": response.received_at,
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
        identity = (
            "uri",
            "sha256",
            "byte_size",
            "mime_type",
            "schema_version",
            "source",
            "observed_at",
            "available_at",
            "received_at",
        )
        if artifact is None or any(getattr(artifact, field) != values[field] for field in identity):
            raise RuntimeError("FOSUN_MARKET_RAW_REPLAY_CONFLICT")
        if commit:
            await session.commit()
        return MarketRawArchive(raw_uri=artifact.uri, raw_artifact=artifact)
    except ProviderObjectIOUnavailable:
        # The object write is the first side effect in this function. Preserve
        # its exact stop code; rollback cannot help and may mask the failure.
        raise
    except Exception:
        await session.rollback()
        raise


async def ingest_price_page(
    session: AsyncSession,
    object_store: MarketObjectStore,
    page: ProviderPage[PriceBar],
    *,
    provider_binding: ProviderMarketBinding | None = None,
    raw_artifact: DataArtifact | None = None,
    calendar_authority_id: UUID | None = None,
    gateway_request_start_at: datetime | None = None,
    gateway_request_end_at: datetime | None = None,
    source_policy_id: UUID | None = None,
    commit: bool = True,
) -> MarketIngestionResult:
    """Prepare immutable objects, then atomically persist their metadata."""
    try:
        prepared = await prepare_price_page(
            object_store,
            page,
            provider_binding=provider_binding,
            raw_artifact=raw_artifact,
            calendar_authority_id=calendar_authority_id,
            gateway_request_start_at=gateway_request_start_at,
            gateway_request_end_at=gateway_request_end_at,
        )
        result = await persist_prepared_price_page(
            session,
            prepared,
            provider_binding=provider_binding,
            raw_artifact=raw_artifact,
            calendar_authority_id=calendar_authority_id,
            gateway_request_start_at=gateway_request_start_at,
            gateway_request_end_at=gateway_request_end_at,
            source_policy_id=source_policy_id,
        )
        if commit:
            await session.commit()
        return result
    except ProviderObjectIOUnavailable:
        # prepare_price_page performs every object write before DB persistence.
        # No transaction cleanup is needed when that phase fails.
        raise
    except Exception:
        await session.rollback()
        raise


async def prepare_price_page(
    object_store: MarketObjectStore,
    page: ProviderPage[PriceBar],
    *,
    provider_binding: ProviderMarketBinding | None = None,
    raw_artifact: DataArtifact | None = None,
    calendar_authority_id: UUID | None = None,
    gateway_request_start_at: datetime | None = None,
    gateway_request_end_at: datetime | None = None,
) -> PreparedPricePage:
    """Create immutable raw/normalized objects without touching the database."""
    bars, instrument_id, received_at = _validated_price_page(
        page,
        provider_binding=provider_binding,
        raw_artifact=raw_artifact,
        calendar_authority_id=calendar_authority_id,
        gateway_request_start_at=gateway_request_start_at,
        gateway_request_end_at=gateway_request_end_at,
    )
    raw_digest = sha256(page.raw_payload).hexdigest()
    date_path = received_at.astimezone(UTC).strftime("%Y/%m/%d")
    request_id = page.source_request_id or raw_digest[:16]
    raw_key = f"raw/market/{date_path}/{request_id}/{raw_digest}.json"
    if raw_artifact is None:
        raw_uri = await object_store.archive_raw(raw_key, page.raw_payload)
        if not object_uri_matches_key(raw_uri, raw_key):
            raise RuntimeError("MARKET_RAW_OBJECT_URI_MISMATCH")
    else:
        if (
            raw_artifact.sha256 != raw_digest
            or raw_artifact.byte_size != len(page.raw_payload)
            or raw_artifact.source != page.provenance.source
            or raw_artifact.schema_version != "fosun_gateway_market_bars_v1"
        ):
            raise RuntimeError("FOSUN_MARKET_RAW_ARCHIVE_MISMATCH")
        raw_uri = raw_artifact.uri
    parquet_payload = serialize_price_bars(bars)
    parquet_digest = sha256(parquet_payload).hexdigest()
    parquet_key = f"normalized/market/{date_path}/{instrument_id}/{parquet_digest}.parquet"
    parquet_uri = await object_store.put_normalized(parquet_key, parquet_payload)
    if not object_uri_matches_key(parquet_uri, parquet_key):
        raise RuntimeError("MARKET_NORMALIZED_OBJECT_URI_MISMATCH")
    return PreparedPricePage(
        page=page,
        bars=bars,
        raw_uri=raw_uri,
        raw_digest=raw_digest,
        parquet_payload=parquet_payload,
        parquet_digest=parquet_digest,
        parquet_key=parquet_key,
        parquet_uri=parquet_uri,
    )


async def persist_prepared_price_page(
    session: AsyncSession,
    prepared: PreparedPricePage,
    *,
    provider_binding: ProviderMarketBinding | None = None,
    raw_artifact: DataArtifact | None = None,
    calendar_authority_id: UUID | None = None,
    gateway_request_start_at: datetime | None = None,
    gateway_request_end_at: datetime | None = None,
    source_policy_id: UUID | None = None,
) -> MarketIngestionResult:
    """Persist one fully prepared page inside the caller's transaction."""
    page = prepared.page
    bars, instrument_id, received_at = _validated_price_page(
        page,
        provider_binding=provider_binding,
        raw_artifact=raw_artifact,
        calendar_authority_id=calendar_authority_id,
        gateway_request_start_at=gateway_request_start_at,
        gateway_request_end_at=gateway_request_end_at,
    )
    raw_digest = sha256(page.raw_payload).hexdigest()
    parquet_payload = serialize_price_bars(bars)
    parquet_digest = sha256(parquet_payload).hexdigest()
    date_path = received_at.astimezone(UTC).strftime("%Y/%m/%d")
    parquet_key = f"normalized/market/{date_path}/{instrument_id}/{parquet_digest}.parquet"
    if (
        prepared.bars != bars
        or prepared.raw_digest != raw_digest
        or prepared.parquet_payload != parquet_payload
        or prepared.parquet_digest != parquet_digest
        or prepared.parquet_key != parquet_key
        or not object_uri_matches_key(prepared.parquet_uri, parquet_key)
        or (raw_artifact is not None and prepared.raw_uri != raw_artifact.uri)
    ):
        raise RuntimeError("MARKET_PREPARED_PAGE_MISMATCH")

    observed = [bar.observed_at or bar.available_at for bar in bars]
    artifact = DataArtifact(
        uri=prepared.parquet_uri,
        sha256=parquet_digest,
        byte_size=len(parquet_payload),
        mime_type="application/vnd.apache.parquet",
        schema_version="price_bars_v1",
        source=page.provenance.source,
        observed_at=min(observed),
        available_at=max(bar.available_at for bar in bars),
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
        gateway_transport_artifact_id=(None if raw_artifact is None else raw_artifact.id),
        daily_calendar_authority_id=calendar_authority_id,
        gateway_request_start_at=gateway_request_start_at,
        gateway_request_end_at=gateway_request_end_at,
        instrument=str(instrument_id),
        source=page.provenance.source,
        feed=page.provenance.feed,
        delivery_mode=page.provenance.delivery_mode,
        adjustment=page.provenance.adjustment,
        interval=page.provenance.interval,
        start_at=min(observed),
        end_at=max(observed),
        row_count=len(bars),
        **_binding_values(provider_binding),
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
        for bar in bars
    )
    await session.flush()
    finalized = await finalize_price_bar_set(session, manifest, source_policy_id=source_policy_id)
    return MarketIngestionResult(
        raw_uri=prepared.raw_uri,
        artifact=artifact,
        manifest=manifest,
        evidence_ref=evidence_ref,
        provenance=page.provenance,
        price_authority=None if finalized is None else finalized[0],
        price_evidence_ref=None if finalized is None else finalized[1],
        raw_artifact=raw_artifact,
    )


def _validated_price_page(
    page: ProviderPage[PriceBar],
    *,
    provider_binding: ProviderMarketBinding | None,
    raw_artifact: DataArtifact | None,
    calendar_authority_id: UUID | None,
    gateway_request_start_at: datetime | None,
    gateway_request_end_at: datetime | None,
) -> tuple[tuple[PriceBar, ...], UUID, datetime]:
    if not page.items:
        raise ValueError("market page must contain at least one bar")
    if not page.raw_payload:
        raise ValueError("market page must retain its raw payload")
    instrument_ids = {bar.instrument_id for bar in page.items}
    if len(instrument_ids) != 1:
        raise ValueError("one market page cannot mix instruments")
    _validate_provider_binding(page.provenance.source, provider_binding)
    if page.provenance.source == "fosun":
        if raw_artifact is None or (
            page.provenance.interval == "1Day"
            and (
                calendar_authority_id is None
                or gateway_request_start_at is None
                or gateway_request_end_at is None
            )
        ):
            raise ValueError("FOSUN_MARKET_LINEAGE_REQUIRED")
    elif any(
        value is not None
        for value in (
            raw_artifact,
            calendar_authority_id,
            gateway_request_start_at,
            gateway_request_end_at,
        )
    ):
        raise ValueError("FOSUN_MARKET_LINEAGE_FORBIDDEN")

    raw_digest = sha256(page.raw_payload).hexdigest()
    bars = (
        tuple(bar.model_copy(update={"revision": raw_digest}) for bar in page.items)
        if page.provenance.source == "fosun"
        else page.items
    )
    received_at = max(bar.received_at or bar.available_at for bar in bars)
    return bars, next(iter(instrument_ids)), received_at


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


def _validate_provider_binding(
    source: str,
    binding: ProviderMarketBinding | None,
) -> None:
    if (source == "fosun") != (binding is not None):
        raise ValueError("FOSUN_MARKET_BINDING_REQUIRED")


def object_uri_matches_key(uri: str, key: str) -> bool:
    """Bind a persisted locator to one exact content-addressed object key."""
    if (
        not isinstance(uri, str)
        or not isinstance(key, str)
        or not uri
        or not key
        or not uri.isascii()
        or not key.isascii()
        or any(ord(char) < 0x20 or ord(char) == 0x7F for char in uri + key)
        or "#" in uri
        or any(char in key for char in "%?#")
    ):
        return False
    try:
        parsed = urlsplit(uri)
        username = parsed.username
        password = parsed.password
        hostname = parsed.hostname
    except ValueError:
        return False
    if username is not None or password is not None:
        return False
    if parsed.scheme == "memory":
        return bool(parsed.netloc) and not parsed.query and f"{parsed.netloc}{parsed.path}" == key
    if parsed.scheme != "s3" or not parsed.netloc or not hostname:
        return False
    if parsed.netloc != hostname or "%" in parsed.path:
        return False
    path_segments = parsed.path.split("/")[1:]
    if (
        not parsed.path.startswith("/")
        or not parsed.path.endswith(f"/{key}")
        or any(segment in {"", ".", ".."} for segment in path_segments)
    ):
        return False
    prefix = "versionId="
    if (
        not parsed.query.startswith(prefix)
        or parsed.query.count("=") != 1
        or any(separator in parsed.query for separator in ("&", ";"))
    ):
        return False
    encoded_version = parsed.query.removeprefix(prefix)
    if not encoded_version or not _has_valid_percent_escapes(encoded_version):
        return False
    try:
        version = unquote(encoded_version, errors="strict")
    except UnicodeError:
        return False
    return (
        bool(version)
        and version != "null"
        and not any(ord(char) < 0x20 or ord(char) == 0x7F for char in version)
    )


def _has_valid_percent_escapes(value: str) -> bool:
    index = 0
    hexdigits = frozenset("0123456789abcdefABCDEF")
    while index < len(value):
        if value[index] != "%":
            index += 1
            continue
        if index + 2 >= len(value) or any(
            char not in hexdigits for char in value[index + 1 : index + 3]
        ):
            return False
        index += 3
    return True


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
