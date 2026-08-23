"""Verified transport-to-domain parsing for immutable diagnostic market artifacts."""

from hashlib import sha256
from io import BytesIO
from typing import Literal, Protocol, cast
from uuid import UUID

import pyarrow.parquet as pq  # type: ignore[import-untyped]

from nexus.domain.evidence import EvidenceRef
from nexus.persistence.models.data import DataArtifact, PriceSeriesManifest
from nexus.providers.base import DeliveryMode, PriceBar, PriceSeries


class ArtifactByteReader(Protocol):
    """Transport-only capability; database metadata remains the authority."""

    async def __call__(self, uri: str) -> bytes: ...


def parse_price_series_artifact(
    payload: bytes,
    artifact: DataArtifact,
    manifest: PriceSeriesManifest,
    expected_instrument_id: UUID,
) -> tuple[PriceSeries, EvidenceRef]:
    """Verify persisted provenance before parsing normalized price-bars v1 Parquet."""
    digest = sha256(payload).hexdigest()
    if digest != artifact.sha256 or len(payload) != artifact.byte_size:
        raise ValueError("diagnostic artifact hash or byte size is invalid")
    if (
        artifact.mime_type != "application/vnd.apache.parquet"
        or artifact.schema_version != "price_bars_v1"
        or manifest.data_artifact_id != artifact.id
        or manifest.source != artifact.source
        or manifest.instrument != str(expected_instrument_id)
    ):
        raise ValueError("diagnostic artifact provenance is invalid")
    rows = pq.read_table(BytesIO(payload)).to_pylist()
    if len(rows) != manifest.row_count or not rows:
        raise ValueError("diagnostic artifact row count is invalid")
    bars = tuple(_price_bar(row, expected_instrument_id) for row in rows)
    observed = tuple(bar.observed_at for bar in bars)
    if (
        any(value is None for value in observed)
        or manifest.start_at != min(value for value in observed if value is not None)
        or manifest.end_at != max(value for value in observed if value is not None)
    ):
        raise ValueError("diagnostic artifact manifest range is invalid")
    mode = _provider_mode(manifest.source, manifest.delivery_mode)
    return (
        PriceSeries(
            source=manifest.source,
            feed=manifest.feed,
            adjustment=manifest.adjustment,
            bars=bars,
            interval="1Day",
            mode=mode,
            delivery_mode=cast(DeliveryMode, manifest.delivery_mode),
            quality="PASS",
        ),
        EvidenceRef(
            source=artifact.source,
            object_id=f"price-series-manifest:{manifest.id}",
            available_at=artifact.available_at,
            location=f"db://data-artifacts/{artifact.id}",
            content_hash=artifact.sha256,
        ),
    )


def _price_bar(row: dict[str, object], expected_instrument_id: UUID) -> PriceBar:
    if row.get("instrument_id") != str(expected_instrument_id):
        raise ValueError("diagnostic artifact instrument is invalid")
    values = dict(row)
    values["instrument_id"] = expected_instrument_id
    values["quality_flags"] = tuple(cast(list[str], row["quality_flags"]))
    return PriceBar.model_validate(values)


def _provider_mode(
    source: str, delivery_mode: str
) -> Literal["fixture", "read_only", "delayed"]:
    if source == "fixture" and delivery_mode == "fixture":
        return "fixture"
    if source == "alpaca" and delivery_mode == "delayed":
        return "delayed"
    if source == "fosun" and delivery_mode in {"real_time", "delayed", "unknown"}:
        return "read_only"
    raise ValueError("diagnostic artifact delivery authority is invalid")
