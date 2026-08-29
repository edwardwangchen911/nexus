from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from hashlib import sha256
from uuid import UUID

import pytest
from nexus.diagnostics.artifacts import parse_price_series_artifact
from nexus.market.ingestion import serialize_price_bars
from nexus.persistence.models.data import DataArtifact, PriceSeriesManifest
from nexus.providers.base import PriceBar

INSTRUMENT_ID = UUID("0198d500-1000-7000-8000-000000000077")


def test_parser_verifies_hash_and_reconstructs_typed_parquet_series() -> None:
    bar = PriceBar(
        instrument_id=INSTRUMENT_ID,
        session=date(2026, 8, 21),
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1000"),
        observed_at=datetime(2026, 8, 21, 20, tzinfo=UTC),
        available_at=datetime(2026, 8, 21, 20, 1, tzinfo=UTC),
        received_at=datetime(2026, 8, 21, 20, 2, tzinfo=UTC),
        revision="r1",
    )
    payload = serialize_price_bars((bar,))
    artifact, manifest = _records(payload)

    series, ref = parse_price_series_artifact(payload, artifact, manifest, INSTRUMENT_ID)

    assert series.bars == (bar,)
    assert ref.content_hash == sha256(payload).hexdigest()
    assert ref.location == f"db://data-artifacts/{artifact.id}"

    with pytest.raises(ValueError, match="artifact hash"):
        parse_price_series_artifact(payload + b"tampered", artifact, manifest, INSTRUMENT_ID)

    manifest.start_at = manifest.start_at - timedelta(days=1)
    with pytest.raises(ValueError, match="manifest range"):
        parse_price_series_artifact(payload, artifact, manifest, INSTRUMENT_ID)


def test_parser_requires_daily_complete_fosun_binding_and_keeps_contract_hash() -> None:
    bar = PriceBar(
        instrument_id=INSTRUMENT_ID,
        session=date(2026, 8, 21),
        open=Decimal("100"),
        high=Decimal("101"),
        low=Decimal("99"),
        close=Decimal("100.5"),
        volume=Decimal("1000"),
        observed_at=datetime(2026, 8, 21, 20, tzinfo=UTC),
        available_at=datetime(2026, 8, 21, 20, 1, tzinfo=UTC),
        received_at=datetime(2026, 8, 21, 20, 2, tzinfo=UTC),
        revision="r1",
    )
    payload = serialize_price_bars((bar,))
    artifact, manifest = _records(payload)
    artifact.source = "fosun"
    manifest.source = "fosun"
    manifest.feed = "l1"
    manifest.delivery_mode = "real_time"
    manifest.adjustment = "raw"
    manifest.provider_capability_id = UUID("0198d500-1000-7000-8000-000000000081")
    manifest.provider_status_id = UUID("0198d500-1000-7000-8000-000000000082")
    manifest.provider_status_version = 1
    manifest.provider_contract_hash = "a" * 64
    manifest.provider_authorized_at = datetime(2026, 8, 21, 20, 3, tzinfo=UTC)

    series, _ = parse_price_series_artifact(payload, artifact, manifest, INSTRUMENT_ID)

    assert series.contract_hash == "a" * 64
    manifest.interval = "1Min"
    with pytest.raises(ValueError, match="provenance"):
        parse_price_series_artifact(payload, artifact, manifest, INSTRUMENT_ID)
    manifest.interval = "1Day"
    manifest.provider_status_version = None
    with pytest.raises(ValueError, match="provenance"):
        parse_price_series_artifact(payload, artifact, manifest, INSTRUMENT_ID)


def _records(payload: bytes) -> tuple[DataArtifact, PriceSeriesManifest]:
    artifact = DataArtifact(
        id=UUID("0198d500-1000-7000-8000-000000000078"),
        uri="memory://fixture",
        sha256=sha256(payload).hexdigest(),
        byte_size=len(payload),
        mime_type="application/vnd.apache.parquet",
        schema_version="price_bars_v1",
        source="fixture",
        observed_at=datetime(2026, 8, 21, 20, tzinfo=UTC),
        available_at=datetime(2026, 8, 21, 20, 1, tzinfo=UTC),
        received_at=datetime(2026, 8, 21, 20, 2, tzinfo=UTC),
    )
    manifest = PriceSeriesManifest(
        id=UUID("0198d500-1000-7000-8000-000000000079"),
        data_artifact_id=artifact.id,
        instrument=str(INSTRUMENT_ID),
        source="fixture",
        feed="fixture",
        delivery_mode="fixture",
        adjustment="all",
        interval="1Day",
        start_at=artifact.observed_at,
        end_at=artifact.observed_at,
        row_count=1,
    )
    return artifact, manifest
