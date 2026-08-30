"""Frozen market-artifact identity contracts."""

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest
from nexus.market.artifact_authority import data_artifact_identity_sha256
from nexus.persistence.models.data import DataArtifact

NOW = datetime(2026, 8, 30, 8, tzinfo=UTC)


def _artifact(**changes: object) -> DataArtifact:
    values: dict[str, object] = {
        "id": UUID("019d0000-0000-7000-8000-0000000000a1"),
        "uri": "memory://raw/market/fosun/example.json",
        "sha256": "a" * 64,
        "byte_size": 42,
        "mime_type": "application/json",
        "schema_version": "fosun_gateway_market_bars_v1",
        "source": "fosun",
        "observed_at": NOW,
        "available_at": NOW + timedelta(seconds=1),
        "received_at": NOW + timedelta(seconds=2),
        "created_at": NOW + timedelta(seconds=3),
    }
    values.update(changes)
    return DataArtifact(**values)


@pytest.mark.parametrize(
    "changes",
    (
        {"id": UUID("019d0000-0000-7000-8000-0000000000a2")},
        {"uri": "memory://raw/market/fosun/other.json"},
        {"sha256": "b" * 64},
        {"byte_size": 43},
        {"mime_type": "application/octet-stream"},
        {"schema_version": "other_v1"},
        {"source": "other"},
        {"observed_at": NOW + timedelta(microseconds=1)},
        {"available_at": NOW + timedelta(seconds=1, microseconds=1)},
        {"received_at": NOW + timedelta(seconds=2, microseconds=1)},
        {"created_at": NOW + timedelta(seconds=3, microseconds=1)},
    ),
)
def test_artifact_identity_hash_covers_every_persisted_authority_field(
    changes: dict[str, object],
) -> None:
    assert data_artifact_identity_sha256(_artifact(**changes)) != (
        data_artifact_identity_sha256(_artifact())
    )


def test_artifact_identity_hash_is_deterministic() -> None:
    assert data_artifact_identity_sha256(_artifact()) == data_artifact_identity_sha256(_artifact())
