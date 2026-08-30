"""Frozen identity and transaction locks for immutable market artifacts."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.canonical import canonical_json
from nexus.persistence.models.data import DataArtifact


def data_artifact_identity_sha256(artifact: DataArtifact) -> str:
    """Hash every persisted field that gives an artifact evidentiary meaning."""
    return sha256(
        canonical_json(
            {
                "id": str(artifact.id),
                "uri": artifact.uri,
                "sha256": artifact.sha256,
                "byte_size": artifact.byte_size,
                "mime_type": artifact.mime_type,
                "schema_version": artifact.schema_version,
                "source": artifact.source,
                "observed_at": _utc_text(artifact.observed_at),
                "available_at": _utc_text(artifact.available_at),
                "received_at": _utc_text(artifact.received_at),
                "created_at": _utc_text(artifact.created_at),
            }
        ).encode("utf-8")
    ).hexdigest()


async def lock_and_load_data_artifacts(
    session: AsyncSession,
    artifact_ids: Iterable[UUID],
) -> dict[UUID, DataArtifact]:
    """Lock and refresh artifacts in deterministic order for the outer transaction.

    The advisory key matches the database lineage triggers. The subsequent
    ``populate_existing`` read refreshes any identity-map value while retaining
    compatibility with the provider role's intentionally read-only privilege.
    """
    loaded: dict[UUID, DataArtifact] = {}
    for artifact_id in sorted(set(artifact_ids), key=str):
        await session.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": f"fosun-data-artifact:{artifact_id}"},
        )
        artifact = await session.scalar(
            select(DataArtifact)
            .where(DataArtifact.id == artifact_id)
            .execution_options(populate_existing=True)
        )
        if artifact is not None:
            loaded[artifact_id] = artifact
    return loaded


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("artifact authority time must be timezone-aware")
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
