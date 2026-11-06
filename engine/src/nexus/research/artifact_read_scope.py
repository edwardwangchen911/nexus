"""Read-local immutable registrations, never cached source qualifications.

The schema's diagnostic-authority trigger forbids DataArtifact UPDATE/DELETE,
and its digest is unique. Callers still check every binding, time and original.
Pending ORM writes and existing identity-map objects retain the original query.
"""

from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, fields
from datetime import datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.data import DataArtifact


@dataclass(frozen=True, slots=True)
class ArtifactRegistration:
    id: UUID
    uri: str
    sha256: str
    byte_size: int
    mime_type: str
    schema_version: str
    source: str
    observed_at: datetime
    available_at: datetime
    received_at: datetime
    created_at: datetime


class _Scope:
    def __init__(self, session: AsyncSession, limit: int) -> None:
        self.owner = session
        self.limit = limit
        self.active = True
        self.rows: dict[str, ArtifactRegistration] = {}
        self.transaction: tuple[object, object] | None = None


def _transaction(session: AsyncSession) -> tuple[object, object]:
    return (session.sync_session.get_transaction(), session.sync_session.get_nested_transaction())


_KEY = "nexus_immutable_artifact_read_scope"


@contextmanager
def artifact_read_scope(session: AsyncSession, *, max_entries: int = 256) -> Iterator[None]:
    if type(max_entries) is not int or not 1 <= max_entries <= 512:
        raise ValueError("ARTIFACT_READ_SCOPE_LIMIT_INVALID")
    present, previous = _KEY in session.info, session.info.get(_KEY)
    scope = _Scope(session, max_entries)
    session.info[_KEY] = scope
    try:
        yield
    finally:
        scope.active = False
        scope.rows.clear()
        if present:
            session.info[_KEY] = previous
        else:
            session.info.pop(_KEY, None)


async def read_artifact(
    session: AsyncSession, digest: str
) -> DataArtifact | ArtifactRegistration | None:
    scope = session.info.get(_KEY)
    if isinstance(scope, _Scope) and scope.owner is session:
        current = _transaction(session)
        if current != scope.transaction:
            scope.rows.clear()
            scope.transaction = current
    reusable = (
        isinstance(scope, _Scope)
        and scope.active
        and scope.owner is session
        and not (session.new or session.dirty or session.deleted)
    )
    if reusable and isinstance(scope, _Scope) and (row := scope.rows.get(digest)) is not None:
        if session.identity_key(DataArtifact, row.id) not in session.identity_map:
            return row
    record = await session.scalar(select(DataArtifact).where(DataArtifact.sha256 == digest))
    if (
        reusable
        and isinstance(scope, _Scope)
        and scope.active
        and record is not None
        and record.sha256 == digest
        and len(scope.rows) < scope.limit
        and not (session.new or session.dirty or session.deleted)
    ):
        current = _transaction(session)
        if current != scope.transaction:
            scope.rows.clear()
            scope.transaction = current
        scope.rows[digest] = ArtifactRegistration(
            **{field.name: getattr(record, field.name) for field in fields(ArtifactRegistration)}
        )
    return record
