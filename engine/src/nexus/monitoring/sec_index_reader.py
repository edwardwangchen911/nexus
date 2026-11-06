"""Explicit capture-version routing for monitor evidence consumers, never a baseline rewrite."""

from datetime import datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from nexus.persistence.models.data import DataSnapshot
from nexus.research.sec_disclosure_index import SecDisclosureIndex
from nexus.research.sec_exhibit_storage import ExhibitStore
from nexus.research.sec_index_storage import read_index_capture
from nexus.research.sec_listed_storage import SOURCE, read_listed_capture


async def read_monitor_index(
    session: AsyncSession, store: ExhibitStore, *, snapshot_id: UUID, as_of: datetime
) -> tuple[DataSnapshot, SecDisclosureIndex]:
    row = await session.get(DataSnapshot, snapshot_id)
    if row is not None and row.source == SOURCE:
        return await read_listed_capture(session, store, snapshot_id=snapshot_id, as_of=as_of)
    return await read_index_capture(session, store, snapshot_id=snapshot_id, as_of=as_of)
