"""Publish optional SEC baselines from the scheduled host's verified identity originals."""

from datetime import date, datetime
from typing import TYPE_CHECKING
from zoneinfo import ZoneInfo

from sqlalchemy import select

from nexus.ops.preparation_diagnostics import exception_code, preparation_unavailable
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.session import session_factory
from nexus.research.current_identity_storage import ResearchIdentityBinding, _clock, _read_artifact
from nexus.research.sec_exhibit_storage import configured_exhibit_store
from nexus.research.sec_index_storage import (
    SOURCE,
    IndexCapture,
    persist_index_archive,
    read_index_capture,
)
from nexus.settings import EngineSettings

if TYPE_CHECKING:
    from nexus.ops.formal_research_preparation import FormalPreparationContext


async def prepare_identity_indexes(
    settings: EngineSettings,
    bindings: tuple[ResearchIdentityBinding, ...],
    *,
    day: date,
    cutoff: datetime,
    execution: "FormalPreparationContext | None",
) -> None:
    if execution is not None and execution.reservation.trigger_mode != "scheduled":
        return
    factory = session_factory(settings)
    try:
        for binding in bindings:
            try:
                async with factory.begin() as session:
                    now = await _clock(session)
                    if binding.decision_session != day or not _in_window(now, day, cutoff):
                        return
                    if execution is not None:
                        from nexus.ops.formal_research_preparation import require_preparation_scope

                        await require_preparation_scope(session, execution, now=now)
                    store = configured_exhibit_store(session)
                    key = f"{binding.instrument_id}:{binding.source_artifacts[3].sha256}"
                    existing = (
                        await session.scalars(
                            select(DataSnapshot).where(
                                DataSnapshot.source == SOURCE,
                                DataSnapshot.provider_object_id == key,
                            )
                        )
                    ).one_or_none()
                    if existing is not None:
                        _, index = await read_index_capture(
                            session, store, snapshot_id=existing.id, as_of=now
                        )
                        capture = IndexCapture.model_validate(existing.payload)
                        if index.identity_snapshot_id != binding.identity_snapshot_id or tuple(
                            item.sha256 for item in capture.originals
                        ) != tuple(item.sha256 for item in binding.source_artifacts):
                            raise ValueError("SEC_INDEX_IDENTITY_INVALID")
                        continue
                    raw = [
                        await _read_artifact(session, store, item, now)
                        for item in binding.source_artifacts
                    ]
                    await persist_index_archive(
                        session,
                        store,
                        identity_snapshot_id=binding.identity_snapshot_id,
                        directory=(raw[0], raw[1]),
                        submissions=(raw[2], raw[3]),
                        now=now,
                    )
                    end = await _clock(session)
                    if not _in_window(end, day, cutoff):
                        raise ValueError("SEC_INDEX_WINDOW_CLOSED")
                    if execution is not None:
                        await require_preparation_scope(session, execution, now=end)
            except Exception as error:
                # Optional monitoring evidence cannot grant or veto the underlying research.
                # Each issuer commits independently. A later preparation verifies exact captures.
                preparation_unavailable("sec_index", "import", exception_code(error), error=error)
    finally:
        await factory.kw["bind"].dispose()


def _in_window(now: datetime, day: date, cutoff: datetime) -> bool:
    return now < cutoff and now.astimezone(ZoneInfo("America/New_York")).date() == day
