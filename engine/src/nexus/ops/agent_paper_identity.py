"""Once-per-session SEC research identity preparation for the normal Paper host."""

import json
import os
import stat
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from uuid import UUID
from zoneinfo import ZoneInfo

from sqlalchemy import func, select, text

from nexus.agents.persistence import select_authoritative_snapshot
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.preparation_diagnostics import (
    preparation_call,
    preparation_rejected,
    preparation_step,
    preparation_unavailable,
)
from nexus.ops.research_identity_local import collect_identity_sources
from nexus.paper.agent_cycle_authority import load_agent_paper_service_calendar_authority
from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch
from nexus.paper.portfolio_policy import PORTFOLIO_ALLOCATION_V1
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.paper import AgentPaperCycleRecord
from nexus.persistence.session import session_factory
from nexus.research.current_identity_storage import (
    CAPACITY_REASON,
    SOURCE,
    _base,
    import_identity_binding,
    inspect_identity_binding_snapshot,
    read_identity_bindings,
)
from nexus.research.fmp_profile_archive import _read
from nexus.research.sec_exhibit_storage import configured_exhibit_store
from nexus.settings import EngineSettings


@dataclass(frozen=True)
class IdentityPreparationScope:
    day: date
    slot: datetime
    candidates: tuple[tuple[UUID, str, UUID | None], ...]
    registered: frozenset[UUID]
    ready: bool
    evidence_ready: bool
    capacity_exhausted: bool


async def _read_scope(
    settings: EngineSettings, *, account_id: UUID, batch_id: UUID, calendar_id: UUID
) -> IdentityPreparationScope | None:
    _validate_local_database(settings.database_url)
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            if (
                await session.scalar(text("SELECT version_num FROM alembic_version"))
                != CURRENT_MIGRATION_HEAD
            ):
                return None
            now = await session.scalar(select(func.clock_timestamp()))
            if not isinstance(now, datetime):
                raise ValueError("AGENT_PAPER_IDENTITY_CLOCK_INVALID")
            calendar = await load_agent_paper_service_calendar_authority(
                session, now=now, expected_calendar_authority_id=calendar_id
            )
            slot = datetime.combine(
                calendar.session,
                PORTFOLIO_ALLOCATION_V1.decision_local_time,
                tzinfo=ZoneInfo(PORTFOLIO_ALLOCATION_V1.decision_timezone),
            ).astimezone(UTC)
            if calendar.session != calendar.local_date or not calendar.market_open_at <= now < slot:
                return None
            if await session.scalar(
                select(AgentPaperCycleRecord.id)
                .where(
                    AgentPaperCycleRecord.account_id == account_id,
                    AgentPaperCycleRecord.decision_session == calendar.session,
                )
                .limit(1)
            ):
                return None
            _, candidates = await validate_agent_paper_candidate_batch(
                session, account_id=account_id, batch_id=batch_id, as_of=now
            )
            store = configured_exhibit_store(session)
            entries = []
            diagnostics = []
            registered = set()
            for candidate in sorted(candidates, key=lambda item: item.symbol):
                snapshot = await select_authoritative_snapshot(
                    session, instrument_id=candidate.instrument_id, as_of=now
                )
                diagnostics.append(
                    await inspect_identity_binding_snapshot(
                        session,
                        instrument_id=candidate.instrument_id,
                        snapshot=snapshot,
                        as_of=now,
                        decision_session=calendar.session,
                    )
                )
                bindings = await read_identity_bindings(
                    session,
                    instrument_id=candidate.instrument_id,
                    as_of=now,
                    decision_session=calendar.session,
                )
                base_id = next(iter(bindings.values())).identity_snapshot_id if bindings else None
                if bindings:
                    registered.add(candidate.instrument_id)
                existing = await session.scalar(
                    select(DataSnapshot.id)
                    .where(
                        DataSnapshot.source == SOURCE,
                        DataSnapshot.provider_object_id == str(candidate.instrument_id),
                        DataSnapshot.payload["decision_session"].astext
                        == calendar.session.isoformat(),
                    )
                    .limit(1)
                )
                if not bindings and existing is None:
                    bases = tuple(
                        await session.scalars(
                            select(DataSnapshot).where(
                                DataSnapshot.source.in_(
                                    ("current_instrument_identity", "current_listing_revision")
                                ),
                                DataSnapshot.payload["instrument_id"].astext
                                == str(candidate.instrument_id),
                                DataSnapshot.created_at <= now,
                            )
                        )
                    )
                    # Never select a newer or favorable proof from ambiguous history.
                    if len(bases) == 1:
                        try:
                            _, proof = await _base(session, store, bases[0].id, now)
                            if (
                                proof.instrument_id == candidate.instrument_id
                                and proof.identity.symbol == candidate.symbol
                            ):
                                base_id = bases[0].id
                        except ValueError:
                            pass
                entries.append((candidate.instrument_id, candidate.symbol, base_id))
            return IdentityPreparationScope(
                day=calendar.session,
                slot=slot,
                candidates=tuple(entries),
                registered=frozenset(registered),
                ready=all(item["snapshot_status"] == "ready" for item in diagnostics),
                evidence_ready=all(item["evidence_status"] == "ready" for item in diagnostics),
                capacity_exhausted=any(item["reason"] == CAPACITY_REASON for item in diagnostics),
            )
    finally:
        await factory.kw["bind"].dispose()


def _private_parent(root: Path) -> Path:
    parent = root
    for part in (".secrets", "nexus-agent-paper", "research-identity"):
        parent = parent / part
        if not parent.exists() and not parent.is_symlink():
            parent.mkdir(mode=0o700)
        metadata = parent.lstat()
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise ValueError("AGENT_PAPER_IDENTITY_DIRECTORY_INVALID")
    return parent


def _archive_result(directory: Path) -> object:
    fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(fd)
        if metadata.st_uid != os.getuid() or stat.S_IMODE(metadata.st_mode) != 0o700:
            raise ValueError("AGENT_PAPER_IDENTITY_DIRECTORY_INVALID")
        return json.loads(_read(fd, "result.json", 8192))
    finally:
        os.close(fd)


def _import_failure_marker(path: Path) -> None:
    # The transaction context has already rolled back a known validation failure.
    # A sibling receipt does not alter the strict source archive's file set.
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    except FileExistsError:
        return
    with os.fdopen(fd, "w") as handle:
        json.dump(
            {"status": "failed", "stage": "import", "code": "AGENT_PAPER_IDENTITY_IMPORT_FAILED"},
            handle,
        )


async def prepare_agent_paper_identity_once(
    settings: EngineSettings,
    *,
    account_id: UUID,
    batch_id: UUID,
    expected_calendar_authority_id: UUID,
    repo_root: Path,
    contact: str,
) -> str:
    """Archive once, atomically register the batch, then freeze future snapshots."""
    if not contact:
        return preparation_unavailable("research_identity", "configuration", "CONTACT_MISSING")
    scope = await preparation_call(
        "research_identity",
        "scope",
        _read_scope(
            settings,
            account_id=account_id,
            batch_id=batch_id,
            calendar_id=expected_calendar_authority_id,
        ),
    )
    if scope is None:
        return preparation_unavailable("research_identity", "scope", "SCOPE_UNAVAILABLE")
    if scope.capacity_exhausted:
        return preparation_unavailable("research_identity", "scope", "CAPACITY_EXHAUSTED")
    if scope.ready:
        return "ready"
    if any(base is None for _, _, base in scope.candidates):
        return preparation_unavailable("research_identity", "scope", "IDENTITY_BASE_MISSING")
    if not scope.evidence_ready:
        with preparation_step("research_identity", "archive"):
            directory = _private_parent(repo_root) / scope.day.strftime("%Y%m%d")
        failure_marker = directory.with_name(directory.name + "-import-failure.json")
        if failure_marker.exists() or failure_marker.is_symlink():
            return preparation_unavailable("research_identity", "archive", "PRIOR_IMPORT_FAILED")
        result: object
        if not directory.exists() and not directory.is_symlink():
            try:
                result = await preparation_call(
                    "research_identity",
                    "collect",
                    collect_identity_sources(
                        symbols=tuple(symbol for _, symbol, _ in scope.candidates),
                        contact=contact,
                        output_dir=directory,
                    ),
                )
            except ValueError as error:
                if str(error) != "IDENTITY_COLLECTION_FAILED":
                    raise
                # The collector persisted its fixed failure receipt. Keep other
                # independent preparations running without retrying this source.
                return preparation_unavailable(
                    "research_identity", "collect", "COLLECTION_UNAVAILABLE"
                )
        else:
            # Existing incomplete or failed archives never cause a fresh request.
            try:
                with preparation_step("research_identity", "archive"):
                    result = _archive_result(directory)
            except (OSError, ValueError):
                return preparation_unavailable("research_identity", "archive", "ARCHIVE_INVALID")
        if not isinstance(result, dict) or result.get("status") != "archived":
            return preparation_unavailable("research_identity", "archive", "ARCHIVE_INVALID")
        refreshed = await preparation_call(
            "research_identity",
            "readback",
            _read_scope(
                settings,
                account_id=account_id,
                batch_id=batch_id,
                calendar_id=expected_calendar_authority_id,
            ),
        )
        if (
            refreshed is None
            or refreshed.candidates != scope.candidates
            or refreshed.registered != scope.registered
        ):
            return preparation_unavailable("research_identity", "readback", "SCOPE_CHANGED")
        factory = session_factory(settings)
        try:
            with preparation_step("research_identity", "import"):
                async with factory() as session, session.begin():
                    store = configured_exhibit_store(session)
                    for instrument_id, _, base_id in scope.candidates:
                        if instrument_id in scope.registered:
                            continue
                        assert base_id is not None
                        await preparation_call(
                            "research_identity",
                            "import",
                            import_identity_binding(
                                session,
                                store,
                                identity_snapshot_id=base_id,
                                collection_dir=directory,
                            ),
                        )
                    now = await session.scalar(select(func.clock_timestamp()))
                    if (
                        not isinstance(now, datetime)
                        or now >= scope.slot
                        or now.astimezone(ZoneInfo("America/New_York")).date() != scope.day
                    ):
                        raise ValueError("AGENT_PAPER_IDENTITY_WINDOW_CLOSED")
        except ValueError as error:
            with preparation_step("research_identity", "archive"):
                _import_failure_marker(failure_marker)
            return preparation_rejected("research_identity", "import", error)
        finally:
            await factory.kw["bind"].dispose()
    # Use the existing host producer, preserving macro/trend snapshot contracts.
    from nexus.ops.agent_paper_loop import _prepare_current_trends_research_snapshot

    for instrument_id, _, _ in scope.candidates:
        current = await preparation_call(
            "research_identity",
            "readback",
            _read_scope(
                settings,
                account_id=account_id,
                batch_id=batch_id,
                calendar_id=expected_calendar_authority_id,
            ),
        )
        if current is None or current.candidates != scope.candidates:
            return preparation_unavailable("research_identity", "readback", "SCOPE_CHANGED")
        await preparation_call(
            "research_identity",
            "snapshot",
            _prepare_current_trends_research_snapshot(
                settings, instrument_id=instrument_id, now=datetime.now(UTC)
            ),
        )
    verified = await preparation_call(
        "research_identity",
        "readback",
        _read_scope(
            settings,
            account_id=account_id,
            batch_id=batch_id,
            calendar_id=expected_calendar_authority_id,
        ),
    )
    return (
        "prepared"
        if verified is not None and verified.ready
        else preparation_unavailable("research_identity", "readback", "READBACK_MISMATCH")
    )
