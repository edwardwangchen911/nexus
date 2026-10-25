"""Once-only exact filing collection between short, account-scoped preparation transactions."""

import asyncio
import json
import os
import stat
from datetime import datetime
from pathlib import Path
from uuid import UUID, uuid4

from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from nexus.audit.service import append_audit_event
from nexus.monitoring.admission import require_increment_binding
from nexus.monitoring.batch_inputs import SOURCE, prepare_batch_inputs, read_batch_inputs
from nexus.monitoring.daily_consumption import not_daily_covered
from nexus.monitoring.mixed_readback import not_mixed_consumed
from nexus.monitoring.storage import lock_security
from nexus.ops.agent_paper_once import _validate_local_database
from nexus.ops.monitor_content_local import _contact, collect_filing_content
from nexus.paper.candidate_batch import validate_agent_paper_candidate_batch
from nexus.paper.manual_request import require_manual_account_available
from nexus.paper.target_service import lock_portfolio_account
from nexus.persistence.migrations import CURRENT_MIGRATION_HEAD, SCHEMA_ADMISSION_LOCK_KEY
from nexus.persistence.models.agents import AgentResearchRunRecord
from nexus.persistence.models.data import DataSnapshot
from nexus.persistence.models.monitoring import (
    MonitorCheck,
    MonitorIncrement,
    MonitorObservationBatch,
    MonitorTrigger,
)
from nexus.persistence.models.runtime import AuditEvent
from nexus.persistence.session import session_factory
from nexus.settings import EngineSettings

CLAIM = "research.monitor.material_collection_claimed"
RESULT = "research.monitor.material_preparation_result"
SCHEMA = "monitor_batch_prepare_v1"


def _result(
    batch_id: UUID, status: str, reason: str | None = None, **extra: object
) -> dict[str, object]:
    return {
        "batch_id": str(batch_id),
        "status": status,
        "reason": reason,
        "provider_requests": 0,
        "model_requests": 0,
        "job_created": False,
        "execution_locked": True,
        **extra,
    }


async def _history(
    session: AsyncSession, batch: MonitorObservationBatch
) -> tuple[bool, dict[str, object] | None]:
    rows = tuple(
        await session.scalars(
            select(AuditEvent)
            .where(
                AuditEvent.action.in_((CLAIM, RESULT)),
                AuditEvent.payload["batch_id"].astext == str(batch.id),
            )
            .order_by(AuditEvent.event_seq)
        )
    )
    claims: set[str] = set()
    settled: set[str] = set()
    latest = None
    for row in rows:
        payload = row.payload
        operation = payload.get("operation_id")
        if (
            row.schema_version != SCHEMA
            or not isinstance(operation, str)
            or payload.get("account_id") != str(batch.account_id)
            or payload.get("instrument_id") != str(batch.instrument_id)
        ):
            return True, None
        if row.action == CLAIM:
            if operation in claims or operation in settled:
                return True, None
            claims.add(operation)
        else:
            if operation in settled or payload.get("status") not in {
                "prepared",
                "preparing",
                "blocked",
                "unknown",
            }:
                return True, None
            settled.add(operation)
            latest = payload
    return bool(claims - settled), latest


async def inspect_batch_preparation(session: AsyncSession, batch_id: UUID) -> str:
    batch = await session.get(MonitorObservationBatch, batch_id)
    if batch is None:
        return "blocked"
    unfinished, latest = await _history(session, batch)
    if unfinished or latest is not None and latest.get("status") == "unknown":
        return "unknown"
    row = await session.scalar(
        select(DataSnapshot)
        .where(DataSnapshot.source == SOURCE, DataSnapshot.provider_object_id == str(batch_id))
        .order_by(DataSnapshot.created_at, DataSnapshot.id)
        .limit(1)
    )
    if row is not None:
        now = await session.scalar(select(func.clock_timestamp()))
        if not isinstance(now, datetime):
            return "unknown"
        return (
            "prepared"
            if await read_batch_inputs(session, snapshot_id=row.id, as_of=now) is not None
            else "blocked"
        )
    return "blocked" if latest is not None and latest.get("status") == "blocked" else "pending"


async def confirm_batch_preparation(
    settings: EngineSettings, *, account_id: UUID, batch_id: UUID
) -> bool:
    _validate_local_database(settings.database_url)
    if account_id != settings.agent_paper_account_id:
        return False
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            await session.execute(text("SET TRANSACTION READ ONLY"))
            batch = await session.get(MonitorObservationBatch, batch_id)
            if batch is None or batch.account_id != account_id:
                return False
            unfinished, latest = await _history(session, batch)
            return (
                not unfinished
                and latest is not None
                and latest.get("status") in {"prepared", "preparing", "blocked"}
            )
    finally:
        await factory.kw["bind"].dispose()


def _archive_root(repo_root: Path) -> Path:
    if repo_root.is_symlink() or not repo_root.is_dir() or repo_root.stat().st_uid != os.getuid():
        raise ValueError("MONITOR_COLLECTION_DIRECTORY_INVALID")
    root = repo_root.resolve()
    for name in (".secrets", "research-monitor-collections"):
        root = root / name
        if not root.exists() and not root.is_symlink():
            root.mkdir(mode=0o700)
        if (
            root.is_symlink()
            or not root.is_dir()
            or root.stat().st_uid != os.getuid()
            or stat.S_IMODE(root.stat().st_mode) != 0o700
        ):
            raise ValueError("MONITOR_COLLECTION_DIRECTORY_INVALID")
    return root


async def _lock_scope(
    session: AsyncSession, account_id: UUID, batch_id: UUID
) -> tuple[MonitorObservationBatch, datetime]:
    await session.execute(text("SET LOCAL statement_timeout='5s'"))
    await session.execute(text("SET LOCAL lock_timeout='2s'"))
    await session.execute(
        text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key,0))"),
        {"key": SCHEMA_ADMISSION_LOCK_KEY},
    )
    if (
        await session.scalar(text("SELECT version_num FROM alembic_version"))
        != CURRENT_MIGRATION_HEAD
    ):
        raise ValueError("MONITOR_SCHEMA_UNAVAILABLE")
    await lock_portfolio_account(session, account_id)
    batch = await session.get(MonitorObservationBatch, batch_id, populate_existing=True)
    if batch is None or batch.account_id != account_id:
        raise ValueError("MONITOR_BATCH_UNAVAILABLE")
    await lock_security(session, account_id=account_id, instrument_id=batch.instrument_id)
    now = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(now, datetime):
        raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
    return batch, now


async def _record(
    session: AsyncSession,
    batch: MonitorObservationBatch,
    operation: UUID,
    result: dict[str, object],
    now: datetime,
) -> None:
    completed = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(completed, datetime) or completed < now:
        raise ValueError("MONITOR_CLOCK_UNAVAILABLE")
    await append_audit_event(
        session,
        actor_id="research-monitor",
        action=RESULT,
        schema_version=SCHEMA,
        occurred_at=completed,
        payload={
            **result,
            "operation_id": str(operation),
            "account_id": str(batch.account_id),
            "instrument_id": str(batch.instrument_id),
        },
    )


async def _eligible(
    session: AsyncSession, settings: EngineSettings, batch: MonitorObservationBatch, now: datetime
) -> tuple[MonitorTrigger, ...]:
    await require_manual_account_available(session, account_id=batch.account_id, now=now)
    candidate, _ = await validate_agent_paper_candidate_batch(
        session,
        batch_id=settings.agent_paper_candidate_batch_id,
        account_id=batch.account_id,
        as_of=now,
    )
    if batch.instrument_id not in candidate.instrument_ids:
        raise ValueError("MONITOR_SCOPE_INVALID")
    from nexus.monitoring.mixed_readback import unadmitted_stock_count

    if (
        await unadmitted_stock_count(
            session, account_id=batch.account_id, instrument_id=batch.instrument_id
        )
        > 32
    ):
        raise ValueError("MONITOR_PENDING_OVERFLOW")
    members = tuple(
        await session.scalars(
            select(MonitorTrigger)
            .where(MonitorTrigger.batch_id == batch.id, not_daily_covered(), not_mixed_consumed())
            .order_by(MonitorTrigger.id)
        )
    )
    if not 1 <= len(batch.trigger_ids) <= 32:
        raise ValueError("MONITOR_PENDING_OVERFLOW")
    if {str(t.id) for t in members} != set(batch.trigger_ids) or any(
        t.account_id != batch.account_id
        or t.instrument_id != batch.instrument_id
        or t.status not in {"pending", "blocked"}
        or t.research_run_id is not None
        for t in members
    ):
        raise ValueError("MONITOR_TRIGGER_UNAVAILABLE")
    return members


async def prepare_pending_batch(
    settings: EngineSettings, *, batch_id: UUID, repo_root: Path, sec_contact: str | None = None
) -> dict[str, object]:
    if not settings.agent_paper_cycle_enabled or settings.agent_paper_account_id is None:
        raise ValueError("MONITOR_HOST_DISABLED")
    _validate_local_database(settings.database_url)
    account_id = settings.agent_paper_account_id
    operation = uuid4()
    factory = session_factory(settings)
    try:
        async with factory() as session, session.begin():
            batch, now = await _lock_scope(session, account_id, batch_id)
            prior = await session.scalar(
                select(MonitorIncrement).where(MonitorIncrement.batch_id == batch_id)
            )
            if prior is not None:
                run = await session.get(AgentResearchRunRecord, prior.run_id)
                if run is None:
                    raise ValueError("MONITOR_INCREMENT_BINDING_INVALID")
                bound = await require_increment_binding(session, run)
                return _result(
                    batch_id,
                    "blocked",
                    "MONITOR_BATCH_ALREADY_ADMITTED",
                    run_id=str(bound.run_id),
                    job_id=str(bound.job_id),
                )
            state = await inspect_batch_preparation(session, batch_id)
            if state in {"blocked", "unknown"}:
                return _result(batch_id, state, "MONITOR_PREPARATION_" + state.upper())
            try:
                members = await _eligible(session, settings, batch, now)
                result = await prepare_batch_inputs(
                    session, account_id=account_id, batch_id=batch_id, now=now
                )
                if (
                    result["status"] == "prepared"
                    or result.get("reason") != "MONITOR_CONTENT_MISSING"
                ):
                    result = {"reason": None, **result}
                    await _record(session, batch, operation, result, now)
                    return result
                missing = result.get("missing_event_ids")
                if not isinstance(missing, list) or not missing:
                    raise ValueError("MONITOR_CONTEXT_INVALID")
                selected = [
                    t for t in members if t.kind == "disclosure" and t.event_key == missing[0]
                ]
                if len(selected) != 1:
                    raise ValueError("MONITOR_CONTEXT_INVALID")
                trigger = selected[0]
                check = await session.get(MonitorCheck, trigger.check_id)
                if check is None or not isinstance(check.observation, dict):
                    raise ValueError("MONITOR_CONTEXT_INVALID")
                index_id = UUID(str(check.observation["current_snapshot_id"]))
                entry = check.observation.get("entry")
                if not isinstance(entry, dict):
                    raise ValueError("MONITOR_CONTEXT_INVALID")
                accession = str(entry["accession_number"])
                _contact(sec_contact or "")
                archive = _archive_root(repo_root) / str(operation)
            except ValueError as error:
                code = (
                    str(error)
                    if str(error)
                    in {
                        "MONITOR_PENDING_OVERFLOW",
                        "MONITOR_SCOPE_INVALID",
                        "MONITOR_TRIGGER_UNAVAILABLE",
                        "MONITOR_ACCOUNT_BUSY_OR_UNKNOWN",
                        "SEC_MONITOR_CONTACT_REQUIRED",
                        "MONITOR_COLLECTION_DIRECTORY_INVALID",
                    }
                    else "MONITOR_PREPARATION_INELIGIBLE"
                )
                result = _result(batch_id, "blocked", code)
                await _record(session, batch, operation, result, now)
                return result
            await append_audit_event(
                session,
                actor_id="research-monitor",
                action=CLAIM,
                schema_version=SCHEMA,
                occurred_at=now,
                payload={
                    "operation_id": str(operation),
                    "account_id": str(account_id),
                    "instrument_id": str(batch.instrument_id),
                    "batch_id": str(batch_id),
                    "trigger_id": str(trigger.id),
                    "event_key": trigger.event_key,
                    "index_snapshot_id": str(index_id),
                    "accession": accession,
                    "archive_dir": str(archive),
                },
            )
        # No account/security lock or DB transaction is held across this bounded HTTP operation.
        try:
            async with asyncio.timeout(60):
                collected = await collect_filing_content(
                    settings,
                    index_snapshot_id=index_id,
                    accession=accession,
                    archive_dir=archive,
                    contact=sec_contact or "",
                )
        except Exception:
            receipt = archive / "result.json"
            try:
                if (
                    receipt.is_symlink()
                    or not receipt.is_file()
                    or receipt.stat().st_uid != os.getuid()
                    or stat.S_IMODE(receipt.stat().st_mode) != 0o600
                    or receipt.stat().st_size > 65536
                ):
                    raise ValueError("missing receipt")
                failure = json.loads(receipt.read_bytes())
                count = failure["provider_requests"]
                if (
                    type(count) is not int
                    or not 0 <= count <= 18
                    or failure.get("status") != "unavailable"
                ):
                    raise ValueError("invalid receipt")
            except (OSError, ValueError, KeyError, TypeError):
                # The original claim remains unfinished. Never manufacture a terminal receipt.
                return _result(
                    batch_id, "unknown", "MONITOR_COLLECTION_UNCONFIRMED", provider_requests=None
                )
            async with factory() as session, session.begin():
                batch, now = await _lock_scope(session, account_id, batch_id)
                result = _result(
                    batch_id,
                    "blocked",
                    "MONITOR_CONTENT_COLLECTION_FAILED",
                    provider_requests=count,
                    collection_stage=failure.get("stage")
                    if failure.get("stage") in {"inventory", "documents", "import"}
                    else "unknown",
                )
                await _record(session, batch, operation, result, now)
                return result
        async with factory() as session, session.begin():
            batch, now = await _lock_scope(session, account_id, batch_id)
            try:
                await _eligible(session, settings, batch, now)
                result = await prepare_batch_inputs(
                    session, account_id=account_id, batch_id=batch_id, now=now
                )
                if result.get("reason") == "MONITOR_CONTENT_MISSING":
                    result["status"] = "preparing"
                result = {
                    "reason": None,
                    **result,
                    "provider_requests": collected["provider_requests"],
                    "collection_snapshot_id": collected["snapshot_id"],
                    "collection_evidence_ref_id": collected["evidence_ref_id"],
                    "collection_hash": collected["canonical_hash"],
                }
            except ValueError:
                result = _result(
                    batch_id,
                    "blocked",
                    "MONITOR_PREPARATION_INELIGIBLE",
                    provider_requests=collected["provider_requests"],
                )
            await _record(session, batch, operation, result, now)
            return result
    finally:
        await factory.kw["bind"].dispose()
